use super::{CellRange, SheetCoordinate, SheetRange};
use crate::size_check::NonNegativeIsize;
use crate::{Cell, Sheet};
use im::vector::Focus;
use im::Vector;
use std::cmp;
use std::isize;

/// A trait that allows you to create an IntoIterator-like with the context of T
pub trait IntoIteratorWith<'a, T> {
    type Item;
    type IntoIter;

    fn into_iter_with(self, state: &'a T, allow_unbounded: bool) -> Self::IntoIter;
}

/// Create an iterator over a CellRange with the context of a Sheet.
impl<'a> IntoIteratorWith<'a, Sheet> for &'a CellRange {
    type Item = SheetCoordinate;
    type IntoIter = CellRangeIterator<'a>;

    fn into_iter_with(self, sheet: &'a Sheet, _allow_unbouded: bool) -> Self::IntoIter {
        CellRangeIterator {
            sheet,
            cell_range: self.clone(),
            current: self.start().clone(),
        }
    }
}

/// Create an iterator over a SheetRange with the context of a Sheet.
impl<'a> IntoIteratorWith<'a, Sheet> for &'a SheetRange {
    type Item = SheetCoordinate;
    type IntoIter = CellRangeIterator<'a>;

    fn into_iter_with(self, sheet: &'a Sheet, allow_unbounded: bool) -> Self::IntoIter {
        match self {
            SheetRange::CellRange(cell_range) => CellRangeIterator {
                sheet,
                cell_range: cell_range.clone(),
                current: cell_range.start().clone(),
            },
            SheetRange::RowRange((start, end)) => {
                let end_col = if allow_unbounded {
                    isize::max_size() as usize
                } else {
                    sheet.columns_len()
                };

                CellRangeIterator {
                    sheet,
                    cell_range: CellRange::new(*start, *end, 0, end_col)
                        .expect("Should have valid range."),
                    current: SheetCoordinate::new(*start, 0)
                        .expect("Start row index of SheetRange::RowRange should already be valid."),
                }
            }
            SheetRange::ColumnRange((start, end)) => {
                let end_row = if allow_unbounded {
                    isize::max_size() as usize
                } else {
                    sheet
                        .columns()
                        .iter()
                        .fold(0, |acc, col| cmp::max(acc, col.cells_len()))
                };

                CellRangeIterator {
                    sheet,
                    cell_range: CellRange::new(0, end_row, *start, *end)
                        .expect("Should have valid range."),
                    current: SheetCoordinate::new(0, *start).expect(
                        "Start col index of SheetRange::ColumnRange should already be valid.",
                    ),
                }
            }
        }
    }
}

/// An iterator over a CellRange with the context of a Sheet.
pub struct CellRangeIterator<'a> {
    pub sheet: &'a Sheet,
    pub cell_range: CellRange,
    pub current: SheetCoordinate,
}

impl<'a> Iterator for CellRangeIterator<'a> {
    type Item = SheetCoordinate;

    // TODO: Perf enhancements, make it more cache friendly.
    fn next(&mut self) -> Option<Self::Item> {
        if self.current.col() > self.cell_range.end().col() {
            return None;
        }

        let has_cell = self.sheet.cell_at(&self.current);

        match has_cell {
            None => None,
            Some(_) => {
                let next_row = self
                    .sheet
                    .column_at(self.current.col())
                    .map(|col| {
                        if col.cells_len() > self.current.row() + 1
                            && self.cell_range.end().row() > self.current.row()
                        {
                            self.current.row() + 1
                        } else {
                            0
                        }
                    })
                    .expect("This should have been a valid column");

                let next_col = match next_row {
                    0 => self.current.col() + 1,
                    _ => self.current.col(),
                };

                let result = self.current.clone();

                self.current = SheetCoordinate::new(next_row, next_col)
                    .expect("Calculated next row and next column should be valid.");

                Some(result)
            }
        }
    }
}

/// An iterator over a CellRangeView. The CellRangeView itself already contains the context necessary (it already contains cells.)
pub struct CellRangeViewIterator<'a> {
    view: Vector<Focus<'a, Cell>>,
    current: im::vector::Iter<'a, Cell>,
}

impl CellRangeViewIterator<'_> {
    pub fn new<'a>(
        view: Vector<Focus<'a, Cell>>,
        current: im::vector::Iter<'a, Cell>,
    ) -> CellRangeViewIterator<'a> {
        CellRangeViewIterator { view, current }
    }
}

impl<'a> Iterator for CellRangeViewIterator<'a> {
    type Item = &'a Cell;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current.next() {
            Some(cell) => Some(cell),
            None => match self.view.pop_front() {
                Some(new_focus) => {
                    self.current = new_focus.into_iter();
                    self.next()
                }
                None => None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::location::Coordinate;
    use crate::CoreDocument;

    #[test]
    fn test_cell_range_iterator() {
        let cells = vec![
            vec![Some("=1".to_string()), Some("hello".to_string())],
            vec![Some("=3".to_string()), Some("world".to_string())],
        ];

        let mut document = CoreDocument::new();
        document.add_sheet();
        document
            .insert_cell_facts(&Coordinate::new(0, 0, 0).unwrap(), cells.clone())
            .unwrap();

        let a1_b2 = CellRange::new(0, 1, 0, 1).expect("Fail");
        let iterated: Vec<_> = a1_b2
            .into_iter_with(document.sheet_at(0).unwrap(), false)
            .collect();

        assert_eq!(
            iterated,
            vec![
                SheetCoordinate::new(0, 0).unwrap(),
                SheetCoordinate::new(1, 0).unwrap(),
                SheetCoordinate::new(0, 1).unwrap(),
                SheetCoordinate::new(1, 1).unwrap()
            ]
        );
    }

    #[test]
    fn test_cell_range_iterator_limited_range() {
        let cells = vec![
            vec![
                Some("=1".to_string()),
                Some("hello".to_string()),
                Some("hello2".to_string()),
                Some("hello3".to_string()),
            ],
            vec![Some("=3".to_string()), Some("world".to_string())],
        ];

        let mut document = CoreDocument::new();
        document.add_sheet();
        document
            .insert_cell_facts(&Coordinate::new(0, 0, 0).unwrap(), cells.clone())
            .unwrap();

        let a1_a3 = CellRange::new(0, 2, 0, 0).expect("Fail");
        let iterated: Vec<_> = a1_a3
            .into_iter_with(document.sheet_at(0).unwrap(), false)
            .collect();

        assert_eq!(
            iterated,
            vec![
                SheetCoordinate::new(0, 0).unwrap(),
                SheetCoordinate::new(1, 0).unwrap(),
                SheetCoordinate::new(2, 0).unwrap()
            ]
        );
    }
}
