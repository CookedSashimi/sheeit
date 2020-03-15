use sheeit_storage::location::{CellRange, CellRangeViewIterator, SheetRange};
use sheeit_storage::{Sheet, Value};
use std::cmp;

pub trait IntoValueIteratorWith<'a, T> {
    type Item;
    type IntoIter;

    fn into_value_iter_with(self, state: &'a T) -> Self::IntoIter;
}

pub struct CellRangeIterator<'a> {
    pub cell_iterator: CellRangeViewIterator<'a>,
}

// TODO: Consolidate it with the implementation in coordinate.rs
impl<'a> IntoValueIteratorWith<'a, Sheet> for &'a SheetRange {
    type Item = &'a Value;
    type IntoIter = CellRangeIterator<'a>;

    fn into_value_iter_with(self, sheet: &'a Sheet) -> Self::IntoIter {
        match self {
            SheetRange::CellRange(cell_range) => CellRangeIterator {
                cell_iterator: sheet.cells_in_range(cell_range).into_iter(),
            },
            SheetRange::RowRange((start, end)) => {
                let end_col = sheet.columns_len();
                let cell_range =
                    CellRange::new(*start, *end, 0, end_col).expect("Should have valid range.");

                CellRangeIterator {
                    cell_iterator: sheet.cells_in_range(&cell_range).into_iter(),
                }
            }
            SheetRange::ColumnRange((start, end)) => {
                let end_row = sheet
                    .columns()
                    .iter()
                    .fold(0, |acc, col| cmp::max(acc, col.cells_len()));

                let cell_range =
                    CellRange::new(0, end_row, *start, *end).expect("Should have valid range.");

                CellRangeIterator {
                    cell_iterator: sheet.cells_in_range(&cell_range).into_iter(),
                }
            }
        }
    }
}

impl<'a> Iterator for CellRangeIterator<'a> {
    type Item = &'a Value;

    fn next(&mut self) -> Option<Self::Item> {
        self.cell_iterator.next().map(|cell| cell.value())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sheeit_storage::location::{CellRange, Coordinate, SheetRange};
    use sheeit_storage::{CoreDocument, Value};

    #[test]
    fn test_cell_range_iterator() {
        let cells = vec![
            vec![Some("=1".to_string()), Some("hello".to_string()), None],
            vec![],
            vec![Some("=3".to_string()), None, Some("world".to_string())],
        ];

        let mut document = CoreDocument::new();
        document.add_sheet();
        document
            .insert_cell_facts(&Coordinate::new(0, 0, 0).unwrap(), cells.clone())
            .unwrap();

        let a1_b2 = &SheetRange::CellRange(CellRange::new(0, 2, 0, 2).expect("Fail"));
        let iterated: Vec<_> = a1_b2
            .into_value_iter_with(document.sheet_at(0).unwrap())
            .map(|val| val.clone())
            .collect();

        assert_eq!(
            iterated,
            vec![
                Value::Number(1.0),
                Value::String("hello".to_string().into_boxed_str()),
                Value::Number(3.0),
                Value::Blank,
                Value::String("world".to_string().into_boxed_str()),
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

        let a1_a3 = &SheetRange::CellRange(CellRange::new(0, 2, 0, 0).expect("Fail"));
        let iterated: Vec<_> = a1_a3
            .into_value_iter_with(document.sheet_at(0).unwrap())
            .map(|val| val.clone())
            .collect();

        assert_eq!(
            iterated,
            vec![
                Value::Number(1.0),
                Value::String("hello".to_string().into_boxed_str()),
                Value::String("hello2".to_string().into_boxed_str()),
            ]
        );
    }

    #[test]
    fn test_cell_range_iterator_more_complex_1() {
        let col_a = vec![
            // Ref A1
            Some("1".to_string()),
            // Left
            Some("1".to_string()),
            // Straddle
            Some("=SUM(A4:D4, 1, 1)".to_string()),
            Some("1".to_string()),
        ];

        let col_b = vec![];

        let col_c = vec![
            // On operation.
            Some("1".to_string()),
            None,
            None,
            Some("2".to_string()),
        ];

        let col_d = vec![
            // Right
            Some("3".to_string()),
            None,
            None,
            Some("3".to_string()),
        ];

        let cells = vec![col_a, col_b, col_c, col_d];

        let mut document = CoreDocument::new();
        document.add_sheet();
        document
            .insert_cell_facts(&Coordinate::new(0, 0, 0).unwrap(), cells.clone())
            .unwrap();

        let a4_d4 = &SheetRange::CellRange(CellRange::new(3, 3, 0, 3).expect("Fail"));
        let iterated: Vec<_> = a4_d4
            .into_value_iter_with(document.sheet_at(0).unwrap())
            .map(|val| val.clone())
            .collect();

        assert_eq!(
            iterated,
            vec![Value::Number(1.0), Value::Number(2.0), Value::Number(3.0),]
        );
    }
}
