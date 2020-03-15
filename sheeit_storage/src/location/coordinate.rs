use super::CellRangeViewIterator;
use crate::size_check::NonNegativeIsize;
use crate::{Cell, StorageErrorKind, Value};
use im::vector::Focus;
use im::Vector;
use sheeit_parser::raw_parser::{CellRef, Ref, RefType};
use sheeit_parser::transformer::TransformContext;
use std::fmt;
use std::fmt::{Error, Formatter};

/// A coordinate in a Sheet. The numbers are 0-indexed.
/// The numbers cannot exceed NonNegativeIsize::max_size()
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct SheetCoordinate {
    row: usize,
    col: usize,
}

/// A coordinate in a Document. sheet_index is 0-indexed.
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Coordinate {
    sheet_index: usize,
    coordinate: SheetCoordinate,
}

/// A coordinate range within a Sheet. Typically represents a rectangular area,
/// however, a CellRange that is a single point (same SheetCoordinates) is a valid CellRange.
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct CellRange {
    start: SheetCoordinate,
    end: SheetCoordinate,
}

/// A view into a CellRange.
/// Use this to efficiently iterate over the cells present in a CellRange.
// This is essentially a mini Sheet. Can we do something smart about this here?
pub struct CellRangeView<'a> {
    cell_range: CellRange,
    // col-major order, just like sheet. [col][row]
    values: Vector<Focus<'a, Cell>>,
}

/// Represents either a Row Range or a Column Range.
/// They are unbounded as they refer to the entire row/column
pub(crate) type UnboundedRange = (usize, usize);

/// Represents a range in the Sheet. It could be an unbounded row/column range, or a cell range.
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub enum SheetRange {
    RowRange(UnboundedRange),
    ColumnRange(UnboundedRange),
    CellRange(CellRange),
}

impl CellRange {
    /// Constructs a new CellRange, returns an Error if the values passed in:
    /// 1) Do not meet the requirements of being a rectangular area (or a single point)
    /// 2) Not a valid NonNegativeIsize
    pub fn new(
        start_row: usize,
        end_row: usize,
        start_col: usize,
        end_col: usize,
    ) -> Result<CellRange, StorageErrorKind> {
        let _ = start_row.ensure()?;
        let _ = end_row.ensure()?;
        let _ = start_col.ensure()?;
        let _ = end_col.ensure()?;

        if start_row > end_row || start_col > end_col {
            return Err(StorageErrorKind::InvalidParameter);
        }

        Ok(CellRange {
            start: SheetCoordinate {
                row: start_row,
                col: start_col,
            },
            end: SheetCoordinate {
                row: end_row,
                col: end_col,
            },
        })
    }

    /// Constructs a new CellRange from Coordinates.
    /// Returns an Error if the values passed in:
    /// 1) Do not meet the requirements of being a rectangular area (or a single point)
    /// 2) Not a valid NonNegativeIsize
    pub fn from_coords(
        start_coord: SheetCoordinate,
        end_coord: SheetCoordinate,
    ) -> Result<CellRange, StorageErrorKind> {
        CellRange::new(
            start_coord.row,
            end_coord.row,
            start_coord.col,
            end_coord.col,
        )
    }

    pub fn start(&self) -> &SheetCoordinate {
        &self.start
    }

    pub fn end(&self) -> &SheetCoordinate {
        &self.end
    }
}

impl SheetRange {
    /// Determines whether the coordinate passed in is within this SheetRange.
    pub fn is_in_range(&self, coord: &SheetCoordinate) -> bool {
        match self {
            SheetRange::CellRange(cell_range) => {
                cell_range.start.row <= coord.row
                    && cell_range.end.row >= coord.row
                    && cell_range.start.col <= coord.col
                    && cell_range.end.col >= coord.col
            }
            SheetRange::RowRange((start, end)) => *start <= coord.row && *end >= coord.row,
            SheetRange::ColumnRange((start, end)) => *start <= coord.col && *end >= coord.col,
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Blank
    }
}

impl Coordinate {
    // TODO: Move this to a From<Coordinate> implementation instead.
    pub fn to_transform_context(&self) -> Result<TransformContext, StorageErrorKind> {
        Ok(TransformContext {
            sheet_index: self.sheet_index.ensure()?,
            row_index: self.coordinate.row.ensure()?,
            col_index: self.coordinate.col.ensure()?,
        })
    }

    pub fn new_with_coord(
        sheet_index: usize,
        coordinate: SheetCoordinate,
    ) -> Result<Coordinate, StorageErrorKind> {
        let _ = sheet_index.ensure()?;

        Ok(Coordinate {
            sheet_index,
            coordinate,
        })
    }

    pub fn new(
        sheet_index: usize,
        row_index: usize,
        col_index: usize,
    ) -> Result<Coordinate, StorageErrorKind> {
        let _ = sheet_index.ensure()?;
        let coordinate = SheetCoordinate::new(row_index, col_index)?;

        Ok(Coordinate {
            sheet_index,
            coordinate,
        })
    }

    pub fn sheet(&self) -> usize {
        self.sheet_index
    }

    pub fn row(&self) -> usize {
        self.coordinate.row()
    }

    pub fn col(&self) -> usize {
        self.coordinate.col()
    }

    pub fn sheet_coord(&self) -> &SheetCoordinate {
        &self.coordinate
    }
}

impl SheetCoordinate {
    pub fn new(row_index: usize, col_index: usize) -> Result<SheetCoordinate, StorageErrorKind> {
        let _ = row_index.ensure()?;
        let _ = col_index.ensure()?;

        Ok(SheetCoordinate {
            row: row_index,
            col: col_index,
        })
    }

    pub fn row(&self) -> usize {
        self.row
    }

    pub fn col(&self) -> usize {
        self.col
    }
}

impl fmt::Display for SheetCoordinate {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "R{}C{}", self.row, self.col)
    }
}

impl<'a> CellRangeView<'a> {
    /// Constructs a new CellRangeView from a Vector of Focuses.
    /// The cell range passed in must be reflective of the values passed in. No checks are done currently to ensure that.
    pub fn new(values: Vector<Focus<'a, Cell>>, cell_range: CellRange) -> CellRangeView<'a> {
        CellRangeView { values, cell_range }
    }

    pub fn cell_range(&self) -> &CellRange {
        &self.cell_range
    }

    pub fn values(&mut self) -> &mut Vector<Focus<'a, Cell>> {
        &mut self.values
    }
}

impl<'a> IntoIterator for CellRangeView<'a> {
    type Item = &'a Cell;
    type IntoIter = CellRangeViewIterator<'a>;

    fn into_iter(mut self) -> Self::IntoIter {
        let current_focus = self.values.pop_front().unwrap();

        CellRangeViewIterator::new(self.values, current_focus.into_iter())
    }
}

impl From<&SheetCoordinate> for Ref {
    fn from(coord: &SheetCoordinate) -> Self {
        Ref::CellRef(CellRef::new(
            RefType::Absolute,
            coord.row as isize,
            RefType::Absolute,
            coord.col as isize,
        ))
    }
}

impl From<&Coordinate> for Ref {
    fn from(coord: &Coordinate) -> Self {
        Ref::from(&coord.coordinate)
    }
}
