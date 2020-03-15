use super::util;
use super::util::Range;
use super::{CellRange, SheetCoordinate, SheetRange, UnboundedRange};
use crate::size_check::NonNegativeIsize;
use crate::StorageErrorKind;
use sheeit_parser::raw_parser::{CellRef, Ref, RefType};
use std::cmp;
use std::collections::HashSet;

/// A trait where T is some kind of a coordinate-backed data structure.
/// The implementers is some kind of a reference data structure.
/// Given a coordinate, tell where the reference is pointing to.
pub trait RefersToLocation<T> {
    fn refers_to(&self, from: &SheetCoordinate) -> Result<T, StorageErrorKind>;
}

// row, col, cell_range
type DistinctRanges = (
    Option<UnboundedRange>,
    Option<UnboundedRange>,
    Option<CellRange>,
);

// TODO: Add unit tests for the functions in this file.
// TODO: To handle sheet references, we need a RefersToLocation<Coordinate> -> Coordinate
impl RefersToLocation<SheetCoordinate> for CellRef {
    fn refers_to(&self, from: &SheetCoordinate) -> Result<SheetCoordinate, StorageErrorKind> {
        let to_index = |ref_type: &RefType, ref_loc: isize, from_loc| match ref_type {
            RefType::Absolute => Ok(ref_loc as usize),
            RefType::Relative => {
                let result = (from_loc as isize + ref_loc).ensure()?;
                Ok(result as usize)
            }
        };

        let row = to_index(self.row_reference_type(), self.row(), from.row())?;
        let col = to_index(self.col_reference_type(), self.col(), from.col())?;

        SheetCoordinate::new(row, col)
    }
}

impl RefersToLocation<CellRange> for CellRef {
    fn refers_to(&self, from: &SheetCoordinate) -> Result<CellRange, StorageErrorKind> {
        let coord: SheetCoordinate = self.refers_to(from)?;

        Ok(CellRange::from_coords(coord.clone(), coord)
            .expect("Cell Range Should be Valid with valid coordinates"))
    }
}

impl RefersToLocation<HashSet<SheetRange>> for Ref {
    fn refers_to(&self, coord: &SheetCoordinate) -> Result<HashSet<SheetRange>, StorageErrorKind> {
        let mut result = HashSet::new();
        let (row_range_node, col_range_node, cell_range_node) =
            points_to_distinct_ranges(self, coord)?;

        match row_range_node {
            None => {}
            Some(range) => {
                result.insert(SheetRange::RowRange(range));
            }
        }
        match col_range_node {
            None => {}
            Some(range) => {
                result.insert(SheetRange::ColumnRange(range));
            }
        }
        match cell_range_node {
            None => {}
            Some(cell_range) => {
                result.insert(SheetRange::CellRange(cell_range));
            }
        }

        Ok(result)
    }
}

fn points_to_distinct_ranges(
    ref_expr: &Ref,
    coord: &SheetCoordinate,
) -> Result<DistinctRanges, StorageErrorKind> {
    let result = match ref_expr {
        Ref::CellRef(cell_ref) => (None, None, Some(cell_ref.refers_to(coord)?)),
        // TODO: Handle intersection differently.
        Ref::RangeRef(ref1, ref2) | Ref::UnionRef(ref1, ref2) | Ref::IntersectRef(ref1, ref2) => {
            let (row_range_1, col_range_1, cell_range_1) = points_to_distinct_ranges(ref1, coord)?;
            let (row_range_2, col_range_2, cell_range_2) = points_to_distinct_ranges(ref2, coord)?;

            let new_row_range = util::merge_range(row_range_1, row_range_2);
            let new_col_range = util::merge_range(col_range_1, col_range_2);

            let new_cell_range = match (cell_range_1, cell_range_2) {
                (None, None) => None,
                (Some(cell_range), None) | (None, Some(cell_range)) => Some(cell_range),
                (Some(cell_range_1), Some(cell_range_2)) => {
                    Some(cell_range_1.merge_range(&cell_range_2))
                }
            };

            (new_row_range, new_col_range, new_cell_range)
        }
        Ref::RowRangeRef((ref_type_1, row_num_1), (ref_type_2, row_num_2)) => {
            let row_num_1 = match ref_type_1 {
                RefType::Absolute => *row_num_1,
                RefType::Relative => coord.row() as isize + *row_num_1,
            };

            let row_num_2 = match ref_type_2 {
                RefType::Absolute => *row_num_2,
                RefType::Relative => coord.row() as isize + *row_num_2,
            };

            let min = cmp::min(row_num_1, row_num_2);
            let max = cmp::max(row_num_1, row_num_2);

            (Some((min as usize, max as usize)), None, None)
        }
        Ref::ColumnRangeRef((ref_type_1, col_num_1), (ref_type_2, col_num_2)) => {
            let col_num_1 = match ref_type_1 {
                RefType::Absolute => *col_num_1,
                RefType::Relative => coord.col() as isize + *col_num_1,
            };

            let col_num_2 = match ref_type_2 {
                RefType::Absolute => *col_num_2,
                RefType::Relative => coord.col() as isize + *col_num_2,
            };

            let min = cmp::min(col_num_1, col_num_2);
            let max = cmp::max(col_num_1, col_num_2);

            (None, Some((min as usize, max as usize)), None)
        }
    };

    Ok(result)
}
