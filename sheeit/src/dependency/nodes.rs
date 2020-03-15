use crate::storage::size_check::NonNegativeIsize;
use crate::storage::{CoreDocument, Sequence, StorageErrorKind};

use rstar::{RTreeObject, AABB};

use crate::storage::location::{
    CellRange, CellRangeIterator, Coordinate, IntoIteratorWith, SheetCoordinate, SheetRange,
};
use std::isize;
use uuid::Uuid;

// Ranges are INCLUSIVE
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum NodeType {
    CellRange(CellRange),
    RowRange(usize, usize),
    ColumnRange(usize, usize),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct NodeLocation {
    pub sheet_index: usize,
    pub node_type: NodeType,
}

// TODO: Consider using a constructor for all callers.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Node {
    pub id: Uuid,
    pub location: NodeLocation,
    pub dirtied_at: Sequence,
}

impl NodeLocation {
    pub fn from_coord(coord: &Coordinate) -> Result<NodeLocation, StorageErrorKind> {
        Ok(NodeLocation {
            sheet_index: coord.sheet(),
            node_type: NodeType::CellRange(CellRange::from_coords(
                coord.sheet_coord().clone(),
                coord.sheet_coord().clone(),
            )?),
        })
    }

    pub fn from_node_type(sheet_index: usize, node_type: NodeType) -> NodeLocation {
        NodeLocation {
            sheet_index,
            node_type,
        }
    }

    pub fn from_cell_range(sheet_index: usize, cell_range: CellRange) -> NodeLocation {
        NodeLocation {
            sheet_index,
            node_type: NodeType::CellRange(cell_range),
        }
    }

    pub fn from_row_range(sheet_index: usize, start: usize, end: usize) -> NodeLocation {
        NodeLocation {
            sheet_index,
            node_type: NodeType::RowRange(start, end),
        }
    }

    pub fn from_column_range(sheet_index: usize, start: usize, end: usize) -> NodeLocation {
        NodeLocation {
            sheet_index,
            node_type: NodeType::ColumnRange(start, end),
        }
    }

    pub fn is_in_range(&self, coord: &Coordinate) -> bool {
        if self.sheet_index != coord.sheet() {
            return false;
        }

        let sheet_range: SheetRange = self.node_type.clone().into();
        sheet_range.is_in_range(coord.sheet_coord())
    }
}

impl Into<SheetRange> for NodeType {
    fn into(self) -> SheetRange {
        match self {
            NodeType::CellRange(cell_range) => SheetRange::CellRange(cell_range),
            NodeType::RowRange(start, end) => SheetRange::RowRange((start, end)),
            NodeType::ColumnRange(start, end) => SheetRange::ColumnRange((start, end)),
        }
    }
}

impl From<SheetRange> for NodeType {
    fn from(sheet_range: SheetRange) -> Self {
        match sheet_range {
            SheetRange::CellRange(cell_range) => NodeType::CellRange(cell_range),
            SheetRange::RowRange((start, end)) => NodeType::RowRange(start, end),
            SheetRange::ColumnRange((start, end)) => NodeType::ColumnRange(start, end),
        }
    }
}

impl RTreeObject for NodeType {
    type Envelope = AABB<[isize; 2]>;

    fn envelope(&self) -> Self::Envelope {
        match &self {
            NodeType::CellRange(cell_range) => {
                // Casting here is safe because the internal data structures are guaranteed not to overflow the isize value.
                let start_row = cell_range.start().row() as isize;
                let start_col = cell_range.start().col() as isize;
                let end_row = cell_range.end().row() as isize;
                let end_col = cell_range.end().col() as isize;

                if start_row == end_row && start_col == end_col {
                    AABB::from_point([start_row, start_col])
                } else {
                    AABB::from_corners([start_row, start_col], [end_row, end_col])
                }
            }
            NodeType::RowRange(start, end) => {
                let start = start.ensure().expect("Invalid usize");
                let end = end.ensure().expect("Invalid usize");

                AABB::from_corners([start, 0], [end, isize::max_size()])
            }
            NodeType::ColumnRange(start, end) => {
                let start = start.ensure().expect("Invalid usize");
                let end = end.ensure().expect("Invalid usize");

                AABB::from_corners([0, start], [isize::max_size(), end])
            }
        }
    }
}

impl RTreeObject for NodeLocation {
    type Envelope = AABB<[isize; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.node_type.envelope()
    }
}

impl<'a> IntoIteratorWith<'a, CoreDocument> for &'a NodeLocation {
    type Item = SheetCoordinate;
    type IntoIter = CellRangeIterator<'a>;

    fn into_iter_with(self, document: &'a CoreDocument, _allow_unbounded: bool) -> Self::IntoIter {
        let sheet = document
            .sheet_at(self.sheet_index)
            .expect("Sheet index should be valid for NodeLocation");

        match &self.node_type {
            NodeType::CellRange(cell_range) => CellRangeIterator {
                sheet,
                cell_range: cell_range.clone(),
                current: cell_range.start().clone(),
            },
            NodeType::RowRange(start, end) => CellRangeIterator {
                sheet,
                cell_range: CellRange::new(*start, *end, 0, isize::max_size() as usize)
                    .expect("Should have valid range."),
                current: SheetCoordinate::new(*start, 0)
                    .expect("NodeType::RowRange should have a valid coordinate."),
            },
            NodeType::ColumnRange(start, end) => CellRangeIterator {
                sheet,
                cell_range: CellRange::new(0, isize::max_size() as usize, *start, *end)
                    .expect("Should have valid range."),
                current: SheetCoordinate::new(0, *start)
                    .expect("NodeType::ColumnRange should have a valid coordinate."),
            },
        }
    }
}
