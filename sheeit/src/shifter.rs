use crate::dependency::{NodeLocation, NodeType};
use crate::storage::location::{CellRange, Coordinate};

pub(crate) enum StructuralMutationOp {
    Insert,
    Delete,
}

pub(crate) enum StructuralMutationType {
    Row,
    Column,
}

pub(crate) fn shift_node(
    node: &NodeLocation,
    num_ops: usize,
    sheet_index: usize,
    ops_index: usize,
    mutation_op: &StructuralMutationOp,
    mutation_type: &StructuralMutationType,
) -> Option<NodeLocation> {
    if node.sheet_index != sheet_index {
        return Some(node.clone());
    }

    let sheet_index = node.sheet_index;

    match &node.node_type {
        NodeType::CellRange(cell_range) => match mutation_type {
            StructuralMutationType::Row => calculate_node_shift(
                ops_index,
                num_ops,
                cell_range.start().row(),
                cell_range.end().row(),
                mutation_op,
            )
            .map(|(new_start_row, new_end_row)| {
                let new_cell_range = CellRange::new(
                    new_start_row,
                    new_end_row,
                    cell_range.start().col(),
                    cell_range.end().col(),
                )
                .expect("Shifted CellRange should still be valid.");

                NodeLocation {
                    sheet_index,
                    node_type: NodeType::CellRange(new_cell_range),
                }
            }),
            StructuralMutationType::Column => calculate_node_shift(
                ops_index,
                num_ops,
                cell_range.start().col(),
                cell_range.end().col(),
                mutation_op,
            )
            .map(|(new_start_col, new_end_col)| {
                let new_cell_range = CellRange::new(
                    cell_range.start().row(),
                    cell_range.end().row(),
                    new_start_col,
                    new_end_col,
                )
                .expect("Shifted CellRange should still be valid.");

                NodeLocation {
                    sheet_index,
                    node_type: NodeType::CellRange(new_cell_range),
                }
            }),
        },
        NodeType::ColumnRange(start, end) => match mutation_type {
            StructuralMutationType::Row => Some(node.clone()),
            StructuralMutationType::Column => {
                calculate_node_shift(ops_index, num_ops, *start, *end, mutation_op).map(
                    |(new_start, new_end)| NodeLocation {
                        sheet_index,
                        node_type: NodeType::ColumnRange(new_start, new_end),
                    },
                )
            }
        },
        NodeType::RowRange(start, end) => match mutation_type {
            StructuralMutationType::Column => Some(node.clone()),
            StructuralMutationType::Row => {
                calculate_node_shift(ops_index, num_ops, *start, *end, mutation_op).map(
                    |(new_start, new_end)| NodeLocation {
                        sheet_index,
                        node_type: NodeType::RowRange(new_start, new_end),
                    },
                )
            }
        },
    }
}

pub(crate) fn shift_coordinate(
    coord: &Coordinate,
    num_ops: usize,
    sheet_index: usize,
    ops_index: usize,
    mutation_op: &StructuralMutationOp,
    mutation_type: &StructuralMutationType,
) -> Option<Coordinate> {
    if sheet_index != coord.sheet() {
        return Some(coord.clone());
    }

    let coord_index = match mutation_type {
        StructuralMutationType::Row => coord.row(),
        StructuralMutationType::Column => coord.col(),
    };

    calculate_node_shift(ops_index, num_ops, coord_index, coord_index, mutation_op).map(
        |(start, end)| {
            assert_eq!(start, end);
            match mutation_type {
                StructuralMutationType::Row => Coordinate::new(coord.sheet(), start, coord.col())
                    .expect("Shifted coord should be valid. TODO: Verify this."),
                StructuralMutationType::Column => {
                    Coordinate::new(coord.sheet(), coord.row(), start)
                        .expect("Shifted coord should be valid. TODO: Verify this.")
                }
            }
        },
    )
}

fn calculate_node_shift(
    ops_index: usize,
    num_ops: usize,
    start_node_index: usize,
    end_node_index: usize,
    mutation_op: &StructuralMutationOp,
) -> Option<(usize, usize)> {
    match mutation_op {
        StructuralMutationOp::Insert => {
            // If the insert is *inside* the range of the node, expand it.
            if ops_index > start_node_index && ops_index <= end_node_index {
                return Some((start_node_index, end_node_index + num_ops));
            }

            // If the operation is to the left/above, move it right/below.
            if ops_index <= start_node_index {
                return Some((start_node_index + num_ops, end_node_index + num_ops));
            }

            // Else, the operation doesn't affect this node.
            Some((start_node_index, end_node_index))
        }
        StructuralMutationOp::Delete => {
            let delete_end = ops_index + ops_index - 1;

            // If the delete is entirely within the node's range, delete the whole node.
            if ops_index <= start_node_index && delete_end >= end_node_index {
                return None;
            }

            // If the entire delete is a subset of the node range, shrink the node. No need to move.
            if ops_index >= start_node_index && delete_end <= end_node_index {
                return Some((start_node_index, end_node_index - num_ops));
            }

            // If the delete affects a subset of the node's range, move left/above and trim it.
            if delete_end >= start_node_index && delete_end < end_node_index {
                return Some((ops_index, end_node_index - num_ops));
            }

            // If the delete is to the left/above, move entire range left/above.
            if delete_end < start_node_index {
                return Some((start_node_index - ops_index, end_node_index - num_ops));
            }

            Some((start_node_index, end_node_index))
        }
    }
}
