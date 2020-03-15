use rayon::prelude::*;
use sheeit_parser::raw_parser::{ExprErr, Expression, Ref, RefType};

struct ShiftContext {
    expr_row_or_col: isize,
    start_op_index: isize,
    ops_num: isize,
    op_type: OpType,
    shift_type: ShiftType,
}

impl ShiftContext {
    pub fn new(
        expr_row: isize,
        start_row_index: isize,
        rows: isize,
        op_type: OpType,
        shift_type: ShiftType,
    ) -> ShiftContext {
        ShiftContext {
            expr_row_or_col: expr_row,
            start_op_index: start_row_index,
            ops_num: rows,
            op_type,
            shift_type,
        }
    }
}

enum OpType {
    Insert,
    Delete,
}

enum ShiftType {
    Row,
    Column,
}

struct RefError;

fn is_in_insertion_range(shift_context: &ShiftContext) -> bool {
    let ShiftContext {
        expr_row_or_col,
        start_op_index: insertion_index,
        ops_num: num_of_inserts,
        ..
    } = shift_context;

    *expr_row_or_col >= *insertion_index && *expr_row_or_col < (*insertion_index + *num_of_inserts)
}

fn shift_ref_val_relative_on_insert(shift_context: &ShiftContext, ref_val: &mut isize) {
    let ShiftContext {
        expr_row_or_col,
        start_op_index: insertion_index,
        ops_num: num_of_inserts,
        ..
    } = shift_context;

    if is_in_insertion_range(shift_context) {
        return;
    }

    let is_above_or_left = *expr_row_or_col <= *insertion_index;

    // Current expression row is above insertion point/expression col is left of insertion point, and
    // refers to anything at or after insertion point:
    if is_above_or_left && *expr_row_or_col + *ref_val >= *insertion_index {
        *ref_val += *num_of_inserts;
    }

    // Current expression row is below/expression column is left of insertion point, and
    // refers to row above insertion point.
    let expr_row_before_insert = *expr_row_or_col - *num_of_inserts; // Need to calculate what was the expression row/col before insertion happened.
    if !is_above_or_left && expr_row_before_insert + *ref_val < *insertion_index {
        *ref_val -= *num_of_inserts;
    }
}

fn shift_ref_val_absolute_on_insert(shift_context: &ShiftContext, ref_val: &mut isize) {
    let ShiftContext {
        start_op_index,
        ops_num: num_of_inserts,
        ..
    } = shift_context;

    if is_in_insertion_range(shift_context) {
        return;
    }

    if *ref_val >= *start_op_index {
        *ref_val += *num_of_inserts;
    }
}

fn shift_ref_val_relative_on_delete(
    shift_context: &ShiftContext,
    ref_val: &mut isize,
) -> Result<(), RefError> {
    let ShiftContext {
        expr_row_or_col,
        start_op_index: start_delete_index,
        ops_num: num_of_deletes,
        ..
    } = shift_context;

    let is_above_or_left = expr_row_or_col < start_delete_index;
    let start_delete_index = *start_delete_index;

    let referred_index = if is_above_or_left {
        *expr_row_or_col + *ref_val
    } else {
        (*expr_row_or_col + *num_of_deletes) + *ref_val
    };

    if referred_index >= start_delete_index && referred_index < start_delete_index + *num_of_deletes
    {
        return Err(RefError);
    }

    if is_above_or_left && referred_index >= start_delete_index {
        *ref_val -= *num_of_deletes;
    }

    if !is_above_or_left && referred_index < start_delete_index {
        *ref_val += *num_of_deletes;
    }

    Ok(())
}

fn shift_ref_val_absolute_on_delete(
    shift_context: &ShiftContext,
    ref_val: &mut isize,
) -> Result<(), RefError> {
    let ShiftContext {
        start_op_index: start_delete_index,
        ops_num: num_of_deletes,
        ..
    } = shift_context;

    let referred_index = *ref_val;

    if referred_index >= *start_delete_index && referred_index < start_delete_index + num_of_deletes
    {
        return Err(RefError);
    }

    if *ref_val > *start_delete_index {
        *ref_val -= *num_of_deletes;
    }

    Ok(())
}

fn call_shift_op(
    shift_context: &ShiftContext,
    pos_type: &RefType,
    ref_val: &mut isize,
) -> Result<(), RefError> {
    let ShiftContext { op_type, .. } = shift_context;

    match op_type {
        OpType::Insert => match *pos_type {
            RefType::Absolute => {
                shift_ref_val_absolute_on_insert(shift_context, ref_val);
            }
            RefType::Relative => {
                shift_ref_val_relative_on_insert(shift_context, ref_val);
            }
        },
        OpType::Delete => match *pos_type {
            RefType::Absolute => {
                shift_ref_val_absolute_on_delete(shift_context, ref_val)?;
            }
            RefType::Relative => {
                shift_ref_val_relative_on_delete(shift_context, ref_val)?;
            }
        },
    };

    Ok(())
}

pub fn shift_expression_col_on_insert(
    expr: &mut Expression,
    expr_col: isize,
    start_col_index: isize,
    cols: isize,
) {
    let shift_context = ShiftContext::new(
        expr_col,
        start_col_index,
        cols,
        OpType::Insert,
        ShiftType::Column,
    );

    shift_expression(expr, &shift_context);
}

pub fn shift_expression_col_on_delete(
    expr: &mut Expression,
    expr_col: isize,
    start_col_index: isize,
    cols: isize,
) {
    let shift_context = ShiftContext::new(
        expr_col,
        start_col_index,
        cols,
        OpType::Delete,
        ShiftType::Column,
    );

    shift_expression(expr, &shift_context);
}

pub fn shift_expression_row_on_insert(
    expr: &mut Expression,
    expr_row: isize,
    start_row_index: isize,
    rows: isize,
) {
    let shift_context = ShiftContext::new(
        expr_row,
        start_row_index,
        rows,
        OpType::Insert,
        ShiftType::Row,
    );

    shift_expression(expr, &shift_context);
}

pub fn shift_expression_row_on_delete(
    expr: &mut Expression,
    expr_row: isize,
    start_row_index: isize,
    rows: isize,
) {
    let shift_context = ShiftContext::new(
        expr_row,
        start_row_index,
        rows,
        OpType::Delete,
        ShiftType::Row,
    );

    shift_expression(expr, &shift_context);
}
// We might want to consider using impl Expression here, but let's hold off until we now how our
// dependency graph will look like.
fn shift_expression(expr: &mut Expression, shift_context: &ShiftContext) {
    match *expr {
        Expression::Err(_)
        | Expression::ValueNum(_)
        | Expression::ValueString(_)
        | Expression::ValueBool(_) => { /* No mutations needed */ }
        Expression::RefA1(_) => {
            unreachable!("RefA1 should already be converted to Ref by this point")
        }
        Expression::Ref(ref mut ref_expr) => {
            match shift_expression_ref(ref_expr, shift_context) {
                Ok(_) => { /* Mutations complete, do nothing */ }
                Err(_e) => {
                    // expr should NOT be mutated by the shift at this point.
                    // We can guarantee that because we never passed the Expression to the function,
                    // mutating the Expression::Ref is safe because we're mutating the entire Expression anyway
                    *expr = Expression::Err(ExprErr::RefErr);
                }
            }
        }
        Expression::Parens(ref mut expr)
        | Expression::Percent(ref mut expr)
        | Expression::Negate(ref mut expr) => shift_expression(expr, shift_context),
        Expression::Add(ref mut expr1, ref mut expr2)
        | Expression::Subtract(ref mut expr1, ref mut expr2)
        | Expression::Multiply(ref mut expr1, ref mut expr2)
        | Expression::Divide(ref mut expr1, ref mut expr2)
        | Expression::Exponentiate(ref mut expr1, ref mut expr2)
        | Expression::Eq(ref mut expr1, ref mut expr2)
        | Expression::NotEq(ref mut expr1, ref mut expr2)
        | Expression::Gt(ref mut expr1, ref mut expr2)
        | Expression::Gte(ref mut expr1, ref mut expr2)
        | Expression::Lt(ref mut expr1, ref mut expr2)
        | Expression::Lte(ref mut expr1, ref mut expr2)
        | Expression::Concat(ref mut expr1, ref mut expr2) => {
            shift_expression(expr1, shift_context);
            shift_expression(expr2, shift_context);
        }
        Expression::Fn(ref mut _fn_str, ref mut exprs) => {
            exprs.par_iter_mut().for_each(|args_expr| {
                shift_expression(args_expr, shift_context);
            })
        }
    };
}

fn shift_expression_ref(ref_expr: &mut Ref, shift_context: &ShiftContext) -> Result<(), RefError> {
    match *ref_expr {
        Ref::RangeRef(ref mut ref1, ref mut ref2)
        | Ref::UnionRef(ref mut ref1, ref mut ref2)
        | Ref::IntersectRef(ref mut ref1, ref mut ref2) => {
            shift_expression_ref(ref1, shift_context)?;
            shift_expression_ref(ref2, shift_context)?;
        }
        Ref::RowRangeRef((ref ref_type1, ref mut ref_val1), (ref ref_type2, ref mut ref_val2)) => {
            match shift_context.shift_type {
                ShiftType::Row => {
                    call_shift_op(shift_context, ref_type1, ref_val1)?;
                    call_shift_op(shift_context, ref_type2, ref_val2)?;
                }
                ShiftType::Column => { /* Do nothing */ }
            }
        }
        Ref::CellRef(ref mut cell_ref) => {
            let (ref_type, val_to_shift) = match shift_context.shift_type {
                ShiftType::Row => (cell_ref.row_reference_type().clone(), cell_ref.row_mut()),
                ShiftType::Column => (cell_ref.col_reference_type().clone(), cell_ref.col_mut()),
            };

            call_shift_op(shift_context, &ref_type, val_to_shift)?
        }
        Ref::ColumnRangeRef(
            (ref ref_type1, ref mut ref_val1),
            (ref ref_type2, ref mut ref_val2),
        ) => {
            match shift_context.shift_type {
                ShiftType::Column => {
                    call_shift_op(shift_context, ref_type1, ref_val1)?;
                    call_shift_op(shift_context, ref_type2, ref_val2)?;
                }
                ShiftType::Row => { /* Do nothing */ }
            }
        }
    }

    Ok(())
}
