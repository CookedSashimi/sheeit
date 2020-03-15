//! This module performs mutations to Expressions to change all of the Ref to RefA1, and vice versa.

use crate::raw_parser::{CellRef, CellRefA1, Expression, Ref, RefA1, RefType};
use std::convert::TryFrom;

#[derive(Clone, Debug)]
pub struct TransformContext {
    pub sheet_index: isize,
    pub row_index: isize,
    pub col_index: isize,
}

#[derive(Clone, Debug)]
pub struct TransformError {
    message: String,
    context: TransformContext,
    source_ref_a1: Option<RefA1>,
    source_ref: Option<Ref>,
}

/// Transforms any RefA1 in an Expression to a Ref.
/// Collects transformation errors as we go instead of immediately returning, since we will mutate the Expression passed in.
pub fn transform_to_expr_ref(
    expression: &mut Expression,
    context: &TransformContext,
) -> Result<(), Vec<TransformError>> {
    match expression {
        Expression::RefA1(ref_a1) => {
            let new_ref = transform_to_ref(ref_a1, context)?;

            *expression = Expression::Ref(new_ref);
            Ok(())
        }
        Expression::Percent(expr) | Expression::Negate(expr) | Expression::Parens(expr) => {
            transform_to_expr_ref(expr, context)
        }
        Expression::Add(first, second)
        | Expression::Subtract(first, second)
        | Expression::Multiply(first, second)
        | Expression::Divide(first, second)
        | Expression::Exponentiate(first, second)
        | Expression::Eq(first, second)
        | Expression::Gt(first, second)
        | Expression::Lt(first, second)
        | Expression::Gte(first, second)
        | Expression::Lte(first, second)
        | Expression::Concat(first, second)
        | Expression::NotEq(first, second) => {
            let mut errors = vec![];

            match transform_to_expr_ref(first, context).err() {
                None => {}
                Some(errs) => errors.extend(errs),
            };

            match transform_to_expr_ref(second, context).err() {
                None => {}
                Some(errs) => errors.extend(errs),
            };

            match errors.len() {
                0 => Ok(()),
                _ => Err(errors),
            }
        }

        Expression::Fn(_name, args) => {
            let errors = args.iter_mut().fold(vec![], |mut acc, expr| {
                match transform_to_expr_ref(expr, context).err() {
                    None => {}
                    Some(errs) => acc.extend(errs),
                };

                acc
            });

            match errors.len() {
                0 => Ok(()),
                _ => Err(errors),
            }
        }
        Expression::Err(_)
        | Expression::ValueBool(_)
        | Expression::ValueNum(_)
        | Expression::ValueString(_)
        | Expression::Ref(_) => Ok(()),
    }
}

/// Transforms any Ref in an Expression to a RefA1.
/// Collects transformation errors as we go instead of immediately returning, since we will mutate the Expression passed in.
pub fn transform_to_expr_refa1(
    expression: &mut Expression,
    context: &TransformContext,
) -> Result<(), Vec<TransformError>> {
    match expression {
        Expression::Ref(expr_ref) => {
            let new_ref = transform_to_refa1(expr_ref, context)?;

            *expression = Expression::RefA1(new_ref);
            Ok(())
        }
        Expression::Percent(expr) | Expression::Negate(expr) | Expression::Parens(expr) => {
            transform_to_expr_refa1(expr, context)
        }
        Expression::Add(first, second)
        | Expression::Subtract(first, second)
        | Expression::Multiply(first, second)
        | Expression::Divide(first, second)
        | Expression::Exponentiate(first, second)
        | Expression::Eq(first, second)
        | Expression::Gt(first, second)
        | Expression::Lt(first, second)
        | Expression::Gte(first, second)
        | Expression::Lte(first, second)
        | Expression::Concat(first, second)
        | Expression::NotEq(first, second) => {
            let mut errors = vec![];

            match transform_to_expr_refa1(first, context).err() {
                None => {}
                Some(errs) => errors.extend(errs),
            };

            match transform_to_expr_refa1(second, context).err() {
                None => {}
                Some(errs) => errors.extend(errs),
            };

            match errors.len() {
                0 => Ok(()),
                _ => Err(errors),
            }
        }

        Expression::Fn(_name, args) => {
            let errors = args.iter_mut().fold(vec![], |mut acc, expr| {
                match transform_to_expr_refa1(expr, context).err() {
                    None => {}
                    Some(errs) => acc.extend(errs),
                };

                acc
            });

            match errors.len() {
                0 => Ok(()),
                _ => Err(errors),
            }
        }
        Expression::Err(_)
        | Expression::ValueBool(_)
        | Expression::ValueNum(_)
        | Expression::ValueString(_)
        | Expression::RefA1(_) => Ok(()),
    }
}

/// Transforms any RefA1 to a Ref.
/// Collects transformation errors as we go instead of immediately returning, since we will mutate the Expression passed in.
pub fn transform_to_ref(
    ref_a1: &RefA1,
    context: &TransformContext,
) -> Result<Ref, Vec<TransformError>> {
    match ref_a1 {
        RefA1::CellRef(cell_ref) => {
            // Can't use map_err due to borrow checker not being happy with borrowing ref_a1 in closure.
            let row =
                match row_num_from_refa1(cell_ref.row(), &cell_ref.row_reference_type(), context) {
                    Ok(row) => row,
                    Err(err) => {
                        return Err(vec![TransformError {
                            message: err,
                            source_ref_a1: Some(ref_a1.clone()),
                            source_ref: None,
                            context: context.clone(),
                        }]);
                    }
                };
            let col =
                match col_num_from_refa1(cell_ref.col(), cell_ref.col_reference_type(), context) {
                    Ok(row) => row,
                    Err(err) => {
                        return Err(vec![TransformError {
                            message: err,
                            source_ref_a1: Some(ref_a1.clone()),
                            source_ref: None,
                            context: context.clone(),
                        }]);
                    }
                };

            Ok(Ref::CellRef(CellRef::new(
                cell_ref.row_reference_type().clone(),
                row,
                cell_ref.col_reference_type().clone(),
                col,
            )))
        }
        RefA1::ColumnRangeRef(
            (first_ref_type, first_col_string),
            (second_ref_type, second_col_string),
        ) => {
            let first_col_num_transform =
                match col_num_from_refa1(*first_col_string, &first_ref_type, context) {
                    Ok(row) => row,
                    Err(err) => {
                        return Err(vec![TransformError {
                            message: err,
                            source_ref_a1: Some(ref_a1.clone()),
                            source_ref: None,
                            context: context.clone(),
                        }]);
                    }
                };
            let second_col_num_transform =
                match col_num_from_refa1(*second_col_string, &second_ref_type, context) {
                    Ok(row) => row,
                    Err(err) => {
                        return Err(vec![TransformError {
                            message: err,
                            source_ref_a1: Some(ref_a1.clone()),
                            source_ref: None,
                            context: context.clone(),
                        }]);
                    }
                };

            Ok(Ref::ColumnRangeRef(
                (first_ref_type.clone(), first_col_num_transform),
                (second_ref_type.clone(), second_col_num_transform),
            ))
        }
        RefA1::RowRangeRef(
            (first_ref_type, first_row_num_a1),
            (second_ref_type, second_row_num_a1),
        ) => {
            let first_row_num_transform =
                match row_num_from_refa1(*first_row_num_a1, &first_ref_type, context) {
                    Ok(row) => row,
                    Err(err) => {
                        return Err(vec![TransformError {
                            message: err,
                            source_ref_a1: Some(ref_a1.clone()),
                            source_ref: None,
                            context: context.clone(),
                        }]);
                    }
                };
            let second_row_num_transform =
                match row_num_from_refa1(*second_row_num_a1, &second_ref_type, context) {
                    Ok(row) => row,
                    Err(err) => {
                        return Err(vec![TransformError {
                            message: err,
                            source_ref_a1: Some(ref_a1.clone()),
                            source_ref: None,
                            context: context.clone(),
                        }]);
                    }
                };

            Ok(Ref::RowRangeRef(
                (first_ref_type.clone(), first_row_num_transform),
                (second_ref_type.clone(), second_row_num_transform),
            ))
        }
        RefA1::IntersectRef(ref1, ref2) => {
            let ref1_transformed = transform_to_ref(ref1, context)?;
            let ref2_tranformed = transform_to_ref(ref2, context)?;

            Ok(Ref::IntersectRef(
                Box::new(ref1_transformed),
                Box::new(ref2_tranformed),
            ))
        }
        RefA1::UnionRef(ref1, ref2) => {
            let ref1_transformed = transform_to_ref(ref1, context)?;
            let ref2_tranformed = transform_to_ref(ref2, context)?;

            Ok(Ref::UnionRef(
                Box::new(ref1_transformed),
                Box::new(ref2_tranformed),
            ))
        }
        RefA1::RangeRef(ref1, ref2) => {
            let ref1_transformed = transform_to_ref(ref1, context)?;
            let ref2_tranformed = transform_to_ref(ref2, context)?;

            Ok(Ref::RangeRef(
                Box::new(ref1_transformed),
                Box::new(ref2_tranformed),
            ))
        }
    }
}

/// Transforms any Ref to a RefA1.
/// Collects transformation errors as we go instead of immediately returning, since we will mutate the Expression passed in.
pub fn transform_to_refa1(
    expr_ref: &Ref,
    context: &TransformContext,
) -> Result<RefA1, Vec<TransformError>> {
    match expr_ref {
        Ref::CellRef(cell_ref) => {
            // Can't use map_err due to borrow checker not being happy with borrowing expr_ref in closure.
            // This explains the duplicate code as well, not sure how to solve this.
            let row =
                match row_num_from_expr_ref(cell_ref.row(), cell_ref.row_reference_type(), context)
                {
                    Ok(row) => row,
                    Err(err) => {
                        return Err(vec![TransformError {
                            message: err,
                            source_ref: Some(expr_ref.clone()),
                            source_ref_a1: None,
                            context: context.clone(),
                        }]);
                    }
                };
            let col =
                match col_num_from_expr_ref(cell_ref.col(), cell_ref.col_reference_type(), context)
                {
                    Ok(row) => row,
                    Err(err) => {
                        return Err(vec![TransformError {
                            message: err,
                            source_ref: Some(expr_ref.clone()),
                            source_ref_a1: None,
                            context: context.clone(),
                        }]);
                    }
                };

            Ok(RefA1::CellRef(CellRefA1::new_with_col_num(
                cell_ref.row_reference_type().clone(),
                row,
                cell_ref.col_reference_type().clone(),
                col,
            )))
        }
        Ref::ColumnRangeRef(
            (first_ref_type, first_col_string),
            (second_ref_type, second_col_string),
        ) => {
            let first_col_num_transform =
                match col_num_from_expr_ref(*first_col_string, &first_ref_type, context) {
                    Ok(row) => row,
                    Err(err) => {
                        return Err(vec![TransformError {
                            message: err,
                            source_ref: Some(expr_ref.clone()),
                            source_ref_a1: None,
                            context: context.clone(),
                        }]);
                    }
                };
            let second_col_num_transform =
                match col_num_from_expr_ref(*second_col_string, &second_ref_type, context) {
                    Ok(row) => row,
                    Err(err) => {
                        return Err(vec![TransformError {
                            message: err,
                            source_ref: Some(expr_ref.clone()),
                            source_ref_a1: None,
                            context: context.clone(),
                        }]);
                    }
                };

            Ok(RefA1::ColumnRangeRef(
                (first_ref_type.clone(), first_col_num_transform),
                (second_ref_type.clone(), second_col_num_transform),
            ))
        }
        Ref::RowRangeRef(
            (first_ref_type, first_row_num_a1),
            (second_ref_type, second_row_num_a1),
        ) => {
            let first_row_num_transform =
                match row_num_from_expr_ref(*first_row_num_a1, &first_ref_type, context) {
                    Ok(row) => row,
                    Err(err) => {
                        return Err(vec![TransformError {
                            message: err,
                            source_ref: Some(expr_ref.clone()),
                            source_ref_a1: None,
                            context: context.clone(),
                        }]);
                    }
                };
            let second_row_num_transform =
                match row_num_from_expr_ref(*second_row_num_a1, &second_ref_type, context) {
                    Ok(row) => row,
                    Err(err) => {
                        return Err(vec![TransformError {
                            message: err,
                            source_ref: Some(expr_ref.clone()),
                            source_ref_a1: None,
                            context: context.clone(),
                        }]);
                    }
                };

            Ok(RefA1::RowRangeRef(
                (first_ref_type.clone(), first_row_num_transform),
                (second_ref_type.clone(), second_row_num_transform),
            ))
        }
        Ref::IntersectRef(ref1, ref2) => {
            let ref1_transformed = transform_to_refa1(ref1, context)?;
            let ref2_tranformed = transform_to_refa1(ref2, context)?;

            Ok(RefA1::IntersectRef(
                Box::new(ref1_transformed),
                Box::new(ref2_tranformed),
            ))
        }
        Ref::UnionRef(ref1, ref2) => {
            let ref1_transformed = transform_to_refa1(ref1, context)?;
            let ref2_tranformed = transform_to_refa1(ref2, context)?;

            Ok(RefA1::UnionRef(
                Box::new(ref1_transformed),
                Box::new(ref2_tranformed),
            ))
        }
        Ref::RangeRef(ref1, ref2) => {
            let ref1_transformed = transform_to_refa1(ref1, context)?;
            let ref2_tranformed = transform_to_refa1(ref2, context)?;

            Ok(RefA1::RangeRef(
                Box::new(ref1_transformed),
                Box::new(ref2_tranformed),
            ))
        }
    }
}

fn row_num_from_refa1(
    cell_ref_row: isize,
    cell_ref_type: &RefType,
    context: &TransformContext,
) -> Result<isize, String> {
    let cell_ref_row =
        isize::try_from(cell_ref_row).map_err(|_| String::from("Row is too large"))?;
    let context_row =
        isize::try_from(context.row_index).map_err(|_| String::from("Row is too large"))?;
    let row_index = cell_ref_row - 1;

    Ok(match cell_ref_type {
        RefType::Absolute => row_index,
        RefType::Relative => row_index - context_row,
    })
}

fn col_num_from_refa1(
    cell_ref_col: isize,
    cell_ref_type: &RefType,
    context: &TransformContext,
) -> Result<isize, String> {
    let cell_ref_col =
        isize::try_from(cell_ref_col).map_err(|_| String::from("Column is too large"))?;
    let context_col =
        isize::try_from(context.col_index).map_err(|_| String::from("Column is too large"))?;
    let col_index = cell_ref_col - 1;

    Ok(match cell_ref_type {
        RefType::Absolute => col_index,
        RefType::Relative => col_index - context_col,
    })
}

fn row_num_from_expr_ref(
    cell_ref_row: isize,
    cell_ref_type: &RefType,
    context: &TransformContext,
) -> Result<isize, String> {
    let context_row =
        isize::try_from(context.row_index).map_err(|_| String::from("Row is too large"))?;

    Ok(match cell_ref_type {
        RefType::Absolute => cell_ref_row + 1,
        RefType::Relative => context_row + cell_ref_row + 1,
    })
}

fn col_num_from_expr_ref(
    cell_ref_col: isize,
    cell_ref_type: &RefType,
    context: &TransformContext,
) -> Result<isize, String> {
    let context_col =
        isize::try_from(context.col_index).map_err(|_| String::from("Column is too large"))?;

    Ok(match cell_ref_type {
        RefType::Absolute => cell_ref_col + 1,
        RefType::Relative => context_col + cell_ref_col + 1,
    })
}

impl TransformContext {
    pub fn new(sheet_index: isize, row_number: isize, col_number: isize) -> TransformContext {
        TransformContext {
            sheet_index,
            row_index: row_number,
            col_index: col_number,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw_parser;
    use crate::raw_parser::{CellRef, Expression, Ref, RefType};

    #[test]
    fn test_transformation_positive() {
        let mut expr_a1 = raw_parser::parse_cell_content("=A1:$D$4 + $B2:C$3").unwrap();

        transform_to_expr_ref(&mut expr_a1, &TransformContext::new(0, 0, 0)).unwrap();

        assert_eq!(
            expr_a1,
            Expression::Add(
                Box::new(Expression::Ref(Ref::RangeRef(
                    Box::new(Ref::CellRef(CellRef::RRRC(0, 0))),
                    Box::new(Ref::CellRef(CellRef::ARAC(3, 3)))
                ))),
                Box::new(Expression::Ref(Ref::RangeRef(
                    Box::new(Ref::CellRef(CellRef::RRAC(1, 1))),
                    Box::new(Ref::CellRef(CellRef::ARRC(2, 2)))
                )))
            )
        );
    }

    #[test]
    fn test_transformation_negative() {
        let mut expr_a1 = raw_parser::parse_cell_content("=1:$4 + $A:C").unwrap();

        transform_to_expr_ref(&mut expr_a1, &TransformContext::new(0, 4, 4)).unwrap();

        assert_eq!(
            expr_a1,
            Expression::Add(
                Box::new(Expression::Ref(Ref::RowRangeRef(
                    (RefType::Relative, -4),
                    (RefType::Absolute, 3)
                ))),
                Box::new(Expression::Ref(Ref::ColumnRangeRef(
                    (RefType::Absolute, 0),
                    (RefType::Relative, -2)
                )))
            )
        );
    }

    #[test]
    fn test_roundabout_transform() {
        for formula in vec!["=A1:$D$4 + $B2:C$3", "=1:$4 + $A:C"] {
            let context = TransformContext::new(0, 0, 0);

            let base = raw_parser::parse_cell_content(formula).unwrap();

            let mut roundabout = raw_parser::parse_cell_content(formula).unwrap();
            transform_to_expr_ref(&mut roundabout, &context).unwrap();
            transform_to_expr_refa1(&mut roundabout, &context).unwrap();

            assert_eq!(base, roundabout);
        }
    }
}
