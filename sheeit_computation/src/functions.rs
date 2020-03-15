mod add;
mod concat;
mod divide;
mod eq;
mod eval_helper;
mod exponentiate;
mod gt;
mod gte;
mod iffunc;
mod lt;
mod lte;
mod multiply;
mod negate;
mod noteq;
mod now;
mod percent;
mod rand;
mod subtract;
mod sum;

use crate::{EvalContext, EvalErrorKind, RefResolver};
use either::Either;
use lazy_static::lazy_static;
use sheeit_storage::location::{
    CellRange, Coordinate, RefersToLocation, SheetCoordinate, SheetRange,
};
use sheeit_storage::raw_parser::{Expression, Ref};
use sheeit_storage::{Cell, CoreDocument, EvalErrorVal, Value};
use std::cmp;
use std::collections::HashMap;

// This is preferable to trait objects due to static dispatch.
// We should maybe figure out how to unify the type for dynamic functions.
type BuiltInFunction = for<'a> fn(
    fn_name: &str,
    context: &'a EvalContext,
    document: CoreDocument,
    args: &[&'a Expression],
    eval_ref_handle: RefResolver,
) -> Result<(CoreDocument, Value), EvalErrorKind>;

static PERCENT_FUNC: &str = "PERCENT";
static NEGATIVE_FUNC: &str = "NEGATIVE";
static ADD_FUNC: &str = "ADD";
static SUBTRACT_FUNC: &str = "SUBTRACT";
static MULTIPLY_FUNC: &str = "MULTIPLY";
static DIVIDE_FUNC: &str = "DIVIDE";
static EXPONENTIATE_FUNC: &str = "EXPONENTIATE";
static EQ_FUNC: &str = "EQ";
static NOTEQ_FUNC: &str = "NOTEQ";
static GT_FUNC: &str = "GT";
static LT_FUNC: &str = "LT";
static GTE_FUNC: &str = "GTE";
static LTE_FUNC: &str = "LTE";
static CONCAT_FUNC: &str = "CONCATENATE";

lazy_static! {
    static ref BUILT_IN_FUNCTIONS: HashMap<&'static str, BuiltInFunction> = {
        let mut set = HashMap::new();
        set.insert(PERCENT_FUNC, percent::eval as BuiltInFunction);
        set.insert(NEGATIVE_FUNC, negate::eval as BuiltInFunction);
        set.insert(ADD_FUNC, add::eval as BuiltInFunction);
        set.insert(SUBTRACT_FUNC, subtract::eval as BuiltInFunction);
        set.insert(MULTIPLY_FUNC, multiply::eval as BuiltInFunction);
        set.insert(DIVIDE_FUNC, divide::eval as BuiltInFunction);
        set.insert(EXPONENTIATE_FUNC, exponentiate::eval as BuiltInFunction);
        set.insert(CONCAT_FUNC, concat::eval as BuiltInFunction);
        set.insert(EQ_FUNC, eq::eval as BuiltInFunction);
        set.insert(NOTEQ_FUNC, noteq::eval as BuiltInFunction);
        set.insert(GT_FUNC, gt::eval as BuiltInFunction);
        set.insert(GTE_FUNC, gte::eval as BuiltInFunction);
        set.insert(LT_FUNC, lt::eval as BuiltInFunction);
        set.insert(LTE_FUNC, lte::eval as BuiltInFunction);

        set.insert("RAND", rand::eval as BuiltInFunction);
        set.insert("SUM", sum::eval as BuiltInFunction);
        set.insert("IF", iffunc::eval as BuiltInFunction);
        set.insert("NOW", now::eval as BuiltInFunction);

        set
    };
}

pub fn eval_expression_to_val_or_ref<'a>(
    context: &'a EvalContext,
    document: CoreDocument,
    expr: &'a Expression,
    eval_ref_handle: RefResolver,
) -> Result<(CoreDocument, Either<&'a Ref, Value>), EvalErrorKind> {
    match expr {
        Expression::Parens(parens_expr) => {
            eval_expression_to_val_or_ref(context, document, parens_expr, eval_ref_handle)
        }
        Expression::ValueNum(num) => Ok((document, Either::Right(Value::Number(*num)))),
        Expression::ValueBool(bool_val) => Ok((document, Either::Right(Value::Bool(*bool_val)))),
        Expression::ValueString(expr_str) => Ok((
            document,
            Either::Right(Value::String(expr_str.clone().into_boxed_str())),
        )),
        Expression::Ref(expr_ref) => Ok((document, Either::Left(expr_ref))),
        Expression::Percent(arg) => eval_function(
            context,
            document,
            PERCENT_FUNC,
            vec![arg.as_ref()].as_slice(),
            eval_ref_handle,
        )
        .map(|(doc, val)| (doc, Either::Right(val))),
        Expression::Negate(arg) => eval_function(
            context,
            document,
            NEGATIVE_FUNC,
            vec![arg.as_ref()].as_slice(),
            eval_ref_handle,
        )
        .map(|(doc, val)| (doc, Either::Right(val))),
        Expression::Add(arg1, arg2) => eval_function(
            context,
            document,
            ADD_FUNC,
            vec![arg1.as_ref(), arg2.as_ref()].as_slice(),
            eval_ref_handle,
        )
        .map(|(doc, val)| (doc, Either::Right(val))),
        Expression::Subtract(arg1, arg2) => eval_function(
            context,
            document,
            SUBTRACT_FUNC,
            vec![arg1.as_ref(), arg2.as_ref()].as_slice(),
            eval_ref_handle,
        )
        .map(|(doc, val)| (doc, Either::Right(val))),
        Expression::Multiply(arg1, arg2) => eval_function(
            context,
            document,
            MULTIPLY_FUNC,
            vec![arg1.as_ref(), arg2.as_ref()].as_slice(),
            eval_ref_handle,
        )
        .map(|(doc, val)| (doc, Either::Right(val))),
        Expression::Divide(arg1, arg2) => eval_function(
            context,
            document,
            DIVIDE_FUNC,
            vec![arg1.as_ref(), arg2.as_ref()].as_slice(),
            eval_ref_handle,
        )
        .map(|(doc, val)| (doc, Either::Right(val))),
        Expression::Exponentiate(arg1, arg2) => eval_function(
            context,
            document,
            EXPONENTIATE_FUNC,
            vec![arg1.as_ref(), arg2.as_ref()].as_slice(),
            eval_ref_handle,
        )
        .map(|(doc, val)| (doc, Either::Right(val))),
        Expression::Eq(arg1, arg2) => eval_function(
            context,
            document,
            EQ_FUNC,
            vec![arg1.as_ref(), arg2.as_ref()].as_slice(),
            eval_ref_handle,
        )
        .map(|(doc, val)| (doc, Either::Right(val))),
        Expression::NotEq(arg1, arg2) => eval_function(
            context,
            document,
            NOTEQ_FUNC,
            vec![arg1.as_ref(), arg2.as_ref()].as_slice(),
            eval_ref_handle,
        )
        .map(|(doc, val)| (doc, Either::Right(val))),
        Expression::Gt(arg1, arg2) => eval_function(
            context,
            document,
            GT_FUNC,
            vec![arg1.as_ref(), arg2.as_ref()].as_slice(),
            eval_ref_handle,
        )
        .map(|(doc, val)| (doc, Either::Right(val))),
        Expression::Gte(arg1, arg2) => eval_function(
            context,
            document,
            GTE_FUNC,
            vec![arg1.as_ref(), arg2.as_ref()].as_slice(),
            eval_ref_handle,
        )
        .map(|(doc, val)| (doc, Either::Right(val))),
        Expression::Lt(arg1, arg2) => eval_function(
            context,
            document,
            LT_FUNC,
            vec![arg1.as_ref(), arg2.as_ref()].as_slice(),
            eval_ref_handle,
        )
        .map(|(doc, val)| (doc, Either::Right(val))),
        Expression::Lte(arg1, arg2) => eval_function(
            context,
            document,
            LTE_FUNC,
            vec![arg1.as_ref(), arg2.as_ref()].as_slice(),
            eval_ref_handle,
        )
        .map(|(doc, val)| (doc, Either::Right(val))),
        Expression::Concat(arg1, arg2) => eval_function(
            context,
            document,
            CONCAT_FUNC,
            vec![arg1.as_ref(), arg2.as_ref()].as_slice(),
            eval_ref_handle,
        )
        .map(|(doc, val)| (doc, Either::Right(val))),
        Expression::Fn(fun_name, args) => {
            let args: Vec<&Expression> = args.iter().collect();

            eval_function(
                context,
                document,
                fun_name,
                args.as_slice(),
                eval_ref_handle,
            )
            .map(|(doc, val)| (doc, Either::Right(val)))
        }
        Expression::Err(_) | Expression::RefA1(_) => {
            Err(EvalErrorKind::UnsupportedExpression(expr.clone()))
        }
    }
}

pub fn eval_expression_to_val<'a>(
    context: &'a EvalContext,
    document: CoreDocument,
    expr: &'a Expression,
    eval_ref_handle: RefResolver,
) -> Result<(CoreDocument, Value), EvalErrorKind> {
    let (new_doc, val_or_ref) =
        { eval_expression_to_val_or_ref(context, document, expr, eval_ref_handle.clone())? };

    match val_or_ref {
        Either::Left(expr_ref) => eval_ref(context, new_doc, expr_ref, eval_ref_handle),
        Either::Right(val) => Ok((new_doc, val)),
    }
}

fn evaluable_ref_area(
    context: &EvalContext,
    expr_ref: &Ref,
) -> Result<(Coordinate, Coordinate), EvalErrorKind> {
    match expr_ref {
        Ref::RowRangeRef(_, _) | Ref::ColumnRangeRef(_, _) => Err(EvalErrorKind::UnlimitedSpill),
        Ref::UnionRef(_, _) | Ref::IntersectRef(_, _) => {
            Err(EvalErrorKind::UnimplementedRefEval(expr_ref.clone()))
        }
        Ref::CellRef(cell_ref) => {
            let location: SheetCoordinate = cell_ref
                .refers_to(&context.coord.sheet_coord())
                .map_err(|_| EvalErrorKind::RefEvalError)?;

            let cell_ref_coord = Coordinate::new_with_coord(context.coord.sheet(), location)
                .expect("Coordinate in CellRef should already be valid.");

            Ok((cell_ref_coord.clone(), cell_ref_coord))
        }
        Ref::RangeRef(ref1, ref2) => {
            let (ref1_start, ref1_end) = evaluable_ref_area(context, ref1)?;
            let (ref2_start, ref2_end) = evaluable_ref_area(context, ref2)?;

            // Only support range ref within the same sheet for now.
            if !(ref1_start.sheet() == ref1_end.sheet()
                && ref1_start.sheet() == ref2_start.sheet()
                && ref1_start.sheet() == ref2_end.sheet())
            {
                return Err(EvalErrorKind::UnsupportedRef(expr_ref.clone()));
            }

            let min_start_row = cmp::min(ref1_start.row(), ref2_start.row());
            let max_end_row = cmp::max(ref1_end.row(), ref2_end.row());
            let min_start_col = cmp::min(ref1_start.col(), ref2_start.col());
            let max_end_col = cmp::max(ref1_end.col(), ref2_end.col());

            let start_coord = Coordinate::new(ref1_start.sheet(), min_start_row, min_start_col)
                .expect("Coordinates from Ref should already be valid");
            let end_coord = Coordinate::new(ref1_start.sheet(), max_end_row, max_end_col)
                .expect("Coordinates from Ref should already be valid");

            Ok((start_coord, end_coord))
        }
    }
}

pub fn eval_ref<'a>(
    context: &'a EvalContext,
    document: CoreDocument,
    expr_ref: &'a Ref,
    eval_ref_handle: RefResolver,
) -> Result<(CoreDocument, Value), EvalErrorKind> {
    match expr_ref {
        Ref::CellRef(cell_ref) => {
            let coord: SheetCoordinate = cell_ref
                .refers_to(context.coord.sheet_coord())
                .map_err(|_| EvalErrorKind::RefEvalError)?;

            if coord.eq(context.coord.sheet_coord()) {
                return Ok((document, Value::EvalError(EvalErrorVal::CyclicDependency)));
            }

            let sheet = match document.sheet_at(context.coord.sheet()) {
                None => return Ok((document, Value::Blank)),
                Some(sheet) => sheet,
            };

            let cell = match sheet.cell_at(&coord) {
                None => return Ok((document, Value::Blank)),
                Some(cell) => cell,
            };

            if has_cyclic_ref(cell, &coord, context)? {
                return Ok((document, Value::EvalError(EvalErrorVal::CyclicDependency)));
            }

            let new_doc = {
                // Excessive deref due to to Rust bug: https://github.com/rust-lang/rust/issues/51886
                (&mut *(*eval_ref_handle).borrow_mut())(expr_ref, &document)?
            };

            let sheet = match new_doc.sheet_at(context.coord.sheet()) {
                None => return Ok((new_doc, Value::Blank)),
                Some(sheet) => sheet,
            };

            let cell = match sheet.cell_at(&coord) {
                None => return Ok((new_doc, Value::Blank)),
                Some(cell) => cell,
            };

            let val = cell.value().clone();

            Ok((new_doc, val))
        }
        Ref::RangeRef(_, _) => {
            // Guarantees that we are in the same sheet.
            let (start_coord, end_coord) = evaluable_ref_area(context, expr_ref)?;

            let cell_range = CellRange::from_coords(
                start_coord.sheet_coord().clone(),
                end_coord.sheet_coord().clone(),
            )
            .expect("Cell Range Should be valid.");

            if SheetRange::CellRange(cell_range.clone()).is_in_range(&context.coord.sheet_coord()) {
                return Ok((document, Value::EvalError(EvalErrorVal::CyclicDependency)));
            }

            let new_doc = {
                // Excessive deref due to to Rust bug: https://github.com/rust-lang/rust/issues/51886
                (&mut *(*eval_ref_handle).borrow_mut())(expr_ref, &document)?
            };

            let sheet = match new_doc.sheet_at(start_coord.sheet()) {
                None => {
                    return Ok((new_doc, Value::Blank));
                }
                Some(sheet) => sheet,
            };

            let mut result = vec![];

            for col in sheet.cells_in_range(&cell_range).values().iter_mut() {
                let mut col_result = vec![];
                for row_index in 0..col.len() {
                    col_result.push(
                        col.get(row_index)
                            .map_or_else(|| Value::Blank, |cell| cell.value().clone()),
                    )
                }

                result.push(col_result);
            }

            Ok((new_doc, Value::Spill(Box::new(result))))
        }
        Ref::RowRangeRef(_, _) | Ref::ColumnRangeRef(_, _) => Err(EvalErrorKind::UnlimitedSpill),
        Ref::UnionRef(_, _) | Ref::IntersectRef(_, _) => {
            // unimplemented!()
            Err(EvalErrorKind::UnimplementedRefEval(expr_ref.clone()))
        }
    }
}

pub fn eval_function<'a>(
    context: &'a EvalContext,
    document: CoreDocument,
    fn_name: &str,
    args: &[&'a Expression],
    eval_ref_handle: RefResolver,
) -> Result<(CoreDocument, Value), EvalErrorKind> {
    // TODO: Get dynamically-gotten functions.

    match BUILT_IN_FUNCTIONS.get(fn_name) {
        None => Err(EvalErrorKind::UnimplementedFunction(fn_name.to_string())),
        Some(func) => func(fn_name, context, document, args, eval_ref_handle),
    }
}

fn has_cyclic_ref(
    cell: &Cell,
    cell_coord: &SheetCoordinate,
    context: &EvalContext,
) -> Result<bool, EvalErrorKind> {
    match cell.formula() {
        None => Ok(false),
        Some(formula) => expr_has_cyclic_ref(formula.parsed(), cell_coord, context),
    }
}

fn expr_has_cyclic_ref(
    expr: &Expression,
    expr_source: &SheetCoordinate,
    context: &EvalContext,
) -> Result<bool, EvalErrorKind> {
    match expr {
        Expression::Err(_)
        | Expression::ValueBool(_)
        | Expression::ValueNum(_)
        | Expression::ValueString(_) => Ok(false),
        Expression::RefA1(refa1) => {
            println!(
                "Got an unexpected RefA1 during cyclic detection: {:#?}",
                refa1
            );

            Ok(false)
        }
        Expression::Parens(expr) | Expression::Percent(expr) | Expression::Negate(expr) => {
            expr_has_cyclic_ref(expr, expr_source, context)
        }
        Expression::Add(expr1, expr2)
        | Expression::Subtract(expr1, expr2)
        | Expression::Multiply(expr1, expr2)
        | Expression::Divide(expr1, expr2)
        | Expression::Exponentiate(expr1, expr2)
        | Expression::Eq(expr1, expr2)
        | Expression::NotEq(expr1, expr2)
        | Expression::Gt(expr1, expr2)
        | Expression::Lt(expr1, expr2)
        | Expression::Gte(expr1, expr2)
        | Expression::Lte(expr1, expr2)
        | Expression::Concat(expr1, expr2) => Ok(expr_has_cyclic_ref(expr1, expr_source, context)?
            || expr_has_cyclic_ref(expr2, expr_source, context)?),
        Expression::Fn(_, exprs) => {
            for expr in exprs {
                if expr_has_cyclic_ref(expr, expr_source, context)? {
                    return Ok(true);
                }
            }

            Ok(false)
        }
        Expression::Ref(expr_ref) => is_cyclic_ref(expr_ref, expr_source, context),
    }
}

fn is_cyclic_ref(
    expr_ref: &Ref,
    expr_source: &SheetCoordinate,
    context: &EvalContext,
) -> Result<bool, EvalErrorKind> {
    let sheet_ranges = expr_ref
        .refers_to(expr_source)
        .map_err(|_| EvalErrorKind::RefEvalError)?;

    Ok(sheet_ranges
        .iter()
        .any(|range| range.is_in_range(context.coord.sheet_coord())))
}
