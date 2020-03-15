//! This is the crate responsible for actually performing the evaluation for the Sheeit ecosystem.
//! It deliberately doesn't have any dependency on the main `sheeit` crate. Rather, it only depends
//! on the `sheeit_storage` crate to iterate and extract data.
//!
//! It has a strong a strong assumption that it's working with immutable data structures with cheap
//! cloning.

mod functions;
mod util;

use sheeit_storage::location::Coordinate;
use sheeit_storage::raw_parser::{Expression, Ref};
use sheeit_storage::{CoreDocument, Value};
use std::cell::RefCell;
use std::fmt::{Display, Error, Formatter};
use std::rc::Rc;
use thiserror::Error;

// TODO: Implement move cell. This needs to leverage the graph somehow.
// I'm leaning towards implementing this in Sheet though. Pass the graph as dependency.

// TODO: RELEASE-BLOCKER: Figure out what this module actually is?
// This module is more like sheeit, I think.

/// The Arity of the function. Used as a value in EvalErrorKind
#[derive(Debug, Clone)]
pub enum Arity {
    Zero,
    One,
    Two,
    Three,
    OneOrMore,
}

impl Display for Arity {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Arity::Zero => write!(f, "0"),
            Arity::One => write!(f, "1"),
            Arity::Two => write!(f, "2"),
            Arity::Three => write!(f, "3"),
            Arity::OneOrMore => write!(f, "1 or more"),
        }
    }
}

/// The errors that may occur during evaluation.
#[derive(Error, Debug)]
pub enum EvalErrorKind {
    #[error("Cell is not a formula")]
    NotAFormula,

    #[error("Coordinate `{:?}` is invalid", .0)]
    InvalidCoordinate(Coordinate),

    #[error("Expression `{:?}` is invalid", .0)]
    UnsupportedExpression(Expression),

    #[error("Reference `{}` is invalid", .0)]
    UnsupportedRef(Ref),

    #[error("Invalid parameter length. Expected: {}, Got: {}", .0, .1)]
    InvalidParameterLength(Arity, usize),

    #[error("Function `{}` is not implemented.", .0)]
    UnimplementedFunction(String),

    //TODO: Implement Display for Expression and Value.
    #[error("Function `{}` cannot evaluate arguments of the type of `{}`", .0, .1)]
    InvalidType(String, Value),

    #[error("Function `{}` has not implemented evaluating arguments of the type of `{:?}`", .0, .1)]
    UnimplementedFunctionEval(String, Value),

    #[error("This type of expression is not implemented yet. Expression: {:?}", .0)]
    UnimplementedEval(Expression),

    #[error("This type of reference expression is not implemented yet: {}", .0)]
    UnimplementedRefEval(Ref),

    #[error("Spill formulas only work on a limited range.")]
    UnlimitedSpill,

    #[error("Referred cell has a lower Sequence than document to evaluate. This is likely due to cyclic dependency.")]
    InvalidSequence,

    #[error("Encountered an error when attempting to evaluate the Ref value in the callback.")]
    RefEvalError,
}

/// The context of evaluation.
/// Today, it only contains the coordinate of the cell which we're evaluating.
/// We use this coordinate to locate the cell and evaluate its expression.
#[derive(Clone, Debug)]
pub struct EvalContext {
    pub coord: Coordinate,
}

/// This is the lazy evaluation handle that callers must pass in for evaluation.
/// During evaluation, we don't expect the Document to have an up-to-date value for all cells.
/// Not even the cells that the current cell being evaluated depends on.
///
/// Therefore, during evaluation, we will call this handle whenever we decide that we need an up-to-date
/// Document for the specific Ref that we pass in.
///
/// It is the responsibility of the callee to return a Document that has evaluated value on all cells
/// that is referenced by the Ref we pass.
///
/// The callee may return an EvalErrorKind if the Ref is invalid. The error will be propagated back
/// to the caller of eval_cell.
pub type RefResolver =
    Rc<RefCell<dyn Fn(&Ref, &CoreDocument) -> Result<CoreDocument, EvalErrorKind>>>;

/// Evaluates a cell located in the coordinate provided by the context.
///
/// Does not expect the Document to have all its cells, or even all the cells that the context is dependent on
/// to be evaluated. (Read `RefResolver` for more information)
///
/// Does not mutate the cell. Instead, returns an evaluated value.
///
/// Also returns the newest Document that is provided by the last `RefResolver` call.
pub fn eval_cell(
    context: &EvalContext,
    document: CoreDocument,
    eval_ref_handle: RefResolver,
) -> Result<(CoreDocument, Value), EvalErrorKind> {
    let sheet = document
        .sheet_at(context.coord.sheet())
        .ok_or_else(|| EvalErrorKind::InvalidCoordinate(context.coord.clone()))?;

    let cell = sheet
        .cell_at(&context.coord.sheet_coord())
        .ok_or_else(|| EvalErrorKind::InvalidCoordinate(context.coord.clone()))?;

    match cell.formula() {
        Some(formula) => functions::eval_expression_to_val(
            context,
            document.clone(),
            formula.parsed(),
            eval_ref_handle,
        ),
        None => Err(EvalErrorKind::NotAFormula),
    }
}

#[cfg(test)]
pub mod test_utils {
    use crate::{EvalContext, RefResolver};
    use lazy_static::lazy_static;
    use sheeit_storage::location::{Coordinate, SheetCoordinate};
    use sheeit_storage::raw_parser::Ref;
    use sheeit_storage::{Cell, CoreDocument, Value};
    use std::cell::RefCell;
    use std::rc::Rc;

    lazy_static! {
        pub static ref A1_B2_VALUES: Vec<Vec<Option<Cell>>> = vec![
            vec![Some(Cell::with_value(Value::Number(23.2), 0)), None],
            vec![
                None,
                Some(Cell::with_value(
                    Value::String("hello".to_string().into_boxed_str()),
                    0
                )),
            ],
        ];
        pub static ref SHEET1_A1_COORD: Coordinate = Coordinate::new(0, 0, 0).unwrap();
        pub static ref SHEET1_A1_CONTEXT: EvalContext = EvalContext {
            coord: SHEET1_A1_COORD.clone(),
        };
    }

    pub fn create_default_eval_handle() -> RefResolver {
        Rc::new(RefCell::new(|_ref: &Ref, doc: &CoreDocument| {
            Ok(doc.clone())
        }))
    }

    pub fn setup<F: Fn(&mut CoreDocument) -> ()>(
        coord: SheetCoordinate,
        sheet_op: F,
    ) -> (EvalContext, CoreDocument) {
        // Not testing dependencies for now.
        let mut document = CoreDocument::new();
        document.add_sheet();

        sheet_op(&mut document);

        (
            EvalContext {
                coord: Coordinate::new_with_coord(0, coord).unwrap(),
            },
            document,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::eval_cell;
    use crate::test_utils;
    use crate::test_utils::A1_B2_VALUES;

    use sheeit_storage::location::{Coordinate, SheetCoordinate};
    use sheeit_storage::{Cell, Value};

    #[test]
    fn test_eval_cell_spill() {
        let c1_coord = SheetCoordinate::new(0, 2).unwrap();
        let c1_ref_spill = vec![vec![Some("=A1:B2".to_string())]];

        let (c1_context, doc) = test_utils::setup(c1_coord.clone(), |document| {
            document
                .insert_cells(
                    &Coordinate::new(0, 0, 0).unwrap(),
                    test_utils::A1_B2_VALUES.clone(),
                )
                .ok()
                .unwrap();

            document
                .insert_cell_facts(
                    &Coordinate::new_with_coord(0, c1_coord.clone()).unwrap(),
                    c1_ref_spill.clone(),
                )
                .ok()
                .unwrap();
        });

        // Based on A1_B2_VALUES
        let a1_b2_stored_vals: Vec<_> = vec![
            vec![A1_B2_VALUES[0][0].clone().unwrap().value().clone()],
            vec![
                Value::Blank,
                A1_B2_VALUES[1][1].clone().unwrap().value().clone(),
            ],
        ];

        let (_, c1_eval) = eval_cell(&c1_context, doc, test_utils::create_default_eval_handle())
            .expect("Should succeed");
        assert_eq!(c1_eval, Value::Spill(Box::new(a1_b2_stored_vals)));
    }

    #[test]
    fn test_eval_cell_ref_spill() {
        let c1_coord = Coordinate::new(0, 0, 2).unwrap();
        let e1_coord = Coordinate::new(0, 0, 4).unwrap();
        let c1_ref_spill = vec![vec![Some("=A1:B2".to_string())]];
        let e1_ref_ref_spill = vec![vec![Some("=$C$1".to_string())]];

        let (c1_context, doc) = test_utils::setup(c1_coord.sheet_coord().clone(), |document| {
            document
                .insert_cells(
                    &Coordinate::new(0, 0, 0).unwrap(),
                    test_utils::A1_B2_VALUES.clone(),
                )
                .ok()
                .unwrap();

            document
                .insert_cell_facts(&c1_coord, c1_ref_spill.clone())
                .ok()
                .unwrap();
        });

        let (_, c1_eval) = eval_cell(&c1_context, doc, test_utils::create_default_eval_handle())
            .expect("Should succeed");
        let (e1_context, doc) = test_utils::setup(e1_coord.sheet_coord().clone(), |document| {
            document
                .insert_cells(
                    &Coordinate::new(0, 0, 0).unwrap(),
                    test_utils::A1_B2_VALUES.clone(),
                )
                .ok()
                .unwrap();

            document
                .insert_cell_facts(&e1_coord, e1_ref_ref_spill.clone())
                .ok()
                .unwrap();

            document
                .insert_cells(
                    &c1_coord,
                    vec![vec![Some(Cell::with_value(c1_eval.clone(), 0))]],
                )
                .unwrap();
        });

        let (_, e1_eval) = eval_cell(&e1_context, doc, test_utils::create_default_eval_handle())
            .expect("Should succeed.");
        assert_eq!(e1_eval, c1_eval);
    }
}
