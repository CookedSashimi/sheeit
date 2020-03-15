use super::eval_helper::spill_eval;
use crate::functions::eval_helper::numeric::ValuePartialOrd;
use crate::{EvalContext, EvalErrorKind, RefResolver};
use sheeit_storage::raw_parser::Expression;
use sheeit_storage::{CoreDocument, Value};
use std::cmp::Ordering;

// TODO: Handle strings, other value types.
fn eval_gt(val_left: &Value, val_right: &Value, _document: &CoreDocument) -> Value {
    match (val_left, val_right) {
        (Value::EvalError(err_val), _) | (_, Value::EvalError(err_val)) => {
            Value::EvalError(err_val.clone())
        }
        _ => val_left
            .partial_cmp(val_right)
            .map(|ordering| ordering == Ordering::Less)
            .map(Value::Bool)
            .unwrap_or_else(|| Value::Bool(false)),
    }
}

pub fn eval<'a>(
    _fn_name: &str,
    context: &'a EvalContext,
    document: CoreDocument,
    args: &[&'a Expression],
    eval_ref_handle: RefResolver,
) -> Result<(CoreDocument, Value), EvalErrorKind> {
    spill_eval::eval_two_arity_spill(context, document, args, eval_ref_handle, eval_gt)
}

#[cfg(test)]
mod tests {
    use crate::{eval_cell, test_utils};

    use lazy_static::lazy_static;
    use sheeit_storage::location::SheetCoordinate;
    use sheeit_storage::Value;

    lazy_static! {
        static ref A2: SheetCoordinate = SheetCoordinate::new(1, 0).unwrap();
    }

    #[test]
    fn test_lt_true() {
        let (context, doc) = test_utils::setup(A2.clone(), |doc| {
            doc.insert_cell_facts(
                &test_utils::SHEET1_A1_COORD,
                vec![vec![Some("=1".to_string()), Some("=A1 < 1.9".to_string())]],
            )
            .unwrap();

            ()
        });

        let (_doc, result) = eval_cell(&context, doc, test_utils::create_default_eval_handle())
            .expect("Should succeed");

        assert_eq!(result, Value::Bool(true))
    }

    #[test]
    fn test_lt_false_eq() {
        let (context, doc) = test_utils::setup(A2.clone(), |doc| {
            doc.insert_cell_facts(
                &test_utils::SHEET1_A1_COORD,
                vec![vec![Some("=1".to_string()), Some("=A1 < 0.9".to_string())]],
            )
            .unwrap();

            ()
        });

        let (_doc, result) = eval_cell(&context, doc, test_utils::create_default_eval_handle())
            .expect("Should succeed");

        assert_eq!(result, Value::Bool(false))
    }
}
