use super::eval_helper::spill_eval;
use crate::{EvalContext, EvalErrorKind, RefResolver};
use sheeit_storage::raw_parser::Expression;
use sheeit_storage::{CoreDocument, Value};

fn eval_eq(val_left: &Value, val_right: &Value, _document: &CoreDocument) -> Value {
    match (val_left, val_right) {
        (Value::EvalError(err_val), _) | (_, Value::EvalError(err_val)) => {
            Value::EvalError(err_val.clone())
        }
        _ => Value::Bool(val_left.eq(val_right)),
    }
}

pub fn eval<'a>(
    _fn_name: &str,
    context: &'a EvalContext,
    document: CoreDocument,
    args: &[&'a Expression],
    eval_ref_handle: RefResolver,
) -> Result<(CoreDocument, Value), EvalErrorKind> {
    spill_eval::eval_two_arity_spill(context, document, args, eval_ref_handle, eval_eq)
}

#[cfg(test)]
mod tests {
    use crate::{eval_cell, test_utils};
    use lazy_static::lazy_static;
    use sheeit_storage::location::SheetCoordinate;
    use sheeit_storage::raw_parser::ExprErr;
    use sheeit_storage::{EvalErrorVal, Value};

    lazy_static! {
        static ref A2: SheetCoordinate = SheetCoordinate::new(1, 0).unwrap();
    }

    #[test]
    fn test_1_err_val() {
        let (context, doc) = test_utils::setup(A2.clone(), |doc| {
            doc.insert_cell_facts(
                &test_utils::SHEET1_A1_COORD,
                vec![vec![
                    Some("=#REF!".to_string()),
                    Some("=A1 = 1".to_string()),
                ]],
            )
            .unwrap();

            ()
        });

        let (_doc, result) = eval_cell(&context, doc, test_utils::create_default_eval_handle())
            .expect("Should succeed");

        assert_eq!(
            result,
            Value::EvalError(EvalErrorVal::ExpressionErr(ExprErr::RefErr))
        )
    }

    #[test]
    fn test_both_err_vals() {
        let (context, doc) = test_utils::setup(SheetCoordinate::new(2, 0).unwrap(), |doc| {
            doc.insert_cell_facts(
                &test_utils::SHEET1_A1_COORD,
                vec![vec![
                    Some("=#NUM!".to_string()),
                    Some("=#REF!".to_string()),
                    Some("=A1 = A2".to_string()),
                ]],
            )
            .unwrap();

            ()
        });

        let (_doc, result) = eval_cell(&context, doc, test_utils::create_default_eval_handle())
            .expect("Should succeed");

        assert_eq!(
            result,
            Value::EvalError(EvalErrorVal::ExpressionErr(ExprErr::NumErr))
        )
    }

    #[test]
    fn test_true_eq() {
        let (context, doc) = test_utils::setup(A2.clone(), |doc| {
            doc.insert_cell_facts(
                &test_utils::SHEET1_A1_COORD,
                vec![vec![Some("=1".to_string()), Some("=A1 = 1".to_string())]],
            )
            .unwrap();

            ()
        });

        let (_doc, result) = eval_cell(&context, doc, test_utils::create_default_eval_handle())
            .expect("Should succeed");

        assert_eq!(result, Value::Bool(true))
    }

    #[test]
    fn test_false_eq() {
        let (context, doc) = test_utils::setup(A2.clone(), |doc| {
            doc.insert_cell_facts(
                &test_utils::SHEET1_A1_COORD,
                vec![vec![
                    Some("=\"2\"".to_string()),
                    Some("=A1 = 1".to_string()),
                ]],
            )
            .unwrap();

            ()
        });

        let (_doc, result) = eval_cell(&context, doc, test_utils::create_default_eval_handle())
            .expect("Should succeed");

        assert_eq!(result, Value::Bool(false))
    }
}
