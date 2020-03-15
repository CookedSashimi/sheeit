use super::eq;
use crate::{EvalContext, EvalErrorKind, RefResolver};
use sheeit_storage::raw_parser::Expression;
use sheeit_storage::{CoreDocument, Value};

pub fn eval<'a>(
    fn_name: &str,
    context: &'a EvalContext,
    document: CoreDocument,
    args: &[&'a Expression],
    eval_ref_handle: RefResolver,
) -> Result<(CoreDocument, Value), EvalErrorKind> {
    match eq::eval(fn_name, context, document, args, eval_ref_handle)? {
        (doc, Value::Bool(bool)) => Ok((doc, Value::Bool(!bool))),
        (doc, val) => Ok((doc, val)),
    }
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
    fn test_false_eq() {
        let (context, doc) = test_utils::setup(A2.clone(), |doc| {
            doc.insert_cell_facts(
                &test_utils::SHEET1_A1_COORD,
                vec![vec![Some("=1".to_string()), Some("=A1 <> 1".to_string())]],
            )
            .unwrap();

            ()
        });

        let (_doc, result) = eval_cell(&context, doc, test_utils::create_default_eval_handle())
            .expect("Should succeed");

        assert_eq!(result, Value::Bool(false))
    }

    #[test]
    fn test_true_eq() {
        let (context, doc) = test_utils::setup(A2.clone(), |doc| {
            doc.insert_cell_facts(
                &test_utils::SHEET1_A1_COORD,
                vec![vec![
                    Some("=\"2\"".to_string()),
                    Some("=A1 <> 1".to_string()),
                ]],
            )
            .unwrap();

            ()
        });

        let (_doc, result) = eval_cell(&context, doc, test_utils::create_default_eval_handle())
            .expect("Should succeed");

        assert_eq!(result, Value::Bool(true))
    }
}
