use crate::functions::eval_expression_to_val;
use crate::{Arity, EvalContext, EvalErrorKind, RefResolver};
use sheeit_storage::raw_parser::Expression;
use sheeit_storage::{CoreDocument, Value};

// TODO: Handle spills
pub fn eval<'a>(
    _fn_name: &str,
    context: &'a EvalContext,
    document: CoreDocument,
    args: &[&'a Expression],
    eval_ref_handle: RefResolver,
) -> Result<(CoreDocument, Value), EvalErrorKind> {
    if args.len() != 3 {
        return Err(EvalErrorKind::InvalidParameterLength(
            Arity::Three,
            args.len(),
        ));
    }

    let (document, predicate_val) =
        eval_expression_to_val(context, document, args[0], eval_ref_handle.clone())?;

    let bool_val = match predicate_val {
        Value::Bool(bool_val) => bool_val,
        Value::Number(num) => num != 0.0,
        Value::Integer(num) => num != 0,
        _ => false,
    };

    if bool_val {
        eval_expression_to_val(context, document, args[1], eval_ref_handle)
    } else {
        eval_expression_to_val(context, document, args[2], eval_ref_handle)
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
    fn test_choose_left() {
        let (context, doc) = test_utils::setup(A2.clone(), |doc| {
            doc.insert_cell_facts(
                &test_utils::SHEET1_A1_COORD,
                vec![vec![
                    Some("=1".to_string()),
                    Some("=IF(A1, \"chosen\", A3)".to_string()),
                    Some("=2+2".to_string()),
                ]],
            )
            .unwrap();

            ()
        });

        let (doc, val) = eval_cell(&context, doc, test_utils::create_default_eval_handle())
            .expect("Should succeed");

        assert_eq!(val, Value::String("chosen".to_string().into_boxed_str()));

        // Check that it doesn't evaluate the unnecessary side.
        let a3_value = doc
            .sheet_at(0)
            .and_then(|sheet| sheet.cell_at(&SheetCoordinate::new(2, 0).unwrap()))
            .map(|cell| cell.value().clone())
            .unwrap();

        assert_eq!(a3_value, Value::Blank);
    }

    #[test]
    fn test_choose_right() {
        let (context, doc) = test_utils::setup(A2.clone(), |doc| {
            doc.insert_cell_facts(
                &test_utils::SHEET1_A1_COORD,
                vec![vec![
                    Some("=0.0".to_string()),
                    Some("=IF(A1, A3, \"chosen\")".to_string()),
                    Some("=2+2".to_string()),
                ]],
            )
            .unwrap();

            ()
        });

        let (doc, val) = eval_cell(&context, doc, test_utils::create_default_eval_handle())
            .expect("Should succeed");

        assert_eq!(val, Value::String("chosen".to_string().into_boxed_str()));

        // Check that it doesn't evaluate the unnecessary side.
        let a3_value = doc
            .sheet_at(0)
            .and_then(|sheet| sheet.cell_at(&SheetCoordinate::new(2, 0).unwrap()))
            .map(|cell| cell.value().clone())
            .unwrap();

        assert_eq!(a3_value, Value::Blank);
    }
}
