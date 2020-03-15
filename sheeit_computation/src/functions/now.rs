use super::eval_helper::datetime;
use crate::{Arity, EvalContext, EvalErrorKind, RefResolver};

use sheeit_storage::raw_parser::Expression;
use sheeit_storage::{CoreDocument, Value};
use std::time::SystemTime;

pub fn eval<'a>(
    _fn_name: &str,
    _context: &'a EvalContext,
    document: CoreDocument,
    args: &[&'a Expression],
    _eval_ref_handle: RefResolver,
) -> Result<(CoreDocument, Value), EvalErrorKind> {
    if !args.is_empty() {
        return Err(EvalErrorKind::InvalidParameterLength(
            Arity::Zero,
            args.len(),
        ));
    }

    let now = SystemTime::now();

    Ok((document, Value::Number(datetime::to_excel_datetime(now))))
}

#[cfg(test)]
mod tests {
    use super::datetime;
    use crate::{eval_cell, test_utils};
    use sheeit_storage::Value;
    use std::time::SystemTime;

    #[test]
    fn test_now() {
        let (context, doc) =
            test_utils::setup(test_utils::SHEET1_A1_COORD.sheet_coord().clone(), |doc| {
                doc.insert_cell_facts(
                    &test_utils::SHEET1_A1_COORD,
                    vec![vec![Some("=NOW()".to_string())]],
                )
                .unwrap();
            });

        // Rely on this implementation to be accurate.
        let now = datetime::to_excel_datetime(SystemTime::now());
        let (_doc, val) = eval_cell(&context, doc, test_utils::create_default_eval_handle())
            .expect("Should succeed");

        let val_num = if let Value::Number(val_num) = val {
            val_num
        } else {
            panic!("Did not get a number value.");
        };

        assert!(val_num - now < 0.000001);
    }
}
