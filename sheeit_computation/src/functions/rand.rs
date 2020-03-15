use crate::{Arity, EvalContext, EvalErrorKind, RefResolver};
use rand;
use rand::Rng;
use sheeit_storage::raw_parser::Expression;
use sheeit_storage::{CoreDocument, Value};

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

    let mut rng = rand::thread_rng();

    Ok((document, Value::Number(rng.gen::<f64>())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{eval_cell, test_utils};
    use matches::assert_matches;

    #[test]
    fn test_more_args() {
        let (_a1_context, doc) = test_utils::setup(
            test_utils::SHEET1_A1_COORD.sheet_coord().clone(),
            |document| {
                document
                    .insert_cell_facts(
                        &test_utils::SHEET1_A1_COORD,
                        vec![vec![Some("=RAND(1)".to_string())]],
                    )
                    .ok()
                    .unwrap();
            },
        );

        assert_matches!(
            eval_cell(
                &test_utils::SHEET1_A1_CONTEXT,
                doc,
                test_utils::create_default_eval_handle()
            )
            .unwrap_err(),
            EvalErrorKind::InvalidParameterLength(Arity::Zero, 1)
        );
    }

    #[test]
    fn test_correct_eval() {
        let (_a1_context, doc) = test_utils::setup(
            test_utils::SHEET1_A1_COORD.sheet_coord().clone(),
            |document| {
                document
                    .insert_cell_facts(
                        &test_utils::SHEET1_A1_COORD,
                        vec![vec![Some("=RAND()".to_string())]],
                    )
                    .ok()
                    .unwrap();
            },
        );

        assert_matches!(
            eval_cell(
                &test_utils::SHEET1_A1_CONTEXT,
                doc,
                test_utils::create_default_eval_handle()
            )
            .unwrap()
            .1,
            Value::Number(_)
        );
    }
}
