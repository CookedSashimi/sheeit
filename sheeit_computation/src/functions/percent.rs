use crate::{functions, Arity, RefResolver};
use crate::{EvalContext, EvalErrorKind};
use sheeit_storage::raw_parser::Expression;
use sheeit_storage::{CoreDocument, Value};

fn eval_percent(val: f64) -> Value {
    Value::Number(val / 100.0)
}

fn eval_value(fn_name: &str, val: Value) -> Result<Value, EvalErrorKind> {
    match val {
        Value::Number(num) => Ok(eval_percent(num)),
        Value::Integer(integer) => Ok(eval_percent(integer as f64)),
        Value::Bool(_) | Value::String(_) | Value::EvalError(_) | Value::Custom(_) => {
            Err(EvalErrorKind::InvalidType(fn_name.to_string(), val))
        }
        Value::Ref(_) => Err(EvalErrorKind::UnimplementedFunctionEval(
            fn_name.to_string(),
            val,
        )),
        Value::Blank => Ok(Value::Number(0.0)),
        Value::Spill(vals) => {
            let mut result = vec![];

            for col in vals.iter() {
                let mut col_result = vec![];

                for item in col {
                    let new_val = eval_value(fn_name, item.clone())?;
                    col_result.push(new_val);
                }
                result.push(col_result);
            }

            Ok(Value::Spill(Box::new(result)))
        }
        Value::SpillRef(_) => Ok(val),
    }
}

pub fn eval<'a>(
    fn_name: &str,
    context: &'a EvalContext,
    document: CoreDocument,
    args: &[&'a Expression],
    eval_ref_handle: RefResolver,
) -> Result<(CoreDocument, Value), EvalErrorKind> {
    if args.len() != 1 {
        return Err(EvalErrorKind::InvalidParameterLength(
            Arity::One,
            args.len(),
        ));
    }

    let arg = args[0];
    let (doc, expr_val) =
        functions::eval_expression_to_val(context, document, arg, eval_ref_handle)?;

    eval_value(fn_name, expr_val).map(|val| (doc, val))
}

#[cfg(test)]
mod tests {
    use crate::{eval_cell, test_utils, EvalErrorKind};
    use matches::assert_matches;
    use sheeit_storage::location::Coordinate;
    use sheeit_storage::{Cell, Value};

    #[test]
    fn test_eval_cell_spill_percent_invalid_type() {
        let g1_coord = Coordinate::new(0, 0, 6).unwrap();
        let g1_ref_spill_percent = vec![vec![Some("=A1:B2%".to_string())]];

        let (g1_context, doc) = test_utils::setup(g1_coord.sheet_coord().clone(), |document| {
            document
                .insert_cells(
                    &Coordinate::new(0, 0, 0).unwrap(),
                    test_utils::A1_B2_VALUES.clone(),
                )
                .ok()
                .unwrap();

            document
                .insert_cell_facts(&g1_coord, g1_ref_spill_percent.clone())
                .ok()
                .unwrap();
        });

        let g1_eval = eval_cell(&g1_context, doc, test_utils::create_default_eval_handle())
            .expect_err("Should be invalid");
        assert_matches!(g1_eval, EvalErrorKind::InvalidType(_, _));
    }

    #[test]
    fn test_eval_cell_spill_percent_valid() {
        let g1_coord = Coordinate::new(0, 0, 6).unwrap();
        let g1_ref_spill_percent = vec![vec![Some("=A1:B2%".to_string())]];

        let b2_val = 500.0;

        let (g1_context, doc) = test_utils::setup(g1_coord.sheet_coord().clone(), |document| {
            document
                .insert_cells(
                    &Coordinate::new(0, 0, 0).unwrap(),
                    test_utils::A1_B2_VALUES.clone(),
                )
                .ok()
                .unwrap();

            // Override B2 with numbers
            document
                .insert_cells(
                    &Coordinate::new(0, 1, 1).unwrap(),
                    vec![vec![Some(Cell::with_value(Value::Number(b2_val), 0))]],
                )
                .ok()
                .unwrap();

            document
                .insert_cell_facts(&g1_coord, g1_ref_spill_percent.clone())
                .ok()
                .unwrap();
        });

        let (_, g1_eval) = eval_cell(&g1_context, doc, test_utils::create_default_eval_handle())
            .expect("Should succeed");

        let a1_val = match test_utils::A1_B2_VALUES.clone()[0][0]
            .as_ref()
            .unwrap()
            .value()
        {
            Value::Number(num) => Value::Number(num / 100.0),
            _ => unreachable!("Nope"),
        };

        assert_eq!(
            g1_eval,
            Value::Spill(Box::new(vec![
                vec![a1_val],
                vec![Value::Number(0.0), Value::Number(b2_val / 100.0)]
            ]))
        );
    }
}
