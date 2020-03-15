use crate::{functions, Arity, EvalContext, EvalErrorKind, RefResolver};
use sheeit_storage::raw_parser::Expression;
use sheeit_storage::{CoreDocument, Value};

fn eval_negate(val: f64) -> Value {
    Value::Number(-val)
}

fn eval_value(fn_name: &str, val: Value) -> Result<Value, EvalErrorKind> {
    match val {
        Value::Number(num) => Ok(eval_negate(num)),
        Value::Integer(integer) => Ok(eval_negate(integer as f64)),
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

    #[test]
    fn num_val() {}
}
