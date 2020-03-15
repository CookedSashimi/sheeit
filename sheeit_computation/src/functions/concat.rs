use super::eval_helper::spill_eval;
use crate::{EvalContext, EvalErrorKind, RefResolver};
use either::Either;
use sheeit_storage::raw_parser::Expression;
use sheeit_storage::{CoreDocument, EvalErrorVal, Value};

fn obtain_string(val: &Value, document: &CoreDocument) -> Either<String, Value> {
    match val {
        Value::String(_)
        | Value::Blank
        | Value::Number(_)
        | Value::Integer(_)
        | Value::Bool(_)
        | Value::Custom(_) => Either::Left(format!("{}", val)),
        Value::Spill(spill) => match spill.get(0).and_then(|rows| rows.get(0)) {
            Some(spill_val) => obtain_string(spill_val, document),
            None => Either::Left(String::new()),
        },
        // TODO: Handle multiple sheets.
        Value::SpillRef(coord) => {
            match document.sheet_at(0).and_then(|sheet| sheet.cell_at(coord)) {
                Some(cell) => obtain_string(cell.value(), document),
                None => Either::Left(String::new()),
            }
        }
        Value::Ref(_ref_val) => Either::Right(Value::EvalError(EvalErrorVal::Invalid)),
        Value::EvalError(_error_val) => Either::Right(val.clone()),
    }
}

fn eval_concat(val_left: &Value, val_right: &Value, document: &CoreDocument) -> Value {
    let left_string = match obtain_string(val_left, document) {
        Either::Left(format_string) => format_string,
        Either::Right(val) => return val,
    };

    let right_string = match obtain_string(val_right, document) {
        Either::Left(format_string) => format_string,
        Either::Right(val) => return val,
    };

    Value::String(format!("{}{}", left_string, right_string).into_boxed_str())
}

pub fn eval<'a>(
    _fn_name: &str,
    context: &'a EvalContext,
    document: CoreDocument,
    args: &[&'a Expression],
    eval_ref_handle: RefResolver,
) -> Result<(CoreDocument, Value), EvalErrorKind> {
    spill_eval::eval_two_arity_spill(context, document, args, eval_ref_handle, eval_concat)
}
#[cfg(test)]
mod tests {}
