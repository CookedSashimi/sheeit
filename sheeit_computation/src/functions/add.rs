use super::eval_helper::{numeric, spill_eval};
use crate::{EvalContext, EvalErrorKind, RefResolver};
use sheeit_storage::raw_parser::Expression;
use sheeit_storage::{CoreDocument, EvalErrorVal, Value};

fn eval_to_int(first: i64, second: i64) -> Result<i64, EvalErrorVal> {
    Ok(first + second)
}

fn eval_to_float(first: f64, second: f64) -> Result<f64, EvalErrorVal> {
    Ok(first + second)
}

pub fn eval<'a>(
    _fn_name: &str,
    context: &'a EvalContext,
    document: CoreDocument,
    args: &[&'a Expression],
    eval_ref_handle: RefResolver,
) -> Result<(CoreDocument, Value), EvalErrorKind> {
    spill_eval::eval_two_arity_spill(
        context,
        document,
        args,
        eval_ref_handle,
        numeric::eval_non_spill_val_for_numeric(eval_to_int, eval_to_float),
    )
}
#[cfg(test)]
mod tests {}
