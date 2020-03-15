use sheeit_storage::{CoreDocument, EvalErrorVal, Value};
use std::cmp::Ordering;

/// A trait that has the same API as PartialOrd, just used for evaluating values.
// TODO: Determine if we want this to be inherent to Value itself. This feels like just a spreadsheet-eval thing.
pub trait ValuePartialOrd {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering>;
}

// TODO: Handle strings, other types of values.
impl ValuePartialOrd for Value {
    fn partial_cmp(&self, rhs: &Value) -> Option<Ordering> {
        match (self, rhs) {
            (Value::Number(left_num), Value::Number(right_num)) => left_num.partial_cmp(right_num),
            (Value::Integer(left_num), Value::Integer(right_num)) => {
                left_num.partial_cmp(right_num)
            }
            (Value::Number(left_num), Value::Integer(right_num)) => {
                left_num.partial_cmp(&(*right_num as f64))
            }
            (Value::Integer(left_num), Value::Number(right_num)) => {
                (*left_num as f64).partial_cmp(right_num)
            }
            _ => None,
        }
    }
}

// TODO: Handle strings.
pub fn eval_non_spill_val_for_numeric(
    eval_to_int: fn(i64, i64) -> Result<i64, EvalErrorVal>,
    eval_to_float: fn(f64, f64) -> Result<f64, EvalErrorVal>,
) -> impl for<'a> Fn(&'a Value, &'a Value, &'a CoreDocument) -> Value {
    move |left, right, _doc| match (left, right) {
        (Value::Number(left_num), Value::Number(right_num)) => {
            match eval_to_float(*left_num, *right_num) {
                Ok(num) => Value::Number(num),
                Err(val) => Value::EvalError(val),
            }
        }
        (Value::Number(left_num), Value::Integer(right_int)) => {
            match eval_to_float(*left_num, *right_int as f64) {
                Ok(num) => Value::Number(num),
                Err(val) => Value::EvalError(val),
            }
        }
        (Value::Integer(left_int), Value::Number(right_num)) => {
            match eval_to_float(*left_int as f64, *right_num) {
                Ok(num) => Value::Number(num),
                Err(val) => Value::EvalError(val),
            }
        }
        (Value::Integer(left_int), Value::Integer(right_int)) => {
            match eval_to_int(*left_int, *right_int) {
                Ok(num) => Value::Integer(num),
                Err(val) => Value::EvalError(val),
            }
        }
        (Value::Number(num), Value::Blank) | (Value::Blank, Value::Number(num)) => {
            Value::Number(*num)
        }
        (Value::Integer(num), Value::Blank) | (Value::Blank, Value::Integer(num)) => {
            Value::Integer(*num)
        }
        _ => Value::EvalError(EvalErrorVal::Invalid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils;
    use lazy_static::lazy_static;
    use sheeit_storage::location::Coordinate;
    use sheeit_storage::Value;

    lazy_static! {
        // A5
        static ref FORMULA_COORD: Coordinate = Coordinate::new(0, 4, 0).unwrap();
    }

    #[test]
    fn test_eval_non_spill_val_for_numeric() {
        let (_eval_context, doc) =
            test_utils::setup(FORMULA_COORD.sheet_coord().clone(), |_doc| ());

        let closure = eval_non_spill_val_for_numeric(|x, y| Ok(x + y), |x, y| Ok(x + y));

        assert_eq!(
            closure(&Value::Number(1.0), &Value::Number(2.0), &doc),
            Value::Number(3.0)
        );
        assert_eq!(
            closure(&Value::Number(1.0), &Value::Integer(2), &doc),
            Value::Number(3.0)
        );
        assert_eq!(
            closure(&Value::Integer(1), &Value::Number(2.0), &doc),
            Value::Number(3.0)
        );
        assert_eq!(
            closure(&Value::Integer(1), &Value::Integer(2), &doc),
            Value::Integer(3)
        );
        assert_eq!(
            closure(&Value::Number(1.0), &Value::Blank, &doc),
            Value::Number(1.0)
        );
        assert_eq!(
            closure(&Value::Blank, &Value::Number(1.0), &doc),
            Value::Number(1.0)
        );
        assert_eq!(
            closure(&Value::Integer(1), &Value::Blank, &doc),
            Value::Integer(1)
        );
        assert_eq!(
            closure(&Value::Blank, &Value::Integer(1), &doc),
            Value::Integer(1)
        );

        assert_eq!(
            closure(
                &Value::String("stuff".to_string().into_boxed_str()),
                &Value::Integer(1),
                &doc
            ),
            Value::EvalError(EvalErrorVal::Invalid)
        );
    }
}
