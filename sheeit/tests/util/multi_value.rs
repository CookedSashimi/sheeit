use sheeit::storage::*;
use std::any::Any;

#[derive(Debug, Clone)]
pub struct MultiValue {
    vals: Vec<Box<Value>>,
}

impl MultiValue {
    pub fn new() -> MultiValue {
        MultiValue { vals: Vec::new() }
    }

    pub fn vals(&self) -> &[Box<Value>] {
        &self.vals
    }

    pub fn add_val(&mut self, val: Value) {
        self.vals.push(Box::new(val));
    }
}

impl CustomValue for MultiValue {
    fn formatted_value(&self) -> String {
        let vals_len = self.vals.len();
        self.vals
            .iter()
            .enumerate()
            .fold(String::new(), |mut acc, (index, val)| {
                acc.push_str(format!("{}", &val).as_str());
                if index <= vals_len - 2 {
                    acc.push_str(", ");
                }
                acc
            })
    }

    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::Custom(custom) => {
                let custom_any = custom as &dyn Any;
                match custom_any.downcast_ref::<MultiValue>() {
                    Some(custom_multi) => custom_multi.vals().eq(self.vals()),
                    None => false,
                }
            }
            _ => false,
        }
    }
}

#[test]
fn multi_value_test() {
    use sheeit::storage::*;
    use MultiValue;

    let mut multi_val = MultiValue::new();
    multi_val.add_val(Value::String(String::from("First").into_boxed_str()));
    multi_val.add_val(Value::Integer(1));

    let mut multi_val_2 = MultiValue::new();
    multi_val_2.add_val(Value::String(String::from("Second").into_boxed_str()));
    multi_val_2.add_val(Value::Integer(2));
    multi_val_2.add_val(Value::Custom(Box::new(multi_val)));

    assert_eq!(multi_val_2.formatted_value(), "Second, 2, First, 1");
}
