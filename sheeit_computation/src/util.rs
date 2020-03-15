mod iterator;

pub use iterator::*;
use sheeit_storage::Value;

pub fn max_rows(spill: &[Vec<Value>]) -> usize {
    spill
        .iter()
        .max_by(|a, b| a.len().cmp(&b.len()))
        .map(|col| col.len())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use crate::util::max_rows;
    use sheeit_storage::Value;

    #[test]
    fn test_max_rows() {
        let spill = vec![
            vec![Value::Blank, Value::Blank],
            vec![],
            vec![Value::Blank, Value::Blank, Value::Blank],
        ];

        assert_eq!(max_rows(&spill), 3);
    }
}
