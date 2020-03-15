use crate::functions;
use crate::util;
use crate::{Arity, EvalContext, EvalErrorKind, RefResolver};

use sheeit_storage::raw_parser::{ExprErr, Expression};
use sheeit_storage::{CoreDocument, EvalErrorVal, Value};
use std::cmp;

// Locality in understanding spills here is important.
#[allow(clippy::cognitive_complexity)]
pub fn eval_two_arity_spill<'a, F: for<'b> Fn(&'b Value, &'b Value, &'b CoreDocument) -> Value>(
    context: &'a EvalContext,
    document: CoreDocument,
    args: &[&'a Expression],
    eval_ref_handle: RefResolver,
    eval_non_spill_val: F,
) -> Result<(CoreDocument, Value), EvalErrorKind> {
    // TODO: Handle non-2-arity cases.
    if args.len() != 2 {
        return Err(EvalErrorKind::InvalidParameterLength(
            Arity::Two,
            args.len(),
        ));
    }

    let mut current_doc = document;
    let (new_doc, val_left) =
        functions::eval_expression_to_val(context, current_doc, args[0], eval_ref_handle.clone())?;
    let (new_doc, val_right) =
        functions::eval_expression_to_val(context, new_doc, args[1], eval_ref_handle.clone())?;

    current_doc = new_doc;

    // Left and right matters here, as not every operation is commutative, e.g. divide or subtract.
    let final_val = match (val_left, val_right) {
        (Value::Spill(spill_val_left), Value::Spill(spill_val_right)) => {
            let left_col_len = spill_val_left.len();
            let right_col_len = spill_val_right.len();

            let left_row_len = util::max_rows(&spill_val_left);
            let right_row_len = util::max_rows(&spill_val_right);

            let spill_cols_len = cmp::max(left_col_len, right_col_len);
            let spill_rows_len = cmp::max(left_row_len, right_row_len);

            let (left_is_single_row, left_is_single_col, right_is_single_row, right_is_single_col) = (
                left_row_len == 1,
                left_col_len == 1,
                right_row_len == 1,
                right_col_len == 1,
            );

            let spill_result = match (
                left_is_single_row,
                left_is_single_col,
                right_is_single_row,
                right_is_single_col,
            ) {
                // We're dealing with just single cells here: Just eval to a single val.
                (true, true, true, true) => vec![vec![eval_non_spill_val(
                    &spill_val_left[0][0],
                    &spill_val_right[0][0],
                    &current_doc,
                )]],
                // Left is a single cell, Right is: (a row, a column, an area): Apply single cell to each area.
                (true, true, true, false)
                | (true, true, false, true)
                | (true, true, false, false) => {
                    let left_val = &spill_val_left[0][0];
                    spill_val_right
                        .iter()
                        .map(|right_rows| {
                            right_rows
                                .iter()
                                .map(|right_val| {
                                    eval_non_spill_val(left_val, right_val, &current_doc)
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>()
                }
                // Right is a single cell, Left is: (a row, a column, an area): Apply single cell to each area.
                (true, false, true, true)
                | (false, true, true, true)
                | (false, false, true, true) => {
                    let right_val = &spill_val_right[0][0];
                    spill_val_left
                        .iter()
                        .map(|left_rows| {
                            left_rows
                                .iter()
                                .map(|left_val| {
                                    eval_non_spill_val(left_val, right_val, &current_doc)
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>()
                }
                // Left is Row, Right is Column: Calculate product of area
                (true, false, false, true) => {
                    // In this case, we can guarantee that accessing via index will not panic.
                    let left_row = spill_val_left.iter().map(|row| &row[0]).collect::<Vec<_>>();
                    let right_col = &spill_val_right[0];

                    let mut result_val = vec![];
                    // This is a bit funky:
                    // Essentially your row acts as a col now, and your col as a row.
                    for val_from_left_row in left_row.iter() {
                        let mut result_col = vec![];

                        for val_from_right_col in right_col.iter() {
                            result_col.push(eval_non_spill_val(
                                &val_from_left_row,
                                &val_from_right_col,
                                &current_doc,
                            ));
                        }

                        result_val.push(result_col);
                    }

                    result_val
                }
                // Right is Row, Left is Column: Calculate product of area
                (false, true, true, false) => {
                    // In this case, we can guarantee that accessing via index will not panic.
                    let right_row = spill_val_right
                        .iter()
                        .map(|row| &row[0])
                        .collect::<Vec<_>>();
                    let left_col = &spill_val_left[0];

                    let mut result_val = vec![];
                    // This is a bit funky:
                    // Essentially your row acts as a col now, and your col as a row.
                    for val_from_right_row in right_row.iter() {
                        let mut result_col = vec![];

                        for val_from_left_col in left_col.iter() {
                            result_col.push(eval_non_spill_val(
                                &val_from_left_col,
                                &val_from_right_row,
                                &current_doc,
                            ));
                        }

                        result_val.push(result_col);
                    }

                    result_val
                }
                // Left is Row, Right is Area: Calculate enclosing perimeter, with row applied to each row
                (true, false, false, false) => {
                    let left_row = spill_val_left.iter().map(|row| &row[0]).collect::<Vec<_>>();
                    let cols_to_iterate = spill_cols_len;

                    let mut result_val = vec![];
                    for col_index in 0..cols_to_iterate {
                        let mut result_col = vec![];

                        for row_index in 0..right_row_len {
                            match (
                                left_row.get(col_index),
                                spill_val_right
                                    .get(col_index)
                                    .and_then(|rows| rows.get(row_index)),
                            ) {
                                (Some(left_val), Some(right_val)) => {
                                    result_col.push(eval_non_spill_val(
                                        left_val,
                                        right_val,
                                        &current_doc,
                                    ));
                                }
                                _ => {
                                    result_col.push(Value::EvalError(EvalErrorVal::ExpressionErr(
                                        ExprErr::NotAvailable,
                                    )));
                                }
                            }
                        }

                        result_val.push(result_col);
                    }

                    result_val
                }
                // Right is Row, Left is Area: Calculate enclosing perimeter, with row applied to each row
                (false, false, true, false) => {
                    let right_row = spill_val_right
                        .iter()
                        .map(|row| &row[0])
                        .collect::<Vec<_>>();
                    let cols_to_iterate = spill_cols_len;

                    let mut result_val = vec![];
                    for col_index in 0..cols_to_iterate {
                        let mut result_col = vec![];

                        for row_index in 0..left_row_len {
                            match (
                                spill_val_left
                                    .get(col_index)
                                    .and_then(|rows| rows.get(row_index)),
                                right_row.get(col_index),
                            ) {
                                (Some(left_val), Some(right_val)) => {
                                    result_col.push(eval_non_spill_val(
                                        left_val,
                                        right_val,
                                        &current_doc,
                                    ));
                                }
                                _ => {
                                    result_col.push(Value::EvalError(EvalErrorVal::ExpressionErr(
                                        ExprErr::NotAvailable,
                                    )));
                                }
                            }
                        }

                        result_val.push(result_col);
                    }

                    result_val
                }
                // Left is Column, Right is Area: Calculate enclosing perimeter, with column applied to each column
                (false, true, false, false) => {
                    let left_column = &spill_val_left[0];

                    let mut result_val = vec![];
                    for col_index in 0..right_col_len {
                        let mut result_col = vec![];

                        for row_index in 0..spill_rows_len {
                            match (
                                left_column.get(row_index),
                                spill_val_right
                                    .get(col_index)
                                    .and_then(|rows| rows.get(row_index)),
                            ) {
                                (Some(left_val), Some(right_val)) => {
                                    result_col.push(eval_non_spill_val(
                                        left_val,
                                        right_val,
                                        &current_doc,
                                    ));
                                }
                                _ => {
                                    result_col.push(Value::EvalError(EvalErrorVal::ExpressionErr(
                                        ExprErr::NotAvailable,
                                    )));
                                }
                            }
                        }

                        result_val.push(result_col);
                    }

                    result_val
                }
                // Right is Column, left is Area: Calculate enclosing perimeter, with column applied to each column
                (false, false, false, true) => {
                    let right_column = &spill_val_right[0];

                    let mut result_val = vec![];
                    for col_index in 0..left_col_len {
                        let mut result_col = vec![];

                        for row_index in 0..spill_rows_len {
                            match (
                                spill_val_left
                                    .get(col_index)
                                    .and_then(|rows| rows.get(row_index)),
                                right_column.get(row_index),
                            ) {
                                (Some(left_val), Some(right_val)) => {
                                    result_col.push(eval_non_spill_val(
                                        left_val,
                                        right_val,
                                        &current_doc,
                                    ));
                                }
                                _ => {
                                    result_col.push(Value::EvalError(EvalErrorVal::ExpressionErr(
                                        ExprErr::NotAvailable,
                                    )));
                                }
                            }
                        }

                        result_val.push(result_col);
                    }

                    result_val
                }
                // Both are single rows, Both are single columns, Both are areas: Calculate enclosing perimeter.
                (true, false, true, false)
                | (false, true, false, true)
                | (false, false, false, false) => {
                    let mut result_val = vec![];
                    for col_index in 0..spill_cols_len {
                        let mut result_col = vec![];

                        for row_index in 0..spill_rows_len {
                            let left_val = match spill_val_left
                                .get(col_index)
                                .and_then(|rows| rows.get(row_index))
                            {
                                None => {
                                    result_col.push(Value::EvalError(EvalErrorVal::ExpressionErr(
                                        ExprErr::NotAvailable,
                                    )));
                                    continue;
                                }
                                Some(val) => val,
                            };

                            let right_val = match spill_val_right
                                .get(col_index)
                                .and_then(|rows| rows.get(row_index))
                            {
                                None => {
                                    result_col.push(Value::EvalError(EvalErrorVal::ExpressionErr(
                                        ExprErr::NotAvailable,
                                    )));
                                    continue;
                                }
                                Some(val) => val,
                            };

                            result_col.push(eval_non_spill_val(left_val, right_val, &current_doc));
                        }

                        result_val.push(result_col);
                    }
                    result_val
                }
            };

            Value::Spill(Box::new(spill_result))
        }
        (Value::Spill(spill_left), val_right) => {
            let mut result_val = vec![];
            for col in spill_left.iter() {
                let mut result_col = vec![];
                for val in col.iter() {
                    result_col.push(eval_non_spill_val(val, &val_right, &current_doc));
                }

                result_val.push(result_col);
            }

            Value::Spill(Box::new(result_val))
        }
        (val_left, Value::Spill(spill_right)) => {
            let mut result_val = vec![];
            for col in spill_right.iter() {
                let mut result_col = vec![];
                for val in col.iter() {
                    result_col.push(eval_non_spill_val(&val_left, val, &current_doc));
                }

                result_val.push(result_col);
            }

            Value::Spill(Box::new(result_val))
        }
        (left_val, right_val) => eval_non_spill_val(&left_val, &right_val, &current_doc),
    };

    Ok((current_doc, final_val))
}

#[cfg(test)]
mod tests {
    use crate::{eval_cell, test_utils};
    use lazy_static::lazy_static;
    use sheeit_storage::location::Coordinate;
    use sheeit_storage::raw_parser::ExprErr;
    use sheeit_storage::{EvalErrorVal, Value};

    // 3x3 square starting at A1
    // 3x3 square starting at E1
    fn initial_cells() -> Vec<Vec<Option<String>>> {
        vec![
            vec![
                Some("A1".to_string()),
                Some("A2".to_string()),
                Some("A3".to_string()),
            ],
            vec![
                Some("B1".to_string()),
                Some("B2".to_string()),
                Some("B3".to_string()),
            ],
            vec![
                Some("C1".to_string()),
                Some("C2".to_string()),
                Some("C3".to_string()),
            ],
            vec![],
            vec![
                Some("E1".to_string()),
                Some("E2".to_string()),
                Some("E3".to_string()),
            ],
            vec![
                Some("F1".to_string()),
                Some("F2".to_string()),
                Some("F3".to_string()),
            ],
            vec![
                Some("G1".to_string()),
                Some("G2".to_string()),
                Some("G3".to_string()),
            ],
        ]
    }

    lazy_static! {
        // A5
        static ref FORMULA_COORD: Coordinate = Coordinate::new(0, 4, 0).unwrap();
    }

    // Test with non-commutative operations (concat) to ensure left/right is working.
    #[test]
    fn test_spills() {
        let test_cases = [
            ("Both single cells", "=A1:A1 & B1:B1", vec![vec!["A1B1"]]),
            (
                "Left is single cell, Right is a row",
                "=A1:A1 & E1:G1",
                vec![vec!["A1E1"], vec!["A1F1"], vec!["A1G1"]],
            ),
            (
                "Left is single cell, Right is a column",
                "=A1:A1 & E1:E2",
                vec![vec!["A1E1", "A1E2"]],
            ),
            (
                "Left is single cell, Right is an area",
                "=A1:A1 & E1:F2",
                vec![vec!["A1E1", "A1E2"], vec!["A1F1", "A1F2"]],
            ),
            (
                "Right is single cell, Left is a row",
                "=E1:G1 & A1:A1",
                vec![vec!["E1A1"], vec!["F1A1"], vec!["G1A1"]],
            ),
            (
                "Right is single cell, Left is a column",
                "=E1:E2 & A1:A1",
                vec![vec!["E1A1", "E2A1"]],
            ),
            (
                "Right is single cell, Left is an area",
                "=E1:F2 & A1:A1",
                vec![vec!["E1A1", "E2A1"], vec!["F1A1", "F2A1"]],
            ),
            (
                "Left is row, Right is Column", // Row and columns don't have the same length
                "=A1:B1 & E1:E3",
                vec![vec!["A1E1", "A1E2", "A1E3"], vec!["B1E1", "B1E2", "B1E3"]],
            ),
            (
                "Right is row, Left is Column", // Row and columns don't have the same length
                "=E1:E2 & A1:C1",
                vec![
                    vec!["E1A1", "E2A1"],
                    vec!["E1B1", "E2B1"],
                    vec!["E1C1", "E2C1"],
                ],
            ),
            (
                "Left is row, Right is Area. Row has longer length",
                "=A1:C1 & E1:F2",
                vec![
                    vec!["A1E1", "A1E2"],
                    vec!["B1F1", "B1F2"],
                    vec!["N/A", "N/A"],
                ],
            ),
            (
                "Right is row, Left is Area. Area has longer length",
                "=E1:G2 & A1:B1",
                vec![
                    vec!["E1A1", "E2A1"],
                    vec!["F1B1", "F2B1"],
                    vec!["N/A", "N/A"],
                ],
            ),
            (
                "Left is column, Right is Area. Area has longer length",
                "=A1:A2 & E1:F3",
                vec![vec!["A1E1", "A2E2", "N/A"], vec!["A1F1", "A2F2", "N/A"]],
            ),
            (
                "Right is column, Left is Area. Column has longer length",
                "=E1:F2 & A1:A3",
                vec![vec!["E1A1", "E2A2", "N/A"], vec!["F1A1", "F2A2", "N/A"]],
            ),
            (
                "Both are areas. Different lengths",
                "=A2:C3 & E1:F3",
                vec![
                    vec!["A2E1", "A3E2", "N/A"],
                    vec!["B2F1", "B3F2", "N/A"],
                    vec!["N/A", "N/A", "N/A"],
                ],
            ),
            (
                "Spill Left. Right is single",
                "=A1:B2 & E1",
                vec![vec!["A1E1", "A2E1"], vec!["B1E1", "B2E1"]],
            ),
            (
                "Spill Right. Left is single",
                "=E1 & A1:B2",
                vec![vec!["E1A1", "E1A2"], vec!["E1B1", "E1B2"]],
            ),
        ];

        for (_description, formula_str, expected_str) in test_cases.iter() {
            let formula = Some(formula_str.to_string());

            let (eval_context, doc) =
                test_utils::setup(FORMULA_COORD.sheet_coord().clone(), |doc| {
                    doc.insert_cell_facts(&test_utils::SHEET1_A1_COORD, initial_cells())
                        .unwrap();

                    doc.insert_cell_facts(&FORMULA_COORD, vec![vec![formula.clone()]])
                        .unwrap();

                    ()
                });

            let (_doc, value) =
                eval_cell(&eval_context, doc, test_utils::create_default_eval_handle())
                    .expect("Should succeed");

            let expected = expected_str
                .iter()
                .map(|rows| {
                    rows.iter()
                        .map(|concatted| match *concatted {
                            "N/A" => {
                                Value::EvalError(EvalErrorVal::ExpressionErr(ExprErr::NotAvailable))
                            }
                            _ => Value::String(concatted.to_string().into_boxed_str()),
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            assert_eq!(value, Value::Spill(Box::new(expected)))
        }
    }
}
