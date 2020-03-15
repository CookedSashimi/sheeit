use crate::functions::eval_expression_to_val_or_ref;
use crate::util::IntoValueIteratorWith;
use crate::{Arity, EvalContext, EvalErrorKind, RefResolver};
use either::Either;
use sheeit_storage::raw_parser::{Expression, Ref};

use sheeit_storage::location::RefersToLocation;
use sheeit_storage::{CoreDocument, Value};

pub fn eval<'a>(
    _fn_name: &str,
    context: &'a EvalContext,
    document: CoreDocument,
    args: &[&'a Expression],
    eval_ref_handle: RefResolver,
) -> Result<(CoreDocument, Value), EvalErrorKind> {
    if args.is_empty() {
        return Err(EvalErrorKind::InvalidParameterLength(
            Arity::OneOrMore,
            args.len(),
        ));
    }

    let mut resolved: Vec<Either<&'a Ref, Value>> = vec![];
    let mut curr_document = document;
    for expr in args {
        let (doc, val) = eval_expression_to_val_or_ref(
            context,
            curr_document.clone(), // Maybe this can be optimized by a RefCell, but I don't it's necessary since we already assume clone is cheap for Document.
            *expr,
            eval_ref_handle.clone(),
        )?;

        curr_document = doc;

        resolved.push(val);
    }

    let mut acc = 0.0;

    for arg in resolved.iter() {
        match arg {
            Either::Left(expr_ref) => {
                curr_document = {
                    // Excessive deref due to to Rust bug: https://github.com/rust-lang/rust/issues/51886
                    (&mut *(*eval_ref_handle.clone()).borrow_mut())(expr_ref, &curr_document)?
                };
                let sheet = curr_document
                    .sheet_at(context.coord.sheet())
                    .ok_or_else(|| EvalErrorKind::InvalidCoordinate(context.coord.clone()))?;

                let ranges = expr_ref
                    .refers_to(context.coord.sheet_coord())
                    .map_err(|_| EvalErrorKind::RefEvalError)?;

                for range in ranges.iter() {
                    for val in range.into_value_iter_with(sheet) {
                        if let Some(num) = extract_val(val, context, &curr_document)? {
                            acc += num
                        }
                    }
                }
            }
            Either::Right(val) => {
                if let Some(num) = extract_val(val, context, &curr_document)? {
                    acc += num;
                }
            }
        };
    }

    Ok((curr_document, Value::Number(acc)))
}

fn extract_val<'a>(
    val: &Value,
    context: &'a EvalContext,
    doc: &CoreDocument,
) -> Result<Option<f64>, EvalErrorKind> {
    match val {
        Value::Number(num) => Ok(Some(*num)),
        Value::Integer(num) => Ok(Some(*num as f64)),
        Value::String(_)
        | Value::EvalError(_)
        | Value::Blank
        | Value::Bool(_)
        | Value::Ref(_)
        | Value::Custom(_) => Ok(None),
        Value::Spill(spill_vals) => {
            // Get only the first value.
            let val = spill_vals
                .get(0)
                .and_then(|rows| rows.get(0))
                .ok_or_else(|| EvalErrorKind::InvalidCoordinate(context.coord.clone()))?;

            extract_val(val, context, doc)
        }
        Value::SpillRef(coord) => {
            // We don't need to evaluate this ref because if it's in a value, it should have already been evaluated.
            let cell = doc
                .sheet_at(context.coord.sheet())
                .ok_or_else(|| EvalErrorKind::InvalidCoordinate(context.coord.clone()))?
                .cell_at(coord)
                .ok_or_else(|| EvalErrorKind::InvalidCoordinate(context.coord.clone()))?;

            extract_val(cell.value(), context, doc)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{eval_cell, test_utils};
    use sheeit_storage::location::{Coordinate, SheetCoordinate};
    use sheeit_storage::Value;

    #[test]
    fn sum_with_overlapping_refs_and_vals_args() {
        let overlapping_cells = vec![
            vec![
                Some("1".to_string()),
                // SKIPS
                None,
                Some("".to_string()),
                Some("hello".to_string()),
                Some("2".to_string()),
                Some("3".to_string()),
            ],
            vec![
                Some("2".to_string()),
                // DO NOT COUNT
                Some("99999".to_string()),
            ],
            vec![
                Some("3".to_string()),
                // DO NOT COUNT
                Some("99999".to_string()),
            ],
        ];

        let e1 = SheetCoordinate::new(0, 4).unwrap();

        let (context, doc) = test_utils::setup(e1.clone(), |document| {
            document
                .insert_cell_facts(&test_utils::SHEET1_A1_COORD, overlapping_cells.clone())
                .unwrap();

            document
                .insert_cell_facts(
                    &Coordinate::new_with_coord(0, e1.clone()).unwrap(),
                    vec![vec![Some("=SUM(A1:C1, A1:A6, 1,3)".to_string())]],
                )
                .unwrap();
        });

        let (_, e1_eval) =
            eval_cell(&context, doc, test_utils::create_default_eval_handle()).unwrap();

        assert_eq!(
            e1_eval,
            Value::Number((1 + 2 + 3 + 1 + 2 + 3 + 1 + 3) as f64)
        )
    }

    #[test]
    fn sum_with_more_complicated_references() {
        let col_a = vec![
            // Ref A1
            Some("1".to_string()),
            // Left
            Some("1".to_string()),
            // Straddle
            Some("=SUM(A4:D4, 1, 1)".to_string()),
            Some("1".to_string()),
        ];

        let col_b = vec![];

        let col_c = vec![
            // On operation.
            Some("1".to_string()),
            None,
            None,
            Some("2".to_string()),
        ];

        let col_d = vec![
            // Right
            Some("3".to_string()),
            None,
            None,
            Some("3".to_string()),
        ];

        let cells = vec![col_a, col_b, col_c, col_d];

        let a3 = SheetCoordinate::new(2, 0).unwrap();

        let (context, doc) = test_utils::setup(a3.clone(), |document| {
            document
                .insert_cell_facts(&test_utils::SHEET1_A1_COORD, cells.clone())
                .unwrap();
        });

        let (_, a3_eval) =
            eval_cell(&context, doc, test_utils::create_default_eval_handle()).unwrap();

        assert_eq!(a3_eval, Value::Number(8.0));
    }

    // TODO: More tests around SpillRefs etc.
}
