use pretty_assertions::assert_eq;
use sheeit::storage::Storage;

use rand::Rng;
use sheeit::storage::location::{CellRange, Coordinate};
use sheeit::storage::{EvalErrorVal, Value};
use std::thread;
use uuid::Uuid;

fn into_some_cells(cells: Vec<Vec<&str>>) -> Vec<Vec<Option<String>>> {
    cells
        .into_iter()
        .map(|rows| rows.into_iter().map(|e| Some(e.to_string())).collect())
        .collect::<Vec<_>>()
}

#[test]
fn test_multiple_inserts_deletes_gets_consistent_result() {
    let storage = Storage::obtain();
    let uuid = storage.add_ledger();

    let cells = into_some_cells(vec![
        //Ref A1
        vec!["=100%", "=A1", "=A1"],
        vec!["=A1"],
        vec!["=A1"],
    ]);

    let (result, _) = storage
        .transact_write(uuid, |document| {
            document.add_sheet();

            document
                .insert_cell_facts(&Coordinate::new(0, 0, 0).unwrap(), cells.clone())
                .unwrap();

            document.delete_column_at(0, 1).unwrap();
            document.delete_row_at(0, 1).unwrap();

            document.insert_column_at(0, 1).unwrap();
            document.insert_row_at(0, 1).unwrap();

            ()
        })
        .unwrap();

    result
        .eval_handle
        .join()
        .unwrap_or_else(|_| panic!("Failed to join"));

    let doc = storage.read(&uuid).unwrap();

    let cells = doc
        .sheet_at(0)
        .unwrap()
        .owned_cells_in_range(CellRange::new(0, 2, 0, 2).unwrap())
        .into_iter()
        .map(|rows| {
            rows.into_iter()
                .map(|cell| cell.map(|inner| inner.value().clone()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let expected = vec![
        vec![
            Some(Value::Number(1.0)),
            Some(Value::Blank),
            Some(Value::Number(1.0)),
        ],
        vec![],
        vec![Some(Value::Number(1.0))],
    ];

    assert_eq!(cells, expected);
}

#[test]
fn test_eval_consistency_on_write_before_eval_on_insert_column() {
    let storage = Storage::obtain();
    let uuid = storage.add_ledger();

    let col_a = vec![
        // Ref A1
        Some("=SUM(1)".to_string()),
        // Left
        Some("=A1".to_string()),
        // Straddle
        Some("=SUM(A4:C4, A1, C1)".to_string()),
        Some("=1".to_string()),
    ];

    let col_b = vec![
        // On operation.
        Some("=A1".to_string()),
        None,
        None,
        Some("=2".to_string()),
    ];

    let col_c = vec![
        // Right
        Some("=SUM(A1, 2)".to_string()),
        None,
        None,
        Some("=3".to_string()),
    ];

    let (result1, _) = storage
        .transact_write(uuid, |document| {
            document.add_sheet();

            document
                .insert_cell_facts(
                    &Coordinate::new(0, 0, 0).unwrap(),
                    vec![col_a.clone(), col_b.clone(), col_c.clone()],
                )
                .unwrap();

            ()
        })
        .unwrap();

    let (result2, _) = storage
        .transact_write(uuid, |document| {
            document.insert_column_at(0, 1).unwrap();

            ()
        })
        .unwrap();

    result2
        .eval_handle
        .join()
        .unwrap_or_else(|e| panic!("Failed join, error: {:#?}", e));

    result1
        .eval_handle
        .join()
        .unwrap_or_else(|e| panic!("Failed join, error: {:#?}", e));

    let document = storage.read(&uuid).unwrap();

    let vals = document
        .sheet_at(0)
        .unwrap()
        .owned_cells_in_range(CellRange::new(0, 2, 0, 3).unwrap())
        .iter()
        .map(|rows| {
            rows.iter()
                .map(|cell_opt| cell_opt.as_ref().map(|cell| cell.value().clone()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        vals,
        vec![
            vec![
                Some(Value::Number(1.0)),
                Some(Value::Number(1.0)),
                Some(Value::Number(10.0))
            ],
            vec![],
            vec![
                Some(Value::Number(1.0)),
                Some(Value::Blank),
                Some(Value::Blank)
            ],
            vec![
                Some(Value::Number(3.0)),
                Some(Value::Blank),
                Some(Value::Blank)
            ]
        ]
    )
}

#[test]
fn test_eval_consistency_on_write_before_eval_on_delete_column() {
    let storage = Storage::obtain();
    let uuid = storage.add_ledger();

    let col_a = vec![
        // Ref A1
        Some("=SUM(1)".to_string()),
        // Left
        Some("=A1".to_string()),
        // Straddle
        Some("=SUM(A4:C4, A1, C1)".to_string()),
        Some("=1".to_string()),
    ];

    let col_b = vec![
        // On operation.
        Some("=A1".to_string()),
        None,
        None,
        Some("=2".to_string()),
    ];

    let col_c = vec![
        // Right
        Some("=SUM(A1, 2)".to_string()),
        // Referring to cell on operation.
        Some("=SUM(A1, B1, 2)".to_string()),
        None,
        Some("=3".to_string()),
    ];

    let (result1, _) = storage
        .transact_write(uuid, |document| {
            document.add_sheet();

            document
                .insert_cell_facts(
                    &Coordinate::new(0, 0, 0).unwrap(),
                    vec![col_a.clone(), col_b.clone(), col_c.clone()],
                )
                .unwrap();

            ()
        })
        .unwrap();

    let (result2, _) = storage
        .transact_write(uuid, |document| {
            document.delete_column_at(0, 1).unwrap();

            ()
        })
        .unwrap();

    result2
        .eval_handle
        .join()
        .unwrap_or_else(|e| panic!("Failed join, error: {:#?}", e));

    result1
        .eval_handle
        .join()
        .unwrap_or_else(|e| panic!("Failed join, error: {:#?}", e));

    let document = storage.read(&uuid).unwrap();

    let vals = document
        .sheet_at(0)
        .unwrap()
        .owned_cells_in_range(CellRange::new(0, 2, 0, 1).unwrap())
        .iter()
        .map(|rows| {
            rows.iter()
                .map(|cell_opt| cell_opt.as_ref().map(|cell| cell.value().clone()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        vals,
        vec![
            vec![
                Some(Value::Number(1.0)),
                Some(Value::Number(1.0)),
                Some(Value::Number(8.0))
            ],
            vec![
                Some(Value::Number(3.0)),
                Some(Value::EvalError(EvalErrorVal::UnsupportedExpression)),
                Some(Value::Blank)
            ]
        ]
    )
}

#[test]
fn test_eval_consistency_on_write_before_eval_on_insert_row() {
    let storage = Storage::obtain();
    let uuid = storage.add_ledger();

    let ref_a1 = vec![Some("=SUM(1)".to_string())];
    let below = vec![None, None, Some("=A1".to_string())];
    let straddle = vec![Some("=SUM(C1:C2, A1)".to_string()), Some("=3".to_string())];
    let on_operation = vec![None, Some("=A1".to_string())];
    let above = vec![Some("=A1".to_string())];

    let cells = vec![ref_a1, below, straddle, on_operation, above];

    let (result1, _) = storage
        .transact_write(uuid, |document| {
            document.add_sheet();

            document
                .insert_cell_facts(&Coordinate::new(0, 0, 0).unwrap(), cells.clone())
                .unwrap();

            ()
        })
        .unwrap();

    let (result2, _) = storage
        .transact_write(uuid, |document| {
            document.insert_row_at(0, 1).unwrap();

            // Add to the new C2.
            document
                .insert_cell_facts(
                    &Coordinate::new(0, 0, 0).unwrap(),
                    vec![vec![], vec![], vec![None, Some("=2".to_string())]],
                )
                .unwrap();

            ()
        })
        .unwrap();

    result2
        .eval_handle
        .join()
        .unwrap_or_else(|_| panic!("Failed join"));

    result1
        .eval_handle
        .join()
        .unwrap_or_else(|_| panic!("Failed join"));

    let document = storage.read(&uuid).unwrap();

    let vals = document
        .sheet_at(0)
        .unwrap()
        .owned_cells_in_range(CellRange::new(0, 3, 0, 4).unwrap())
        .iter()
        .map(|rows| {
            rows.iter()
                .map(|cell_opt| cell_opt.as_ref().map(|cell| cell.value().clone()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        vals,
        vec![
            vec![Some(Value::Number(1.0)),],
            vec![
                Some(Value::Blank),
                Some(Value::Blank),
                Some(Value::Blank),
                Some(Value::Number(1.0)),
            ],
            vec![
                Some(Value::Number(6.0)),
                Some(Value::Number(2.0)),
                Some(Value::Number(3.0)),
            ],
            vec![
                Some(Value::Blank),
                Some(Value::Blank),
                Some(Value::Number(1.0)),
            ],
            vec![Some(Value::Number(1.0)),],
        ]
    )
}

#[test]
fn test_eval_consistency_on_write_before_eval_on_delete_row() {
    let storage = Storage::obtain();
    let uuid = storage.add_ledger();

    let ref_a1 = vec![Some("=SUM(1)".to_string())];
    let below = vec![None, None, Some("=A1".to_string())];
    let straddle = vec![
        Some("=SUM(C1:C3, A1)".to_string()),
        Some("=2".to_string()),
        Some("=3".to_string()),
    ];
    let on_operation = vec![None, Some("=A1".to_string())];
    let above = vec![Some("=A1".to_string())];

    let cells = vec![ref_a1, below, straddle, on_operation.clone(), above];

    let (result1, _) = storage
        .transact_write(uuid, |document| {
            document.add_sheet();

            document
                .insert_cell_facts(&Coordinate::new(0, 0, 0).unwrap(), cells.clone())
                .unwrap();

            ()
        })
        .unwrap();

    let (result2, _) = storage
        .transact_write(uuid, |document| {
            document.delete_row_at(0, 1).unwrap();

            ()
        })
        .unwrap();

    result2
        .eval_handle
        .join()
        .unwrap_or_else(|_| panic!("Failed join"));

    result1
        .eval_handle
        .join()
        .unwrap_or_else(|_| panic!("Failed join"));

    let document = storage.read(&uuid).unwrap();

    let vals = document
        .sheet_at(0)
        .unwrap()
        .owned_cells_in_range(CellRange::new(0, 2, 0, 4).unwrap())
        .iter()
        .map(|rows| {
            rows.iter()
                .map(|cell_opt| cell_opt.as_ref().map(|cell| cell.value().clone()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        vals,
        vec![
            vec![Some(Value::Number(1.0)),],
            vec![Some(Value::Blank), Some(Value::Number(1.0)),],
            vec![Some(Value::Number(4.0)), Some(Value::Number(3.0)),],
            vec![Some(Value::Blank),],
            vec![Some(Value::Number(1.0)),],
        ]
    )
}

fn generate_random_num() -> String {
    let value = rand::thread_rng().gen_range(0, 1000);

    value.to_string()
}

fn generate_formula_need_rewrite_for_row(
    insert_delete_index: usize,
    cell_row_index: usize,
) -> String {
    // Very crude implementation. Should have a converter that can give us a proper A1-based formula string.

    format!("=A{} + A{}", cell_row_index + 1, insert_delete_index + 1)
}

fn generate_doc(rows: u64, generate_fn: impl Fn(usize, usize, u64) -> String) -> Uuid {
    let storage = Storage::obtain();
    let uuid = storage.add_ledger();

    let (write_result, _) = storage
        .transact_write(uuid, |doc| {
            doc.add_sheet();

            for col in 0..10 {
                let rows_vals = (0..rows)
                    .map(|row_index| Some(generate_fn(col, row_index as usize, rows)))
                    .collect::<Vec<_>>();

                doc.insert_cell_facts(&Coordinate::new(0, 0, col).unwrap(), vec![rows_vals])
                    .unwrap();
                ()
            }
        })
        .expect("Should succeed");

    write_result
        .eval_handle
        .join()
        .expect("Eval should succeed");

    uuid
}

#[test]
fn test_benching_bug() {
    let row_num = 10;

    let row_to_insert_then_delete = (row_num / 2) as usize;
    let uuid = generate_doc(row_num, |col_index, row_index, _rows| {
        if row_index < 1000 && col_index == 9 {
            generate_formula_need_rewrite_for_row(row_to_insert_then_delete, row_index)
        } else {
            generate_random_num()
        }
    });

    let mut handles = vec![];
    for _ in 0..100 {
        handles.push(thread::spawn(move || {
            let (result, _) = Storage::obtain()
                .transact_write(uuid, |doc| {
                    doc.insert_row_at(0, row_to_insert_then_delete).unwrap();

                    ()
                })
                .expect("Should succeed.");

            result.eval_handle.join().expect("Should succeed.");

            let (result, _) = Storage::obtain()
                .transact_write(uuid, |doc| {
                    doc.delete_row_at(0, row_to_insert_then_delete).unwrap();

                    ()
                })
                .expect("Should succeed.");

            result.eval_handle.join().expect("Should succeed.");
        }));
    }

    handles
        .into_iter()
        .for_each(|handle| handle.join().expect("Should succeed."));
}

fn generate_doc_refers_to(
    rows: u64,
    col_num: usize,
    generate_fn: impl Fn(usize, usize, u64) -> String,
) -> Uuid {
    let storage = Storage::obtain();
    let uuid = storage.add_ledger();

    let (write_result, _) = storage
        .transact_write(uuid, |doc| {
            doc.add_sheet();

            for col in 0..col_num {
                let rows_vals = (0..rows)
                    .map(|row_index| Some(generate_fn(col, row_index as usize, rows)))
                    .collect::<Vec<_>>();

                doc.insert_cell_facts(&Coordinate::new(0, 0, col).unwrap(), vec![rows_vals])
                    .unwrap();
                ()
            }
        })
        .expect("Should succeed");

    write_result
        .eval_handle
        .join()
        .expect("Eval should succeed");

    uuid
}

// TODO: Handle child thread panics. No idea why joins are not failing the tests???
#[test]
fn test_benching_bug_refers_to() {
    let uuid = generate_doc_refers_to(1000, 2, |col_index, row_index, _rows_size| {
        if col_index == 0 {
            generate_random_num()
        } else {
            format!("=SUM(A{}:A{})", row_index, row_index + 1)
        }
    });

    let (result, _) = Storage::obtain()
        .transact_write(uuid, |doc| {
            doc.insert_cell_facts(
                &Coordinate::new(0, 0, 5).unwrap(),
                vec![vec![Some(format!("=SUM(A1:A{})", 1000))]],
            )
            .expect("Should succeed");

            ()
        })
        .expect("Should succeed");

    result.eval_handle.join().expect("TODO handle panics here.")
}
