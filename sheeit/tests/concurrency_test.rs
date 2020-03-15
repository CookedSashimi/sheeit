mod util;

use core::cmp;

use prettytable;
use prettytable::{Row, Table};
use rand;
use rand::Rng;
use sheeit::storage::Storage;
use sheeit::storage::{Cell, CoreDocument, Value};

use std::sync::Arc;
use std::thread;

use std::time::{Duration, Instant};
use util::multi_value::MultiValue;
use uuid::Uuid;

#[test]
fn test_multiple_readers_and_writers() {
    let storage = Arc::new(Storage::obtain());

    let handles: Vec<_> = (0..4)
        .map(|_i| {
            let storage_clone = Arc::clone(&storage);
            thread::spawn(move || {
                //                println!("Generating from index: {}", i);

                storage_clone.add_ledger()
            })
        })
        .collect();

    let document_uuids: Arc<Vec<Uuid>> = Arc::new(
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect(),
    );

    let write_handles: Vec<_> = (0..4)
        .map(|_i| {
            let storage_clone = Arc::clone(&storage);
            let document_uuids_clone = Arc::clone(&document_uuids);

            thread::spawn(move || {
                let start = Instant::now();

                loop {
                    let random_index = rand::thread_rng().gen_range(0, document_uuids_clone.len());
                    let uuid = document_uuids_clone.get(random_index).unwrap();

                    let (_, ()) = storage_clone
                        .transact_write(*uuid, |document| {
                            if document.sheets().len() == 0 {
                                document.add_sheet();
                            }

                            let sheet = document.sheet_at(0).unwrap();

                            let column_index = if sheet.columns().len() == 0
                                || rand::thread_rng().gen_range(0, 2) == 1
                            {
                                document.add_column(0).unwrap();
                                0
                            } else {
                                rand::thread_rng().gen_range(0, sheet.columns().len())
                            };

                            let rows_to_write = 0..rand::thread_rng().gen_range(0, 10);
                            //                            println!("Will write {} rows", rows_to_write.len());
                            for _ in rows_to_write {
                                let mut multi_val = MultiValue::new();
                                multi_val.add_val(Value::Integer(
                                    rand::thread_rng().gen_range(100, 1000),
                                ));
                                multi_val.add_val(Value::Integer(
                                    rand::thread_rng().gen_range(100, 1000),
                                ));

                                let value = Value::Custom(Box::new(multi_val));

                                //                                println!("Column value before adding:");
                                //                                for cell in column.cells() {
                                ////                                    println!("Cell: {}", cell);
                                ////                                }

                                //                                println!("Adding value: {}", value);

                                document
                                    .add_cells_at_column(
                                        0,
                                        column_index,
                                        vec![Cell::with_value(value, 0)],
                                    )
                                    .ok()
                                    .unwrap();

                                //                                println!("Column value after adding:");
                                //                                for cell in column.cells() {
                                //                                    println!("Cell: {}", cell);
                                //                                }
                            }

                            ()
                        })
                        .unwrap();
                    //                    println!("Thread {} is done writing", i);

                    let now = Instant::now();
                    if now - start > Duration::from_millis(100) {
                        //                    println!("=== BEGIN Document FROM THREAD: {}, with UUID: {} ===", i, uuid);
                        //                    print_document(document);
                        //                    println!("=== END Document FROM TRHEAD: {}, with UUID: {} ===", i, uuid);
                        break;
                    }
                }
            })
        })
        .collect();

    let read_handles: Vec<_> = (0..4)
        .map(|i| {
            let storage_clone = Arc::clone(&storage);
            let document_uuids_clone = Arc::clone(&document_uuids);

            thread::spawn(move || {
                thread::sleep(Duration::from_millis(100));
                let random_index = rand::thread_rng().gen_range(0, document_uuids_clone.len());
                let uuid = document_uuids_clone.get(random_index).unwrap();

                let document = storage_clone.read(&uuid).unwrap();
                let start = Instant::now();

                loop {
                    print_document_stat(i, &document, false);
                    print_document_stat(i, &storage_clone.read(&uuid).unwrap(), true);

                    thread::sleep(Duration::from_millis(10));

                    let now = Instant::now();
                    if now - start > Duration::from_millis(200) {
                        break;
                    }
                }
            })
        })
        .collect();

    for handle in write_handles {
        handle.join().unwrap();
    }

    for handle in read_handles {
        handle.join().unwrap();
    }

    //    for uuid in document_uuids.iter() {
    //        let document = storage.read(&uuid).unwrap();
    //        print_document(document);
    //    }
}

fn print_document_stat(_i: i32, document: &CoreDocument, _is_new: bool) {
    let sheets = document.sheets();
    let mut result = String::new();

    let sheet = sheets.get(0).expect("No sheets!");
    let columns = sheet.columns();
    let first_column = columns.get(0).unwrap();

    for cell in first_column.cells() {
        result = result + &format!("FirstColCellVal: {}\n", cell);
    }
    //
    //        print!(
    //            "{}: Thread no.: {}\n{}",
    //            if is_new { "NEW" } else { "STALE " },
    //            i,
    //            result
    //        );
}

#[allow(dead_code)]
fn print_document(document: CoreDocument) {
    for sheet in document.sheets() {
        let mut table = Table::new();
        let max_rows = sheet
            .columns()
            .iter()
            .fold(0, |acc, col| cmp::max(acc, col.cells().len()));

        for i in 0..max_rows {
            let mut cells = Vec::new();
            for column in sheet.columns() {
                cells.push(
                    column
                        .cells()
                        .get(i)
                        .map(|cell| prettytable::Cell::new(&format!("{}", cell)))
                        .unwrap_or(prettytable::Cell::new("")),
                )
            }

            table.add_row(Row::new(cells));
        }

        table.printstd();
    }
}
