/// This benchmark tests and showcases the latencies on inserting + deleting a row and column in the middle of a sheet.
/// Normally, this kind of scenario is a nightmare for any database-like spreadsheet engines (or even spreadsheet engines in general)
/// This is because it involves a lot of expensive operations:
///
/// 1) Moving every element down/to-the-right
/// 2) Shifting formulas to point to the new location
/// 3) Evaluating shifted formula (We can be smarter about this, certain formulas don't need to be re-evaluated)
///
/// However, our usage of immutable data structures saves us a lot of time on 1), and we have a rather fast implementation of shifting for 2)
/// Evaluation speed however, leaves more to be desired. This is an area of improvement, as the benchmark will show.
///
/// Also, it looks like we have a performance bug on inserting columns, since it should be as fast, if not faster (because we're column-oriented)
/// then inserting rows, but that's not the case today.
use criterion::{criterion_group, AxisScale, BenchmarkId, Criterion, PlotConfiguration};
use lazy_static::lazy_static;
use rand;
use rand::Rng;
use sheeit::storage::location::Coordinate;
use sheeit::storage::Storage;

use std::thread;
use std::time::Duration;
use uuid::Uuid;

const COLUMN_SIZE: usize = 10;

type ColumnIndex = usize;
type RowIndex = usize;
type RowsSize = u64;

lazy_static! {
    static ref CASES : Vec<(u64, usize)> = {
        let mut cases = Vec::new();
        cases.push((1_000, 100));
        cases.push((10_000, 100));
        cases.push((100_000, 10));
        cases.push((1_000_000, 10));
        // cases.push((50_000_000, 10));

        cases
    };
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

    format!("=A{} + A{}", cell_row_index + 1, insert_delete_index + 3)
}

fn generate_formula_need_rewrite_for_col(cell_index: usize) -> String {
    // Very crude implementation. Should have a converter that can give us a proper A1-based formula string.
    // Based on COLUMN_SIZE = 10, where insert/delete index == 5
    // AND we insert somewhere after column index of 5
    format!("=A{} + B{}", cell_index + 1, cell_index + 1)
}

fn generate_doc(
    rows: u64,
    generate_fn: impl Fn(ColumnIndex, RowIndex, RowsSize) -> String,
) -> Uuid {
    let storage = Storage::obtain();
    let uuid = storage.add_ledger();

    let (write_result, _) = storage
        .transact_write(uuid, |doc| {
            doc.add_sheet();

            for col in 0..COLUMN_SIZE {
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

fn insert_delete_row_in_middle_no_formulas(crit: &mut Criterion) {
    structural_mutation_test_base(
        crit,
        "insert-delete-row-in-middle-no-formulas",
        |_, _, _| generate_random_num(),
        false,
        OpType::Row,
    );
}

fn insert_delete_column_in_middle_no_formulas(crit: &mut Criterion) {
    structural_mutation_test_base(
        crit,
        "insert-delete-column-in-middle-no-formulas",
        |_, _, _| generate_random_num(),
        false,
        OpType::Column,
    );
}

// TODO: Figure out how to teardown on each iteration without counting the eval step better.
fn insert_delete_row_in_middle_1000_formulas_no_eval(crit: &mut Criterion) {
    structural_mutation_test_base(
        crit,
        "insert-delete-row-in-middle-1000-formulas-no-eval",
        |col_index, row_index, rows| {
            let row_to_insert_then_delete = (rows / 2) as usize;

            if row_index < 1000 && col_index == 7 {
                generate_formula_need_rewrite_for_row(row_to_insert_then_delete, row_index as usize)
            } else {
                generate_random_num()
            }
        },
        false,
        OpType::Row,
    );
}

fn insert_delete_row_in_middle_1000_formulas_with_eval(crit: &mut Criterion) {
    structural_mutation_test_base(
        crit,
        "insert-delete-row-in-middle-1000-formulas-with-eval",
        |col_index, row_index, rows| {
            let row_to_insert_then_delete = (rows / 2) as usize;

            if row_index < 1000 && col_index == 7 {
                generate_formula_need_rewrite_for_row(row_to_insert_then_delete, row_index as usize)
            } else {
                generate_random_num()
            }
        },
        true,
        OpType::Row,
    );
}

// TODO: Figure out how to teardown on each iteration without counting the eval step better.
fn insert_delete_column_in_middle_1000_formulas_no_eval(crit: &mut Criterion) {
    structural_mutation_test_base(
        crit,
        "insert-delete-column-in-middle-1000-formulas-no-eval",
        |col_index, row_index, _rows| {
            if row_index < 1000 && col_index == 7 {
                generate_formula_need_rewrite_for_col(row_index as usize)
            } else {
                generate_random_num()
            }
        },
        false,
        OpType::Column,
    );
}

fn insert_delete_column_in_middle_1000_formulas_with_eval(crit: &mut Criterion) {
    structural_mutation_test_base(
        crit,
        "insert-delete-column-in-middle-1000-formulas-with-eval",
        |col_index, row_index, _rows| {
            if row_index < 1000 && col_index == 7 {
                generate_formula_need_rewrite_for_col(row_index as usize)
            } else {
                generate_random_num()
            }
        },
        true,
        OpType::Column,
    );
}

#[derive(Debug, Clone, Copy)]
enum OpType {
    Row,
    Column,
}

fn structural_mutation_test_base(
    crit: &mut Criterion,
    benchmark_name: &str,
    fact_generator: fn(ColumnIndex, RowIndex, RowsSize) -> String,
    eval_after_write: bool,
    op_type: OpType,
) {
    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = crit.benchmark_group("structural_mutation");
    group.plot_config(plot_config);

    for (row_num, sample_size) in CASES.iter() {
        let row_to_insert_then_delete = (row_num / 2) as usize;
        let col_to_insert_then_delete = (COLUMN_SIZE / 2) as usize;
        let uuid = generate_doc(*row_num, fact_generator);

        group.sample_size(*sample_size);
        group.bench_with_input(
            BenchmarkId::new(benchmark_name, row_num),
            row_num,
            |bencher, _row_num| {
                bencher.iter(move || {
                    let (result1, _) = Storage::obtain()
                        .transact_write(uuid, |doc| {
                            match op_type {
                                OpType::Row => doc
                                    .insert_row_at(0, row_to_insert_then_delete)
                                    .expect("Should succeed"),
                                OpType::Column => doc
                                    .insert_column_at(0, col_to_insert_then_delete)
                                    .expect("Should succeed"),
                            };

                            ()
                        })
                        .expect("Should succeed.");

                    let (result2, _) = Storage::obtain()
                        .transact_write(uuid, |doc| {
                            match op_type {
                                OpType::Row => {
                                    doc.delete_row_at(0, row_to_insert_then_delete)
                                        .expect("Should succeed");
                                }
                                OpType::Column => {
                                    doc.delete_column_at(0, col_to_insert_then_delete)
                                        .expect("Should succeed");
                                }
                            };

                            ()
                        })
                        .expect("Should succeed.");

                    if eval_after_write {
                        result1.eval_handle.join().expect("Should succeed.");
                        result2.eval_handle.join().expect("Should succeed.");
                    }
                });
            },
        );

        thread::spawn(move || {
            thread::sleep(Duration::from_secs(2));
            Storage::obtain().delete(&uuid);
        });
    }
}

criterion_group! {
    name = structural_mutation;
    config = Criterion::default();
    targets = insert_delete_row_in_middle_no_formulas,
    insert_delete_column_in_middle_no_formulas,
    insert_delete_row_in_middle_1000_formulas_no_eval,
    insert_delete_row_in_middle_1000_formulas_with_eval,
    insert_delete_column_in_middle_1000_formulas_no_eval,
    insert_delete_column_in_middle_1000_formulas_with_eval
}
