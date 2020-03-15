/// The purpose of this benchmark is to better see the difference in latencies between:
/// 1) Just writing, without evaluating (The user's perception of lantency)
/// 2) Writing and evaluating. (The user's perception of 'work done')
///
/// There are 2 scenarios presented here:
/// 1) Writing a single SUM formula that sums the entire column
/// 2) Writing a running sum, where there is a SUM formula in each row.
///
/// The second case can be considered a worst-case scenario, because each row needs to be evaluated.
/// When you have 1M rows, it's going to take a while.
///
/// However, with concurrency evaluation, we see that the control is returned to the user quickly.
use criterion::{criterion_group, AxisScale, BatchSize, BenchmarkId, Criterion, PlotConfiguration};
use lazy_static::lazy_static;
use rand::Rng;
use sheeit::storage::location::Coordinate;
use sheeit::storage::{Storage, TransactWriteResult};

use std::mem;
use uuid::Uuid;

lazy_static! {
    static ref SINGLE_SUM_CASES : Vec<(u64, usize)> = {
        let mut cases = Vec::new();
        cases.push((1_000, 10));
        cases.push((10_000, 10));
        cases.push((100_000, 10));
        cases.push((1_000_000, 10));
        // cases.push((50_000_000, 10));

        cases
    };

    // TODO: Until we optimize running-sum case, 1M takes too long.
    static ref RUNNING_SUM_CASES : Vec<(u64, usize)> = {
        let mut cases = Vec::new();
        cases.push((1_000, 10));
        cases.push((10_000, 10));
        cases.push((100_000, 10));
        // cases.push((50_000_000, 10));

        cases
    };
}

type ColumnIndex = usize;
type RowIndex = usize;
type RowsSize = u64;

struct DropEval {
    write_result: Option<TransactWriteResult>,
    uuid: Uuid,
}

impl Drop for DropEval {
    fn drop(&mut self) {
        match self.write_result.take() {
            None => {}
            Some(write_result) => {
                write_result.eval_handle.join().expect("Should succeed");
            }
        };
        Storage::obtain().delete(&self.uuid);
    }
}

fn generate_random_num() -> String {
    let value = rand::thread_rng().gen_range(0, 100);

    value.to_string()
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

            for col in 0..1 {
                let rows_vals = (0..rows)
                    .map(|row_index| Some(generate_fn(col, row_index as usize, rows)))
                    .collect::<Vec<_>>();

                doc.insert_cell_facts(&Coordinate::new(0, 0, col).unwrap(), vec![rows_vals])
                    .unwrap();
                ()
            }
        })
        .expect("Should succeed");

    write_result.eval_handle.join().expect("Should succeed");

    uuid
}

fn evaluate_single_sum(crit: &mut Criterion) {
    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = crit.benchmark_group("evaluation-single-sum");
    group.plot_config(plot_config);

    for (row_num, sample_size) in SINGLE_SUM_CASES.iter() {
        group.sample_size(*sample_size);
        group.bench_with_input(
            BenchmarkId::new("single-sum-no-eval", row_num),
            row_num,
            |bencher, row_num| {
                bencher.iter_batched(
                    || {
                        generate_doc(*row_num, |_col_index, _row_index, _rows_size| {
                            generate_random_num()
                        })
                    },
                    |uuid| {
                        let (result1, _) = Storage::obtain()
                            .transact_write(uuid, |doc| {
                                doc.insert_cell_facts(
                                    &Coordinate::new(0, 0, 5).unwrap(),
                                    vec![vec![Some(format!("=SUM(A1:A{})", row_num))]],
                                )
                                .expect("Should succeed");

                                ()
                            })
                            .expect("Should succeed.");

                        DropEval {
                            write_result: Some(result1),
                            uuid,
                        }
                    },
                    BatchSize::LargeInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("single-sum-with-eval", row_num),
            row_num,
            |bencher, row_num| {
                bencher.iter_batched(
                    || {
                        generate_doc(*row_num, |_col_index, _row_index, _rows_size| {
                            generate_random_num()
                        })
                    },
                    |uuid| {
                        let (result1, _) = Storage::obtain()
                            .transact_write(uuid, |doc| {
                                doc.insert_cell_facts(
                                    &Coordinate::new(0, 0, 6).unwrap(),
                                    vec![vec![Some(format!("=SUM(A1:A{})", row_num))]],
                                )
                                .expect("Should succeed");

                                ()
                            })
                            .expect("Should succeed.");

                        result1.eval_handle.join().expect("Should succeed");

                        DropEval {
                            write_result: None,
                            uuid,
                        }
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
}

fn evaluate_running_sum(crit: &mut Criterion) {
    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = crit.benchmark_group("evaluation-running-sum");
    group.plot_config(plot_config);

    for (row_num, sample_size) in RUNNING_SUM_CASES.iter() {
        group.sample_size(*sample_size);
        group.bench_with_input(
            BenchmarkId::new("running-sum-no-eval", row_num),
            row_num,
            |bencher, row_num| {
                bencher.iter_batched(
                    || {
                        let doc_uuid =
                            generate_doc(*row_num, |_col_index, _row_index, _rows_size| {
                                generate_random_num()
                            });

                        let running_sums = (0..*row_num)
                            .map(|row_index| {
                                Some(format!("=SUM(A{}:A{})", row_index, row_index + 1))
                            })
                            .collect::<Vec<_>>();

                        (doc_uuid, running_sums)
                    },
                    |(uuid, mut running_sums)| {
                        let (result1, _) = Storage::obtain()
                            .transact_write(uuid, move |doc| {
                                let mut owned = vec![];
                                mem::swap(&mut running_sums, &mut owned);

                                doc.insert_cell_facts(
                                    &Coordinate::new(0, 0, 5).unwrap(),
                                    vec![owned],
                                )
                                .expect("Should succeed");

                                ()
                            })
                            .expect("Should succeed.");

                        DropEval {
                            write_result: Some(result1),
                            uuid,
                        }
                    },
                    BatchSize::LargeInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("running-sum-with-eval", row_num),
            row_num,
            |bencher, row_num| {
                bencher.iter_batched(
                    || {
                        let doc_uuid =
                            generate_doc(*row_num, |_col_index, _row_index, _rows_size| {
                                generate_random_num()
                            });

                        let running_sums = (0..*row_num)
                            .map(|row_index| {
                                Some(format!("=SUM(A{}:A{})", row_index, row_index + 1))
                            })
                            .collect::<Vec<_>>();

                        (doc_uuid, running_sums)
                    },
                    |(uuid, mut running_sums)| {
                        let (result1, _) = Storage::obtain()
                            .transact_write(uuid, |doc| {
                                let mut owned = vec![];
                                mem::swap(&mut running_sums, &mut owned);

                                doc.insert_cell_facts(
                                    &Coordinate::new(0, 0, 6).unwrap(),
                                    vec![owned],
                                )
                                .expect("Should succeed");

                                ()
                            })
                            .expect("Should succeed.");

                        result1.eval_handle.join().expect("Should succeed");

                        DropEval {
                            write_result: None,
                            uuid,
                        }
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
}

criterion_group! {
    name = evaluation;
    config = Criterion::default();
    targets = evaluate_single_sum,evaluate_running_sum
}
