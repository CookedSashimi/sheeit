/// WARNING: Benchmark is telling an incomplete story.
/// The purpose of this benchmark is to test how does write latencies suffer from high read or write contention.
/// Achieve this by spawning a variable-number of threads that either perform reads or writes.
/// Then, time how long it takes to complete 10 writes.
///
/// From the benchmark results, it looks like even at high concurrent writes, the write lantecy does not suffer.
/// This does not make sense, because there should be lock contention on the write lock.
/// My guess is, since I don't actually have 1000 threads, the OS is evenly-distributing the load to the point
/// where no difference can be seen.
///
/// This benchmark needs to be updated to better reflect write contention.
use criterion::{criterion_group, AxisScale, BenchmarkId, Criterion, PlotConfiguration};
use lazy_static::lazy_static;
use rand::Rng;
use sheeit::storage::location::Coordinate;
use sheeit::storage::{Storage, TransactWriteResult};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use std::thread;
use uuid::Uuid;

lazy_static! {
    static ref CASES: Vec<(u64, usize)> = {
        let mut cases = Vec::new();
        cases.push((1, 10));
        cases.push((10, 10));
        cases.push((100, 10));
        cases.push((1000, 10));
        cases.push((2000, 10));

        cases
    };
}

const ROWS_NUM: u64 = 100_000;
const COLS_NUM: usize = 10;

struct DropEval {
    write_result: Option<Vec<TransactWriteResult>>,
}

impl Drop for DropEval {
    fn drop(&mut self) {
        match self.write_result.take() {
            None => {}
            Some(write_results) => {
                for write_result in write_results {
                    write_result.eval_handle.join().expect("Should succeed");
                }
            }
        };
    }
}

fn generate_random_num() -> String {
    let value = rand::thread_rng().gen_range(0, 100);

    value.to_string()
}

fn generate_doc(rows: u64) -> Uuid {
    let storage = Storage::obtain();
    let uuid = storage.add_ledger();

    let (write_result, _) = storage
        .transact_write(uuid, |doc| {
            doc.add_sheet();

            for col in 0..COLS_NUM - 1 {
                let rows_vals = (0..rows)
                    .map(|_row_index| Some(generate_random_num()))
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
        .expect("First write should succeed");

    let mut handles = vec![];
    let step = (ROWS_NUM / 16) as usize;
    for row_index in (0..ROWS_NUM).step_by(step) {
        let start = row_index;
        let end = row_index + step as u64;

        let (write_result, _) = storage
            .transact_write(uuid, |doc| {
                let rows: Vec<_> = (start..end)
                    .map(|index| Some(format!("=SUM(A{}:I{})", index, index + 1)))
                    .collect();

                doc.insert_cell_facts(
                    &Coordinate::new(0, start as usize, COLS_NUM - 1).unwrap(),
                    vec![rows],
                )
                .expect("Insert formula should succeed");

                ()
            })
            .expect("Write formula should succeed");

        handles.push(write_result);
    }

    for handle in handles {
        handle.eval_handle.join().expect("Eval should succeed");
    }

    uuid
}

fn concurrency_contention_on_increasing_reads(crit: &mut Criterion) {
    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = crit.benchmark_group("concurrency-contention");
    group.plot_config(plot_config);
    let uuid = generate_doc(ROWS_NUM);

    for (readers_num, sample_size) in CASES.iter() {
        group.sample_size(*sample_size);

        group.bench_with_input(
            BenchmarkId::new("concurrency-contention-on-increasing-reads", readers_num),
            readers_num,
            |bencher, readers_num| {
                let handles = (0..*readers_num).map(|_| {
                    let atomic = Arc::new(AtomicBool::new(false));
                    let uuid = uuid.clone();

                    let atomic_clone = atomic.clone();
                    let handle = thread::spawn(move || {
                        let atomic = atomic_clone;

                        loop {
                            let doc = Storage::obtain()
                                .read(&uuid)
                                .expect("Document should be present");
                            doc.sequence();

                            if atomic.load(Ordering::SeqCst) {
                                break;
                            }
                        }
                    });

                    (atomic, handle)
                });

                bencher.iter_with_large_drop(|| {
                    // To better test lock contention, we benchmark writing 10 times.
                    let mut handles = vec![];

                    let (result1, _) = Storage::obtain()
                        .transact_write(uuid, |doc| {
                            let insert_index = ROWS_NUM / 2;

                            doc.insert_cell_facts(
                                &Coordinate::new(0, insert_index as usize, COLS_NUM - 1).unwrap(),
                                vec![vec![Some(format!(
                                    "=SUM(A{}:I{}) + {}",
                                    insert_index,
                                    insert_index + 1,
                                    generate_random_num()
                                ))]],
                            )
                            .expect("Should succeed");

                            ()
                        })
                        .expect("Should succeed.");

                    handles.push(result1);

                    DropEval {
                        write_result: Some(handles),
                    }
                });

                for (should_break, handle) in handles {
                    should_break.store(true, Ordering::SeqCst);

                    handle.join().expect("Should succeed join");
                }
            },
        );
    }

    Storage::obtain().delete(&uuid);
}

fn concurrency_contention_on_increasing_writes(crit: &mut Criterion) {
    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = crit.benchmark_group("concurrency-contention");
    group.plot_config(plot_config);
    let uuid = generate_doc(ROWS_NUM);

    for (writers_num, sample_size) in CASES.iter() {
        group.sample_size(*sample_size);
        group.bench_with_input(
            BenchmarkId::new("concurrency-contention-on-increasing-writes", writers_num),
            writers_num,
            |bencher, writers_num| {
                let handles = (0..*writers_num).map(|_| {
                    let should_break = Arc::new(AtomicBool::new(false));
                    let uuid = uuid.clone();

                    let should_break_clone = should_break.clone();
                    let handle = thread::spawn(move || {
                        let should_break = should_break_clone;

                        loop {
                            let col_index = rand::thread_rng().gen_range(11, 30);
                            let row_index = rand::thread_rng().gen_range(0, ROWS_NUM);
                            let coord = Coordinate::new(0, row_index as usize, col_index).unwrap();

                            let (write_result, _) = Storage::obtain()
                                .transact_write(uuid, move |doc| {
                                    doc.insert_cell_facts(
                                        &coord,
                                        vec![vec![Some(generate_random_num())]],
                                    )
                                    .expect("Insert background writes should succeed.");
                                    ()
                                })
                                .expect("Document should be present");

                            write_result
                                .eval_handle
                                .join()
                                .expect("Eval should succeed");

                            if should_break.load(Ordering::SeqCst) {
                                break;
                            }
                        }
                    });

                    (should_break, handle)
                });

                bencher.iter_with_large_drop(|| {
                    // To better test lock contention, we benchmark writing 10 times.
                    let mut handles = vec![];

                    let (result1, _) = Storage::obtain()
                        .transact_write(uuid, |doc| {
                            let insert_index = ROWS_NUM / 2;

                            doc.insert_cell_facts(
                                &Coordinate::new(0, insert_index as usize, COLS_NUM - 1).unwrap(),
                                vec![vec![Some(format!(
                                    "=SUM(A{}:I{}) + {}",
                                    insert_index,
                                    insert_index + 1,
                                    generate_random_num()
                                ))]],
                            )
                            .expect("Should succeed");

                            ()
                        })
                        .expect("Should succeed.");

                    handles.push(result1);

                    DropEval {
                        write_result: Some(handles),
                    }
                });

                for (should_break, handle) in handles {
                    should_break.store(true, Ordering::SeqCst);

                    handle.join().expect("Should succeed join");
                }
            },
        );
    }

    Storage::obtain().delete(&uuid);
}

criterion_group! {
    name = concurrency_contention;
    config = Criterion::default();
    targets = concurrency_contention_on_increasing_writes,concurrency_contention_on_increasing_reads
}
