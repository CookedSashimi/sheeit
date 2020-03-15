use cpuprofiler::PROFILER;
use criterion::profiler::Profiler;
use criterion::{
    criterion_group, criterion_main, AxisScale, BenchmarkId, Criterion, PlotConfiguration,
    Throughput,
};
use im::Vector;
use rand;
use rand::distributions::Alphanumeric;
use rand::Rng;
use sheeit_storage::location::Coordinate;
use sheeit_storage::{Cell, CoreDocument, Value};
use std::path::Path;

fn bench_sheet_operations(c: &mut Criterion) {
    let generate_value = || {
        let value = rand::thread_rng().gen_range(0, 1000);

        Value::Integer(value)
    };

    let column_size = 10;
    let cases: Vec<(i32, CoreDocument)> = vec![1000, 10_000, 100_000, 1_000_000]
        .iter()
        .map(|num| {
            (
                *num,
                generate_single_sheet(column_size, *num, generate_value),
            )
        })
        .collect();

    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = c.benchmark_group("insert-in-middle");
    group.plot_config(plot_config);
    for (row_num, document) in cases {
        group.throughput(Throughput::Elements(row_num as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(row_num),
            &(row_num, document),
            |b, (row_num, document)| {
                let mut document_clone = document.clone();

                let insert_index = (*row_num as usize) / 2;

                let row: Vec<_> = vec![
                    Some(Cell::new_blank(0)),
                    None,
                    Some(Cell::with_value(Value::Integer(69), 0)),
                    None,
                    Some(Cell::with_value(
                        Value::String(String::from("world").into_boxed_str()),
                        0,
                    )),
                    Some(Cell::with_value(
                        Value::String(String::from("end").into_boxed_str()),
                        0,
                    )),
                    Some(Cell::with_value(Value::Number(69.5), 0)),
                    Some(Cell::with_value(Value::Number(699999.5), 0)),
                    Some(Cell::with_value(
                        Value::String(String::from("more string").into_boxed_str()),
                        0,
                    )),
                    Some(Cell::new_blank(0)),
                ]
                .iter()
                .map(|cell| vec![cell.as_ref().cloned()])
                .collect();

                b.iter(move || {
                    document_clone.insert_row_at(0, insert_index).ok().unwrap();

                    document_clone
                        .insert_cells(&Coordinate::new(0, insert_index, 0).unwrap(), row.clone())
                        .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn generate_single_sheet<F: Fn() -> Value>(
    column_size: usize,
    row_size: i32,
    generate_value: F,
) -> CoreDocument {
    let mut document = CoreDocument::new();
    document.add_sheet();
    for col_index in 0..column_size {
        document.add_column(0).unwrap();

        let mut cells = Vec::new();
        for _j in 0..row_size {
            cells.push(Cell::with_value(generate_value(), 0))
        }

        document
            .add_cells_at_column(0, col_index, cells)
            .ok()
            .unwrap();
    }

    document
}

// TODO: Remove allow dead_code and create a separate benchmark for this.
#[allow(dead_code)]
fn bench_iteration(criterion: &mut Criterion) {
    let gen_percent_of_int = |percent: i32| {
        move || {
            let int_range = rand::thread_rng().gen_range(0, 100);
            let is_int = int_range < percent;

            if is_int {
                let value = rand::thread_rng().gen_range(0, std::i64::MAX);

                Value::Integer(value)
            } else {
                let chars: String = rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .collect();

                Value::String(chars.into_boxed_str())
            }
        }
    };

    let generate_int_vec = |num| {
        let mut vec = Vec::with_capacity(num);
        for i in 0..num {
            let _value = rand::thread_rng().gen_range(0, std::i64::MAX);
            vec.push(i);
        }

        vec
    };

    let generate_int_vector = |num| {
        let mut vec = Vector::new();
        for i in 0..num {
            let _value = rand::thread_rng().gen_range(0, std::i64::MAX);
            vec.push_back(i);
        }

        vec
    };

    let samples = vec![1000, 10_000];

    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
    let mut group = criterion.benchmark_group("Enum Vector vs Vec Iteration");
    group.plot_config(plot_config);
    for num in samples {
        group.throughput(Throughput::Elements(num as u64));
        for percent in &[100] {
            let doc = generate_single_sheet(1, num, gen_percent_of_int(*percent));
            let sheet = doc.sheet_at(0).unwrap();
            let column = sheet.column_at(0).unwrap();
            let enum_case_cells = column.cells();

            group.bench_with_input(
                BenchmarkId::new(format!("Enum {} percent:", percent), num),
                &enum_case_cells,
                |b, enum_case_cells| {
                    b.iter(|| {
                        enum_case_cells
                            .iter()
                            .fold(0 as f64, |acc, cell| match cell.value() {
                                Value::Number(i) => acc + *i,
                                _ => acc,
                            })
                    })
                },
            );
        }

        let vec_num = generate_int_vec(num as usize);

        group.bench_with_input(BenchmarkId::new("Vec:", num), &vec_num, |b, vec_num| {
            b.iter(|| vec_num.iter().fold(0, |acc, val| acc + *val))
        });

        let vector_num = generate_int_vector(num);

        group.bench_with_input(
            BenchmarkId::new("VectorIntOnly:", num),
            &vector_num,
            |b, vector_num| b.iter(|| vector_num.iter().fold(0, |acc, val| acc + *val)),
        );
    }

    group.finish();
}

struct SimpleProfiler;

impl Profiler for SimpleProfiler {
    fn start_profiling(&mut self, benchmark_id: &str, _benchmark_dir: &Path) {
        let splitted: Vec<_> = benchmark_id.split('/').collect();
        let profile_id = splitted[1];
        PROFILER
            .lock()
            .unwrap()
            .start(format!("./benchmark-profile-{}.profile", profile_id))
            .unwrap();
    }

    fn stop_profiling(&mut self, _benchmark_id: &str, _benchmark_dir: &Path) {
        PROFILER.lock().unwrap().stop().unwrap();
    }
}

fn profiled() -> Criterion {
    Criterion::default().with_profiler(SimpleProfiler)
}

criterion_group!(
name = benches;
config = profiled();
targets = bench_sheet_operations);
//criterion_group!(benches, bench_iteration);

criterion_main!(benches);
