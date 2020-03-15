use clap::{App, Arg};
use rand;
use rand::Rng;
use sheeit::storage::{Coordinate, SheetCoordinate, Storage};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

fn main() {
    let matches =
        App::new("Sheeit concurrent operations benchmarks. Can be extended for other use cases.")
            .version("0.1.0")
            .author("Wilfred Wee <wilfred.wee@outlook.com>")
            .about("Tests read and write throughputs on highly concurrent environment.")
            .arg(
                Arg::with_name("readers")
                    .short("r")
                    .long("readers")
                    .help("Number of concurrent readers")
                    .takes_value(true)
                    .required(true),
            )
            .arg(
                Arg::with_name("writers")
                    .short("w")
                    .long("writers")
                    .help("Number of concurrent writers")
                    .takes_value(true)
                    .required(true),
            )
            .get_matches();

    let readers = matches
        .value_of("readers")
        .unwrap()
        .parse::<u64>()
        .expect("Readers should be a valid non-negative integer.");
    let writers = matches
        .value_of("writers")
        .unwrap()
        .parse::<u64>()
        .expect("Writers should be a valid non-negative integer.");

    let uuid = generate_doc();

    let duration = Duration::from_secs(10);
    let start = Instant::now();
    let end = start + duration;

    let mut read_handles = vec![];
    let mut write_handles = vec![];

    for _ in 0..readers {
        let handle = thread::spawn(move || {
            let mut ops = 0;
            let uuid = uuid;

            while Instant::now() < end {
                let doc = Storage::obtain().read(&uuid).unwrap();
                let _ = doc
                    .sheet_at(0)
                    .unwrap()
                    .cell_at(&SheetCoordinate::new(0, 0).unwrap())
                    .unwrap();

                ops += 1;
            }

            ops
        });

        read_handles.push(handle);
    }

    for _ in 0..writers {
        let handle = thread::spawn(move || {
            let mut ops = 0;
            let uuid = uuid;

            while Instant::now() < end {
                let (_, _) = Storage::obtain()
                    .transact_write(uuid, |doc| {
                        doc.insert_cell_facts(
                            &Coordinate::new(0, 0, 0).unwrap(),
                            vec![vec![Some("=NOW()".to_string())]],
                        )
                        .expect("Inserting should be successful");

                        // Make the writer do at least as much work at the reader.
                        let _ = doc
                            .sheet_at(0)
                            .unwrap()
                            .cell_at(&SheetCoordinate::new(1, 1).unwrap())
                            .unwrap();
                    })
                    .expect("Writing to doc should be successful");

                ops += 1;
            }

            ops
        });

        write_handles.push(handle);
    }

    let read_ops = read_handles.into_iter().fold(0, |mut acc, handle| {
        acc += handle.join().unwrap();
        acc
    });

    let write_ops = write_handles.into_iter().fold(0, |mut acc, handle| {
        acc += handle.join().unwrap();
        acc
    });

    let reads_per_second = read_ops / duration.as_secs();
    let writes_per_second = write_ops / duration.as_secs();

    println!(
        r##"
        Readers:      {}
        Writers:      {}
        Reads/s:      {}
        Writes/s:     {}
        Total Reads:  {}
        Total Writes: {}
    "##,
        readers, writers, reads_per_second, writes_per_second, read_ops, write_ops
    );
}

fn generate_random_num() -> String {
    let value = rand::thread_rng().gen_range(0, 100);

    value.to_string()
}

// Consider making these parameters.
const COLS_NUM: usize = 10;
const ROWS_NUM: u64 = 1000;

fn generate_doc() -> Uuid {
    let storage = Storage::obtain();
    let uuid = storage.add_ledger();

    let (write_result, _) = storage
        .transact_write(uuid, |doc| {
            doc.add_sheet();

            for col in 0..COLS_NUM - 1 {
                let rows_vals = (0..ROWS_NUM)
                    .map(|_row_index| Some(generate_random_num()))
                    .collect::<Vec<_>>();

                doc.insert_cell_facts(&Coordinate::new(0, 0, col).unwrap(), vec![rows_vals])
                    .unwrap();
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
                    .map(|index| Some(format!("=SUM(A{}:I{})", index + 1, index + 1)))
                    .collect();

                doc.insert_cell_facts(
                    &Coordinate::new(0, start as usize, COLS_NUM - 1).unwrap(),
                    vec![rows],
                )
                .expect("Insert formula should succeed");
            })
            .expect("Write formula should succeed");

        handles.push(write_result);
    }

    for handle in handles {
        handle.eval_handle.join().expect("Eval should succeed");
    }

    uuid
}
