use rand;
use rand::Rng;
use sheeit::storage::{Coordinate, Storage};

fn generate_random_num() -> String {
    let value = rand::thread_rng().gen_range(0, 100);

    value.to_string()
}

#[test]
fn test_infinite_transitive_dependents() {
    let storage = Storage::obtain();
    let uuid = storage.add_ledger();
    let rows_num = 100;
    let cols_num = 5;

    let (write_result, _) = storage
        .transact_write(uuid, |doc| {
            doc.add_sheet();

            for col in 0..cols_num - 1 {
                let rows_vals = (0..rows_num)
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
    let step = (rows_num / 16) as usize;
    for row_index in (0..rows_num).step_by(step) {
        let start = row_index;
        let end = row_index + step as u64;

        let (write_result, _) = storage
            .transact_write(uuid, |doc| {
                let rows: Vec<_> = (start..end)
                    .map(|index| Some(format!("=SUM(A{}:I{})", index, index + 1)))
                    .collect();

                doc.insert_cell_facts(
                    &Coordinate::new(0, start as usize, cols_num - 1).unwrap(),
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
}
