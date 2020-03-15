use criterion::criterion_main;

mod concurrency_contention;
mod evaluation;
mod structural_mutation;

criterion_main!(
    concurrency_contention::concurrency_contention,
    structural_mutation::structural_mutation,
    evaluation::evaluation
);
