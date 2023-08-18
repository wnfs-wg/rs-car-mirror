use criterion::{criterion_group, criterion_main, Criterion};

pub fn add_benchmark(c: &mut Criterion) {
    c.bench_function("todo", |b| {
        b.iter(|| {
            todo!("Add benchmark")
        })
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
