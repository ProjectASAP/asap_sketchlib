use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use sketchlib_rust::{SketchInput, sketches::StructuredCountMin};

fn insert_benchmark(c: &mut Criterion) {
    let updates: Vec<SketchInput<'static>> = (0..5_000)
        .map(|i| SketchInput::U64((i % 1_024) as u64))
        .collect();

    let mut group = c.benchmark_group("structured_countmin_insert");

    group.bench_function("insert", |b| {
        b.iter_batched(
            || StructuredCountMin::with_dimensions(3, 4_096),
            |mut sketch| {
                for value in &updates {
                    sketch.insert(value);
                }
                black_box(sketch);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("fast_insert", |b| {
        b.iter_batched(
            || StructuredCountMin::with_dimensions(3, 4_096),
            |mut sketch| {
                for value in &updates {
                    sketch.fast_insert(value);
                }
                black_box(sketch);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn estimate_benchmark(c: &mut Criterion) {
    let updates: Vec<SketchInput<'static>> = (0..5_000)
        .map(|i| SketchInput::U64((i % 1_024) as u64))
        .collect();
    let queries: Vec<SketchInput<'static>> = (0..256)
        .map(|i| SketchInput::U64((i * 17 % 1_024) as u64))
        .collect();

    let mut group = c.benchmark_group("structured_countmin_estimate");

    group.bench_function("estimate", |b| {
        b.iter_batched(
            || {
                let mut sketch = StructuredCountMin::with_dimensions(3, 4_096);
                for value in &updates {
                    sketch.insert(value);
                }
                sketch
            },
            |sketch| {
                for query in &queries {
                    black_box(sketch.estimate(query));
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("fast_estimate", |b| {
        b.iter_batched(
            || {
                let mut sketch = StructuredCountMin::with_dimensions(3, 4_096);
                for value in &updates {
                    sketch.fast_insert(value);
                }
                sketch
            },
            |sketch| {
                for query in &queries {
                    black_box(sketch.fast_estimate(query));
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    structured_countmin_benches,
    insert_benchmark,
    estimate_benchmark
);
criterion_main!(structured_countmin_benches);
