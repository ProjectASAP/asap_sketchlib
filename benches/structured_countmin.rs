use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use sketchlib_rust::{
    SketchInput,
    sketches::{StructuredCount, StructuredCountMin, VectorCountMin},
};

fn insert_benchmark(c: &mut Criterion) {
    let updates: Vec<SketchInput<'static>> = (0..5_000)
        .map(|i| SketchInput::U64((i % 1_024) as u64))
        .collect();

    let mut group = c.benchmark_group("countmin_insert");

    group.bench_function("matrix_insert", |b| {
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

    group.bench_function("matrix_fast_insert", |b| {
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

    group.bench_function("vector_insert", |b| {
        b.iter_batched(
            || VectorCountMin::with_dimensions(3, 4_096),
            |mut sketch| {
                for value in &updates {
                    sketch.insert(value);
                }
                black_box(sketch);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("vector_fast_insert", |b| {
        b.iter_batched(
            || VectorCountMin::with_dimensions(3, 4_096),
            |mut sketch| {
                for value in &updates {
                    sketch.fast_insert(value);
                }
                black_box(sketch);
            },
            BatchSize::SmallInput,
        );
    });

    // group.bench_function("classic_insert", |b| {
    //     b.iter_batched(
    //         || StructuredCount::default(),
    //         |mut sketch| {
    //             for value in &updates {
    //                 sketch.insert_with_count(value);
    //             }
    //             black_box(sketch);
    //         },
    //         BatchSize::SmallInput,
    //     );
    // });

    group.finish();
}

fn estimate_benchmark(c: &mut Criterion) {
    let updates: Vec<SketchInput<'static>> = (0..5_000)
        .map(|i| SketchInput::U64((i % 1_024) as u64))
        .collect();
    let queries: Vec<SketchInput<'static>> = (0..256)
        .map(|i| SketchInput::U64((i * 17 % 1_024) as u64))
        .collect();

    let mut group = c.benchmark_group("countmin_estimate");

    group.bench_function("matrix_estimate", |b| {
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

    group.bench_function("matrix_fast_estimate", |b| {
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

    group.bench_function("vector_estimate", |b| {
        b.iter_batched(
            || {
                let mut sketch = VectorCountMin::with_dimensions(3, 4_096);
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

    group.bench_function("vector_fast_estimate", |b| {
        b.iter_batched(
            || {
                let mut sketch = VectorCountMin::with_dimensions(3, 4_096);
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

    // group.bench_function("classic_estimate", |b| {
    //     b.iter_batched(
    //         || {
    //             let mut sketch = Count::init_count_with_rc(3, 4_096);
    //             for value in &updates {
    //                 sketch.insert_count(value);
    //             }
    //             sketch
    //         },
    //         |sketch| {
    //             for query in &queries {
    //                 black_box(sketch.get_est(query));
    //             }
    //         },
    //         BatchSize::SmallInput,
    //     );
    // });

    group.finish();
}

criterion_group!(countmin_benches, insert_benchmark, estimate_benchmark);
criterion_main!(countmin_benches);
