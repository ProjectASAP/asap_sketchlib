use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sketchlib_rust::{Count, SketchInput};

const SAMPLE_COUNT: usize = 16_384;
const RNG_SEED: u64 = 0x5eed_c0de_1234_5678;
const DEFAULT_ROW_NUM: usize = 5;
const DEFAULT_COL_NUM: usize = 32768;

fn build_keys() -> Vec<SketchInput<'static>> {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    (0..SAMPLE_COUNT)
        .map(|_| SketchInput::U64(rng.random::<u64>()))
        .collect()
}

fn bench_count(c: &mut Criterion) {
    let keys = build_keys();
    let mut group = c.benchmark_group("count_default");

    group.bench_function("insert_only", |b| {
        b.iter_with_setup(
            || Count::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM),
            |mut sketch| {
                for key in &keys {
                    sketch.insert(key);
                }
                black_box(sketch);
            },
        );
    });

    group.bench_function("fast_insert_only", |b| {
        b.iter_with_setup(
            || Count::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM),
            |mut sketch| {
                for key in &keys {
                    sketch.fast_insert(key);
                }
                black_box(sketch);
            },
        );
    });

    let mut insert_prefilled = Count::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM);
    for key in &keys {
        insert_prefilled.insert(key);
    }

    let mut fast_prefilled = Count::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM);
    for key in &keys {
        fast_prefilled.fast_insert(key);
    }

    group.bench_function("estimate", |b| {
        b.iter(|| {
            for key in &keys {
                black_box(insert_prefilled.estimate(key));
            }
        });
    });

    group.bench_function("fast_estimate", |b| {
        b.iter(|| {
            for key in &keys {
                black_box(fast_prefilled.fast_estimate(key));
            }
        });
    });

    group.finish();
}

criterion_group!(count_benches, bench_count);
criterion_main!(count_benches);
