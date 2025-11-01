use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sketchlib_rust::{CountMin, SketchInput};

const SAMPLE_COUNT: usize = 16_384;
const RNG_SEED: u64 = 0x5eed_c0de_1234_5678;

fn build_keys() -> Vec<SketchInput<'static>> {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    (0..SAMPLE_COUNT)
        .map(|_| SketchInput::U64(rng.random::<u64>()))
        .collect()
}

fn bench_countmin(c: &mut Criterion) {
    let keys = build_keys();
    let mut group = c.benchmark_group("countmin_default");

    group.bench_function("insert_only", |b| {
        b.iter_with_setup(CountMin::default, |mut sketch| {
            for key in &keys {
                sketch.insert(key);
            }
            black_box(sketch);
        });
    });

    group.bench_function("fast_insert_only", |b| {
        b.iter_with_setup(CountMin::default, |mut sketch| {
            for key in &keys {
                sketch.fast_insert(key);
            }
            black_box(sketch);
        });
    });

    let mut insert_prefilled = CountMin::default();
    for key in &keys {
        insert_prefilled.insert(key);
    }

    let mut fast_prefilled = CountMin::default();
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

criterion_group!(countmin_benches, bench_countmin);
criterion_main!(countmin_benches);
