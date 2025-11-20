use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sketchlib_rust::{CountMin, SketchInput};

const SAMPLE_COUNT: usize = 16_384;
const RNG_SEED: u64 = 0x5eed_c0de_cafe_f00d;

fn build_keys() -> Vec<SketchInput<'static>> {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    (0..SAMPLE_COUNT)
        .map(|_| SketchInput::U64(rng.random::<u64>()))
        .collect()
}

fn bench_nitro(c: &mut Criterion) {
    let keys = build_keys();
    let mut group = c.benchmark_group("countmin_nitro");

    group.bench_function("fast_insert", |b| {
        b.iter_with_setup(CountMin::default, |mut sketch| {
            for key in &keys {
                sketch.fast_insert(key);
            }
            black_box(sketch);
        });
    });

    group.bench_function("fast_insert_nitro_100", |b| {
        b.iter_with_setup(CountMin::default, |mut sketch| {
            sketch.enable_nitro(1.0);
            for key in &keys {
                sketch.fast_insert_nitro(key);
            }
            black_box(sketch);
        });
    });

    group.bench_function("fast_insert_nitro_70", |b| {
        b.iter_with_setup(CountMin::default, |mut sketch| {
            sketch.enable_nitro(0.7);
            for key in &keys {
                sketch.fast_insert_nitro(key);
            }
            black_box(sketch);
        });
    });

    group.bench_function("fast_insert_nitro_40", |b| {
        b.iter_with_setup(CountMin::default, |mut sketch| {
            sketch.enable_nitro(0.4);
            for key in &keys {
                sketch.fast_insert_nitro(key);
            }
            black_box(sketch);
        });
    });

    group.bench_function("fast_insert_nitro_10", |b| {
        b.iter_with_setup(CountMin::default, |mut sketch| {
            sketch.enable_nitro(0.1);
            for key in &keys {
                sketch.fast_insert_nitro(key);
            }
            black_box(sketch);
        });
    });

    group.bench_function("fast_insert_nitro_7", |b| {
        b.iter_with_setup(CountMin::default, |mut sketch| {
            sketch.enable_nitro(0.07);
            for key in &keys {
                sketch.fast_insert_nitro(key);
            }
            black_box(sketch);
        });
    });

    group.bench_function("fast_insert_nitro_4", |b| {
        b.iter_with_setup(CountMin::default, |mut sketch| {
            sketch.enable_nitro(0.04);
            for key in &keys {
                sketch.fast_insert_nitro(key);
            }
            black_box(sketch);
        });
    });

    group.bench_function("fast_insert_nitro_1", |b| {
        b.iter_with_setup(CountMin::default, |mut sketch| {
            sketch.enable_nitro(0.01);
            for key in &keys {
                sketch.fast_insert_nitro(key);
            }
            black_box(sketch);
        });
    });

    group.finish();
}

criterion_group!(nitro_benches, bench_nitro);
criterion_main!(nitro_benches);
