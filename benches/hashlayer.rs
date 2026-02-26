use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sketchlib_rust::{
    Count, CountMin, FastPath, HyperLogLog, DataFusion, RegularPath, SketchInput, Vector2D,
    sketch_framework::hashlayer::HashLayer,
};

const SAMPLE_COUNT: usize = 10_000;
const RNG_SEED: u64 = 0x5eed_c0de_1234_5678;

fn build_keys() -> Vec<SketchInput<'static>> {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    (0..SAMPLE_COUNT)
        .map(|_| SketchInput::U64(rng.random::<u64>()))
        .collect()
}

/// Benchmark: Insert to three sketches separately (hashing 3 times per value)
fn bench_separate_insert_three_sketches(c: &mut Criterion) {
    let keys = build_keys();

    c.bench_function("separate_insert_three_sketches", |b| {
        b.iter_with_setup(
            || {
                (
                    CountMin::<Vector2D<i32>, FastPath>::default(),
                    Count::<Vector2D<i32>, RegularPath>::default(),
                    HyperLogLog::<DataFusion>::default(),
                )
            },
            |(mut cm, mut count, mut hll)| {
                for key in &keys {
                    // Each insert computes its own hash
                    cm.insert(key);
                    count.insert(key);
                    hll.insert(key);
                }
                black_box((cm, count, hll));
            },
        );
    });
}

/// Benchmark: HashLayer.insert_all (hash once, insert to all)
fn bench_hashlayer_insert_all(c: &mut Criterion) {
    let keys = build_keys();

    c.bench_function("hashlayer_insert_all", |b| {
        b.iter_with_setup(HashLayer::default, |mut layer| {
            for key in &keys {
                layer.insert_all(key);
            }
            black_box(layer);
        });
    });
}

criterion_group!(
    benches,
    bench_separate_insert_three_sketches,
    bench_hashlayer_insert_all,
);

criterion_main!(benches);
