use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sketchlib_rust::SEEDLIST;
use twox_hash::{
    xxhash3_64::Hasher as Xxh3_64, xxhash3_128::Hasher as Xxh3_128, xxhash32::Hasher as XxHash32,
    xxhash64::Hasher as XxHash64,
};

const SAMPLE_COUNT: usize = 4_096;
const PAYLOAD_LEN: usize = 64;
const RNG_SEED: u64 = 0xFEED_CAFE_DEAD_BEEF;

fn build_payloads() -> Vec<[u8; PAYLOAD_LEN]> {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    (0..SAMPLE_COUNT)
        .map(|_| {
            let mut buf = [0u8; PAYLOAD_LEN];
            rng.fill(&mut buf);
            buf
        })
        .collect()
}

fn bench_xxhash_variants(c: &mut Criterion) {
    let payloads = Arc::new(build_payloads());
    let seed64 = SEEDLIST[0];
    let seed32 = seed64 as u32;

    let mut group = c.benchmark_group("xxhash_variants");

    group.bench_function(BenchmarkId::new("xxhash32", PAYLOAD_LEN), |b| {
        let payloads = Arc::clone(&payloads);
        b.iter(|| {
            let mut acc: u32 = 0;
            for payload in payloads.iter() {
                acc ^= XxHash32::oneshot(seed32, payload);
            }
            black_box(acc);
        });
    });

    group.bench_function(BenchmarkId::new("xxhash64", PAYLOAD_LEN), |b| {
        let payloads = Arc::clone(&payloads);
        b.iter(|| {
            let mut acc: u64 = 0;
            for payload in payloads.iter() {
                acc ^= XxHash64::oneshot(seed64, payload);
            }
            black_box(acc);
        });
    });

    group.bench_function(BenchmarkId::new("xxhash3_64", PAYLOAD_LEN), |b| {
        let payloads = Arc::clone(&payloads);
        b.iter(|| {
            let mut acc: u64 = 0;
            for payload in payloads.iter() {
                acc ^= Xxh3_64::oneshot_with_seed(seed64, payload);
            }
            black_box(acc);
        });
    });

    group.bench_function(BenchmarkId::new("xxhash3_128", PAYLOAD_LEN), |b| {
        let payloads = Arc::clone(&payloads);
        b.iter(|| {
            let mut acc: u128 = 0;
            for payload in payloads.iter() {
                acc ^= Xxh3_128::oneshot_with_seed(seed64, payload);
            }
            black_box(acc);
        });
    });

    group.finish();
}

criterion_group!(hash_detail_benches, bench_xxhash_variants);
criterion_main!(hash_detail_benches);
