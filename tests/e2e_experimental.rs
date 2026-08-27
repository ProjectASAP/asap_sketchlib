//! E2E suites for feature-gated (`experimental`) sketches: KMV cardinality
//! and shard merge, UniformSampling's retention rate and same-rate merge, and
//! the EHUnivOptimized exact map tier.
//!
//! CocoSketch and the Elastic sketch are feature-gated too, but they are a
//! family of their own; `tests/e2e_keyed_buckets.rs` covers them.
//!
//! Compiled only under `--features experimental`.

#![cfg(feature = "experimental")]

mod common;

use common::{assert_between, uniform_u64};

use asap_sketchlib::{DataInput, EHUnivOptimized, KMV, UniformSampling};
use std::collections::HashMap;

#[test]
fn kmv_cardinality_and_shard_merge() {
    let mut a = KMV::<asap_sketchlib::DefaultXxHasher>::new(4096);
    let mut b = KMV::<asap_sketchlib::DefaultXxHasher>::new(4096);
    let mut full = KMV::<asap_sketchlib::DefaultXxHasher>::new(4096);

    let stream = uniform_u64(60_000, 500_000, 5001);
    let truth: std::collections::HashSet<u64> = stream.iter().copied().collect();
    let t = truth.len() as f64;

    for (i, k) in stream.iter().enumerate() {
        full.insert(&DataInput::U64(*k));
        if i % 2 == 0 {
            a.insert(&DataInput::U64(*k));
        } else {
            b.insert(&DataInput::U64(*k));
        }
    }

    // KMV standard error ~ 1/sqrt(k-1); allow 4x that for single-seed runs.
    assert_between(full.estimate(), t * 0.96, t * 1.04, "KMV cardinality");

    a.merge(&mut b);
    let merged = a.estimate();
    assert_between(merged, t * 0.96, t * 1.04, "KMV after shard merge");
}

/// Cardinality accuracy at checkpoints spanning the exact regime (below `k`)
/// and the estimated regime (above `k`), single-pass and after an even/odd
/// shard merge.
#[test]
fn kmv_accuracy_across_cardinality_checkpoints() {
    const CHECKPOINTS: [usize; 6] = [10, 100, 1_000, 10_000, 100_000, 1_000_000];
    const TOL: f64 = 0.02;

    let mut single = KMV::<asap_sketchlib::DefaultXxHasher>::new(4096);
    let mut even = KMV::<asap_sketchlib::DefaultXxHasher>::new(4096);
    let mut odd = KMV::<asap_sketchlib::DefaultXxHasher>::new(4096);
    let mut inserted = 0usize;

    for &target in &CHECKPOINTS {
        while inserted < target {
            let d = DataInput::U64(inserted as u64);
            single.insert(&d);
            if inserted % 2 == 0 {
                even.insert(&d);
            } else {
                odd.insert(&d);
            }
            inserted += 1;
        }

        let t = target as f64;
        assert_between(
            single.estimate(),
            t * (1.0 - TOL),
            t * (1.0 + TOL),
            &format!("KMV cardinality @ {target}"),
        );

        let mut merged = even.clone();
        let mut rhs = odd.clone();
        merged.merge(&mut rhs);
        assert_between(
            merged.estimate(),
            t * (1.0 - TOL),
            t * (1.0 + TOL),
            &format!("KMV shard merge @ {target}"),
        );
    }
}

#[test]
fn uniform_sampling_rate_and_merge() {
    let rate = 0.1f64;
    let mut us = UniformSampling::with_seed(rate, 42);
    let stream: Vec<f64> = uniform_u64(10_000, u32::MAX as u64, 5002)
        .into_iter()
        .map(|v| v as f64)
        .collect();
    for v in &stream {
        us.update(*v);
    }

    assert_eq!(us.total_seen(), 10_000, "total_seen must count every input");
    // target_size uses ceil, so retained is around n*rate within Poisson slack.
    assert_between(us.len() as f64, 850.0, 1150.0, "retained sample count");
    for s in us.samples().iter() {
        assert!(
            stream.contains(s),
            "sample {s} not drawn from the input stream"
        );
    }

    // Merging two same-rate sketches unions the samples and sums totals.
    let mut other = UniformSampling::with_seed(rate, 43);
    let other_stream: Vec<f64> = uniform_u64(5_000, u32::MAX as u64, 5003)
        .into_iter()
        .map(|v| v as f64 + 0.5)
        .collect();
    for v in &other_stream {
        other.update(*v);
    }
    us.merge(&other).expect("same-rate merge");
    assert_eq!(us.total_seen(), 15_000, "merge must sum totals");
    assert!(
        us.len() <= 1500 + 160,
        "retained samples bounded by combined budget"
    );
}
#[test]
fn eh_univ_optimized_map_tier_exact_windows() {
    let window = 100u64;
    let mut eh = EHUnivOptimized::with_defaults(2, window);

    for t in 0..150u64 {
        eh.update(t, &DataInput::U32((t % 10) as u32), (t as i64 % 3) + 1);
    }

    // Interval fully inside the retained range: map tier answers EXACTLY.
    match eh.query_interval(120, 149) {
        Some(asap_sketchlib::EHUnivQueryResult::Map {
            freq_map,
            total_count,
        }) => {
            let expect_total: usize = (120..=149u64).map(|t| (t as i64 % 3 + 1) as usize).sum();
            assert_eq!(total_count, expect_total, "interval total");
            let mut expect_freq: HashMap<u32, i64> = HashMap::new();
            for t in 120..=149 {
                *expect_freq.entry((t % 10) as u32).or_insert(0) += (t as i64 % 3) + 1;
            }
            assert_eq!(
                expect_freq.len(),
                freq_map.len(),
                "distinct keys in interval"
            );
            for (k, v) in expect_freq {
                assert_eq!(
                    freq_map.get(&asap_sketchlib::HeapItem::U32(k)),
                    Some(&v),
                    "interval count for key {k}"
                );
            }
        }
        _ => panic!("expected exact Map-tier result"),
    }
}
