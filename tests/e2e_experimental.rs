//! E2E suites for feature-gated (`experimental`) sketches: KMV cardinality,
//! UniformSampling, CocoSketch over-attribution bounds, Elastic heavy-flow
//! tracking, and the EHUnivOptimized exact map tier.
//!
//! Compiled only under `--features experimental`.

#![cfg(feature = "experimental")]

mod common;

use common::{assert_between, uniform_u64};

use asap_sketchlib::{Coco, DataInput, EHUnivOptimized, Elastic, KMV, UniformSampling};
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
fn coco_over_attribution_bounds_with_disjoint_prefixes() {
    // Table sized well above distinct-key count keeps eviction loss bounded;
    // Coco remains an approximate estimator either way.
    let mut coco = Coco::<asap_sketchlib::DefaultXxHasher>::init_with_size(256, 2);
    let mut truth: HashMap<String, u64> = HashMap::new();
    let mut total = 0u64;
    for i in 0..3000u64 {
        let key = format!("aaa{}", i % 50);
        coco.insert(&key, 7);
        *truth.entry("aaa".to_string()).or_insert(0) += 7;
        total += 7;
        let _ = i;
    }
    for i in 0..2000u64 {
        let key = format!("zzz{}", i % 30);
        coco.insert(&key, 3);
        *truth.entry("zzz".to_string()).or_insert(0) += 3;
        total += 3;
    }

    // Substring matching: "aaa" only matches aaa* buckets, never zzz*.
    let got_aaa = coco.estimate("aaa");
    let true_aaa = truth["aaa"];
    assert!(
        got_aaa >= true_aaa * 3 / 4 && got_aaa <= total,
        "coco 'aaa' estimate {got_aaa} outside [{}, {total}]",
        true_aaa * 3 / 4
    );

    // Exact-match UDF pins down the precise family sum.
    let exact =
        coco.estimate_with_udf("zzz", |full: &str, partial: &str| full.starts_with(partial));
    let true_zzz = truth["zzz"];
    assert!(
        exact >= true_zzz * 3 / 4 && exact <= total,
        "coco 'zzz' estimate {exact} outside [{}, {total}]",
        true_zzz * 3 / 4
    );
}

#[test]
fn elastic_tracks_hot_flows() {
    let mut sk = Elastic::<asap_sketchlib::DefaultXxHasher>::init_with_length(64);

    // Three hot flows amid background chatter.
    let mut expected: HashMap<String, i64> = HashMap::new();
    for i in 0..12_000u64 {
        let id = match i % 10 {
            0 => "hot-alpha".to_string(),
            1 => "hot-beta".to_string(),
            2 => "hot-gamma".to_string(),
            _ => format!("bg{}", i % 977),
        };
        sk.insert(id.clone());
        *expected.entry(id).or_insert(0) += 1;
    }

    for hot in ["hot-alpha", "hot-beta", "hot-gamma"] {
        let c = sk.query(hot.to_string()) as i64;
        let t = expected[hot];
        assert_between(
            c as f64,
            t as f64 * 0.80,
            t as f64 * 1.20,
            &format!("elastic flow {hot}"),
        );
    }
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
