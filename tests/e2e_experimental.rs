//! E2E suites for feature-gated (`experimental`) sketches: KMV cardinality,
//! UniformSampling, CocoSketch over-attribution bounds and its unbiasedness and
//! recall properties, Elastic heavy-flow tracking, and the EHUnivOptimized
//! exact map tier.
//!
//! Compiled only under `--features experimental`.

#![cfg(feature = "experimental")]

mod common;

use common::{assert_between, uniform_u64, zipf_u64};

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
    let got_aaa = coco.estimate_substring("aaa");
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

/// The light layer carries every flow the heavy part evicted, so its
/// dimensions set the error on those flows. Section 4.1 discusses picking the
/// depth; this checks the choice actually reaches the estimate.
#[test]
fn elastic_light_dimensions_set_the_error_on_evicted_flows() {
    const TARGET: i32 = 50;
    const BG_KEYS: usize = 20_000;

    // 8 heavy buckets against 20k distinct flows evicts the target every time.
    let evicted_estimate = |rows: usize, cols: usize| {
        let mut sk =
            Elastic::<asap_sketchlib::DefaultXxHasher>::init_with_dimensions(8, rows, cols);
        for _ in 0..TARGET {
            sk.insert("flow::target".to_string());
        }
        for i in 0..BG_KEYS {
            sk.insert(format!("bg::{i}"));
        }
        assert!(
            !sk.heavy.iter().any(|b| b.flow_id == "flow::target"),
            "the target must be evicted for this to measure the light layer"
        );
        sk.query("flow::target".to_string())
    };

    let narrow = evicted_estimate(1, 64);
    let wide = evicted_estimate(3, 4096);

    // Elastic never underestimates, whatever the light layer costs in error.
    assert!(narrow >= TARGET, "narrow light underestimated: {narrow}");
    assert!(wide >= TARGET, "wide light underestimated: {wide}");

    // Measured 379 against 50: a 1x64 light collides ~7.6x worse than 3x4096.
    assert!(
        wide <= TARGET * 2,
        "a 3x4096 light should stay near the truth, got {wide} for {TARGET}"
    );
    assert!(
        narrow >= wide * 4,
        "light dimensions must reach the estimate: 1x64 gave {narrow}, 3x4096 gave {wide}"
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

/// Elastic's estimator is one-sided: a resident flow reports its own votes, a
/// displaced flow keeps its full size in the light layer, and every other flow
/// reads a Count-Min over-estimate. No flow may come back short.
#[test]
fn elastic_never_underestimates_under_eviction_pressure() {
    // A small heavy table against a wide key space forces repeated takeovers.
    let mut sk = Elastic::<asap_sketchlib::DefaultXxHasher>::init_with_length(16);
    let mut truth: HashMap<String, i64> = HashMap::new();

    for (i, key) in zipf_u64(60_000, 4_000, 1.1, 909).into_iter().enumerate() {
        let id = if i % 500 == 0 {
            "flow::elephant".to_string()
        } else {
            format!("flow::{key}")
        };
        sk.insert(id.clone());
        *truth.entry(id).or_insert(0) += 1;
    }

    let mut evicted_seen = 0usize;
    for (id, count) in &truth {
        let est = sk.query(id.clone()) as i64;
        assert!(
            est >= *count,
            "elastic underestimated {id}: got {est}, true {count}"
        );
        if sk.heavy.iter().any(|b| b.flow_id == *id) {
            continue;
        }
        evicted_seen += 1;
    }
    assert!(
        evicted_seen > 0,
        "the workload must actually push flows out of the heavy part"
    );

    let elephant = sk.query("flow::elephant".to_string()) as i64;
    let true_elephant = truth["flow::elephant"];
    assert_between(
        elephant as f64,
        true_elephant as f64,
        true_elephant as f64 * 1.05,
        "elastic elephant",
    );
}

/// CocoSketch attributes every increment to exactly one bucket, so the paper's
/// point query partitions the stream: summing it over the observed keys returns
/// the total inserted mass, never more.
#[test]
fn coco_point_queries_partition_the_inserted_mass() {
    let mut coco = Coco::<asap_sketchlib::DefaultXxHasher>::init_with_size(128, 3);
    let mut truth: HashMap<String, u64> = HashMap::new();
    let mut total = 0u64;

    for key in zipf_u64(40_000, 2_000, 1.2, 4242) {
        let id = format!("key::{key}");
        coco.insert(&id, 2);
        *truth.entry(id).or_insert(0) += 2;
        total += 2;
    }

    let attributed: u64 = truth.keys().map(|k| coco.estimate_key(k)).sum();
    assert_eq!(
        attributed, total,
        "point queries must partition the inserted mass"
    );

    // Heavy keys hold their own bucket, so their estimates track the truth.
    let mut ranked: Vec<(&String, &u64)> = truth.iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
    for (key, count) in ranked.iter().take(10) {
        let est = coco.estimate_key(key);
        assert_between(
            est as f64,
            **count as f64 * 0.5,
            **count as f64 * 1.5,
            &format!("coco heavy key {key}"),
        );
    }
}

// CocoSketch's accuracy theorems (Theorem 3 error bound, Theorem 4 recall) are
// stated for the hardware-friendly variant of section 4.2, which this crate does
// not implement, and neither transfers to the basic sketch as a floor. Recall
// runs the other way: the hardware variant updates each of the d mapped buckets
// independently, so a flow gets d chances to be recorded, while the basic sketch
// updates only the smallest and records the flow in at most one bucket. Measured
// at l=8, d=2, the basic sketch recalls 0.1350 against a Theorem 4 bound of
// 0.1440. So the test below asserts the paper's own worked 99% figure at the
// paper's own configuration, not a bound recomputed from the table.

/// Section 3.2 claims stochastic variance minimization "yields unbiased size
/// estimation", and Theorem 1 gives the per-bucket update distribution that
/// makes it so. Unbiasedness is a statement about the mean, not any one run.
#[test]
fn coco_point_estimates_are_unbiased_under_heavy_eviction() {
    const TRIALS: usize = 800;
    const BG_KEYS: usize = 200;
    const BG_WEIGHT: u64 = 10;
    const TARGET: u64 = 20;

    // 201 flows over 64 buckets: the target is evicted outright in roughly a
    // third of the runs, and the surviving runs must overshoot to compensate.
    let keys: Vec<String> = (0..BG_KEYS).map(|i| format!("bg::{i}")).collect();
    let mut estimates: Vec<u64> = Vec::with_capacity(TRIALS);

    for _ in 0..TRIALS {
        let mut coco = Coco::<asap_sketchlib::DefaultXxHasher>::init_with_size(32, 2);
        let mut sent = 0u64;
        for (i, key) in keys.iter().enumerate() {
            for _ in 0..BG_WEIGHT {
                coco.insert(key, 1);
            }
            // spread the target's packets evenly through the background stream
            while sent * (BG_KEYS as u64) < TARGET * (i as u64 + 1) {
                coco.insert("flow::target", 1);
                sent += 1;
            }
        }
        while sent < TARGET {
            coco.insert("flow::target", 1);
            sent += 1;
        }
        estimates.push(coco.estimate_key("flow::target"));
    }

    let dropped = estimates.iter().filter(|est| **est == 0).count();
    assert!(
        dropped > TRIALS / 10,
        "the table must actually evict the target sometimes, dropped {dropped}/{TRIALS}"
    );

    let mean = estimates.iter().map(|est| *est as f64).sum::<f64>() / TRIALS as f64;
    // the mean's standard error here is ~0.55, so this band is over 5 sigma
    assert_between(
        mean,
        TARGET as f64 - 3.0,
        TARGET as f64 + 3.0,
        "coco mean point estimate over independent runs",
    );
}

/// Theorem 4 bounds how often a flow is recorded at all. Section 5.3 works the
/// bound at d=2, l=900 for a heavy hitter holding 1% of the traffic and reads
/// off a 99% recall target; this reproduces that operating point.
#[test]
fn coco_recall_meets_the_papers_heavy_hitter_target() {
    const TRIALS: usize = 200;
    const BG_KEYS: usize = 5_000;
    const HEAVY: u64 = 51;
    const WIDTH: usize = 900;
    const DEPTH: usize = 2;

    // Theorem 4: P[Z(e) = 1] >= 1 - (1 + l * f(e) / f_bar(e))^-d. The bound is
    // read off the configuration, so it cannot be the assertion target -- it
    // would sink with the table. It only confirms the setup reaches section
    // 5.3's worked case, and TARGET_RECALL is what the run must clear.
    const TARGET_RECALL: f64 = 0.99;
    let ratio = HEAVY as f64 / BG_KEYS as f64;
    let bound = 1.0 - (1.0 + WIDTH as f64 * ratio).powi(-(DEPTH as i32));
    assert!(
        bound >= TARGET_RECALL,
        "this configuration does not reach the paper's 99% case: bound {bound:.4}"
    );

    let keys: Vec<String> = (0..BG_KEYS).map(|i| format!("bg::{i}")).collect();
    let mut recorded = 0usize;

    for _ in 0..TRIALS {
        let mut coco = Coco::<asap_sketchlib::DefaultXxHasher>::init_with_size(WIDTH, DEPTH);
        let mut sent = 0u64;
        for (i, key) in keys.iter().enumerate() {
            coco.insert(key, 1);
            while sent * (BG_KEYS as u64) < HEAVY * (i as u64 + 1) {
                coco.insert("flow::heavy", 1);
                sent += 1;
            }
        }
        while sent < HEAVY {
            coco.insert("flow::heavy", 1);
            sent += 1;
        }
        if coco.recorded_flows().any(|(key, _)| key == "flow::heavy") {
            recorded += 1;
        }
    }

    let recall = recorded as f64 / TRIALS as f64;
    assert!(
        recall >= TARGET_RECALL,
        "coco recall {recall:.4} below the paper's {TARGET_RECALL} target ({recorded}/{TRIALS})"
    );
}
