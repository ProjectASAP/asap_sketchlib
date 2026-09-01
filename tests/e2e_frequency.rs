//! E2E frequency-sketch pipelines on synthetic streams with exact ground
//! truth.
//!
//! Two different error metrics live in this file and must not be confused:
//!
//! - **Count-Min** is one-sided and *additive*: `est >= f` always, and the
//!   excess is bounded by `e * (N - f) / w` with probability `1 - e^-d`.
//! - **Count Sketch** is two-sided and *L2*: the error is
//!   `sqrt(kappa / w) * ||f_-i||_2`, rank-independent, and has nothing to do
//!   with `eps * N`. A Count Sketch checked against `e/w * N` is not being
//!   checked against its own theorem — that bound is enormously looser on a
//!   skewed stream, and passing it says almost nothing.
//!
//! Both bounds live in `common::specs` alongside the binomial acceptance rules
//! that turn a per-key failure probability into a pass/fail decision.

mod common;
#[path = "e2e_frequency/count_min_variants.rs"]
mod count_min_variants;
#[path = "e2e_frequency/count_sketch_variants.rs"]
mod count_sketch_variants;

use common::FreqTruth;
use common::specs::{CountMinSpec, Tally};
use common::streams::zipf_u64;
use std::collections::HashMap;

use asap_sketchlib::message_pack_format::portable::countminsketch::CountMinSketch;
use asap_sketchlib::message_pack_format::portable::countsketch::CountSketch;
use asap_sketchlib::{
    CMSHeap, CSHeap, CountL2HH, CountMin, DataInput, DefaultXxHasher, FastPath, FoldCMS, FoldCS,
    HeapItem, RegularPath, Vector2D,
};

// ----------------------------------------------------------------- CountMin

/// Count-Min's own theorem end to end: the one-sided guarantee on every key,
/// the additive `e*(N-f)/w` excess under the binomial acceptance rule, and
/// exact equality between a shard-merged sketch and a single-pass one.
#[test]
fn countmin_fast_path_zipf_conforms_to_the_count_min_model_and_merges_shards_exactly() {
    const ROWS: usize = 4;
    const COLS: usize = 4096;
    const STREAM_SEED: u64 = 1001;

    let stream = zipf_u64(100_000, 8192, 1.1, STREAM_SEED);
    let mut single = CountMin::<Vector2D<i64>, FastPath>::with_dimensions(ROWS, COLS);
    let (mut shard_a, mut shard_b, mut shard_c) = (
        CountMin::<Vector2D<i64>, FastPath>::with_dimensions(ROWS, COLS),
        CountMin::<Vector2D<i64>, FastPath>::with_dimensions(ROWS, COLS),
        CountMin::<Vector2D<i64>, FastPath>::with_dimensions(ROWS, COLS),
    );
    let mut truth = FreqTruth::default();
    for (i, k) in stream.iter().enumerate() {
        let d = DataInput::I64(*k as i64);
        truth.observe(*k as i64);
        single.insert(&d);
        match i % 3 {
            0 => shard_a.insert(&d),
            1 => shard_b.insert(&d),
            _ => shard_c.insert(&d),
        }
    }
    shard_b.merge(&shard_c);
    shard_a.merge(&shard_b);

    let spec = CountMinSpec::new(ROWS, COLS);
    let context = format!("zipf(1.1) domain=8192 n=100000 stream_seed={STREAM_SEED}");
    spec.assert_contract(
        "CountMin<Vector2D<i64>, FastPath> single-pass",
        &truth,
        |k| single.estimate(&DataInput::I64(k)) as f64,
        &context,
    );
    spec.assert_contract(
        "CountMin<Vector2D<i64>, FastPath> 3-way shard merge",
        &truth,
        |k| shard_a.estimate(&DataInput::I64(k)) as f64,
        &context,
    );

    // Merging counter matrices is exact addition, so a merged sketch is not
    // merely close to the single-pass one — it is identical on every key.
    let mut merge_equality = Tally::default();
    for (k, _) in truth.pairs() {
        let a = single.estimate(&DataInput::I64(k));
        let b = shard_a.estimate(&DataInput::I64(k));
        merge_equality.record(a == b, || {
            format!("key {k}: single-pass {a} != shard-merged {b}")
        });
    }
    merge_equality.assert_none("CountMin merge equality", &context);
}

#[test]
fn countmin_regular_fast_paths_agree_on_stream() {
    let mut reg = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(3, 2048);
    let mut fast = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 2048);
    let stream = zipf_u64(20_000, 512, 1.2, 1002);
    for k in &stream {
        reg.insert(&DataInput::I64(*k as i64));
        fast.insert(&DataInput::I64(*k as i64));
    }
    for k in [0i64, 7, 42, 511] {
        assert_eq!(
            reg.estimate(&DataInput::I64(k)),
            fast.estimate(&DataInput::I64(k)) as i64,
            "regular/fast paths disagree for key {k}"
        );
    }
}

// -------------------------------------------------------------- CountSketch

#[test]
fn countsketch_turnstile_net_zero_and_median_bound() {
    // True turnstile: +500 then -500 must net to zero.
    let mut cs = asap_sketchlib::Count::<Vector2D<i64>, RegularPath>::with_dimensions(4, 1024);
    cs.insert_many(&DataInput::U32(9), 500);
    cs.insert_many(&DataInput::U32(9), -500);
    assert_eq!(
        cs.estimate(&DataInput::U32(9)),
        0.0,
        "turnstile did not cancel"
    );

    // Median estimator bound |est - true| <= sqrt(e/cols) * L2.
    let (rows, cols) = (5usize, 4096usize);
    let mut cs = asap_sketchlib::Count::<Vector2D<i64>, RegularPath>::with_dimensions(rows, cols);
    let stream = zipf_u64(200_000, 8192, 1.1, 1003);
    let mut truth = FreqTruth::default();
    for k in &stream {
        truth.observe(*k as i64);
        cs.insert(&DataInput::I64(*k as i64));
    }
    let bound = (std::f64::consts::E / cols as f64).sqrt() * truth.l2_norm();
    for (k, c) in truth.top_k(100) {
        let est = cs.estimate(&DataInput::I64(k));
        assert!(
            (est - c as f64).abs() <= 1.5 * bound,
            "key {k}: |{est:.0} - {c}| > 1.5 * median bound {:.0}",
            1.5 * bound
        );
    }
}

// ------------------------------------------------------------- Top-k heaps

#[test]
fn heaps_top_k_recall_and_consistency() {
    let (top_k, domain) = (16usize, 1024usize);
    let mut cms_heap = CMSHeap::<Vector2D<i64>, RegularPath>::new(3, 4096, top_k);
    let mut cs_heap = CSHeap::<Vector2D<i64>, RegularPath>::new(5, 4096, top_k);
    let stream = zipf_u64(20_000, domain, 1.1, 1004);
    let mut truth = FreqTruth::default();
    for k in &stream {
        let d = DataInput::I64(*k as i64);
        truth.observe(*k as i64);
        cms_heap.insert(&d);
        cs_heap.insert(&d);
    }
    let expected_min_count = truth.top_k(top_k)[top_k - 1].1;

    for (label, items, cms) in [
        ("CMSHeap", cms_heap.heap().heap().to_vec(), true),
        ("CSHeap", cs_heap.heap().heap().to_vec(), false),
    ] {
        assert!(items.len() <= top_k, "{label} heap exceeded capacity");
        let mut recall = 0usize;
        for it in &items {
            let key = match &it.key {
                HeapItem::I64(v) => *v,
                other => panic!("{label}: unexpected heap key {other:?}"),
            };
            let est = if cms {
                cms_heap.estimate(&DataInput::I64(key))
            } else {
                cs_heap.estimate(&DataInput::I64(key)) as i64
            };
            // Invariant: heap entry must always equal the sketch estimate.
            assert_eq!(
                it.count, est,
                "{label}: heap {} != estimate {est} for key {key}",
                it.count
            );
            if truth.get(key) >= expected_min_count {
                recall += 1;
            }
        }
        assert!(
            recall >= top_k - 1,
            "{label} recovered only {recall}/{top_k} heavy hitters"
        );
    }
}

// -------------------------------------------------------------- CountL2HH

#[test]
fn countl2hh_weighted_f2_with_decrements() {
    let mut sk = CountL2HH::<DefaultXxHasher>::with_dimensions_and_seed(4, 2048, 11);
    let mut truth = FreqTruth::default();
    let stream = zipf_u64(30_000, 512, 1.3, 1005);
    for (i, k) in stream.iter().enumerate() {
        let w = 1 + (i % 5) as i64;
        sk.fast_insert_with_count(&DataInput::I64(*k as i64), w);
        truth.observe_weighted(*k as i64, w);
    }
    // Turnstile decrement on the hottest key must track through the median path.
    let hot = truth.top_k(1)[0].0;
    sk.fast_insert_with_count(&DataInput::I64(hot), -10);
    truth.observe_weighted(hot, -10);
    let est_hot = sk.fast_update_and_est(&DataInput::I64(hot), 0);
    let rel = ((est_hot - truth.get(hot) as f64) / truth.get(hot) as f64).abs();
    assert!(
        rel <= 0.02,
        "hot-key frequency after decrement: {est_hot} vs {}",
        truth.get(hot)
    );

    assert!(
        (sk.get_l2_sqr() - truth.f2()).abs() / truth.f2() <= 0.10,
        "F2 over weighted turnstile stream off by >10%"
    );
}

// ------------------------------------------- FoldCMS / FoldCS counts + merge

#[test]
fn fold_cms_foldcs_counts_and_hierarchical_merge() {
    // Weighted per-key counts stay exact on sparse dims.
    let mut a = FoldCMS::<DefaultXxHasher>::new(3, 2048, 0, 32);
    let mut b = FoldCMS::<DefaultXxHasher>::new(3, 2048, 0, 32);
    for _ in 0..100 {
        a.insert(&DataInput::Str("alpha"), 2);
        b.insert(&DataInput::Str("alpha"), 2);
        b.insert(&DataInput::Str("beta"), 1);
    }
    assert_eq!(a.query(&DataInput::Str("alpha")), 200);
    assert_eq!(b.query(&DataInput::Str("alpha")), 200);
    assert_eq!(b.query(&DataInput::Str("beta")), 100);

    // Same-level merge sums disjoint contributions.
    a.merge_same_level(&b);
    assert_eq!(a.query(&DataInput::Str("alpha")), 400);
    assert_eq!(a.query(&DataInput::Str("beta")), 100);

    // Signed weighted updates through FoldCS.
    let mut fs = FoldCS::<DefaultXxHasher>::new(3, 2048, 0, 32);
    fs.insert(&DataInput::Str("gamma"), 60);
    fs.insert(&DataInput::Str("gamma"), -20);
    assert_eq!(fs.query(&DataInput::Str("gamma")), 40);

    // Hierarchical merge of level-matched sketches preserves totals.
    let s1 = build_fold_cms("s1", 30);
    let s2 = build_fold_cms("s2", 45);
    let merged = FoldCMS::hierarchical_merge(&[s1, s2]);
    assert_eq!(merged.query(&DataInput::Str("s1")), 90);
    assert_eq!(merged.query(&DataInput::Str("s2")), 135);
}

fn build_fold_cms(key: &'static str, n: usize) -> FoldCMS<DefaultXxHasher> {
    let mut s = FoldCMS::<DefaultXxHasher>::new(3, 2048, 0, 32);
    for _ in 0..n {
        s.insert(&DataInput::Str(key), 3);
    }
    s
}

// ------------------------------------------------------ Portable wire twins

#[test]
fn portable_cms_and_cs_string_keys_zipf_bound() {
    let stream = zipf_u64(50_000, 2048, 1.1, 1006);
    let mut truth: HashMap<String, i64> = HashMap::new();

    let mut pcs = CountMinSketch::new(3, 4096);
    let mut pcss = CountSketch::new(5, 4096);
    for k in &stream {
        let key = format!("k{k}");
        *truth.entry(key.clone()).or_insert(0) += 1;
        pcs.update(&key, 1.0);
        pcss.update(&key, 1.0);
    }
    let l2sq: f64 = truth.values().map(|c| (*c as f64) * (*c as f64)).sum();
    let l2 = l2sq.sqrt();
    let total = truth.values().sum::<i64>() as f64;
    let cm_bound = std::f64::consts::E / 4096.0 * total;
    let cs_bound = (std::f64::consts::E / 4096.0).sqrt() * l2;

    let mut by_freq: Vec<(String, i64)> = truth.into_iter().collect();
    by_freq.sort_by_key(|(_, c)| -*c);
    for (k, c) in by_freq.iter().take(60) {
        let cm_est = pcs.estimate(k);
        assert!(
            cm_est >= *c as f64 && cm_est <= *c as f64 + cm_bound,
            "portable CM key {k}: est {cm_est} vs true {c}"
        );
        let cs_est = pcss.estimate(k);
        assert!(
            (cs_est - *c as f64).abs() <= 1.5 * cs_bound,
            "portable CS key {k}: est {cs_est} vs true {c} (bound {:.0})",
            1.5 * cs_bound
        );
    }
}
