//! E2E frequency-sketch pipelines on synthetic streams with exact ground
//! truth: CountMin (regular/fast + shard merge), CountSketch turnstile,
//! top-k heaps, CountL2HH weighted F2, FoldCMS/FoldCS counts and hierarchical
//! merge, portable wire twins (string keys).

mod common;

use common::{FreqTruth, uniform_u64, zipf_u64};
use std::collections::HashMap;

use asap_sketchlib::message_pack_format::portable::countminsketch::CountMinSketch;
use asap_sketchlib::message_pack_format::portable::countsketch::CountSketch;
use asap_sketchlib::{
    CMSHeap, CSHeap, CountL2HH, CountMin, DataInput, DefaultXxHasher, FastPath, FoldCMS, FoldCS,
    HeapItem, RegularPath, Vector2D,
};

// ----------------------------------------------------------------- CountMin

#[test]
fn countmin_zipf_one_sided_bound_and_shard_merge() {
    let (rows, cols) = (4usize, 4096usize);
    let stream = zipf_u64(100_000, 8192, 1.1, 1001);

    let mut single = CountMin::<Vector2D<i64>, FastPath>::with_dimensions(rows, cols);
    let (mut shard_a, mut shard_b, mut shard_c) = (
        CountMin::<Vector2D<i64>, FastPath>::with_dimensions(rows, cols),
        CountMin::<Vector2D<i64>, FastPath>::with_dimensions(rows, cols),
        CountMin::<Vector2D<i64>, FastPath>::with_dimensions(rows, cols),
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
    let merged = &shard_a;

    // One-sided guarantee: est >= true, excess within eps*N (eps = e/cols).
    let eps_n = std::f64::consts::E / cols as f64 * truth.total() as f64;
    for (k, c) in truth.pairs() {
        if c < 50 {
            continue; // only assert keys dense enough to be meaningful
        }
        let est_single = single.estimate(&DataInput::I64(k));
        let est_merged = merged.estimate(&DataInput::I64(k));
        assert!(
            est_single >= c && est_merged >= c,
            "underestimate: key {k} true {c} single {est_single} merged {est_merged}"
        );
        assert!(
            (est_single - c) as f64 <= eps_n && (est_merged - c) as f64 <= eps_n,
            "excess beyond eps*N: key {k} true {c} single {est_single} merged {est_merged}"
        );
    }

    // Shard-merge must equal the single-pass estimate on hot keys.
    for (k, _) in truth.top_k(50) {
        let a = single.estimate(&DataInput::I64(k));
        let b = merged.estimate(&DataInput::I64(k));
        assert_eq!(a, b, "merged counters diverge from single pass for key {k}");
    }
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

/// Counts keys whose estimate sits within `bound` of the exact count.
fn keys_within_bound<F>(truth: &FreqTruth, estimate: F, bound: f64) -> usize
where
    F: Fn(i64) -> f64,
{
    truth
        .pairs()
        .into_iter()
        .filter(|(k, c)| (estimate(*k) - *c as f64).abs() < bound)
        .count()
}

/// Bounded integer draws mapped onto distinct f64 values in `[100, 1000)`,
/// identified by bit pattern so exact counts stay comparable.
fn uniform_f64_key(v: u64) -> i64 {
    (100.0 + v as f64 * (900.0 / 4096.0)).to_bits() as i64
}

fn f64_input(key: i64) -> DataInput<'static> {
    DataInput::F64(f64::from_bits(key as u64))
}

fn u64_input(key: i64) -> DataInput<'static> {
    DataInput::U64(key as u64)
}

/// A named key stream paired with the `DataInput` constructor for its keys.
type BoundStream = (&'static str, Vec<i64>, fn(i64) -> DataInput<'static>);

/// Zipf over `u64` keys and uniform over `f64` keys, exercising both
/// `DataInput` hashing paths.
fn bound_streams() -> [BoundStream; 2] {
    [
        (
            "zipf/u64",
            zipf_u64(BOUND_N, 8192, 1.1, 1005)
                .into_iter()
                .map(|v| v as i64)
                .collect(),
            u64_input as fn(i64) -> DataInput<'static>,
        ),
        (
            "uniform/f64",
            uniform_u64(BOUND_N, 4096, 1006)
                .into_iter()
                .map(uniform_f64_key)
                .collect(),
            f64_input as fn(i64) -> DataInput<'static>,
        ),
    ]
}

const BOUND_ROWS: usize = 3;
const BOUND_COLS: usize = 4096;
const BOUND_N: usize = 200_000;

/// Probabilistic bound with `eps = e / cols` and `delta = e^-rows`: at least a
/// `1 - delta` fraction of keys estimate within `eps * N`, on both insert paths.
#[test]
fn countmin_error_bound_covers_most_keys_on_both_paths() {
    for (name, keys, to_input) in bound_streams() {
        let mut truth = FreqTruth::default();
        let mut regular =
            CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(BOUND_ROWS, BOUND_COLS);
        let mut fast = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(BOUND_ROWS, BOUND_COLS);
        for k in &keys {
            let d = to_input(*k);
            truth.observe(*k);
            regular.insert(&d);
            fast.insert(&d);
        }

        let eps_n = std::f64::consts::E / BOUND_COLS as f64 * BOUND_N as f64;
        let floor =
            truth.distinct() as f64 * (1.0 - 1.0 / std::f64::consts::E.powi(BOUND_ROWS as i32));
        for (path, within) in [
            (
                "regular",
                keys_within_bound(&truth, |k| regular.estimate(&to_input(k)) as f64, eps_n),
            ),
            (
                "fast",
                keys_within_bound(&truth, |k| fast.estimate(&to_input(k)) as f64, eps_n),
            ),
        ] {
            assert!(
                within as f64 > floor,
                "CountMin {name}/{path}: {within} of {} keys within eps*N={eps_n:.0}, need > {floor:.1}",
                truth.distinct()
            );
        }
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

/// The same `1 - delta` coverage bound as CountMin, for the signed sketch.
#[test]
fn countsketch_error_bound_covers_most_keys_on_both_paths() {
    for (name, keys, to_input) in bound_streams() {
        let mut truth = FreqTruth::default();
        let mut regular = asap_sketchlib::Count::<Vector2D<i32>, RegularPath>::with_dimensions(
            BOUND_ROWS, BOUND_COLS,
        );
        let mut fast = asap_sketchlib::Count::<Vector2D<i32>, FastPath>::with_dimensions(
            BOUND_ROWS, BOUND_COLS,
        );
        for k in &keys {
            let d = to_input(*k);
            truth.observe(*k);
            regular.insert(&d);
            fast.insert(&d);
        }

        let eps_n = std::f64::consts::E / BOUND_COLS as f64 * BOUND_N as f64;
        let floor =
            truth.distinct() as f64 * (1.0 - 1.0 / std::f64::consts::E.powi(BOUND_ROWS as i32));
        for (path, within) in [
            (
                "regular",
                keys_within_bound(&truth, |k| regular.estimate(&to_input(k)), eps_n),
            ),
            (
                "fast",
                keys_within_bound(&truth, |k| fast.estimate(&to_input(k)), eps_n),
            ),
        ] {
            assert!(
                within as f64 > floor,
                "CountSketch {name}/{path}: {within} of {} keys within eps*N={eps_n:.0}, need > {floor:.1}",
                truth.distinct()
            );
        }
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

/// Sixteen sub-window sketches folded to a quarter width, then hierarchically
/// merged: the merged answers must still satisfy the `1 - delta` coverage
/// bound, additive `eps * N` for FoldCMS and `sqrt(eps) * L2` for FoldCS.
#[test]
fn fold_sketches_survive_a_sixteen_way_hierarchical_merge() {
    const ROWS: usize = 3;
    const FULL_COLS: usize = 4096;
    const FOLD_LEVEL: u32 = 4;
    const TOP_K: usize = 20;
    const WINDOWS: usize = 16;
    const N: usize = 200_000;

    let stream = zipf_u64(N, 10_000, 1.1, 1009);
    let per_window = N / WINDOWS;
    let mut truth = FreqTruth::default();
    let mut cms_windows = Vec::with_capacity(WINDOWS);
    let mut cs_windows = Vec::with_capacity(WINDOWS);

    for w in 0..WINDOWS {
        let mut cms = FoldCMS::<DefaultXxHasher>::new(ROWS, FULL_COLS, FOLD_LEVEL, TOP_K);
        let mut cs = FoldCS::<DefaultXxHasher>::new(ROWS, FULL_COLS, FOLD_LEVEL, TOP_K);
        for &v in &stream[w * per_window..(w + 1) * per_window] {
            let d = DataInput::U64(v);
            cms.insert(&d, 1);
            cs.insert(&d, 1);
            truth.observe(v as i64);
        }
        cms_windows.push(cms);
        cs_windows.push(cs);
    }

    let cms_merged = FoldCMS::hierarchical_merge(&cms_windows);
    let cs_merged = FoldCS::hierarchical_merge(&cs_windows);

    let floor = truth.distinct() as f64 * (1.0 - 1.0 / std::f64::consts::E.powi(ROWS as i32));
    let cms_bound = std::f64::consts::E / FULL_COLS as f64 * truth.total() as f64;
    let cs_bound = (std::f64::consts::E / FULL_COLS as f64).sqrt() * truth.l2_norm();

    let cms_within = keys_within_bound(
        &truth,
        |k| cms_merged.query(&DataInput::U64(k as u64)) as f64,
        cms_bound,
    );
    assert!(
        cms_within as f64 > floor,
        "FoldCMS 16-way merge: {cms_within} of {} keys within eps*N={cms_bound:.0}, need > {floor:.1}",
        truth.distinct()
    );

    let cs_within = keys_within_bound(
        &truth,
        |k| cs_merged.query(&DataInput::U64(k as u64)) as f64,
        cs_bound,
    );
    assert!(
        cs_within as f64 > floor,
        "FoldCS 16-way merge: {cs_within} of {} keys within sqrt(eps)*L2={cs_bound:.0}, need > {floor:.1}",
        truth.distinct()
    );
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
