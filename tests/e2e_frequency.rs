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

use common::specs::{CountMinSpec, CountSketchSpec, SecondMomentSpec, Tally};
use common::{FreqTruth, uniform_u64, zipf_u64};
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
fn countmin_zipf_satisfies_the_count_min_additive_bound_and_shard_merge() {
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

/// Both hashing paths satisfy Count-Min's contract on the same stream.
///
/// They are *not* required to return equal estimates and generally do not:
/// `RegularPath` makes one hash call per row with `seed_list[r]`, while
/// `FastPath` makes a single call with `seed_list[0]` and slices row bits out
/// of it. Different hash functions place keys in different columns, so the two
/// collide on different key pairs. On a 40k-update Zipf stream at 3x4096 they
/// disagree on roughly a third of keys — every disagreement inside the bound.
/// An earlier version of this test asserted equality on four hand-picked keys,
/// which held by coincidence at those dimensions rather than by contract.
///
/// `e2e_matrix_instances.rs` covers the equality that *is* contractual: on a
/// collision-free workload both paths return exact counts.
#[test]
fn countmin_both_paths_satisfy_the_bound_on_the_same_stream() {
    const ROWS: usize = 3;
    const COLS: usize = 2048;
    const STREAM_SEED: u64 = 1002;

    let mut reg = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(ROWS, COLS);
    let mut fast = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(ROWS, COLS);
    let mut truth = FreqTruth::default();
    let stream = zipf_u64(20_000, 512, 1.2, STREAM_SEED);
    for k in &stream {
        truth.observe(*k as i64);
        reg.insert(&DataInput::I64(*k as i64));
        fast.insert(&DataInput::I64(*k as i64));
    }
    let spec = CountMinSpec::new(ROWS, COLS);
    let context = format!("zipf(1.2) domain=512 n=20000 stream_seed={STREAM_SEED}");
    spec.assert_contract(
        "CountMin<Vector2D<i64>, RegularPath>",
        &truth,
        |k| reg.estimate(&DataInput::I64(k)) as f64,
        &context,
    );
    spec.assert_contract(
        "CountMin<Vector2D<i32>, FastPath>",
        &truth,
        |k| fast.estimate(&DataInput::I64(k)) as f64,
        &context,
    );
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

const BOUND_ROWS: usize = 3;
const BOUND_COLS: usize = 4096;
const BOUND_N: usize = 120_000;
/// Independent key populations per shape. The library fixes its hash seed
/// table, so a test cannot resample the hash function; what it *can* resample
/// is the key population, which under the random-oracle model the sketches
/// assume yields an independent collision configuration per trial. This is
/// stated plainly rather than dressed up as seed independence.
const BOUND_TRIALS: u64 = 3;

/// Zipf over `u64` keys and uniform over `f64` keys for one trial, exercising
/// both `DataInput` hashing paths. Each trial uses a disjoint key domain so
/// its collisions are unrelated to the previous trial's.
fn bound_streams(trial: u64) -> [BoundStream; 2] {
    [
        (
            "zipf/u64",
            zipf_u64(BOUND_N, 8192, 1.1, 1005 + trial * 977)
                .into_iter()
                .map(|v| v as i64 + (trial as i64) * 100_000)
                .collect(),
            u64_input as fn(i64) -> DataInput<'static>,
        ),
        (
            "uniform/f64",
            uniform_u64(BOUND_N, 4096, 1006 + trial * 977)
                .into_iter()
                .map(|v| uniform_f64_key(v + trial * 8192))
                .collect(),
            f64_input as fn(i64) -> DataInput<'static>,
        ),
    ]
}

/// Count-Min Theorem 1 on both insert paths and both `DataInput` hash
/// domains, over independent key populations.
#[test]
fn countmin_satisfies_the_count_min_theorem_on_both_paths() {
    let spec = CountMinSpec::new(BOUND_ROWS, BOUND_COLS);
    for trial in 0..BOUND_TRIALS {
        for (name, keys, to_input) in bound_streams(trial) {
            let mut truth = FreqTruth::default();
            let mut regular =
                CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(BOUND_ROWS, BOUND_COLS);
            let mut fast =
                CountMin::<Vector2D<i32>, FastPath>::with_dimensions(BOUND_ROWS, BOUND_COLS);
            for k in &keys {
                let d = to_input(*k);
                truth.observe(*k);
                regular.insert(&d);
                fast.insert(&d);
            }
            let context =
                format!("{name} trial={trial} n={BOUND_N} (see bound_streams for stream seeds)");
            spec.assert_contract(
                &format!("CountMin RegularPath {name}"),
                &truth,
                |k| regular.estimate(&to_input(k)) as f64,
                &context,
            );
            spec.assert_contract(
                &format!("CountMin FastPath {name}"),
                &truth,
                |k| fast.estimate(&to_input(k)) as f64,
                &context,
            );
        }
    }
}

// -------------------------------------------------------------- CountSketch

/// Count Sketch's median-of-rows L2 bound, plus exact turnstile cancellation.
///
/// The bound asserted here is `sqrt(kappa / w) * ||f_-i||_2` per key, with
/// `||f_-i||_2` recomputed from the exact frequency vector for each key —
/// *not* Count-Min's `e/w * N`. See `CountSketchSpec` for the derivation.
#[test]
fn countsketch_turnstile_cancels_and_satisfies_the_l2_median_bound() {
    // True turnstile: +500 then -500 must net to zero. Exact, not approximate:
    // the same cells are incremented and decremented.
    let mut cs = asap_sketchlib::Count::<Vector2D<i64>, RegularPath>::with_dimensions(4, 1024);
    cs.insert_many(&DataInput::U32(9), 500);
    cs.insert_many(&DataInput::U32(9), -500);
    assert_eq!(
        cs.estimate(&DataInput::U32(9)),
        0.0,
        "turnstile did not cancel"
    );

    const ROWS: usize = 5;
    const COLS: usize = 4096;
    const STREAM_SEED: u64 = 1003;
    let mut cs = asap_sketchlib::Count::<Vector2D<i64>, RegularPath>::with_dimensions(ROWS, COLS);
    let stream = zipf_u64(200_000, 8192, 1.1, STREAM_SEED);
    let mut truth = FreqTruth::default();
    for k in &stream {
        truth.observe(*k as i64);
        cs.insert(&DataInput::I64(*k as i64));
    }
    let spec = CountSketchSpec::new(ROWS, COLS);
    spec.assert_contract(
        "Count<Vector2D<i64>, RegularPath>",
        &truth,
        |k| cs.estimate(&DataInput::I64(k)),
        &format!("zipf(1.1) domain=8192 n=200000 stream_seed={STREAM_SEED}"),
    );
}

/// Count Sketch's L2 bound on both insert paths and both `DataInput` hash
/// domains, with every key in every trial pooled into one binomial acceptance
/// rule at the theorem's own post-median failure probability.
#[test]
fn countsketch_satisfies_the_l2_median_bound_on_both_paths() {
    let spec = CountSketchSpec::new(BOUND_ROWS, BOUND_COLS);
    // One tally per (shape, path): all trials pool into a single acceptance
    // rule, so the decision is made on the whole population rather than on
    // whichever trial happened to look worst.
    let mut tallies: HashMap<String, Tally> = HashMap::new();
    for trial in 0..BOUND_TRIALS {
        for (name, keys, to_input) in bound_streams(trial) {
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
            spec.tally_into(
                tallies.entry(format!("{name}/RegularPath")).or_default(),
                &truth,
                |k| regular.estimate(&to_input(k)),
            );
            spec.tally_into(
                tallies.entry(format!("{name}/FastPath")).or_default(),
                &truth,
                |k| fast.estimate(&to_input(k)),
            );
        }
    }
    for (label, tally) in tallies {
        tally.assert_within(
            &format!("Count {label} / L2 median bound"),
            spec.per_key_failure(),
            &format!(
                "rows={BOUND_ROWS} cols={BOUND_COLS} kappa={} n={BOUND_N} \
                 trials={BOUND_TRIALS} (independent key populations, fixed library hash seed)",
                spec.kappa
            ),
        );
    }
}

/// Count Sketch's error is *rank-independent*: a cold key carries the same
/// absolute error band as the hottest one, which is exactly what separates it
/// from Count-Min. Documented empirical regression, not a theorem: the
/// theorem bounds each key individually and says nothing about how the
/// measured mean error compares across frequency strata.
///
/// Band source: measured on this exact stream (zipf(1.1), domain 8192,
/// n=200_000, seed 1007, rows=5, cols=4096) where the mean |error| per decile
/// spans 20.6 to 26.5 — a spread of 1.29x. The 3x ceiling below leaves ample
/// room for run-to-run movement while still failing loudly if the error ever
/// starts tracking frequency the way Count-Min's does (on this stream
/// Count-Min's per-decile mean error spans more than 40x).
#[test]
fn countsketch_error_stays_rank_independent_within_the_documented_empirical_band() {
    const ROWS: usize = 5;
    const COLS: usize = 4096;
    const STREAM_SEED: u64 = 1007;
    const DECILES: usize = 10;

    let stream = zipf_u64(200_000, 8192, 1.1, STREAM_SEED);
    let mut truth = FreqTruth::default();
    let mut cs = asap_sketchlib::Count::<Vector2D<i64>, RegularPath>::with_dimensions(ROWS, COLS);
    for k in &stream {
        truth.observe(*k as i64);
        cs.insert(&DataInput::I64(*k as i64));
    }

    let mut pairs = truth.pairs();
    pairs.sort_by_key(|(_, c)| *c);
    let per = pairs.len() / DECILES;
    let means: Vec<f64> = (0..DECILES)
        .map(|d| {
            let slice = &pairs[d * per..(d + 1) * per];
            slice
                .iter()
                .map(|(k, c)| (cs.estimate(&DataInput::I64(*k)) - *c as f64).abs())
                .sum::<f64>()
                / slice.len() as f64
        })
        .collect();
    let lo = means.iter().cloned().fold(f64::INFINITY, f64::min);
    let hi = means.iter().cloned().fold(0.0f64, f64::max);
    assert!(
        hi <= lo * 3.0,
        "Count Sketch mean |error| per frequency decile spans {lo:.1}..{hi:.1} ({:.2}x), \
         beyond the documented 3x empirical band — the error has started tracking key \
         frequency, which Count Sketch's L2 guarantee says it must not. \
         stream_seed={STREAM_SEED} rows={ROWS} cols={COLS} deciles={means:?}",
        hi / lo
    );
}

// ------------------------------------------------------------- Top-k heaps

/// The two heap-backed sketches share heap bookkeeping but not an error
/// metric: `CMSHeap` carries Count-Min's one-sided additive bound and
/// `CSHeap` carries Count Sketch's L2 bound. Both must keep every heap entry
/// equal to what the sketch itself estimates, and both must recover the true
/// heavy hitters that the top-k contract actually guarantees.
#[test]
fn heaps_satisfy_their_own_bounds_and_stay_heap_consistent() {
    const TOP_K: usize = 16;
    const DOMAIN: usize = 1024;
    const CM_ROWS: usize = 3;
    const CS_ROWS: usize = 5;
    const COLS: usize = 4096;
    const STREAM_SEED: u64 = 1004;

    let mut cms_heap = CMSHeap::<Vector2D<i64>, RegularPath>::new(CM_ROWS, COLS, TOP_K);
    let mut cs_heap = CSHeap::<Vector2D<i64>, RegularPath>::new(CS_ROWS, COLS, TOP_K);
    let stream = zipf_u64(20_000, DOMAIN, 1.1, STREAM_SEED);
    let mut truth = FreqTruth::default();
    for k in &stream {
        let d = DataInput::I64(*k as i64);
        truth.observe(*k as i64);
        cms_heap.insert(&d);
        cs_heap.insert(&d);
    }
    let context = format!("zipf(1.1) domain={DOMAIN} n=20000 stream_seed={STREAM_SEED}");

    // Each sketch against its own theorem.
    CountMinSpec::new(CM_ROWS, COLS).assert_contract(
        "CMSHeap point estimate",
        &truth,
        |k| cms_heap.estimate(&DataInput::I64(k)) as f64,
        &context,
    );
    CountSketchSpec::new(CS_ROWS, COLS).assert_contract(
        "CSHeap point estimate",
        &truth,
        |k| cs_heap.estimate(&DataInput::I64(k)),
        &context,
    );

    // Heap/sketch consistency is structural: a heap entry that disagrees with
    // the sketch is a bug regardless of any error bound.
    let kth_count = truth.top_k(TOP_K)[TOP_K - 1].1;
    for (label, items, cms) in [
        ("CMSHeap", cms_heap.heap().heap().to_vec(), true),
        ("CSHeap", cs_heap.heap().heap().to_vec(), false),
    ] {
        assert!(items.len() <= TOP_K, "{label} heap exceeded capacity");
        let mut consistency = Tally::default();
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
            consistency.record(it.count == est, || {
                format!(
                    "key {key}: heap holds {} but the sketch estimates {est}",
                    it.count
                )
            });
            if truth.get(key) >= kth_count {
                recall += 1;
            }
        }
        consistency.assert_none(&format!("{label} heap/sketch consistency"), &context);

        // Recall target: a key whose true count is at least the true k-th
        // count must be admitted. Count-Min never underestimates, so every
        // such key's estimate dominates the heap minimum once seen; one
        // displacement slot of slack covers the eviction race at the
        // boundary, where several keys share the k-th count.
        assert!(
            recall >= TOP_K - 1,
            "{label} recovered only {recall}/{TOP_K} keys at or above the true k-th count \
             ({kth_count}); {context}"
        );
    }
}

// -------------------------------------------------------------- CountL2HH

/// CountL2HH is a Count Sketch carrying a running F2, so its point estimates
/// obey the Count Sketch L2 bound and its `get_l2_sqr` tracks the exact F2.
#[test]
fn countl2hh_weighted_turnstile_satisfies_the_l2_median_bound() {
    const ROWS: usize = 4;
    const COLS: usize = 2048;
    const HASH_SEED_IDX: usize = 11;
    const STREAM_SEED: u64 = 1005;

    let mut sk = CountL2HH::<DefaultXxHasher>::with_dimensions_and_seed(ROWS, COLS, HASH_SEED_IDX);
    let mut truth = FreqTruth::default();
    let stream = zipf_u64(30_000, 512, 1.3, STREAM_SEED);
    for (i, k) in stream.iter().enumerate() {
        let w = 1 + (i % 5) as i64;
        sk.fast_insert_with_count(&DataInput::I64(*k as i64), w);
        truth.observe_weighted(*k as i64, w);
    }
    // Turnstile decrement on the hottest key must track through the median path.
    let hot = truth.top_k(1)[0].0;
    sk.fast_insert_with_count(&DataInput::I64(hot), -10);
    truth.observe_weighted(hot, -10);

    let context = format!(
        "zipf(1.3) domain=512 n=30000 weighted 1..5 with one -10 decrement, \
         stream_seed={STREAM_SEED} hash_seed_idx={HASH_SEED_IDX}"
    );
    CountSketchSpec::new(ROWS, COLS).assert_contract(
        "CountL2HH point estimate",
        &truth,
        |k| sk.fast_get_est(&DataInput::I64(k)),
        &context,
    );

    // F2 comes from the median of the per-row counter sums, i.e. the AMS
    // tug-of-war estimator over this sketch's own matrix — an estimate with a
    // real bound, not an exactly maintained total.
    let f2_spec = SecondMomentSpec::new(ROWS, COLS);
    if let Err(detail) = f2_spec.check(sk.get_l2_sqr(), truth.f2()) {
        panic!("CountL2HH F2: {detail}\n  context: {context}");
    }
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
/// merged. Folding halves the width `fold_level` times, so the merged sketches
/// answer at `FULL_COLS >> FOLD_LEVEL` columns — and that, not the unfolded
/// width, is the `w` each theorem is evaluated at. FoldCMS keeps Count-Min's
/// additive bound; FoldCS keeps Count Sketch's L2 bound.
#[test]
fn folded_sketches_keep_their_own_bounds_through_a_sixteen_way_merge() {
    const ROWS: usize = 3;
    const FULL_COLS: usize = 4096;
    const FOLD_LEVEL: u32 = 4;
    const TOP_K: usize = 20;
    const WINDOWS: usize = 16;
    // Enough per window that folding to 256 columns is a real test, while an
    // unoptimised `cargo test` run stays affordable. The bounds are computed
    // from the exact truth, so a shorter stream narrows nothing.
    const N: usize = 240_000;
    const STREAM_SEED: u64 = 1009;

    let stream = zipf_u64(N, 10_000, 1.1, STREAM_SEED);
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
    let folded_cols = FULL_COLS >> FOLD_LEVEL;
    let context = format!(
        "zipf(1.1) domain=10000 n={N} stream_seed={STREAM_SEED}, {WINDOWS} windows folded \
         {FOLD_LEVEL} levels: {FULL_COLS} -> {folded_cols} columns"
    );

    CountMinSpec::new(ROWS, folded_cols).assert_contract(
        "FoldCMS after a 16-way hierarchical merge",
        &truth,
        |k| cms_merged.query(&DataInput::U64(k as u64)) as f64,
        &context,
    );
    CountSketchSpec::new(ROWS, folded_cols).assert_contract(
        "FoldCS after a 16-way hierarchical merge",
        &truth,
        |k| cs_merged.query(&DataInput::U64(k as u64)) as f64,
        &context,
    );
}

// ------------------------------------------------------ Portable wire twins

/// The portable wire twins answer over string keys and must satisfy the same
/// two theorems as their core counterparts — Count-Min additive, Count Sketch
/// L2 — with no extra slack for being on the wire side.
#[test]
fn portable_cms_and_cs_string_keys_satisfy_their_own_bounds() {
    const CM_ROWS: usize = 3;
    const CS_ROWS: usize = 5;
    const COLS: usize = 4096;
    const STREAM_SEED: u64 = 1006;

    let stream = zipf_u64(50_000, 2048, 1.1, STREAM_SEED);
    // The portable types key on strings; `FreqTruth` keys on i64. The stream's
    // u64 values are the identity behind both, so `k -> "k{k}"` is injective
    // and the two truths agree key for key.
    let mut truth = FreqTruth::default();
    let mut pcs = CountMinSketch::new(CM_ROWS, COLS);
    let mut pcss = CountSketch::new(CS_ROWS, COLS);
    for k in &stream {
        truth.observe(*k as i64);
        let key = format!("k{k}");
        pcs.update(&key, 1.0);
        pcss.update(&key, 1.0);
    }
    let context = format!("zipf(1.1) domain=2048 n=50000 string keys, stream_seed={STREAM_SEED}");

    CountMinSpec::new(CM_ROWS, COLS).assert_contract(
        "portable CountMinSketch",
        &truth,
        |k| pcs.estimate(&format!("k{k}")),
        &context,
    );
    CountSketchSpec::new(CS_ROWS, COLS).assert_contract(
        "portable CountSketch",
        &truth,
        |k| pcss.estimate(&format!("k{k}")),
        &context,
    );
}
