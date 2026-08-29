//! Every built-in `(storage, hashing path)` instance of the four matrix-backed
//! frequency families, held to its own family's theorem.
//!
//! The deep, production-sized accuracy runs live in `e2e_frequency.rs` on each
//! family's headline instance. This file is about *coverage of the instance
//! matrix*: a storage backend that silently mis-indexes, a counter width that
//! wraps, or a fast path that reads cells the regular path never wrote, would
//! all pass a suite that only ever exercised `Vector2D<i64>`.
//!
//! Each instance therefore gets a medium stream and three checks:
//!
//! 1. its family's error bound (`CountMinSpec` or `CountSketchSpec`) evaluated
//!    at the dimensions the instance itself reports, so the fixed 5x2048 and
//!    3x4096 layouts are judged at their own `w` rather than a borrowed one;
//! 2. agreement between the regular and fast paths on the same storage, which
//!    is exact — they are two ways of deriving the same cells;
//! 3. shard merge, which for counter matrices is exact addition.
//!
//! Counter-width overflow is covered separately, once per width, because that
//! is where `i32`, `i64` and `i128` actually differ.

mod common;

use common::specs::{CountMinSpec, CountSketchSpec, Tally};
use common::{FreqTruth, zipf_u64};

use asap_sketchlib::{
    CMSHeap, CSHeap, Count, CountMin, DataInput, DefaultMatrixI32, DefaultMatrixI64,
    DefaultMatrixI128, FastPath, FixedMatrix, HeapItem, QuickMatrixI64, QuickMatrixI128,
    RegularPath, Vector2D,
};

/// Medium stream: large enough that collisions are statistically meaningful at
/// 2048 and 4096 columns, small enough that seventy instances stay quick.
const N: usize = 40_000;
const DOMAIN: usize = 4_096;
const STREAM_SEED: u64 = 0x10BE_C700;

fn stream_and_truth() -> (Vec<u64>, FreqTruth) {
    let stream = zipf_u64(N, DOMAIN, 1.1, STREAM_SEED);
    let mut truth = FreqTruth::default();
    for k in &stream {
        truth.observe(*k as i64);
    }
    (stream, truth)
}

fn context(label: &str, rows: usize, cols: usize) -> String {
    format!("{label} rows={rows} cols={cols} zipf(1.1) domain={DOMAIN} n={N} seed={STREAM_SEED:#x}")
}

/// Counter values reach the assertions as `f64`; the concrete counter type is
/// known at each macro expansion, so this stays a plain conversion rather than
/// a lossy generic cast scattered through the tests.
trait CounterValue: Copy {
    fn as_f64(self) -> f64;
}
impl CounterValue for i32 {
    fn as_f64(self) -> f64 {
        self as f64
    }
}
impl CounterValue for i64 {
    fn as_f64(self) -> f64 {
        self as f64
    }
}
impl CounterValue for i128 {
    fn as_f64(self) -> f64 {
        self as f64
    }
}
impl CounterValue for f64 {
    fn as_f64(self) -> f64 {
        self
    }
}

// ----------------------------------------------------------------- CountMin

/// One `CountMin<$storage, $path>` instance: build it from `Default` (which is
/// what fixes each fixed-layout storage's dimensions), run the stream, and
/// assert Count-Min's own contract at the dimensions it reports.
macro_rules! countmin_instance {
    ($storage:ty, $path:ty, $stream:expr, $truth:expr) => {{
        let mut single = CountMin::<$storage, $path>::default();
        let mut left = CountMin::<$storage, $path>::default();
        let mut right = CountMin::<$storage, $path>::default();
        for (i, k) in $stream.iter().enumerate() {
            let d = DataInput::U64(*k);
            single.insert(&d);
            if i % 2 == 0 {
                left.insert(&d);
            } else {
                right.insert(&d);
            }
        }
        left.merge(&right);

        let (rows, cols) = (single.rows(), single.cols());
        let label = concat!(
            "CountMin<",
            stringify!($storage),
            ", ",
            stringify!($path),
            ">"
        );
        let ctx = context(label, rows, cols);
        let spec = CountMinSpec::new(rows, cols);
        spec.assert_contract(
            label,
            $truth,
            |k| single.estimate(&DataInput::U64(k as u64)).as_f64(),
            &ctx,
        );
        spec.assert_contract(
            &format!("{label} shard merge"),
            $truth,
            |k| left.estimate(&DataInput::U64(k as u64)).as_f64(),
            &ctx,
        );

        // Merging counter matrices is exact addition, so the merged sketch is
        // identical to the single-pass one, not merely close.
        let mut equality = Tally::default();
        for (k, _) in $truth.pairs() {
            let a = single.estimate(&DataInput::U64(k as u64)).as_f64();
            let b = left.estimate(&DataInput::U64(k as u64)).as_f64();
            equality.record(a == b, || format!("key {k}: single {a} merged {b}"));
        }
        equality.assert_none(&format!("{label} merge equality"), &ctx);
        single
    }};
}

/// On a workload small enough to be collision-free, *both* paths must return
/// exact counts — every probed cell belongs to the queried key alone.
///
/// This, and not estimate-for-estimate equality, is the cross-path contract.
/// `RegularPath` makes one hash call per row with `seed_list[r]`; `FastPath`
/// makes a single call with `seed_list[0]` and slices row bits out of it.
/// They are different hash functions, so they place keys in different columns
/// and legitimately disagree wherever either one collides. Asserting equality
/// on a colliding stream would be asserting a coincidence: on a 40k-update
/// Zipf stream at 3x4096 the two paths differ on roughly a third of keys, all
/// of them inside Count-Min's bound.
macro_rules! assert_both_paths_exact_without_collisions {
    ($sketch:ident, $storage:ty, $keys:expr) => {{
        let mut regular = $sketch::<$storage, RegularPath>::default();
        let mut fast = $sketch::<$storage, FastPath>::default();
        let mut truth = FreqTruth::default();
        for (i, k) in $keys.iter().enumerate() {
            for _ in 0..(i + 1) * 10 {
                let d = DataInput::U64(*k);
                regular.insert(&d);
                fast.insert(&d);
                truth.observe(*k as i64);
            }
        }
        let label = concat!(
            stringify!($sketch),
            "<",
            stringify!($storage),
            "> collision-free exactness on both paths"
        );
        let ctx = context(label, regular.rows(), regular.cols());
        let mut tally = Tally::default();
        for (k, c) in truth.pairs() {
            let r = regular.estimate(&DataInput::U64(k as u64)).as_f64();
            let f = fast.estimate(&DataInput::U64(k as u64)).as_f64();
            tally.record(r == c as f64 && f == c as f64, || {
                format!("key {k}: true {c}, regular {r}, fast {f}")
            });
        }
        tally.assert_none(label, &ctx);
    }};
}

/// Eight well-separated keys in a 2048- or 4096-column grid: the chance that
/// any pair collides in every row is negligible, and with fixed seeds the
/// outcome is deterministic, so a failure here means a storage backend is
/// mis-indexing rather than that the stream was unlucky.
const COLLISION_FREE_KEYS: [u64; 8] = [
    1,
    7,
    4_242,
    90_210,
    1_000_003,
    2_147_483_647,
    4_294_967_311,
    9_007_199_254_740_993,
];

macro_rules! countmin_matrix_test {
    ($name:ident, $path:ty, $( $storage:ty ),+ $(,)?) => {
        #[test]
        fn $name() {
            let (stream, truth) = stream_and_truth();
            $( { let _ = countmin_instance!($storage, $path, stream, &truth); } )+
        }
    };
}

countmin_matrix_test!(
    countmin_regular_path_instances_satisfy_the_count_min_bound,
    RegularPath,
    Vector2D<i32>,
    Vector2D<i64>,
    Vector2D<i128>,
    Vector2D<f64>,
    FixedMatrix,
    DefaultMatrixI32,
    QuickMatrixI64,
    QuickMatrixI128,
    DefaultMatrixI64,
    DefaultMatrixI128,
);

countmin_matrix_test!(
    countmin_fast_path_instances_satisfy_the_count_min_bound,
    FastPath,
    Vector2D<i32>,
    Vector2D<i64>,
    Vector2D<i128>,
    Vector2D<f64>,
    FixedMatrix,
    DefaultMatrixI32,
    QuickMatrixI64,
    QuickMatrixI128,
    DefaultMatrixI64,
    DefaultMatrixI128,
);

#[test]
fn countmin_both_paths_are_exact_on_a_collision_free_workload() {
    assert_both_paths_exact_without_collisions!(CountMin, Vector2D<i32>, COLLISION_FREE_KEYS);
    assert_both_paths_exact_without_collisions!(CountMin, Vector2D<i64>, COLLISION_FREE_KEYS);
    assert_both_paths_exact_without_collisions!(CountMin, Vector2D<i128>, COLLISION_FREE_KEYS);
    assert_both_paths_exact_without_collisions!(CountMin, Vector2D<f64>, COLLISION_FREE_KEYS);
    assert_both_paths_exact_without_collisions!(CountMin, FixedMatrix, COLLISION_FREE_KEYS);
    assert_both_paths_exact_without_collisions!(CountMin, DefaultMatrixI32, COLLISION_FREE_KEYS);
    assert_both_paths_exact_without_collisions!(CountMin, QuickMatrixI64, COLLISION_FREE_KEYS);
    assert_both_paths_exact_without_collisions!(CountMin, QuickMatrixI128, COLLISION_FREE_KEYS);
    assert_both_paths_exact_without_collisions!(CountMin, DefaultMatrixI64, COLLISION_FREE_KEYS);
    assert_both_paths_exact_without_collisions!(CountMin, DefaultMatrixI128, COLLISION_FREE_KEYS);
}

// -------------------------------------------------------------- CountSketch

macro_rules! count_instance {
    ($storage:ty, $path:ty, $stream:expr, $truth:expr) => {{
        let mut single = Count::<$storage, $path>::default();
        let mut left = Count::<$storage, $path>::default();
        let mut right = Count::<$storage, $path>::default();
        for (i, k) in $stream.iter().enumerate() {
            let d = DataInput::U64(*k);
            single.insert(&d);
            if i % 2 == 0 {
                left.insert(&d);
            } else {
                right.insert(&d);
            }
        }
        left.merge(&right);

        let (rows, cols) = (single.rows(), single.cols());
        let label = concat!("Count<", stringify!($storage), ", ", stringify!($path), ">");
        let ctx = context(label, rows, cols);
        let spec = CountSketchSpec::new(rows, cols);
        spec.assert_contract(
            label,
            $truth,
            |k| single.estimate(&DataInput::U64(k as u64)),
            &ctx,
        );
        spec.assert_contract(
            &format!("{label} shard merge"),
            $truth,
            |k| left.estimate(&DataInput::U64(k as u64)),
            &ctx,
        );

        // Signed counters still add exactly on merge.
        let mut equality = Tally::default();
        for (k, _) in $truth.pairs() {
            let a = single.estimate(&DataInput::U64(k as u64));
            let b = left.estimate(&DataInput::U64(k as u64));
            equality.record(a == b, || format!("key {k}: single {a} merged {b}"));
        }
        equality.assert_none(&format!("{label} merge equality"), &ctx);
    }};
}

macro_rules! count_matrix_test {
    ($name:ident, $path:ty, $( $storage:ty ),+ $(,)?) => {
        #[test]
        fn $name() {
            let (stream, truth) = stream_and_truth();
            $( count_instance!($storage, $path, stream, &truth); )+
        }
    };
}

count_matrix_test!(
    countsketch_regular_path_instances_satisfy_the_l2_bound,
    RegularPath,
    Vector2D<i32>,
    Vector2D<i64>,
    Vector2D<i128>,
    FixedMatrix,
    DefaultMatrixI32,
    QuickMatrixI64,
    QuickMatrixI128,
    DefaultMatrixI64,
    DefaultMatrixI128,
);

count_matrix_test!(
    countsketch_fast_path_instances_satisfy_the_l2_bound,
    FastPath,
    Vector2D<i32>,
    Vector2D<i64>,
    Vector2D<i128>,
    FixedMatrix,
    DefaultMatrixI32,
    QuickMatrixI64,
    QuickMatrixI128,
    DefaultMatrixI64,
    DefaultMatrixI128,
);

/// The same collision-free exactness contract for the signed family: with no
/// collisions every row reports `sign * sign * f = f`, so the median is exact
/// on both paths.
#[test]
fn countsketch_both_paths_are_exact_on_a_collision_free_workload() {
    macro_rules! exact {
        ($storage:ty) => {{
            let mut regular = Count::<$storage, RegularPath>::default();
            let mut fast = Count::<$storage, FastPath>::default();
            let mut truth = FreqTruth::default();
            for (i, k) in COLLISION_FREE_KEYS.iter().enumerate() {
                for _ in 0..(i + 1) * 10 {
                    let d = DataInput::U64(*k);
                    regular.insert(&d);
                    fast.insert(&d);
                    truth.observe(*k as i64);
                }
            }
            let label = concat!(
                "Count<",
                stringify!($storage),
                "> collision-free exactness on both paths"
            );
            let ctx = context(label, regular.rows(), regular.cols());
            let mut tally = Tally::default();
            for (k, c) in truth.pairs() {
                let r = regular.estimate(&DataInput::U64(k as u64));
                let f = fast.estimate(&DataInput::U64(k as u64));
                tally.record(r == c as f64 && f == c as f64, || {
                    format!("key {k}: true {c}, regular {r}, fast {f}")
                });
            }
            tally.assert_none(label, &ctx);
        }};
    }
    exact!(Vector2D<i32>);
    exact!(Vector2D<i64>);
    exact!(Vector2D<i128>);
    exact!(FixedMatrix);
    exact!(DefaultMatrixI32);
    exact!(QuickMatrixI64);
    exact!(QuickMatrixI128);
    exact!(DefaultMatrixI64);
    exact!(DefaultMatrixI128);
}

// ------------------------------------------------------------ Heap variants

/// `CMSHeap` is a Count-Min plus a top-k heap. The point estimate carries
/// Count-Min's bound; the heap must stay consistent with it and must recover
/// the keys the top-k contract actually guarantees.
macro_rules! cmsheap_instance {
    ($storage:ty, $path:ty, $stream:expr, $truth:expr) => {{
        let mut single = CMSHeap::<$storage, $path>::default();
        let mut left = CMSHeap::<$storage, $path>::default();
        let mut right = CMSHeap::<$storage, $path>::default();
        for (i, k) in $stream.iter().enumerate() {
            let d = DataInput::U64(*k);
            single.insert(&d);
            if i % 2 == 0 {
                left.insert(&d);
            } else {
                right.insert(&d);
            }
        }
        left.merge(&right);

        let (rows, cols) = (single.rows(), single.cols());
        let label = concat!(
            "CMSHeap<",
            stringify!($storage),
            ", ",
            stringify!($path),
            ">"
        );
        let ctx = context(label, rows, cols);
        let spec = CountMinSpec::new(rows, cols);
        spec.assert_contract(
            label,
            $truth,
            |k| single.estimate(&DataInput::U64(k as u64)).as_f64(),
            &ctx,
        );
        spec.assert_contract(
            &format!("{label} shard merge"),
            $truth,
            |k| left.estimate(&DataInput::U64(k as u64)).as_f64(),
            &ctx,
        );
        assert_heap_matches_sketch(label, &ctx, single.heap().heap(), $truth, |k| {
            single.estimate(&DataInput::U64(k as u64)).as_f64()
        });
    }};
}

/// `CSHeap` is a Count Sketch plus the same heap, so the point estimate
/// carries the L2 bound instead.
macro_rules! csheap_instance {
    ($storage:ty, $path:ty, $stream:expr, $truth:expr) => {{
        let mut single = CSHeap::<$storage, $path>::default();
        let mut left = CSHeap::<$storage, $path>::default();
        let mut right = CSHeap::<$storage, $path>::default();
        for (i, k) in $stream.iter().enumerate() {
            let d = DataInput::U64(*k);
            single.insert(&d);
            if i % 2 == 0 {
                left.insert(&d);
            } else {
                right.insert(&d);
            }
        }
        left.merge(&right);

        let (rows, cols) = (single.rows(), single.cols());
        let label = concat!(
            "CSHeap<",
            stringify!($storage),
            ", ",
            stringify!($path),
            ">"
        );
        let ctx = context(label, rows, cols);
        let spec = CountSketchSpec::new(rows, cols);
        spec.assert_contract(
            label,
            $truth,
            |k| single.estimate(&DataInput::U64(k as u64)),
            &ctx,
        );
        spec.assert_contract(
            &format!("{label} shard merge"),
            $truth,
            |k| left.estimate(&DataInput::U64(k as u64)),
            &ctx,
        );
        assert_heap_matches_sketch(label, &ctx, single.heap().heap(), $truth, |k| {
            single.estimate(&DataInput::U64(k as u64))
        });
    }};
}

/// Shared heap checks: every entry agrees with the sketch's own estimate, and
/// the heap holds keys the top-k contract guarantees.
fn assert_heap_matches_sketch<F>(
    label: &str,
    ctx: &str,
    items: &[asap_sketchlib::HHItem],
    truth: &FreqTruth,
    estimate: F,
) where
    F: Fn(i64) -> f64,
{
    let mut consistency = Tally::default();
    let capacity = items.len().max(1);
    let kth = truth.top_k(capacity)[capacity.min(truth.distinct()) - 1].1;
    let mut recall = 0usize;
    for it in items {
        let key = match it.key {
            HeapItem::U64(v) => v as i64,
            ref other => panic!("{label}: unexpected heap key {other:?}"),
        };
        let est = estimate(key);
        consistency.record(it.count as f64 == est, || {
            format!(
                "key {key}: heap holds {} but the sketch estimates {est}",
                it.count
            )
        });
        if truth.get(key) >= kth {
            recall += 1;
        }
    }
    consistency.assert_none(&format!("{label} heap/sketch consistency"), ctx);
    assert!(
        recall + 1 >= items.len(),
        "{label}: only {recall} of {} heap entries are at or above the true k-th count \
         ({kth}). {ctx}",
        items.len()
    );
}

macro_rules! heap_matrix_test {
    ($name:ident, $mac:ident, $path:ty, $( $storage:ty ),+ $(,)?) => {
        #[test]
        fn $name() {
            let (stream, truth) = stream_and_truth();
            $( $mac!($storage, $path, stream, &truth); )+
        }
    };
}

// `CMSHeap`/`CSHeap` bound their insert paths on `S::Counter: Into<i64>` so
// that a heap entry can hold the estimate. `i128` does not satisfy that, so
// `CMSHeap<QuickMatrixI128>`, `CMSHeap<DefaultMatrixI128>` and their `CSHeap`
// twins are *constructible but not insertable*: their `Default` impls exist
// and compile, but calling `insert` on one does not. Those four instances per
// family are therefore absent below by necessity rather than oversight; see
// `docs/e2e_coverage_matrix.md`.
heap_matrix_test!(
    cmsheap_regular_path_instances_satisfy_the_count_min_bound,
    cmsheap_instance,
    RegularPath,
    Vector2D<i32>,
    Vector2D<i64>,
    FixedMatrix,
    DefaultMatrixI32,
    QuickMatrixI64,
    DefaultMatrixI64,
);

heap_matrix_test!(
    cmsheap_fast_path_instances_satisfy_the_count_min_bound,
    cmsheap_instance,
    FastPath,
    Vector2D<i32>,
    Vector2D<i64>,
    FixedMatrix,
    DefaultMatrixI32,
    QuickMatrixI64,
    DefaultMatrixI64,
);

heap_matrix_test!(
    csheap_regular_path_instances_satisfy_the_l2_bound,
    csheap_instance,
    RegularPath,
    Vector2D<i32>,
    Vector2D<i64>,
    FixedMatrix,
    DefaultMatrixI32,
    QuickMatrixI64,
    DefaultMatrixI64,
);

heap_matrix_test!(
    csheap_fast_path_instances_satisfy_the_l2_bound,
    csheap_instance,
    FastPath,
    Vector2D<i32>,
    Vector2D<i64>,
    FixedMatrix,
    DefaultMatrixI32,
    QuickMatrixI64,
    DefaultMatrixI64,
);

// ------------------------------------------------------- Counter-width edges

/// The only thing the three counter widths actually promise differently is how
/// much mass a single cell can hold. Each width must carry a count the next
/// smaller one cannot, and the estimate must come back intact rather than
/// wrapped.
#[test]
fn countmin_counter_widths_carry_the_mass_their_type_allows() {
    let key = DataInput::U64(0xFEED_FACE);

    // i32: just under the signed 32-bit ceiling.
    let mut cm32 = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);
    let near_i32 = i32::MAX - 1;
    cm32.insert_many(&key, near_i32);
    assert_eq!(
        cm32.estimate(&key),
        near_i32,
        "i32 counters must hold {near_i32} without wrapping"
    );

    // i64: a count an i32 cell could not represent at all.
    let mut cm64 = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(3, 64);
    let beyond_i32 = i32::MAX as i64 * 4;
    cm64.insert_many(&key, beyond_i32);
    assert_eq!(
        cm64.estimate(&key),
        beyond_i32,
        "i64 counters must hold {beyond_i32}, which overflows i32"
    );

    // i128: a count an i64 cell could not represent.
    let mut cm128 = CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(3, 64);
    let beyond_i64 = i64::MAX as i128 * 4;
    cm128.insert_many(&key, beyond_i64);
    assert_eq!(
        cm128.estimate(&key),
        beyond_i64,
        "i128 counters must hold {beyond_i64}, which overflows i64"
    );

    // Merging two sketches each holding half the i64 range must not wrap the
    // i128 target either.
    let mut a = CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(3, 64);
    let mut b = CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(3, 64);
    a.insert_many(&key, i64::MAX as i128);
    b.insert_many(&key, i64::MAX as i128);
    a.merge(&b);
    assert_eq!(
        a.estimate(&key),
        i64::MAX as i128 * 2,
        "merging two i64::MAX counts into i128 storage must not wrap"
    );
}

/// The signed families have a symmetric requirement: a decrement must reach
/// the negative end of the counter's range without wrapping.
#[test]
fn countsketch_counter_widths_carry_signed_mass_in_both_directions() {
    let key = DataInput::U64(0xDEAD_BEEF);

    let mut cs32 = Count::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);
    cs32.insert_many(&key, i32::MAX / 2);
    cs32.insert_many(&key, -(i32::MAX / 2));
    assert_eq!(
        cs32.estimate(&key),
        0.0,
        "i32 Count Sketch must cancel a half-range increment exactly"
    );

    let mut cs64 = Count::<Vector2D<i64>, RegularPath>::with_dimensions(3, 64);
    let big = i32::MAX as i64 * 4;
    cs64.insert_many(&key, big);
    assert_eq!(cs64.estimate(&key), big as f64);
    cs64.insert_many(&key, -big * 2);
    assert_eq!(
        cs64.estimate(&key),
        -(big as f64),
        "i64 Count Sketch must reach the negative side of the i32 range"
    );

    let mut cs128 = Count::<Vector2D<i128>, RegularPath>::with_dimensions(3, 64);
    let huge = i64::MAX as i128 * 4;
    cs128.insert_many(&key, huge);
    assert_eq!(cs128.estimate(&key), huge as f64);
    cs128.insert_many(&key, -huge * 2);
    assert_eq!(
        cs128.estimate(&key),
        -(huge as f64),
        "i128 Count Sketch must reach the negative side of the i64 range"
    );
}
