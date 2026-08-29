//! Every built-in `NumericalValue` type carried through the generic sketches:
//! `KLL<T>`, `KLLDynamic<T>` and `DDSketch::add<T>`.
//!
//! The library implements `NumericalValue` for fourteen concrete types —
//! `i8 i16 i32 i64 i128 isize`, `u8 u16 u32 u64 u128 usize`, `f32 f64` — and
//! each is a public instance a caller can reach. A suite that only ever
//! instantiates `f64` would not notice a type whose ordering or `to_f64`
//! projection was wrong for its own domain: `u128` overflowing an intermediate,
//! `i8` wrapping in a comparison, `f32` losing a tie.
//!
//! Both sketches keep their own error metric here — rank error for the KLL
//! family, relative value error for DDSketch. The type is what varies, not the
//! contract.
//!
//! ## The `to_f64` projection
//!
//! `NumericalValue::to_f64` is how a value reaches the sketch's arithmetic, and
//! for `i128`/`u128` (and `i64`/`u64` past `2^53`) that projection is itself
//! lossy — 64 and 128-bit integers have more distinct values than `f64` has
//! mantissa bits. Ground truth here is therefore taken over the **projected**
//! values, which is precisely what the sketch was handed. The precision limit
//! that belongs to the caller's type choice is covered separately and
//! explicitly by `the_f64_projection_is_exact_below_two_to_the_53`.

mod common;

use common::specs::{RankErrorSpec, RelativeQuantileSpec, Tally};
use common::{NumericTruth, uniform_u64};

use asap_sketchlib::{DDSketch, KLL, KLLDynamic, NumericalValue};

/// KLL parameter used throughout; `eps(200) = 1.65%` at 99% confidence.
const K: i32 = 200;
/// Compaction-coin seeds. Fixed, and separate from the stream seed.
const SKETCH_SEEDS: [u64; 3] = [0x4E17_0001, 0x4E17_0002, 0x4E17_0003];
const STREAM_SEED: u64 = 0x57EA_0001;
const N: usize = 20_000;
const QS: [f64; 7] = [0.0, 0.01, 0.1, 0.5, 0.9, 0.99, 1.0];

/// A positive-valued stream of `$ty`, drawn from one seeded `u64` source and
/// folded into the type's own range so every type sees the same shape at its
/// own scale. Values stay strictly positive because DDSketch only tracks
/// positive reals, and the same stream feeds all three sketches.
macro_rules! typed_stream {
    ($ty:ty, $span:expr) => {{
        let span: u64 = $span;
        uniform_u64(N, span, STREAM_SEED)
            .into_iter()
            .map(|v| (v + 1) as $ty)
            .collect::<Vec<$ty>>()
    }};
}

/// Ground truth over the projected values — what the sketch actually saw.
fn projected_truth<T: NumericalValue>(values: &[T]) -> NumericTruth {
    NumericTruth::new(values.iter().map(|v| v.to_f64()).collect())
}

// --------------------------------------------------------------- KLL family

/// One numeric type through both KLL implementations, single pass and after a
/// shard merge, against the normalized-rank-error contract.
macro_rules! kll_type_case {
    ($tally:ident, $ty:ty, $span:expr) => {{
        let values = typed_stream!($ty, $span);
        let truth = projected_truth(&values);
        let spec = RankErrorSpec::datasketches(K as usize);

        for &seed in &SKETCH_SEEDS {
            let mut fixed = KLL::<$ty>::init_kll_with_seed(K, seed);
            let mut dynamic = KLLDynamic::<$ty>::init_kll_with_seed(K, seed);
            for v in &values {
                fixed.update(v);
                dynamic.update(v);
            }
            spec.tally_into(&mut $tally, truth.sorted(), &QS, |q| fixed.quantile(q));
            spec.tally_into(&mut $tally, truth.sorted(), &QS, |q| dynamic.quantile(q));

            // Two shards merged must answer under the same contract.
            let mut left = KLL::<$ty>::init_kll_with_seed(K, seed ^ 0xAAAA);
            let mut right = KLL::<$ty>::init_kll_with_seed(K, seed ^ 0x5555);
            let mut dleft = KLLDynamic::<$ty>::init_kll_with_seed(K, seed ^ 0xAAAA);
            let mut dright = KLLDynamic::<$ty>::init_kll_with_seed(K, seed ^ 0x5555);
            for (i, v) in values.iter().enumerate() {
                if i % 2 == 0 {
                    left.update(v);
                    dleft.update(v);
                } else {
                    right.update(v);
                    dright.update(v);
                }
            }
            left.merge(&right);
            dleft.merge(&dright);
            spec.tally_into(&mut $tally, truth.sorted(), &QS, |q| left.quantile(q));
            spec.tally_into(&mut $tally, truth.sorted(), &QS, |q| dleft.quantile(q));

            // Bulk ingestion must be equivalent to the loop.
            let mut bulk = KLL::<$ty>::init_kll_with_seed(K, seed);
            bulk.bulk_update(&values);
            spec.tally_into(&mut $tally, truth.sorted(), &QS, |q| bulk.quantile(q));
            let mut dbulk = KLLDynamic::<$ty>::init_kll_with_seed(K, seed);
            dbulk.bulk_update(&values);
            spec.tally_into(&mut $tally, truth.sorted(), &QS, |q| dbulk.quantile(q));
        }
    }};
}

/// Every built-in `NumericalValue` type through `KLL<T>` and `KLLDynamic<T>`.
///
/// The spans are the type's own usable range, so the narrow types genuinely
/// exercise the tie-dense case: an `i8` stream of 20,000 observations over 127
/// distinct values gives every value a rank interval nearly 1% wide, which the
/// rank-interval predicate handles and a value-error check could not.
#[test]
fn every_numeric_type_satisfies_the_kll_rank_error_contract() {
    let mut tally = Tally::default();
    kll_type_case!(tally, i8, 127);
    kll_type_case!(tally, i16, 32_767);
    kll_type_case!(tally, i32, 1_000_000_000);
    kll_type_case!(tally, i64, 1_000_000_000_000_000);
    kll_type_case!(tally, i128, 1_000_000_000_000_000);
    kll_type_case!(tally, isize, 1_000_000_000_000_000);
    kll_type_case!(tally, u8, 255);
    kll_type_case!(tally, u16, 65_535);
    kll_type_case!(tally, u32, 4_000_000_000);
    kll_type_case!(tally, u64, 1_000_000_000_000_000);
    kll_type_case!(tally, u128, 1_000_000_000_000_000);
    kll_type_case!(tally, usize, 1_000_000_000_000_000);
    kll_type_case!(tally, f32, 1_000_000);
    kll_type_case!(tally, f64, 1_000_000_000_000_000);

    tally.assert_within(
        "KLL<T> / KLLDynamic<T> across every built-in NumericalValue type",
        RankErrorSpec::datasketches(K as usize).failure_probability,
        &format!(
            "k={K} sketch seeds {SKETCH_SEEDS:02x?} stream_seed={STREAM_SEED:#x} n={N}, \
             modes: single pass, bulk update, two-shard merge; q grid {QS:?}"
        ),
    );
}

/// The KLL family must also order negative and mixed-sign values correctly.
/// The unsigned types cannot represent them, so this covers the signed and
/// floating types only — and it is where a `total_cmp` that fell back to a
/// bitwise comparison would show up.
#[test]
fn signed_numeric_types_order_negative_values_correctly_in_kll() {
    macro_rules! signed_case {
        ($tally:ident, $ty:ty, $span:expr) => {{
            let span: i64 = $span;
            let values: Vec<$ty> = uniform_u64(N, (span * 2) as u64, STREAM_SEED)
                .into_iter()
                .map(|v| (v as i64 - span) as $ty)
                .collect();
            let truth = projected_truth(&values);
            let spec = RankErrorSpec::datasketches(K as usize);
            for &seed in &SKETCH_SEEDS {
                let mut fixed = KLL::<$ty>::init_kll_with_seed(K, seed);
                let mut dynamic = KLLDynamic::<$ty>::init_kll_with_seed(K, seed);
                for v in &values {
                    fixed.update(v);
                    dynamic.update(v);
                }
                spec.tally_into(&mut $tally, truth.sorted(), &QS, |q| fixed.quantile(q));
                spec.tally_into(&mut $tally, truth.sorted(), &QS, |q| dynamic.quantile(q));
            }
            // KLL answers with *retained* items, and compaction may discard
            // the actual extremes — so q=0 is not required to be the exact
            // minimum (its rank-error obligation at q=0 is already tallied
            // above). What is structural is that the sketch never invents a
            // value: every answer must lie inside the observed range, on the
            // correct side of zero. A `total_cmp` that ordered negatives by
            // their bit pattern would break this immediately.
            let mut probe = KLL::<$ty>::init_kll_with_seed(K, SKETCH_SEEDS[0]);
            for v in &values {
                probe.update(v);
            }
            for q in [0.0f64, 0.25, 0.5, 0.75, 1.0] {
                let got = probe.quantile(q);
                assert!(
                    got >= truth.min() && got <= truth.max(),
                    concat!(
                        stringify!($ty),
                        ": quantile({}) returned {}, outside the observed range [{}, {}]"
                    ),
                    q,
                    got,
                    truth.min(),
                    truth.max()
                );
            }
            // The median of a symmetric mixed-sign stream must land near zero,
            // which a broken sign ordering could not produce.
            let median = probe.quantile(0.5);
            assert!(
                median.abs() <= truth.max() * 0.10,
                concat!(
                    stringify!($ty),
                    ": median {} is far from zero on a stream symmetric about it \
                     (range [{}, {}]); check the type's ordering"
                ),
                median,
                truth.min(),
                truth.max()
            );
        }};
    }

    let mut tally = Tally::default();
    signed_case!(tally, i8, 100);
    signed_case!(tally, i16, 30_000);
    signed_case!(tally, i32, 1_000_000_000);
    signed_case!(tally, i64, 1_000_000_000_000_000);
    signed_case!(tally, i128, 1_000_000_000_000_000);
    signed_case!(tally, isize, 1_000_000_000_000_000);
    signed_case!(tally, f32, 1_000_000);
    signed_case!(tally, f64, 1_000_000_000_000_000);

    tally.assert_within(
        "KLL family over signed and mixed-sign values",
        RankErrorSpec::datasketches(K as usize).failure_probability,
        &format!("k={K} sketch seeds {SKETCH_SEEDS:02x?} stream_seed={STREAM_SEED:#x} n={N}"),
    );
}

// ------------------------------------------------------------------ DDSketch

/// `DDSketch::add<T>` accepts any `NumericalValue`. The relative-error
/// guarantee is on the projected value, so converting through a wider integer
/// type must not change it: the same numbers must come back within `alpha`
/// whether they arrived as `u8` or `u128`.
macro_rules! dds_type_case {
    ($alpha:expr, $tally:ident, $ty:ty, $span:expr) => {{
        let values = typed_stream!($ty, $span);
        let truth = projected_truth(&values);
        let spec = RelativeQuantileSpec::new($alpha);
        let mut sketch = DDSketch::new($alpha);
        for v in &values {
            sketch.add(v);
        }
        assert_eq!(
            sketch.get_count() as usize,
            values.len(),
            concat!(
                stringify!($ty),
                ": every positive value must be trackable at this alpha"
            )
        );
        spec.tally_into(&mut $tally, truth.sorted(), &QS, |q| {
            sketch.get_value_at_quantile(q)
        });
    }};
}

#[test]
fn every_numeric_type_satisfies_the_ddsketch_relative_value_error_contract() {
    for alpha in [0.001f64, 0.01, 0.05] {
        let mut tally = Tally::default();
        dds_type_case!(alpha, tally, i8, 127);
        dds_type_case!(alpha, tally, i16, 32_767);
        dds_type_case!(alpha, tally, i32, 1_000_000_000);
        dds_type_case!(alpha, tally, i64, 1_000_000_000_000_000);
        dds_type_case!(alpha, tally, i128, 1_000_000_000_000_000);
        dds_type_case!(alpha, tally, isize, 1_000_000_000_000_000);
        dds_type_case!(alpha, tally, u8, 255);
        dds_type_case!(alpha, tally, u16, 65_535);
        dds_type_case!(alpha, tally, u32, 4_000_000_000);
        dds_type_case!(alpha, tally, u64, 1_000_000_000_000_000);
        dds_type_case!(alpha, tally, u128, 1_000_000_000_000_000);
        dds_type_case!(alpha, tally, usize, 1_000_000_000_000_000);
        dds_type_case!(alpha, tally, f32, 1_000_000);
        dds_type_case!(alpha, tally, f64, 1_000_000_000_000_000);

        // Deterministic guarantee: no hashing, no sampling, so no violations
        // are tolerated at any alpha.
        tally.assert_none(
            "DDSketch::add<T> across every built-in NumericalValue type",
            &format!("alpha={alpha} stream_seed={STREAM_SEED:#x} n={N} q grid {QS:?}"),
        );
    }
}

/// The 128-bit types past `f64`'s exact-integer range.
///
/// `2^53` is where `f64` stops representing consecutive integers, and
/// `NumericalValue::to_f64` is a plain `as` cast, so an `i128`/`u128` above it
/// reaches the sketch already rounded. The relative-error contract still holds
/// on the projected value — the rounding is at most one part in `2^53`, twelve
/// orders of magnitude below the smallest alpha the library supports — but the
/// *identity* of the value is not preserved, and that is a documented limit of
/// the caller's type choice rather than a sketch defect.
#[test]
fn the_f64_projection_is_exact_below_two_to_the_53() {
    const TWO_53: u128 = 1u128 << 53;

    // Exact below the boundary, in both 128-bit types.
    for v in [1u128, 1_000, TWO_53 - 1, TWO_53] {
        assert_eq!(
            (v as f64) as u128,
            v,
            "u128 {v} must project into f64 exactly (at or below 2^53)"
        );
        assert_eq!(
            ((v as i128).to_f64()) as i128,
            v as i128,
            "i128 {v} must project into f64 exactly (at or below 2^53)"
        );
    }
    // Just past it, consecutive integers collapse — this is the documented
    // limit, asserted so the boundary cannot move unnoticed.
    assert_eq!(
        (TWO_53 + 1) as f64,
        TWO_53 as f64,
        "2^53 + 1 is expected to collapse onto 2^53 in f64"
    );

    // The relative-error contract survives regardless: a stream of very large
    // 128-bit values still answers within alpha of the projected truth.
    const ALPHA: f64 = 0.01;
    let values: Vec<u128> = uniform_u64(5_000, 1_000_000, STREAM_SEED)
        .into_iter()
        .map(|v| (v as u128 + 1) * (1u128 << 70))
        .collect();
    let truth = projected_truth(&values);
    let mut sketch = DDSketch::new(ALPHA);
    for v in &values {
        sketch.add(v);
    }
    assert_eq!(sketch.get_count() as usize, values.len());
    let mut tally = Tally::default();
    RelativeQuantileSpec::new(ALPHA).tally_into(&mut tally, truth.sorted(), &QS, |q| {
        sketch.get_value_at_quantile(q)
    });
    tally.assert_none(
        "DDSketch over u128 values above 2^70",
        &format!("alpha={ALPHA} values in [2^70, 10^6 * 2^70], stream_seed={STREAM_SEED:#x}"),
    );

    // And the KLL family orders them correctly at that magnitude.
    let mut kll = KLL::<u128>::init_kll_with_seed(K, SKETCH_SEEDS[0]);
    for v in &values {
        kll.update(v);
    }
    let mut rank_tally = Tally::default();
    RankErrorSpec::datasketches(K as usize)
        .tally_into(&mut rank_tally, truth.sorted(), &QS, |q| kll.quantile(q));
    rank_tally.assert_within(
        "KLL<u128> over values above 2^70",
        RankErrorSpec::datasketches(K as usize).failure_probability,
        &format!("k={K} sketch_seed={:#x}", SKETCH_SEEDS[0]),
    );
}
