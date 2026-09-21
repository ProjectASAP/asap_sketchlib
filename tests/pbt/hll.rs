//! Property tests for HyperLogLog: insertion, estimation, and merge.
//!
//! The models here are derived from the published definition, not from the
//! implementation they check. Where the paper leaves a choice free - which bits
//! of a hash index a register, how a bucket maps to a leading-zero count - the
//! model states only what the paper fixes, because an implementation is
//! entitled to the other option.
//!
//! For `Classic` and `ErtlMLE`, `estimate` is a deterministic function of the
//! registers, so no model of it can be independent of the code it checks. Its
//! laws here are relations between estimates instead: a permutation, a uniform
//! shift, a single rise. The HIP estimator is the exception - its estimate is
//! an accumulator over arrivals, and the increment each arrival owes is fixed
//! in closed form by the registers that preceded it, so that one is stated as
//! an equality.
//!
//! Each law is written once inside a macro and expanded per shape, so a failure
//! names its own precision and variant. Insertion expands over precision alone:
//! `Variant` is a `PhantomData` marker and every insertion method lives on the
//! `impl<Variant, Registers, H>` block, so the two variants share one
//! monomorphization. Estimation expands over both.
//!
//! The merge laws cover both representations: `HyperLogLogImpl` itself and
//! `HllSketch`, the portable one with a register array of its own.
//!
//! Flajolet, Fusy, Gandouet, Meunier, AofA '07.

use crate::support::keys;
use asap_sketchlib::message_pack_format::MessagePackCodec;
use asap_sketchlib::sketches::hll::{HyperLogLogHIPImpl, HyperLogLogImpl};
use asap_sketchlib::{Classic, DataInput, ErtlMLE, HllSketch, HllVariant, HyperLogLog};
use proptest::prelude::*;

asap_sketchlib::impl_hll_bucket_list!(BucketsP4, 4, 1_usize << 4);
asap_sketchlib::impl_hll_bucket_list!(BucketsP6, 6, 1_usize << 6);
asap_sketchlib::impl_hll_bucket_list!(BucketsP8, 8, 1_usize << 8);
asap_sketchlib::impl_hll_bucket_list!(BucketsP10, 10, 1_usize << 10);

/// Register values stay in this band so the sum of `2^-v` is exact in f64: a
/// 41-bit span plus the 10 bits of count at the widest precision here sits
/// inside the 53-bit significand, so the order the terms are added in cannot
/// reach the result.
const MAX_LEADING_ZERO: u8 = 40;

/// How far below `2 * base` a doubled estimate may land, set by the estimator's
/// own conversion to `usize`: `floor(2b)` is `2*floor(b)` or one above, while
/// `round(2b)` reaches one below as well.
const TRUNCATES: i64 = 0;
const ROUNDS: i64 = -1;

/// A distinct set paired with a shuffled stream over it, every key carrying a
/// multiplicity of its own. The repeats are built in rather than left to a
/// collision between random keys: over the whole 64-bit universe a stream of a
/// few hundred draws repeats nothing.
fn set_and_stream(
    max_distinct: usize,
    max_repeat: usize,
) -> impl Strategy<Value = (Vec<u64>, Vec<u64>)> {
    prop::collection::hash_set(any::<u64>(), 0..max_distinct.max(1))
        .prop_flat_map(move |set| {
            let set: Vec<u64> = set.into_iter().collect();
            let repeats = prop::collection::vec(1usize..=max_repeat, set.len());
            (Just(set), repeats)
        })
        .prop_flat_map(|(set, repeats)| {
            let stream: Vec<u64> = set
                .iter()
                .zip(&repeats)
                .flat_map(|(k, r)| std::iter::repeat_n(*k, *r))
                .collect();
            (Just(set), Just(stream).prop_shuffle())
        })
}

fn repeating_stream(max_distinct: usize, max_repeat: usize) -> impl Strategy<Value = Vec<u64>> {
    set_and_stream(max_distinct, max_repeat).prop_map(|(_, stream)| stream)
}

/// Register arrays with no zero entry: the estimator's raw branch, the one the
/// small range correction cannot fire in.
fn dense_registers(m: usize) -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(1u8..=MAX_LEADING_ZERO, m)
}

fn registers_and_permutation(m: usize) -> impl Strategy<Value = (Vec<u8>, Vec<u8>)> {
    prop::collection::vec(0u8..=MAX_LEADING_ZERO, m)
        .prop_flat_map(|v| (Just(v.clone()), Just(v).prop_shuffle()))
}

/// The hash that drives `bucket` to `leading_zero`: the top `precision` bits
/// select the register, the highest set bit below them fixes the count.
fn hash_placing(precision: u32, bucket: usize, leading_zero: u8) -> u64 {
    let payload_bits = 64 - precision;
    ((bucket as u64) << payload_bits) | (1u64 << (payload_bits - leading_zero as u32))
}

/// One register with at least two distinct leading-zero counts to land on it,
/// sorted. Ascending arrival upgrades the register once per value, descending
/// upgrades it once.
fn bucket_and_values(m: usize) -> impl Strategy<Value = (usize, Vec<u8>)> {
    (
        0..m,
        prop::collection::hash_set(1u8..=MAX_LEADING_ZERO, 2..6),
    )
        .prop_map(|(bucket, set)| {
            let mut values: Vec<u8> = set.into_iter().collect();
            values.sort_unstable();
            (bucket, values)
        })
}

/// `sum(2^-M[j])`, the denominator of the HIP increment. Each term is a power
/// of two, so every partial sum is a multiple of `2^-MAX_LEADING_ZERO` under
/// `2^10` and the whole sum is exact in f64.
fn inverse_probability_sum(registers: &[u8]) -> f64 {
    registers.iter().map(|&v| 1.0 / ((1_u64 << v) as f64)).sum()
}

/// A run of placements long enough that the increments have to spread apart:
/// the sum falls as registers fill, so a run of this length separates the HIP
/// estimate from the count of upgrades by more than the estimator's truncation
/// to `usize` can hide.
fn upgrade_plan(m: usize) -> impl Strategy<Value = Vec<(usize, u8)>> {
    prop::collection::vec(
        (0..m, 1u8..=MAX_LEADING_ZERO),
        m.min(192)..=(2 * m).min(384),
    )
}

macro_rules! insertion_laws {
    ($name:ident, $buckets:ty) => {
        mod $name {
            use super::*;

            type S = HyperLogLogImpl<Classic, $buckets>;
            const M: usize = <$buckets>::NUM_REGISTERS;

            fn sketch_of(keys: &[u64]) -> S {
                let mut s = S::new();
                for k in keys {
                    s.insert(&DataInput::U64(*k));
                }
                s
            }

            proptest! {
                // What reaches the registers is the distinct set: neither how
                // often a key arrives nor the order the repeats interleave in
                // can be read back out.
                #[test]
                fn registers_are_determined_by_the_distinct_set(
                    (set, stream) in set_and_stream(3 * M, 4),
                ) {
                    let repeated = sketch_of(&stream);
                    let distinct = sketch_of(&set);
                    prop_assert_eq!(repeated.registers_as_slice(), distinct.registers_as_slice());
                }

                #[test]
                fn the_hashed_entry_point_matches_the_value_one(
                    stream in repeating_stream(3 * M, 4),
                ) {
                    let by_value = sketch_of(&stream);

                    let hashes: Vec<u64> = stream
                        .iter()
                        .map(|k| S::canonical_hash(&DataInput::U64(*k)))
                        .collect();
                    let mut by_hash = S::new();
                    by_hash.insert_many_with_hashes(&hashes);

                    prop_assert_eq!(by_value.registers_as_slice(), by_hash.registers_as_slice());
                }
            }

            // These two walk the whole register array after every arrival, so
            // they run at a lighter load and fewer cases than the laws that
            // read only a final state.
            proptest! {
                #![proptest_config(ProptestConfig::with_cases(48))]

                #[test]
                fn registers_never_decrease(stream in repeating_stream(M, 4)) {
                    let mut sketch = S::new();
                    let mut previous = vec![0u8; M];
                    for k in &stream {
                        sketch.insert(&DataInput::U64(*k));
                        for (j, (now, before)) in
                            sketch.registers_as_slice().iter().zip(&previous).enumerate()
                        {
                            prop_assert!(
                                now >= before,
                                "register {} fell from {} to {} on {}", j, before, now, k
                            );
                        }
                        previous.copy_from_slice(sketch.registers_as_slice());
                    }
                }

                #[test]
                fn one_arrival_raises_at_most_one_register(stream in repeating_stream(M, 4)) {
                    let mut sketch = S::new();
                    let mut previous = vec![0u8; M];
                    for k in &stream {
                        sketch.insert(&DataInput::U64(*k));
                        let changed = sketch
                            .registers_as_slice()
                            .iter()
                            .zip(&previous)
                            .filter(|(now, before)| now != before)
                            .count();
                        prop_assert!(changed <= 1, "inserting {} moved {} registers", k, changed);
                        previous.copy_from_slice(sketch.registers_as_slice());
                    }
                }
            }
        }
    };
}

insertion_laws!(insert_p4, BucketsP4);
insertion_laws!(insert_p6, BucketsP6);
insertion_laws!(insert_p8, BucketsP8);
insertion_laws!(insert_p10, BucketsP10);

macro_rules! estimation_laws {
    ($name:ident, $variant:ty, $buckets:ty, $below:expr) => {
        mod $name {
            use super::*;

            type S = HyperLogLogImpl<$variant, $buckets>;
            const M: usize = <$buckets>::NUM_REGISTERS;
            const P: u32 = <$buckets>::PRECISION as u32;

            /// A sketch whose registers are exactly `target`, built through the
            /// public insertion path. The assertion holds the hash split in
            /// place: a different one fails here rather than handing the laws
            /// below some other state.
            fn sketch_with_registers(target: &[u8]) -> S {
                let mut sketch = S::new();
                for (bucket, &value) in target.iter().enumerate() {
                    if value != 0 {
                        sketch.insert_with_hash(hash_placing(P, bucket, value));
                    }
                }
                assert_eq!(sketch.registers_as_slice(), target, "crafted registers");
                sketch
            }

            // Zero registers put the estimator in linear counting, where
            // `m * ln(m/m)` is zero at every precision.
            #[test]
            fn the_empty_sketch_estimates_zero() {
                assert_eq!(S::new().estimate(), 0);
            }

            proptest! {
                // The registers reach the estimate as a multiset: what the
                // formula reads is the sum of `2^-M[j]`, never the position a
                // register sits at.
                #[test]
                fn the_estimate_ignores_register_order(
                    (registers, permuted) in registers_and_permutation(M),
                ) {
                    prop_assert_eq!(
                        sketch_with_registers(&permuted).estimate(),
                        sketch_with_registers(&registers).estimate()
                    );
                }

                // Raising every register by one halves the sum of `2^-M[j]`, so
                // the raw indicator doubles exactly; the slack is the
                // estimator's own rounding.
                #[test]
                fn raising_every_register_by_one_doubles_the_estimate(
                    registers in dense_registers(M),
                ) {
                    let base = sketch_with_registers(&registers).estimate() as i64;
                    let raised: Vec<u8> = registers.iter().map(|v| v + 1).collect();
                    let doubled = sketch_with_registers(&raised).estimate() as i64;

                    let drift = doubled - 2 * base;
                    prop_assert!(
                        ($below..=1).contains(&drift),
                        "every register raised by one: {} against a base of {}, drift {}",
                        doubled, base, drift
                    );
                }

                // Arrival order reaches the registers only through `max`, and
                // the estimate reads nothing else.
                #[test]
                fn the_estimate_ignores_arrival_order(
                    (bucket, values) in bucket_and_values(M),
                ) {
                    let mut ascending = S::new();
                    for v in &values {
                        ascending.insert_with_hash(hash_placing(P, bucket, *v));
                    }
                    let mut descending = S::new();
                    for v in values.iter().rev() {
                        descending.insert_with_hash(hash_placing(P, bucket, *v));
                    }

                    let mut expected = vec![0u8; M];
                    expected[bucket] = *values.last().expect("at least two values");
                    prop_assert_eq!(ascending.registers_as_slice(), &expected[..]);
                    prop_assert_eq!(descending.registers_as_slice(), &expected[..]);

                    prop_assert_eq!(ascending.estimate(), descending.estimate());
                }

                // A higher register contributes less to the sum the estimate
                // divides by.
                #[test]
                fn the_estimate_never_falls_when_a_register_rises(
                    registers in dense_registers(M),
                    index in 0..M,
                    lift in 1u8..8,
                ) {
                    let before = sketch_with_registers(&registers).estimate();
                    let mut raised = registers.clone();
                    raised[index] = (raised[index] + lift).min(MAX_LEADING_ZERO);
                    let after = sketch_with_registers(&raised).estimate();

                    prop_assert!(
                        after >= before,
                        "register {} at {}: {} fell from {}",
                        index, raised[index], after, before
                    );
                }
            }
        }
    };
}

estimation_laws!(classic_p4, Classic, BucketsP4, TRUNCATES);
estimation_laws!(classic_p6, Classic, BucketsP6, TRUNCATES);
estimation_laws!(classic_p8, Classic, BucketsP8, TRUNCATES);
estimation_laws!(classic_p10, Classic, BucketsP10, TRUNCATES);
estimation_laws!(ertl_p4, ErtlMLE, BucketsP4, ROUNDS);
estimation_laws!(ertl_p6, ErtlMLE, BucketsP6, ROUNDS);
estimation_laws!(ertl_p8, ErtlMLE, BucketsP8, ROUNDS);
estimation_laws!(ertl_p10, ErtlMLE, BucketsP10, ROUNDS);

macro_rules! merge_laws {
    ($name:ident, $buckets:ty) => {
        mod $name {
            use super::*;

            type S = HyperLogLogImpl<Classic, $buckets>;
            const M: usize = <$buckets>::NUM_REGISTERS;

            fn sketch_of(keys: &[u64]) -> S {
                let mut s = S::new();
                for k in keys {
                    s.insert(&DataInput::U64(*k));
                }
                s
            }

            fn two_streams() -> impl Strategy<Value = (Vec<u64>, Vec<u64>)> {
                (repeating_stream(M, 4), repeating_stream(M, 4))
            }

            proptest! {
                // Cardinality sketches merge by `max`, the one operation that
                // cannot double count what both sides already saw.
                #[test]
                fn merge_is_the_elementwise_maximum((a, b) in two_streams()) {
                    let left = sketch_of(&a);
                    let right = sketch_of(&b);
                    let expected: Vec<u8> = left
                        .registers_as_slice()
                        .iter()
                        .zip(right.registers_as_slice())
                        .map(|(x, y)| *x.max(y))
                        .collect();

                    let mut merged = left.clone();
                    merged.merge(&right);

                    prop_assert_eq!(merged.registers_as_slice(), &expected[..]);
                }

                #[test]
                fn merge_is_commutative((a, b) in two_streams()) {
                    let mut ab = sketch_of(&a);
                    ab.merge(&sketch_of(&b));
                    let mut ba = sketch_of(&b);
                    ba.merge(&sketch_of(&a));

                    prop_assert_eq!(ab.registers_as_slice(), ba.registers_as_slice());
                }

                #[test]
                fn merge_is_idempotent(a in repeating_stream(M, 4)) {
                    let mut sketch = sketch_of(&a);
                    let before = sketch.registers_as_slice().to_vec();
                    let again = sketch.clone();
                    sketch.merge(&again);

                    prop_assert_eq!(sketch.registers_as_slice(), &before[..]);
                }

                #[test]
                fn the_empty_sketch_is_a_merge_identity(a in repeating_stream(M, 4)) {
                    let mut sketch = sketch_of(&a);
                    let before = sketch.registers_as_slice().to_vec();
                    sketch.merge(&S::new());

                    prop_assert_eq!(sketch.registers_as_slice(), &before[..]);
                }

                #[test]
                fn merge_equals_streaming_the_concatenation((a, b) in two_streams()) {
                    let mut merged = sketch_of(&a);
                    merged.merge(&sketch_of(&b));

                    let concatenated: Vec<u64> = a.iter().chain(&b).copied().collect();
                    let streamed = sketch_of(&concatenated);

                    prop_assert_eq!(merged.registers_as_slice(), streamed.registers_as_slice());
                }
            }
        }
    };
}

merge_laws!(merge_p4, BucketsP4);
merge_laws!(merge_p6, BucketsP6);
merge_laws!(merge_p8, BucketsP8);
merge_laws!(merge_p10, BucketsP10);

// `HyperLogLogHIPImpl` exposes no merge and keeps its registers private, so
// the register state reaches these laws through the ASAPv1 envelope. That
// estimate accumulates one increment per register upgrade, which makes it a
// function of the arrival order and not of the registers alone - the opposite
// of the law the other two variants obey.
macro_rules! hip_laws {
    ($name:ident, $buckets:ty) => {
        mod $name {
            use super::*;

            type S = HyperLogLogHIPImpl<$buckets>;
            const M: usize = <$buckets>::NUM_REGISTERS;
            const P: u32 = <$buckets>::PRECISION as u32;

            /// The registers, read back through the wire format. The struct
            /// holds them privately behind the `kxq0`/`kxq1` accumulators, and
            /// those accumulators are what the increment law checks, so the
            /// envelope is the one view of the registers that stays independent
            /// of it.
            fn registers_of(sketch: &S) -> Vec<u8> {
                let bytes = sketch.to_msgpack().expect("encode");
                HllSketch::from_msgpack(&bytes).expect("decode").registers
            }

            #[test]
            fn the_empty_sketch_estimates_zero() {
                assert_eq!(S::new().estimate(), 0);
            }

            proptest! {
                #![proptest_config(ProptestConfig::with_cases(48))]

                #[test]
                fn the_estimate_never_decreases(stream in repeating_stream(M, 4)) {
                    let mut sketch = S::new();
                    let mut previous = sketch.estimate();
                    for k in &stream {
                        sketch.insert(&DataInput::U64(*k));
                        let now = sketch.estimate();
                        prop_assert!(now >= previous, "estimate fell to {} on {}", now, k);
                        previous = now;
                    }
                }

                #[test]
                fn a_repeated_key_leaves_the_estimate(stream in repeating_stream(M, 4)) {
                    let mut sketch = S::new();
                    let mut seen = std::collections::HashSet::new();
                    for k in &stream {
                        let before = sketch.estimate();
                        sketch.insert(&DataInput::U64(*k));
                        if !seen.insert(*k) {
                            prop_assert_eq!(
                                sketch.estimate(),
                                before,
                                "a second {} moved the estimate", k
                            );
                        }
                    }
                }
            }

            proptest! {
                // Ascending arrival upgrades the register once per value and
                // each upgrade adds a positive increment; descending upgrades
                // it once, for an increment of exactly `m / m`.
                #[test]
                fn the_estimate_depends_on_arrival_order(
                    (bucket, values) in bucket_and_values(M),
                ) {
                    let mut ascending = S::new();
                    for v in &values {
                        ascending.insert_with_hash(hash_placing(P, bucket, *v));
                    }
                    let mut descending = S::new();
                    for v in values.iter().rev() {
                        descending.insert_with_hash(hash_placing(P, bucket, *v));
                    }

                    prop_assert_eq!(descending.estimate(), 1);
                    prop_assert!(
                        ascending.estimate() > descending.estimate(),
                        "{} values into register {}: ascending {} against descending {}",
                        values.len(), bucket, ascending.estimate(), descending.estimate()
                    );
                }
            }

            proptest! {
                #![proptest_config(ProptestConfig::with_cases(24))]

                // The HIP increment: an arrival that raises a register adds
                // `m / sum(2^-M[j])` over the register state M that preceded
                // it, and an arrival that raises nothing adds nothing. Lang,
                // arXiv:1708.06839.
                //
                // Both sides are exact in f64 - the sum, the division, and the
                // running total are the same operations in the same order - so
                // the truncation to `usize` is compared, not tolerated.
                #[test]
                fn every_upgrade_adds_m_over_the_inverse_probability_sum(
                    plan in upgrade_plan(M),
                ) {
                    let mut sketch = S::new();
                    let mut before = registers_of(&sketch);
                    let mut expected = 0.0_f64;

                    for (step, (bucket, value)) in plan.iter().enumerate() {
                        sketch.insert_with_hash(hash_placing(P, *bucket, *value));
                        let after = registers_of(&sketch);
                        if after != before {
                            expected += M as f64 / inverse_probability_sum(&before);
                        }

                        prop_assert_eq!(
                            sketch.estimate(),
                            expected as usize,
                            "step {}, register {} to {}: {} against {}",
                            step, bucket, value, sketch.estimate(), expected
                        );
                        before = after;
                    }
                }
            }
        }
    };
}

hip_laws!(hip_p4, BucketsP4);
hip_laws!(hip_p6, BucketsP6);
hip_laws!(hip_p8, BucketsP8);
hip_laws!(hip_p10, BucketsP10);

fn byte_items(max: usize) -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::vec(prop::collection::vec(any::<u8>(), 1..16), 0..max)
}

fn portable_hll_of(precision: u32, items: &[Vec<u8>]) -> HllSketch {
    let mut s = HllSketch::new(HllVariant::Regular, precision);
    for i in items {
        s.update(i);
    }
    s
}

proptest! {
    // ===== Merge =====

    #[test]
    fn hll_register_merge_is_commutative_and_idempotent(
        precision in 4u32..12,
        a in byte_items(50),
        b in byte_items(50),
    ) {
        let mut ab = portable_hll_of(precision, &a);
        ab.merge(&portable_hll_of(precision, &b)).expect("merge");
        let mut ba = portable_hll_of(precision, &b);
        ba.merge(&portable_hll_of(precision, &a)).expect("merge");

        prop_assert_eq!(&ab.registers, &ba.registers);

        let before = ab.registers.clone();
        let again = ab.clone();
        ab.merge(&again).expect("merge");
        prop_assert_eq!(&ab.registers, &before);
    }

    #[test]
    fn hll_merge_equals_streaming_the_concatenation(
        precision in 4u32..12,
        a in byte_items(50),
        b in byte_items(50),
    ) {
        let mut merged = portable_hll_of(precision, &a);
        merged.merge(&portable_hll_of(precision, &b)).expect("merge");

        let concatenated: Vec<_> = a.iter().chain(b.iter()).cloned().collect();
        let streamed = portable_hll_of(precision, &concatenated);

        prop_assert_eq!(&merged.registers, &streamed.registers);
    }

    #[test]
    fn hll_empty_is_a_merge_identity(
        precision in 4u32..12,
        a in byte_items(50),
    ) {
        let base = portable_hll_of(precision, &a);
        let mut merged = base.clone();
        merged.merge(&HllSketch::new(HllVariant::Regular, precision)).expect("merge");

        prop_assert_eq!(&merged.registers, &base.registers);
    }
}

fn hll_variant() -> impl Strategy<Value = HllVariant> {
    prop_oneof![
        Just(HllVariant::Regular),
        Just(HllVariant::Datafusion),
        Just(HllVariant::Hip),
    ]
}

proptest! {
    // ===== Wire =====

    #[test]
    fn hll_round_trip_preserves_registers(
        variant in hll_variant(),
        precision in 4u32..14,
        items in byte_items(64),
    ) {
        let mut s = HllSketch::new(variant, precision);
        for item in &items {
            s.update(item);
        }

        let bytes = s.to_msgpack().expect("encode");
        let restored = HllSketch::from_msgpack(&bytes).expect("decode");

        prop_assert_eq!(restored.variant, s.variant);
        prop_assert_eq!(restored.precision, s.precision);
        prop_assert_eq!(&restored.registers, &s.registers);
        prop_assert_eq!(restored.estimate(), s.estimate());
    }

    #[test]
    fn hyperloglog_round_trips_including_the_empty_sketch(stream in keys(400)) {
        type Hll = HyperLogLog<ErtlMLE>;
        let mut sketch = Hll::new();
        for k in &stream {
            sketch.insert(&DataInput::U64(*k));
        }

        round_trip!(
            Hll,
            sketch,
            |s: &Hll| s.registers_as_slice().to_vec(),
            |s: &Hll| s.estimate(),
        );
    }
}
