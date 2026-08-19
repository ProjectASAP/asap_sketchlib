//! Exercises `impl_hll_bucket_list!` from outside the crate.
//!
//! This is an integration test, so it compiles as a separate crate: it only
//! sees `asap_sketchlib`'s public surface and does not depend on `serde` or
//! `serde-big-array` itself. That makes it a real check that the exported
//! macro is usable downstream — see issue #94.
//!
//! The precisions here are deliberately outside the built-in
//! {12, 14, 16} set, including `lg_k = 18` (needed by sketch-bench's
//! `lkarger` search space).

use asap_sketchlib::sketches::hll::{HyperLogLogHIPImpl, HyperLogLogImpl};
use asap_sketchlib::{Classic, DataInput, HllRegisterStorage};

asap_sketchlib::impl_hll_bucket_list!(HllBucketListP4, 4, 1_usize << 4);
asap_sketchlib::impl_hll_bucket_list!(HllBucketListP8, 8, 1_usize << 8);
asap_sketchlib::impl_hll_bucket_list!(HllBucketListP10, 10, 1_usize << 10);
asap_sketchlib::impl_hll_bucket_list!(HllBucketListP13, 13, 1_usize << 13);
asap_sketchlib::impl_hll_bucket_list!(HllBucketListP18, 18, 1_usize << 18);

/// Distinct, deterministic inputs so every assertion below is reproducible.
fn item(i: usize) -> String {
    format!("key-{i:08}")
}

/// Checks the storage constants, then builds Classic and HIP sketches over
/// `cardinality` distinct items and asserts both land within `tolerance`
/// relative error.
fn check_precision<R: HllRegisterStorage>(lg_k: usize, cardinality: usize, tolerance: f64) {
    let num_registers = 1_usize << lg_k;
    assert_eq!(R::PRECISION, lg_k, "PRECISION for lg_k={lg_k}");
    assert_eq!(
        R::NUM_REGISTERS,
        num_registers,
        "NUM_REGISTERS for lg_k={lg_k}"
    );
    assert_eq!(R::REGISTER_BITS, 64 - lg_k, "REGISTER_BITS for lg_k={lg_k}");
    assert_eq!(
        R::P_MASK,
        (num_registers as u64) - 1,
        "P_MASK for lg_k={lg_k}"
    );

    let storage = R::default();
    assert_eq!(storage.len(), num_registers);
    assert_eq!(storage.as_slice().len(), num_registers);
    assert!(!storage.is_empty());
    assert!(
        storage.as_slice().iter().all(|&r| r == 0),
        "fresh storage for lg_k={lg_k} should be zeroed"
    );

    let mut classic = HyperLogLogImpl::<Classic, R>::new();
    let mut hip = HyperLogLogHIPImpl::<R>::new();
    for i in 0..cardinality {
        let value = DataInput::String(item(i));
        classic.insert(&value);
        hip.insert(&value);
    }

    // Registers really were written, and none exceeds the rank ceiling.
    let touched = classic
        .registers_as_slice()
        .iter()
        .filter(|&&r| r > 0)
        .count();
    assert!(
        touched > 0,
        "no register was set for lg_k={lg_k} after {cardinality} inserts"
    );
    assert!(
        classic
            .registers_as_slice()
            .iter()
            .all(|&r| (r as usize) <= R::REGISTER_BITS + 1),
        "register rank out of range for lg_k={lg_k}"
    );

    for (name, estimate) in [("classic", classic.estimate()), ("hip", hip.estimate())] {
        let error = (estimate as f64 - cardinality as f64).abs() / cardinality as f64;
        assert!(
            error < tolerance,
            "{name} estimate at lg_k={lg_k}: got {estimate}, want ~{cardinality} \
             (relative error {error:.4} >= tolerance {tolerance})"
        );
    }
}

#[test]
fn custom_precisions_estimate_correctly() {
    // Tolerances are ~4x the 1.04/sqrt(m) standard error for the precision,
    // which is comfortably loose for these fixed inputs (the inputs are
    // deterministic, so these assertions cannot flake).
    check_precision::<HllBucketListP4>(4, 100, 1.10);
    check_precision::<HllBucketListP8>(8, 1_000, 0.30);
    check_precision::<HllBucketListP10>(10, 10_000, 0.15);
    check_precision::<HllBucketListP13>(13, 50_000, 0.06);
    check_precision::<HllBucketListP18>(18, 200_000, 0.02);
}

#[test]
fn custom_precision_merges() {
    let mut left = HyperLogLogImpl::<Classic, HllBucketListP18>::new();
    let mut right = HyperLogLogImpl::<Classic, HllBucketListP18>::new();
    for i in 0..60_000 {
        left.insert(&DataInput::String(item(i)));
    }
    // Overlapping halves: the union is 100_000 distinct items.
    for i in 40_000..100_000 {
        right.insert(&DataInput::String(item(i)));
    }

    left.merge(&right);
    let estimate = left.estimate();
    let error = (estimate as f64 - 100_000.0).abs() / 100_000.0;
    assert!(
        error < 0.02,
        "merged estimate at lg_k=18: got {estimate}, want ~100000 (relative error {error:.4})"
    );
}

#[test]
fn custom_precision_indexing_and_iteration() {
    let mut storage = HllBucketListP10::default();
    storage[0] = 7;
    storage[HllBucketListP10::NUM_REGISTERS - 1] = 3;
    assert_eq!(storage[0], 7);
    assert_eq!(storage[HllBucketListP10::NUM_REGISTERS - 1], 3);
    assert_eq!(&storage[0..2], &[7, 0]);

    storage[1..3].copy_from_slice(&[1, 2]);
    assert_eq!(storage.as_slice()[1..3], [1, 2]);

    let sum: u32 = (&storage).into_iter().map(|&r| r as u32).sum();
    assert_eq!(sum, 7 + 1 + 2 + 3);
}

#[test]
fn custom_precision_round_trips_through_serde() {
    let mut sketch = HyperLogLogImpl::<Classic, HllBucketListP13>::new();
    for i in 0..5_000 {
        sketch.insert(&DataInput::String(item(i)));
    }

    let bytes = rmp_serde::to_vec(&sketch).expect("serialize");
    let restored: HyperLogLogImpl<Classic, HllBucketListP13> =
        rmp_serde::from_slice(&bytes).expect("deserialize");

    assert_eq!(restored.registers_as_slice(), sketch.registers_as_slice());
    assert_eq!(restored.estimate(), sketch.estimate());
}
