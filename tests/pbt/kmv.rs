//! Property tests for KMV, the k-minimum-values cardinality sketch.
//!
//! The retained set is exactly the k smallest distinct hashes, which makes
//! most of this family's laws exact equalities rather than bounds. The oracle
//! is that definition read literally: a `BTreeSet` over the stream, truncated
//! to k.
//!
//! Most laws drive `insert_by_hash`, so the stream the oracle reads is the
//! stream the sketch retains from and no hash function stands between them.
//! One law covers the key path separately, pinning what the wire format
//! declares: each key is hashed once, at the canonical seed.
//!
//! `estimate` takes `&mut self` without mutating, so a round-trip probe -
//! which is handed `&T` - clones and estimates on the copy.
//!
//! Beyer, Haas, Reinwald, Sismanis, Gemulla, SIGMOD '07.

use std::collections::BTreeSet;

use crate::support::keys;
use asap_sketchlib::{CANONICAL_HASH_SEED, DataInput, KMV, hash64_seeded};
use proptest::prelude::*;

type Kmv = KMV;

/// Retention bounds small enough that a few hundred draws cross them.
fn bound() -> impl Strategy<Value = usize> {
    prop_oneof![Just(1usize), Just(2), Just(3), Just(8), Just(32)]
}

/// Bounds the estimate is defined over. At `k` of 1 the numerator `k - 1` is
/// zero, so the estimate is zero for every non-empty stream and the laws
/// relating it to the retained set do not hold.
fn estimating_bound() -> impl Strategy<Value = usize> {
    prop_oneof![Just(2usize), Just(3), Just(8), Just(32)]
}

/// Hash streams on both sides of the bound: a domain under the smallest bound,
/// one straddling the largest, and the full 64-bit range, which leaves every
/// bound far behind.
fn hash_stream(max: usize) -> impl Strategy<Value = Vec<u64>> {
    prop_oneof![
        prop::collection::vec(0u64..2, 0..max),
        prop::collection::vec(0u64..48, 0..max),
        prop::collection::vec(any::<u64>(), 0..max),
    ]
}

/// A distinct set the bound cannot reach, paired with a bound above it and a
/// shuffled stream that carries every member twice.
fn under_the_bound(max_distinct: usize) -> impl Strategy<Value = (usize, Vec<u64>, Vec<u64>)> {
    prop::collection::hash_set(any::<u64>(), 0..max_distinct).prop_flat_map(move |set| {
        let distinct: Vec<u64> = set.into_iter().collect();
        let stream: Vec<u64> = distinct.iter().chain(distinct.iter()).copied().collect();
        (
            (distinct.len() + 1)..=(max_distinct + 8),
            Just(distinct),
            Just(stream).prop_shuffle(),
        )
    })
}

fn stream_and_permutation(max: usize) -> impl Strategy<Value = (Vec<u64>, Vec<u64>)> {
    hash_stream(max).prop_flat_map(|v| (Just(v.clone()), Just(v).prop_shuffle()))
}

/// Two streams of k distinct hashes that agree on their k - 1 smallest and
/// differ in the largest, the second's being strictly the larger. The hashes
/// are spaced `1 << 32` apart so that distinct values stay distinct through
/// the truncation the estimator reads them under.
fn two_full_streams() -> impl Strategy<Value = (usize, Vec<u64>, Vec<u64>)> {
    estimating_bound()
        .prop_flat_map(|k| {
            (
                Just(k),
                prop::collection::hash_set(any::<u32>(), k + 1..k + 2),
            )
        })
        .prop_map(|(k, set)| {
            let mut values: Vec<u64> = set.into_iter().map(|v| (v as u64) << 32).collect();
            values.sort_unstable();
            let mut higher = values[..k - 1].to_vec();
            higher.push(values[k]);
            (k, values[..k].to_vec(), higher)
        })
}

/// The retained hashes, ascending. The heap's array order follows the arrivals,
/// so the set is read sorted.
fn retained(sketch: &Kmv) -> Vec<u64> {
    let mut hashes: Vec<u64> = sketch.k_vals.iter().copied().collect();
    hashes.sort_unstable();
    hashes
}

/// The oracle: the k smallest distinct values of a stream, ascending.
fn smallest_distinct(stream: &[u64], k: usize) -> Vec<u64> {
    stream
        .iter()
        .copied()
        .collect::<BTreeSet<u64>>()
        .into_iter()
        .take(k)
        .collect()
}

fn from_hashes(k: usize, stream: &[u64]) -> Kmv {
    let mut sketch = Kmv::new(k);
    for hash in stream {
        sketch.insert_by_hash(*hash);
    }
    sketch
}

fn merged(k: usize, left: &[u64], right: &[u64]) -> Kmv {
    let mut a = from_hashes(k, left);
    let mut b = from_hashes(k, right);
    a.merge(&mut b);
    a
}

proptest! {
    // ===== Retention =====

    #[test]
    fn the_retained_hashes_are_the_k_smallest_distinct_hashes_of_the_stream(
        k in bound(),
        stream in hash_stream(300),
    ) {
        let sketch = from_hashes(k, &stream);

        prop_assert_eq!(
            retained(&sketch),
            smallest_distinct(&stream, k),
            "k {}, {} arrivals",
            k,
            stream.len()
        );
        prop_assert_eq!(sketch.k_vals.capacity(), k);
    }

    #[test]
    fn reinserting_a_hash_the_stream_already_carried_changes_nothing(
        k in bound(),
        (stream, again) in stream_and_permutation(300),
    ) {
        let mut sketch = from_hashes(k, &stream);
        let before = retained(&sketch);

        for hash in &again {
            sketch.insert_by_hash(*hash);
        }

        prop_assert_eq!(
            retained(&sketch),
            before,
            "k {}, {} arrivals replayed",
            k,
            again.len()
        );
    }

    #[test]
    fn the_retained_set_does_not_depend_on_the_arrival_order(
        k in bound(),
        (stream, shuffled) in stream_and_permutation(300),
    ) {
        prop_assert_eq!(
            retained(&from_hashes(k, &shuffled)),
            retained(&from_hashes(k, &stream)),
            "k {}, {} arrivals",
            k,
            stream.len()
        );
    }

    /// The wire format declares one hash per key at the canonical seed index,
    /// so the key path must land on exactly the hashes that seed names.
    #[test]
    fn the_key_path_hashes_each_key_once_at_the_canonical_seed(
        k in bound(),
        stream in keys(300),
    ) {
        let mut sketch = Kmv::new(k);
        for key in &stream {
            sketch.insert(&DataInput::U64(*key));
        }

        let hashed: Vec<u64> = stream
            .iter()
            .map(|key| hash64_seeded(CANONICAL_HASH_SEED, &DataInput::U64(*key)))
            .collect();

        prop_assert_eq!(
            retained(&sketch),
            smallest_distinct(&hashed, k),
            "k {}, {} keys",
            k,
            stream.len()
        );
    }

    // ===== Estimation =====

    #[test]
    fn the_estimate_never_falls_as_the_stream_runs(
        k in estimating_bound(),
        stream in hash_stream(300),
    ) {
        let mut sketch = Kmv::new(k);
        let mut previous = sketch.estimate();

        for (i, hash) in stream.iter().enumerate() {
            sketch.insert_by_hash(*hash);
            let now = sketch.estimate();
            prop_assert!(
                now >= previous,
                "k {}: the estimate fell from {} to {} at arrival {} of {}",
                k,
                previous,
                now,
                i,
                stream.len()
            );
            previous = now;
        }
    }

    #[test]
    fn a_sketch_under_its_bound_estimates_its_distinct_count_exactly(
        (k, distinct, stream) in under_the_bound(48),
    ) {
        let mut sketch = from_hashes(k, &stream);

        prop_assert_eq!(sketch.k_vals.len(), distinct.len());
        prop_assert_eq!(
            sketch.estimate(),
            distinct.len() as f64,
            "k {}, {} distinct hashes over {} arrivals",
            k,
            distinct.len(),
            stream.len()
        );
    }

    /// Once the bound is reached the estimate is read off the largest retained
    /// hash, so two full sketches holding a different one cannot report the
    /// same cardinality: the smaller maximum stands for the denser stream.
    #[test]
    fn a_full_sketch_estimates_above_one_holding_a_larger_k_th_hash(
        (k, lower, higher) in two_full_streams(),
    ) {
        let mut lower_sketch = from_hashes(k, &lower);
        let mut higher_sketch = from_hashes(k, &higher);
        prop_assert_eq!(lower_sketch.k_vals.len(), k);
        prop_assert_eq!(higher_sketch.k_vals.len(), k);

        let (near, far) = (lower_sketch.estimate(), higher_sketch.estimate());
        prop_assert!(
            near > far,
            "k {}: maxima {} and {} estimate {} and {}",
            k,
            lower[k - 1],
            higher[k - 1],
            near,
            far
        );
    }

    // ===== Merge =====

    #[test]
    fn a_merge_retains_the_k_smallest_distinct_hashes_of_both_streams(
        k in bound(),
        left in hash_stream(200),
        right in hash_stream(200),
    ) {
        let mut a = from_hashes(k, &left);
        let mut b = from_hashes(k, &right);
        let read_before = retained(&b);
        a.merge(&mut b);

        let both: Vec<u64> = left.iter().chain(right.iter()).copied().collect();
        prop_assert_eq!(
            retained(&a),
            smallest_distinct(&both, k),
            "k {}, {} and {} arrivals",
            k,
            left.len(),
            right.len()
        );
        prop_assert_eq!(a.k_vals.capacity(), k);
        prop_assert_eq!(retained(&b), read_before, "the merge changed the operand it read");
    }

    #[test]
    fn a_merge_does_not_depend_on_the_order_or_the_grouping_of_its_operands(
        k in bound(),
        first in hash_stream(150),
        second in hash_stream(150),
        third in hash_stream(150),
    ) {
        prop_assert_eq!(
            retained(&merged(k, &second, &first)),
            retained(&merged(k, &first, &second)),
            "k {}: merging in the other order",
            k
        );

        let mut left_first = merged(k, &first, &second);
        let mut third_sketch = from_hashes(k, &third);
        left_first.merge(&mut third_sketch);

        let mut right_first = from_hashes(k, &first);
        let mut tail = merged(k, &second, &third);
        right_first.merge(&mut tail);

        prop_assert_eq!(
            retained(&left_first),
            retained(&right_first),
            "k {}: regrouping the three merges",
            k
        );
    }

    #[test]
    fn merging_a_sketch_with_a_copy_of_itself_changes_nothing(
        k in bound(),
        stream in hash_stream(300),
    ) {
        let mut sketch = from_hashes(k, &stream);
        let before = retained(&sketch);
        let mut copy = sketch.clone();
        sketch.merge(&mut copy);

        prop_assert_eq!(
            retained(&sketch),
            before,
            "k {}, {} arrivals",
            k,
            stream.len()
        );
    }

    // ===== Wire =====

    #[test]
    fn a_kmv_round_trips_through_its_envelope_including_the_empty_sketch(
        k in bound(),
        stream in hash_stream(300),
    ) {
        let sketch = from_hashes(k, &stream);

        round_trip!(
            Kmv,
            sketch,
            retained,
            |s: &Kmv| s.k,
            |s: &Kmv| s.clone().estimate().to_bits(),
        );
    }
}
