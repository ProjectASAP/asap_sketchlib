//! Property tests for Space-Saving.
//!
//! Metwally, Agrawal, El Abbadi, ICDT '05. The oracle is an exact `HashMap`
//! count of the generated stream, independent of the summary's own
//! bookkeeping. Every bound asserted here is deterministic, so a
//! counterexample is a defect rather than an unlucky draw.

use crate::support::{PROBE_KEYS, edge_dimension};
use asap_sketchlib::{DataInput, DefaultXxHasher, HeapItem, SpaceSaving};
use proptest::prelude::*;
use std::collections::HashMap;

fn keys(max: usize) -> impl Strategy<Value = Vec<i64>> {
    prop::collection::vec(0i64..32, 0..max)
}

/// A capacity paired with two streams over a domain barely wider than it, so
/// both sides monitor the same keys and their merged union fits the capacity.
fn crowded_pair(max: usize) -> impl Strategy<Value = (usize, Vec<i64>, Vec<i64>)> {
    (1usize..12).prop_flat_map(move |capacity| {
        let domain = capacity as i64 + 3;
        (
            Just(capacity),
            prop::collection::vec(0..domain, 0..max),
            prop::collection::vec(0..domain, 0..max),
        )
    })
}

/// Weights drawn so that unit arrivals, the regime that evicts most, stay the
/// common case while heavy ones still appear.
fn weight() -> impl Strategy<Value = u64> {
    prop_oneof![3 => Just(1u64), 1 => 2u64..1_000]
}

fn weighted_keys(max: usize) -> impl Strategy<Value = Vec<(i64, u64)>> {
    prop::collection::vec((0i64..32, weight()), 0..max)
}

/// A stream over a domain narrow enough that a wide summary still has counters
/// to spare after absorbing two narrow ones.
fn narrow_keys(max: usize) -> impl Strategy<Value = Vec<i64>> {
    prop::collection::vec(0i64..10, 0..max)
}

/// A capacity paired with two orderings of one stream over a domain no wider
/// than the capacity, so every distinct key holds a counter of its own and
/// nothing is ever evicted.
fn roomy_pair(max: usize) -> impl Strategy<Value = (usize, Vec<i64>, Vec<i64>)> {
    (1usize..12)
        .prop_flat_map(move |capacity| {
            (
                Just(capacity),
                prop::collection::vec(0..capacity as i64, 0..max),
            )
        })
        .prop_flat_map(|(capacity, stream)| {
            (
                Just(capacity),
                Just(stream.clone()),
                Just(stream).prop_shuffle(),
            )
        })
}

/// A capacity paired with a weighted stream over a domain no wider than it,
/// with weights small enough to expand into unit arrivals.
fn roomy_weighted(max: usize) -> impl Strategy<Value = (usize, Vec<(i64, u64)>)> {
    (1usize..12).prop_flat_map(move |capacity| {
        (
            Just(capacity),
            prop::collection::vec((0..capacity as i64, 1u64..8), 0..max),
        )
    })
}

/// The exact counts as the summary would hold them when nothing is evicted.
fn exact_counters(truth: &HashMap<i64, u64>) -> HashMap<i64, (u64, u64)> {
    truth.iter().map(|(k, t)| (*k, (*t, 0))).collect()
}

fn truth_of(stream: &[i64]) -> HashMap<i64, u64> {
    let mut counts: HashMap<i64, u64> = HashMap::new();
    for k in stream {
        *counts.entry(*k).or_default() += 1;
    }
    counts
}

fn weighted_truth_of(stream: &[(i64, u64)]) -> HashMap<i64, u64> {
    let mut counts: HashMap<i64, u64> = HashMap::new();
    for (k, w) in stream {
        *counts.entry(*k).or_default() += w;
    }
    counts
}

fn summary_of(capacity: usize, stream: &[i64]) -> SpaceSaving {
    let mut s = SpaceSaving::with_capacity(capacity);
    for k in stream {
        s.insert(&DataInput::I64(*k));
    }
    s
}

fn weighted_summary_of(capacity: usize, stream: &[(i64, u64)]) -> SpaceSaving {
    let mut s = SpaceSaving::with_capacity(capacity);
    for (k, w) in stream {
        s.insert_many(&DataInput::I64(*k), *w);
    }
    s
}

fn key_of(item: &HeapItem) -> i64 {
    match item {
        HeapItem::I64(v) => *v,
        other => panic!("unexpected key form {other:?}"),
    }
}

/// The monitored keys as `key -> (count, error)`.
fn monitored(s: &SpaceSaving) -> HashMap<i64, (u64, u64)> {
    s.entries()
        .iter()
        .map(|(item, count, error)| (key_of(item), (*count, *error)))
        .collect()
}

/// The smallest count the summary still holds.
fn lowest_count(s: &SpaceSaving) -> Option<u64> {
    s.entries().iter().map(|(_, count, _)| *count).min()
}

proptest! {
    // ===== Bounds against the exact counts =====

    #[test]
    fn counts_straddle_the_truth(
        capacity in 1usize..40,
        stream in weighted_keys(400),
    ) {
        let s = weighted_summary_of(capacity, &stream);
        let truth = weighted_truth_of(&stream);

        for (key, (count, error)) in monitored(&s) {
            let t = *truth.get(&key).expect("a monitored key the stream never carried");
            prop_assert!(error <= count, "key {}: error {} above count {}", key, error, count);
            prop_assert!(count >= t, "key {}: count {} below truth {}", key, count, t);
            prop_assert!(
                count - error <= t,
                "key {}: count {} less error {} above truth {}", key, count, error, t
            );
        }
    }

    #[test]
    fn upper_bound_never_falls_below_the_truth(
        capacity in 1usize..40,
        stream in weighted_keys(400),
    ) {
        let s = weighted_summary_of(capacity, &stream);

        for (key, t) in weighted_truth_of(&stream) {
            let bound = s.upper_bound(&DataInput::I64(key));
            prop_assert!(bound >= t, "key {}: ceiling {} below truth {}", key, bound, t);
        }
    }

    #[test]
    fn a_key_outside_the_stream_reports_the_unmonitored_ceiling(
        capacity in 1usize..40,
        stream in keys(400),
    ) {
        let s = summary_of(capacity, &stream);
        let absent = DataInput::I64(1_000);

        prop_assert_eq!(s.estimate(&absent), 0);
        prop_assert_eq!(s.upper_bound(&absent), s.min_count());
        prop_assert_eq!(s.error(&absent), s.min_count());
        prop_assert!(!s.is_guaranteed(&absent));
    }

    // ===== Structural invariants =====

    #[test]
    fn the_summary_never_outgrows_its_capacity(
        capacity in 1usize..40,
        stream in keys(400),
    ) {
        let s = summary_of(capacity, &stream);
        let distinct = truth_of(&stream).len();

        prop_assert_eq!(s.capacity(), capacity);
        prop_assert!(s.len() <= s.capacity(), "{} counters over {}", s.len(), s.capacity());
        prop_assert_eq!(s.len(), distinct.min(capacity));
    }

    #[test]
    fn every_error_sits_under_the_unmonitored_ceiling(
        capacity in 1usize..40,
        stream in keys(400),
    ) {
        let s = summary_of(capacity, &stream);
        let ceiling = s.min_count();

        for (key, (_, error)) in monitored(&s) {
            prop_assert!(error <= ceiling, "key {}: error {} above ceiling {}", key, error, ceiling);
        }
    }

    #[test]
    fn unit_counters_sum_to_the_total_weight(
        capacity in 1usize..40,
        stream in keys(400),
    ) {
        let s = summary_of(capacity, &stream);
        let held: u64 = s.entries().iter().map(|(_, count, _)| count).sum();

        prop_assert_eq!(s.total(), stream.len() as u64);
        prop_assert_eq!(held, s.total());
    }

    /// The ceiling is pinned from both sides: it covers every counter the
    /// summary could still evict, and a full summary of `capacity` counters
    /// all at or above it cannot have absorbed less than their sum.
    #[test]
    fn a_full_summarys_ceiling_sits_between_its_smallest_counter_and_the_average(
        capacity in 1usize..40,
        stream in keys(400),
    ) {
        let s = summary_of(capacity, &stream);
        prop_assume!(s.len() == s.capacity());
        let lowest = lowest_count(&s).expect("a full summary holds a counter");

        prop_assert!(
            s.min_count() >= lowest,
            "ceiling {} below the smallest of {} counters at {}",
            s.min_count(), capacity, lowest
        );
        prop_assert!(
            u128::from(s.min_count()) * capacity as u128 <= u128::from(s.total()),
            "ceiling {} over {} counters exceeds the total {}",
            s.min_count(), capacity, s.total()
        );
    }

    #[test]
    fn top_k_is_the_ranking_prefix(
        capacity in 1usize..40,
        k in 0usize..48,
        stream in keys(400),
    ) {
        let s = summary_of(capacity, &stream);
        let held = monitored(&s);
        let top = s.top_k(k);

        prop_assert_eq!(top.len(), k.min(s.len()));
        for pair in top.windows(2) {
            prop_assert!(pair[0].1 >= pair[1].1, "{} before {}", pair[0].1, pair[1].1);
        }
        for (item, count, error) in &top {
            let key = key_of(item);
            prop_assert_eq!(held.get(&key), Some(&(*count, *error)));
        }
        if let Some((_, lowest, _)) = top.last() {
            let cut = *lowest;
            prop_assert!(
                held.values().filter(|(c, _)| *c > cut).count() < top.len(),
                "a counter above the cut {} was left out of the ranking", cut
            );
        }
    }

    #[test]
    fn a_guaranteed_key_outranks_every_key_the_summary_dropped(
        capacity in 1usize..40,
        stream in keys(400),
    ) {
        let s = summary_of(capacity, &stream);
        let truth = truth_of(&stream);
        let held = monitored(&s);

        for (key, t) in &truth {
            if !s.is_guaranteed(&DataInput::I64(*key)) {
                continue;
            }
            for (other, other_t) in &truth {
                if held.contains_key(other) {
                    continue;
                }
                prop_assert!(
                    t > other_t,
                    "guaranteed key {} at {} does not outrank dropped key {} at {}",
                    key, t, other, other_t
                );
            }
        }
    }

    // ===== The regime with a counter to spare for every key =====

    #[test]
    fn a_summary_with_room_for_every_key_is_exact_and_order_free(
        (capacity, stream, shuffled) in roomy_pair(200),
    ) {
        let s = summary_of(capacity, &stream);
        let truth = truth_of(&stream);

        prop_assert_eq!(s.len(), truth.len());
        prop_assert_eq!(
            monitored(&s), exact_counters(&truth),
            "capacity {} over {} arrivals", capacity, stream.len()
        );

        for (key, t) in &truth {
            let probe = DataInput::I64(*key);
            prop_assert_eq!(s.estimate(&probe), *t, "key {}", key);
            prop_assert_eq!(s.upper_bound(&probe), *t, "key {}", key);
            prop_assert_eq!(s.error(&probe), 0, "key {}", key);
        }

        let reordered = summary_of(capacity, &shuffled);
        prop_assert_eq!(monitored(&reordered), monitored(&s), "capacity {}", capacity);
        prop_assert_eq!(reordered.total(), s.total());
        prop_assert_eq!(reordered.min_count(), s.min_count());
    }

    #[test]
    fn a_weighted_arrival_matches_that_many_unit_arrivals(
        (capacity, stream) in roomy_weighted(80),
    ) {
        let weighted = weighted_summary_of(capacity, &stream);

        let mut expanded = SpaceSaving::with_capacity(capacity);
        for (key, weight) in &stream {
            for _ in 0..*weight {
                expanded.insert(&DataInput::I64(*key));
            }
        }

        let exact = exact_counters(&weighted_truth_of(&stream));
        prop_assert_eq!(
            monitored(&weighted), exact.clone(),
            "capacity {} over {} arrivals", capacity, stream.len()
        );
        prop_assert_eq!(monitored(&expanded), exact, "capacity {}", capacity);
        prop_assert_eq!(weighted.total(), expanded.total());
        prop_assert_eq!(weighted.min_count(), expanded.min_count());
    }

    #[test]
    fn a_bulk_insert_matches_the_same_values_one_at_a_time(
        capacity in 1usize..40,
        stream in keys(400),
    ) {
        let values: Vec<DataInput> = stream.iter().map(|k| DataInput::I64(*k)).collect();
        let mut bulk: SpaceSaving = SpaceSaving::with_capacity(capacity);
        bulk.bulk_insert(&values);
        let one_at_a_time = summary_of(capacity, &stream);

        prop_assert_eq!(
            monitored(&bulk), monitored(&one_at_a_time),
            "capacity {} over {} arrivals", capacity, stream.len()
        );
        prop_assert_eq!(bulk.total(), one_at_a_time.total());
        prop_assert_eq!(bulk.min_count(), one_at_a_time.min_count());
    }

    // ===== State carried across a reset and along the stream =====

    #[test]
    fn a_cleared_summary_starts_over_as_a_fresh_one(
        capacity in 1usize..24,
        used in keys(400),
        stream in keys(400),
    ) {
        let mut reused = summary_of(capacity, &used);
        reused.clear();

        prop_assert!(reused.is_empty());
        prop_assert_eq!(reused.len(), 0);
        prop_assert_eq!(reused.total(), 0);
        prop_assert_eq!(reused.min_count(), 0);

        for key in &stream {
            reused.insert(&DataInput::I64(*key));
        }
        let fresh = summary_of(capacity, &stream);

        prop_assert_eq!(
            monitored(&reused), monitored(&fresh),
            "capacity {} after {} discarded arrivals", capacity, used.len()
        );
        prop_assert_eq!(reused.total(), fresh.total());
        prop_assert_eq!(reused.min_count(), fresh.min_count());
    }

    #[test]
    fn the_unmonitored_ceiling_never_falls_as_the_stream_advances(
        capacity in 1usize..12,
        stream in keys(400),
    ) {
        let mut s: SpaceSaving = SpaceSaving::with_capacity(capacity);
        let mut ceiling = 0;

        for (i, key) in stream.iter().enumerate() {
            s.insert(&DataInput::I64(*key));
            let now = s.min_count();
            prop_assert!(
                now >= ceiling,
                "arrival {} of key {}: ceiling fell from {} to {}", i, key, ceiling, now
            );
            ceiling = now;
        }
    }

    // ===== Bounds surviving a merge =====

    #[test]
    fn merged_bounds_hold_over_the_concatenated_stream(
        capacity in 1usize..24,
        other_capacity in 1usize..24,
        left in keys(200),
        right in keys(200),
    ) {
        let mut merged = summary_of(capacity, &left);
        merged.merge(&summary_of(other_capacity, &right));

        let concatenated: Vec<i64> = left.iter().chain(right.iter()).copied().collect();
        let truth = truth_of(&concatenated);
        let held = monitored(&merged);

        prop_assert!(merged.len() <= merged.capacity());
        prop_assert_eq!(merged.total(), concatenated.len() as u64);

        for (key, t) in &truth {
            let bound = merged.upper_bound(&DataInput::I64(*key));
            prop_assert!(bound >= *t, "key {}: ceiling {} below truth {}", key, bound, t);

            if let Some((count, error)) = held.get(key) {
                prop_assert!(error <= count, "key {}: error {} above count {}", key, error, count);
                prop_assert!(count >= t, "key {}: count {} below truth {}", key, count, t);
                prop_assert!(
                    count - error <= *t,
                    "key {}: count {} less error {} above truth {}", key, count, error, t
                );
            }
        }
    }

    #[test]
    fn merging_an_empty_summary_leaves_every_counter_untouched(
        capacity in 1usize..24,
        narrow in 1usize..4,
        stream in keys(400),
        dropped in narrow_keys(120),
    ) {
        let mut base = summary_of(capacity, &stream);
        base.merge(&summary_of(narrow, &dropped));

        let mut merged = base.clone();
        merged.merge(&SpaceSaving::with_capacity(capacity));

        prop_assert_eq!(
            monitored(&merged), monitored(&base),
            "capacity {} over {} arrivals", capacity, stream.len()
        );
        prop_assert_eq!(merged.len(), base.len());
        prop_assert_eq!(merged.total(), base.total());
        prop_assert_eq!(merged.min_count(), base.min_count());
    }

    #[test]
    fn merging_in_either_order_reaches_the_same_summary(
        (capacity, left, right) in crowded_pair(200),
    ) {
        let a = summary_of(capacity, &left);
        let b = summary_of(capacity, &right);

        let mut ab = a.clone();
        ab.merge(&b);
        let mut ba = b.clone();
        ba.merge(&a);

        prop_assert_eq!(
            monitored(&ab), monitored(&ba),
            "capacity {} over {} and {} arrivals", capacity, left.len(), right.len()
        );
        prop_assert_eq!(ab.total(), ba.total());
        prop_assert_eq!(ab.min_count(), ba.min_count());
    }

    /// The keys a merge drops leave no counter behind, so all a merged summary
    /// can still say about them is its ceiling: every later merge and every
    /// later arrival has to keep seating against it.
    #[test]
    fn a_merged_summary_keeps_bounding_what_it_can_no_longer_see(
        capacity in 4usize..24,
        narrow in 1usize..6,
        other_narrow in 1usize..6,
        left in narrow_keys(120),
        middle in narrow_keys(120),
        right in narrow_keys(120),
        tail in narrow_keys(120),
    ) {
        let mut s = summary_of(capacity, &left);
        s.merge(&summary_of(narrow, &middle));
        s.merge(&summary_of(other_narrow, &right));
        for key in &tail {
            s.insert(&DataInput::I64(*key));
        }

        let whole: Vec<i64> = left.iter()
            .chain(&middle)
            .chain(&right)
            .chain(&tail)
            .copied()
            .collect();
        let held = monitored(&s);

        prop_assert!(s.len() <= s.capacity());
        prop_assert_eq!(s.total(), whole.len() as u64);

        for (key, t) in truth_of(&whole) {
            let bound = s.upper_bound(&DataInput::I64(key));
            prop_assert!(bound >= t, "key {}: ceiling {} below truth {}", key, bound, t);

            if let Some((count, error)) = held.get(&key) {
                prop_assert!(error <= count, "key {}: error {} above count {}", key, error, count);
                prop_assert!(*count >= t, "key {}: count {} below truth {}", key, count, t);
                prop_assert!(
                    count - error <= t,
                    "key {}: count {} less error {} above truth {}", key, count, error, t
                );
            }
        }
    }

    // ===== Wire =====

    #[test]
    fn space_saving_round_trips_at_edge_capacities(
        capacity in edge_dimension(),
        stream in prop::collection::vec(0u64..512, 0..400),
        dropped in prop::collection::vec(0u64..4, 0..16),
    ) {
        type Ss = SpaceSaving<DefaultXxHasher>;
        let mut sketch = Ss::with_capacity(capacity);
        for k in &stream {
            sketch.insert(&DataInput::U64(*k));
        }

        // A single counter over several keys leaves behind a ceiling that no
        // counter of the merged summary holds.
        let mut narrow = Ss::with_capacity(1);
        for k in &dropped {
            narrow.insert(&DataInput::U64(*k));
        }
        sketch.merge(&narrow);

        round_trip!(
            Ss,
            sketch,
            |s: &Ss| s.total(),
            |s: &Ss| s.min_count(),
            |s: &Ss| (0..PROBE_KEYS as u64)
                .map(|k| (s.estimate(&DataInput::U64(k)), s.error(&DataInput::U64(k))))
                .collect::<Vec<_>>(),
        );
    }
}
