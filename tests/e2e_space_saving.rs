//! E2E suite for Space-Saving: the counter guarantees and the Stream-Summary
//! structure behind them.
//!
//! `tests/conformance_kit.rs` runs the standard `frequency_battery` over 1024
//! counters. Here the summary is pushed on what the battery does not model -
//! the error sandwich around every monitored key, the ceiling an unmonitored
//! key cannot exceed, `is_guaranteed` as a claim about the real top-k, and the
//! bucket and counter lists staying well formed under sustained eviction.

mod common;

use common::{FreqTruth, zipf_u64};

use asap_sketchlib::{DataInput, HeapItem, SpaceSaving};
use std::collections::{HashMap, HashSet};

const STREAM: usize = 60_000;
const DOMAIN: usize = 2_048;
const SEED: u64 = 9_001;

fn stream() -> Vec<i64> {
    zipf_u64(STREAM, DOMAIN, 1.1, SEED)
        .iter()
        .map(|v| *v as i64)
        .collect()
}

fn truth_of(stream: &[i64]) -> FreqTruth {
    let mut truth = FreqTruth::default();
    for key in stream {
        truth.observe(*key);
    }
    truth
}

fn filled(capacity: usize, stream: &[i64]) -> SpaceSaving {
    let mut summary = SpaceSaving::with_capacity(capacity);
    for key in stream {
        summary.insert(&DataInput::I64(*key));
    }
    summary
}

fn key_of(item: &HeapItem) -> i64 {
    match item {
        HeapItem::I64(v) => *v,
        other => panic!("unexpected key form {other:?}"),
    }
}

/// The counter guarantee, both halves at once: a monitored key's count is at
/// least its true count and at most its true count plus its own error.
#[test]
fn a_monitored_key_is_sandwiched_by_its_error() {
    let stream = stream();
    let truth = truth_of(&stream);
    let summary = filled(256, &stream);

    let mut monitored = 0;
    for (key, count) in truth.pairs() {
        let probe = DataInput::I64(key);
        let estimate = summary.estimate(&probe);
        if estimate == 0 {
            continue;
        }
        monitored += 1;
        let error = summary.error(&probe);
        assert!(
            estimate >= count as u64,
            "key {key} true {count} estimated {estimate} - a monitored key must never read low"
        );
        assert!(
            estimate - error <= count as u64,
            "key {key} true {count} estimated {estimate} error {error} - the sandwich is open at the bottom"
        );
    }
    assert_eq!(monitored, summary.len());
    assert_eq!(summary.len(), 256, "the summary should be saturated");
}

/// No key the summary dropped can be larger than the smallest it kept, which
/// is what makes `min_count` a usable ceiling and `upper_bound` one-sided for
/// every key in the stream.
#[test]
fn an_unmonitored_key_never_exceeds_the_minimum_count() {
    let stream = stream();
    let truth = truth_of(&stream);
    let summary = filled(256, &stream);
    let floor = summary.min_count();
    assert!(floor > 0, "a saturated summary has a positive minimum");

    for (key, count) in truth.pairs() {
        let probe = DataInput::I64(key);
        if summary.estimate(&probe) != 0 {
            continue;
        }
        assert!(
            count as u64 <= floor,
            "dropped key {key} has true count {count}, above the {floor} ceiling"
        );
        assert_eq!(summary.upper_bound(&probe), floor);
        assert_eq!(summary.error(&probe), floor);
    }
}

/// The heavy keys the summary is bought for come back exactly, in order.
#[test]
fn the_true_heavy_hitters_are_reported_exactly_and_in_order() {
    let stream = stream();
    let truth = truth_of(&stream);
    let summary = filled(256, &stream);

    let mut expected: Vec<(i64, i64)> = truth.pairs();
    expected.sort_by_key(|(key, count)| (-*count, *key));
    expected.truncate(20);

    let reported = summary.top_k(20);
    assert_eq!(reported.len(), 20);

    let mut previous = u64::MAX;
    for (slot, (item, count, error)) in reported.iter().enumerate() {
        assert!(*count <= previous, "top_k is not descending at slot {slot}");
        previous = *count;

        let (want_key, want_count) = expected[slot];
        assert_eq!(key_of(item), want_key, "slot {slot} holds the wrong key");
        assert_eq!(*count as i64, want_count, "slot {slot} count");
        assert_eq!(
            *error, 0,
            "a true heavy hitter is never displaced, so it carries no error"
        );
    }
}

/// `is_guaranteed` is a claim about the real stream: every key it accepts is
/// genuinely above every key the summary dropped.
#[test]
fn a_guaranteed_key_really_outranks_everything_dropped() {
    let stream = stream();
    let truth = truth_of(&stream);
    let summary = filled(256, &stream);

    let dropped_max = truth
        .pairs()
        .into_iter()
        .filter(|(key, _)| summary.estimate(&DataInput::I64(*key)) == 0)
        .map(|(_, count)| count)
        .max()
        .unwrap_or(0);

    let mut guaranteed = 0;
    for (key, count) in truth.pairs() {
        if !summary.is_guaranteed(&DataInput::I64(key)) {
            continue;
        }
        guaranteed += 1;
        assert!(
            count > dropped_max,
            "key {key} is claimed guaranteed at true count {count}, but a dropped key reached {dropped_max}"
        );
    }
    assert!(
        guaranteed > 0,
        "a saturated summary should guarantee someone"
    );
}

/// Eviction takes the minimum and hands over its count as the arrival's error.
/// Hand-built so the whole sequence is forced rather than sampled.
#[test]
fn an_arrival_displaces_the_minimum_and_inherits_its_count() {
    let mut summary: SpaceSaving = SpaceSaving::with_capacity(2);
    for _ in 0..3 {
        summary.insert(&DataInput::I64(1));
    }
    for _ in 0..2 {
        summary.insert(&DataInput::I64(2));
    }
    assert_eq!(summary.min_count(), 2);

    summary.insert(&DataInput::I64(3));

    assert_eq!(summary.len(), 2);
    assert_eq!(summary.estimate(&DataInput::I64(1)), 3);
    assert_eq!(summary.error(&DataInput::I64(1)), 0);
    // Key 2 held the minimum, so key 3 took its slot at min + 1 with min as
    // the allowance for what key 3 may not have contributed.
    assert_eq!(summary.estimate(&DataInput::I64(2)), 0);
    assert_eq!(summary.estimate(&DataInput::I64(3)), 3);
    assert_eq!(summary.error(&DataInput::I64(3)), 2);
    assert_eq!(summary.min_count(), 3);
    assert_eq!(summary.total(), 6);
}

/// The bucket and counter lists stay well formed under sustained eviction:
/// walking them reaches every counter exactly once, in descending count order.
#[test]
fn the_stream_summary_lists_stay_well_formed_under_eviction() {
    let stream = stream();
    for capacity in [1usize, 2, 17, 256, 1_024] {
        let summary = filled(capacity, &stream);
        let expected = capacity.min(truth_of(&stream).distinct());
        assert_eq!(summary.len(), expected, "capacity {capacity} residency");

        let walked = summary.top_k(usize::MAX);
        assert_eq!(
            walked.len(),
            summary.len(),
            "capacity {capacity}: the bucket walk reached {} of {} counters",
            walked.len(),
            summary.len()
        );

        let mut previous = u64::MAX;
        let mut seen: HashSet<i64> = HashSet::new();
        for (item, count, _) in &walked {
            assert!(count <= &previous, "capacity {capacity}: walk out of order");
            previous = *count;
            assert!(
                seen.insert(key_of(item)),
                "capacity {capacity}: key {} reached twice",
                key_of(item)
            );
        }

        // `entries` reaches the counters directly; the two views must agree.
        let entries = summary.entries();
        assert_eq!(entries.len(), walked.len());
        let direct: HashSet<i64> = entries.iter().map(|(k, _, _)| key_of(k)).collect();
        assert_eq!(direct, seen, "capacity {capacity}: the two views disagree");
    }
}

/// A weighted arrival is the same as that many single arrivals.
#[test]
fn a_weighted_arrival_matches_repeating_it() {
    let stream = stream();
    let mut singles: SpaceSaving = SpaceSaving::with_capacity(2_048);
    let mut weighted: SpaceSaving = SpaceSaving::with_capacity(2_048);

    let mut counts: HashMap<i64, u64> = HashMap::new();
    for key in &stream {
        singles.insert(&DataInput::I64(*key));
        *counts.entry(*key).or_default() += 1;
    }
    let mut keys: Vec<i64> = counts.keys().copied().collect();
    keys.sort_unstable();
    for key in keys {
        weighted.insert_many(&DataInput::I64(key), counts[&key]);
    }

    assert_eq!(weighted.total(), singles.total());
    assert_eq!(weighted.len(), singles.len());
    for (key, count) in &counts {
        assert_eq!(
            weighted.estimate(&DataInput::I64(*key)),
            *count,
            "weighted estimate for key {key}"
        );
        assert_eq!(singles.estimate(&DataInput::I64(*key)), *count);
    }
}

/// With room for every distinct key the summary is exact and carries no error.
#[test]
fn a_summary_larger_than_the_domain_is_exact() {
    let stream = stream();
    let truth = truth_of(&stream);
    let summary = filled(DOMAIN * 2, &stream);

    assert_eq!(summary.len(), truth.distinct());
    assert_eq!(summary.min_count(), 0);
    for (key, count) in truth.pairs() {
        let probe = DataInput::I64(key);
        assert_eq!(summary.estimate(&probe), count as u64);
        assert_eq!(summary.error(&probe), 0);
    }
}

/// Merging keeps the total, respects capacity, and never reports a shared key
/// below what either side saw.
#[test]
fn a_merge_keeps_the_total_and_never_reads_low() {
    let stream = stream();
    let (left_stream, right_stream): (Vec<i64>, Vec<i64>) =
        stream.iter().partition(|key| *key % 2 == 0);

    let mut left = filled(256, &left_stream);
    let right = filled(256, &right_stream);
    let left_truth = truth_of(&left_stream);
    let right_truth = truth_of(&right_stream);

    left.merge_from(&right);

    assert_eq!(left.total(), stream.len() as u64);
    assert!(left.len() <= left.capacity());

    for (key, count) in left_truth.pairs().into_iter().chain(right_truth.pairs()) {
        let estimate = left.estimate(&DataInput::I64(key));
        if estimate == 0 {
            continue;
        }
        assert!(
            estimate >= count as u64,
            "merged key {key} reads {estimate} against a per-side truth of {count}"
        );
    }
}

/// A round-trip through serde preserves every answer, including the key index
/// the summary looks up on.
#[test]
fn a_serde_round_trip_preserves_every_answer() {
    let stream = stream();
    let truth = truth_of(&stream);
    let summary = filled(256, &stream);

    let bytes = rmp_serde::to_vec(&summary).expect("serialize");
    let decoded: SpaceSaving = rmp_serde::from_slice(&bytes).expect("deserialize");

    assert_eq!(decoded.len(), summary.len());
    assert_eq!(decoded.capacity(), summary.capacity());
    assert_eq!(decoded.total(), summary.total());
    assert_eq!(decoded.min_count(), summary.min_count());
    for (key, _) in truth.pairs() {
        let probe = DataInput::I64(key);
        assert_eq!(decoded.estimate(&probe), summary.estimate(&probe));
        assert_eq!(decoded.error(&probe), summary.error(&probe));
    }

    // The decoded summary still takes inserts against its rebuilt index.
    let mut decoded = decoded;
    let hottest = key_of(&summary.top_k(1)[0].0);
    let before = decoded.estimate(&DataInput::I64(hottest));
    decoded.insert(&DataInput::I64(hottest));
    assert_eq!(decoded.estimate(&DataInput::I64(hottest)), before + 1);
}

/// A summary that saw nothing answers without panicking.
#[test]
fn an_empty_summary_answers() {
    let summary: SpaceSaving = SpaceSaving::with_capacity(8);
    assert!(summary.is_empty());
    assert_eq!(summary.len(), 0);
    assert_eq!(summary.min_count(), 0);
    assert_eq!(summary.total(), 0);
    assert_eq!(summary.estimate(&DataInput::I64(1)), 0);
    assert_eq!(summary.upper_bound(&DataInput::I64(1)), 0);
    assert!(!summary.is_guaranteed(&DataInput::I64(1)));
    assert!(summary.top_k(5).is_empty());
    assert!(summary.entries().is_empty());
}

/// A weighted arrival that has to displace the minimum takes the same slot,
/// count and error as the arrivals it stands in for.
#[test]
fn a_weighted_arrival_matches_repeating_it_under_eviction() {
    let stream = stream();
    let mut counts: HashMap<i64, u64> = HashMap::new();
    for key in &stream {
        *counts.entry(*key).or_default() += 1;
    }
    let mut keys: Vec<i64> = counts.keys().copied().collect();
    keys.sort_unstable();

    let mut singles: SpaceSaving = SpaceSaving::with_capacity(64);
    let mut weighted: SpaceSaving = SpaceSaving::with_capacity(64);
    for key in &keys {
        for _ in 0..counts[key] {
            singles.insert(&DataInput::I64(*key));
        }
        weighted.insert_many(&DataInput::I64(*key), counts[key]);
    }

    assert_eq!(weighted.len(), 64, "the run should have evicted heavily");
    assert_eq!(weighted.total(), singles.total());
    assert_eq!(weighted.min_count(), singles.min_count());

    let mut left: Vec<(i64, u64, u64)> = weighted
        .entries()
        .iter()
        .map(|(key, count, error)| (key_of(key), *count, *error))
        .collect();
    let mut right: Vec<(i64, u64, u64)> = singles
        .entries()
        .iter()
        .map(|(key, count, error)| (key_of(key), *count, *error))
        .collect();
    left.sort_unstable();
    right.sort_unstable();
    assert_eq!(left, right, "a weighted arrival diverged from repeating it");
}

/// `bulk_insert` is the loop it replaces, and a zero weight records nothing.
#[test]
fn bulk_insert_matches_repeated_inserts_and_a_zero_weight_is_inert() {
    let stream = stream();
    let values: Vec<DataInput> = stream.iter().map(|key| DataInput::I64(*key)).collect();

    let mut bulk: SpaceSaving = SpaceSaving::with_capacity(128);
    bulk.bulk_insert(&values);
    let one_at_a_time = filled(128, &stream);

    assert_eq!(bulk.total(), one_at_a_time.total());
    assert_eq!(bulk.top_k(usize::MAX), one_at_a_time.top_k(usize::MAX));

    let before = bulk.top_k(usize::MAX);
    bulk.insert_many(&DataInput::I64(stream[0]), 0);
    bulk.insert_many(&DataInput::I64(i64::MIN), 0);
    assert_eq!(bulk.total(), one_at_a_time.total());
    assert_eq!(bulk.len(), one_at_a_time.len());
    assert_eq!(bulk.top_k(usize::MAX), before);
}

/// A summary asked for no counters still holds one.
#[test]
fn a_zero_capacity_summary_still_answers() {
    let mut summary: SpaceSaving = SpaceSaving::with_capacity(0);
    assert_eq!(summary.capacity(), 1);
    for key in 0..10i64 {
        summary.insert(&DataInput::I64(key));
    }
    assert_eq!(summary.len(), 1);
    assert_eq!(summary.total(), 10);
    let held = summary.top_k(4);
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].1, 10);
    assert_eq!(held[0].2, 9);
}

/// A merge into a summary with counters to spare still reports a ceiling that
/// covers everything the other side had already dropped.
#[test]
fn a_merge_into_an_under_full_summary_keeps_the_ceiling_honest() {
    let mut left: SpaceSaving = SpaceSaving::with_capacity(33);
    let mut right: SpaceSaving = SpaceSaving::with_capacity(1);
    for _ in 0..10 {
        right.insert(&DataInput::I64(7));
    }
    for _ in 0..20 {
        right.insert(&DataInput::I64(8));
    }

    left.merge_from(&right);

    assert_eq!(left.len(), 1);
    assert!(left.len() < left.capacity());
    assert!(
        left.upper_bound(&DataInput::I64(7)) >= 10,
        "key 7 truly reached 10 but is capped at {}",
        left.upper_bound(&DataInput::I64(7))
    );
    assert!(
        !left.is_guaranteed(&DataInput::I64(8)),
        "a merged summary that dropped a key cannot guarantee anything for free"
    );
}

/// A chain of merges over asymmetric capacities: every key of the combined
/// stream stays under its ceiling, and every guarantee holds against the truth.
#[test]
fn a_merge_chain_stays_one_sided_against_the_truth() {
    let stream = stream();
    let mut shards: Vec<Vec<i64>> = vec![Vec::new(); 3];
    for (position, key) in stream.iter().enumerate() {
        shards[position % 3].push(*key);
    }
    let truth = truth_of(&stream);

    let mut merged = filled(64, &shards[0]);
    merged.merge_from(&filled(512, &shards[1]));
    merged.merge_from(&filled(7, &shards[2]));

    assert_eq!(merged.total(), stream.len() as u64);
    assert!(merged.len() <= merged.capacity());

    let mut dropped_max = 0;
    for (key, count) in truth.pairs() {
        let probe = DataInput::I64(key);
        assert!(
            merged.upper_bound(&probe) >= count as u64,
            "key {key} truly reached {count} but is capped at {}",
            merged.upper_bound(&probe)
        );
        if merged.estimate(&probe) == 0 {
            dropped_max = dropped_max.max(count);
        } else {
            assert!(
                merged.estimate(&probe) >= count as u64,
                "monitored key {key} reads below its true {count}"
            );
        }
    }
    for (key, count) in truth.pairs() {
        if merged.is_guaranteed(&DataInput::I64(key)) {
            assert!(
                count > dropped_max,
                "key {key} is claimed guaranteed at {count}, under a dropped key at {dropped_max}"
            );
        }
    }
}

/// The same two summaries merge to the same answer every time, ties included,
/// and that answer depends on what they hold rather than on the order the keys
/// happened to arrive in.
#[test]
fn a_merge_picks_the_same_survivors_every_time() {
    fn shape_of(ascending: bool) -> Vec<(i64, u64, u64)> {
        let mut left: SpaceSaving = SpaceSaving::with_capacity(200);
        let mut right: SpaceSaving = SpaceSaving::with_capacity(200);
        let mut left_keys: Vec<i64> = (0..150).collect();
        let mut right_keys: Vec<i64> = (100..250).collect();
        if !ascending {
            left_keys.reverse();
            right_keys.reverse();
        }
        for key in left_keys {
            left.insert(&DataInput::I64(key));
        }
        for key in right_keys {
            right.insert(&DataInput::I64(key));
        }
        left.merge_from(&right);
        left.top_k(usize::MAX)
            .iter()
            .map(|(key, count, error)| (key_of(key), *count, *error))
            .collect()
    }

    let reference = shape_of(true);
    assert_eq!(reference.len(), 200, "the union should fill the capacity");
    assert_eq!(
        reference.iter().filter(|(_, count, _)| *count == 2).count(),
        50,
        "the fifty shared keys should be the heavy ones"
    );
    for round in 0..5 {
        assert_eq!(
            shape_of(true),
            reference,
            "round {round} merged differently"
        );
    }
    assert_eq!(
        shape_of(false),
        reference,
        "the merge depends on the order the keys arrived in"
    );
}

/// The ceiling a merge established survives the wire.
#[test]
fn a_round_trip_carries_the_merged_ceiling() {
    let stream = stream();
    let mut merged = filled(32, &stream[..stream.len() / 2]);
    merged.merge_from(&filled(4, &stream[stream.len() / 2..]));

    let bytes = rmp_serde::to_vec(&merged).expect("serialize");
    let decoded: SpaceSaving = rmp_serde::from_slice(&bytes).expect("deserialize");

    assert_eq!(decoded.min_count(), merged.min_count());
    assert_eq!(decoded.total(), merged.total());
    assert_eq!(decoded.len(), merged.len());
    for (key, count) in truth_of(&stream).pairs() {
        let probe = DataInput::I64(key);
        assert_eq!(decoded.estimate(&probe), merged.estimate(&probe));
        assert!(
            decoded.upper_bound(&probe) >= count as u64,
            "key {key} truly reached {count} but decodes capped at {}",
            decoded.upper_bound(&probe)
        );
    }
}

/// Serialized state that no run of the algorithm could have produced is
/// refused, rather than decoded into a summary that panics or loops.
#[test]
fn crafted_state_fails_closed() {
    #[derive(serde::Serialize)]
    struct CraftedState {
        capacity: usize,
        total: u64,
        floor: u64,
        entries: Vec<(HeapItem, u64, u64)>,
    }

    let crafted = |capacity: usize, entries: Vec<(HeapItem, u64, u64)>| {
        rmp_serde::to_vec(&CraftedState {
            capacity,
            total: 100,
            floor: 0,
            entries,
        })
        .expect("serialize")
    };

    let refused = [
        crafted(0, Vec::new()),
        crafted(1, vec![(HeapItem::I64(1), 2, 0), (HeapItem::I64(2), 2, 0)]),
        crafted(4, vec![(HeapItem::I64(1), 0, 0)]),
        crafted(4, vec![(HeapItem::I64(1), 3, 9)]),
        crafted(4, vec![(HeapItem::I64(1), 3, 0), (HeapItem::I64(1), 2, 0)]),
    ];
    for (case, bytes) in refused.iter().enumerate() {
        assert!(
            rmp_serde::from_slice::<SpaceSaving>(bytes).is_err(),
            "case {case} decoded into a summary it should have refused"
        );
    }

    let bytes = crafted(4, vec![(HeapItem::I64(1), 3, 1), (HeapItem::I64(2), 9, 2)]);
    let decoded: SpaceSaving = rmp_serde::from_slice(&bytes).expect("a well formed state");
    assert_eq!(decoded.top_k(8).len(), 2);
    assert_eq!(decoded.estimate(&DataInput::I64(2)), 9);
}
