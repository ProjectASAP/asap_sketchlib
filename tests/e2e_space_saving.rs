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
