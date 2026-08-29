//! E2E suite for the heavy-hitter sketches: Space-Saving, CocoSketch (SIGCOMM
//! '21) and the Elastic sketch (SIGCOMM '18). All three answer "which flows are
//! big" by keeping a flow key beside every counter, so a query is served from
//! the keys the structure still holds rather than from an unkeyed counter array
//! -- Space-Saving through its Stream-Summary of monitored counters, Coco
//! through partial-key aggregation over its recorded flows, Elastic through its
//! heavy/light split.
//!
//! `tests/conformance_kit.rs` runs the standard `frequency_battery` over 1024
//! Space-Saving counters. Here the summary is pushed on what that battery does
//! not model - the error sandwich around every monitored key, the ceiling an
//! unmonitored key cannot exceed, `is_guaranteed` as a claim about the real
//! top-k, and the bucket and counter lists staying well formed under sustained
//! eviction.
//!
//! Coco and Elastic run the standard conformance batteries their documented
//! contracts justify, then the depth no battery models: Coco's
//! over-attribution under substring matching, its point-query mass partition,
//! its unbiasedness under eviction and its recall at the paper's worked
//! operating point; Elastic's hot-flow tracking, its one-sided estimator under
//! eviction pressure, and the reach of the light layer's dimensions.
//!
//! `tests/e2e_octo.rs` covers the multi-threaded OctoSketch variants of Coco
//! and Elastic in its `heavy_hitters` module; everything here is the
//! single-threaded sketch. The top-k heap sketches (`CMSHeap`, `CSHeap`) answer
//! the same question from an unkeyed sketch plus a heap, and stay with their
//! own family in `tests/e2e_frequency.rs`.

mod common;

use common::{FreqTruth, zipf_u64};

use asap_sketchlib::{DataInput, HeapItem, SPACE_SAVING_DEFAULT_CAPACITY, SpaceSaving};
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

// ---------------------------------------------------------------------------
// Space-Saving
// ---------------------------------------------------------------------------

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

        // `entries` reaches the counters directly; the two views must agree on
        // every triple, not just on which keys are held.
        let triples = |view: &[(HeapItem, u64, u64)]| {
            let mut out: Vec<(i64, u64, u64)> = view
                .iter()
                .map(|(key, count, error)| (key_of(key), *count, *error))
                .collect();
            out.sort_unstable();
            out
        };
        let direct = triples(&summary.entries());
        assert_eq!(direct.len(), seen.len());
        assert_eq!(
            direct,
            triples(&walked),
            "capacity {capacity}: the two views disagree"
        );
        assert!(
            direct.iter().any(|(_, _, error)| *error > 0),
            "capacity {capacity}: eviction left no error behind"
        );
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

    let mut checked: HashSet<i64> = HashSet::new();
    for (key, count) in left_truth.pairs().into_iter().chain(right_truth.pairs()) {
        let estimate = left.estimate(&DataInput::I64(key));
        if estimate == 0 {
            continue;
        }
        checked.insert(key);
        assert!(
            estimate >= count as u64,
            "merged key {key} reads {estimate} against a per-side truth of {count}"
        );
    }
    assert_eq!(
        checked.len(),
        left.len(),
        "the loop reached {} of the {} merged counters",
        checked.len(),
        left.len()
    );
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
    let mut guaranteed = 0;
    for (key, count) in truth.pairs() {
        if merged.is_guaranteed(&DataInput::I64(key)) {
            guaranteed += 1;
            assert!(
                count > dropped_max,
                "key {key} is claimed guaranteed at {count}, under a dropped key at {dropped_max}"
            );
        }
    }
    assert!(guaranteed > 0, "the merge chain guarantees nobody");
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

    let crafted = |capacity: usize, total: u64, floor: u64, entries: Vec<(HeapItem, u64, u64)>| {
        rmp_serde::to_vec(&CraftedState {
            capacity,
            total,
            floor,
            entries,
        })
        .expect("serialize")
    };

    let refused = [
        (crafted(0, 100, 0, Vec::new()), "capacity is zero"),
        (
            crafted(
                1,
                100,
                0,
                vec![(HeapItem::I64(1), 2, 0), (HeapItem::I64(2), 2, 0)],
            ),
            "over a capacity of 1",
        ),
        (
            crafted(4, 100, 0, vec![(HeapItem::I64(1), 0, 0)]),
            "counter at zero",
        ),
        (
            crafted(4, 100, 0, vec![(HeapItem::I64(1), 3, 9)]),
            "error of 9 against a count of 3",
        ),
        (
            crafted(
                4,
                100,
                0,
                vec![(HeapItem::I64(1), 3, 0), (HeapItem::I64(1), 2, 0)],
            ),
            "same key twice",
        ),
        (
            crafted(4, 100, 200, vec![(HeapItem::I64(1), 3, 0)]),
            "ceiling of 200 above its lowest count of 3",
        ),
        (
            crafted(4, 2, 0, vec![(HeapItem::I64(1), 9, 0)]),
            "total of 2 under the 9",
        ),
    ];
    for (bytes, expected) in &refused {
        let problem = rmp_serde::from_slice::<SpaceSaving>(bytes)
            .expect_err("a crafted state must be refused, not decoded")
            .to_string();
        assert!(
            problem.contains(expected),
            "expected a complaint about {expected}, got {problem}"
        );
    }

    let bytes = crafted(
        4,
        100,
        0,
        vec![(HeapItem::I64(1), 3, 1), (HeapItem::I64(2), 9, 2)],
    );
    let decoded: SpaceSaving = rmp_serde::from_slice(&bytes).expect("a well formed state");
    assert_eq!(decoded.top_k(8).len(), 2);
    assert_eq!(decoded.estimate(&DataInput::I64(2)), 9);
}

/// `SpaceSaving::default()` is the documented counter budget, and it fills.
#[test]
fn the_default_summary_holds_the_default_capacity() {
    assert_eq!(SPACE_SAVING_DEFAULT_CAPACITY, 1_024);

    let mut summary: SpaceSaving = SpaceSaving::default();
    assert_eq!(summary.capacity(), SPACE_SAVING_DEFAULT_CAPACITY);
    assert!(summary.is_empty());

    let stream = stream();
    for key in &stream {
        summary.insert(&DataInput::I64(*key));
    }

    assert_eq!(summary.len(), SPACE_SAVING_DEFAULT_CAPACITY);
    assert_eq!(summary.total(), stream.len() as u64);
    assert_eq!(
        summary.top_k(usize::MAX).len(),
        SPACE_SAVING_DEFAULT_CAPACITY
    );
}

// ---------------------------------------------------------------------------
// CocoSketch and Elastic: the keyed-bucket tables
// ---------------------------------------------------------------------------

mod keyed_bucket {
    use super::common::assert_between;
    use super::common::conformance::{self, FrequencyOps, FrequencySpec, MergeOps};
    use super::*;

    use asap_sketchlib::{Coco, DefaultXxHasher, Elastic};

    // -----------------------------------------------------------------------
    // Conformance adapters: both sketches are string-keyed, so the kit's integer
    // keys are rendered through one shared flow-id format.
    // -----------------------------------------------------------------------

    fn flow_key(key: i64) -> String {
        format!("flow::{key}")
    }

    /// Coco at its documented default, `1024 x 4`: 4096 buckets against the ~2000
    /// distinct flows the battery stream carries, which is the regime the sizing
    /// note asks for -- the table attributes mass to at most `w * d` keys at once.
    struct CocoAdapter(Coco<DefaultXxHasher>);

    impl CocoAdapter {
        fn new() -> Self {
            Self(Coco::new())
        }
    }

    impl FrequencyOps<i64> for CocoAdapter {
        fn ingest(&mut self, key: &i64) {
            self.0.insert(&flow_key(*key), 1);
        }
        fn estimate(&self, key: &i64) -> f64 {
            self.0.estimate_key(&flow_key(*key)) as f64
        }
    }

    impl MergeOps for CocoAdapter {
        fn merge_from(&mut self, other: &Self) {
            self.0.merge(&other.0);
        }
    }

    /// Elastic at 256 heavy buckets over the default 3 x 4096 light layer.
    /// Section 3.1.2 puts the elephant collision rate at `1 - (H/w + 1) e^(-H/w)`;
    /// the battery stream carries 243 dense flows, so `H/w ~ 0.95` and a quarter of
    /// the buckets hold more than one elephant. Contested buckets are the point:
    /// the losers read through the light layer, which is where the one-sided claim
    /// is worth checking.
    struct ElasticAdapter(Elastic<DefaultXxHasher>);

    impl ElasticAdapter {
        fn new() -> Self {
            Self(Elastic::init_with_length(256))
        }
    }

    impl FrequencyOps<i64> for ElasticAdapter {
        fn ingest(&mut self, key: &i64) {
            self.0.insert(flow_key(*key));
        }
        fn estimate(&self, key: &i64) -> f64 {
            self.0.query(flow_key(*key)) as f64
        }
    }

    impl MergeOps for ElasticAdapter {
        fn merge_from(&mut self, other: &Self) {
            self.0.merge(&other.0);
        }
    }

    // -----------------------------------------------------------------------
    // Battery runs
    // -----------------------------------------------------------------------

    /// Coco is *unbiased*, not one-sided: an estimate comes back either side of the
    /// truth, so `one_sided` stays false and the spec is Count Sketch's two-sided
    /// reference spec from `conformance_kit.rs`, unchanged.
    ///
    /// `turnstile_battery` does not fit: `insert` takes an unsigned weight and the
    /// sketch has no decrement path.
    #[test]
    fn coco_passes_frequency_and_merge_conformance() {
        let stream = stream();
        let truth = truth_of(&stream);
        let spec = FrequencySpec {
            one_sided: false,
            rel_tol: 0.06,
            abs_tol: 25.0,
        };

        conformance::frequency_battery("Coco", CocoAdapter::new, &stream, &truth, spec).assert_ok();
        conformance::merge_equivalence_battery("Coco", CocoAdapter::new, &stream, spec).assert_ok();
    }

    /// `docs/api/api_elastic.md`: "The estimator is one-sided: it never returns
    /// less than the true count." A resident, unflagged flow reports its own votes
    /// exactly; every other flow reads the light layer, so the excess above the
    /// truth is that layer's Count-Min error and the whole tolerance is absolute.
    ///
    /// `turnstile_battery` does not fit: `insert_many` is documented as repeated
    /// positive votes, and there is no decrement path. The battery ingests through
    /// `insert` alone -- overload mode is documented as breaking the one-sided
    /// guarantee, so a battery holding `one_sided: true` must not reach it.
    #[test]
    fn elastic_passes_frequency_and_merge_conformance() {
        let stream = stream();
        let truth = truth_of(&stream);
        // Count-Min's additive bound over the light layer: eps * N with
        // eps = e / cols. Measured worst dense-key excess is 8.
        let spec = FrequencySpec {
            one_sided: true,
            rel_tol: 0.0,
            abs_tol: std::f64::consts::E / 4096.0 * stream.len() as f64,
        };

        conformance::frequency_battery("Elastic", ElasticAdapter::new, &stream, &truth, spec)
            .assert_ok();
        conformance::merge_equivalence_battery("Elastic", ElasticAdapter::new, &stream, spec)
            .assert_ok();
    }

    // -----------------------------------------------------------------------
    // CocoSketch
    // -----------------------------------------------------------------------

    #[test]
    fn coco_over_attribution_bounds_with_disjoint_prefixes() {
        // Table sized well above distinct-key count keeps eviction loss bounded;
        // Coco remains an approximate estimator either way.
        let mut coco = Coco::<asap_sketchlib::DefaultXxHasher>::init_with_size(256, 2);
        let mut truth: HashMap<String, u64> = HashMap::new();
        let mut total = 0u64;
        for i in 0..3000u64 {
            let key = format!("aaa{}", i % 50);
            coco.insert(&key, 7);
            *truth.entry("aaa".to_string()).or_insert(0) += 7;
            total += 7;
            let _ = i;
        }
        for i in 0..2000u64 {
            let key = format!("zzz{}", i % 30);
            coco.insert(&key, 3);
            *truth.entry("zzz".to_string()).or_insert(0) += 3;
            total += 3;
        }

        // Substring matching: "aaa" only matches aaa* buckets, never zzz*.
        let got_aaa = coco.estimate_substring("aaa");
        let true_aaa = truth["aaa"];
        assert!(
            got_aaa >= true_aaa * 3 / 4 && got_aaa <= total,
            "coco 'aaa' estimate {got_aaa} outside [{}, {total}]",
            true_aaa * 3 / 4
        );

        // Exact-match UDF pins down the precise family sum.
        let exact =
            coco.estimate_with_udf("zzz", |full: &str, partial: &str| full.starts_with(partial));
        let true_zzz = truth["zzz"];
        assert!(
            exact >= true_zzz * 3 / 4 && exact <= total,
            "coco 'zzz' estimate {exact} outside [{}, {total}]",
            true_zzz * 3 / 4
        );
    }

    /// CocoSketch attributes every increment to exactly one bucket, so the paper's
    /// point query partitions the stream: summing it over the observed keys returns
    /// the total inserted mass, never more.
    #[test]
    fn coco_point_queries_partition_the_inserted_mass() {
        const COCO_BUCKETS: usize = 128;
        let mut coco = Coco::<asap_sketchlib::DefaultXxHasher>::init_with_size(COCO_BUCKETS, 3);
        let mut truth: HashMap<String, u64> = HashMap::new();
        let mut total = 0u64;

        for key in zipf_u64(40_000, 2_000, 1.2, 4242) {
            let id = format!("key::{key}");
            coco.insert(&id, 2);
            *truth.entry(id).or_insert(0) += 2;
            total += 2;
        }

        let attributed: u64 = truth.keys().map(|k| coco.estimate_key(k)).sum();
        assert_eq!(
            attributed, total,
            "point queries must partition the inserted mass"
        );

        // Heavy keys hold their own bucket, so their estimates track the truth.
        //
        // The **ceiling is derived**: CocoSketch's stochastic-variance-
        // minimizing eviction attributes each increment to exactly one bucket,
        // so a key's estimate can only exceed its count by the mass of the other
        // keys that landed in the same bucket. With `w` buckets per array that
        // expectation is `(total - count) / w`, and Markov at `e` — the same
        // step Count-Min's Theorem 1 takes — gives a per-array ceiling of
        // `count + e (total - count) / w` that holds with probability
        // `1 - 1/e`. The union bound over the ten probed keys is what the
        // failure message quotes.
        //
        // The **floor is empirical and is named as such below**: eviction can
        // take a key's whole bucket at any moment, so nothing forbids a low
        // read, and 0.5x is a measured regression pin (the worst of the ten on
        // this stream and seed is 1.00x, i.e. exact) rather than a bound.
        let mut ranked: Vec<(&String, &u64)> = truth.iter().collect();
        ranked.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
        for (key, count) in ranked.iter().take(10) {
            let est = coco.estimate_key(key);
            let ceiling = **count as f64
                + std::f64::consts::E * (total - **count) as f64 / COCO_BUCKETS as f64;
            assert!(
                est as f64 <= ceiling,
                "coco heavy key {key}: estimate {est} above count + e*(total-count)/w = \
                 {ceiling:.1} (true {count}, total {total}, w={COCO_BUCKETS})"
            );
            assert!(
                est as f64 >= **count as f64 * 0.5,
                "coco heavy key {key}: estimate {est} below the documented empirical floor \
                 of 0.5x its true count {count}; measured worst on this stream is 1.00x"
            );
        }
    }

    // Theorems 3 and 4 are stated for the section 4.2 hardware variant, which this
    // crate does not implement. The test below asserts the paper's own worked 99%
    // figure at the paper's own configuration.

    /// Section 3.2 claims stochastic variance minimization "yields unbiased size
    /// estimation", and Theorem 1 gives the per-bucket update distribution that
    /// makes it so. Unbiasedness is a statement about the mean, not any one run.
    #[test]
    fn coco_point_estimates_are_unbiased_under_heavy_eviction() {
        const TRIALS: usize = 800;
        const BG_KEYS: usize = 200;
        const BG_WEIGHT: u64 = 10;
        const TARGET: u64 = 20;

        // 201 flows over 64 buckets: the target is evicted outright in roughly a
        // third of the runs, and the surviving runs must overshoot to compensate.
        let keys: Vec<String> = (0..BG_KEYS).map(|i| format!("bg::{i}")).collect();
        let mut estimates: Vec<u64> = Vec::with_capacity(TRIALS);

        for _ in 0..TRIALS {
            let mut coco = Coco::<asap_sketchlib::DefaultXxHasher>::init_with_size(32, 2);
            let mut sent = 0u64;
            for (i, key) in keys.iter().enumerate() {
                for _ in 0..BG_WEIGHT {
                    coco.insert(key, 1);
                }
                // spread the target's packets evenly through the background stream
                while sent * (BG_KEYS as u64) < TARGET * (i as u64 + 1) {
                    coco.insert("flow::target", 1);
                    sent += 1;
                }
            }
            while sent < TARGET {
                coco.insert("flow::target", 1);
                sent += 1;
            }
            estimates.push(coco.estimate_key("flow::target"));
        }

        let dropped = estimates.iter().filter(|est| **est == 0).count();
        assert!(
            dropped > TRIALS / 10,
            "the table must actually evict the target sometimes, dropped {dropped}/{TRIALS}"
        );

        let mean = estimates.iter().map(|est| *est as f64).sum::<f64>() / TRIALS as f64;
        // the mean's standard error here is ~0.55, so this band is over 5 sigma
        assert_between(
            mean,
            TARGET as f64 - 3.0,
            TARGET as f64 + 3.0,
            "coco mean point estimate over independent runs",
        );
    }

    /// Theorem 4 bounds how often a flow is recorded at all. Section 5.3 works the
    /// bound at d=2, l=900 for a heavy hitter holding 1% of the traffic and reads
    /// off a 99% recall target; this reproduces that operating point.
    #[test]
    fn coco_recall_meets_the_papers_heavy_hitter_target() {
        const TRIALS: usize = 200;
        const BG_KEYS: usize = 5_000;
        const HEAVY: u64 = 51;
        const WIDTH: usize = 900;
        const DEPTH: usize = 2;

        // Theorem 4: P[Z(e) = 1] >= 1 - (1 + l * f(e) / f_bar(e))^-d. The bound is
        // read off the configuration, so it cannot be the assertion target -- it
        // would sink with the table. It only confirms the setup reaches section
        // 5.3's worked case, and TARGET_RECALL is what the run must clear.
        const TARGET_RECALL: f64 = 0.99;
        let ratio = HEAVY as f64 / BG_KEYS as f64;
        let bound = 1.0 - (1.0 + WIDTH as f64 * ratio).powi(-(DEPTH as i32));
        assert!(
            bound >= TARGET_RECALL,
            "this configuration does not reach the paper's 99% case: bound {bound:.4}"
        );

        let keys: Vec<String> = (0..BG_KEYS).map(|i| format!("bg::{i}")).collect();
        let mut recorded = 0usize;

        for _ in 0..TRIALS {
            let mut coco = Coco::<asap_sketchlib::DefaultXxHasher>::init_with_size(WIDTH, DEPTH);
            let mut sent = 0u64;
            for (i, key) in keys.iter().enumerate() {
                coco.insert(key, 1);
                while sent * (BG_KEYS as u64) < HEAVY * (i as u64 + 1) {
                    coco.insert("flow::heavy", 1);
                    sent += 1;
                }
            }
            while sent < HEAVY {
                coco.insert("flow::heavy", 1);
                sent += 1;
            }
            if coco.recorded_flows().any(|(key, _)| key == "flow::heavy") {
                recorded += 1;
            }
        }

        let recall = recorded as f64 / TRIALS as f64;
        assert!(
            recall >= TARGET_RECALL,
            "coco recall {recall:.4} below the paper's {TARGET_RECALL} target ({recorded}/{TRIALS})"
        );
    }

    // -----------------------------------------------------------------------
    // Elastic
    // -----------------------------------------------------------------------

    #[test]
    fn elastic_tracks_hot_flows() {
        let mut sk = Elastic::<asap_sketchlib::DefaultXxHasher>::init_with_length(64);

        // Three hot flows amid background chatter.
        let mut expected: HashMap<String, i64> = HashMap::new();
        for i in 0..12_000u64 {
            let id = match i % 10 {
                0 => "hot-alpha".to_string(),
                1 => "hot-beta".to_string(),
                2 => "hot-gamma".to_string(),
                _ => format!("bg{}", i % 977),
            };
            sk.insert(id.clone());
            *expected.entry(id).or_insert(0) += 1;
        }

        for hot in ["hot-alpha", "hot-beta", "hot-gamma"] {
            let c = sk.query(hot.to_string()) as i64;
            let t = expected[hot];
            assert_between(
                c as f64,
                t as f64 * 0.80,
                t as f64 * 1.20,
                &format!("elastic flow {hot}"),
            );
        }
    }

    /// The light layer carries every flow the heavy part evicted, so its
    /// dimensions set the error on those flows. Section 4.1 discusses picking the
    /// depth; this checks the choice actually reaches the estimate.
    #[test]
    fn elastic_light_dimensions_set_the_error_on_evicted_flows() {
        const TARGET: i32 = 50;
        const BG_KEYS: usize = 20_000;

        // 8 heavy buckets against 20k distinct flows evicts the target every time.
        let evicted_estimate = |rows: usize, cols: usize| {
            let mut sk =
                Elastic::<asap_sketchlib::DefaultXxHasher>::init_with_dimensions(8, rows, cols);
            for _ in 0..TARGET {
                sk.insert("flow::target".to_string());
            }
            for i in 0..BG_KEYS {
                sk.insert(format!("bg::{i}"));
            }
            assert!(
                !sk.heavy.iter().any(|b| b.flow_id == "flow::target"),
                "the target must be evicted for this to measure the light layer"
            );
            sk.query("flow::target".to_string())
        };

        let narrow = evicted_estimate(1, 64);
        let wide = evicted_estimate(3, 4096);

        // Elastic never underestimates, whatever the light layer costs in error.
        assert!(narrow >= TARGET, "narrow light underestimated: {narrow}");
        assert!(wide >= TARGET, "wide light underestimated: {wide}");

        // Measured 379 against 50: a 1x64 light collides ~7.6x worse than 3x4096.
        assert!(
            wide <= TARGET * 2,
            "a 3x4096 light should stay near the truth, got {wide} for {TARGET}"
        );
        assert!(
            narrow >= wide * 4,
            "light dimensions must reach the estimate: 1x64 gave {narrow}, 3x4096 gave {wide}"
        );
    }

    /// Elastic's estimator is one-sided: a resident flow reports its own votes, a
    /// displaced flow keeps its full size in the light layer, and every other flow
    /// reads a Count-Min over-estimate. No flow may come back short.
    #[test]
    fn elastic_never_underestimates_under_eviction_pressure() {
        // A small heavy table against a wide key space forces repeated takeovers.
        let mut sk = Elastic::<asap_sketchlib::DefaultXxHasher>::init_with_length(16);
        let mut truth: HashMap<String, i64> = HashMap::new();

        for (i, key) in zipf_u64(60_000, 4_000, 1.1, 909).into_iter().enumerate() {
            let id = if i % 500 == 0 {
                "flow::elephant".to_string()
            } else {
                format!("flow::{key}")
            };
            sk.insert(id.clone());
            *truth.entry(id).or_insert(0) += 1;
        }

        let mut evicted_seen = 0usize;
        for (id, count) in &truth {
            let est = sk.query(id.clone()) as i64;
            assert!(
                est >= *count,
                "elastic underestimated {id}: got {est}, true {count}"
            );
            if sk.heavy.iter().any(|b| b.flow_id == *id) {
                continue;
            }
            evicted_seen += 1;
        }
        assert!(
            evicted_seen > 0,
            "the workload must actually push flows out of the heavy part"
        );

        // The elephant is resident in the heavy table, so its estimate is its
        // own vote count plus whatever the light layer adds if it was ever
        // displaced. The ceiling is Count-Min's on the light layer at its own
        // dimensions — `e * (N - f) / cols` — not a written 5%: the light layer
        // *is* a Count-Min, and this is the bound it promises.
        let elephant = sk.query("flow::elephant".to_string()) as i64;
        let true_elephant = truth["flow::elephant"];
        let total: i64 = truth.values().sum();
        let light_ceiling = true_elephant as f64
            + std::f64::consts::E * (total - true_elephant) as f64 / sk.light.cols() as f64;
        assert!(
            elephant >= true_elephant,
            "elastic must never read the elephant low: got {elephant}, true {true_elephant}"
        );
        assert!(
            elephant as f64 <= light_ceiling,
            "elastic elephant {elephant} above true {true_elephant} + e*(N-f)/cols = \
             {light_ceiling:.1} (N={total}, light cols={})",
            sk.light.cols()
        );
    }
}
