//! Property tests for `SetAggregator`, the exact distinct-set structure.
//!
//! There is no error band here, so every law is an exact equality. The oracle
//! is a `std::collections::HashSet` built from the stream on the test side.

use asap_sketchlib::SetAggregator;
use asap_sketchlib::message_pack_format::MessagePackCodec;
use proptest::prelude::*;
use std::collections::HashSet;

/// A key domain narrow enough that a stream repeats itself and two streams
/// intersect by construction rather than by luck.
fn set_keys(max: usize) -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec("[a-c]{0,2}", 0..max)
}

/// Keys that stress the wire encoding: empty, non-ASCII, embedded NUL,
/// and long, mixed into a draw that is mostly ordinary.
fn awkward_keys(max: usize) -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(
        prop_oneof![
            6 => "[a-c]{0,2}".boxed(),
            1 => Just(String::new()).boxed(),
            1 => Just("\u{e9}\u{4e2d}\u{1f600}".to_string()).boxed(),
            1 => Just("a\u{0}b".to_string()).boxed(),
            1 => Just("x".repeat(300)).boxed(),
            1 => any::<String>().boxed(),
        ],
        0..max,
    )
}

fn aggregator_of(keys: &[String]) -> SetAggregator {
    let mut s = SetAggregator::new();
    for k in keys {
        s.update(k);
    }
    s
}

fn oracle(keys: &[String]) -> HashSet<String> {
    keys.iter().cloned().collect()
}

fn stream_and_permutation(max: usize) -> impl Strategy<Value = (Vec<String>, Vec<String>)> {
    set_keys(max).prop_flat_map(|v| (Just(v.clone()), Just(v).prop_shuffle()))
}

proptest! {
    // ===== Exactness =====

    #[test]
    fn the_key_set_is_exactly_the_distinct_keys_of_the_stream(stream in set_keys(200)) {
        let s = aggregator_of(&stream);
        let want = oracle(&stream);

        prop_assert_eq!(&s.values, &want);
        prop_assert_eq!(s.values.len(), want.len(), "distinct count for {:?}", stream);
    }

    #[test]
    fn membership_answers_yes_for_the_inserted_keys_and_no_for_the_rest(
        stream in set_keys(200),
        probes in set_keys(40),
    ) {
        let s = aggregator_of(&stream);
        let want = oracle(&stream);

        for p in &probes {
            prop_assert_eq!(
                s.values.contains(p),
                want.contains(p),
                "membership of {:?} after {:?}", p, stream
            );
        }
    }

    #[test]
    fn stream_order_does_not_reach_the_key_set((a, b) in stream_and_permutation(200)) {
        prop_assert_eq!(aggregator_of(&a).values, aggregator_of(&b).values);
    }

    #[test]
    fn replaying_a_stream_leaves_the_key_set_unchanged(stream in set_keys(200)) {
        let mut s = aggregator_of(&stream);
        let before = s.values.clone();

        for k in stream.iter().rev() {
            s.update(k);
        }

        prop_assert_eq!(&s.values, &before, "replay moved the set for {:?}", stream);
    }

    // ===== Merge is set union =====

    #[test]
    fn merge_is_the_union_of_the_two_key_sets_and_leaves_the_other_alone(
        a in set_keys(120),
        b in set_keys(120),
    ) {
        let mut left = aggregator_of(&a);
        let right = aggregator_of(&b);
        let right_before = right.values.clone();

        left.merge(&right).expect("merge of two aggregators");

        let want: HashSet<String> = oracle(&a).union(&oracle(&b)).cloned().collect();
        prop_assert_eq!(&left.values, &want, "union of {:?} and {:?}", a, b);
        prop_assert_eq!(&right.values, &right_before, "merge mutated its argument");
    }

    #[test]
    fn merge_is_commutative_associative_and_idempotent(
        a in set_keys(80),
        b in set_keys(80),
        c in set_keys(80),
    ) {
        let (sa, sb, sc) = (aggregator_of(&a), aggregator_of(&b), aggregator_of(&c));

        let mut ab = sa.clone();
        ab.merge(&sb).expect("merge");
        let mut ba = sb.clone();
        ba.merge(&sa).expect("merge");
        prop_assert_eq!(&ab.values, &ba.values, "not commutative on {:?} and {:?}", a, b);

        let mut left = ab.clone();
        left.merge(&sc).expect("merge");
        let mut bc = sb.clone();
        bc.merge(&sc).expect("merge");
        let mut right = sa.clone();
        right.merge(&bc).expect("merge");
        prop_assert_eq!(&left.values, &right.values, "not associative on {:?}, {:?}, {:?}", a, b, c);

        let mut again = ab.clone();
        again.merge(&ab).expect("merge");
        prop_assert_eq!(&again.values, &ab.values, "not idempotent on {:?} and {:?}", a, b);
    }

    #[test]
    fn the_empty_aggregator_is_the_identity_of_merge(stream in set_keys(120)) {
        let s = aggregator_of(&stream);

        let mut from_left = SetAggregator::new();
        from_left.merge(&s).expect("merge");
        prop_assert_eq!(&from_left.values, &s.values, "empty was not a left identity");

        let mut from_right = s.clone();
        from_right.merge(&SetAggregator::new()).expect("merge");
        prop_assert_eq!(&from_right.values, &s.values, "empty was not a right identity");
    }

    #[test]
    fn merge_refs_unions_every_input_and_refuses_an_empty_list(
        streams in prop::collection::vec(set_keys(60), 0..5),
    ) {
        let parts: Vec<SetAggregator> = streams.iter().map(|s| aggregator_of(s)).collect();
        let refs: Vec<&SetAggregator> = parts.iter().collect();

        match SetAggregator::merge_refs(&refs) {
            Ok(merged) => {
                prop_assert!(!streams.is_empty(), "an empty input list must not produce a set");
                let want: HashSet<String> = streams.iter().flatten().cloned().collect();
                prop_assert_eq!(&merged.values, &want, "union of {:?}", streams);
            }
            Err(_) => prop_assert!(streams.is_empty(), "a non-empty input list must merge"),
        }
    }

    // ===== Wire =====

    #[test]
    fn a_msgpack_round_trip_preserves_the_key_set(stream in awkward_keys(120)) {
        let s = aggregator_of(&stream);

        let bytes = s.to_msgpack().expect("encode");
        let restored = SetAggregator::from_msgpack(&bytes).expect("decode");

        prop_assert_eq!(&restored.values, &s.values, "round trip moved the set");
        prop_assert_eq!(restored.values.len(), s.values.len());

        let twice = SetAggregator::from_msgpack(
            &restored.to_msgpack().expect("re-encode")
        ).expect("decode a re-encode");
        prop_assert_eq!(&twice.values, &s.values, "the second round trip moved the set");
    }
}
