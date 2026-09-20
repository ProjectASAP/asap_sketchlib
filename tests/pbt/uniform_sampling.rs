//! Property tests for `UniformSampling`, the reservoir-style sampler.
//!
//! The draw is the rng's business, so no law names a priority. What the sampler
//! does promise is checked against a plain `Vec` of the stream: the arrival
//! count is exact, every retained value is one that arrived, the retained count
//! is `ceil(total_seen * rate)`, and a rate of 1 degenerates to keeping the
//! stream whole.
//!
//! Which draws survive is pinned one step further. A full-rate run over the same
//! seed and stream keeps every arrival and lists it in draw order, so it reveals
//! the ranking the sampler is working from; a dropping rate is then replayed
//! against that ranking. The same ranking settles the merge: each side is kept
//! head first, and which side the merge is called on does not move the cut.
//!
//! Values are compared by `to_bits`, so `-0.0`, `0.0` and each NaN payload
//! stay distinct members of the stream multiset.
//!
//! Order independence is not a law here: a value's priority comes from the rng
//! position it arrives at, so a permuted stream retains a different set.

use std::collections::HashMap;
use std::panic::catch_unwind;

use crate::support::extreme_values;
use asap_sketchlib::{DataInput, UniformSampling};
use proptest::prelude::*;

/// Rates low enough that most of a few-hundred-value stream is dropped, both
/// ends of the accepted interval, and the degenerate rate that keeps
/// everything.
fn rate() -> impl Strategy<Value = f64> {
    prop_oneof![
        Just(f64::MIN_POSITIVE),
        Just(1e-9f64),
        Just(0.01),
        Just(0.05),
        Just(0.1),
        Just(0.25),
        Just(0.5),
        Just(0.75),
        Just(1.0),
    ]
}

/// The rates that actually discard, for laws a full-retention rate would
/// satisfy vacuously.
fn dropping_rate() -> impl Strategy<Value = f64> {
    prop_oneof![Just(0.01f64), Just(0.05), Just(0.1), Just(0.25), Just(0.5)]
}

/// Rates the constructor refuses: both open ends, the non-finite values, and
/// the negative zero that compares equal to the lower bound.
fn refused_rate() -> impl Strategy<Value = f64> {
    prop_oneof![
        Just(0.0f64),
        Just(-0.0),
        Just(f64::NAN),
        Just(f64::INFINITY),
        Just(f64::NEG_INFINITY),
        Just(f64::MAX),
        Just(f64::MIN),
        -1_000.0f64..0.0,
        1.0f64 + f64::EPSILON..1_000.0,
    ]
}

/// The sampler only carries its values, so the stream includes the f64 edges.
fn stream(max: usize) -> impl Strategy<Value = Vec<f64>> {
    extreme_values(max)
}

fn fed(rate: f64, seed: u64, values: &[f64]) -> UniformSampling {
    let mut sampler = UniformSampling::with_seed(rate, seed);
    for value in values {
        sampler.update(*value);
    }
    sampler
}

fn bits(values: &[f64]) -> Vec<u64> {
    values.iter().map(|v| v.to_bits()).collect()
}

fn sorted_bits(values: &[f64]) -> Vec<u64> {
    let mut b = bits(values);
    b.sort_unstable();
    b
}

fn bag(values: &[f64]) -> HashMap<u64, usize> {
    let mut counts = HashMap::new();
    for value in values {
        *counts.entry(value.to_bits()).or_insert(0usize) += 1;
    }
    counts
}

fn contained(inner: &HashMap<u64, usize>, outer: &HashMap<u64, usize>) -> bool {
    inner
        .iter()
        .all(|(value, n)| outer.get(value).is_some_and(|m| m >= n))
}

/// The retention target read off the spec rather than off the sampler.
fn target(total_seen: usize, rate: f64) -> usize {
    if total_seen == 0 {
        0
    } else {
        ((total_seen as f64) * rate).ceil() as usize
    }
}

proptest! {
    // ===== The stream counter =====

    #[test]
    fn total_seen_counts_every_arrival_whatever_the_rate(
        rate in rate(),
        seed in any::<u64>(),
        values in stream(200),
    ) {
        let mut sampler = UniformSampling::with_seed(rate, seed);
        for (idx, value) in values.iter().enumerate() {
            sampler.update(*value);
            prop_assert_eq!(
                sampler.total_seen(),
                idx as u64 + 1,
                "rate {}, arrival {}", rate, idx
            );
        }

        prop_assert_eq!(sampler.total_seen(), values.len() as u64, "rate {}", rate);
    }

    /// The two entry points agree, and an input the sampler cannot read is not
    /// an arrival.
    #[test]
    fn a_refused_input_is_not_an_arrival(
        rate in rate(),
        seed in any::<u64>(),
        values in prop::collection::vec(-1_000i64..1_000, 0..120),
    ) {
        let mut sampler = UniformSampling::with_seed(rate, seed);
        let mut twin = UniformSampling::with_seed(rate, seed);
        for (idx, value) in values.iter().enumerate() {
            prop_assert!(sampler.update_input(&DataInput::I64(*value)).is_ok());
            prop_assert!(
                sampler.update_input(&DataInput::Str("not a number")).is_err(),
                "a string was accepted at arrival {}", idx
            );
            twin.update(*value as f64);
            prop_assert_eq!(sampler.total_seen(), twin.total_seen(), "arrival {}", idx);
        }

        prop_assert_eq!(bits(&sampler.samples()), bits(&twin.samples()), "rate {}", rate);
    }

    // ===== What is retained =====

    #[test]
    fn every_retained_sample_came_from_the_stream(
        rate in rate(),
        seed in any::<u64>(),
        values in stream(200),
    ) {
        let sampler = fed(rate, seed, &values);

        prop_assert!(
            contained(&bag(&sampler.samples()), &bag(&values)),
            "rate {}, seed {}: retained {:?} is not drawn from the stream",
            rate, seed, sampler.samples()
        );
        prop_assert!(
            sampler.len() <= values.len(),
            "rate {}: {} retained out of {}", rate, sampler.len(), values.len()
        );
    }

    #[test]
    fn the_retained_count_is_the_ceiling_of_the_rate_times_the_stream(
        rate in rate(),
        seed in any::<u64>(),
        values in stream(200),
    ) {
        let mut sampler = UniformSampling::with_seed(rate, seed);
        prop_assert_eq!(sampler.len(), 0);

        for (idx, value) in values.iter().enumerate() {
            sampler.update(*value);
            prop_assert_eq!(
                sampler.len(),
                target(idx + 1, rate),
                "rate {}, arrival {}", rate, idx
            );
        }
    }

    #[test]
    fn a_rate_of_one_retains_the_whole_stream(
        seed in any::<u64>(),
        values in stream(200),
    ) {
        let sampler = fed(1.0, seed, &values);

        prop_assert_eq!(sampler.len(), values.len(), "seed {}", seed);
        prop_assert_eq!(bag(&sampler.samples()), bag(&values), "seed {}", seed);
    }

    #[test]
    fn the_indexed_read_walks_the_retained_samples(
        rate in rate(),
        seed in any::<u64>(),
        values in stream(200),
    ) {
        let sampler = fed(rate, seed, &values);
        let listed = sampler.samples();

        for (idx, value) in listed.iter().enumerate() {
            prop_assert_eq!(sampler.sample_at(idx).map(f64::to_bits), Some(value.to_bits()), "index {}", idx);
        }
        prop_assert_eq!(sampler.sample_at(listed.len()), None);
    }

    // ===== The draw sequence =====

    /// The seed and the stream fix the sample: a second sampler on the same
    /// seed agrees even though another sampler is driven between its arrivals.
    #[test]
    fn a_seed_and_a_stream_fix_which_samples_are_kept(
        rate in dropping_rate(),
        seed in any::<u64>(),
        other_seed in any::<u64>(),
        values in stream(200),
    ) {
        let first = fed(rate, seed, &values);

        let mut second = UniformSampling::with_seed(rate, seed);
        let mut interleaved = UniformSampling::with_seed(rate, other_seed);
        for value in &values {
            interleaved.update(*value);
            second.update(*value);
            interleaved.update(*value);
        }

        prop_assert_eq!(bits(&second.samples()), bits(&first.samples()), "rate {}, seed {}", rate, seed);
        prop_assert_eq!(second.total_seen(), first.total_seen());
    }

    /// Seed zero is the one seed the constructor rewrites, to the seed `new`
    /// uses. The draw stays reproducible.
    #[test]
    fn seed_zero_falls_back_to_the_default_seed(
        rate in dropping_rate(),
        values in stream(200),
    ) {
        let zeroed = fed(rate, 0, &values);

        let mut defaulted = UniformSampling::new(rate);
        for value in &values {
            defaulted.update(*value);
        }

        prop_assert_eq!(bits(&zeroed.samples()), bits(&defaulted.samples()), "rate {}", rate);
        prop_assert_eq!(bits(&fed(rate, 0, &values).samples()), bits(&zeroed.samples()), "rate {}", rate);
    }

    /// Which draws survive, not just how many. A full-rate run over the same
    /// seed and stream drops nothing and lists its entries in draw order, so it
    /// reveals the rank of every arrival without naming a priority. Replaying
    /// the stream against that ranking -- each arrival takes its rank's place,
    /// and an overfull sampler gives up its worst-ranked entry -- reproduces
    /// the dropping-rate run exactly, order included.
    #[test]
    fn the_retained_draws_follow_the_ranking_a_full_rate_run_reveals(
        rate in dropping_rate(),
        seed in any::<u64>(),
        len in 0..200usize,
    ) {
        let values: Vec<f64> = (0..len).map(|position| position as f64).collect();

        let ranking = fed(1.0, seed, &values).samples();
        prop_assert_eq!(ranking.len(), len, "the full-rate run dropped a draw");
        let mut rank_of = vec![0usize; len];
        for (rank, value) in ranking.iter().enumerate() {
            rank_of[*value as usize] = rank;
        }

        let mut held: Vec<usize> = Vec::new();
        for position in 0..len {
            let at = held.partition_point(|held| rank_of[*held] < rank_of[position]);
            held.insert(at, position);
            while held.len() > target(position + 1, rate) {
                held.pop();
            }
        }

        let expected: Vec<u64> = held.iter().map(|p| (*p as f64).to_bits()).collect();
        prop_assert_eq!(
            bits(&fed(rate, seed, &values).samples()),
            expected,
            "rate {}, seed {}", rate, seed
        );
    }

    /// The seed has to reach the draw. Two samplers that differ only in their
    /// seed keep different halves of a long stream; agreeing would mean the
    /// seed never reached the priorities.
    #[test]
    fn distinct_seeds_keep_different_halves_of_a_stream(
        rate in prop_oneof![Just(0.25f64), Just(0.5), Just(0.75)],
        (first_seed, second_seed) in (1u64.., 1u64..)
            .prop_filter("distinct seeds", |(a, b)| a != b),
    ) {
        let values: Vec<f64> = (0..128).map(|position| position as f64).collect();

        prop_assert_ne!(
            sorted_bits(&fed(rate, first_seed, &values).samples()),
            sorted_bits(&fed(rate, second_seed, &values).samples()),
            "rate {}, seeds {} and {}", rate, first_seed, second_seed
        );
    }

    // ===== The construction contract =====

    #[test]
    fn a_rate_outside_the_unit_interval_is_refused(
        rate in refused_rate(),
        seed in any::<u64>(),
    ) {
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let refused = catch_unwind(|| UniformSampling::with_seed(rate, seed)).is_err()
            && catch_unwind(|| UniformSampling::new(rate)).is_err();
        std::panic::set_hook(hook);

        prop_assert!(refused, "rate {} was accepted", rate);
    }

    // ===== Merge =====

    #[test]
    fn a_merge_adds_the_stream_lengths_and_keeps_only_what_the_sides_held(
        rate in rate(),
        left_seed in any::<u64>(),
        right_seed in any::<u64>(),
        left in stream(150),
        right in stream(150),
    ) {
        let a = fed(rate, left_seed, &left);
        let b = fed(rate, right_seed, &right);

        let mut merged = a.clone();
        merged.merge(&b).expect("equal rates merge");

        prop_assert_eq!(
            merged.total_seen(),
            a.total_seen() + b.total_seen(),
            "rate {}: {} and {}", rate, left.len(), right.len()
        );

        let mut union = a.samples();
        union.extend(b.samples());
        prop_assert!(
            contained(&bag(&merged.samples()), &bag(&union)),
            "rate {}: merged {:?} holds what neither side held", rate, merged.samples()
        );
        prop_assert_eq!(
            merged.len(),
            target(left.len() + right.len(), rate),
            "rate {}", rate
        );
    }

    /// A merge keeps the head of each side, in each side's own order: it ranks
    /// the two sides against each other and drops from the far end, so what it
    /// gives up is the tail of one side or the other. A side that never saw a
    /// value is the identity, since the target for the combined stream is the
    /// other side's own count.
    ///
    /// The two streams are told apart by sign, so each retained value names the
    /// side and the position it came from.
    #[test]
    fn a_merge_keeps_the_head_of_each_side_in_order(
        rate in rate(),
        left_seed in any::<u64>(),
        right_seed in any::<u64>(),
        left_len in 0..150usize,
        right_len in 0..150usize,
    ) {
        let left: Vec<f64> = (0..left_len).map(|position| position as f64).collect();
        let right: Vec<f64> = (0..right_len).map(|position| -(position as f64) - 1.0).collect();

        let a = fed(rate, left_seed, &left);
        let b = fed(rate, right_seed, &right);
        let mut merged = a.clone();
        merged.merge(&b).expect("equal rates merge");

        let kept = merged.samples();
        let from_left: Vec<u64> = bits(&kept.iter().copied().filter(|v| *v >= 0.0).collect::<Vec<_>>());
        let from_right: Vec<u64> = bits(&kept.iter().copied().filter(|v| *v < 0.0).collect::<Vec<_>>());
        prop_assert_eq!(from_left.len() + from_right.len(), kept.len());
        prop_assert!(
            from_left.len() <= a.len() && from_right.len() <= b.len(),
            "rate {}: the merge kept more of a side than the side held", rate
        );

        prop_assert_eq!(
            &from_left[..],
            &bits(&a.samples())[..from_left.len()],
            "rate {}: the left side is not kept head first", rate
        );
        prop_assert_eq!(
            &from_right[..],
            &bits(&b.samples())[..from_right.len()],
            "rate {}: the right side is not kept head first", rate
        );
    }

    /// Ranking the two sides against each other is symmetric: which sampler the
    /// merge is called on decides the order of the arguments, not which samples
    /// survive. The seeds are distinct, so the two sides draw apart and the
    /// ranking has no tie for the side order to settle.
    #[test]
    fn a_merge_keeps_the_same_samples_from_either_side(
        rate in rate(),
        (left_seed, right_seed) in (1u64.., 1u64..)
            .prop_filter("distinct seeds", |(a, b)| a != b),
        left in stream(150),
        right in stream(150),
    ) {
        let a = fed(rate, left_seed, &left);
        let b = fed(rate, right_seed, &right);

        let mut forward = a.clone();
        forward.merge(&b).expect("equal rates merge");
        let mut backward = b.clone();
        backward.merge(&a).expect("equal rates merge");

        prop_assert_eq!(
            sorted_bits(&forward.samples()),
            sorted_bits(&backward.samples()),
            "rate {}", rate
        );
        prop_assert_eq!(forward.total_seen(), backward.total_seen());
    }

    /// A merge carries the other side's draw sequence forward: two samplers
    /// that merged different partners hold the same samples at that moment but
    /// must not keep drawing in lockstep afterwards.
    #[test]
    fn a_merge_carries_the_other_sides_draw_sequence(
        seed in any::<u64>(),
        (first_partner, second_partner) in (1u64.., 1u64..)
            .prop_filter("distinct seeds", |(a, b)| a != b),
    ) {
        let values: Vec<f64> = (0..24).map(|position| position as f64).collect();
        let later: Vec<f64> = (0..24).map(|position| -(position as f64) - 1.0).collect();

        let mut first = fed(1.0, seed, &values);
        first.merge(&UniformSampling::with_seed(1.0, first_partner)).expect("equal rates merge");
        let mut second = fed(1.0, seed, &values);
        second.merge(&UniformSampling::with_seed(1.0, second_partner)).expect("equal rates merge");

        prop_assert_eq!(bits(&first.samples()), bits(&second.samples()), "the merge itself differed");

        for value in &later {
            first.update(*value);
            second.update(*value);
        }
        prop_assert_ne!(
            bits(&first.samples()),
            bits(&second.samples()),
            "partners {} and {} left the same draw sequence", first_partner, second_partner
        );
    }

    #[test]
    fn a_merge_across_rates_is_refused_and_leaves_the_sampler_alone(
        (left_rate, right_rate) in (rate(), rate())
            .prop_filter("distinct rates", |(a, b)| (a - b).abs() > f64::EPSILON),
        left_seed in any::<u64>(),
        right_seed in any::<u64>(),
        left in stream(150),
        right in stream(150),
    ) {
        let mut a = fed(left_rate, left_seed, &left);
        let b = fed(right_rate, right_seed, &right);
        let before = (a.total_seen(), bits(&a.samples()));

        prop_assert!(
            a.merge(&b).is_err(),
            "rates {} and {} merged", left_rate, right_rate
        );
        prop_assert_eq!((a.total_seen(), bits(&a.samples())), before, "the refused merge changed the sampler");
    }

    // ===== Wire =====

    #[test]
    fn a_sampler_round_trips_through_its_envelope(
        rate in rate(),
        seed in any::<u64>(),
        values in stream(200),
    ) {
        let sampler = fed(rate, seed, &values);

        round_trip!(
            UniformSampling,
            sampler,
            |s: &UniformSampling| sorted_bits(&s.samples()),
            |s: &UniformSampling| s.total_seen(),
            |s: &UniformSampling| s.len(),
            |s: &UniformSampling| s.sample_rate().to_bits(),
        );
    }

    /// The envelope carries the draw sequence too: a decoded sampler goes on
    /// drawing where the encoded one stopped, so the same continuation lands
    /// the same samples on both.
    #[test]
    fn a_decoded_sampler_draws_where_the_encoded_one_stopped(
        rate in dropping_rate(),
        seed in any::<u64>(),
        len in 0..120usize,
        later_len in 0..120usize,
    ) {
        let values: Vec<f64> = (0..len).map(|position| position as f64).collect();
        let later: Vec<f64> = (0..later_len).map(|position| -(position as f64) - 1.0).collect();
        let mut original = fed(rate, seed, &values);

        if let Ok(bytes) = original.serialize_to_bytes() {
            let mut decoded = UniformSampling::deserialize_from_bytes(&bytes)
                .expect("bytes the encoder produced must decode");
            for value in &later {
                original.update(*value);
                decoded.update(*value);
            }
            prop_assert_eq!(
                bits(&decoded.samples()),
                bits(&original.samples()),
                "rate {}, seed {}", rate, seed
            );
        }
    }
}
