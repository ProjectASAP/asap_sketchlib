//! Property tests for UnivMon-Q, the quantile-carrying UnivMon variant.
//!
//! The order-preserving u64 encoding underneath the quantile path is an exact
//! equality: it must be injective and order preserving over every f64,
//! negatives, zero, subnormals and both infinities included.
//!
//! The oracle for that encoding is IEEE 754-2008 `totalOrder`, read out of the
//! standard library as `f64::total_cmp` — a comparison written independently
//! of this crate, and a total order over bit patterns, so NaN participates
//! (negative NaN below every number, positive NaN above) and `-0.0` sits below
//! `0.0`. The encoder itself is private, so the laws reach it through `min`
//! and `max`, the two answers built by comparing encoded keys and decoding the
//! winner: a pair whose extremes come back bit-identical and on the side
//! `total_cmp` puts them pins injectivity, order preservation and the decode
//! at once.
//!
//! The ordered-query laws are relations rather than values. `quantile` and
//! `rank` read one CDF whose breakpoints are keyed by the encoding, so both
//! inherit its order: `quantile` cannot fall as `q` rises and must stay inside
//! the exact extrema, and `rank` cannot fall as its argument rises.

use crate::support::extreme_f64;
use asap_sketchlib::{DefaultXxHasher, UnivMonQ, UnivMonQConfig};
use proptest::prelude::*;

type Q = UnivMonQ<DefaultXxHasher>;

/// A hierarchy small enough to build thousands of times, wide enough that the
/// candidate tables and the ordered sample both fill and evict.
fn config(ordered_samples: usize) -> UnivMonQConfig {
    UnivMonQConfig {
        levels: 4,
        width: 32,
        width_halving_period: 0,
        depth: 3,
        counter_bits: 32,
        candidates: 8,
        ordered_samples,
        hash_seed: 5,
    }
}

/// Geometries the wire law walks: both counter widths, an even and an odd
/// halving period, and an ordered sample that the stream can overrun.
fn shapes() -> impl Strategy<Value = UnivMonQConfig> {
    (
        2usize..5,
        prop_oneof![Just(16usize), Just(32), Just(48)],
        prop_oneof![Just(0u8), Just(1), Just(2)],
        prop_oneof![Just(1usize), Just(3)],
        prop_oneof![Just(32u8), Just(64)],
        1usize..12,
        1usize..48,
    )
        .prop_map(
            |(
                levels,
                width,
                width_halving_period,
                depth,
                counter_bits,
                candidates,
                ordered_samples,
            )| {
                UnivMonQConfig {
                    levels,
                    width,
                    width_halving_period,
                    depth,
                    counter_bits,
                    candidates,
                    ordered_samples,
                    hash_seed: 5,
                }
            },
        )
}

fn sketch_of(config: UnivMonQConfig, values: &[f64]) -> Q {
    let mut sketch = Q::new(config).expect("the configuration is valid");
    for value in values {
        sketch.update(value);
    }
    sketch
}

fn stream(max: usize) -> impl Strategy<Value = Vec<f64>> {
    prop::collection::vec(extreme_f64(), 1..max)
}

/// Probes in ascending `totalOrder`, the order the monotone laws are stated in.
fn ascending_values(max: usize) -> impl Strategy<Value = Vec<f64>> {
    prop::collection::vec(extreme_f64(), 1..max).prop_map(|mut values| {
        values.sort_by(f64::total_cmp);
        values
    })
}

/// Ascending `q`, with both endpoints present: `quantile` answers 0 and 1 from
/// the exact extrema and everything between from the CDF, so the chain has to
/// cross that boundary to constrain it.
fn ascending_quantiles() -> impl Strategy<Value = Vec<f64>> {
    prop::collection::vec(0.0f64..=1.0, 1..8).prop_map(|mut qs| {
        qs.push(0.0);
        qs.push(1.0);
        qs.sort_by(f64::total_cmp);
        qs
    })
}

fn bits(value: f64) -> u64 {
    value.to_bits()
}

proptest! {
    // ===== Encoding =====

    // Two arrivals, and the encoding decides which one `min` reports: the
    // answer agrees with `totalOrder` over the whole f64 domain, and both
    // extrema decode to the bits that went in. A non-injective encoding
    // collapses the pair and fails one of the two.
    #[test]
    fn the_extrema_of_a_pair_follow_the_ieee_total_order(
        left in extreme_f64(),
        right in extreme_f64(),
    ) {
        let sketch = sketch_of(config(0), &[left, right]);

        let (lower, upper) = if left.total_cmp(&right).is_le() {
            (left, right)
        } else {
            (right, left)
        };

        prop_assert_eq!(
            sketch.min().map(bits),
            Some(bits(lower)),
            "min of ({:?}, {:?}): total_cmp puts {:?} first",
            left, right, lower
        );
        prop_assert_eq!(
            sketch.max().map(bits),
            Some(bits(upper)),
            "max of ({:?}, {:?}): total_cmp puts {:?} last",
            left, right, upper
        );
    }

    // Arrival order cannot reach the extrema, and a single value survives the
    // encode and the decode with every bit intact - the sign of a zero and a
    // NaN payload included.
    #[test]
    fn a_lone_value_returns_bit_identical_from_both_extrema(value in extreme_f64()) {
        let sketch = sketch_of(config(0), &[value]);

        prop_assert_eq!(sketch.min().map(bits), Some(bits(value)), "min lost bits of {:?}", value);
        prop_assert_eq!(sketch.max().map(bits), Some(bits(value)), "max lost bits of {:?}", value);
    }

    // The whole stream, not just a pair: the reported extrema are the
    // `totalOrder` extrema of the multiset.
    #[test]
    fn the_extrema_of_a_stream_are_the_total_order_extrema(values in stream(64)) {
        let sketch = sketch_of(config(0), &values);

        let lowest = values.iter().copied().min_by(f64::total_cmp).expect("non-empty");
        let highest = values.iter().copied().max_by(f64::total_cmp).expect("non-empty");

        prop_assert_eq!(
            sketch.min().map(bits), Some(bits(lowest)),
            "min over {} values: expected {:?}", values.len(), lowest
        );
        prop_assert_eq!(
            sketch.max().map(bits), Some(bits(highest)),
            "max over {} values: expected {:?}", values.len(), highest
        );
    }
}

proptest! {
    // ===== Ordered queries =====

    // The CDF is keyed by the encoding and read with a partition point, so a
    // higher `q` can only select a breakpoint at or to the right of a lower
    // one. The extrema bound it because the CDF always carries them.
    #[test]
    fn quantile_never_falls_as_q_rises(
        values in stream(64),
        quantiles in ascending_quantiles(),
    ) {
        let sketch = sketch_of(config(16), &values);
        let lowest = sketch.min().expect("non-empty");
        let highest = sketch.max().expect("non-empty");

        let answers = sketch.quantiles(&quantiles);
        let mut previous: Option<(f64, f64)> = None;
        for (q, answer) in quantiles.iter().zip(&answers) {
            let value = answer.expect("a non-empty sketch with an ordered sample answers");
            prop_assert!(
                lowest.total_cmp(&value).is_le() && value.total_cmp(&highest).is_le(),
                "quantile({}) = {:?} outside the exact extrema [{:?}, {:?}]",
                q, value, lowest, highest
            );
            if let Some((before_q, before)) = previous {
                prop_assert!(
                    before.total_cmp(&value).is_le(),
                    "quantile({}) = {:?} fell below quantile({}) = {:?}",
                    q, value, before_q, before
                );
            }
            previous = Some((*q, value));
        }
    }

    // `quantiles` reconstructs the CDF once and `quantile` once per call; the
    // two must not disagree about any breakpoint.
    #[test]
    fn the_batched_quantiles_match_the_one_at_a_time_answers(
        values in stream(64),
        quantiles in ascending_quantiles(),
    ) {
        let sketch = sketch_of(config(16), &values);

        let batched: Vec<Option<u64>> = sketch
            .quantiles(&quantiles)
            .into_iter()
            .map(|answer| answer.map(bits))
            .collect();
        let one_at_a_time: Vec<Option<u64>> = quantiles
            .iter()
            .map(|q| sketch.quantile(*q).map(bits))
            .collect();

        prop_assert_eq!(batched, one_at_a_time, "over {:?}", quantiles);
    }

    // Rank counts what the CDF places at or below its argument, and the
    // encoding carries `totalOrder` into that comparison, so the count cannot
    // shrink as the argument climbs. It stays inside `0..=count` at every
    // probe, including the two saturating branches outside the extrema.
    #[test]
    fn rank_never_falls_as_its_argument_rises(
        values in stream(64),
        probes in ascending_values(12),
    ) {
        let sketch = sketch_of(config(16), &values);
        let count = sketch.count();

        let mut previous: Option<(f64, u64)> = None;
        for probe in &probes {
            let rank = sketch.rank(*probe).expect("a non-empty sketch with an ordered sample answers");
            prop_assert!(
                rank <= count,
                "rank({:?}) = {} over a count of {}", probe, rank, count
            );
            if let Some((before_probe, before)) = previous {
                prop_assert!(
                    before <= rank,
                    "rank({:?}) = {} fell below rank({:?}) = {}",
                    probe, rank, before_probe, before
                );
            }
            previous = Some((*probe, rank));
        }
    }
}

proptest! {
    // ===== Wire =====

    #[test]
    fn univmon_q_round_trips_at_edge_geometries(
        config in shapes(),
        values in stream(96),
    ) {
        let sketch = sketch_of(config, &values);

        round_trip!(
            Q,
            sketch,
            |s: &Q| s.count(),
            |s: &Q| s.min().map(bits),
            |s: &Q| s.max().map(bits),
            |s: &Q| s.cdf().iter().map(|p| (bits(p.value), bits(p.rank))).collect::<Vec<_>>(),
            |s: &Q| bits(s.estimate_f2()),
        );
    }
}
