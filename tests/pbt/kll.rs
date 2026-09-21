//! Property tests for KLL: the native sketch and the portable `KllSketch`
//! that carries it over the wire.
//!
//! Karnin, Lang, Liberty, FOCS '16. Compaction is randomized, so a law may
//! not name the items it keeps. What survives: a quantile is monotone in q,
//! it is one of the ingested values, the count and the quantile error stay
//! inside the band the compaction schedule allows, `rank` and `cdf` agree
//! with each other, and a seeded sketch replays exactly.
//!
//! The sketch carries no separate min/max register, so the extremes are only
//! exact until the first compaction; see
//! `kll_the_ends_are_exact_until_the_first_compaction`.

use crate::support::{QUANTILES, close, extreme_values};
use asap_sketchlib::message_pack_format::MessagePackCodec;
use asap_sketchlib::{KLL, KllSketch};
use proptest::prelude::*;

const QUANTILE_PROBES: [f64; 7] = [0.0, 0.01, 0.25, 0.5, 0.75, 0.99, 1.0];

/// Compaction empties level 0 once it holds more than `k` items, so a stream
/// this much longer than `k` has compacted many times over and carries items
/// at several weights.
const OVER_THRESHOLD: usize = 30;

/// `q * count()` rounds a few ulps above the exact product, so a rank sitting
/// exactly on it reads as a hair below. Scaled by the count, this absorbs that.
const RANK_SLACK: f64 = 1e-9;

/// The randomized KLL rank error, `eps * n` with `eps = C / k`. `C` is set
/// well above the worst drift this suite reaches (about 3.3) because the
/// guarantee is probabilistic and the suite draws thousands of streams.
const ERROR_CONSTANT: f64 = 16.0;

fn native_kll(k: i32, seed: u64, values: &[f64]) -> KLL<f64> {
    let mut s: KLL<f64> = KLL::init_kll_with_seed(k, seed);
    for v in values {
        s.update(v);
    }
    s
}

/// A stream with no repeated value, shuffled out of sorted order. The rank
/// laws need distinctness: with duplicates, `rank(x)` counts every copy while
/// the CDF's binary search stops at an arbitrary one of them.
fn distinct_stream(len: impl Strategy<Value = usize>) -> impl Strategy<Value = Vec<f64>> {
    (
        len,
        -1e4f64..1e4,
        prop_oneof![Just(1.0f64), Just(1e-3), Just(7.5)],
    )
        .prop_flat_map(|(n, offset, step)| {
            let values: Vec<f64> = (0..n).map(|i| offset + step * i as f64).collect();
            Just(values).prop_shuffle()
        })
}

/// A stream over a domain narrow enough that most values arrive many times.
fn repeating_stream(max: usize) -> impl Strategy<Value = Vec<f64>> {
    prop::collection::vec(-64i64..64, 1..max)
        .prop_map(|v| v.into_iter().map(|x| x as f64).collect())
}

/// `k` large enough that the rank budget below is a fraction of the stream
/// rather than wider than it, paired with a stream thirty times longer. At
/// `k = 8` the budget `ERROR_CONSTANT * n / k` is twice the stream itself, so
/// the accuracy law would hold for any answer at all.
fn accurate_k_and_stream(extra: usize) -> impl Strategy<Value = (i32, Vec<f64>)> {
    prop_oneof![Just(200usize), Just(400)]
        .prop_flat_map(move |k| {
            let n = (OVER_THRESHOLD * k)..(OVER_THRESHOLD * k + extra);
            (Just(k as i32), distinct_stream(n))
        })
        .boxed()
}

/// `k` paired with a stream long enough to force compaction at `k`.
fn k_and_long_stream(extra: usize) -> impl Strategy<Value = (i32, Vec<f64>)> {
    (8usize..=64)
        .prop_flat_map(move |k| {
            let n = (OVER_THRESHOLD * k)..(OVER_THRESHOLD * k + extra);
            (Just(k as i32), distinct_stream(n))
        })
        .boxed()
}

/// The heaviest weight any retained item can carry: level `num_levels - 1`.
fn top_weight(s: &KLL<f64>) -> f64 {
    (1u64 << (s.wire_num_levels() - 1)) as f64
}

fn kll_of(k: u16, values: &[f64]) -> KllSketch {
    let mut s = KllSketch::with_seed(k, 0xC0FFEE);
    for v in values {
        s.update(*v);
    }
    s
}

proptest! {
    // ===== Count =====

    #[test]
    fn kll_count_is_exact_below_the_compaction_threshold(
        k in 8u16..256,
        values in prop::collection::vec(-1e5f64..1e5, 0..8),
    ) {
        let s = kll_of(k, &values);
        prop_assert_eq!(s.count(), values.len() as u64);
    }

    #[test]
    fn kll_merged_count_stays_within_a_relative_band(
        k in 32u16..256,
        a in prop::collection::vec(-1e5f64..1e5, 0..400),
        b in prop::collection::vec(-1e5f64..1e5, 0..400),
    ) {
        let mut merged = kll_of(k, &a);
        merged.merge(&kll_of(k, &b)).expect("merge");

        let n = (a.len() + b.len()) as f64;
        let got = merged.count() as f64;
        let slack = (0.10 * n).max(4.0);
        prop_assert!(
            (got - n).abs() <= slack,
            "k={} n={} count()={} outside +/-{}", k, n, got, slack
        );
    }

    // ===== Quantiles =====

    #[test]
    fn kll_quantiles_are_monotone_in_q(
        k in 32u16..256,
        values in prop::collection::vec(-1e5f64..1e5, 1..400),
    ) {
        let s = kll_of(k, &values);
        let mut prev = f64::NEG_INFINITY;
        for i in 0..=20 {
            let q = i as f64 / 20.0;
            let v = s.quantile(q);
            prop_assert!(v >= prev, "q={} gave {} after {}", q, v, prev);
            prev = v;
        }
    }

    #[test]
    fn kll_quantile_range_is_inside_the_observed_range(
        k in 32u16..256,
        values in prop::collection::vec(-1e5f64..1e5, 1..400),
    ) {
        let s = kll_of(k, &values);
        let lo = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let hi = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        prop_assert!(s.quantile(0.0) >= lo, "min {} below observed {}", s.quantile(0.0), lo);
        prop_assert!(s.quantile(1.0) <= hi, "max {} above observed {}", s.quantile(1.0), hi);
    }

    // ===== Wire =====

    #[test]
    fn kll_round_trip_preserves_count_and_quantiles(
        k in 8u16..256,
        values in prop::collection::vec(-1e6f64..1e6, 0..300),
    ) {
        let mut s = KllSketch::with_seed(k, 0x5EED);
        for v in &values {
            s.update(*v);
        }

        let bytes = s.to_msgpack().expect("encode");
        let restored = KllSketch::from_msgpack(&bytes).expect("decode");

        prop_assert_eq!(restored.k(), s.k());
        prop_assert_eq!(restored.count(), s.count());
        for q in QUANTILE_PROBES {
            prop_assert_eq!(restored.quantile(q), s.quantile(q), "q={}", q);
        }
    }

    #[test]
    fn kll_round_trips_under_extreme_values(
        k in prop_oneof![Just(1i32), Just(8), Just(64), Just(200), Just(1_024)],
        values in extreme_values(300),
    ) {
        let mut sketch: KLL<f64> = KLL::init_kll_with_seed(k, 0x5EED_0001);
        for v in &values {
            sketch.update(v);
        }

        round_trip!(
            KLL<f64>,
            sketch,
            |s: &KLL<f64>| s.count(),
            |s: &KLL<f64>| QUANTILES.iter().map(|q| s.quantile(*q).to_bits()).collect::<Vec<_>>(),
        );
    }
}

proptest! {
    // ===== Retained values =====

    // KLL only ever copies items: compaction drops half a level and promotes
    // the rest untouched, and the CDF reads values straight out of the
    // compactors. No query may synthesize a value the stream never carried.
    #[test]
    fn kll_every_quantile_is_a_value_the_stream_carried(
        k in 8i32..=64,
        seed in any::<u64>(),
        values in repeating_stream(1_500),
    ) {
        let s = native_kll(k, seed, &values);
        let seen: std::collections::HashSet<u64> =
            values.iter().map(|v| v.to_bits()).collect();

        for i in 0..=40 {
            let q = i as f64 / 40.0;
            let v = s.quantile(q);
            prop_assert!(
                seen.contains(&v.to_bits()),
                "k={} q={} returned {}, which never arrived", k, q, v
            );
        }
    }

    // Level 0 holds every item until it exceeds `k`, so up to that point the
    // sketch is the exact stream and both ends are the stream's own. Past the
    // first compaction the extremes are ordinary items with no protection, so
    // this law stops where compaction starts.
    #[test]
    fn kll_the_ends_are_exact_until_the_first_compaction(
        (k, values) in (8u16..=256).prop_flat_map(|k| {
            // `k` itself is drawn on its own: the boundary is where the law
            // ends, and a uniform length would reach it once in `k` cases.
            let n = prop_oneof![Just(k as usize), 1..=k as usize];
            (Just(k), n.prop_flat_map(|n| prop::collection::vec(-1e5f64..1e5, n)))
        }),
    ) {
        let s = kll_of(k, &values);
        let lo = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let hi = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        prop_assert_eq!(s.quantile(0.0), lo, "k={} n={} lower end", k, values.len());
        prop_assert_eq!(s.quantile(1.0), hi, "k={} n={} upper end", k, values.len());
    }

    // ===== rank against quantile =====

    // `quantile(q)` returns the first retained value whose cumulative weight
    // reaches `q * count()`, and `rank` re-derives that same cumulative weight
    // by walking the levels. So the overshoot is exactly the weight of that one
    // item, at most the top level's `2^(num_levels - 1)`, and there is no
    // undershoot at all.
    #[test]
    fn kll_rank_of_a_quantile_overshoots_by_at_most_one_item_weight(
        (k, values) in k_and_long_stream(1_000),
        seed in any::<u64>(),
    ) {
        let s = native_kll(k, seed, &values);
        let n = s.count() as f64;
        let ceiling = top_weight(&s);

        for i in 0..=40 {
            let q = i as f64 / 40.0;
            let v = s.quantile(q);
            let drift = s.rank(v) as f64 - q * n;
            prop_assert!(
                drift >= -RANK_SLACK * n,
                "k={} q={} rank {} below q*n {}", k, q, s.rank(v), q * n
            );
            prop_assert!(
                drift <= ceiling,
                "k={} q={} rank {} exceeds q*n {} by {}, over one weight of {}",
                k, q, s.rank(v), q * n, drift, ceiling
            );
        }
    }

    #[test]
    fn kll_rank_never_falls_as_the_probe_rises(
        (k, values) in k_and_long_stream(1_000),
        seed in any::<u64>(),
    ) {
        let s = native_kll(k, seed, &values);
        let lo = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let hi = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let span = hi - lo;

        prop_assert_eq!(s.rank(lo - span - 1.0), 0);
        prop_assert_eq!(s.rank(hi), s.count());

        let mut previous = 0usize;
        for i in 0..=40 {
            let x = lo + span * i as f64 / 40.0;
            let r = s.rank(x);
            prop_assert!(r >= previous, "k={} x={} rank fell to {} from {}", k, x, r, previous);
            prop_assert!(r <= s.count(), "k={} x={} rank {} above count", k, x, r);
            previous = r;
        }
    }

    // ===== cdf =====

    // Two independent readings of the same compactors: `rank` sums level
    // weights in place, `cdf` flattens every item into one sorted table and
    // takes prefix sums. They must land on the same number.
    #[test]
    fn kll_the_cdf_is_rank_normalized_by_the_count(
        (k, values) in k_and_long_stream(1_000),
        seed in any::<u64>(),
    ) {
        let s = native_kll(k, seed, &values);
        let cdf = s.cdf();
        let n = s.count() as f64;
        let lo = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let hi = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let span = hi - lo;

        let mut previous = 0.0f64;
        for i in 0..=40 {
            let x = lo + span * i as f64 / 40.0;
            let p = cdf.quantile(x);

            prop_assert!((0.0..=1.0).contains(&p), "k={} x={} cdf {} outside [0, 1]", k, x, p);
            prop_assert!(p >= previous, "k={} x={} cdf fell to {} from {}", k, x, p, previous);
            prop_assert!(
                close(p * n, s.rank(x) as f64),
                "k={} x={} cdf {} times count {} against rank {}", k, x, p, n, s.rank(x)
            );
            previous = p;
        }

        prop_assert_eq!(cdf.quantile(lo - span - 1.0), 0.0);
        prop_assert_eq!(cdf.quantile(hi), 1.0);
    }

    // ===== Accuracy =====

    // The oracle is the sorted stream itself. KLL's guarantee is on rank, not
    // on the value: `quantile(q)` may be far from the true q-th value when the
    // distribution is flat there, but its true rank stays within `eps * n`.
    #[test]
    fn kll_quantile_rank_error_stays_inside_the_kll_band(
        (k, values) in accurate_k_and_stream(2_000),
        seed in any::<u64>(),
    ) {
        let s = native_kll(k, seed, &values);
        let mut oracle = values.clone();
        oracle.sort_by(f64::total_cmp);

        let n = oracle.len() as f64;
        let budget = ERROR_CONSTANT / k as f64 * n;
        prop_assert!(
            budget < n / 10.0,
            "k={} n={} leaves a budget of {}, which bounds nothing",
            k, n, budget
        );

        for i in 0..=40 {
            let q = i as f64 / 40.0;
            let v = s.quantile(q);
            let true_rank = oracle.partition_point(|x| *x <= v) as f64;
            prop_assert!(
                (true_rank - q * n).abs() <= budget,
                "k={} n={} q={} landed at true rank {}, {} off a budget of {}",
                k, n, q, true_rank, (true_rank - q * n).abs(), budget
            );
        }
    }

    // ===== Determinism and lifecycle =====

    // The compaction coin is the only nondeterminism in the sketch. Seeded, it
    // is a pure function of the seed, so the retained items and the level
    // boundaries are fixed by `(k, seed, stream)` alone.
    #[test]
    fn kll_a_seeded_sketch_replays_item_for_item(
        k in 8i32..=64,
        seed in any::<u64>(),
        values in distinct_stream(1usize..1_500),
    ) {
        let first = native_kll(k, seed, &values);
        let second = native_kll(k, seed, &values);

        prop_assert_eq!(first.wire_items(), second.wire_items());
        prop_assert_eq!(first.wire_levels(), second.wire_levels());
        prop_assert_eq!(first.count(), second.count());
    }

    // `clear` re-seeds the coin from the stored seed, so a cleared sketch is
    // not merely empty but back at the state a fresh one starts from: replaying
    // the stream reproduces the same items.
    #[test]
    fn kll_clear_returns_the_sketch_to_its_freshly_seeded_state(
        k in 8i32..=64,
        seed in any::<u64>(),
        values in distinct_stream(1usize..1_500),
    ) {
        let mut reused = native_kll(k, seed, &values);
        reused.clear();

        prop_assert_eq!(reused.count(), 0);
        prop_assert_eq!(reused.wire_num_levels(), 1);
        prop_assert!(reused.wire_items().is_empty());
        prop_assert_eq!(reused.quantile(0.5), 0.0);

        for v in &values {
            reused.update(v);
        }
        let fresh = native_kll(k, seed, &values);
        prop_assert_eq!(reused.wire_items(), fresh.wire_items());
        prop_assert_eq!(reused.wire_levels(), fresh.wire_levels());
    }
}
