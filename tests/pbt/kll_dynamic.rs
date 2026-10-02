//! Property tests for `KLLDynamic`, the KLL variant that widens its item type
//! at runtime.
//!
//! The quantile laws are P-13's, restated here against the dynamic item path.
//!
//! Compaction halves a level by keeping every other item from an offset the
//! coin picks, so which items survive is not fixed by the specification and no
//! model here binds it. What is fixed is the answer's shape: it is one of the
//! items that arrived, it walks the item order as `q` rises, and the weight
//! standing at or below it brackets `q * n`. The oracle for all of that is the
//! sorted stream, built without reference to the sketch.
//!
//! Below the first compaction the sketch holds the whole stream, so there the
//! oracle is exact, both ends included. Above it the ends are ordinary items
//! with nothing protecting them - there is no min/max register - so only the
//! two-sided bracket and the `O(n / k)` rank band survive.

use crate::support::{QUANTILES, close, extreme_f64, extreme_values};
use asap_sketchlib::KLLDynamic;
use proptest::prelude::*;
use std::cmp::Ordering;
use std::collections::HashSet;

/// `k` values whose level 0 overflows many times over within the stream
/// lengths below.
fn small_k() -> impl Strategy<Value = i32> {
    prop_oneof![Just(8i32), Just(16), Just(64), Just(200)]
}

/// A narrow domain mixed into a wide one, so equal values are common rather
/// than a coincidence between two draws out of a continuum.
fn ordinary() -> impl Strategy<Value = f64> {
    prop_oneof![
        3 => -1.0e6f64..1.0e6,
        1 => (0u32..64).prop_map(f64::from),
    ]
}

/// A stream thirty times `k` and longer: level 0 spills on the order of `n / k`
/// times and the sketch grows to a handful of levels.
fn stream(k: i32) -> impl Strategy<Value = Vec<f64>> {
    let n = 30 * k as usize;
    prop::collection::vec(ordinary(), n..n + 2_000)
}

fn compacting() -> impl Strategy<Value = (i32, Vec<f64>)> {
    small_k().prop_flat_map(|k| (Just(k), stream(k)))
}

/// The same lengths over a draw that includes NaN, the infinities and the
/// signed zeros.
fn extreme_compacting() -> impl Strategy<Value = (i32, Vec<f64>)> {
    small_k().prop_flat_map(|k| {
        let n = 30 * k as usize;
        (Just(k), prop::collection::vec(extreme_f64(), n..n + 500))
    })
}

/// `k` large enough for the rank band to say something, paired with a stream
/// thirty times longer: the budget below is a multiple of `n / k`, so a small
/// `k` would make it wider than the stream itself.
fn accurate() -> impl Strategy<Value = (i32, Vec<f64>)> {
    prop_oneof![Just(200i32), Just(400)].prop_flat_map(|k| {
        let n = 30 * k as usize;
        (Just(k), prop::collection::vec(ordinary(), n..n + 2_000))
    })
}

/// Streams that fit in level 0, including the last length before the first
/// compaction (`n == k`), which is drawn on its own so it is always covered.
fn uncompacted() -> impl Strategy<Value = (i32, Vec<f64>)> {
    small_k().prop_flat_map(|k| {
        let sizes = prop_oneof![1usize..=(k as usize), Just(k as usize)];
        (
            Just(k),
            sizes.prop_flat_map(|n| prop::collection::vec(-1.0e6f64..1.0e6, n)),
        )
    })
}

/// Quantiles to probe: both ends, a spread over the middle, and a random draw,
/// ascending.
fn probes() -> impl Strategy<Value = Vec<f64>> {
    prop::collection::vec(0.0f64..=1.0, 1..8).prop_map(|mut qs| {
        qs.extend_from_slice(&[0.0, 0.01, 0.25, 0.5, 0.75, 0.99, 1.0]);
        qs.sort_by(f64::total_cmp);
        qs
    })
}

/// Values to probe a rank or a CDF with, ascending. They land between and
/// outside the stream's values; `probe_values` adds the ones that land on it.
fn ascending_probes() -> impl Strategy<Value = Vec<f64>> {
    prop::collection::vec(-1.2e6f64..1.2e6, 1..16).prop_map(|mut xs| {
        xs.sort_by(f64::total_cmp);
        xs
    })
}

fn sketch_of(k: i32, seed: u64, values: &[f64]) -> KLLDynamic<f64> {
    let mut sketch = KLLDynamic::<f64>::init_kll_with_seed(k, seed);
    for v in values {
        sketch.update(v);
    }
    sketch
}

fn ascending(values: &[f64]) -> Vec<f64> {
    let mut v = values.to_vec();
    v.sort_by(f64::total_cmp);
    v
}

/// The probes, plus four of the stream's own values so the searches that stop
/// on a match are exercised as well as the ones that fall between.
fn probe_values(sorted: &[f64], extra: &[f64]) -> Vec<f64> {
    let mut xs = extra.to_vec();
    let last = sorted.len() - 1;
    xs.extend([sorted[0], sorted[last / 3], sorted[last / 2], sorted[last]]);
    xs.sort_by(f64::total_cmp);
    xs
}

/// `a >= b` up to the slack the CDF's own division by the total weight leaves,
/// which is orders of magnitude below the one-item granularity of a rank.
fn at_least(a: f64, b: f64) -> bool {
    a >= b || close(a, b)
}

/// How many multiples of `n / k` of rank error the band below allows. An order
/// of magnitude above what a sorted compaction leaves and an order of
/// magnitude below what an unsorted one does.
const ERROR_CONSTANT: f64 = 8.0;

proptest! {
    // ===== Wire =====

    #[test]
    fn kll_dynamic_round_trips_under_extreme_values(
        k in prop_oneof![Just(1i32), Just(8), Just(64), Just(200), Just(1_024)],
        values in extreme_values(300),
    ) {
        let mut sketch = KLLDynamic::<f64>::init_kll_with_seed(k, 0x5EED_0002);
        for v in &values {
            sketch.update(v);
        }

        round_trip!(
            KLLDynamic<f64>,
            sketch,
            |s: &KLLDynamic<f64>| s.count(),
            |s: &KLLDynamic<f64>| QUANTILES.iter().map(|q| s.quantile(*q).to_bits()).collect::<Vec<_>>(),
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // The payload carries the compaction coin's live state, not just the
    // retained items, so a decoded sketch tosses the same coin the original
    // would have tossed next. Answers read off a sketch that has stopped
    // cannot see that: the two only part company once a further update
    // compacts, and then they part for good.
    #[test]
    fn a_decoded_sketch_carries_on_where_the_original_left_off(
        (k, values) in compacting(),
        seed in any::<u64>(),
        more in prop::collection::vec(ordinary(), 200..400),
    ) {
        let mut original = sketch_of(k, seed, &values);
        let bytes = original.serialize_to_bytes().expect("encode");
        let mut decoded = KLLDynamic::<f64>::deserialize_from_bytes(&bytes).expect("decode");

        for v in &more {
            original.update(v);
            decoded.update(v);
        }

        prop_assert_eq!(decoded.count(), original.count(), "k={}: count after the replay", k);
        prop_assert_eq!(
            decoded.serialize_to_bytes().expect("encode"),
            original.serialize_to_bytes().expect("encode"),
            "k={}: a decoded sketch took a different compaction than the original", k
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // ===== Quantile =====

    // The CDF is one run sorted by `total_cmp` and `q` indexes into its prefix
    // weights, so the answer walks that order - NaN and the signed zeros
    // included, which is the order `f64`'s own `<` cannot state.
    #[test]
    fn the_quantile_never_falls_as_q_rises(
        (k, values) in extreme_compacting(),
        seed in any::<u64>(),
        qs in probes(),
    ) {
        let sketch = sketch_of(k, seed, &values);

        let mut previous: Option<f64> = None;
        for q in &qs {
            let v = sketch.quantile(*q);
            if let Some(before) = previous {
                prop_assert!(
                    before.total_cmp(&v) != Ordering::Greater,
                    "k={}, n={}, q={}: {:?} follows {:?}",
                    k, values.len(), q, v, before
                );
            }
            previous = Some(v);
        }
    }

    // Compaction only ever drops items, so every answer is one that arrived,
    // to the bit. The observed-range law follows from this one: a member of
    // the stream cannot leave the stream's own extremes.
    #[test]
    fn every_quantile_is_a_value_the_stream_carried(
        (k, values) in extreme_compacting(),
        seed in any::<u64>(),
        qs in probes(),
    ) {
        let sketch = sketch_of(k, seed, &values);
        let arrived: HashSet<u64> = values.iter().map(|v| v.to_bits()).collect();

        for q in &qs {
            let v = sketch.quantile(*q);
            prop_assert!(
                arrived.contains(&v.to_bits()),
                "k={}, n={}, q={}: {:?} never arrived",
                k, values.len(), q, v
            );
        }
    }

    // ===== Rank =====

    // `quantile(q)` returns the item at which the CDF's prefix weight first
    // reaches `q * n`, so the weight standing at that item is at least `q * n`
    // and the weight standing below it is at most `q * n`. The lower probe is
    // the stream's own predecessor of the answer, taken in the `<` order
    // `rank` counts in, which keeps the bracket exact under ties.
    #[test]
    fn the_rank_of_a_quantile_brackets_q_times_the_count(
        (k, values) in compacting(),
        seed in any::<u64>(),
        qs in probes(),
    ) {
        let sketch = sketch_of(k, seed, &values);
        let sorted = ascending(&values);
        let n = sketch.count() as f64;

        for q in &qs {
            let v = sketch.quantile(*q);
            let target = q * n;

            let upto = sketch.rank(v) as f64;
            prop_assert!(
                at_least(upto, target),
                "k={k}, q={q}: rank({v}) = {upto} is under q*n = {target}"
            );
            prop_assert!(
                upto <= n,
                "k={k}, q={q}: rank({v}) = {upto} is over the count {n}"
            );

            if let Some(under) = sorted.iter().rev().find(|x| **x < v) {
                let below = sketch.rank(*under) as f64;
                prop_assert!(
                    at_least(target, below),
                    "k={k}, q={q}: rank({under}) = {below} is over q*n = {target}"
                );
            }
        }
    }

    // Nothing is retained below the stream, everything is retained at or below
    // the largest value the sketch still holds, and the climb between the two
    // never turns back. The top probe is `quantile(1.0)` - the sketch's own
    // largest retained value - rather than the stream's largest: compaction
    // drops that one more often than not, and a probe above everything
    // retained reaches the count whether `rank` counts with `<=` or with `<`.
    #[test]
    fn rank_climbs_from_zero_to_the_count(
        (k, values) in compacting(),
        seed in any::<u64>(),
        xs in ascending_probes(),
    ) {
        let sketch = sketch_of(k, seed, &values);
        let sorted = ascending(&values);
        let n = sketch.count();

        prop_assert_eq!(
            sketch.rank(f64::NEG_INFINITY), 0,
            "k={}, n={}: rank below the stream", k, n
        );
        prop_assert_eq!(
            sketch.rank(sketch.quantile(1.0)), n,
            "k={}: rank at the largest retained value", k
        );
        prop_assert!(
            sketch.rank(sketch.quantile(0.0)) >= 1,
            "k={}: rank at the smallest retained value counts nothing", k
        );

        let mut previous = 0;
        for x in probe_values(&sorted, &xs) {
            let r = sketch.rank(x);
            prop_assert!(r >= previous, "k={k}: rank({x}) = {r} fell from {previous}");
            prop_assert!(r <= n, "k={k}: rank({x}) = {r} is over the count {n}");
            previous = r;
        }
    }

    // ===== CDF =====

    // The table is normalized by the total weight, so it starts below the
    // stream at zero, ends above it at one, and never turns back in between.
    #[test]
    fn the_cdf_is_bounded_monotone_and_ends_at_one(
        (k, values) in compacting(),
        seed in any::<u64>(),
        xs in ascending_probes(),
    ) {
        let sketch = sketch_of(k, seed, &values);
        let cdf = sketch.cdf();

        prop_assert_eq!(cdf.quantile(f64::NEG_INFINITY), 0.0, "k={}: cdf below the stream", k);
        prop_assert_eq!(cdf.quantile(f64::INFINITY), 1.0, "k={}: cdf above the stream", k);

        let mut previous = 0.0;
        for x in probe_values(&ascending(&values), &xs) {
            let c = cdf.quantile(x);
            prop_assert!((0.0..=1.0).contains(&c), "k={k}: cdf({x}) = {c}");
            prop_assert!(c >= previous, "k={k}: cdf({x}) = {c} fell from {previous}");
            previous = c;
        }
    }

    // Two readings of one state that share no code: `rank` walks the levels and
    // weights each by `2^h`, the CDF flattens every item into one sorted run and
    // takes a prefix sum. The stream repeats its values and the probes land on
    // them, so both readings answer for a whole run of copies rather than for
    // one member of it. A NaN probe stands below the stream to both.
    #[test]
    fn the_cdf_agrees_with_rank(
        (k, values) in compacting(),
        seed in any::<u64>(),
        xs in ascending_probes(),
    ) {
        let sketch = sketch_of(k, seed, &values);
        let cdf = sketch.cdf();
        let n = sketch.count() as f64;

        for x in probe_values(&ascending(&values), &xs) {
            let scaled = cdf.quantile(x) * n;
            let ranked = sketch.rank(x) as f64;
            prop_assert!(
                close(scaled, ranked),
                "k={k}: cdf({x}) * {n} = {scaled} against rank({x}) = {ranked}"
            );
        }

        prop_assert_eq!(
            cdf.quantile(f64::NAN) * n, sketch.rank(f64::NAN) as f64,
            "k={}: cdf at a NaN probe", k
        );
    }

    // ===== Rank at the extremes =====

    // The CDF sorts by `total_cmp`, but `rank` counts with `f64`'s own `<=`,
    // and the two orders disagree exactly where the extremes live: a NaN probe
    // stands below every item, a retained NaN is counted by no probe at all,
    // and the signed zeros are one value rather than two.
    #[test]
    fn rank_reads_the_extremes_in_float_order(
        (k, values) in extreme_compacting(),
        seed in any::<u64>(),
        xs in ascending_probes(),
    ) {
        let sketch = sketch_of(k, seed, &values);
        let n = sketch.count();

        prop_assert_eq!(sketch.rank(f64::NAN), 0, "k={}: rank of a NaN probe", k);
        prop_assert_eq!(
            sketch.rank(-0.0), sketch.rank(0.0),
            "k={}: the signed zeros are one value to rank", k
        );

        let mut previous = 0;
        for x in [f64::NEG_INFINITY, f64::MIN]
            .into_iter()
            .chain(xs)
            .chain([f64::MAX, f64::INFINITY])
        {
            let r = sketch.rank(x);
            prop_assert!(r >= previous, "k={k}: rank({x}) = {r} fell from {previous}");
            prop_assert!(r <= n, "k={k}: rank({x}) = {r} is over the count {n}");
            previous = r;
        }
    }

    // ===== Accuracy =====

    // Halving a *sorted* level moves one item's weight across any probe, which
    // holds the rank error to a multiple of `n / k`; halving an unsorted one
    // leaves every shape law above intact and widens the error to a multiple
    // of `n / sqrt(k)`. This is the only law that reads that difference. The
    // oracle is the sorted stream and the claim is on the answer's rank, not
    // on the answer: `quantile(q)` may sit far from the true q-th value
    // wherever the distribution is flat.
    #[test]
    fn the_true_rank_of_a_quantile_stays_inside_the_kll_band(
        (k, values) in accurate(),
        seed in any::<u64>(),
    ) {
        let sketch = sketch_of(k, seed, &values);
        let sorted = ascending(&values);
        let n = sorted.len() as f64;
        let budget = ERROR_CONSTANT / k as f64 * n;

        for i in 0..=20 {
            let q = f64::from(i) / 20.0;
            let v = sketch.quantile(q);
            let true_rank = sorted.partition_point(|x| *x <= v) as f64;
            prop_assert!(
                (true_rank - q * n).abs() <= budget,
                "k={}, n={}, q={}: {} lands at true rank {}, {} off a budget of {}",
                k, n, q, v, true_rank, (true_rank - q * n).abs(), budget
            );
        }
    }

    // ===== Merge =====

    // A fresh sketch of the same `k` has nothing of its own to interleave and
    // every level of the source is already inside the capacity that `k` sets,
    // so the cascade finds nothing to compact: the merged sketch stands for
    // the source's own weighted multiset, level for level. Rank reads that
    // multiset through the level weights, so equal ranks at every probe is
    // equal weight at every level.
    #[test]
    fn merging_into_an_empty_sketch_reproduces_the_source(
        (k, values) in compacting(),
        seed in any::<u64>(),
        xs in ascending_probes(),
    ) {
        let source = sketch_of(k, seed, &values);
        let mut target = KLLDynamic::<f64>::init_kll_with_seed(k, seed);
        target.merge(&source);

        prop_assert_eq!(target.count(), source.count(), "k={}: count after the merge", k);
        for x in probe_values(&ascending(&values), &xs) {
            prop_assert_eq!(
                target.rank(x), source.rank(x),
                "k={}: rank({}) after a merge into an empty sketch", k, x
            );
        }
    }

    // ===== Exactness =====

    // Level 0 holds the whole stream until it first spills, so up to `n == k`
    // the sketch is the stream: the count is exact, the ends are the stream's
    // own, and every answer sits at the exact rank `q` asks for. Past that the
    // ends are no longer exact - `compact` keeps every other item of a sorted
    // level and nothing protects the two extremes.
    #[test]
    fn the_answer_is_exact_until_the_first_compaction(
        (k, values) in uncompacted(),
        seed in any::<u64>(),
        qs in probes(),
    ) {
        let sketch = sketch_of(k, seed, &values);
        let sorted = ascending(&values);
        let n = values.len();

        prop_assert_eq!(sketch.count(), n, "k={}, n={}: count under the threshold", k, n);
        prop_assert_eq!(
            sketch.quantile(0.0).to_bits(), sorted[0].to_bits(),
            "k={}, n={}: quantile(0) is not the stream's smallest value", k, n
        );
        prop_assert_eq!(
            sketch.quantile(1.0).to_bits(), sorted[n - 1].to_bits(),
            "k={}, n={}: quantile(1) is not the stream's largest value", k, n
        );

        for q in &qs {
            let v = sketch.quantile(*q);
            let target = q * n as f64;
            let below = sorted.iter().filter(|x| **x < v).count() as f64;
            let upto = sorted.iter().filter(|x| **x <= v).count() as f64;
            prop_assert!(
                at_least(target, below) && at_least(upto, target),
                "k={k}, n={n}, q={q}: {v} sits at ranks {below}..{upto}, not at q*n = {target}"
            );
        }
    }

    // ===== Lifecycle =====

    // The coin is the only thing between a stream and the retained set, and a
    // seeded coin replays. Equal bytes is equal items, equal level boundaries
    // and equal coin state.
    #[test]
    fn the_same_seed_and_stream_build_the_same_sketch(
        (k, values) in compacting(),
        seed in any::<u64>(),
    ) {
        let first = sketch_of(k, seed, &values);
        let second = sketch_of(k, seed, &values);

        prop_assert_eq!(first.count(), second.count(), "k={}, seed={}", k, seed);
        prop_assert_eq!(
            first.serialize_to_bytes().expect("encode"),
            second.serialize_to_bytes().expect("encode"),
            "k={}, seed={}: same seed and stream, different state", k, seed
        );
    }

    // `clear` re-seeds from the construction seed, so a cleared sketch is a new
    // one both at rest and over the replay of the same stream.
    #[test]
    fn clearing_returns_the_sketch_to_a_new_one(
        (k, values) in compacting(),
        seed in any::<u64>(),
    ) {
        let mut reused = sketch_of(k, seed, &values);
        reused.clear();

        let empty = KLLDynamic::<f64>::init_kll_with_seed(k, seed);
        prop_assert_eq!(reused.count(), 0, "k={}, seed={}: cleared count", k, seed);
        prop_assert_eq!(
            reused.serialize_to_bytes().expect("encode"),
            empty.serialize_to_bytes().expect("encode"),
            "k={}, seed={}: a cleared sketch is not an empty one", k, seed
        );

        for v in &values {
            reused.update(v);
        }
        let fresh = sketch_of(k, seed, &values);
        prop_assert_eq!(
            reused.serialize_to_bytes().expect("encode"),
            fresh.serialize_to_bytes().expect("encode"),
            "k={}, seed={}: a cleared sketch does not replay as a new one", k, seed
        );
    }
}
