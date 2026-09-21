//! Property tests for DDSketch: the native sketch and the portable `DdSketch`
//! that carries it over the wire.
//!
//! Masson, Rim, Lee, VLDB '19. The model is the paper's relative-error
//! guarantee, not a transcription of the bucket arithmetic: an answer sits
//! within a relative alpha of the exact order statistic of the stream.

use crate::support::{QUANTILES, extreme_values};
use asap_sketchlib::message_pack_format::MessagePackCodec;
use asap_sketchlib::{DDSketch, DdSketch};
use proptest::prelude::*;

const QUANTILE_PROBES: [f64; 7] = [0.0, 0.01, 0.25, 0.5, 0.75, 0.99, 1.0];

/// First dense allocation, in buckets, and the slot the seeding value lands in.
/// The Go producer's `Buckets.ensure` seeds a chunk this wide centred on the
/// first index, and the `store_counts` / `store_offset` bytes are compared
/// across the two languages.
const GROW_CHUNK: usize = 128;
const GROW_CHUNK_CENTRE: usize = GROW_CHUNK / 2;

fn alpha() -> impl Strategy<Value = f64> {
    prop_oneof![
        Just(0.001f64),
        Just(0.005),
        Just(0.01),
        Just(0.05),
        Just(0.1),
        Just(0.2),
    ]
}

/// A stream that always carries all three branches — negatives, exact zeros,
/// positives — with magnitudes inside the indexable band, rotated so the two
/// dense stores grow in varying order.
fn signed_values(max: usize) -> impl Strategy<Value = Vec<f64>> {
    (
        prop::collection::vec(1e-6f64..1e9, 1..max),
        prop::collection::vec(1e-6f64..1e9, 1..max),
        1usize..6,
        0usize..64,
    )
        .prop_map(|(negatives, positives, zeros, rotation)| {
            let mut out: Vec<f64> = (0..zeros)
                .map(|i| if i % 2 == 0 { 0.0 } else { -0.0 })
                .collect();
            let mut negatives = negatives.into_iter();
            let mut positives = positives.into_iter();
            loop {
                let (n, p) = (negatives.next(), positives.next());
                if n.is_none() && p.is_none() {
                    break;
                }
                out.extend(n.map(|v| -v));
                out.extend(p);
            }
            let len = out.len();
            out.rotate_left(rotation % len);
            out
        })
}

/// A signed stream or no stream at all: merge and the wire format both answer
/// for an empty operand.
fn maybe_signed_values(max: usize) -> impl Strategy<Value = Vec<f64>> {
    prop_oneof![1 => Just(Vec::new()), 9 => signed_values(max)]
}

fn dd_of(alpha: f64, values: &[f64]) -> DDSketch {
    let mut s = DDSketch::new(alpha);
    for v in values {
        s.add(v);
    }
    s
}

fn portable_of(alpha: f64, values: &[f64]) -> DdSketch {
    let mut s = DdSketch::new(alpha);
    for v in values {
        s.update(*v);
    }
    s
}

fn sorted(values: &[f64]) -> Vec<f64> {
    let mut v = values.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).expect("the generator excludes NaN"));
    v
}

/// Rank of the q-quantile in the paper's convention, which is the one
/// `DDSketch::get_value_at_quantile` answers: the ceil(q*n)-th smallest.
fn ceil_rank(q: f64, n: usize) -> usize {
    ((q * n as f64).ceil() as usize).max(1)
}

fn probe_quantiles(steps: u32) -> impl Iterator<Item = f64> {
    (0..=steps).map(move |i| f64::from(i) / f64::from(steps))
}

proptest! {
    // ===== What the stream leaves behind =====

    #[test]
    fn ddsketch_sends_each_sign_to_its_own_store(
        alpha in alpha(),
        values in signed_values(120),
    ) {
        let sketch = dd_of(alpha, &values);
        let negatives = values.iter().filter(|v| **v < 0.0).count() as u64;
        let zeros = values.iter().filter(|v| **v == 0.0).count() as u64;
        let positives = values.iter().filter(|v| **v > 0.0).count() as u64;

        prop_assert_eq!(
            sketch.negative_store_counts().iter().sum::<u64>(), negatives,
            "negative store holds the wrong number of the {} negatives", negatives
        );
        prop_assert_eq!(sketch.zero_count(), zeros, "zero bucket");
        prop_assert_eq!(
            sketch.store_counts().iter().sum::<u64>(), positives,
            "positive store holds the wrong number of the {} positives", positives
        );
        prop_assert_eq!(sketch.get_count(), values.len() as u64, "total count");
    }

    #[test]
    fn ddsketch_portable_keeps_negative_and_zero_observations(
        alpha in alpha(),
        values in signed_values(120),
    ) {
        let sketch = portable_of(alpha, &values);
        let negatives = values.iter().filter(|v| **v < 0.0).count() as u64;
        let zeros = values.iter().filter(|v| **v == 0.0).count() as u64;
        let positives = values.iter().filter(|v| **v > 0.0).count() as u64;

        prop_assert_eq!(
            sketch.negative_store_counts.iter().sum::<u64>(), negatives, "negative store"
        );
        prop_assert_eq!(sketch.zero_count, zeros, "zero bucket");
        prop_assert_eq!(sketch.store_counts.iter().sum::<u64>(), positives, "positive store");
        prop_assert_eq!(sketch.total_count(), values.len() as u64, "total count");
    }

    /// `sum` is carried on the wire and read back through `sum()`, so it is
    /// the running total of the stream, not an estimate off the buckets.
    #[test]
    fn ddsketch_sum_tracks_the_stream(
        alpha in alpha(),
        values in signed_values(120),
    ) {
        let sketch = dd_of(alpha, &values);
        let want = values.iter().fold(0.0f64, |acc, v| acc + v);

        prop_assert_eq!(
            sketch.sum().to_bits(), want.to_bits(),
            "alpha={}: sum {} over {} values, running total {}",
            alpha, sketch.sum(), values.len(), want
        );
    }

    /// The first dense allocation is one `GROW_CHUNK` centred on the value's
    /// bucket, in both implementations and in both signed stores. The layout
    /// reaches the wire as `store_counts` / `store_offset`, where the Go
    /// producer's bytes have to match it for the same input.
    #[test]
    fn ddsketch_seeds_its_dense_store_as_one_centered_grow_chunk(
        alpha in alpha(),
        value in 1e-6f64..1e9,
    ) {
        let native = dd_of(alpha, &[value, -value]);
        let portable = portable_of(alpha, &[value, -value]);

        for (label, counts) in [
            ("native positive", native.store_counts()),
            ("native negative", native.negative_store_counts()),
            ("portable positive", portable.store_counts.as_slice()),
            ("portable negative", portable.negative_store_counts.as_slice()),
        ] {
            prop_assert_eq!(counts.len(), GROW_CHUNK, "{} store width", label);
            prop_assert_eq!(counts[GROW_CHUNK_CENTRE], 1, "{} store centre", label);
            prop_assert_eq!(
                counts.iter().sum::<u64>(), 1,
                "{} store put the value somewhere other than the centre", label
            );
        }
    }

    // ===== The paper's guarantee =====

    /// Sharper than the paper's bound: the sketch tracks the extremes outside
    /// the buckets and clamps a bucket answer to them, so a lone value comes
    /// back unrounded at every quantile.
    #[test]
    fn ddsketch_returns_a_lone_value_exactly(
        alpha in alpha(),
        value in 1e-6f64..1e9,
        q in 0.0f64..=1.0,
    ) {
        let sketch = dd_of(alpha, &[value]);
        let answer = sketch.get_value_at_quantile(q).expect("a non-empty sketch answers");

        prop_assert_eq!(answer.to_bits(), value.to_bits());
    }

    /// Also sharper than the bound: q=0 and q=1 read the tracked extremes
    /// rather than a bucket, so they are exact and say nothing about alpha.
    #[test]
    fn ddsketch_returns_the_signed_extremes_exactly(
        alpha in alpha(),
        values in signed_values(120),
    ) {
        let sketch = dd_of(alpha, &values);
        let ordered = sorted(&values);
        let last = ordered.len() - 1;

        prop_assert_eq!(sketch.min(), Some(ordered[0]), "min over {} values", ordered.len());
        prop_assert_eq!(sketch.max(), Some(ordered[last]), "max over {} values", ordered.len());
        prop_assert_eq!(sketch.get_value_at_quantile(0.0), Some(ordered[0]), "q=0");
        prop_assert_eq!(sketch.get_value_at_quantile(1.0), Some(ordered[last]), "q=1");
    }

    /// The paper's guarantee across the sign boundary, against the exact
    /// order statistic rather than merely some value of the stream.
    #[test]
    fn ddsketch_signed_answers_stay_within_alpha_of_the_exact_order_statistic(
        alpha in alpha(),
        values in signed_values(50),
    ) {
        let sketch = dd_of(alpha, &values);
        let ordered = sorted(&values);

        for q in probe_quantiles(40) {
            let want = ordered[ceil_rank(q, ordered.len()) - 1];
            let got = sketch.get_value_at_quantile(q).expect("a non-empty sketch answers");
            prop_assert!(
                (got - want).abs() <= want.abs() * (alpha + 1e-12),
                "alpha={} q={} n={}: got {}, exact order statistic {}",
                alpha, q, ordered.len(), got, want
            );
        }
    }

    #[test]
    fn ddsketch_answers_stay_ordered_across_the_sign_boundary(
        alpha in alpha(),
        values in signed_values(120),
    ) {
        let sketch = dd_of(alpha, &values);
        let answers: Vec<f64> = probe_quantiles(20)
            .map(|q| sketch.get_value_at_quantile(q).expect("a non-empty sketch answers"))
            .collect();

        for (i, pair) in answers.windows(2).enumerate() {
            prop_assert!(
                pair[0] <= pair[1],
                "alpha={}: q={} answered {} but q={} answered {}",
                alpha, i as f64 / 20.0, pair[0], (i + 1) as f64 / 20.0, pair[1]
            );
        }
    }

    /// The portable twin answers the floor(q*(n-1)) order statistic, and it
    /// carries no min/max, so the bound applies at every q including 0 and 1.
    #[test]
    fn ddsketch_portable_signed_answers_stay_within_alpha_of_the_exact_order_statistic(
        alpha in alpha(),
        values in signed_values(50),
    ) {
        let sketch = portable_of(alpha, &values);
        let ordered = sorted(&values);

        for q in probe_quantiles(40) {
            let want = ordered[(q * (ordered.len() - 1) as f64).floor() as usize];
            let got = sketch.quantile(q).expect("a non-empty sketch answers");
            prop_assert!(
                (got - want).abs() <= want.abs() * (alpha + 1e-12),
                "alpha={} q={} n={}: got {}, exact order statistic {}",
                alpha, q, ordered.len(), got, want
            );
        }
    }

    // ===== Merge =====

    #[test]
    fn ddsketch_merge_is_count_additive_and_commutative(
        alpha in alpha(),
        a in maybe_signed_values(40),
        b in maybe_signed_values(40),
    ) {
        let mut ab = portable_of(alpha, &a);
        ab.merge(&portable_of(alpha, &b)).expect("merge");
        let mut ba = portable_of(alpha, &b);
        ba.merge(&portable_of(alpha, &a)).expect("merge");

        prop_assert_eq!(ab.total_count(), (a.len() + b.len()) as u64);
        prop_assert_eq!(ab.total_count(), ba.total_count());
        prop_assert_eq!(&ab.store_counts, &ba.store_counts);
        prop_assert_eq!(ab.store_offset, ba.store_offset);
        prop_assert_eq!(&ab.negative_store_counts, &ba.negative_store_counts);
        prop_assert_eq!(ab.negative_store_offset, ba.negative_store_offset);
        prop_assert_eq!(ab.zero_count, ba.zero_count);
        prop_assert_eq!(
            ab.zero_count,
            a.iter().chain(&b).filter(|v| **v == 0.0).count() as u64,
            "the merged zero bucket"
        );
        prop_assert_eq!(
            ab.negative_store_counts.iter().sum::<u64>(),
            a.iter().chain(&b).filter(|v| **v < 0.0).count() as u64,
            "the merged negative store"
        );
    }

    /// Merging keeps the paper's guarantee against the combined stream: the
    /// merged answer is still within alpha of the union's order statistic.
    #[test]
    fn ddsketch_merge_answers_within_alpha_of_the_combined_order_statistic(
        alpha in alpha(),
        left in signed_values(30),
        right in signed_values(30),
    ) {
        let mut merged = dd_of(alpha, &left);
        merged.merge(&dd_of(alpha, &right)).expect("a shared alpha merges");

        let mut all = left.clone();
        all.extend_from_slice(&right);
        let ordered = sorted(&all);

        prop_assert_eq!(merged.get_count(), ordered.len() as u64, "merged count");
        for q in probe_quantiles(40) {
            let want = ordered[ceil_rank(q, ordered.len()) - 1];
            let got = merged.get_value_at_quantile(q).expect("a non-empty sketch answers");
            prop_assert!(
                (got - want).abs() <= want.abs() * (alpha + 1e-12),
                "alpha={} q={} n={}: merged answered {}, exact order statistic {}",
                alpha, q, ordered.len(), got, want
            );
        }
    }

    /// Two alphas are two index mappings, so the bucket indices of one sketch
    /// mean nothing under the other's gamma. Both implementations refuse the
    /// merge and leave the receiver as it was.
    #[test]
    fn ddsketch_merge_refuses_a_different_index_mapping(
        alpha in alpha(),
        other_alpha in alpha(),
        values in signed_values(20),
    ) {
        prop_assume!((alpha - other_alpha).abs() > 1e-9);

        let mut native = dd_of(alpha, &values);
        prop_assert!(
            native.merge(&dd_of(other_alpha, &values)).is_err(),
            "alpha {} merged a sketch built with alpha {}", alpha, other_alpha
        );
        prop_assert_eq!(native.get_count(), values.len() as u64, "refused merge changed the count");

        let mut portable = portable_of(alpha, &values);
        prop_assert!(
            portable.merge(&portable_of(other_alpha, &values)).is_err(),
            "portable alpha {} merged a sketch built with alpha {}", alpha, other_alpha
        );
        prop_assert_eq!(portable.total_count(), values.len() as u64, "refused merge changed the count");

        prop_assert!(
            dd_of(alpha, &values).merge(&dd_of(alpha, &values)).is_ok(),
            "a shared mapping must still merge"
        );
    }

    // ===== Wire =====

    #[test]
    fn ddsketch_round_trip_preserves_buckets_and_quantiles(
        alpha in alpha(),
        values in maybe_signed_values(120),
    ) {
        let s = portable_of(alpha, &values);

        let bytes = s.to_msgpack().expect("encode");
        let restored = DdSketch::from_msgpack(&bytes).expect("decode");

        prop_assert_eq!(restored.total_count(), s.total_count());
        prop_assert_eq!(&restored.store_counts, &s.store_counts);
        prop_assert_eq!(restored.store_offset, s.store_offset);
        prop_assert_eq!(&restored.negative_store_counts, &s.negative_store_counts);
        prop_assert_eq!(restored.negative_store_offset, s.negative_store_offset);
        prop_assert_eq!(restored.zero_count, s.zero_count);
        for q in QUANTILE_PROBES {
            prop_assert_eq!(restored.quantile(q), s.quantile(q), "q={}", q);
        }
    }

    #[test]
    fn ddsketch_round_trips_under_extreme_values(
        alpha in prop_oneof![
            Just(0.001f64), Just(0.01), Just(0.1), Just(0.5), Just(0.9), Just(0.999)
        ],
        values in extreme_values(200),
    ) {
        let mut sketch = DDSketch::new(alpha);
        for v in &values {
            sketch.add(v);
        }

        round_trip!(
            DDSketch,
            sketch,
            |s: &DDSketch| s.get_count(),
            |s: &DDSketch| s.alpha().to_bits(),
            |s: &DDSketch| s.sum().to_bits(),
            |s: &DDSketch| QUANTILES.iter()
                .map(|q| s.get_value_at_quantile(*q).map(f64::to_bits))
                .collect::<Vec<_>>(),
        );
    }
}
