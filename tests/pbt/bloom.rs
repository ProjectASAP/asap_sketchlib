//! Property tests for the Bloom filter.
//!
//! Bloom, CACM '70. The model is the paper's own: a membership test may lie
//! upward and never downward, and a bit that is set stays set. Both are exact
//! statements, so a counterexample is a defect rather than an unlucky draw.

use crate::support::{bloom_rows, edge_dimension, grid, keys, wide_keys};
use asap_sketchlib::{
    BLOOM_MAX_BITS, BLOOM_MAX_SLICES, BitMatrix, Bloom, DataInput, FastPath, RegularPath,
};
use proptest::prelude::*;
use std::collections::HashSet;

fn matrix_bits(m: &BitMatrix) -> Vec<bool> {
    grid(m.rows(), m.cols(), |r, c| m.get(r, c))
}

fn bloom_bits(f: &Bloom) -> Vec<bool> {
    matrix_bits(f.as_bits())
}

fn bloom_of(rows: usize, cols: usize, values: &[u64]) -> Bloom {
    let mut f: Bloom = Bloom::with_dimensions(rows, cols);
    for v in values {
        f.insert(&DataInput::U64(*v));
    }
    f
}

fn stream_and_permutation(max: usize) -> impl Strategy<Value = (Vec<u64>, Vec<u64>)> {
    prop::collection::vec(0u64..512, 0..max)
        .prop_flat_map(|v| (Just(v.clone()), Just(v).prop_shuffle()))
}

/// Grids narrow enough that a few hundred keys set every bit.
fn saturating_geometry() -> impl Strategy<Value = (usize, usize)> {
    (1usize..5, 1usize..9)
}

/// The saturating grids and grids that stay mostly zero under the same stream.
fn mixed_geometry() -> impl Strategy<Value = (usize, usize)> {
    prop_oneof![(1usize..5, 1usize..9), (1usize..8, 256usize..1024)]
}

fn distinct(stream: &[u64]) -> usize {
    stream.iter().collect::<HashSet<_>>().len()
}

/// Insert `stream` into a fresh filter of the given mode, then hold that mode
/// to the paper's guarantee and to the partitioning: one bit per slice per key.
/// The two modes reach their bits through different code, so each is checked
/// through its own inherent `insert` and `contains`.
macro_rules! mode_laws {
    ($ty:ty, $path:expr, $rows:expr, $cols:expr, $stream:expr) => {{
        let mut filter = <$ty>::with_dimensions($rows, $cols);
        for v in $stream {
            filter.insert(&DataInput::U64(*v));
        }

        for v in $stream {
            prop_assert!(
                filter.contains(&DataInput::U64(*v)),
                "{} path lost member {} in a {}x{} filter",
                $path,
                v,
                $rows,
                $cols
            );
        }

        let set = filter.as_bits().count_ones();
        prop_assert!(
            set <= $rows * distinct($stream),
            "{} path set {} bits for {} distinct keys over {} slices",
            $path,
            set,
            distinct($stream),
            $rows
        );
        prop_assert!(
            set >= if $stream.is_empty() { 0 } else { $rows },
            "{} path set {} bits for a stream of {} keys over {} slices",
            $path,
            set,
            $stream.len(),
            $rows
        );
        prop_assert!(
            set <= filter.bit_capacity() && filter.bit_capacity() <= BLOOM_MAX_BITS,
            "{} path: {} bits set in a {}-bit grid, ceiling {}",
            $path,
            set,
            filter.bit_capacity(),
            BLOOM_MAX_BITS
        );
        prop_assert_eq!(
            filter.fill_ratio(),
            set as f64 / filter.bit_capacity() as f64,
            "{} path fill ratio is not the bits set over the addressable grid",
            $path
        );
        prop_assert_eq!(
            filter.inserted(),
            $stream.len() as u64,
            "{} path counted {} of {} inserts",
            $path,
            filter.inserted(),
            $stream.len()
        );
    }};
}

proptest! {
    // ===== The paper's guarantee =====

    #[test]
    fn bloom_never_reports_a_false_negative(
        rows in 1usize..8,
        cols in 8usize..256,
        stream in wide_keys(200),
    ) {
        let filter = bloom_of(rows, cols, &stream);

        for k in &stream {
            prop_assert!(filter.contains(&DataInput::U64(*k)), "key {} was inserted", k);
        }
    }

    #[test]
    fn bloom_membership_only_ever_turns_on(
        rows in 1usize..8,
        cols in 8usize..256,
        stream in wide_keys(150),
        probes in wide_keys(40),
    ) {
        let mut filter: Bloom = Bloom::with_dimensions(rows, cols);
        let mut seen: Vec<bool> = probes
            .iter()
            .map(|p| filter.contains(&DataInput::U64(*p)))
            .collect();

        for k in &stream {
            filter.insert(&DataInput::U64(*k));
            for (i, p) in probes.iter().enumerate() {
                let now = filter.contains(&DataInput::U64(*p));
                prop_assert!(now || !seen[i], "probe {} stopped being a member", p);
                seen[i] = now;
            }
        }
    }

    // ===== Stream order must not reach the bits =====

    #[test]
    fn bloom_is_insensitive_to_stream_order(
        rows in 1usize..6,
        cols in 8usize..128,
        (a, b) in stream_and_permutation(200),
    ) {
        prop_assert_eq!(bloom_bits(&bloom_of(rows, cols, &a)), bloom_bits(&bloom_of(rows, cols, &b)));
    }

    // ===== Merge =====

    #[test]
    fn bloom_has_no_false_negatives_before_or_after_merge(
        rows in 1usize..6,
        cols in 64usize..1024,
        a in wide_keys(40),
        b in wide_keys(40),
    ) {
        let mut fa = bloom_of(rows, cols, &a);
        for v in &a {
            prop_assert!(fa.contains(&DataInput::U64(*v)), "false negative on {}", v);
        }

        fa.merge(&bloom_of(rows, cols, &b));
        for v in a.iter().chain(b.iter()) {
            prop_assert!(
                fa.contains(&DataInput::U64(*v)),
                "false negative after merge on {}", v
            );
        }
    }

    #[test]
    fn bloom_merge_is_commutative_and_idempotent(
        rows in 1usize..6,
        cols in 64usize..1024,
        a in wide_keys(40),
        b in wide_keys(40),
    ) {
        let mut ab = bloom_of(rows, cols, &a);
        ab.merge(&bloom_of(rows, cols, &b));
        let mut ba = bloom_of(rows, cols, &b);
        ba.merge(&bloom_of(rows, cols, &a));

        prop_assert_eq!(bloom_bits(&ab), bloom_bits(&ba));

        let before = bloom_bits(&ab);
        let again = ab.clone();
        ab.merge(&again);
        prop_assert_eq!(bloom_bits(&ab), before);
    }

    // ===== Wire =====

    #[test]
    fn bloom_round_trips_or_refuses_the_geometry(
        rows in bloom_rows(),
        cols in edge_dimension(),
        stream in keys(200),
    ) {
        let sketch = bloom_of(rows, cols, &stream);

        round_trip!(Bloom, sketch, bloom_bits);
    }

    // ===== The degenerate end of the rate =====

    #[test]
    fn bloom_answers_yes_to_every_query_once_every_bit_is_set(
        (rows, cols) in saturating_geometry(),
        probes in wide_keys(40),
    ) {
        let mut filter: Bloom = Bloom::with_dimensions(rows, cols);
        let mut inserts = 0u64;
        while !bloom_bits(&filter).iter().all(|b| *b) && inserts < 4_096 {
            filter.insert(&DataInput::U64(inserts));
            inserts += 1;
        }
        prop_assert!(
            bloom_bits(&filter).iter().all(|b| *b),
            "a {}x{} filter still had a clear bit after {} inserts",
            rows, cols, inserts
        );

        for p in &probes {
            prop_assert!(
                filter.contains(&DataInput::U64(*p)),
                "probe {} missed in a saturated {}x{} filter",
                p, rows, cols
            );
        }
        prop_assert_eq!(filter.fill_ratio(), 1.0, "a saturated filter is not full");
        prop_assert_eq!(
            filter.estimated_fpp(),
            1.0,
            "a saturated {}x{} filter reported a rate below certainty",
            rows, cols
        );
    }

    // ===== Both hash paths =====

    #[test]
    fn both_hash_paths_lose_no_member_and_set_one_bit_per_slice_per_key(
        (rows, cols) in mixed_geometry(),
        stream in keys(400),
    ) {
        mode_laws!(Bloom<RegularPath>, "regular", rows, cols, &stream);
        mode_laws!(Bloom<FastPath>, "fast", rows, cols, &stream);
    }

    // ===== Clearing =====

    #[test]
    fn clearing_a_bloom_filter_returns_it_to_the_state_it_was_built_in(
        (rows, cols) in mixed_geometry(),
        stream in keys(300),
    ) {
        let mut filter = bloom_of(rows, cols, &stream);
        filter.clear();

        prop_assert_eq!(
            bloom_bits(&filter),
            bloom_bits(&Bloom::with_dimensions(rows, cols)),
            "clear left a {}x{} filter unlike a fresh one", rows, cols
        );
        prop_assert_eq!(filter.inserted(), 0, "clear left the insert count standing");
        prop_assert!(filter.is_empty(), "a cleared filter reports bits set");
        prop_assert_eq!(filter.fill_ratio(), 0.0, "a cleared filter reports a fill");
        for v in &stream {
            prop_assert!(
                !filter.contains(&DataInput::U64(*v)),
                "member {} survived clear in a {}x{} filter", v, rows, cols
            );
        }
    }

    // ===== Merge against the stream it stands for =====

    #[test]
    fn bloom_merge_builds_what_the_concatenated_streams_build(
        (rows, cols) in mixed_geometry(),
        a in keys(150),
        b in keys(150),
    ) {
        let mut merged = bloom_of(rows, cols, &a);
        merged.merge(&bloom_of(rows, cols, &b));

        let concatenated: Vec<u64> = a.iter().chain(b.iter()).copied().collect();
        prop_assert_eq!(
            bloom_bits(&merged),
            bloom_bits(&bloom_of(rows, cols, &concatenated)),
            "merging {} and {} keys into a {}x{} filter is not the concatenated stream",
            a.len(), b.len(), rows, cols
        );
        prop_assert_eq!(
            merged.inserted(),
            concatenated.len() as u64,
            "the merged count is {} over {} inserts",
            merged.inserted(), concatenated.len()
        );
    }

    // ===== Where the ceilings bind =====

    #[test]
    fn sizing_stays_inside_the_slice_and_bit_ceilings_for_any_finite_target(
        expected in prop_oneof![
            Just(0usize),
            Just(1),
            1usize..10_000_000,
            Just(usize::MAX / 2),
            Just(usize::MAX),
        ],
        target in prop_oneof![
            0.0f64..1.0,
            Just(0.0),
            Just(-0.0),
            Just(1.0),
            Just(-1.0),
            Just(f64::MIN_POSITIVE),
            Just(f64::EPSILON),
            Just(f64::MAX),
            Just(f64::MIN),
        ],
    ) {
        let (rows, cols) = Bloom::<RegularPath>::dimensions_for(expected, target);

        prop_assert!(
            (1..=BLOOM_MAX_SLICES).contains(&rows),
            "sizing for {} items at {} chose {} slices",
            expected, target, rows
        );
        prop_assert!(cols >= 1 && cols.is_power_of_two(), "slice width {} is not a power of two", cols);
        prop_assert!(
            rows.checked_mul(cols).is_some_and(|bits| bits <= BLOOM_MAX_BITS),
            "sizing for {} items at {} wants {}x{}, past the {}-bit ceiling",
            expected, target, rows, cols, BLOOM_MAX_BITS
        );
    }
}
