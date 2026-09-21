//! Property tests for Nitro, the sampling layer that wraps another sketch.
//!
//! At rate 1.0 the layer must be transparent; below it the relation between
//! the admitted weight and the compensation factor is fixed by the rate alone.
//!
//! The oracles are a bare target sketch driven through its own `insert` — a
//! different code path from `NitroTarget::update_sample` — and integer
//! arithmetic done in the test. A single insert below rate 1.0 has no
//! deterministic oracle at all, so nothing here asserts one: the laws stated
//! below rate 1.0 are the ones sampling does not disturb.

use crate::support::grid;
use asap_sketchlib::{Count, CountMin, DataInput, FastPath, NitroBatch, Vector2D};
use proptest::prelude::*;

type CmTarget = CountMin<Vector2D<i32>, FastPath>;
type CsTarget = Count<Vector2D<i32>, FastPath>;

fn cm_cells(sketch: &CmTarget) -> Vec<i32> {
    grid(sketch.rows(), sketch.cols(), |r, c| {
        sketch.as_storage().query_one_counter(r, c)
    })
}

fn cs_cells(sketch: &CsTarget) -> Vec<i32> {
    grid(sketch.rows(), sketch.cols(), |r, c| {
        sketch.as_storage().query_one_counter(r, c)
    })
}

fn bare_cells(sketch: &Vector2D<u32>) -> Vec<u32> {
    grid(sketch.rows(), sketch.cols(), |r, c| {
        sketch.query_one_counter(r, c)
    })
}

/// A key domain narrow enough that collisions happen by construction.
fn stream(max: usize) -> impl Strategy<Value = Vec<i64>> {
    prop::collection::vec(0i64..512, 0..max)
}

/// Long enough that a rate at or below 1/2 drops part of it with certainty
/// past any probability worth naming.
fn long_stream() -> impl Strategy<Value = Vec<i64>> {
    prop::collection::vec(0i64..512, 256..512)
}

/// Full sampling, the dyadic rates whose reciprocal is exact in `f64`, and
/// rates low enough that most arrivals never reach the target.
fn sampling_rate() -> impl Strategy<Value = f64> {
    prop_oneof![
        Just(1.0f64),
        Just(0.9),
        Just(0.7),
        Just(0.5),
        Just(0.3),
        Just(0.25),
        Just(0.125),
        Just(0.0625),
        Just(0.03125),
        Just(0.01),
    ]
}

/// Rates whose reciprocal is not an integer, so every admitted update pays a
/// stochastic rounding draw.
fn fractional_rate() -> impl Strategy<Value = f64> {
    prop_oneof![
        Just(0.9f64),
        Just(0.7),
        Just(0.6),
        Just(0.35),
        Just(0.3),
        Just(0.15),
    ]
}

proptest! {
    // ===== Rate 1.0 is transparent =====
    //
    // The oracle is the bare target's own `insert`, which derives its cells
    // through `fast_insert` rather than through `NitroTarget::update_sample`.

    #[test]
    fn nitro_at_rate_one_fills_the_cells_a_bare_count_min_fills(
        rows in 1usize..6,
        cols in 1usize..64,
        data in stream(300),
    ) {
        let mut live = NitroBatch::with_target_and_seed(
            1.0, CmTarget::with_dimensions(rows, cols), 0xA5);
        live.insert(&data);
        let mut cached = NitroBatch::with_target_and_seed(
            1.0, CmTarget::with_dimensions(rows, cols), 0xA5);
        cached.insert_cached_step(&data);

        let mut bare = CmTarget::with_dimensions(rows, cols);
        for value in &data {
            bare.insert(&DataInput::I64(*value));
        }

        let expected = cm_cells(&bare);
        prop_assert_eq!(
            cm_cells(live.target()), expected.clone(),
            "live path, {}x{}, {} arrivals", rows, cols, data.len()
        );
        prop_assert_eq!(
            cm_cells(cached.target()), expected,
            "cached path, {}x{}, {} arrivals", rows, cols, data.len()
        );
    }

    #[test]
    fn nitro_at_rate_one_fills_the_cells_a_bare_count_sketch_fills(
        rows in 1usize..6,
        cols in 1usize..64,
        data in stream(300),
    ) {
        let mut live = NitroBatch::with_target_and_seed(
            1.0, CsTarget::with_dimensions(rows, cols), 0xA5);
        live.insert(&data);
        let mut cached = NitroBatch::with_target_and_seed(
            1.0, CsTarget::with_dimensions(rows, cols), 0xA5);
        cached.insert_cached_step(&data);

        let mut bare = CsTarget::with_dimensions(rows, cols);
        for value in &data {
            bare.insert(&DataInput::I64(*value));
        }

        let expected = cs_cells(&bare);
        prop_assert_eq!(
            cs_cells(live.target()), expected.clone(),
            "live path, {}x{}, {} arrivals", rows, cols, data.len()
        );
        prop_assert_eq!(
            cs_cells(cached.target()), expected,
            "cached path, {}x{}, {} arrivals", rows, cols, data.len()
        );
    }

    // ===== A seed fixes the admitted subset =====

    #[test]
    fn nitro_with_the_same_seed_and_stream_fills_the_same_cells(
        rate in sampling_rate(),
        seed in any::<u64>(),
        data in stream(400),
    ) {
        let mut left = NitroBatch::init_nitro_with_seed(rate, seed);
        let mut right = NitroBatch::init_nitro_with_seed(rate, seed);
        left.insert(&data);
        right.insert(&data);
        prop_assert_eq!(
            bare_cells(left.target()), bare_cells(right.target()),
            "live path, rate {}, seed {}, {} arrivals", rate, seed, data.len()
        );

        let mut left = NitroBatch::init_nitro_with_seed(rate, seed);
        let mut right = NitroBatch::init_nitro_with_seed(rate, seed);
        left.insert_cached_step(&data);
        right.insert_cached_step(&data);
        prop_assert_eq!(
            bare_cells(left.target()), bare_cells(right.target()),
            "cached path, rate {}, seed {}, {} arrivals", rate, seed, data.len()
        );
    }

    // ===== The compensation is a function of the rate =====

    #[test]
    fn nitro_compensates_within_one_unit_of_the_floor_whatever_the_stream_was(
        rate in sampling_rate(),
        seed in any::<u64>(),
        data in stream(300),
        weights in prop::collection::vec(1u64..64, 1..8),
    ) {
        let mut nitro = NitroBatch::with_target_and_seed(
            rate, CmTarget::with_dimensions(2, 16), seed);
        nitro.insert(&data);

        for weight in &weights {
            let floor = nitro.scaled_increment(*weight);
            for draw in 0..16 {
                let admitted = nitro.admitted_weight(*weight);
                prop_assert!(
                    admitted >= floor && admitted <= floor + 1,
                    "rate {}: weight {} compensated to {} on draw {}, outside [{}, {}]",
                    rate, weight, admitted, draw, floor, floor + 1
                );
            }
            prop_assert_eq!(
                nitro.scaled_increment(*weight), floor,
                "rate {}: weight {} changed factor while the stream ran", rate, weight
            );
        }
    }

    #[test]
    fn nitro_at_a_dyadic_rate_compensates_by_the_exact_reciprocal(
        shift in 0u32..12,
        seed in any::<u64>(),
        weights in prop::collection::vec(1u64..64, 1..8),
    ) {
        let reciprocal = 1u64 << shift;
        let mut nitro = NitroBatch::with_target_and_seed(
            1.0 / reciprocal as f64, CmTarget::with_dimensions(2, 16), seed);

        for weight in &weights {
            let exact = weight << shift;
            prop_assert_eq!(
                nitro.scaled_increment(*weight), exact,
                "rate 1/{}: weight {} floored to {}, not {}",
                reciprocal, weight, nitro.scaled_increment(*weight), exact
            );
            for draw in 0..8 {
                prop_assert_eq!(
                    nitro.admitted_weight(*weight), exact,
                    "rate 1/{}: weight {} admitted at {} on draw {}, not {}",
                    reciprocal, weight, nitro.admitted_weight(*weight), draw, exact
                );
            }
        }
    }

    // ===== Below rate 1.0 the stream really is thinned =====

    #[test]
    fn nitro_at_a_dyadic_rate_writes_the_reciprocal_once_per_row_and_drops_the_rest(
        shift in 1u32..6,
        seed in any::<u64>(),
        data in long_stream(),
    ) {
        let reciprocal = 1u64 << shift;
        let mut nitro = NitroBatch::init_nitro_with_seed(1.0 / reciprocal as f64, seed);
        nitro.insert(&data);

        let target = nitro.target();
        for (cell, mass) in bare_cells(target).iter().enumerate() {
            prop_assert_eq!(
                u64::from(*mass) % reciprocal, 0,
                "rate 1/{}: cell {} holds {}, not a multiple of the compensation",
                reciprocal, cell, mass
            );
        }

        let row_mass: Vec<u64> = (0..target.rows())
            .map(|r| (0..target.cols()).map(|c| u64::from(target.query_one_counter(r, c))).sum())
            .collect();
        for (row, mass) in row_mass.iter().enumerate() {
            prop_assert_eq!(
                *mass, row_mass[0],
                "rate 1/{}: row {} holds {} against row 0's {}",
                reciprocal, row, mass, row_mass[0]
            );
        }

        let admitted = row_mass[0] / reciprocal;
        prop_assert!(
            admitted < data.len() as u64,
            "rate 1/{} admitted all {} arrivals, so nothing was sampled away",
            reciprocal, data.len()
        );
    }

    // ===== The rounding draw is skipped when there is no remainder =====
    //
    // At a dyadic rate the exact weight is an integer, so `admitted_weight`
    // must not touch the generator: the admitted subset has to be the one a
    // run without any weight probe would admit.

    #[test]
    fn nitro_at_a_dyadic_rate_leaves_the_sampling_stream_where_it_found_it(
        shift in 1u32..6,
        seed in any::<u64>(),
        probes in 1usize..16,
        data in stream(256),
    ) {
        let reciprocal = 1u64 << shift;
        let rate = 1.0 / reciprocal as f64;

        let mut probed = NitroBatch::init_nitro_with_seed(rate, seed);
        for _ in 0..probes {
            probed.admitted_weight(1);
        }
        probed.insert(&data);

        let mut untouched = NitroBatch::init_nitro_with_seed(rate, seed);
        untouched.insert(&data);

        prop_assert_eq!(
            bare_cells(probed.target()), bare_cells(untouched.target()),
            "rate 1/{}, seed {}: {} weight probes moved the admitted subset",
            reciprocal, seed, probes
        );
    }

    // ===== Merge is the target's merge =====

    #[test]
    fn nitro_merge_adds_the_two_targets_cell_by_cell(
        rate in sampling_rate(),
        rows in 1usize..6,
        cols in 1usize..64,
        left_data in stream(300),
        right_data in stream(300),
    ) {
        let mut left = NitroBatch::with_target_and_seed(
            rate, CmTarget::with_dimensions(rows, cols), 7);
        let mut right = NitroBatch::with_target_and_seed(
            rate, CmTarget::with_dimensions(rows, cols), 0xBEEF);
        left.insert(&left_data);
        right.insert(&right_data);

        let before_left = cm_cells(left.target());
        let before_right = cm_cells(right.target());
        left.merge(&right);

        let sum: Vec<i32> = before_left
            .iter()
            .zip(&before_right)
            .map(|(a, b)| a + b)
            .collect();
        prop_assert_eq!(
            cm_cells(left.target()), sum,
            "rate {}, {}x{}: merged cells are not the cell-wise sum", rate, rows, cols
        );
        prop_assert_eq!(
            cm_cells(right.target()), before_right,
            "rate {}, {}x{}: merge disturbed the right-hand target", rate, rows, cols
        );
    }

    #[test]
    fn nitro_merge_at_rate_one_matches_streaming_the_concatenation(
        rows in 1usize..6,
        cols in 1usize..64,
        left_data in stream(200),
        right_data in stream(200),
    ) {
        let mut left = NitroBatch::with_target_and_seed(
            1.0, CmTarget::with_dimensions(rows, cols), 7);
        let mut right = NitroBatch::with_target_and_seed(
            1.0, CmTarget::with_dimensions(rows, cols), 0xBEEF);
        left.insert(&left_data);
        right.insert(&right_data);
        left.merge(&right);

        let mut bare = CmTarget::with_dimensions(rows, cols);
        for value in left_data.iter().chain(right_data.iter()) {
            bare.insert(&DataInput::I64(*value));
        }

        prop_assert_eq!(
            cm_cells(left.target()), cm_cells(&bare),
            "{}x{}: merging {} and {} arrivals is not streaming both",
            rows, cols, left_data.len(), right_data.len()
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    // ===== The compensation is unbiased at every rate =====
    //
    // Stochastic rounding is what makes `E[W] = weight / p` hold when `1/p` is
    // not an integer; rounding the same way every time would bias the
    // estimator by the rounding error. The oracle is the unrounded quotient.

    #[test]
    fn nitro_compensation_averages_the_unrounded_quotient(
        rate in fractional_rate(),
        weight in 1u64..8,
        seed in any::<u64>(),
    ) {
        const DRAWS: u64 = 20_000;
        let mut nitro = NitroBatch::with_target_and_seed(
            rate, CmTarget::with_dimensions(2, 16), seed);

        let total: u64 = (0..DRAWS).map(|_| nitro.admitted_weight(weight)).sum();
        let mean = total as f64 / DRAWS as f64;
        let exact = weight as f64 / rate;

        prop_assert!(
            (mean - exact).abs() < 0.05,
            "rate {}, seed {}: weight {} averaged {} over {} draws, not {}",
            rate, seed, weight, mean, DRAWS, exact
        );
    }
}
