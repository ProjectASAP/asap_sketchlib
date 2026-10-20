//! Property tests for Count-Min.
//!
//! Cormode and Muthukrishnan, J. Algorithms '05.
//!
//! The laws that compare two real implementations - a stream against its own
//! permutation, a weighted arrival against the repeats it stands for, a
//! pre-hashed entry point against the value one - rest on no model at all.
//! The bound laws use an exact `HashMap` of the generated stream as the
//! oracle, which is independent of the sketch's own bookkeeping.

use crate::support::{PROBE_KEYS, edge_dimension, edge_rows, grid, keys};
use asap_sketchlib::{
    CountMinSketch, DataInput, DefaultMatrixI32, FastPath, MatrixFastHash, MatrixStorage,
    QuickMatrixI64, RegularPath, Vector2D, hash_for_matrix,
};
use proptest::prelude::*;
use std::collections::HashMap;

type Cm = CountMinSketch<Vector2D<i32>, RegularPath>;
type CmFast = CountMinSketch<Vector2D<i32>, FastPath>;
type CmWire = CountMinSketch<Vector2D<i64>, FastPath>;
type CmFixedI32 = CountMinSketch<DefaultMatrixI32, FastPath>;
type CmFixedI64 = CountMinSketch<QuickMatrixI64, FastPath>;

/// Key domain the bound and monotonicity laws probe. Narrow enough that a
/// handful of columns forces collisions on every row.
const DOMAIN: u64 = 32;

fn cm_cells(sketch: &Cm) -> Vec<i32> {
    grid(sketch.rows(), sketch.cols(), |r, c| {
        sketch.as_storage().query_one_counter(r, c)
    })
}

fn cm_fast_cells(sketch: &CmFast) -> Vec<i32> {
    grid(sketch.rows(), sketch.cols(), |r, c| {
        sketch.as_storage().query_one_counter(r, c)
    })
}

fn stream_and_permutation(max: usize) -> impl Strategy<Value = (Vec<u64>, Vec<u64>)> {
    prop::collection::vec(0u64..512, 0..max)
        .prop_flat_map(|v| (Just(v.clone()), Just(v).prop_shuffle()))
}

fn weighted(max: usize) -> impl Strategy<Value = Vec<(u64, i32)>> {
    prop::collection::vec((0u64..512, 1i32..8), 0..max)
}

fn crowded_weighted(max: usize) -> impl Strategy<Value = Vec<(u64, i32)>> {
    prop::collection::vec((0u64..DOMAIN, 1i32..8), 0..max)
}

fn truth_of(updates: &[(u64, i32)]) -> HashMap<u64, i64> {
    let mut counts: HashMap<u64, i64> = HashMap::new();
    for (k, w) in updates {
        *counts.entry(*k).or_default() += i64::from(*w);
    }
    counts
}

/// The column key `k` occupies on each row, from the library's public
/// hash entry point.
fn columns_of(rows: usize, cols: usize, k: u64) -> Vec<usize> {
    let hashed = hash_for_matrix(rows, cols, &DataInput::U64(k));
    (0..rows).map(|r| hashed.col_for_row(r, cols)).collect()
}

/// The lightest row's mass in the cell `k` occupies: the tightest ceiling a
/// Count-Min estimate of `k` can reach, own mass included.
fn ceiling_for(rows: usize, columns: &[Vec<usize>], truth: &HashMap<u64, i64>, k: u64) -> i64 {
    (0..rows)
        .map(|r| {
            truth
                .iter()
                .filter(|(j, _)| columns[**j as usize][r] == columns[k as usize][r])
                .map(|(_, mass)| *mass)
                .sum::<i64>()
        })
        .min()
        .expect("a sketch has at least one row")
}

fn cm_fast_of(rows: usize, cols: usize, stream: &[(u64, i32)]) -> CmFast {
    let mut sketch = CmFast::with_dimensions(rows, cols);
    for (k, w) in stream {
        sketch.update_weighted(&DataInput::U64(*k), *w);
    }
    sketch
}

proptest! {
    // ===== Stream order must not reach the state =====

    #[test]
    fn count_min_is_insensitive_to_stream_order(
        rows in 1usize..6,
        cols in 1usize..64,
        (a, b) in stream_and_permutation(300),
    ) {
        let mut first = Cm::with_dimensions(rows, cols);
        let mut second = Cm::with_dimensions(rows, cols);
        for k in &a {
            first.update(&DataInput::U64(*k));
        }
        for k in &b {
            second.update(&DataInput::U64(*k));
        }

        prop_assert_eq!(cm_cells(&first), cm_cells(&second));
    }

    // ===== A weighted arrival must match repeating it =====

    #[test]
    fn count_min_weighted_matches_repeating_the_update(
        rows in 1usize..6,
        cols in 1usize..64,
        updates in weighted(120),
    ) {
        let mut by_weight = Cm::with_dimensions(rows, cols);
        let mut by_repeat = Cm::with_dimensions(rows, cols);
        for (k, n) in &updates {
            by_weight.update_weighted(&DataInput::U64(*k), *n);
            for _ in 0..*n {
                by_repeat.update(&DataInput::U64(*k));
            }
        }

        prop_assert_eq!(cm_cells(&by_weight), cm_cells(&by_repeat));
    }

    // ===== The pre-hashed entry point must address the same cells =====
    //
    // `update_weighted_batch` is a literal loop over `update_weighted`, so comparing
    // the two would hold by construction. The law with content is that a
    // caller which hashes on its own thread lands in the same cells.

    #[test]
    fn count_min_weighted_entry_points_agree(
        rows in 1usize..6,
        cols in 1usize..64,
        updates in weighted(120),
    ) {
        let pairs: Vec<(DataInput, i32)> = updates
            .iter()
            .map(|(k, n)| (DataInput::U64(*k), *n))
            .collect();
        let hashed: Vec<_> = updates
            .iter()
            .map(|(k, n)| (hash_for_matrix(rows, cols, &DataInput::U64(*k)), *n))
            .collect();

        let mut by_loop = CmFast::with_dimensions(rows, cols);
        for (value, n) in &pairs {
            by_loop.update_weighted(value, *n);
        }
        let mut by_hashes = CmFast::with_dimensions(rows, cols);
        by_hashes.update_weighted_batch_with_hashes(&hashed);

        prop_assert_eq!(cm_fast_cells(&by_hashes), cm_fast_cells(&by_loop));
    }

    // ===== Wire =====

    #[test]
    fn count_min_round_trips_at_edge_geometries(
        rows in edge_rows(),
        cols in edge_dimension(),
        stream in keys(200),
    ) {
        let mut sketch = CmWire::with_dimensions(rows, cols);
        for k in &stream {
            sketch.update(&DataInput::U64(*k));
        }

        round_trip!(
            CmWire,
            sketch,
            |s: &CmWire| (0..PROBE_KEYS as u64)
                .map(|k| s.estimate(&DataInput::U64(k)))
                .collect::<Vec<_>>(),
        );
    }

    // ===== A non-negative stream only ever raises an estimate =====

    #[test]
    fn count_min_estimates_never_fall_while_a_non_negative_stream_arrives(
        rows in 1usize..6,
        cols in 1usize..9,
        stream in prop::collection::vec(0u64..DOMAIN, 0..120),
    ) {
        let mut sketch = Cm::with_dimensions(rows, cols);
        let mut before: Vec<i32> = (0..DOMAIN)
            .map(|k| sketch.estimate(&DataInput::U64(k)))
            .collect();

        for (step, arrival) in stream.iter().enumerate() {
            sketch.update(&DataInput::U64(*arrival));
            for probe in 0..DOMAIN {
                let now = sketch.estimate(&DataInput::U64(probe));
                prop_assert!(
                    now >= before[probe as usize],
                    "insert {} of key {} dropped key {} from {} to {}",
                    step, arrival, probe, before[probe as usize], now
                );
                before[probe as usize] = now;
            }
        }
    }

    // ===== The overshoot is the lightest row's collision mass =====
    //
    // Cormode and Muthukrishnan's guarantee, before the probabilistic step
    // that turns it into an epsilon-delta bound: every row reports the key's
    // own mass plus the mass of the other keys sharing its cell, and the
    // query takes the lightest row. The oracle is an exact `HashMap` of the
    // stream grouped by column, so both sides of the sandwich are
    // deterministic.

    #[test]
    fn count_min_overshoots_by_at_most_the_lightest_row_collision_mass(
        rows in 1usize..6,
        cols in 1usize..9,
        updates in crowded_weighted(200),
    ) {
        let mut sketch = CmFast::with_dimensions(rows, cols);
        for (k, w) in &updates {
            sketch.update_weighted(&DataInput::U64(*k), *w);
        }

        let truth = truth_of(&updates);
        let columns: Vec<Vec<usize>> = (0..DOMAIN).map(|k| columns_of(rows, cols, k)).collect();

        for k in 0..DOMAIN {
            let own = truth.get(&k).copied().unwrap_or(0);
            let collisions = (0..rows)
                .map(|r| {
                    truth
                        .iter()
                        .filter(|(j, _)| **j != k && columns[**j as usize][r] == columns[k as usize][r])
                        .map(|(_, mass)| *mass)
                        .sum::<i64>()
                })
                .min()
                .expect("a sketch has at least one row");
            let est = i64::from(sketch.estimate(&DataInput::U64(k)));

            prop_assert!(est >= own, "key {}: estimate {} below truth {}", k, est, own);
            prop_assert!(
                est <= own + collisions,
                "key {}: estimate {} over truth {} plus the lightest row's collision mass {}",
                k, est, own, collisions
            );
        }
    }

    // ===== Fixed-dimension storage lands in the cells the dynamic one does =====
    //
    // `DefaultMatrixI32` and `QuickMatrixI64` fold the column from the
    // compile-time width; `Vector2D` folds it from a cached mask pair. Two
    // implementations of the same key-to-cell map, at the geometries the
    // fixed matrices are cut for.

    #[test]
    fn count_min_fixed_i32_storage_fills_the_same_cells_as_vector2d(
        updates in prop::collection::vec((any::<u64>(), 1i32..8), 0..160),
    ) {
        let mut fixed = CmFixedI32::default();
        let mut dynamic = CmFast::with_dimensions(fixed.rows(), fixed.cols());
        for (k, w) in &updates {
            fixed.update_weighted(&DataInput::U64(*k), *w);
            dynamic.update_weighted(&DataInput::U64(*k), *w);
        }

        for r in 0..fixed.rows() {
            for c in 0..fixed.cols() {
                prop_assert_eq!(
                    fixed.as_storage().query_one_counter(r, c),
                    dynamic.as_storage().query_one_counter(r, c),
                    "cell ({}, {})", r, c
                );
            }
        }
    }

    #[test]
    fn count_min_fixed_i64_storage_fills_the_same_cells_as_vector2d(
        updates in prop::collection::vec((any::<u64>(), 1i64..8), 0..160),
    ) {
        let mut fixed = CmFixedI64::default();
        let mut dynamic = CmWire::with_dimensions(fixed.rows(), fixed.cols());
        for (k, w) in &updates {
            fixed.update_weighted(&DataInput::U64(*k), *w);
            dynamic.update_weighted(&DataInput::U64(*k), *w);
        }

        for r in 0..fixed.rows() {
            for c in 0..fixed.cols() {
                prop_assert_eq!(
                    fixed.as_storage().query_one_counter(r, c),
                    dynamic.as_storage().query_one_counter(r, c),
                    "cell ({}, {})", r, c
                );
            }
        }
    }

    // ===== merge_max, on the disjoint key sets it documents =====
    //
    // Distinct keys still share cells, so the maxed matrix is not the summed
    // one even here. What disjointness buys is the Count-Min sandwich: the
    // estimate still covers the union stream's truth.

    #[test]
    fn count_min_merge_max_of_disjoint_key_sets_keeps_the_sandwich(
        rows in 1usize..6,
        cols in 1usize..9,
        left_stream in prop::collection::vec((0u64..DOMAIN / 2, 1i32..8), 0..100),
        right_stream in prop::collection::vec((DOMAIN / 2..DOMAIN, 1i32..8), 0..100),
    ) {
        let left = cm_fast_of(rows, cols, &left_stream);
        let right = cm_fast_of(rows, cols, &right_stream);
        let mut maxed = left.clone();
        maxed.merge_max(&right);
        let mut summed = left.clone();
        summed.merge(&right);

        for r in 0..rows {
            for c in 0..cols {
                let mine = left.as_storage().query_one_counter(r, c);
                let theirs = right.as_storage().query_one_counter(r, c);
                let max = maxed.as_storage().query_one_counter(r, c);
                let sum = summed.as_storage().query_one_counter(r, c);

                prop_assert!(max <= sum, "cell ({}, {}): maxed {} above summed {}", r, c, max, sum);
                prop_assert!(
                    max >= mine && max >= theirs,
                    "cell ({}, {}): maxed {} below an operand ({}, {})", r, c, max, mine, theirs
                );
                if theirs == 0 {
                    prop_assert_eq!(max, mine, "cell ({}, {}): an empty peer cell moved it", r, c);
                }
                if mine == 0 {
                    prop_assert_eq!(max, theirs, "cell ({}, {}): an empty own cell kept nothing", r, c);
                }
            }
        }

        let both: Vec<(u64, i32)> = left_stream.iter().chain(right_stream.iter()).copied().collect();
        let truth = truth_of(&both);
        let columns: Vec<Vec<usize>> = (0..DOMAIN).map(|k| columns_of(rows, cols, k)).collect();
        for k in 0..DOMAIN {
            let own = truth.get(&k).copied().unwrap_or(0);
            let ceiling = ceiling_for(rows, &columns, &truth, k);
            let est = i64::from(maxed.estimate(&DataInput::U64(k)));
            prop_assert!(
                est >= own && est <= ceiling,
                "key {}: maxed estimate {} outside [{}, {}]", k, est, own, ceiling
            );
        }
    }

    // ===== merge_max is idempotent =====
    //
    // Maxing a sketch into a copy of itself is the extreme shared-key case,
    // and the one a summing merge cannot imitate.

    #[test]
    fn count_min_merge_max_of_a_copy_of_itself_changes_nothing(
        rows in 1usize..6,
        cols in 1usize..9,
        stream in prop::collection::vec((0u64..DOMAIN, 1i32..8), 0..100),
    ) {
        let sketch = cm_fast_of(rows, cols, &stream);
        let mut maxed = sketch.clone();
        maxed.merge_max(&sketch.clone());

        prop_assert_eq!(cm_fast_cells(&maxed), cm_fast_cells(&sketch));
    }

    // ===== merge_max, on the shared keys it does not promise =====
    //
    // A shared key reads back as the larger side, so the estimate sits between
    // each side's own answer and the summed sketch's.

    #[test]
    fn count_min_merge_max_of_shared_keys_stays_between_each_side_and_their_sum(
        rows in 1usize..6,
        cols in 1usize..9,
        left_stream in prop::collection::vec((0u64..DOMAIN, 1i32..8), 0..100),
        right_stream in prop::collection::vec((0u64..DOMAIN, 1i32..8), 0..100),
    ) {
        let left = cm_fast_of(rows, cols, &left_stream);
        let right = cm_fast_of(rows, cols, &right_stream);
        let mut maxed = left.clone();
        maxed.merge_max(&right);
        let mut summed = left.clone();
        summed.merge(&right);

        for r in 0..rows {
            for c in 0..cols {
                let max = maxed.as_storage().query_one_counter(r, c);
                let sum = summed.as_storage().query_one_counter(r, c);
                prop_assert!(max <= sum, "cell ({}, {}): maxed {} above summed {}", r, c, max, sum);
            }
        }

        for k in 0..DOMAIN {
            let probe = DataInput::U64(k);
            let (mine, theirs) = (left.estimate(&probe), right.estimate(&probe));
            let (max, sum) = (maxed.estimate(&probe), summed.estimate(&probe));
            prop_assert!(
                max >= mine && max >= theirs,
                "key {}: maxed estimate {} below an operand ({}, {})", k, max, mine, theirs
            );
            prop_assert!(max <= sum, "key {}: maxed estimate {} above the summed {}", k, max, sum);
        }
    }
}
