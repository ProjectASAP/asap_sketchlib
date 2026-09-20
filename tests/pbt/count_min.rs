//! Property tests for Count-Min: the native matrix sketch and the portable
//! `CountMinSketch` that carries it over the wire.
//!
//! Cormode and Muthukrishnan, J. Algorithms '05.
//!
//! The laws that compare two real implementations - a stream against its own
//! permutation, a weighted arrival against the repeats it stands for, a
//! pre-hashed entry point against the value one - rest on no model at all.
//! The bound laws use an exact `HashMap` of the generated stream as the
//! oracle, which is independent of the sketch's own bookkeeping.

use crate::support::{
    PROBE_KEYS, close, edge_dimension, edge_rows, grid, keyed_updates, keys, matrices_close,
};
use asap_sketchlib::message_pack_format::MessagePackCodec;
use asap_sketchlib::{
    CountMin, CountMinSketch, CountMinSketchDelta, DataInput, DefaultMatrixI32, FastPath,
    MatrixFastHash, MatrixStorage, QuickMatrixI64, RegularPath, Vector2D, hash_for_matrix,
};
use proptest::prelude::*;
use std::collections::HashMap;

type Cm = CountMin<Vector2D<i32>, RegularPath>;
type CmFast = CountMin<Vector2D<i32>, FastPath>;
type CmWire = CountMin<Vector2D<i64>, FastPath>;
type CmFixedI32 = CountMin<DefaultMatrixI32, FastPath>;
type CmFixedI64 = CountMin<QuickMatrixI64, FastPath>;

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

fn cms_of(rows: usize, cols: usize, updates: &[(String, f64)]) -> CountMinSketch {
    let mut s = CountMinSketch::new(rows, cols);
    for (k, v) in updates {
        s.update(k, *v);
    }
    s
}

// ===== Sequence-law scaffolding =====

/// A transition the sequence law draws from. Every variant is a public entry
/// point of `CountMinSketch`.
#[derive(Debug, Clone)]
enum Op {
    Insert(u64, i64),
    Merge(Vec<(u64, i64)>),
    ApplyDelta(Vec<(u64, i64)>),
    RoundTrip,
    /// Merges a `rows + .0` by `cols + .1` peer, a geometry `merge` refuses.
    MergeMismatched(usize, usize),
}

fn seq_stream(max: usize) -> impl Strategy<Value = Vec<(u64, i64)>> {
    prop::collection::vec((0u64..DOMAIN, 1i64..8), 0..max)
}

/// Inserts dominate, so the sequence reaches a deep state; the transitions
/// that rebuild the backend stay frequent enough to land between them.
fn seq_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        16 => (0u64..DOMAIN, 1i64..8).prop_map(|(k, w)| Op::Insert(k, w)),
        4 => seq_stream(20).prop_map(Op::Merge),
        2 => seq_stream(20).prop_map(Op::ApplyDelta),
        2 => Just(Op::RoundTrip),
        1 => prop_oneof![
            Just((1usize, 0usize)),
            Just((0usize, 1usize)),
            Just((1usize, 1usize)),
            Just((3usize, 2usize)),
        ]
        .prop_map(|(over_rows, over_cols)| Op::MergeMismatched(over_rows, over_cols)),
    ]
}

/// `"0"` through `"31"`, the string keys the portable sketch takes.
fn key_domain() -> Vec<String> {
    (0..DOMAIN).map(|k| k.to_string()).collect()
}

fn cms_stream_of(
    rows: usize,
    cols: usize,
    keys: &[String],
    stream: &[(u64, i64)],
) -> CountMinSketch {
    let mut sketch = CountMinSketch::new(rows, cols);
    for (k, w) in stream {
        sketch.update(&keys[*k as usize], *w as f64);
    }
    sketch
}

/// Every non-zero cell of `sketch`, as the delta that adds it to a peer.
fn delta_of(sketch: &CountMinSketch) -> CountMinSketchDelta {
    let mut cells = Vec::new();
    for (r, row) in sketch.sketch().iter().enumerate() {
        for (c, count) in row.iter().enumerate() {
            if *count != 0.0 {
                cells.push((r as u32, c as u32, *count as i64));
            }
        }
    }
    CountMinSketchDelta {
        rows: sketch.rows() as u32,
        cols: sketch.cols() as u32,
        cells,
        ..CountMinSketchDelta::default()
    }
}

/// The column a string key occupies on each row, from the public hash entry
/// point rather than from the sketch's own bookkeeping.
fn str_columns_of(rows: usize, cols: usize, key: &str) -> Vec<usize> {
    let hashed = hash_for_matrix(rows, cols, &DataInput::Str(key));
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
        sketch.insert_many(&DataInput::U64(*k), *w);
    }
    sketch
}

/// The four invariants every state in the sequence holds: the geometry it was
/// built with, non-negative cells, every row carrying the model's whole mass,
/// and each key's estimate between its truth and its lightest row's mass.
fn holds_model(
    sketch: &CountMinSketch,
    model: &[i64],
    keys: &[String],
    columns: &[Vec<usize>],
    geometry: (usize, usize),
    what: &str,
) -> Result<(), TestCaseError> {
    let (rows, cols) = geometry;
    prop_assert_eq!(sketch.rows(), rows, "{}: rows", what);
    prop_assert_eq!(sketch.cols(), cols, "{}: cols", what);

    let cells = sketch.sketch();
    prop_assert_eq!(cells.len(), rows, "{}: rows of the matrix", what);

    let total: i64 = model.iter().sum();
    for (r, row) in cells.iter().enumerate() {
        prop_assert_eq!(row.len(), cols, "{}: width of row {}", what, r);
        prop_assert!(
            row.iter().all(|cell| *cell >= 0.0),
            "{}: row {} holds a negative cell: {:?}",
            what,
            r,
            row
        );
        let carried: f64 = row.iter().sum();
        prop_assert_eq!(
            carried,
            total as f64,
            "{}: row {} carries {} of the model's {}",
            what,
            r,
            carried,
            total
        );
    }

    let mut column_mass = vec![0i64; rows * cols];
    for (k, mass) in model.iter().enumerate() {
        for (r, col) in columns[k].iter().enumerate() {
            column_mass[r * cols + col] += *mass;
        }
    }

    for (k, own) in model.iter().enumerate() {
        let est = sketch.estimate(&keys[k]).round() as i64;
        let ceiling = (0..rows)
            .map(|r| column_mass[r * cols + columns[k][r]])
            .min()
            .expect("a sketch has at least one row");
        prop_assert!(
            est >= *own && est <= ceiling,
            "{}: key {} estimated {} outside [{}, {}]",
            what,
            keys[k],
            est,
            own,
            ceiling
        );
    }
    Ok(())
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
            first.insert(&DataInput::U64(*k));
        }
        for k in &b {
            second.insert(&DataInput::U64(*k));
        }

        prop_assert_eq!(cm_cells(&first), cm_cells(&second));
    }

    // ===== A weighted arrival must match repeating it =====

    #[test]
    fn count_min_weighted_matches_repeating_the_insert(
        rows in 1usize..6,
        cols in 1usize..64,
        updates in weighted(120),
    ) {
        let mut by_weight = Cm::with_dimensions(rows, cols);
        let mut by_repeat = Cm::with_dimensions(rows, cols);
        for (k, n) in &updates {
            by_weight.insert_many(&DataInput::U64(*k), *n);
            for _ in 0..*n {
                by_repeat.insert(&DataInput::U64(*k));
            }
        }

        prop_assert_eq!(cm_cells(&by_weight), cm_cells(&by_repeat));
    }

    // ===== The pre-hashed entry point must address the same cells =====
    //
    // `bulk_insert_many` is a literal loop over `insert_many`, so comparing
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
            by_loop.insert_many(value, *n);
        }
        let mut by_hashes = CmFast::with_dimensions(rows, cols);
        by_hashes.bulk_insert_many_with_hashes(&hashed);

        prop_assert_eq!(cm_fast_cells(&by_hashes), cm_fast_cells(&by_loop));
    }

    // ===== Merge algebra, on the portable representation =====

    #[test]
    fn count_min_merge_is_commutative(
        rows in 1usize..6,
        cols in 4usize..64,
        a in keyed_updates(30),
        b in keyed_updates(30),
    ) {
        let mut ab = cms_of(rows, cols, &a);
        ab.merge(&cms_of(rows, cols, &b)).expect("merge");
        let mut ba = cms_of(rows, cols, &b);
        ba.merge(&cms_of(rows, cols, &a)).expect("merge");

        prop_assert_eq!(ab.sketch(), ba.sketch());
    }

    #[test]
    fn count_min_merge_is_associative(
        rows in 1usize..6,
        cols in 4usize..64,
        a in keyed_updates(20),
        b in keyed_updates(20),
        c in keyed_updates(20),
    ) {
        let mut left = cms_of(rows, cols, &a);
        left.merge(&cms_of(rows, cols, &b)).expect("merge");
        left.merge(&cms_of(rows, cols, &c)).expect("merge");

        let mut right = cms_of(rows, cols, &b);
        right.merge(&cms_of(rows, cols, &c)).expect("merge");
        let mut right_full = cms_of(rows, cols, &a);
        right_full.merge(&right).expect("merge");

        prop_assert!(matrices_close(&left.sketch(), &right_full.sketch()));
    }

    #[test]
    fn count_min_empty_is_a_merge_identity(
        rows in 1usize..6,
        cols in 4usize..64,
        a in keyed_updates(30),
    ) {
        let base = cms_of(rows, cols, &a);
        let mut merged = base.clone();
        merged.merge(&CountMinSketch::new(rows, cols)).expect("merge");

        prop_assert_eq!(merged.sketch(), base.sketch());
    }

    #[test]
    fn count_min_merge_matches_streaming_the_concatenation(
        rows in 1usize..6,
        cols in 4usize..64,
        a in keyed_updates(30),
        b in keyed_updates(30),
    ) {
        let mut merged = cms_of(rows, cols, &a);
        merged.merge(&cms_of(rows, cols, &b)).expect("merge");

        let concatenated: Vec<_> = a.iter().chain(b.iter()).cloned().collect();
        let streamed = cms_of(rows, cols, &concatenated);

        prop_assert!(matrices_close(&merged.sketch(), &streamed.sketch()));
    }

    #[test]
    fn count_min_never_underestimates(
        rows in 1usize..6,
        cols in 4usize..64,
        updates in keyed_updates(40),
    ) {
        let s = cms_of(rows, cols, &updates);
        for (k, _) in &updates {
            let truth: f64 = updates.iter().filter(|(x, _)| x == k).map(|(_, v)| v).sum();
            let est = s.estimate(k);
            prop_assert!(
                est >= truth || close(est, truth),
                "key {}: estimate {} < truth {}", k, est, truth
            );
        }
    }

    // ===== Wire =====

    #[test]
    fn count_min_round_trip_preserves_every_cell(
        rows in 1usize..8,
        cols in 1usize..128,
        updates in keyed_updates(40),
    ) {
        let s = cms_of(rows, cols, &updates);

        let bytes = s.to_msgpack().expect("encode");
        let restored = CountMinSketch::from_msgpack(&bytes).expect("decode");

        prop_assert_eq!(restored.rows(), s.rows());
        prop_assert_eq!(restored.cols(), s.cols());
        prop_assert_eq!(restored.sketch(), s.sketch());
        for (k, _) in &updates {
            prop_assert_eq!(restored.estimate(k), s.estimate(k), "key {}", k);
        }
    }

    #[test]
    fn count_min_round_trips_at_edge_geometries(
        rows in edge_rows(),
        cols in edge_dimension(),
        stream in keys(200),
    ) {
        let mut sketch = CmWire::with_dimensions(rows, cols);
        for k in &stream {
            sketch.insert(&DataInput::U64(*k));
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
            sketch.insert(&DataInput::U64(*arrival));
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
            sketch.insert_many(&DataInput::U64(*k), *w);
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

    // ===== A merge only ever raises an estimate =====

    #[test]
    fn count_min_merge_never_lowers_either_sides_estimate(
        rows in 1usize..6,
        cols in 1usize..16,
        a in keyed_updates(30),
        b in keyed_updates(30),
    ) {
        let left = cms_of(rows, cols, &a);
        let right = cms_of(rows, cols, &b);
        let mut merged = left.clone();
        merged.merge(&right).expect("merge");

        let probes = a
            .iter()
            .chain(b.iter())
            .map(|(k, _)| k.as_str())
            .chain(["0", "!"]);
        for k in probes {
            let after = merged.estimate(k);
            prop_assert!(
                after >= left.estimate(k),
                "key {}: merged {} below the left estimate {}", k, after, left.estimate(k)
            );
            prop_assert!(
                after >= right.estimate(k),
                "key {}: merged {} below the right estimate {}", k, after, right.estimate(k)
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
            fixed.insert_many(&DataInput::U64(*k), *w);
            dynamic.insert_many(&DataInput::U64(*k), *w);
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
            fixed.insert_many(&DataInput::U64(*k), *w);
            dynamic.insert_many(&DataInput::U64(*k), *w);
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

    // ===== A state that survives being rebuilt =====
    //
    // Merge, apply_delta and a msgpack round trip each replace the backend
    // mid-stream. The model is the exact ledger of everything inserted so far,
    // and every invariant is re-checked after every transition.

    #[test]
    fn count_min_holds_its_model_across_a_sequence_of_transitions(
        rows in 1usize..6,
        cols in 1usize..9,
        ops in prop::collection::vec(seq_op(), 0..80),
    ) {
        let keys = key_domain();
        let columns: Vec<Vec<usize>> = keys
            .iter()
            .map(|k| str_columns_of(rows, cols, k))
            .collect();
        let mut model = vec![0i64; keys.len()];
        let mut sketch = CountMinSketch::new(rows, cols);
        holds_model(&sketch, &model, &keys, &columns, (rows, cols), "before the first op")?;

        for (step, op) in ops.iter().enumerate() {
            match op {
                Op::Insert(k, w) => {
                    sketch.update(&keys[*k as usize], *w as f64);
                    model[*k as usize] += *w;
                }
                Op::Merge(stream) => {
                    let other = cms_stream_of(rows, cols, &keys, stream);
                    sketch.merge(&other).expect("a peer of the same geometry merges");
                    for (k, w) in stream {
                        model[*k as usize] += *w;
                    }
                }
                Op::ApplyDelta(stream) => {
                    let other = cms_stream_of(rows, cols, &keys, stream);
                    let mut merged = sketch.clone();
                    merged.merge(&other).expect("a peer of the same geometry merges");

                    sketch.apply_delta(&delta_of(&other)).expect("an in-range delta applies");
                    for (k, w) in stream {
                        model[*k as usize] += *w;
                    }

                    prop_assert_eq!(
                        sketch.sketch(), merged.sketch(),
                        "step {}: apply_delta and the merge it stands for left different matrices", step
                    );
                }
                Op::RoundTrip => {
                    let bytes = sketch.to_msgpack().expect("encode");
                    let restored = CountMinSketch::from_msgpack(&bytes).expect("decode");
                    prop_assert_eq!(
                        restored.sketch(), sketch.sketch(),
                        "step {}: the decoded matrix differs", step
                    );
                    sketch = restored;
                }
                Op::MergeMismatched(over_rows, over_cols) => {
                    let before = sketch.sketch();
                    let peer = CountMinSketch::new(rows + over_rows, cols + over_cols);
                    prop_assert!(
                        sketch.merge(&peer).is_err(),
                        "step {}: merging a {}x{} peer into a {}x{} sketch was accepted",
                        step, peer.rows(), peer.cols(), rows, cols
                    );
                    prop_assert_eq!(
                        sketch.sketch(), before,
                        "step {}: a refused merge still changed the matrix", step
                    );
                }
            }

            holds_model(
                &sketch, &model, &keys, &columns, (rows, cols),
                &format!("step {}, {:?}", step, op),
            )?;
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
