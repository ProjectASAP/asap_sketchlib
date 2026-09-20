//! Property tests for Count Sketch: the native matrix sketch and the portable
//! `CountSketch` that carries it over the wire.
//!
//! Charikar, Chen, Farach-Colton, ICALP '02.

use crate::support::{
    PROBE_KEYS, edge_dimension, edge_rows, grid, keyed_updates, keys, matrices_close,
};
use asap_sketchlib::message_pack_format::MessagePackCodec;
use asap_sketchlib::{Count, CountSketch, DataInput, RegularPath, Vector2D};
use proptest::prelude::*;

type Cs = Count<Vector2D<i32>, RegularPath>;
type CsWire = Count<Vector2D<i64>, RegularPath>;

fn cs_cells(sketch: &Cs) -> Vec<i32> {
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

fn cs_of(rows: usize, cols: usize, updates: &[(String, f64)]) -> CountSketch {
    let mut s = CountSketch::new(rows, cols);
    for (k, v) in updates {
        s.update(k, *v);
    }
    s
}

/// Signed weights small enough that no `i32` cell can overflow.
fn signed_weights(max: usize) -> impl Strategy<Value = Vec<(u64, i32)>> {
    prop::collection::vec((0u64..512, -1_000i32..1_000), 0..max)
}

/// Integral weights, so every portable cell stays exact in `f64`.
fn integral_updates(max: usize) -> impl Strategy<Value = Vec<(String, f64)>> {
    prop::collection::vec(("[a-z]{1,4}", -1_000i32..1_000), 0..max)
        .prop_map(|v| v.into_iter().map(|(k, w)| (k, f64::from(w))).collect())
}

/// Power-of-two widths the packed-64 hash carries at these row counts.
fn packed_cols() -> impl Strategy<Value = usize> {
    prop_oneof![Just(2usize), Just(4), Just(8), Just(16), Just(32)]
}

/// The cell and sign a key holds in each row, read off a sketch holding that
/// key alone.
fn signature(rows: usize, cols: usize, key: u64) -> Vec<(usize, i32)> {
    let mut probe = Cs::with_dimensions(rows, cols);
    probe.insert(&DataInput::U64(key));
    (0..rows)
        .map(|r| {
            let touched: Vec<(usize, i32)> = (0..cols)
                .map(|c| (c, probe.as_storage().query_one_counter(r, c)))
                .filter(|(_, v)| *v != 0)
                .collect();
            assert_eq!(
                touched.len(),
                1,
                "row {r} holds {} nonzero cells after one insert",
                touched.len()
            );
            touched[0]
        })
        .collect()
}

/// Each row's own reading of a key: its cell, put back through its sign.
fn row_readings(sketch: &Cs, signature: &[(usize, i32)]) -> Vec<f64> {
    signature
        .iter()
        .enumerate()
        .map(|(r, (c, sign))| {
            f64::from(*sign) * f64::from(sketch.as_storage().query_one_counter(r, *c))
        })
        .collect()
}

/// The first `(row, col)` where two readings of the same grid disagree.
fn first_difference(cols: usize, got: &[i32], want: &[i32]) -> Option<(usize, usize)> {
    (0..got.len().min(want.len()))
        .find(|i| got[*i] != want[*i])
        .map(|i| (i / cols, i % cols))
}

proptest! {
    // ===== Stream order must not reach the state =====

    #[test]
    fn count_sketch_is_insensitive_to_stream_order(
        rows in 1usize..6,
        cols in 1usize..64,
        (a, b) in stream_and_permutation(300),
    ) {
        let mut first = Cs::with_dimensions(rows, cols);
        let mut second = Cs::with_dimensions(rows, cols);
        for k in &a {
            first.insert(&DataInput::U64(*k));
        }
        for k in &b {
            second.insert(&DataInput::U64(*k));
        }

        prop_assert_eq!(cs_cells(&first), cs_cells(&second));
    }

    // ===== A weighted arrival must match repeating it =====

    #[test]
    fn count_sketch_weighted_matches_repeating_the_insert(
        rows in 1usize..6,
        cols in 1usize..64,
        updates in weighted(120),
    ) {
        let mut by_weight = Cs::with_dimensions(rows, cols);
        let mut by_repeat = Cs::with_dimensions(rows, cols);
        for (k, n) in &updates {
            by_weight.insert_many(&DataInput::U64(*k), *n);
            for _ in 0..*n {
                by_repeat.insert(&DataInput::U64(*k));
            }
        }

        prop_assert_eq!(cs_cells(&by_weight), cs_cells(&by_repeat));
    }

    // ===== Merge algebra, on the portable representation =====

    #[test]
    fn count_sketch_merge_matches_streaming_the_concatenation(
        rows in 1usize..6,
        cols in 4usize..64,
        a in keyed_updates(30),
        b in keyed_updates(30),
    ) {
        let mut merged = cs_of(rows, cols, &a);
        merged.merge(&cs_of(rows, cols, &b)).expect("merge");

        let concatenated: Vec<_> = a.iter().chain(b.iter()).cloned().collect();
        let streamed = cs_of(rows, cols, &concatenated);

        prop_assert!(matrices_close(merged.sketch(), streamed.sketch()));
    }

    // ===== Wire =====

    #[test]
    fn count_sketch_round_trip_preserves_every_cell(
        rows in 1usize..8,
        cols in 1usize..128,
        updates in keyed_updates(40),
    ) {
        let s = cs_of(rows, cols, &updates);

        let bytes = s.to_msgpack().expect("encode");
        let restored = CountSketch::from_msgpack(&bytes).expect("decode");

        prop_assert_eq!(restored.sketch(), s.sketch());
        for (k, _) in &updates {
            prop_assert_eq!(restored.estimate(k), s.estimate(k), "key {}", k);
        }
    }

    #[test]
    fn count_sketch_round_trips_at_edge_geometries(
        rows in edge_rows(),
        cols in edge_dimension(),
        stream in keys(200),
    ) {
        let mut sketch = CsWire::with_dimensions(rows, cols);
        for k in &stream {
            sketch.insert(&DataInput::U64(*k));
        }

        round_trip!(
            CsWire,
            sketch,
            |s: &CsWire| (0..PROBE_KEYS as u64)
                .map(|k| s.estimate(&DataInput::U64(k)))
                .collect::<Vec<_>>(),
        );
    }

    // ===== A mirrored weight runs the insert backwards =====

    #[test]
    fn a_mirrored_weight_puts_every_cell_back_where_it_was(
        rows in 1usize..6,
        cols in 1usize..8,
        background in keys(150),
        undone in signed_weights(24),
    ) {
        let mut sketch = Cs::with_dimensions(rows, cols);
        for k in &background {
            sketch.insert(&DataInput::U64(*k));
        }
        let before = cs_cells(&sketch);

        for (k, w) in &undone {
            sketch.insert_many(&DataInput::U64(*k), *w);
        }
        for (k, w) in undone.iter().rev() {
            sketch.insert_many(&DataInput::U64(*k), -*w);
        }

        prop_assert_eq!(first_difference(cols, &cs_cells(&sketch), &before), None);
    }

    #[test]
    fn a_weight_of_minus_one_undoes_a_unit_insert(
        rows in 1usize..6,
        cols in 1usize..8,
        background in keys(150),
        undone in keys(24),
    ) {
        let mut sketch = Cs::with_dimensions(rows, cols);
        for k in &background {
            sketch.insert(&DataInput::U64(*k));
        }
        let before = cs_cells(&sketch);

        for k in &undone {
            sketch.insert(&DataInput::U64(*k));
        }
        for k in undone.iter().rev() {
            sketch.insert_many(&DataInput::U64(*k), -1);
        }

        prop_assert_eq!(first_difference(cols, &cs_cells(&sketch), &before), None);
    }

    // ===== A key's cell and sign do not move under it =====

    #[test]
    fn a_key_writes_the_same_cell_and_sign_at_every_point_in_the_stream(
        rows in 1usize..6,
        cols in 1usize..8,
        key in 0u64..512,
        background in keys(120),
    ) {
        let signature = signature(rows, cols, key);
        let mut sketch = Cs::with_dimensions(rows, cols);

        for (step, k) in background.iter().enumerate() {
            sketch.insert(&DataInput::U64(*k));
            let before = cs_cells(&sketch);
            sketch.insert(&DataInput::U64(key));
            let after = cs_cells(&sketch);

            for (r, (col, sign)) in signature.iter().enumerate() {
                for c in 0..cols {
                    let i = r * cols + c;
                    let want = if c == *col { *sign } else { 0 };
                    prop_assert_eq!(
                        after[i] - before[i], want,
                        "key {} at step {}: cell ({}, {})", key, step, r, c
                    );
                }
            }
        }
    }

    // ===== A stream of one key has nothing to collide with =====

    #[test]
    fn a_lone_key_is_estimated_exactly(
        rows in 1usize..8,
        cols in 1usize..8,
        key in 0u64..512,
        repeats in 0usize..64,
        weights in prop::collection::vec(-1_000i32..1_000, 0..24),
    ) {
        let mut repeated = Cs::with_dimensions(rows, cols);
        for _ in 0..repeats {
            repeated.insert(&DataInput::U64(key));
        }
        prop_assert_eq!(
            repeated.estimate(&DataInput::U64(key)), repeats as f64,
            "key {} inserted {} times", key, repeats
        );

        let mut signed = Cs::with_dimensions(rows, cols);
        for w in &weights {
            signed.insert_many(&DataInput::U64(key), *w);
        }
        let total: i32 = weights.iter().sum();
        prop_assert_eq!(
            signed.estimate(&DataInput::U64(key)), f64::from(total),
            "key {} carrying {:?}", key, weights
        );
    }

    // ===== The estimate is the rows' median =====

    #[test]
    fn the_estimate_is_the_middle_of_the_rows_own_readings(
        rows in 1usize..8,
        cols in 1usize..6,
        key in 0u64..512,
        stream in keys(200),
    ) {
        let signature = signature(rows, cols, key);
        let mut sketch = Cs::with_dimensions(rows, cols);
        for k in &stream {
            sketch.insert(&DataInput::U64(*k));
        }

        let readings = row_readings(&sketch, &signature);
        let mut ordered = readings.clone();
        ordered.sort_by(|a, b| a.partial_cmp(b).expect("an integer counter is never NaN"));
        let estimate = sketch.estimate(&DataInput::U64(key));

        prop_assert!(
            estimate >= ordered[0] && estimate <= ordered[rows - 1],
            "key {}: estimate {} outside the rows' span [{}, {}], readings {:?}",
            key, estimate, ordered[0], ordered[rows - 1], readings
        );
        if rows % 2 == 1 {
            prop_assert_eq!(
                estimate, ordered[rows / 2],
                "key {}: estimate {} is not the middle of {:?}", key, estimate, ordered
            );
        }
    }

    // ===== Cells are linear in the stream =====

    #[test]
    fn cells_of_two_streams_add_to_the_cells_of_their_concatenation(
        rows in 1usize..6,
        cols in packed_cols(),
        a in integral_updates(30),
        b in integral_updates(30),
    ) {
        let left = cs_of(rows, cols, &a);
        let right = cs_of(rows, cols, &b);
        let concatenated: Vec<_> = a.iter().chain(b.iter()).cloned().collect();
        let both = cs_of(rows, cols, &concatenated);

        for r in 0..rows {
            for c in 0..cols {
                prop_assert_eq!(
                    left.sketch()[r][c] + right.sketch()[r][c], both.sketch()[r][c],
                    "cell ({}, {})", r, c
                );
            }
        }
    }

    #[test]
    fn negating_every_weight_negates_every_cell(
        rows in 1usize..6,
        cols in packed_cols(),
        updates in integral_updates(40),
    ) {
        let forward = cs_of(rows, cols, &updates);
        let negated: Vec<_> = updates.iter().map(|(k, v)| (k.clone(), -v)).collect();
        let backward = cs_of(rows, cols, &negated);

        for r in 0..rows {
            for c in 0..cols {
                prop_assert_eq!(
                    backward.sketch()[r][c], -forward.sketch()[r][c],
                    "cell ({}, {})", r, c
                );
            }
        }
    }
}
