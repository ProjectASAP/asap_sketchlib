//! Property tests for the Elastic sketch.
//!
//! Yang et al., SIGCOMM '18. Eviction moves a whole vote count from the heavy
//! part into the light part, so the conserved quantity is total mass rather
//! than any individual bucket.
//!
//! The oracle is an exact `HashMap` over the generated stream, plus the
//! paper's own accounting: every arrival adds its weight to exactly one place,
//! a takeover carries the resident's whole positive vote into the light layer,
//! and a Count-Min row therefore holds the mass the heavy part is not holding.
//! Nothing here reads the implementation's bookkeeping for its expectation.

use crate::support::{PROBE_KEYS, edge_dimension, edge_rows, keys};
use asap_sketchlib::sketches::elastic::LAMBDA;
use asap_sketchlib::{DefaultXxHasher, Elastic};
use proptest::prelude::*;
use std::collections::HashMap;

type E = Elastic<DefaultXxHasher>;

fn flow(key: u64) -> String {
    format!("flow-{key}")
}

/// A heavy table too small for the key domain, over a stream long enough for
/// residents to be contested: `LAMBDA` negative votes per positive one have to
/// accumulate before anything is evicted.
fn crowded(max_len: usize) -> impl Strategy<Value = (i32, usize, usize, Vec<u64>)> {
    (1i32..5, 1usize..4, 8usize..64, 4u64..48).prop_flat_map(
        move |(buckets, light_rows, light_cols, domain)| {
            (
                Just(buckets),
                Just(light_rows),
                Just(light_cols),
                prop::collection::vec(0..domain, 64..max_len),
            )
        },
    )
}

/// The crowded regime with a weight on every arrival.
fn crowded_weighted(max_len: usize) -> impl Strategy<Value = (i32, usize, usize, Vec<(u64, i32)>)> {
    (1i32..5, 1usize..4, 8usize..64, 4u64..48).prop_flat_map(
        move |(buckets, light_rows, light_cols, domain)| {
            (
                Just(buckets),
                Just(light_rows),
                Just(light_cols),
                prop::collection::vec((0..domain, 1i32..6), 64..max_len),
            )
        },
    )
}

/// Two streams over one geometry, for the merge laws.
fn crowded_pair(max_len: usize) -> impl Strategy<Value = (i32, usize, usize, Vec<u64>, Vec<u64>)> {
    (1i32..5, 1usize..4, 8usize..64, 4u64..48).prop_flat_map(
        move |(buckets, light_rows, light_cols, domain)| {
            (
                Just(buckets),
                Just(light_rows),
                Just(light_cols),
                prop::collection::vec(0..domain, 64..max_len),
                prop::collection::vec(0..domain, 64..max_len),
            )
        },
    )
}

fn truth_of(stream: &[u64]) -> HashMap<u64, i64> {
    let mut counts: HashMap<u64, i64> = HashMap::new();
    for key in stream {
        *counts.entry(*key).or_default() += 1;
    }
    counts
}

fn sketch_of(buckets: i32, light_rows: usize, light_cols: usize, stream: &[u64]) -> E {
    let mut sketch = E::init_with_dimensions(buckets, light_rows, light_cols);
    for key in stream {
        sketch.insert(flow(*key));
    }
    sketch
}

fn heavy_mass(sketch: &E) -> i64 {
    sketch.heavy.iter().map(|b| b.vote_pos as i64).sum()
}

/// Each light-layer row holds the same total: a Count-Min update adds its
/// weight to one cell of every row.
fn light_row_sums(sketch: &E) -> Vec<i64> {
    (0..sketch.light.rows())
        .map(|r| {
            sketch
                .light
                .as_storage()
                .row_slice(r)
                .iter()
                .map(|c| *c as i64)
                .sum()
        })
        .collect()
}

fn light_cells(sketch: &E) -> Vec<i64> {
    sketch
        .light
        .as_storage()
        .as_slice()
        .iter()
        .map(|c| *c as i64)
        .collect()
}

fn flags(sketch: &E) -> Vec<bool> {
    sketch.heavy.iter().map(|b| b.eviction).collect()
}

/// The bucket holding `id`, if the heavy part holds it at all. Without an
/// expansion a flow occupies at most one bucket, so this is what `query` reads.
fn resident_bucket<'a>(sketch: &'a E, id: &str) -> Option<&'a asap_sketchlib::HeavyBucket> {
    sketch
        .heavy
        .iter()
        .find(|b| !b.is_vacant() && b.flow_id == id)
}

/// The generators above have to reach the eviction regime or half these laws
/// run on untouched buckets. One bucket, a round-robin over nine flows: the
/// first resident collects `LAMBDA` negative votes per pass and is replaced.
#[test]
fn the_crowded_regime_reaches_eviction() {
    let stream: Vec<u64> = (0..270u64).map(|i| i % 9).collect();
    let sketch = sketch_of(1, 2, 16, &stream);

    assert!(
        sketch.heavy.iter().any(|b| b.eviction),
        "no bucket was ever evicted: {:?}",
        sketch.heavy
    );
    assert!(
        light_row_sums(&sketch)[0] > 0,
        "nothing reached the light layer"
    );
}

proptest! {
    // ===== Wire =====

    #[test]
    fn elastic_round_trips_at_edge_geometries(
        buckets in edge_dimension(),
        light_rows in edge_rows(),
        light_cols in edge_dimension(),
        stream in keys(200),
    ) {
        let mut sketch = E::init_with_dimensions(buckets as i32, light_rows, light_cols);
        for k in &stream {
            sketch.insert(format!("flow-{k}"));
        }

        round_trip!(
            E,
            sketch,
            |s: &E| (0..PROBE_KEYS as u64)
                .map(|k| s.query(format!("flow-{k}")))
                .collect::<Vec<_>>(),
        );
    }

    // ===== Mass =====

    #[test]
    fn the_heavy_votes_and_any_light_row_add_up_to_the_stream(
        (buckets, light_rows, light_cols, stream) in crowded(400),
    ) {
        let sketch = sketch_of(buckets, light_rows, light_cols, &stream);
        let total = stream.len() as i64;

        for (r, row) in light_row_sums(&sketch).iter().enumerate() {
            prop_assert_eq!(
                heavy_mass(&sketch) + row, total,
                "row {}: heavy {} plus light {} is not the {} inserted",
                r, heavy_mass(&sketch), row, total
            );
        }
    }

    #[test]
    fn weighted_arrivals_keep_the_same_mass_account(
        (buckets, light_rows, light_cols, stream) in crowded_weighted(300),
    ) {
        let mut sketch = E::init_with_dimensions(buckets, light_rows, light_cols);
        for (key, weight) in &stream {
            sketch.insert_many(flow(*key), *weight);
        }
        let total: i64 = stream.iter().map(|(_, w)| *w as i64).sum();

        for (r, row) in light_row_sums(&sketch).iter().enumerate() {
            prop_assert_eq!(
                heavy_mass(&sketch) + row, total,
                "row {}: heavy {} plus light {} is not the {} inserted",
                r, heavy_mass(&sketch), row, total
            );
        }
    }

    // ===== Estimates against the exact counts =====

    #[test]
    fn an_unflagged_resident_reads_its_exact_count(
        (buckets, light_rows, light_cols, stream) in crowded(400),
    ) {
        let sketch = sketch_of(buckets, light_rows, light_cols, &stream);

        for (key, count) in truth_of(&stream) {
            let id = flow(key);
            let Some(bucket) = resident_bucket(&sketch, &id) else { continue };
            if bucket.eviction {
                continue;
            }
            prop_assert_eq!(
                sketch.query(id.clone()) as i64, count,
                "{}: unflagged resident reads {} against a true {}",
                id, sketch.query(id.clone()), count
            );
        }
    }

    #[test]
    fn no_estimate_falls_below_the_exact_count(
        (buckets, light_rows, light_cols, stream) in crowded(400),
    ) {
        let sketch = sketch_of(buckets, light_rows, light_cols, &stream);

        for (key, count) in truth_of(&stream) {
            let id = flow(key);
            let estimate = sketch.query(id.clone()) as i64;
            prop_assert!(
                estimate >= count,
                "{}: estimate {} below the true {}", id, estimate, count
            );
        }
    }

    // ===== Bucket invariants along the insert path =====

    #[test]
    fn the_eviction_flag_is_never_cleared_by_an_insert(
        (buckets, light_rows, light_cols, stream) in crowded(300),
    ) {
        let mut sketch = E::init_with_dimensions(buckets, light_rows, light_cols);
        let mut before = flags(&sketch);

        for (step, key) in stream.iter().enumerate() {
            sketch.insert(flow(*key));
            let after = flags(&sketch);
            for (idx, (was, now)) in before.iter().zip(&after).enumerate() {
                prop_assert!(
                    !(*was && !*now),
                    "bucket {} lost its eviction flag at step {} ({})",
                    idx, step, flow(*key)
                );
            }
            before = after;
        }
    }

    #[test]
    fn an_occupied_bucket_holds_fewer_than_lambda_negative_votes_per_positive(
        (buckets, light_rows, light_cols, stream) in crowded(300),
    ) {
        let mut sketch = E::init_with_dimensions(buckets, light_rows, light_cols);

        for (step, key) in stream.iter().enumerate() {
            sketch.insert(flow(*key));
            for (idx, bucket) in sketch.heavy.iter().enumerate() {
                if bucket.is_vacant() {
                    continue;
                }
                prop_assert!(
                    bucket.vote_neg < LAMBDA * bucket.vote_pos,
                    "bucket {} at step {}: {} negative against {} positive votes",
                    idx, step, bucket.vote_neg, bucket.vote_pos
                );
            }
        }
    }

    // ===== Heavy-part messages =====

    #[test]
    fn splitting_a_heavy_message_in_two_leaves_the_same_estimate(
        (buckets, light_rows, light_cols, stream) in crowded(200),
        key in 0u64..48,
        first in 1i32..40,
        second in 1i32..40,
        eviction in any::<bool>(),
    ) {
        let base = sketch_of(buckets, light_rows, light_cols, &stream);
        let id = flow(key);

        let mut whole = base.clone();
        whole.merge_heavy(id.clone(), first + second, eviction);

        let mut split = base;
        split.merge_heavy(id.clone(), first, eviction);
        split.merge_heavy(id.clone(), second, eviction);

        prop_assert_eq!(
            split.query(id.clone()), whole.query(id.clone()),
            "{}: {} + {} split reads {} against {} whole",
            id, first, second, split.query(id.clone()), whole.query(id.clone())
        );
        prop_assert_eq!(
            resident_bucket(&split, &id).map(|b| b.eviction),
            resident_bucket(&whole, &id).map(|b| b.eviction),
            "{}: the split and whole messages disagree on residency or the flag", id
        );
        prop_assert_eq!(
            heavy_mass(&split) + light_row_sums(&split)[0],
            heavy_mass(&whole) + light_row_sums(&whole)[0],
            "{}: the split message moved a different total mass", id
        );
    }

    // ===== Merge =====

    #[test]
    fn merging_conserves_the_two_masses(
        (buckets, light_rows, light_cols, left, right) in crowded_pair(300),
    ) {
        let mut a = sketch_of(buckets, light_rows, light_cols, &left);
        let b = sketch_of(buckets, light_rows, light_cols, &right);
        let total = (left.len() + right.len()) as i64;

        a.merge(&b);

        for (r, row) in light_row_sums(&a).iter().enumerate() {
            prop_assert_eq!(
                heavy_mass(&a) + row, total,
                "row {}: heavy {} plus light {} is not the {} inserted on both sides",
                r, heavy_mass(&a), row, total
            );
        }
    }

    #[test]
    fn merging_never_loses_a_light_cell_from_either_side(
        (buckets, light_rows, light_cols, left, right) in crowded_pair(300),
    ) {
        let mut a = sketch_of(buckets, light_rows, light_cols, &left);
        let b = sketch_of(buckets, light_rows, light_cols, &right);
        let (cells_a, cells_b) = (light_cells(&a), light_cells(&b));

        a.merge(&b);

        for (idx, cell) in light_cells(&a).iter().enumerate() {
            prop_assert!(
                *cell >= cells_a[idx] + cells_b[idx],
                "cell {}: {} below the {} plus {} the two sides held",
                idx, cell, cells_a[idx], cells_b[idx]
            );
        }
    }

    #[test]
    fn a_merged_estimate_never_falls_below_the_two_streams_combined(
        (buckets, light_rows, light_cols, left, right) in crowded_pair(300),
    ) {
        let mut a = sketch_of(buckets, light_rows, light_cols, &left);
        let b = sketch_of(buckets, light_rows, light_cols, &right);
        let mut truth = truth_of(&left);
        for (key, count) in truth_of(&right) {
            *truth.entry(key).or_default() += count;
        }

        a.merge(&b);

        for (key, count) in truth {
            let id = flow(key);
            let estimate = a.query(id.clone()) as i64;
            prop_assert!(
                estimate >= count,
                "{}: merged estimate {} below the true {}", id, estimate, count
            );
        }
    }
}
