//! Property tests for Coco.
//!
//! Election is randomized and depends on arrival order, so nothing here may
//! assert an order-independent state. What is exact is the bookkeeping around
//! the election: every arrival adds its weight to exactly one bucket.
//!
//! The oracle is a direct scan of the table - `(row, column)` loops that read
//! `full_key` and `val` - plus the exact sum of the generated stream. That
//! scan owes nothing to `recorded_flows`, `estimate_key` or the hash seeds, so
//! the laws below pin the query paths against the state they claim to read.
//!
//! The update rule the paper does fix is where a new key's weight lands: in a
//! smallest of the `d` buckets it maps to. Which of the tied smallest, and
//! whether the key then takes the slot, are left to chance and are not
//! asserted.
//!
//! Zhang, Liu, Chen, Yang, Tu, Cui, SIGCOMM '21.

use crate::support::{PROBE_KEYS, edge_dimension, edge_rows, keys};
use asap_sketchlib::{Coco, DataInput, DefaultXxHasher, SketchHasher};
use proptest::prelude::*;
use std::collections::{HashMap, HashSet};

type C = Coco<DefaultXxHasher>;

/// The field before the `|`, the partial key the grouping laws project onto.
fn family(full: &str) -> &str {
    full.split('|').next().unwrap_or(full)
}

/// Keys carrying a family in front of an item, so a projection folds several
/// of them together and `item1` prefixes `item10`.
fn key_of(id: u64) -> String {
    format!("fam{}|item{id}", id % 5)
}

/// Families no generated key belongs to.
const ABSENT_FAMILY: &str = "fam9";

/// Substrings probed against the containment query. `item1` also occurs inside
/// `item10`, which is the overlap the query is specified to collect.
const SUBSTRINGS: [&str; 5] = ["fam1", "item1", "item31", "|", "zzz"];

/// Suffixes probed against the UDF query.
const SUFFIXES: [&str; 3] = ["1", "0", "item3"];

/// A geometry, a generator seed and one stream over it.
type Crowded = (usize, usize, u64, Vec<(u64, u64)>);

/// A key domain four times the size of the table, so every bucket is claimed
/// and later arrivals contest an occupied one.
fn crowded_stream(max: usize) -> impl Strategy<Value = Crowded> {
    (1usize..=8, 1usize..=4).prop_flat_map(move |(w, d)| {
        let domain = 4 * (w * d) as u64;
        (
            Just(w),
            Just(d),
            any::<u64>(),
            prop::collection::vec((0..domain, 1u64..1_000), 1..max),
        )
    })
}

/// A geometry, a generator seed and two streams over it, for the laws that merge.
type CrowdedPair = (usize, usize, u64, Vec<(u64, u64)>, Vec<(u64, u64)>);

/// Two streams over one geometry, for the laws that merge.
fn crowded_pair(max: usize) -> impl Strategy<Value = CrowdedPair> {
    (1usize..=8, 1usize..=4).prop_flat_map(move |(w, d)| {
        let domain = 4 * (w * d) as u64;
        (
            Just(w),
            Just(d),
            any::<u64>(),
            prop::collection::vec((0..domain, 1u64..1_000), 1..max),
            prop::collection::vec((0..domain, 1u64..1_000), 1..max),
        )
    })
}

fn filled(w: usize, d: usize, seed: u64, stream: &[(u64, u64)]) -> C {
    let mut sketch = C::init_with_size_and_seed(w, d, seed);
    for (id, v) in stream {
        sketch.insert(&key_of(*id), *v);
    }
    sketch
}

fn mass_of(stream: &[(u64, u64)]) -> u64 {
    stream.iter().map(|&(_, v)| v).sum()
}

/// Every bucket's mass in row-major order, occupied buckets and empty ones
/// alike.
fn vals(sketch: &C) -> Vec<u64> {
    let mut cells = Vec::with_capacity(sketch.d * sketch.w);
    for i in 0..sketch.d {
        for j in 0..sketch.w {
            cells.push(sketch.table[i][j].val);
        }
    }
    cells
}

fn table_mass(sketch: &C) -> u64 {
    vals(sketch).iter().sum()
}

/// The `d` buckets `key` maps to, as `(row, column)`.
fn mapped(sketch: &C, key: &str) -> Vec<(usize, usize)> {
    let input = DataInput::Str(key);
    (0..sketch.d)
        .map(|i| {
            (
                i,
                DefaultXxHasher::hash64_seeded(i, &input) as usize % sketch.w,
            )
        })
        .collect()
}

/// Every bucket that holds a key, read off the table in row-major order.
fn occupied(sketch: &C) -> Vec<(String, u64)> {
    let mut cells = Vec::new();
    for i in 0..sketch.d {
        for j in 0..sketch.w {
            let bucket = &sketch.table[i][j];
            if let Some(key) = &bucket.full_key {
                cells.push((key.clone(), bucket.val));
            }
        }
    }
    cells
}

/// Every bucket holding `key`, as `(row, column)`.
fn homes(sketch: &C, key: &str) -> Vec<(usize, usize)> {
    let mut found = Vec::new();
    for i in 0..sketch.d {
        for j in 0..sketch.w {
            if sketch.table[i][j].full_key.as_deref() == Some(key) {
                found.push((i, j));
            }
        }
    }
    found
}

/// The distinct keys of a stream, in first-arrival order.
fn distinct_keys(stream: &[(u64, u64)]) -> Vec<String> {
    let mut seen = HashSet::new();
    stream
        .iter()
        .map(|&(id, _)| key_of(id))
        .filter(|k| seen.insert(k.clone()))
        .collect()
}

proptest! {
    // ===== Wire =====

    #[test]
    fn coco_round_trips_at_edge_geometries(
        width in edge_dimension(),
        depth in edge_rows(),
        seed in any::<u64>(),
        stream in keys(200),
    ) {
        let mut sketch = C::init_with_size_and_seed(width, depth, seed);
        for k in &stream {
            sketch.insert(&format!("flow-{k}"), 1);
        }

        round_trip!(
            C,
            sketch,
            |s: &C| (0..PROBE_KEYS as u64)
                .map(|k| s.estimate_key(&format!("flow-{k}")))
                .collect::<Vec<_>>(),
        );
    }

    // ===== Mass =====

    #[test]
    fn the_table_holds_exactly_the_mass_that_arrived(
        (w, d, seed, stream) in crowded_stream(300),
    ) {
        let sketch = filled(w, d, seed, &stream);

        prop_assert_eq!(
            table_mass(&sketch),
            mass_of(&stream),
            "{}x{} table after {} arrivals",
            w,
            d,
            stream.len()
        );
    }

    #[test]
    fn an_arrival_for_a_new_key_grows_one_smallest_mapped_bucket(
        (w, d, seed, stream) in crowded_stream(300),
        probe in 0u64..64,
        weight in 1u64..1_000,
    ) {
        let mut sketch = filled(w, d, seed, &stream);
        let key = format!("probe|item{probe}");
        prop_assume!(homes(&sketch, &key).is_empty());

        let at = mapped(&sketch, &key);
        let before = vals(&sketch);
        let smallest = at
            .iter()
            .map(|&(i, j)| before[i * w + j])
            .min()
            .expect("a table has at least one row");

        sketch.insert(&key, weight);

        let after = vals(&sketch);
        let grown: Vec<(usize, usize)> = (0..d)
            .flat_map(|i| (0..w).map(move |j| (i, j)))
            .filter(|&(i, j)| after[i * w + j] != before[i * w + j])
            .collect();
        prop_assert_eq!(grown.len(), 1, "buckets {:?} changed, mapped are {:?}", grown, at);

        let (i, j) = grown[0];
        prop_assert!(at.contains(&(i, j)), "({}, {}) is not mapped by {}", i, j, key);
        prop_assert_eq!(after[i * w + j] - before[i * w + j], weight, "bucket ({}, {})", i, j);
        prop_assert_eq!(
            before[i * w + j],
            smallest,
            "bucket ({}, {}) held {}, not the smallest {} of {:?}",
            i,
            j,
            before[i * w + j],
            smallest,
            at.iter().map(|&(r, c)| before[r * w + c]).collect::<Vec<_>>()
        );
    }

    #[test]
    fn every_key_lives_in_at_most_one_bucket(
        (w, d, seed, stream) in crowded_stream(300),
    ) {
        let sketch = filled(w, d, seed, &stream);

        for key in distinct_keys(&stream) {
            let at = homes(&sketch, &key);
            prop_assert!(at.len() <= 1, "{} sits in buckets {:?}", key, at);
        }
    }

    #[test]
    fn recorded_flows_lists_each_occupied_bucket_once_and_carries_all_the_mass(
        (w, d, seed, stream) in crowded_stream(300),
    ) {
        let sketch = filled(w, d, seed, &stream);
        let listed: Vec<(String, u64)> = sketch
            .recorded_flows()
            .map(|(key, val)| (key.to_string(), val))
            .collect();

        prop_assert_eq!(&listed, &occupied(&sketch), "{}x{} table", w, d);

        let unique: HashSet<&String> = listed.iter().map(|(key, _)| key).collect();
        prop_assert_eq!(unique.len(), listed.len(), "a key is listed twice");

        prop_assert_eq!(
            listed.iter().map(|&(_, val)| val).sum::<u64>(),
            mass_of(&stream),
            "mass sits outside the recorded flows"
        );
    }

    // ===== Queries against the table scan =====

    #[test]
    fn group_by_folds_the_occupied_buckets_onto_their_partial_key(
        (w, d, seed, stream) in crowded_stream(300),
    ) {
        let sketch = filled(w, d, seed, &stream);

        let mut expected: HashMap<String, u64> = HashMap::new();
        for (full, val) in occupied(&sketch) {
            *expected.entry(family(&full).to_string()).or_default() += val;
        }

        prop_assert_eq!(sketch.group_by(family), expected, "{}x{} table", w, d);
    }

    #[test]
    fn estimate_projected_answers_one_group_of_the_same_fold(
        (w, d, seed, stream) in crowded_stream(300),
    ) {
        let sketch = filled(w, d, seed, &stream);
        let cells = occupied(&sketch);

        let mut partials: Vec<String> = cells.iter().map(|(full, _)| family(full).to_string()).collect();
        partials.push(ABSENT_FAMILY.to_string());
        for partial in partials {
            let expected: u64 = cells
                .iter()
                .filter(|(full, _)| family(full) == partial)
                .map(|&(_, val)| val)
                .sum();
            prop_assert_eq!(
                sketch.estimate_projected(&partial, family),
                expected,
                "estimate_projected({})",
                partial
            );
        }
    }

    #[test]
    fn estimate_substring_collects_every_key_containing_the_probe(
        (w, d, seed, stream) in crowded_stream(300),
    ) {
        let sketch = filled(w, d, seed, &stream);
        let cells = occupied(&sketch);

        for probe in SUBSTRINGS {
            let expected: u64 = cells
                .iter()
                .filter(|(full, _)| full.contains(probe))
                .map(|&(_, val)| val)
                .sum();
            prop_assert_eq!(
                sketch.estimate_substring(probe),
                expected,
                "estimate_substring({})",
                probe
            );
        }
    }

    #[test]
    fn estimate_with_udf_collects_the_keys_its_predicate_accepts(
        (w, d, seed, stream) in crowded_stream(300),
    ) {
        let sketch = filled(w, d, seed, &stream);
        let cells = occupied(&sketch);
        let ends_with = |full: &str, partial: &str| full.ends_with(partial);

        for probe in SUFFIXES {
            let expected: u64 = cells
                .iter()
                .filter(|(full, _)| full.ends_with(probe))
                .map(|&(_, val)| val)
                .sum();
            prop_assert_eq!(
                sketch.estimate_with_udf(probe, ends_with),
                expected,
                "estimate_with_udf({})",
                probe
            );
        }
    }

    #[test]
    fn estimate_key_reports_the_mass_of_the_bucket_that_holds_the_key(
        (w, d, seed, stream) in crowded_stream(300),
    ) {
        let sketch = filled(w, d, seed, &stream);
        let total = mass_of(&stream);
        let cells = occupied(&sketch);

        let mut probes = distinct_keys(&stream);
        probes.extend((0..4u64).map(|i| format!("never-inserted-{i}")));
        for key in probes {
            let expected: u64 = cells
                .iter()
                .filter(|(full, _)| *full == key)
                .map(|&(_, val)| val)
                .sum();
            let estimate = sketch.estimate_key(&key);
            prop_assert_eq!(estimate, expected, "estimate_key({})", key);
            prop_assert!(estimate <= total, "estimate_key({}) exceeds {}", key, total);
        }
    }

    // ===== Merge =====

    #[test]
    fn merge_adds_the_other_tables_mass_to_this_one(
        (w, d, seed, left_stream, right_stream) in crowded_pair(200),
    ) {
        let mut left = filled(w, d, seed, &left_stream);
        let right = filled(w, d, seed.wrapping_add(1), &right_stream);
        left.merge(&right);

        prop_assert_eq!(
            table_mass(&left),
            mass_of(&left_stream) + mass_of(&right_stream),
            "{}x{} table after merging {} buckets",
            w,
            d,
            occupied(&right).len()
        );
    }

    #[test]
    fn merge_leaves_every_key_in_at_most_one_bucket(
        (w, d, seed, left_stream, right_stream) in crowded_pair(200),
    ) {
        let mut left = filled(w, d, seed, &left_stream);
        let right = filled(w, d, seed.wrapping_add(1), &right_stream);
        left.merge(&right);

        for key in distinct_keys(&left_stream)
            .into_iter()
            .chain(distinct_keys(&right_stream))
        {
            let at = homes(&left, &key);
            prop_assert!(at.len() <= 1, "{} sits in buckets {:?} after merge", key, at);
        }
    }
}
