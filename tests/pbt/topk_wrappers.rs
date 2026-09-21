//! Property tests for the heap-backed matrix wrappers: `CMSHeap`, `CSHeap`
//! and `CountL2HH`.
//!
//! Both sides of the path laws are real implementations: the wrapper must
//! leave its inner matrix exactly as the bare sketch would, so a bug in the
//! heap bookkeeping cannot reach the counters.

use crate::support::{PROBE_KEYS, edge_dimension, edge_rows, grid, keys};
use asap_sketchlib::{
    CMSHeap, CSHeap, Count, CountL2HH, CountMin, DataInput, DefaultXxHasher, FastPath, HHHeap,
    RegularPath, Vector2D, cs_heap_count, heap_item_to_sketch_input, l2hh_cell_for_row,
};
use proptest::prelude::*;
use std::collections::{HashMap, HashSet};

type Cm = CountMin<Vector2D<i32>, RegularPath>;
type Cs = Count<Vector2D<i32>, RegularPath>;
type CmHeap = CMSHeap<Vector2D<i32>, RegularPath>;
type CsHeap = CSHeap<Vector2D<i32>, RegularPath>;
type CmHeapWire = CMSHeap<Vector2D<i64>, FastPath>;
type CsHeapWire = CSHeap<Vector2D<i64>, RegularPath>;

fn cm_cells(sketch: &Cm) -> Vec<i32> {
    grid(sketch.rows(), sketch.cols(), |r, c| {
        sketch.as_storage().query_one_counter(r, c)
    })
}

fn cs_cells(sketch: &Cs) -> Vec<i32> {
    grid(sketch.rows(), sketch.cols(), |r, c| {
        sketch.as_storage().query_one_counter(r, c)
    })
}

fn held_cms(s: &CmHeapWire) -> Vec<(String, i64)> {
    let mut held: Vec<(String, i64)> = s
        .heap()
        .heap()
        .iter()
        .map(|item| (format!("{:?}", item.key), item.count))
        .collect();
    held.sort();
    held
}

fn held_cs(s: &CsHeapWire) -> Vec<(String, i64)> {
    let mut held: Vec<(String, i64)> = s
        .heap()
        .heap()
        .iter()
        .map(|item| (format!("{:?}", item.key), item.count))
        .collect();
    held.sort();
    held
}

fn stream(max: usize) -> impl Strategy<Value = Vec<u64>> {
    prop::collection::vec(0u64..512, 0..max)
}

/// A key domain narrow enough that two heaps of capacity `top_k` hold a union
/// the merged heap still has room for.
fn narrow_stream(max: usize) -> impl Strategy<Value = Vec<u64>> {
    prop::collection::vec(0u64..8, 0..max)
}

/// A capacity paired with a stream over a key domain no wider than it, so the
/// heap never has to evict.
fn roomy(max: usize) -> impl Strategy<Value = (usize, Vec<u64>)> {
    (1usize..24).prop_flat_map(move |k| (Just(k), prop::collection::vec(0u64..k as u64, 0..max)))
}

/// Hashes and counts fed straight to the `CountL2HH` hash-taking entry points,
/// so the cell laws hold whatever the hasher does.
fn hashed_updates(max: usize) -> impl Strategy<Value = Vec<(u128, i64)>> {
    prop::collection::vec((any::<u128>(), -1_000i64..1_000), 0..max)
}

fn truth_of(stream: &[u64]) -> HashMap<u64, i64> {
    let mut counts: HashMap<u64, i64> = HashMap::new();
    for k in stream {
        *counts.entry(*k).or_default() += 1;
    }
    counts
}

fn count_for(heap: &HHHeap, key: u64) -> Option<i64> {
    heap.find(&DataInput::U64(key))
        .map(|idx| heap.heap()[idx].count)
}

fn resident_keys(heap: &HHHeap) -> HashSet<String> {
    heap.heap()
        .iter()
        .map(|item| format!("{:?}", item.key))
        .collect()
}

/// The median `CountL2HH` reports: the middle value, or the mean of the two
/// straddling it.
fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len() % 2 == 1 {
        values[mid]
    } else {
        (values[mid - 1] + values[mid]) / 2.0
    }
}

fn l2hh_cells(sketch: &CountL2HH) -> Vec<i64> {
    grid(sketch.rows(), sketch.cols(), |r, c| {
        sketch.as_storage().query_one_counter(r, c)
    })
}

/// Sum of squares of each row, the quantity `get_l2_sqr` takes the median of.
fn row_squares(sketch: &CountL2HH) -> Vec<f64> {
    (0..sketch.rows())
        .map(|r| {
            (0..sketch.cols())
                .map(|c| {
                    let v = sketch.as_storage().query_one_counter(r, c) as f64;
                    v * v
                })
                .sum()
        })
        .collect()
}

/// Where the shared cell map says a stream of hashed updates lands.
fn mapped_cells(
    rows: usize,
    cols: usize,
    mask_bits: u32,
    updates: &[(u128, i64)],
) -> Vec<Vec<i64>> {
    let mut cells = vec![vec![0i64; cols]; rows];
    for (hash, delta) in updates {
        for (r, row) in cells.iter_mut().enumerate() {
            let (col, sign) = l2hh_cell_for_row(*hash, r, cols, mask_bits);
            row[col] += sign * delta;
        }
    }
    cells
}

proptest! {
    // ===== A top-k wrapper must not disturb its inner matrix =====

    #[test]
    fn cms_heap_leaves_the_matrix_a_bare_count_min_would_build(
        rows in 1usize..6,
        cols in 1usize..64,
        top_k in 1usize..16,
        keys in stream(300),
    ) {
        let mut wrapped = CmHeap::new(rows, cols, top_k);
        let mut bare = Cm::with_dimensions(rows, cols);
        for k in &keys {
            wrapped.insert(&DataInput::U64(*k));
            bare.insert(&DataInput::U64(*k));
        }

        prop_assert_eq!(cm_cells(wrapped.cms()), cm_cells(&bare));
    }

    #[test]
    fn cs_heap_leaves_the_matrix_a_bare_count_sketch_would_build(
        rows in 1usize..6,
        cols in 1usize..64,
        top_k in 1usize..16,
        keys in stream(300),
    ) {
        let mut wrapped = CsHeap::new(rows, cols, top_k);
        let mut bare = Cs::with_dimensions(rows, cols);
        for k in &keys {
            wrapped.insert(&DataInput::U64(*k));
            bare.insert(&DataInput::U64(*k));
        }

        prop_assert_eq!(cs_cells(wrapped.cs()), cs_cells(&bare));
    }

    // ===== Wire =====

    #[test]
    fn cms_heap_round_trips_at_edge_geometries(
        rows in edge_rows(),
        cols in edge_dimension(),
        top_k in 0usize..16,
        stream in keys(200),
    ) {
        let mut sketch = CmHeapWire::new(rows, cols, top_k);
        for k in &stream {
            sketch.insert(&DataInput::U64(*k));
        }

        round_trip!(
            CmHeapWire,
            sketch,
            |s: &CmHeapWire| (0..PROBE_KEYS as u64)
                .map(|k| s.estimate(&DataInput::U64(k)))
                .collect::<Vec<_>>(),
            held_cms,
        );
    }

    #[test]
    fn cs_heap_round_trips_at_edge_geometries(
        rows in edge_rows(),
        cols in edge_dimension(),
        top_k in 0usize..16,
        stream in keys(200),
    ) {
        let mut sketch = CsHeapWire::new(rows, cols, top_k);
        for k in &stream {
            sketch.insert(&DataInput::U64(*k));
        }

        round_trip!(
            CsHeapWire,
            sketch,
            |s: &CsHeapWire| (0..PROBE_KEYS as u64)
                .map(|k| s.estimate(&DataInput::U64(k)))
                .collect::<Vec<_>>(),
            held_cs,
        );
    }

    // ===== The heap and the matrix beside it say the same thing =====

    /// A Count-Min counter only grows, so the count a resident carries is the
    /// estimate its own last occurrence read: never below the exact frequency,
    /// never above what the matrix says now.
    #[test]
    fn a_cms_resident_carries_a_count_between_its_true_and_estimated_frequency(
        rows in 1usize..5,
        cols in 1usize..32,
        top_k in 1usize..12,
        stream in stream(300),
    ) {
        let mut wrapped = CmHeap::new(rows, cols, top_k);
        for k in &stream {
            wrapped.insert(&DataInput::U64(*k));
        }

        for (key, truth) in truth_of(&stream) {
            if let Some(held) = count_for(wrapped.heap(), key) {
                let estimate = i64::from(wrapped.estimate(&DataInput::U64(key)));
                prop_assert!(
                    truth <= held && held <= estimate,
                    "key {}: true {}, held {}, estimate {}", key, truth, held, estimate
                );
            }
        }
    }

    /// A Count Sketch estimate is a signed median and moves either way, so the
    /// resident's count is pinned to the reading its own last insert took.
    #[test]
    fn a_cs_resident_carries_the_estimate_its_last_insert_read(
        rows in 1usize..5,
        cols in 1usize..32,
        top_k in 1usize..12,
        stream in stream(300),
    ) {
        let mut wrapped = CsHeap::new(rows, cols, top_k);
        let mut read_at_last_insert: HashMap<u64, i64> = HashMap::new();
        for k in &stream {
            let probe = DataInput::U64(*k);
            wrapped.insert(&probe);
            read_at_last_insert.insert(*k, cs_heap_count(wrapped.estimate(&probe)));
        }

        for (key, expected) in read_at_last_insert {
            if let Some(held) = count_for(wrapped.heap(), key) {
                prop_assert_eq!(held, expected, "key {}", key);
            }
        }
    }

    #[test]
    fn neither_wrapper_lets_its_heap_outgrow_top_k(
        rows in 1usize..5,
        cols in 1usize..32,
        top_k in 0usize..12,
        stream in stream(300),
    ) {
        let mut cms = CmHeap::new(rows, cols, top_k);
        let mut cs = CsHeap::new(rows, cols, top_k);
        for k in &stream {
            let probe = DataInput::U64(*k);
            cms.insert(&probe);
            cs.insert(&probe);
            prop_assert!(
                cms.heap().len() <= top_k,
                "CMS holds {} of {}", cms.heap().len(), top_k
            );
            prop_assert!(
                cs.heap().len() <= top_k,
                "CS holds {} of {}", cs.heap().len(), top_k
            );
        }

        prop_assert_eq!(cms.heap().capacity(), top_k);
        prop_assert_eq!(cs.heap().capacity(), top_k);
    }

    #[test]
    fn a_heap_with_room_for_the_whole_stream_keeps_every_key_and_the_heaviest(
        rows in 1usize..5,
        cols in 1usize..32,
        (top_k, stream) in roomy(300),
    ) {
        let mut cms = CmHeap::new(rows, cols, top_k);
        let mut cs = CsHeap::new(rows, cols, top_k);
        for k in &stream {
            let probe = DataInput::U64(*k);
            cms.insert(&probe);
            cs.insert(&probe);
        }

        let truth = truth_of(&stream);
        prop_assert_eq!(cms.heap().len(), truth.len());
        prop_assert_eq!(cs.heap().len(), truth.len());
        for key in truth.keys() {
            prop_assert!(count_for(cms.heap(), *key).is_some(), "CMS dropped key {}", key);
            prop_assert!(count_for(cs.heap(), *key).is_some(), "CS dropped key {}", key);
        }

        if let Some((heaviest, mass)) = truth.iter().max_by_key(|(key, count)| (**count, **key)) {
            prop_assert!(
                count_for(cms.heap(), *heaviest).is_some(),
                "CMS dropped the heaviest key {} of mass {}", heaviest, mass
            );
            prop_assert!(
                count_for(cs.heap(), *heaviest).is_some(),
                "CS dropped the heaviest key {} of mass {}", heaviest, mass
            );
        }
    }

    // ===== Merge reconciles the heap against the merged matrix =====

    #[test]
    fn a_merged_heap_holds_only_keys_one_of_the_two_heaps_held(
        rows in 1usize..5,
        cols in 1usize..32,
        top_k in 1usize..12,
        left in stream(200),
        right in stream(200),
    ) {
        let mut cms = CmHeap::new(rows, cols, top_k);
        let mut cms_other = CmHeap::new(rows, cols, top_k);
        let mut cs = CsHeap::new(rows, cols, top_k);
        let mut cs_other = CsHeap::new(rows, cols, top_k);
        for k in &left {
            cms.insert(&DataInput::U64(*k));
            cs.insert(&DataInput::U64(*k));
        }
        for k in &right {
            cms_other.insert(&DataInput::U64(*k));
            cs_other.insert(&DataInput::U64(*k));
        }

        let cms_union: HashSet<String> = resident_keys(cms.heap())
            .union(&resident_keys(cms_other.heap()))
            .cloned()
            .collect();
        let cs_union: HashSet<String> = resident_keys(cs.heap())
            .union(&resident_keys(cs_other.heap()))
            .cloned()
            .collect();
        cms.merge(&cms_other);
        cs.merge(&cs_other);

        for key in resident_keys(cms.heap()) {
            prop_assert!(cms_union.contains(&key), "CMS invented key {}", key);
        }
        for key in resident_keys(cs.heap()) {
            prop_assert!(cs_union.contains(&key), "CS invented key {}", key);
        }
    }

    /// A merge re-queries every candidate, so no resident carries a count from
    /// before the counters were added.
    #[test]
    fn every_merged_resident_carries_the_merged_matrix_estimate(
        rows in 1usize..5,
        cols in 1usize..32,
        top_k in 1usize..12,
        left in stream(200),
        right in stream(200),
    ) {
        let mut cms = CmHeap::new(rows, cols, top_k);
        let mut cms_other = CmHeap::new(rows, cols, top_k);
        let mut cs = CsHeap::new(rows, cols, top_k);
        let mut cs_other = CsHeap::new(rows, cols, top_k);
        for k in &left {
            cms.insert(&DataInput::U64(*k));
            cs.insert(&DataInput::U64(*k));
        }
        for k in &right {
            cms_other.insert(&DataInput::U64(*k));
            cs_other.insert(&DataInput::U64(*k));
        }
        cms.merge(&cms_other);
        cs.merge(&cs_other);

        for item in cms.heap().heap() {
            let probe = heap_item_to_sketch_input(&item.key);
            prop_assert_eq!(
                item.count,
                i64::from(cms.estimate(&probe)),
                "CMS key {:?}", item.key
            );
        }
        for item in cs.heap().heap() {
            let probe = heap_item_to_sketch_input(&item.key);
            prop_assert_eq!(
                item.count,
                cs_heap_count(cs.estimate(&probe)),
                "CS key {:?}", item.key
            );
        }
    }

    #[test]
    fn merging_heaps_the_capacity_can_hold_keeps_exactly_their_union(
        rows in 1usize..5,
        cols in 1usize..32,
        top_k in 8usize..24,
        left in narrow_stream(200),
        right in narrow_stream(200),
    ) {
        let mut cms = CmHeap::new(rows, cols, top_k);
        let mut cms_other = CmHeap::new(rows, cols, top_k);
        let mut cs = CsHeap::new(rows, cols, top_k);
        let mut cs_other = CsHeap::new(rows, cols, top_k);
        for k in &left {
            cms.insert(&DataInput::U64(*k));
            cs.insert(&DataInput::U64(*k));
        }
        for k in &right {
            cms_other.insert(&DataInput::U64(*k));
            cs_other.insert(&DataInput::U64(*k));
        }

        let cms_union: HashSet<String> = resident_keys(cms.heap())
            .union(&resident_keys(cms_other.heap()))
            .cloned()
            .collect();
        let cs_union: HashSet<String> = resident_keys(cs.heap())
            .union(&resident_keys(cs_other.heap()))
            .cloned()
            .collect();
        cms.merge(&cms_other);
        cs.merge(&cs_other);

        prop_assert_eq!(resident_keys(cms.heap()), cms_union);
        prop_assert_eq!(resident_keys(cs.heap()), cs_union);
    }

    // ===== CountL2HH lands where the shared cell map says =====

    /// An OctoSketch worker addresses cells with `l2hh_cell_for_row` and its
    /// parent with the sketch's own insert, so the two must name one cell.
    #[test]
    fn count_l2hh_writes_the_cells_the_shared_map_names(
        rows in 1usize..6,
        cols in edge_dimension(),
        updates in hashed_updates(64),
    ) {
        let mut sketch = CountL2HH::<DefaultXxHasher>::with_dimensions(rows, cols);
        let mask_bits = sketch.mask_bits();
        for (hash, delta) in &updates {
            sketch.fast_insert_with_count_without_l2_and_hash(*hash, *delta);
        }

        let mapped = mapped_cells(rows, cols, mask_bits, &updates);
        for (r, row) in mapped.iter().enumerate() {
            for (c, cell) in row.iter().enumerate() {
                prop_assert_eq!(
                    sketch.as_storage().query_one_counter(r, c),
                    *cell,
                    "row {} col {}", r, c
                );
            }
        }
    }

    /// The read path resolves its own cells and signs; a key inserted alone
    /// comes back exactly, whatever cells the map chose for it.
    #[test]
    fn count_l2hh_reads_back_the_cells_the_shared_map_names(
        rows in 1usize..6,
        cols in edge_dimension(),
        updates in hashed_updates(64),
    ) {
        let mut sketch = CountL2HH::<DefaultXxHasher>::with_dimensions(rows, cols);
        let mask_bits = sketch.mask_bits();
        for (hash, delta) in &updates {
            sketch.fast_insert_with_count_and_hash(*hash, *delta);
        }

        let mapped = mapped_cells(rows, cols, mask_bits, &updates);
        for (hash, _) in &updates {
            let rows_read: Vec<f64> = (0..rows)
                .map(|r| {
                    let (col, sign) = l2hh_cell_for_row(*hash, r, cols, mask_bits);
                    (sign * mapped[r][col]) as f64
                })
                .collect();
            prop_assert_eq!(
                sketch.fast_get_est_with_hash(*hash),
                median(rows_read),
                "hash {}", hash
            );
        }
    }

    #[test]
    fn the_l2_accumulator_stays_the_median_row_sum_of_squares(
        rows in 1usize..6,
        cols in edge_dimension(),
        updates in hashed_updates(64),
    ) {
        let mut sketch = CountL2HH::<DefaultXxHasher>::with_dimensions(rows, cols);
        for (step, (hash, delta)) in updates.iter().enumerate() {
            sketch.fast_insert_with_count_and_hash(*hash, *delta);
            prop_assert_eq!(
                sketch.get_l2_sqr(),
                median(row_squares(&sketch)),
                "after update {}", step
            );
        }
    }

    #[test]
    fn merging_count_l2hh_adds_the_cells_and_re_derives_the_l2(
        rows in 1usize..6,
        cols in edge_dimension(),
        left in hashed_updates(64),
        right in hashed_updates(64),
    ) {
        let mut sketch = CountL2HH::<DefaultXxHasher>::with_dimensions(rows, cols);
        let mut other = CountL2HH::<DefaultXxHasher>::with_dimensions(rows, cols);
        for (hash, delta) in &left {
            sketch.fast_insert_with_count_and_hash(*hash, *delta);
        }
        for (hash, delta) in &right {
            other.fast_insert_with_count_and_hash(*hash, *delta);
        }
        let before = l2hh_cells(&sketch);
        let added = l2hh_cells(&other);
        sketch.merge(&other);

        let summed: Vec<i64> = before.iter().zip(&added).map(|(a, b)| a + b).collect();
        prop_assert_eq!(l2hh_cells(&sketch), summed);
        prop_assert_eq!(sketch.get_l2_sqr(), median(row_squares(&sketch)));
    }
}
