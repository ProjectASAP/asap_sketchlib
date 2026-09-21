//! Property tests for UnivMon.
//!
//! Liu, Manousis, Vorsanger, Sekar, Braverman, SIGCOMM '16. The layer a key
//! lands in comes from a deterministic hash, so the nesting law is an exact
//! equality rather than a probabilistic statement.
//!
//! The oracle for layer membership is the trailing-ones closed form read off
//! `hash64_seeded(BOTTOM_LAYER_FINDER, key)`, which owes nothing to the
//! pyramid's own `bottom_layer_for_hash` loop. The oracle for per-layer state
//! is a direct scan of the counter grids and the heaps, so the merge laws pin
//! the merge against the layers it claims to rebuild.
//!
//! The membership laws give every pyramid a heap as wide as the key domain,
//! so no layer can evict: without that, a key written to layer `i` may be
//! missing from layer `i`'s heap and the exact set equality would not be a
//! statement about the sampling hierarchy at all.

use crate::support::{PROBE_KEYS, close, grid};
use asap_sketchlib::{
    BOTTOM_LAYER_FINDER, DataInput, HHItem, HeapItem, L2HH, UnivMon, bottom_layer_for_hash,
    hash64_seeded, hash128_seeded, l2hh_cell_for_row,
};
use proptest::prelude::*;
use std::collections::{HashMap, HashSet};

/// Key domain of the membership and merge laws. Wide enough that layers 0
/// through about 4 all receive keys, narrow enough that a heap of this
/// capacity never evicts.
const DOMAIN: u64 = 48;

/// Heap capacity that holds the whole key domain.
const WIDE_HEAP: usize = DOMAIN as usize;

fn key_of(item: &HHItem) -> u64 {
    match item.key {
        HeapItem::U64(k) => k,
        ref other => panic!("heap holds a non-u64 key {other:?}"),
    }
}

/// Deepest layer `key` reaches, as the count of set bits above bit 0 of its
/// finder hash, capped by the pyramid depth.
fn deepest_layer(key: u64, layer_size: usize) -> usize {
    let hash = hash64_seeded(BOTTOM_LAYER_FINDER, &DataInput::U64(key));
    ((hash >> 1).trailing_ones() as usize).min(layer_size - 1)
}

/// The keys each layer's heap holds, layer by layer.
fn layer_keys(sketch: &UnivMon) -> Vec<HashSet<u64>> {
    (0..sketch.layer_size)
        .map(|i| sketch.hh_layers[i].heap().iter().map(key_of).collect())
        .collect()
}

/// One layer's counter grid in row-major order.
fn cells(sketch: &UnivMon, layer: usize) -> Vec<i64> {
    let L2HH::COUNT(cs) = &sketch.l2_sketch_layers[layer];
    grid(cs.rows(), cs.cols(), |r, c| {
        cs.as_storage().query_one_counter(r, c)
    })
}

fn estimates(sketch: &UnivMon) -> Vec<Vec<f64>> {
    (0..sketch.layer_size)
        .map(|i| {
            (0..PROBE_KEYS as u64)
                .map(|k| sketch.l2_sketch_layers[i].estimate(&DataInput::U64(k)))
                .collect()
        })
        .collect()
}

fn heap_entries(sketch: &UnivMon) -> Vec<Vec<(u64, i64)>> {
    (0..sketch.layer_size)
        .map(|i| {
            let mut entries: Vec<(u64, i64)> = sketch.hh_layers[i]
                .heap()
                .iter()
                .map(|item| (key_of(item), item.count))
                .collect();
            entries.sort_unstable();
            entries
        })
        .collect()
}

fn filled(layers: usize, rows: usize, cols: usize, stream: &[(u64, i64)]) -> UnivMon {
    let mut sketch = UnivMon::init_univmon(WIDE_HEAP, rows, cols, layers);
    for (k, w) in stream {
        sketch.insert(&DataInput::U64(*k), *w);
    }
    sketch
}

/// Exact per-key totals of a stream.
fn truth_of(stream: &[(u64, i64)]) -> HashMap<u64, i64> {
    let mut counts: HashMap<u64, i64> = HashMap::new();
    for (k, w) in stream {
        *counts.entry(*k).or_default() += *w;
    }
    counts
}

/// A pyramid deep enough for several layers to receive keys, over grids
/// narrow enough that counters collide.
fn shape() -> impl Strategy<Value = (usize, usize, usize)> {
    (
        3usize..=8,
        1usize..=4,
        prop_oneof![Just(32usize), Just(128)],
    )
}

fn stream(max: usize) -> impl Strategy<Value = Vec<(u64, i64)>> {
    prop::collection::vec((0..DOMAIN, 1i64..64), 40..max)
}

proptest! {
    // ===== Which layers an insert reaches =====

    #[test]
    fn every_layer_holds_exactly_the_keys_whose_deepest_layer_reaches_it(
        (layers, rows, cols) in shape(),
        stream in stream(200),
    ) {
        let sketch = filled(layers, rows, cols, &stream);
        let held = layer_keys(&sketch);
        let arrived: HashSet<u64> = stream.iter().map(|&(k, _)| k).collect();

        for (i, holding) in held.iter().enumerate() {
            let expected: HashSet<u64> = arrived
                .iter()
                .copied()
                .filter(|k| deepest_layer(*k, layers) >= i)
                .collect();
            prop_assert_eq!(
                holding,
                &expected,
                "layer {} of {} holds {:?}, the hash sends it {:?}",
                i,
                layers,
                holding,
                expected
            );
        }
    }

    #[test]
    fn the_deepest_layer_a_key_reaches_is_the_trailing_ones_of_its_finder_hash(
        layers in 1usize..=8,
        key in any::<u64>(),
    ) {
        let hash = hash64_seeded(BOTTOM_LAYER_FINDER, &DataInput::U64(key));

        prop_assert_eq!(
            bottom_layer_for_hash(hash, layers),
            deepest_layer(key, layers),
            "key {} at hash {:#x} in a {}-layer pyramid",
            key,
            hash,
            layers
        );
    }

    // ===== Nesting =====

    #[test]
    fn each_layers_key_set_contains_the_next_layers(
        (layers, rows, cols) in shape(),
        stream in stream(200),
    ) {
        let held = layer_keys(&filled(layers, rows, cols, &stream));

        for i in 0..layers - 1 {
            let escaped: Vec<u64> = held[i + 1].difference(&held[i]).copied().collect();
            prop_assert!(
                escaped.is_empty(),
                "layer {} holds {:?}, which layer {} does not",
                i + 1,
                escaped,
                i
            );
        }
    }

    // ===== One key =====
    //
    // A stream over a single key has no collisions to estimate through and no
    // sibling terms in the g-sum recurrence, so every metric is exact: the
    // alternating correction at the layers above the key's own cancels the
    // doubling exactly.

    #[test]
    fn a_stream_over_one_key_reports_that_keys_weight_exactly(
        layers in 1usize..=8,
        rows in 1usize..=4,
        cols in prop_oneof![Just(32usize), Just(128)],
        key in any::<u64>(),
        weights in prop::collection::vec(1i64..1_000, 1..20),
    ) {
        let mut sketch = UnivMon::init_univmon(8, rows, cols, layers);
        for w in &weights {
            sketch.insert(&DataInput::U64(key), *w);
        }
        let total: i64 = weights.iter().sum();
        let at = deepest_layer(key, layers);

        prop_assert_eq!(sketch.calc_l1(), total as f64, "key {} at layer {}", key, at);
        prop_assert!(
            close(sketch.calc_l2(), total as f64),
            "key {} at layer {} of {}: l2 {} is not its weight {}",
            key, at, layers, sketch.calc_l2(), total
        );
        prop_assert!(
            close(sketch.calc_card(), 1.0),
            "key {} at layer {} of {}: cardinality {} is not 1",
            key, at, layers, sketch.calc_card()
        );
        prop_assert!(
            close(sketch.calc_entropy(), 0.0),
            "key {} at layer {} of {}: entropy {} is not 0",
            key, at, layers, sketch.calc_entropy()
        );
    }

    // ===== Merge =====

    #[test]
    fn merge_adds_each_layers_counters_and_leaves_the_other_layers_alone(
        (layers, rows, cols) in shape(),
        left_stream in stream(120),
        right_stream in stream(120),
    ) {
        let left = filled(layers, rows, cols, &left_stream);
        let right = filled(layers, rows, cols, &right_stream);
        let mut merged = left.clone();
        merged.merge(&right);

        for i in 0..layers {
            let expected: Vec<i64> = cells(&left, i)
                .iter()
                .zip(cells(&right, i))
                .map(|(a, b)| a + b)
                .collect();
            prop_assert_eq!(cells(&merged, i), expected, "layer {} of {}", i, layers);
        }
        prop_assert_eq!(
            merged.calc_l1(),
            left.calc_l1() + right.calc_l1(),
            "{} layers",
            layers
        );
    }

    #[test]
    fn merge_rebuilds_each_layers_heap_from_both_sides_keys_and_the_merged_counters(
        (layers, rows, cols) in shape(),
        left_stream in stream(120),
        right_stream in stream(120),
    ) {
        let left = filled(layers, rows, cols, &left_stream);
        let right = filled(layers, rows, cols, &right_stream);
        let mut merged = left.clone();
        merged.merge(&right);

        let before = (layer_keys(&left), layer_keys(&right));
        let after = layer_keys(&merged);
        for (i, merged_keys) in after.iter().enumerate() {
            let union: HashSet<u64> = before.0[i].union(&before.1[i]).copied().collect();
            prop_assert_eq!(
                merged_keys,
                &union,
                "layer {} of {} holds {:?}, the two sides held {:?}",
                i,
                layers,
                after[i],
                union
            );

            for item in merged.hh_layers[i].heap() {
                let k = key_of(item);
                let fresh = merged.l2_sketch_layers[i].estimate(&DataInput::U64(k)) as i64;
                prop_assert_eq!(
                    item.count,
                    fresh,
                    "layer {}: key {} is scored {} against the merged counters' {}",
                    i,
                    k,
                    item.count,
                    fresh
                );
            }
        }
    }

    #[test]
    fn merging_two_streams_lands_where_streaming_their_concatenation_does(
        (layers, rows, cols) in shape(),
        left_stream in stream(120),
        right_stream in stream(120),
    ) {
        let mut merged = filled(layers, rows, cols, &left_stream);
        merged.merge(&filled(layers, rows, cols, &right_stream));

        let both: Vec<(u64, i64)> = left_stream
            .iter()
            .chain(right_stream.iter())
            .copied()
            .collect();
        let streamed = filled(layers, rows, cols, &both);

        for i in 0..layers {
            prop_assert_eq!(cells(&merged, i), cells(&streamed, i), "layer {}", i);
        }
        prop_assert_eq!(layer_keys(&merged), layer_keys(&streamed), "{} layers", layers);
        prop_assert_eq!(merged.calc_l1(), streamed.calc_l1(), "{} layers", layers);
    }

    #[test]
    fn merge_is_commutative_on_every_layers_counters(
        (layers, rows, cols) in shape(),
        left_stream in stream(120),
        right_stream in stream(120),
    ) {
        let left = filled(layers, rows, cols, &left_stream);
        let right = filled(layers, rows, cols, &right_stream);
        let mut forward = left.clone();
        forward.merge(&right);
        let mut backward = right.clone();
        backward.merge(&left);

        for i in 0..layers {
            prop_assert_eq!(cells(&forward, i), cells(&backward, i), "layer {}", i);
        }
        prop_assert_eq!(layer_keys(&forward), layer_keys(&backward), "{} layers", layers);
    }

    // ===== Every counter, against the stream =====
    //
    // Layer `i`'s grid holds the signed mass of exactly the keys whose
    // deepest layer reaches `i`, placed by that layer's own seed index. The
    // oracle sums the generated stream into the cells directly, so it pins
    // the routing, the weight and the sign at once.

    #[test]
    fn each_layers_counters_carry_the_signed_mass_of_the_keys_that_reach_it(
        (layers, rows, cols) in shape(),
        stream in stream(200),
    ) {
        let sketch = filled(layers, rows, cols, &stream);
        let truth = truth_of(&stream);
        let mask_bits = cols.next_power_of_two().ilog2();

        for i in 0..layers {
            let mut expected = vec![0i64; rows * cols];
            for (k, mass) in &truth {
                if deepest_layer(*k, layers) < i {
                    continue;
                }
                let hashed = hash128_seeded(i, &DataInput::U64(*k));
                for r in 0..rows {
                    let (col, sign) = l2hh_cell_for_row(hashed, r, cols, mask_bits);
                    expected[r * cols + col] += sign * mass;
                }
            }

            prop_assert_eq!(
                cells(&sketch, i),
                expected,
                "layer {} of {}, a {}x{} grid over {} distinct keys",
                i,
                layers,
                rows,
                cols,
                truth.len()
            );
        }
    }

    // ===== Wire =====
    //
    // P-25 lists UnivMon among the envelopes without a property-level round
    // trip, on the grounds that a pyramid is expensive to build. At the
    // geometries below it is not: the default 8 x 5 x 2048 shape is a
    // construction default, not a constraint the envelope imposes.

    #[test]
    fn univmon_round_trips_at_small_pyramids(
        (layers, rows, cols) in shape(),
        heap_size in 1usize..=WIDE_HEAP,
        stream in stream(120),
    ) {
        let mut sketch = UnivMon::init_univmon(heap_size, rows, cols, layers);
        for (k, w) in &stream {
            sketch.insert(&DataInput::U64(*k), *w);
        }

        round_trip!(
            UnivMon,
            sketch,
            estimates,
            heap_entries,
            |s: &UnivMon| s.calc_l1(),
            |s: &UnivMon| s.candidates_complete().to_vec(),
            |s: &UnivMon| (s.layer_size, s.sketch_row, s.sketch_col, s.heap_size),
        );
    }
}
