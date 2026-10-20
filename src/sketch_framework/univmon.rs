//! UnivMon (Universal Monitoring) Implementation
//!
//! This module provides an implementation of the UnivMon algorithm as described in:
//! "Universal Sketches for the Next Generation of Real-time Network Data Analytics"
//! by Liu et al. (ACM SIGCOMM 2016).
//! <https://dl.acm.org/doi/10.1145/2934872.2934906>
//!
//! UnivMon is a universal sketch framework that enables the estimation of multiple
//! network flow metrics—such as L1/L2 norms, Shannon entropy, and cardinality—using
//! a single, hierarchical sampling structure.
//!
//! # Architecture
//! The implementation consists of a "Sketch Pyramid" where:
//! * Each layer $i$ samples a subset of the stream from layer $i-1$ with probability 1/2.
//! * Each layer maintains a functional sketch (e.g., Count-Min Sketch) for frequency estimation.
//! * Each layer maintains a Heavy Hitter heap to track the most frequent elements at that sampling level.
//!
//! # Capabilities
//! * **L1/L2 Norm**: Estimation of total flow volume and second moments.
//! * **Entropy**: Calculation of flow distribution complexity.
//! * **Cardinality**: Estimation of the number of distinct elements.
//! * **Heavy Hitters**: Tracking top flows across different sampling granularities.
//!
//! This implementation is part of the `asap_sketchlib` library.

use crate::common::heap::TopKHeap;
use crate::common::{
    DataInput, OwnedDataInput, SEED_LIST, UNIVMON_BOTTOM_LAYER_SEED_INDEX, hash64_seeded,
    hash64_seeded_owned, heap_item_to_sketch_input,
};
use crate::common::{L2HH, Vector1D};
use crate::octo_delta::UnivMonCellDelta;
use crate::sketches::countsketch_heap::CountSketchWithL2Norm;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub(crate) mod wire;

const DEFAULT_ROWS: usize = 5;
const DEFAULT_COLS: usize = 2048;
const DEFAULT_HEAP_SIZE: usize = 32;
const DEFAULT_LAYERS: usize = 8;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum UnivMonUpdateMode {
    #[default]
    Unset,
    Standard,
    Terminal,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
/// UnivMon sketch pyramid for multi-metric stream estimation.
pub struct UnivMon {
    /// Per-layer L2/heavy-hitter sketches.
    pub layer_sketches: Vector1D<L2HH>,
    /// Per-layer heavy-hitter heaps.
    pub layer_heaps: Vector1D<TopKHeap>,
    /// Number of pyramid layers.
    pub layers: usize,
    /// Row count of each underlying sketch.
    pub rows: usize,
    /// Column count of each underlying sketch.
    pub cols: usize,
    /// Heap capacity per layer.
    pub heap_size: usize,
    /// Bucket size used for hashing decisions.
    pub total_weight: usize,
    #[serde(default)]
    update_mode: UnivMonUpdateMode,
    #[serde(default)]
    candidate_complete: Vec<bool>,
}

impl Default for UnivMon {
    fn default() -> Self {
        UnivMon::new(
            DEFAULT_HEAP_SIZE,
            DEFAULT_ROWS,
            DEFAULT_COLS,
            DEFAULT_LAYERS,
        )
    }
}

/// Layers one pyramid may hold.
///
/// Layer `i`'s Count Sketch hashes at seed index `i`, and the layer a key
/// reaches is read from the hash at seed index [`UNIVMON_BOTTOM_LAYER_SEED_INDEX`]. The
/// error bounds need every layer's hash independent of the bits that select
/// its keys and of the other layers' hashes, so no layer may hash at the
/// finder's seed, and none past the seed list, where indices wrap onto layer
/// 0's. Layers `0..UNIVMON_BOTTOM_LAYER_SEED_INDEX` are exactly the distinct seeds below
/// the finder. (The finder's bits are read by shifting a 64-bit hash right by
/// up to `layers - 1`, which this cap also keeps in range.)
pub const UNIVMON_MAX_LAYERS: usize = UNIVMON_BOTTOM_LAYER_SEED_INDEX;

const _: () = assert!(UNIVMON_BOTTOM_LAYER_SEED_INDEX < SEED_LIST.len() && UNIVMON_MAX_LAYERS < 64);

/// How much of the stream reached an aggregator being fed `UnivMonCellDelta`s.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnivMonDeltaFidelity {
    /// Every insert was promoted, so candidate sets can still be complete.
    /// Only true at a promotion threshold of 1.
    EveryInsert,
    /// Only counters that crossed the threshold arrived; candidate sets are
    /// partial by construction.
    PromotedOnly,
}

impl UnivMon {
    /// Deepest pyramid layer the given key hash reaches.
    #[inline(always)]
    pub fn bottom_layer_for_hash(hash: u64, layers: usize) -> usize {
        for l in 1..layers {
            if ((hash >> l) & 1) == 0 {
                return l - 1;
            }
        }
        layers - 1
    }

    /// Whether this state accepts standard updates and standard-state merges.
    /// Empty states have not selected an update strategy yet.
    pub fn accepts_standard_updates(&self) -> bool {
        matches!(
            self.update_mode,
            UnivMonUpdateMode::Unset | UnivMonUpdateMode::Standard
        )
    }
    /// Creates a UnivMon instance with explicit dimensions.
    pub fn new(heap_size: usize, rows: usize, cols: usize, layers: usize) -> Self {
        assert!(heap_size > 0, "heap size must be positive");
        assert!(rows > 0, "sketch row count must be positive");
        assert!(cols > 0, "sketch column count must be positive");
        assert!(layers > 0, "layer count must be positive");
        assert!(
            layers <= UNIVMON_MAX_LAYERS,
            "layer count must be at most MAX_LAYER_SIZE {UNIVMON_MAX_LAYERS}"
        );
        let sk_vec: Vec<L2HH> = (0..layers)
            .map(|i| {
                L2HH::COUNT(CountSketchWithL2Norm::with_dimensions_and_seed_index(
                    rows, cols, i,
                ))
            })
            .collect();

        let hh_vec: Vec<TopKHeap> = (0..layers).map(|_| TopKHeap::new(heap_size)).collect();

        UnivMon {
            layer_sketches: Vector1D::from_vec(sk_vec),
            layer_heaps: Vector1D::from_vec(hh_vec),
            layers,
            rows,
            cols,
            heap_size,
            total_weight: 0,
            update_mode: UnivMonUpdateMode::Unset,
            candidate_complete: vec![true; layers],
        }
    }

    #[inline]
    fn begin_update(&mut self, value: i64, mode: UnivMonUpdateMode) {
        assert!(value >= 0, "UnivMon only supports non-negative updates");
        match self.update_mode {
            UnivMonUpdateMode::Unset => self.update_mode = mode,
            current if current == mode => {}
            _ => panic!("cannot mix standard and terminal-only updates in one UnivMon"),
        }
        self.total_weight = self
            .total_weight
            .checked_add(value as usize)
            .expect("UnivMon total weight overflowed usize");
    }

    #[inline(always)]
    fn find_bottom_layer_num(&self, hash: u64, layer: usize) -> usize {
        UnivMon::bottom_layer_for_hash(hash, layer)
    }

    /// Deepest pyramid layer an insert of `key` reaches; it touches `0..=this`.
    ///
    /// A pure function of the key's hash, so an OctoSketch worker picks exactly
    /// the same layers as a single-threaded insert would.
    pub fn bottom_layer_for(&self, key: &DataInput) -> usize {
        UnivMon::bottom_layer_for_hash(
            hash64_seeded(UNIVMON_BOTTOM_LAYER_SEED_INDEX, key),
            self.layers,
        )
    }

    /// Whether each layer's heavy-hitter heap still holds every key that layer
    /// received. Queries widen their threshold on the layers where it does not.
    pub fn candidates_complete(&self) -> &[bool] {
        &self.candidate_complete
    }

    /// Marks every layer's candidate set as partial.
    ///
    /// An OctoSketch aggregator calls this up front whenever its workers
    /// promote above a threshold of 1: a layer that received traffic but never
    /// promoted any of it sends the aggregator nothing at all, so waiting for a
    /// delta to arrive before lowering the flag would leave exactly those
    /// layers claiming a completeness they cannot have.
    pub fn mark_candidates_incomplete(&mut self) {
        self.candidate_complete.fill(false);
    }

    /// Marks one layer's candidate set as partial.
    pub fn mark_layer_candidates_incomplete(&mut self, layer: usize) {
        self.candidate_complete[layer] = false;
    }

    /// Overwrites the total weight the sketch believes it has seen.
    ///
    /// An OctoSketch aggregator never observes the raw stream, so it restores
    /// this from the running totals its workers report. This *assigns* where
    /// `update_weighted` and `merge` accumulate, so a sketch fed by deltas must not also
    /// be inserted into or merged with: the next delta would erase whatever
    /// those added. Feeding one exclusively through `apply_delta` and
    /// this method is the supported arrangement.
    pub fn set_total_weight(&mut self, weight: usize) {
        debug_assert_ne!(
            self.update_mode,
            UnivMonUpdateMode::Terminal,
            "a terminal-mode UnivMon cannot be fed by the delta path"
        );
        self.total_weight = weight;
        if self.update_mode == UnivMonUpdateMode::Unset {
            self.update_mode = UnivMonUpdateMode::Standard;
        }
    }

    /// Applies one delta promoted by an OctoSketch worker.
    ///
    /// Mirrors `update` for a single layer: the counter lands, the layer's
    /// estimate is re-read, and the layer's heavy-hitter heap follows - which
    /// is Algorithm 2 of the OctoSketch paper.
    pub fn apply_delta(&mut self, delta: &UnivMonCellDelta, fidelity: UnivMonDeltaFidelity) {
        if self.update_mode == UnivMonUpdateMode::Unset {
            self.update_mode = UnivMonUpdateMode::Standard;
        }
        let layer = delta.layer as usize;
        assert!(
            layer < self.layers,
            "delta names layer {layer} but this pyramid has {} layers",
            self.layers
        );

        self.layer_sketches[layer].apply_delta(delta.cell);
        let key = heap_item_to_sketch_input(&delta.key);
        let count = self.layer_sketches[layer].estimate(&key);
        let heap_kept_everything = self.layer_heaps[layer].update(&key, count as i64);

        // A worker holds back every counter that has not reached the promotion
        // threshold, so unless the threshold is 1 the aggregator has provably
        // not seen every key this layer received and its candidate set cannot
        // be called complete. Claiming otherwise would send `heavy_threshold`
        // down the permissive branch and overcount.
        let complete = heap_kept_everything && fidelity == UnivMonDeltaFidelity::EveryInsert;
        if !complete {
            self.candidate_complete[layer] = false;
        }
    }

    #[inline(always)]
    fn update(&mut self, key: &DataInput, value: i64, bottom_layer_num: usize) {
        for i in 0..=bottom_layer_num {
            let count = self.layer_sketches[i].update_and_est(key, value);
            if !self.layer_heaps[i].update(key, count as i64) {
                self.candidate_complete[i] = false;
            }
        }
    }

    #[inline(always)]
    fn process_univmon(&mut self, key: &DataInput, value: i64, bottom_layer_num: usize) {
        self.update(key, value, bottom_layer_num);
    }

    /// Inserts one weighted update.
    pub fn update_weighted(&mut self, key: &DataInput, weight: i64) {
        self.begin_update(weight, UnivMonUpdateMode::Standard);
        let h = hash64_seeded(UNIVMON_BOTTOM_LAYER_SEED_INDEX, key);
        let bottom_layer_num = self.find_bottom_layer_num(h, self.layers);
        self.process_univmon(key, weight, bottom_layer_num)
    }

    /// Inserts one weighted update into its terminal stratum only.
    ///
    /// This is the Joltik update-last-layer construction. Logical sampled
    /// streams and their candidate sets are reconstructed during queries.
    /// Do not mix this method with [`Self::update`] on the same sketch.
    pub fn update_weighted_terminal(&mut self, key: &DataInput, weight: i64) {
        self.begin_update(weight, UnivMonUpdateMode::Terminal);
        let h = hash64_seeded(UNIVMON_BOTTOM_LAYER_SEED_INDEX, key);
        let bottom_layer_num = self.find_bottom_layer_num(h, self.layers);
        let count = self.layer_sketches[bottom_layer_num].update_and_est(key, weight);
        if !self.layer_heaps[bottom_layer_num].update(key, count as i64) {
            self.candidate_complete[bottom_layer_num] = false;
        }
    }

    /// Prints all heavy-hitter layers for debugging.
    pub fn print_layer_heaps(&self) {
        print!("Print HH_Layer: ");
        for i in 0..self.layers {
            println!("layer {i}: ");
            self.layer_heaps[i].print_heap();
        }
    }

    /// Computes a g-sum estimate using the heuristic recurrence.
    pub fn calc_g_sum_heuristic<F>(&self, g: F, heavy_only: bool) -> f64
    where
        F: Fn(f64) -> f64,
    {
        if self.total_weight == 0 {
            return 0.0;
        }
        if self.update_mode == UnivMonUpdateMode::Terminal {
            return self.calc_terminal_g_sum(g);
        }

        let mut y = vec![0.0; self.layers];
        let mut tmp: f64;

        let l2_value = self.layer_sketches[self.layers - 1].l2();
        let threshold = if heavy_only {
            self.heavy_threshold(l2_value, self.candidate_complete[self.layers - 1])
        } else {
            0
        };

        tmp = 0.0;
        for item in self.layer_heaps[self.layers - 1].heap() {
            let input = heap_item_to_sketch_input(&item.key);
            let count = self.layer_sketches[self.layers - 1].estimate(&input) as i64;
            if count > threshold {
                tmp += g(count as f64);
            }
        }
        y[self.layers - 1] = tmp;

        for i in (0..(self.layers - 1)).rev() {
            tmp = 0.0;
            let l2_value = self.layer_sketches[i].l2();
            let threshold = if heavy_only {
                self.heavy_threshold(l2_value, self.candidate_complete[i])
            } else {
                0
            };

            for item in self.layer_heaps[i].heap() {
                let input = heap_item_to_sketch_input(&item.key);
                let count = self.layer_sketches[i].estimate(&input) as i64;
                if count > threshold {
                    let hash = (hash64_seeded_owned(UNIVMON_BOTTOM_LAYER_SEED_INDEX, &item.key)
                        >> (i + 1))
                        & 1;
                    let coe = 1.0 - 2.0 * (hash as f64);
                    tmp += coe * g(count as f64);
                }
            }
            y[i] = 2.0 * y[i + 1] + tmp;
        }

        y[0]
    }

    fn calc_terminal_g_sum<F>(&self, g: F) -> f64
    where
        F: Fn(f64) -> f64,
    {
        let (candidates, complete) = self.logical_terminal_candidates();
        let mut logical_l2 = vec![0.0; self.layers];
        let mut suffix_l2_squared = 0.0;
        for level in (0..self.layers).rev() {
            let terminal_l2 = self.layer_sketches[level].l2();
            suffix_l2_squared += terminal_l2 * terminal_l2;
            logical_l2[level] = suffix_l2_squared.sqrt();
        }

        let mut y = vec![0.0; self.layers];
        let last = self.layers - 1;
        let threshold = self.heavy_threshold(logical_l2[last], complete[last]);
        y[last] = candidates[last]
            .iter()
            .filter(|(_, count)| *count > threshold)
            .map(|(_, count)| g(*count as f64))
            .sum();

        for level in (0..last).rev() {
            let threshold = self.heavy_threshold(logical_l2[level], complete[level]);
            let correction = candidates[level]
                .iter()
                .filter(|(_, count)| *count > threshold)
                .map(|(key, count)| {
                    let hash = (hash64_seeded_owned(UNIVMON_BOTTOM_LAYER_SEED_INDEX, key)
                        >> (level + 1))
                        & 1;
                    (1.0 - 2.0 * hash as f64) * g(*count as f64)
                })
                .sum::<f64>();
            y[level] = 2.0 * y[level + 1] + correction;
        }
        y[0]
    }

    /// Reconstructs the logical sampled-stream candidate sets from disjoint
    /// terminal strata, as required by the Joltik update-last-layer scheme.
    fn logical_terminal_candidates(&self) -> (Vec<Vec<(OwnedDataInput, i64)>>, Vec<bool>) {
        let mut logical = vec![Vec::new(); self.layers];
        let mut complete = vec![false; self.layers];
        let mut cumulative = HashMap::<OwnedDataInput, i64>::with_capacity(self.heap_size * 2);
        let mut suffix_complete = true;
        for level in (0..self.layers).rev() {
            suffix_complete &= self.candidate_complete[level];
            for item in self.layer_heaps[level].heap() {
                let input = heap_item_to_sketch_input(&item.key);
                let count = self.layer_sketches[level].estimate(&input) as i64;
                cumulative
                    .entry(item.key.clone())
                    .and_modify(|old| *old = (*old).max(count))
                    .or_insert(count);
            }
            let mut retained: Vec<_> = cumulative
                .iter()
                .map(|(key, count)| (key.clone(), *count))
                .collect();
            retained.sort_unstable_by(|left, right| {
                right.1.cmp(&left.1).then_with(|| {
                    hash64_seeded_owned(UNIVMON_BOTTOM_LAYER_SEED_INDEX, &left.0).cmp(
                        &hash64_seeded_owned(UNIVMON_BOTTOM_LAYER_SEED_INDEX, &right.0),
                    )
                })
            });
            complete[level] = suffix_complete && retained.len() <= self.heap_size;
            retained.truncate(self.heap_size);
            cumulative = retained.iter().cloned().collect();
            logical[level] = retained;
        }
        (logical, complete)
    }

    #[inline]
    fn heavy_threshold(&self, l2: f64, complete: bool) -> i64 {
        if complete {
            0
        } else {
            (l2 / (self.heap_size as f64).sqrt()) as i64
        }
    }

    /// Computes a g-sum estimate.
    pub fn estimate_g_sum<F>(&self, g: F, heavy_only: bool) -> f64
    where
        F: Fn(f64) -> f64,
    {
        self.calc_g_sum_heuristic(g, heavy_only)
    }

    /// Returns the exact L1 norm for the supported non-negative update stream.
    pub fn estimate_l1(&self) -> f64 {
        self.total_weight as f64
    }

    /// Returns the estimated L2 norm.
    pub fn estimate_l2(&self) -> f64 {
        let tmp = self.estimate_g_sum(|x| x * x, false);
        tmp.sqrt()
    }

    /// Returns the estimated entropy.
    pub fn estimate_entropy(&self) -> f64 {
        if self.total_weight == 0 {
            return 0.0;
        }
        let tmp = self.estimate_g_sum(
            |x| {
                if x > 0.0 { x * x.log2() } else { 0.0 }
            },
            false,
        );
        (self.total_weight as f64).log2() - tmp / (self.total_weight as f64)
    }

    /// Returns the estimated cardinality.
    pub fn estimate_cardinality(&self) -> f64 {
        self.estimate_g_sum(|_| 1.0, true)
    }

    /// Resets the sketch to its initial state without reallocating.
    /// Zeroes all counters and clears all heaps, matching the Go `Free()` method.
    pub fn clear(&mut self) {
        self.total_weight = 0;
        self.update_mode = UnivMonUpdateMode::Unset;
        self.candidate_complete.fill(true);
        for i in 0..self.layers {
            self.layer_sketches[i].clear();
            self.layer_heaps[i].clear();
        }
    }

    /// Merges another compatible UnivMon into this one.
    ///
    /// Both sketches must use the same dimensions and update strategy.
    /// Counters are merged first, then candidate frequencies and cached L2
    /// values are rebuilt from the combined counter state.
    pub fn merge(&mut self, other: &UnivMon) {
        assert_eq!(
            self.layers, other.layers,
            "layer size should be equal to merge"
        );
        assert_eq!(
            (self.rows, self.cols, self.heap_size),
            (other.rows, other.cols, other.heap_size),
            "UnivMon dimensions and heap size must match for merge"
        );
        match (self.update_mode, other.update_mode) {
            (UnivMonUpdateMode::Unset, mode) => self.update_mode = mode,
            (_, UnivMonUpdateMode::Unset) => {}
            (left, right) => assert_eq!(
                left, right,
                "cannot merge standard and terminal-only UnivMon states"
            ),
        }
        self.total_weight = self
            .total_weight
            .checked_add(other.total_weight)
            .expect("merged UnivMon total weight overflowed usize");
        for i in 0..self.layers {
            let sources_complete = self.candidate_complete[i] && other.candidate_complete[i];
            let candidate_keys: HashSet<OwnedDataInput> = self.layer_heaps[i]
                .heap()
                .iter()
                .chain(other.layer_heaps[i].heap())
                .map(|item| item.key.clone())
                .collect();
            let merged_candidates_complete =
                sources_complete && candidate_keys.len() <= self.heap_size;
            self.layer_sketches[i].merge(&other.layer_sketches[i]);
            self.layer_heaps[i].clear();
            for key in candidate_keys {
                let input = heap_item_to_sketch_input(&key);
                let count = self.layer_sketches[i].estimate(&input) as i64;
                self.layer_heaps[i].update(&input, count);
            }
            self.candidate_complete[i] = merged_candidates_complete;
        }
    }

    /// Returns the heap for one layer.
    pub fn layer_heap_mut(&mut self, layer: usize) -> &mut TopKHeap {
        &mut self.layer_heaps[layer]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataInput, OwnedDataInput};
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use std::collections::HashMap;

    /// Every layer hashes at its own seed, inside the seed list (no wrap onto
    /// layer 0) and never at the seed that picks the layers.
    #[test]
    fn layer_seeds_are_distinct_and_never_the_layer_finder() {
        use crate::common::SEED_LIST;
        let deepest = UnivMon::new(4, 2, 16, UNIVMON_MAX_LAYERS);
        let seeds: Vec<usize> = (0..UNIVMON_MAX_LAYERS)
            .map(|layer| {
                let L2HH::COUNT(counter) = &deepest.layer_sketches[layer];
                counter.seed_index()
            })
            .collect();
        for &seed in &seeds {
            assert!(seed < SEED_LIST.len(), "seed index {seed} wraps");
            assert_ne!(
                seed, UNIVMON_BOTTOM_LAYER_SEED_INDEX,
                "a layer hashes at the finder seed"
            );
        }
        let distinct: HashSet<usize> = seeds.iter().copied().collect();
        assert_eq!(distinct.len(), seeds.len(), "two layers share a seed");
    }

    /// Layer `UNIVMON_BOTTOM_LAYER_SEED_INDEX` would hash at the finder's own seed.
    #[test]
    #[should_panic(expected = "MAX_LAYER_SIZE")]
    fn init_univmon_rejects_a_layer_on_the_finder_seed() {
        UnivMon::new(4, 2, 16, UNIVMON_BOTTOM_LAYER_SEED_INDEX + 1);
    }

    #[test]
    #[should_panic(expected = "hash bits")]
    fn init_univmon_rejects_column_bits_that_reach_the_sign_bits() {
        UnivMon::new(4, 11, 2048, 2);
    }

    /// Persisted terminal states must be distinguishable before standard ingestion.
    #[test]
    fn standard_update_compatibility_survives_roundtrip_and_reset() {
        let mut sketch = UnivMon::new(4, 3, 16, 2);
        assert!(sketch.accepts_standard_updates());
        sketch.update_weighted(&DataInput::U64(1), 1);
        assert!(sketch.accepts_standard_updates());
        let standard = UnivMon::from_asapv1(&sketch.to_asapv1().unwrap()).unwrap();
        assert!(standard.accepts_standard_updates());
        sketch.clear();
        sketch.update_weighted_terminal(&DataInput::U64(1), 1);
        let mut terminal = UnivMon::from_asapv1(&sketch.to_asapv1().unwrap()).unwrap();
        assert!(!terminal.accepts_standard_updates());
        terminal.clear();
        assert!(terminal.accepts_standard_updates());
    }

    #[test]
    fn univmon_round_trip_serialization() {
        let mut um = UnivMon::new(12, 3, 64, 4);
        let flows = [
            ("alpha", 5),
            ("beta", 7),
            ("gamma", 9),
            ("alpha", 3),
            ("delta", 11),
        ];

        for (key, count) in flows {
            um.update_weighted(&DataInput::String(key.to_string()), count);
        }

        let bucket_size_before = um.total_weight;
        let l1_before = um.estimate_l1();
        let l2_before = um.estimate_l2();
        let entropy_before = um.estimate_entropy();
        let card_before = um.estimate_cardinality();

        let encoded = um.to_asapv1().expect("serialize UnivMon into MessagePack");
        assert!(!encoded.is_empty(), "serialized bytes should not be empty");
        let data = encoded.clone();

        let decoded = UnivMon::from_asapv1(&data).expect("deserialize UnivMon from MessagePack");

        assert_eq!(um.layers, decoded.layers);
        assert_eq!(um.rows, decoded.rows);
        assert_eq!(um.cols, decoded.cols);
        assert_eq!(um.heap_size, decoded.heap_size);
        assert_eq!(bucket_size_before, decoded.total_weight);
        assert!(
            (decoded.estimate_l1() - l1_before).abs() < 1e-6,
            "L1 changed after round trip"
        );
        assert!(
            (decoded.estimate_l2() - l2_before).abs() < 1e-6,
            "L2 changed after round trip"
        );
        assert!(
            (decoded.estimate_entropy() - entropy_before).abs() < 1e-6,
            "entropy changed after round trip"
        );
        assert!(
            (decoded.estimate_cardinality() - card_before).abs() < f64::EPSILON,
            "cardinality changed after round trip"
        );
    }

    #[test]
    fn update_populates_bucket_size_and_heavy_hitters() {
        // processing a single hot key should record its weight in the heavy hitter layers
        let mut um = UnivMon::new(16, 3, 32, 4);
        let key = "alpha";

        for _ in 0..40 {
            um.update_weighted(&DataInput::Str(key), 1);
        }

        assert_eq!(um.total_weight, 40);

        let idx = um.layer_heaps[0]
            .find_owned(&OwnedDataInput::String(key.to_owned()))
            .expect("heavy hitter should track key");
        assert!(
            um.layer_heaps[0].heap()[idx].count >= 20,
            "expected significant count for heavy hitter, got {}",
            um.layer_heaps[0].heap()[idx].count
        );
        assert!(
            um.estimate_l1() == 40.0,
            "L1 Norm: get {}, expecting 1",
            um.estimate_l1()
        );
        assert!(
            um.estimate_cardinality() == 1.0,
            "Cardinality: get {}, expecting 1",
            um.estimate_cardinality()
        );
    }

    #[test]
    fn merge_with_combines_heavy_hitters() {
        // merging two sketches should keep contributions from both sides
        let mut left = UnivMon::new(16, 3, 32, 4);
        let mut right = UnivMon::new(16, 3, 32, 4);

        let key_left = "left";
        let key_right = "right";

        for _ in 0..25 {
            left.update_weighted(&DataInput::Str(key_left), 1);
        }
        for _ in 0..30 {
            right.update_weighted(&DataInput::Str(key_right), 1);
        }

        left.merge(&right);

        let left_heap = left.layer_heap_mut(00);
        let right_heap = right.layer_heap_mut(0);
        let idx_left = left_heap
            .find_owned(&OwnedDataInput::String(key_left.to_owned()))
            .expect("left key present");
        let idx_right_in_left = left_heap
            .find_owned(&OwnedDataInput::String(key_right.to_owned()))
            .expect("left key present");
        let idx_right = right_heap
            .find_owned(&OwnedDataInput::String(key_right.to_owned()))
            .expect("right key present");
        assert!(
            left_heap.heap()[idx_left].count == 25,
            "left in left is: {}",
            left_heap.heap()[idx_left].count
        );
        assert!(
            right_heap.heap()[idx_right].count == 30,
            "right in right is: {}",
            right_heap.heap()[idx_right].count
        );
        assert!(
            left_heap.heap()[idx_right_in_left].count == 30,
            "right in left is: {}",
            left_heap.heap()[idx_right_in_left].count
        );
    }

    #[test]
    fn merge_combines_weight_l2_and_evicted_candidate_counts() {
        let mut left = UnivMon::new(1, 3, 1024, 1);
        let mut right = UnivMon::new(1, 3, 1024, 1);
        left.update_weighted(&DataInput::Str("x"), 100);
        right.update_weighted(&DataInput::Str("x"), 5);
        right.update_weighted(&DataInput::Str("y"), 10);

        left.merge(&right);

        assert_eq!(left.total_weight, 115);
        let expected_l2 = (105.0_f64.powi(2) + 10.0_f64.powi(2)).sqrt();
        assert!((left.layer_sketches[0].l2() - expected_l2).abs() < 1e-9);
        let x = OwnedDataInput::String("x".to_owned());
        let index = left.layer_heaps[0]
            .find_owned(&x)
            .expect("merged top candidate");
        assert_eq!(left.layer_heaps[0].heap()[index].count, 105);
        assert!(left.estimate_entropy().is_finite());
        assert!(left.estimate_entropy() >= 0.0);
    }

    #[test]
    fn standard_and_terminal_merges_match_one_pass_with_complete_candidates() {
        for terminal_only in [false, true] {
            let mut one_pass = UnivMon::new(128, 5, 2048, 10);
            let mut left = UnivMon::new(128, 5, 2048, 10);
            let mut right = UnivMon::new(128, 5, 2048, 10);
            for observation in 0..2000_u64 {
                let key = DataInput::U64(observation % 64);
                if terminal_only {
                    one_pass.update_weighted_terminal(&key, 1);
                    if observation % 2 == 0 {
                        left.update_weighted_terminal(&key, 1);
                    } else {
                        right.update_weighted_terminal(&key, 1);
                    }
                } else {
                    one_pass.update_weighted(&key, 1);
                    if observation % 2 == 0 {
                        left.update_weighted(&key, 1);
                    } else {
                        right.update_weighted(&key, 1);
                    }
                }
            }

            left.merge(&right);
            assert_eq!(left.total_weight, one_pass.total_weight);
            assert!((left.estimate_l1() - one_pass.estimate_l1()).abs() < 1e-9);
            assert!((left.estimate_l2() - one_pass.estimate_l2()).abs() < 1e-9);
            assert!((left.estimate_cardinality() - one_pass.estimate_cardinality()).abs() < 1e-9);
            assert!((left.estimate_entropy() - one_pass.estimate_entropy()).abs() < 1e-9);
        }
    }

    #[test]
    fn standard_updates_l2_for_every_sampled_layer() {
        let mut sketch = UnivMon::new(16, 3, 128, 6);
        let key = (0_u64..)
            .map(DataInput::U64)
            .find(|key| {
                let hash = hash64_seeded(UNIVMON_BOTTOM_LAYER_SEED_INDEX, key);
                sketch.find_bottom_layer_num(hash, sketch.layers) >= 2
            })
            .expect("find a key sampled into multiple layers");
        let hash = hash64_seeded(UNIVMON_BOTTOM_LAYER_SEED_INDEX, &key);
        let bottom = sketch.find_bottom_layer_num(hash, sketch.layers);
        sketch.update_weighted(&key, 3);
        for level in 0..=bottom {
            assert_eq!(sketch.layer_sketches[level].l2(), 3.0);
        }
    }

    #[test]
    fn terminal_update_touches_one_physical_layer_and_reconstructs_queries() {
        let mut sketch = UnivMon::new(16, 3, 128, 6);
        let key = (0_u64..)
            .map(DataInput::U64)
            .find(|key| {
                let hash = hash64_seeded(UNIVMON_BOTTOM_LAYER_SEED_INDEX, key);
                sketch.find_bottom_layer_num(hash, sketch.layers) >= 2
            })
            .expect("find a key sampled into multiple layers");
        let hash = hash64_seeded(UNIVMON_BOTTOM_LAYER_SEED_INDEX, &key);
        let bottom = sketch.find_bottom_layer_num(hash, sketch.layers);
        sketch.update_weighted_terminal(&key, 3);

        for level in 0..sketch.layers {
            let expected = if level == bottom { 3.0 } else { 0.0 };
            assert_eq!(sketch.layer_sketches[level].l2(), expected);
        }
        assert_eq!(sketch.estimate_l1(), 3.0);
        assert_eq!(sketch.estimate_l2(), 3.0);
        assert_eq!(sketch.estimate_cardinality(), 1.0);
        assert_eq!(sketch.estimate_entropy(), 0.0);
    }

    #[test]
    fn univmon_layers_use_different_seeds() {
        // Verify that different layers in UnivMon use different seeds
        // by checking they produce different hash values
        use crate::common::hash128_seeded;

        let _um = UnivMon::new(20, 3, 1024, 4);

        // Hash the same key with different seed indices (as used by different layers)
        let test_key = DataInput::Str("test_flow");

        // Hash the same key with different seed indices (as used by different layers)
        let hash_0 = hash128_seeded(0, &test_key);
        let hash_1 = hash128_seeded(1, &test_key);
        let hash_2 = hash128_seeded(2, &test_key);
        let hash_3 = hash128_seeded(3, &test_key);

        // All should be different
        assert_ne!(hash_0, hash_1, "Layers 0 and 1 should use different seeds");
        assert_ne!(hash_0, hash_2, "Layers 0 and 2 should use different seeds");
        assert_ne!(hash_0, hash_3, "Layers 0 and 3 should use different seeds");
        assert_ne!(hash_1, hash_2, "Layers 1 and 2 should use different seeds");
        assert_ne!(hash_1, hash_3, "Layers 1 and 3 should use different seeds");
        assert_ne!(hash_2, hash_3, "Layers 2 and 3 should use different seeds");
    }

    #[test]
    fn univmon_cardinality_is_positive() {
        // Basic sanity test: cardinality should be positive after insertions
        let mut um = UnivMon::new(20, 3, 2048, 8);

        for i in 0..20 {
            let key = format!("flow_{i}");
            um.update_weighted(&DataInput::String(key), 1);
        }

        let card = um.estimate_cardinality();
        assert!(
            card == 20.0,
            "Cardinality should be positive after insertions, got {card}"
        );
    }

    #[test]
    fn univmon_bucket_size_tracked_correctly() {
        // Verify that total_weight is correctly tracked with seed configuration
        let mut um = UnivMon::new(20, 3, 1024, 6);

        let flows = [("flow_a", 100), ("flow_b", 200), ("flow_c", 150)];
        let expected_total = 450;

        for (key, count) in &flows {
            um.update_weighted(&DataInput::Str(key), *count);
        }

        assert_eq!(
            um.total_weight, expected_total,
            "Bucket size should equal sum of all counts"
        );
    }

    #[test]
    fn univmon_basic_operation() {
        let cases: Vec<(String, i64)> = vec![
            ("notfound", 1),
            ("hello", 1),
            ("count", 3),
            ("min", 4),
            ("world", 10),
            ("cheatcheat", 3),
            ("cheatcheat", 7),
            ("min", 2),
            ("hello", 2),
            ("tigger", 34),
            ("flow", 9),
            ("miss", 4),
            ("hello", 30),
            ("world", 10),
            ("hello", 10),
            ("mom", 1),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        let mut um = UnivMon::new(100, 3, 2048, 16);
        for case in cases {
            um.update_weighted(&DataInput::String(case.0), case.1);
        }

        assert_eq!(
            um.estimate_cardinality(),
            10.0,
            "Cardinality estimation incorrect"
        );
        assert_eq!(um.estimate_l1(), 131.0, "L1 estimation incorrect");
    }

    #[test]
    fn test_statistical_accuracy() {
        // 1. Setup: Larger sketch for statistical significance
        // k=50 (top-k size), r=5 (rows), c=1024 (cols), l=10 (layers)
        let mut um = UnivMon::new(50, 5, 1024, 10);

        // 2. Generate Data: A simple skewed distribution
        // 1 heavy hitter (count 1000), 10 medium (count 100), 100 noise (count 1)
        let mut true_l2_sq = 0.0;
        let mut true_entropy_term = 0.0;
        let mut total_count = 0.0;

        let scenarios = vec![("heavy", 1000, 1), ("medium", 100, 10), ("noise", 1, 100)];

        for (prefix, count, repeat) in scenarios {
            for i in 0..repeat {
                let key = format!("{prefix}_{i}");
                let val = count as i64;
                let val_f = val as f64;

                // Ground Truth Calculation
                true_l2_sq += val_f * val_f;
                true_entropy_term += val_f * val_f.log2();
                total_count += val_f;

                // Update Sketch
                um.update_weighted(&DataInput::String(key), val);
            }
        }

        // 3. Calculate True Metrics
        let true_l2 = true_l2_sq.sqrt();
        let true_entropy = total_count.log2() - (true_entropy_term / total_count);

        // 4. Get Estimates
        let est_l2 = um.estimate_l2();
        let est_entropy = um.estimate_entropy();

        // 5. Assertions (Allowing ~10% error for test-sized sketches)
        let l2_err = (est_l2 - true_l2).abs() / true_l2;
        let ent_err = (est_entropy - true_entropy).abs() / true_entropy;

        println!(
            "True L2: {:.2}, Est L2: {:.2}, Error: {:.2}%",
            true_l2,
            est_l2,
            l2_err * 100.0
        );
        println!(
            "True Ent: {:.2}, Est Ent: {:.2}, Error: {:.2}%",
            true_entropy,
            est_entropy,
            ent_err * 100.0
        );

        // UnivMon is generally very accurate for L2
        assert!(l2_err < 0.15, "L2 Error too high: {:.2}%", l2_err * 100.0);

        // Entropy is harder, usually requires higher k, allowing slightly looser bound
        assert!(
            ent_err < 0.15,
            "Entropy Error too high: {:.2}%",
            ent_err * 100.0
        );
    }

    #[test]
    fn univmon_random_data_matches_ground_truth_within_configured_tolerance() {
        let mut rng = StdRng::seed_from_u64(0xDEADBEEF);
        let mut um = UnivMon::new(256, 6, 8192, 16);
        let mut truth: HashMap<String, i64> = HashMap::new();

        for _ in 0..10_000 {
            let key_id = rng.random::<u32>() % 5000;
            let key = format!("key_{key_id}");
            let value = (rng.random::<u32>() % 100 + 1) as i64;
            *truth.entry(key.clone()).or_insert(0) += value;
            um.update_weighted(&DataInput::String(key), value);
        }

        let total_mass: f64 = truth.values().map(|&v| v as f64).sum();
        let true_l1 = total_mass;
        let true_l2 = truth
            .values()
            .map(|&v| {
                let val = v as f64;
                val * val
            })
            .sum::<f64>()
            .sqrt();
        let true_card = truth.len() as f64;
        let entropy_term = truth
            .values()
            .map(|&v| {
                let val = v as f64;
                if val > 0.0 { val * val.log2() } else { 0.0 }
            })
            .sum::<f64>();
        let true_entropy = total_mass.log2() - entropy_term / total_mass;

        let to_check = [
            ("cardinality", um.estimate_cardinality(), true_card, 0.07),
            ("l1", um.estimate_l1(), true_l1, 0.05),
            ("l2", um.estimate_l2(), true_l2, 0.05),
            ("entropy", um.estimate_entropy(), true_entropy, 0.05),
        ];

        for (name, estimate, expected, tolerance) in to_check {
            let rel_err = (estimate - expected).abs() / expected;
            assert!(
                rel_err <= tolerance,
                "{name} relative error {:.2}% exceeds {:.2}%: est={estimate}, expected={expected}",
                rel_err * 100.0,
                tolerance * 100.0,
            );
        }
    }
}
