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

use crate::common::Vector1D;
use crate::common::heap::{DEFAULT_HEAP_SIZE, TopKHeap};
use crate::common::{
    DataInput, OwnedDataInput, SEED_LIST, UNIVMON_BOTTOM_LAYER_SEED_INDEX, hash64_seeded,
    hash64_seeded_owned,
};
use crate::octo_delta::UnivMonCellDelta;
use crate::sketches::countsketch_l2_norm::CountSketchWithL2Norm;
use std::collections::{HashMap, HashSet};

pub(crate) mod wire;

const DEFAULT_ROWS: usize = 5;
const DEFAULT_COLS: usize = 2048;
const DEFAULT_LAYERS: usize = 8;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum UnivMonUpdateMode {
    #[default]
    Unset,
    Standard,
    Terminal,
}

#[derive(Clone, Debug)]
/// UnivMon sketch pyramid for multi-metric stream estimation.
pub struct UnivMon {
    /// Per-layer L2/heavy-hitter sketches.
    pub layer_sketches: Vector1D<CountSketchWithL2Norm>,
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
    /// Total inserted weight (the exact L1 norm).
    pub total_weight: usize,
    update_mode: UnivMonUpdateMode,
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
/// finder's seed. Layers `0..UNIVMON_BOTTOM_LAYER_SEED_INDEX` are exactly the seeds below
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
    /// Checks a pyramid shape without building it; [`Self::new`] builds exactly
    /// the shapes this accepts. `Err` names the broken rule; the rules are in
    /// `docs/api/api_univmon.md`.
    ///
    /// More than [`MATRIX_MAX_ROWS`](crate::common::MATRIX_MAX_ROWS) rows pass:
    /// such a pyramid builds and works in memory, but
    /// [`to_asapv1`](Self::to_asapv1) refuses it. A caller that needs an
    /// encodable state must also check `rows <= MATRIX_MAX_ROWS`.
    pub fn check_shape(
        heap_size: usize,
        rows: usize,
        cols: usize,
        layers: usize,
    ) -> Result<(), String> {
        wire::check_univmon_dimensions(layers, rows, cols, heap_size)?;
        wire::check_layers("UnivMon", layers)?;
        CountSketchWithL2Norm::check_hash_bits(rows, cols)?;
        rows.checked_mul(cols)
            .and_then(|cells| cells.checked_mul(layers))
            .map(|_| ())
            .ok_or_else(|| {
                format!("UnivMon {layers} layers of {rows}x{cols} counters overflow usize")
            })
    }

    /// Creates a UnivMon instance with explicit dimensions.
    ///
    /// # Panics
    ///
    /// When [`Self::check_shape`] refuses the shape.
    pub fn new(heap_size: usize, rows: usize, cols: usize, layers: usize) -> Self {
        if let Err(problem) = Self::check_shape(heap_size, rows, cols, layers) {
            panic!("{problem}");
        }
        let sk_vec: Vec<CountSketchWithL2Norm> = (0..layers)
            .map(|i| CountSketchWithL2Norm::with_dimensions_and_seed_index(rows, cols, i))
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

    /// Whether the top layer's heap still holds every key that layer received.
    /// In standard mode a complete top layer is the event the distinct-count
    /// bound in `docs/api/api_univmon.md` is joint with.
    pub fn top_layer_complete(&self) -> bool {
        self.candidate_complete[self.layers - 1]
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
        let key = delta.key.as_data_input();
        let count = self.layer_sketches[layer].estimate(&key);
        let heap_kept_everything = self.layer_heaps[layer].update(&key, count as i64);

        // Unless the promotion threshold is 1, workers hold back counters below
        // it, so this layer's candidate set cannot be complete.
        let complete = heap_kept_everything && fidelity == UnivMonDeltaFidelity::EveryInsert;
        if !complete {
            self.candidate_complete[layer] = false;
        }
    }

    #[inline(always)]
    fn update(&mut self, key: &DataInput, value: i64, bottom_layer_num: usize) {
        for i in 0..=bottom_layer_num {
            let count = self.layer_sketches[i].update_weighted_and_estimate(key, value);
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
        let bottom_layer_num = UnivMon::bottom_layer_for_hash(h, self.layers);
        self.process_univmon(key, weight, bottom_layer_num)
    }

    /// Inserts one weighted update into its terminal stratum only.
    ///
    /// This is the Joltik update-last-layer construction. Logical sampled
    /// streams and their candidate sets are reconstructed during queries.
    /// Do not mix this method with [`Self::update_weighted`] on the same sketch.
    pub fn update_weighted_terminal(&mut self, key: &DataInput, weight: i64) {
        self.begin_update(weight, UnivMonUpdateMode::Terminal);
        let h = hash64_seeded(UNIVMON_BOTTOM_LAYER_SEED_INDEX, key);
        let bottom_layer_num = UnivMon::bottom_layer_for_hash(h, self.layers);
        let count = self.layer_sketches[bottom_layer_num].update_weighted_and_estimate(key, weight);
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

    /// Computes a g-sum with the UnivMon recurrence. With `count_every_key` every heap
    /// key counts; otherwise nonpositive estimates are skipped, and in terminal mode
    /// so are those at or below `floor(logical L2 / sqrt(heap_size))` on incomplete layers.
    pub fn estimate_g_sum<F>(&self, g: F, count_every_key: bool) -> f64
    where
        F: Fn(f64) -> f64,
    {
        if self.total_weight == 0 && !count_every_key {
            return 0.0;
        }
        if self.update_mode == UnivMonUpdateMode::Terminal {
            return self.calc_terminal_g_sum(g, count_every_key);
        }

        let counted = |count: i64| count_every_key || count > 0;
        let mut y = vec![0.0; self.layers];
        let mut tmp: f64;

        tmp = 0.0;
        for item in self.layer_heaps[self.layers - 1].heap() {
            let input = item.key.as_data_input();
            let count = self.layer_sketches[self.layers - 1].estimate(&input) as i64;
            if counted(count) {
                tmp += g(count as f64);
            }
        }
        y[self.layers - 1] = tmp;

        for i in (0..(self.layers - 1)).rev() {
            tmp = 0.0;
            for item in self.layer_heaps[i].heap() {
                let input = item.key.as_data_input();
                let count = self.layer_sketches[i].estimate(&input) as i64;
                if counted(count) {
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

    fn calc_terminal_g_sum<F>(&self, g: F, count_every_key: bool) -> f64
    where
        F: Fn(f64) -> f64,
    {
        let (candidates, complete) = self.logical_terminal_candidates();
        let counted = |count: i64, threshold: i64| count_every_key || count > threshold;
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
            .filter(|(_, count)| counted(*count, threshold))
            .map(|(_, count)| g(*count as f64))
            .sum();

        for level in (0..last).rev() {
            let threshold = self.heavy_threshold(logical_l2[level], complete[level]);
            let correction = candidates[level]
                .iter()
                .filter(|(_, count)| counted(*count, threshold))
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
                let input = item.key.as_data_input();
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

    /// Returns the exact L1 norm for the supported non-negative update stream.
    pub fn estimate_l1(&self) -> f64 {
        self.total_weight as f64
    }

    /// Returns the estimated L2 norm: layer 0's Count Sketch F2 estimate in
    /// standard mode, the summed strata's F2 estimates in terminal mode.
    /// Error bounds: `docs/api/api_univmon.md`.
    pub fn estimate_l2(&self) -> f64 {
        if self.update_mode == UnivMonUpdateMode::Terminal {
            return (0..self.layers)
                .map(|level| self.layer_sketches[level].l2_squared())
                .sum::<f64>()
                .sqrt();
        }
        self.layer_sketches[0].l2()
    }

    /// Returns the estimated Shannon entropy, in bits (log2).
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

    /// Returns the estimated number of distinct keys, counting every heap key
    /// (zero-weight keys included) whatever its frequency estimate.
    /// Error bounds: `docs/api/api_univmon.md`.
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
                let input = key.as_data_input();
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

    /// Bytes this pyramid holds: the struct, each layer's sketch and heap
    /// ([`CountSketchWithL2Norm::memory_bytes`], [`TopKHeap::memory_bytes`])
    /// and the per-layer candidate flags.
    pub fn memory_bytes(&self) -> usize {
        size_of::<Self>()
            + self.layer_sketches.capacity() * size_of::<CountSketchWithL2Norm>()
            + self
                .layer_sketches
                .iter()
                .map(|sketch| sketch.memory_bytes() - size_of::<CountSketchWithL2Norm>())
                .sum::<usize>()
            + self.layer_heaps.capacity() * size_of::<TopKHeap>()
            + self
                .layer_heaps
                .iter()
                .map(|heap| heap.memory_bytes() - size_of::<TopKHeap>())
                .sum::<usize>()
            + self.candidate_complete.capacity() * size_of::<bool>()
    }

    /// Bytes a pyramid of that shape, built by [`Self::new`], decoded or cloned,
    /// holds in steady state once keys shaped like `key` churn through every
    /// layer: the struct and, per layer,
    /// [`CountSketchWithL2Norm::estimated_memory_bytes`],
    /// [`TopKHeap::estimated_memory_bytes`] and a candidate flag. A pyramid
    /// holds at most this, and exactly this once every layer's heap has
    /// churned. Saturates at `usize::MAX`.
    pub fn estimated_memory_bytes(
        heap_size: usize,
        rows: usize,
        cols: usize,
        layers: usize,
        key: &DataInput,
    ) -> usize {
        let layer = CountSketchWithL2Norm::estimated_memory_bytes(rows, cols)
            .saturating_add(TopKHeap::<i64>::estimated_memory_bytes(heap_size, key))
            .saturating_add(size_of::<bool>());
        size_of::<Self>().saturating_add(layers.saturating_mul(layer))
    }
}

/// Object pool for `UnivMon` sketches.
///
/// Maintains a free-list of pre-allocated sketches. Callers take ownership
/// via `take()` and return sketches via `put()`, which resets and recycles
/// them. This avoids repeated heap allocation/deallocation for large sketch
/// matrices during promotion, merge, and expiration cycles.
pub struct UnivMonPool {
    free_list: Vec<UnivMon>,
    total_allocated: usize,
    heap_size: usize,
    rows: usize,
    cols: usize,
    layers: usize,
}

impl UnivMonPool {
    /// Creates a new pool with `initial_size` pre-allocated sketches.
    pub fn new(
        initial_size: usize,
        heap_size: usize,
        rows: usize,
        cols: usize,
        layers: usize,
    ) -> Self {
        let free_list: Vec<UnivMon> = (0..initial_size)
            .map(|_| UnivMon::new(heap_size, rows, cols, layers))
            .collect();
        UnivMonPool {
            free_list,
            total_allocated: initial_size,
            heap_size,
            rows,
            cols,
            layers,
        }
    }

    /// Takes ownership of a clean sketch from the pool.
    ///
    /// Pops a recycled sketch from the free-list if available, otherwise
    /// allocates a fresh one.
    pub fn take(&mut self) -> UnivMon {
        if let Some(sketch) = self.free_list.pop() {
            sketch
        } else {
            self.total_allocated += 1;
            UnivMon::new(self.heap_size, self.rows, self.cols, self.layers)
        }
    }

    /// Returns a sketch to the pool for reuse. Resets all internal state.
    pub fn put(&mut self, mut sketch: UnivMon) {
        sketch.clear();
        self.free_list.push(sketch);
    }

    /// Number of sketches currently available in the pool.
    pub fn available(&self) -> usize {
        self.free_list.len()
    }

    /// Total number of sketches ever allocated by this pool.
    pub fn total_allocated(&self) -> usize {
        self.total_allocated
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataInput, OwnedDataInput};
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use std::collections::HashMap;

    #[test]
    fn pool_basic_take_put() {
        let mut pool = UnivMonPool::new(2, 16, 2, 5, 2);
        assert_eq!(pool.available(), 2);
        assert_eq!(pool.total_allocated(), 2);

        let s0 = pool.take();
        assert_eq!(pool.available(), 1);

        let s1 = pool.take();
        assert_eq!(pool.available(), 0);

        // Pool is empty — next take allocates a new one
        let s2 = pool.take();
        assert_eq!(pool.available(), 0);
        assert_eq!(pool.total_allocated(), 3);

        // Return one
        pool.put(s1);
        assert_eq!(pool.available(), 1);

        // Should reuse the returned sketch
        let s3 = pool.take();
        assert_eq!(pool.available(), 0);
        assert_eq!(pool.total_allocated(), 3); // no new allocation

        // Return all
        pool.put(s0);
        pool.put(s2);
        pool.put(s3);
        assert_eq!(pool.available(), 3);
    }

    #[test]
    fn pool_free_resets_sketch() {
        let mut pool = UnivMonPool::new(1, 16, 2, 5, 2);

        // Take a sketch, insert some data
        let mut sketch = pool.take();
        sketch.update_weighted(&DataInput::I64(42), 100);
        assert!(sketch.total_weight > 0);

        // Return it — should reset
        pool.put(sketch);

        // Take it back — should be clean
        let sketch2 = pool.take();
        assert_eq!(sketch2.total_weight, 0);
        assert!((sketch2.layer_sketches[0].l2()).abs() < 1e-9);
    }

    /// Every layer hashes at its own seed index, inside the seed list, and
    /// never at the seed that picks the layers.
    #[test]
    fn layer_seeds_are_distinct_and_never_the_layer_finder() {
        use crate::common::SEED_LIST;
        let deepest = UnivMon::new(4, 2, 16, UNIVMON_MAX_LAYERS);
        let seeds: Vec<usize> = (0..UNIVMON_MAX_LAYERS)
            .map(|layer| {
                let counter = &deepest.layer_sketches[layer];
                counter.seed_index()
            })
            .collect();
        for &seed in &seeds {
            assert!(
                seed < SEED_LIST.len(),
                "seed index {seed} is past the seed list"
            );
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
    #[should_panic(expected = "UNIVMON_MAX_LAYERS")]
    fn new_rejects_a_layer_on_the_bottom_layer_seed_index() {
        UnivMon::new(4, 2, 16, UNIVMON_BOTTOM_LAYER_SEED_INDEX + 1);
    }

    #[test]
    #[should_panic(expected = "hash bits")]
    fn new_rejects_column_bits_that_reach_the_sign_bits() {
        UnivMon::new(4, 11, 2048, 2);
    }

    /// `check_shape` accepts exactly the shapes `new` builds.
    #[test]
    fn check_shape_accepts_exactly_what_new_builds() {
        use crate::common::MATRIX_MAX_ROWS;
        for (heap, rows, cols, layers, valid) in [
            (4, 3, 2048, 8, true),
            (4, 10, 2048, 2, true),
            (4, 11, 2048, 2, false),
            (4, 2, 16, UNIVMON_MAX_LAYERS, true),
            (4, 2, 16, UNIVMON_MAX_LAYERS + 1, false),
            (0, 3, 16, 2, false),
            (4, 0, 16, 2, false),
            (4, 3, 0, 2, false),
            (4, 3, 16, 0, false),
            (4, MATRIX_MAX_ROWS, 2, 1, true),
            (4, MATRIX_MAX_ROWS + 1, 2, 1, true),
        ] {
            assert_eq!(
                UnivMon::check_shape(heap, rows, cols, layers).is_ok(),
                valid,
                "{heap} {rows}x{cols} x{layers}"
            );
            let built = std::panic::catch_unwind(|| UnivMon::new(heap, rows, cols, layers));
            assert_eq!(built.is_ok(), valid, "{heap} {rows}x{cols} x{layers}");
        }
    }

    #[test]
    fn check_shape_rejects_a_counter_count_past_usize() {
        let problem = UnivMon::check_shape(4, 1, usize::MAX, 2).expect_err("overflow");
        assert!(problem.contains("overflow usize"), "got {problem}");
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
    fn update_populates_total_weight_and_heavy_hitters() {
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
                UnivMon::bottom_layer_for_hash(hash, sketch.layers) >= 2
            })
            .expect("find a key sampled into multiple layers");
        let hash = hash64_seeded(UNIVMON_BOTTOM_LAYER_SEED_INDEX, &key);
        let bottom = UnivMon::bottom_layer_for_hash(hash, sketch.layers);
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
                UnivMon::bottom_layer_for_hash(hash, sketch.layers) >= 2
            })
            .expect("find a key sampled into multiple layers");
        let hash = hash64_seeded(UNIVMON_BOTTOM_LAYER_SEED_INDEX, &key);
        let bottom = UnivMon::bottom_layer_for_hash(hash, sketch.layers);
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
    fn univmon_total_weight_tracked_correctly() {
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

    /// Sign layer 0 gives `key` in its single row (`+1` or `-1`).
    fn layer0_sign(key: &DataInput, cols: usize) -> i64 {
        use crate::common::hash128_seeded;
        use crate::common::structures::matrix_storage::cols_mask_bits;
        CountSketchWithL2Norm::cell_for_row(hash128_seeded(0, key), 0, cols, cols_mask_bits(cols)).1
    }

    #[test]
    fn estimate_cardinality_counts_zero_weight_only_streams() {
        for terminal in [false, true] {
            let mut sketch = UnivMon::new(8, 3, 64, 4);
            assert_eq!(sketch.estimate_cardinality(), 0.0);
            for id in [1, 2, 1, 3] {
                if terminal {
                    sketch.update_weighted_terminal(&DataInput::U64(id), 0);
                } else {
                    sketch.update_weighted(&DataInput::U64(id), 0);
                }
            }
            assert_eq!(sketch.estimate_l1(), 0.0);
            assert_eq!(sketch.estimate_l2(), 0.0);
            assert_eq!(sketch.estimate_entropy(), 0.0);
            assert!(sketch.top_layer_complete());
            assert_eq!(sketch.estimate_cardinality(), 3.0);
            let decoded = UnivMon::from_asapv1(&sketch.to_asapv1().unwrap()).unwrap();
            assert_eq!(decoded.estimate_cardinality(), 3.0);
            sketch.clear();
            assert_eq!(sketch.estimate_cardinality(), 0.0);
        }
    }

    /// A heap key whose Count Sketch estimate is not positive still counts.
    #[test]
    fn estimate_cardinality_counts_heap_keys_with_nonpositive_estimates() {
        let plus = (0_u64..)
            .map(DataInput::U64)
            .find(|key| layer0_sign(key, 1) == 1)
            .expect("a key with sign +1");
        let minus = (0_u64..)
            .map(DataInput::U64)
            .find(|key| layer0_sign(key, 1) == -1)
            .expect("a key with sign -1");
        // One row, one column: the light key reads 1 - 5 = -4.
        let mut sketch = UnivMon::new(4, 1, 1, 1);
        sketch.update_weighted(&plus, 5);
        sketch.update_weighted(&minus, 1);
        assert!(sketch.layer_sketches[0].estimate(&minus) <= 0.0);
        assert!(sketch.top_layer_complete());
        assert_eq!(sketch.estimate_cardinality(), 2.0);
    }

    #[test]
    fn estimate_l2_reads_layer_zero_directly_in_standard_mode() {
        let mut sketch = UnivMon::new(16, 5, 256, 8);
        for key in 0..2000_u64 {
            sketch.update_weighted(&DataInput::U64(key), (key % 7 + 1) as i64);
        }
        let direct = sketch.layer_sketches[0].l2();
        assert!(
            (sketch.estimate_l2() - direct).abs() < 1e-9,
            "estimate_l2 {} differs from layer 0's direct estimate {direct}",
            sketch.estimate_l2()
        );
    }

    #[test]
    fn estimate_l2_sums_stratum_estimates_in_terminal_mode() {
        let mut sketch = UnivMon::new(16, 5, 256, 8);
        for key in 0..2000_u64 {
            sketch.update_weighted_terminal(&DataInput::U64(key), (key % 7 + 1) as i64);
        }
        let direct = (0..sketch.layers)
            .map(|level| sketch.layer_sketches[level].l2_squared())
            .sum::<f64>()
            .sqrt();
        assert!(
            (sketch.estimate_l2() - direct).abs() < 1e-9,
            "estimate_l2 {} differs from the summed strata {direct}",
            sketch.estimate_l2()
        );
    }

    #[test]
    fn top_layer_complete_tracks_the_top_heap() {
        let mut sketch = UnivMon::new(4, 3, 64, 1);
        for key in 0..4_u64 {
            sketch.update_weighted(&DataInput::U64(key), 1);
        }
        assert!(sketch.top_layer_complete());
        sketch.update_weighted(&DataInput::U64(4), 1);
        assert!(!sketch.top_layer_complete());
        sketch.clear();
        assert!(sketch.top_layer_complete());
    }

    /// Monte Carlo checks of the error bounds in `docs/api/api_univmon.md`.
    /// The seed list is fixed, so each trial draws fresh random key ids.
    mod error_bounds {
        use super::*;

        #[derive(Clone, Copy, Debug)]
        enum Shape {
            Zipf,
            Uniform,
            SingleHeavy,
        }

        const SHAPES: [Shape; 3] = [Shape::Zipf, Shape::Uniform, Shape::SingleHeavy];

        /// Per-key weights of `n` keys in one of the three stream shapes.
        fn weights(shape: Shape, n: usize, rng: &mut StdRng) -> Vec<i64> {
            match shape {
                Shape::Zipf => (1..=n)
                    .map(|rank| (10_000.0 / (rank as f64).powf(1.1)).ceil() as i64)
                    .collect(),
                Shape::Uniform => (0..n).map(|_| rng.random_range(1..=10)).collect(),
                Shape::SingleHeavy => (0..n)
                    .map(|i| if i == 0 { 100 * n as i64 } else { 1 })
                    .collect(),
            }
        }

        /// One stream: fresh random key ids with the shape's weights.
        fn stream(shape: Shape, n: usize, rng: &mut StdRng) -> Vec<(u64, i64)> {
            let ws = weights(shape, n, rng);
            let mut seen = HashSet::with_capacity(n);
            let mut ids = Vec::with_capacity(n);
            while ids.len() < n {
                let id = rng.random::<u64>();
                if seen.insert(id) {
                    ids.push(id);
                }
            }
            ids.into_iter().zip(ws).collect()
        }

        /// `P(Binomial(r, p) >= m)`.
        fn binomial_tail(r: u32, p: f64, m: u32) -> f64 {
            let choose = |n: u32, k: u32| {
                (0..k).fold(1.0, |acc, i| acc * f64::from(n - i) / f64::from(i + 1))
            };
            (m..=r)
                .map(|j| choose(r, j) * p.powi(j as i32) * (1.0 - p).powi((r - j) as i32))
                .sum()
        }

        /// Failures allowed out of `trials` at a true rate of at most `delta`:
        /// the mean plus four standard deviations, plus one.
        fn allowed_failures(trials: usize, delta: f64) -> f64 {
            let n = trials as f64;
            n * delta + 4.0 * (n * delta * (1.0 - delta)).sqrt() + 1.0
        }

        /// Collision probability `q` the docs bound for a `w`-column row.
        fn q_bound(w: usize) -> f64 {
            if w.is_power_of_two() {
                1.0 / w as f64
            } else {
                9.0 / (8.0 * w as f64)
            }
        }

        fn check_l2(trials: usize) {
            const ROWS: usize = 5;
            const KEYS: usize = 400;
            const EPS: f64 = 0.5;
            let mut rng = StdRng::seed_from_u64(0x5EED_0012);
            // 48 columns slice 6 bits: the worst case of the modulo fold.
            for w in [64_usize, 48] {
                let q = q_bound(w);
                let p = 2.0 * q / (EPS * EPS);
                let delta = binomial_tail(ROWS as u32, p, ROWS.div_ceil(2) as u32);
                for shape in SHAPES {
                    let mut failures = 0;
                    for _ in 0..trials {
                        let keys = stream(shape, KEYS, &mut rng);
                        let mut sketch = UnivMon::new(8, ROWS, w, 2);
                        for &(id, weight) in &keys {
                            sketch.update_weighted(&DataInput::U64(id), weight);
                        }
                        let f2: f64 = keys.iter().map(|&(_, f)| (f as f64).powi(2)).sum();
                        let estimate = sketch.estimate_l2().powi(2);
                        if (estimate - f2).abs() > EPS * f2 {
                            failures += 1;
                        }
                    }
                    assert!(
                        failures as f64 <= allowed_failures(trials, delta),
                        "L2 {shape:?} w={w}: {failures}/{trials} misses beyond eps={EPS}, delta={delta:.4}"
                    );
                }
            }
        }

        fn check_cardinality(trials: usize) {
            const HEAP: usize = 32;
            const KEYS: usize = 2000;
            const EPS: f64 = 0.5;
            // L >= ceil(log2(F0 / k)) + 2.
            let layers = (KEYS as f64 / HEAP as f64).log2().ceil() as usize + 2;
            let chebyshev = 2.0 / (HEAP as f64 * EPS * EPS);
            let mut rng = StdRng::seed_from_u64(0x5EED_00F0);
            let f0 = KEYS as f64;
            for shape in SHAPES {
                let (mut failures, mut incomplete) = (0, 0);
                let (mut sum, mut sum_sq) = (0.0, 0.0);
                for _ in 0..trials {
                    let mut sketch = UnivMon::new(HEAP, 3, 256, layers);
                    let mut top_keys = 0;
                    for (id, weight) in stream(shape, KEYS, &mut rng) {
                        let key = DataInput::U64(id);
                        top_keys += usize::from(sketch.bottom_layer_for(&key) == layers - 1);
                        sketch.update_weighted(&key, weight);
                    }
                    let complete = sketch.top_layer_complete();
                    incomplete += usize::from(!complete);
                    let estimate = sketch.estimate_cardinality();
                    // The ideal estimator restores the exact top count, also
                    // on trials whose top layer overflowed.
                    let missing = top_keys - sketch.layer_heaps[layers - 1].len();
                    let ideal = estimate + ((1_u64 << (layers - 1)) as f64) * missing as f64;
                    sum += ideal - f0;
                    sum_sq += (ideal - f0).powi(2);
                    if complete && (estimate - f0).abs() > EPS * f0 {
                        failures += 1;
                    }
                }
                // (e/4)^{k/2} is about 2e-3 for k = 32.
                assert!(
                    incomplete as f64 <= allowed_failures(trials, 0.01),
                    "cardinality {shape:?}: top layer incomplete in {incomplete}/{trials} trials"
                );
                let trials_f = trials as f64;
                let variance = sum_sq / trials_f;
                assert!(
                    failures as f64 <= allowed_failures(trials, chebyshev),
                    "cardinality {shape:?}: {failures}/{trials} joint misses beyond eps={EPS}"
                );
                assert!(
                    variance <= 1.35 * f0 * f0 / HEAP as f64,
                    "cardinality {shape:?}: variance {:.4} F0^2/k above 1.35",
                    variance / (f0 * f0 / HEAP as f64)
                );
                assert!(
                    (sum / trials_f).abs() <= 4.0 * (variance / trials_f).sqrt(),
                    "cardinality {shape:?}: mean error {:.2} looks biased",
                    sum / trials_f
                );
            }
        }

        #[test]
        fn estimate_l2_meets_its_documented_bound() {
            check_l2(150);
        }

        #[test]
        #[ignore = "long Monte Carlo run; the fast variant runs by default"]
        fn estimate_l2_meets_its_documented_bound_long() {
            check_l2(3000);
        }

        #[test]
        fn estimate_cardinality_meets_its_documented_bound() {
            check_cardinality(60);
        }

        #[test]
        #[ignore = "long Monte Carlo run; the fast variant runs by default"]
        fn estimate_cardinality_meets_its_documented_bound_long() {
            check_cardinality(2000);
        }
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
