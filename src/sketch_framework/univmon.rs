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

use crate::common::heap::HHHeap;
use crate::common::{
    BOTTOM_LAYER_FINDER, DataInput, HeapItem, hash_item64_seeded, hash64_seeded,
    heap_item_to_sketch_input,
};
use crate::common::{L2HH, Vector1D};
use crate::octo_delta::LayeredCountDelta;
use crate::sketches::countsketch_topk::CountL2HH;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub(crate) mod wire;

const DEFAULT_SKETCH_ROW: usize = 5;
const DEFAULT_SKETCH_COL: usize = 2048;
const DEFAULT_HEAP_SIZE: usize = 32;
const DEFAULT_LAYER_SIZE: usize = 8;

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
    pub l2_sketch_layers: Vector1D<L2HH>,
    /// Per-layer heavy-hitter heaps.
    pub hh_layers: Vector1D<HHHeap>,
    /// Number of pyramid layers.
    pub layer_size: usize,
    /// Row count of each underlying sketch.
    pub sketch_row: usize,
    /// Column count of each underlying sketch.
    pub sketch_col: usize,
    /// Heap capacity per layer.
    pub heap_size: usize,
    /// Bucket size used for hashing decisions.
    pub bucket_size: usize,
    #[serde(default)]
    update_mode: UnivMonUpdateMode,
    #[serde(default)]
    candidate_complete: Vec<bool>,
}

impl Default for UnivMon {
    fn default() -> Self {
        UnivMon::init_univmon(
            DEFAULT_HEAP_SIZE,
            DEFAULT_SKETCH_ROW,
            DEFAULT_SKETCH_COL,
            DEFAULT_LAYER_SIZE,
        )
    }
}

/// Layers one pyramid may hold. [`bottom_layer_for_hash`] and the query
/// recurrences shift a 64-bit key hash right by up to `layer_size - 1`, so a
/// deeper pyramid would shift past the hash's width.
pub const MAX_LAYER_SIZE: usize = 64;

/// Deepest pyramid layer the given key hash reaches.
#[inline(always)]
pub fn bottom_layer_for_hash(hash: u64, layer_size: usize) -> usize {
    for l in 1..layer_size {
        if ((hash >> l) & 1) == 0 {
            return l - 1;
        }
    }
    layer_size - 1
}

/// How much of the stream reached an aggregator being fed `LayeredCountDelta`s.
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
    /// Whether this state accepts standard updates and standard-state merges.
    /// Empty states have not selected an update strategy yet.
    pub fn accepts_standard_updates(&self) -> bool {
        matches!(
            self.update_mode,
            UnivMonUpdateMode::Unset | UnivMonUpdateMode::Standard
        )
    }
    /// Creates a UnivMon instance with explicit dimensions.
    pub fn init_univmon(
        heap_size: usize,
        sketch_row: usize,
        sketch_col: usize,
        layer_size: usize,
    ) -> Self {
        assert!(heap_size > 0, "heap size must be positive");
        assert!(sketch_row > 0, "sketch row count must be positive");
        assert!(sketch_col > 0, "sketch column count must be positive");
        assert!(layer_size > 0, "layer count must be positive");
        assert!(
            layer_size <= MAX_LAYER_SIZE,
            "layer count must be at most MAX_LAYER_SIZE {MAX_LAYER_SIZE}"
        );
        let sk_vec: Vec<L2HH> = (0..layer_size)
            .map(|i| {
                L2HH::COUNT(CountL2HH::with_dimensions_and_seed(
                    sketch_row, sketch_col, i,
                ))
            })
            .collect();

        let hh_vec: Vec<HHHeap> = (0..layer_size).map(|_| HHHeap::new(heap_size)).collect();

        UnivMon {
            l2_sketch_layers: Vector1D::from_vec(sk_vec),
            hh_layers: Vector1D::from_vec(hh_vec),
            layer_size,
            sketch_row,
            sketch_col,
            heap_size,
            bucket_size: 0,
            update_mode: UnivMonUpdateMode::Unset,
            candidate_complete: vec![true; layer_size],
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
        self.bucket_size = self
            .bucket_size
            .checked_add(value as usize)
            .expect("UnivMon total weight overflowed usize");
    }

    #[inline(always)]
    fn find_bottom_layer_num(&self, hash: u64, layer: usize) -> usize {
        bottom_layer_for_hash(hash, layer)
    }

    /// Deepest pyramid layer an insert of `key` reaches; it touches `0..=this`.
    ///
    /// A pure function of the key's hash, so an OctoSketch worker picks exactly
    /// the same layers as a single-threaded insert would.
    pub fn bottom_layer_for(&self, key: &DataInput) -> usize {
        bottom_layer_for_hash(hash64_seeded(BOTTOM_LAYER_FINDER, key), self.layer_size)
    }

    /// Whether each layer's heavy-hitter heap still holds every key that layer
    /// received. Queries widen their threshold on the layers where it does not.
    pub fn candidates_complete(&self) -> &[bool] {
        &self.candidate_complete
    }

    /// Whether the top layer's heap still holds every key that layer received.
    ///
    /// This is the precondition of [`Self::calc_card`]'s error bound: with an
    /// incomplete top layer the distinct count is biased low. The top layer is
    /// the same physical heap in standard and terminal mode.
    pub fn top_layer_complete(&self) -> bool {
        self.candidate_complete[self.layer_size - 1]
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
    /// `insert` and `merge` accumulate, so a sketch fed by deltas must not also
    /// be inserted into or merged with: the next delta would erase whatever
    /// those added. Feeding one exclusively through `apply_layered_delta` and
    /// this method is the supported arrangement.
    pub fn set_total_weight(&mut self, weight: usize) {
        debug_assert_ne!(
            self.update_mode,
            UnivMonUpdateMode::Terminal,
            "a terminal-mode UnivMon cannot be fed by the delta path"
        );
        self.bucket_size = weight;
        if self.update_mode == UnivMonUpdateMode::Unset {
            self.update_mode = UnivMonUpdateMode::Standard;
        }
    }

    /// Applies one delta promoted by an OctoSketch worker.
    ///
    /// Mirrors `update` for a single layer: the counter lands, the layer's
    /// estimate is re-read, and the layer's heavy-hitter heap follows - which
    /// is Algorithm 2 of the OctoSketch paper.
    pub fn apply_layered_delta(
        &mut self,
        delta: &LayeredCountDelta,
        fidelity: UnivMonDeltaFidelity,
    ) {
        if self.update_mode == UnivMonUpdateMode::Unset {
            self.update_mode = UnivMonUpdateMode::Standard;
        }
        let layer = delta.layer as usize;
        assert!(
            layer < self.layer_size,
            "delta names layer {layer} but this pyramid has {} layers",
            self.layer_size
        );

        self.l2_sketch_layers[layer].apply_delta(delta.delta);
        let key = heap_item_to_sketch_input(&delta.key);
        let count = self.l2_sketch_layers[layer].estimate(&key);
        let heap_kept_everything = self.hh_layers[layer].update(&key, count as i64);

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
            let count = self.l2_sketch_layers[i].update_and_est(key, value);
            if !self.hh_layers[i].update(key, count as i64) {
                self.candidate_complete[i] = false;
            }
        }
    }

    #[inline(always)]
    fn process_univmon(&mut self, key: &DataInput, value: i64, bottom_layer_num: usize) {
        self.update(key, value, bottom_layer_num);
    }

    /// Inserts one weighted update.
    pub fn insert(&mut self, key: &DataInput, value: i64) {
        self.begin_update(value, UnivMonUpdateMode::Standard);
        let h = hash64_seeded(BOTTOM_LAYER_FINDER, key);
        let bottom_layer_num = self.find_bottom_layer_num(h, self.layer_size);
        self.process_univmon(key, value, bottom_layer_num)
    }

    /// Inserts one weighted update into its terminal stratum only.
    ///
    /// This is the Joltik update-last-layer construction. Logical sampled
    /// streams and their candidate sets are reconstructed during queries.
    /// Do not mix this method with [`Self::insert`] on the same sketch.
    pub fn fast_insert(&mut self, key: &DataInput, value: i64) {
        self.begin_update(value, UnivMonUpdateMode::Terminal);
        let h = hash64_seeded(BOTTOM_LAYER_FINDER, key);
        let bottom_layer_num = self.find_bottom_layer_num(h, self.layer_size);
        let count = self.l2_sketch_layers[bottom_layer_num].update_and_est(key, value);
        if !self.hh_layers[bottom_layer_num].update(key, count as i64) {
            self.candidate_complete[bottom_layer_num] = false;
        }
    }

    /// Prints all heavy-hitter layers for debugging.
    pub fn print_hh_layer(&self) {
        print!("Print HH_Layer: ");
        for i in 0..self.layer_size {
            println!("layer {i}: ");
            self.hh_layers[i].print_heap();
        }
    }

    /// Computes a g-sum estimate using the heuristic recurrence.
    ///
    /// With `is_card` every heap key contributes `g` of its estimate; that
    /// is [`Self::calc_card`]'s estimator. Otherwise a key whose estimate is
    /// not positive is skipped, and in terminal mode a layer whose candidate
    /// set is incomplete also skips keys below `L2 / √heap_size`.
    pub fn calc_g_sum_heuristic<F>(&self, g: F, is_card: bool) -> f64
    where
        F: Fn(f64) -> f64,
    {
        if self.bucket_size == 0 {
            return 0.0;
        }
        if self.update_mode == UnivMonUpdateMode::Terminal {
            return self.calc_terminal_g_sum(g, is_card);
        }

        // A distinct count weighs every heap key 1 whatever its estimate: the
        // recursion stays unbiased only if no key is dropped by its value.
        let counted = |count: i64| is_card || count > 0;
        let mut y = vec![0.0; self.layer_size];
        let mut tmp: f64;

        tmp = 0.0;
        for item in self.hh_layers[self.layer_size - 1].heap() {
            let input = heap_item_to_sketch_input(&item.key);
            let count = self.l2_sketch_layers[self.layer_size - 1].estimate(&input) as i64;
            if counted(count) {
                tmp += g(count as f64);
            }
        }
        y[self.layer_size - 1] = tmp;

        for i in (0..(self.layer_size - 1)).rev() {
            tmp = 0.0;
            for item in self.hh_layers[i].heap() {
                let input = heap_item_to_sketch_input(&item.key);
                let count = self.l2_sketch_layers[i].estimate(&input) as i64;
                if counted(count) {
                    let hash = (hash_item64_seeded(BOTTOM_LAYER_FINDER, &item.key) >> (i + 1)) & 1;
                    let coe = 1.0 - 2.0 * (hash as f64);
                    tmp += coe * g(count as f64);
                }
            }
            y[i] = 2.0 * y[i + 1] + tmp;
        }

        y[0]
    }

    fn calc_terminal_g_sum<F>(&self, g: F, is_card: bool) -> f64
    where
        F: Fn(f64) -> f64,
    {
        let (candidates, complete) = self.logical_terminal_candidates();
        // As in standard mode, a distinct count weighs every candidate 1.
        let counted = |count: i64, threshold: i64| is_card || count > threshold;
        let mut logical_l2 = vec![0.0; self.layer_size];
        let mut suffix_l2_squared = 0.0;
        for level in (0..self.layer_size).rev() {
            let terminal_l2 = self.l2_sketch_layers[level].get_l2();
            suffix_l2_squared += terminal_l2 * terminal_l2;
            logical_l2[level] = suffix_l2_squared.sqrt();
        }

        let mut y = vec![0.0; self.layer_size];
        let last = self.layer_size - 1;
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
                    let hash = (hash_item64_seeded(BOTTOM_LAYER_FINDER, key) >> (level + 1)) & 1;
                    (1.0 - 2.0 * hash as f64) * g(*count as f64)
                })
                .sum::<f64>();
            y[level] = 2.0 * y[level + 1] + correction;
        }
        y[0]
    }

    /// Reconstructs the logical sampled-stream candidate sets from disjoint
    /// terminal strata, as required by the Joltik update-last-layer scheme.
    fn logical_terminal_candidates(&self) -> (Vec<Vec<(HeapItem, i64)>>, Vec<bool>) {
        let mut logical = vec![Vec::new(); self.layer_size];
        let mut complete = vec![false; self.layer_size];
        let mut cumulative = HashMap::<HeapItem, i64>::with_capacity(self.heap_size * 2);
        let mut suffix_complete = true;
        for level in (0..self.layer_size).rev() {
            suffix_complete &= self.candidate_complete[level];
            for item in self.hh_layers[level].heap() {
                let input = heap_item_to_sketch_input(&item.key);
                let count = self.l2_sketch_layers[level].estimate(&input) as i64;
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
                    hash_item64_seeded(BOTTOM_LAYER_FINDER, &left.0)
                        .cmp(&hash_item64_seeded(BOTTOM_LAYER_FINDER, &right.0))
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
    pub fn calc_g_sum<F>(&self, g: F, is_card: bool) -> f64
    where
        F: Fn(f64) -> f64,
    {
        self.calc_g_sum_heuristic(g, is_card)
    }

    /// Returns the exact L1 norm for the supported non-negative update stream.
    pub fn calc_l1(&self) -> f64 {
        self.bucket_size as f64
    }

    /// Returns the estimated L2 norm.
    ///
    /// In standard mode layer 0 receives every key, so its Count Sketch
    /// estimates the second moment `F2 = Σ f_i²` directly: each row's sum of
    /// squared counters, then the median across rows. In terminal mode the
    /// layers hold disjoint strata of the key space, so their `F2` estimates
    /// add. The heap recursion of [`Self::calc_g_sum`] is not used here; it
    /// estimates the same quantity with more variance.
    ///
    /// # Error bound (standard mode)
    ///
    /// Take one row with `w = sketch_col` columns. With column `c_i` and sign
    /// `s_i` for key `i`, the row's estimate is
    /// `Z = Σ_c (Σ_{i: c_i = c} s_i f_i)² = F2 + Σ_{i≠j} s_i s_j f_i f_j [c_i = c_j]`.
    /// Treating the key hash as uniformly random, the signs are independent
    /// of each other and of the columns, so `E[Z] = F2` and
    /// `Var[Z] = 2q (F2² − F4) ≤ 2q F2²`, where `F4 = Σ f_i⁴` and
    /// `q = P(c_i = c_j)`. A column is a hash slice of `⌈log2 w⌉` bits taken
    /// modulo `w`, so `q = 1/w` when `w` is a power of two and `q ≤ 9/(8w)`
    /// otherwise (the slice values `w..2^⌈log2 w⌉` fold onto the low
    /// columns; the worst case, `w = ¾·2^⌈log2 w⌉`, doubles a third of them).
    ///
    /// By Chebyshev, one row misses by more than `ε F2` with probability at
    /// most `p = 2q / ε²`, i.e. `p = 2 / (w ε²)` for a power-of-two `w` and
    /// `p = 9 / (4 w ε²)` otherwise. The `r = sketch_row` rows hash disjoint
    /// bit slices of one 128-bit hash (while `r · (⌈log2 w⌉ + 1) ≤ 128`), so
    /// they are independent. Their median (the mean of the two middle rows
    /// when `r` is even) misses only when at least `⌈r/2⌉` rows miss on the
    /// same side, hence
    ///
    /// `P(|F2_est − F2| > ε F2) ≤ δ = P(Binomial(r, p) ≥ ⌈r/2⌉)`.
    ///
    /// For odd `r`, `⌈r/2⌉ = (r+1)/2`. The bound is useful only for
    /// `p < 1/2`. It bounds the squared norm; because
    /// `|√(1 ± ε) − 1| ≤ ε`, the same `ε` and `δ` also bound the relative
    /// error of the returned L2 norm. The bound is independent of the stream's
    /// shape and length while `F2` stays below the `i64` counters' range.
    ///
    /// # Terminal mode
    ///
    /// Each stratum's estimate satisfies the bound above for that stratum's
    /// own `F2`. If every non-empty stratum is within `ε` of its own `F2`, the
    /// sum is within `ε` of the total, so by a union bound the failure
    /// probability is at most `δ` times the number of non-empty strata
    /// (at most `layer_size`).
    pub fn calc_l2(&self) -> f64 {
        if self.update_mode == UnivMonUpdateMode::Terminal {
            return (0..self.layer_size)
                .map(|level| self.l2_sketch_layers[level].get_l2_sqr())
                .sum::<f64>()
                .sqrt();
        }
        self.l2_sketch_layers[0].get_l2()
    }

    /// Returns the estimated entropy.
    pub fn calc_entropy(&self) -> f64 {
        if self.bucket_size == 0 {
            return 0.0;
        }
        let tmp = self.calc_g_sum(
            |x| {
                if x > 0.0 { x * x.log2() } else { 0.0 }
            },
            false,
        );
        (self.bucket_size as f64).log2() - tmp / (self.bucket_size as f64)
    }

    /// Returns the estimated number of distinct keys, `F0`.
    ///
    /// This is the recursion of [`Self::calc_g_sum`] with `g = 1`, and every
    /// heap key counts whatever its Count Sketch estimate: a key is in a heap
    /// only because the stream inserted it. A key inserted with weight 0 is
    /// therefore counted too.
    ///
    /// # Error bound (standard mode)
    ///
    /// Write `S_j` for the keys that reach layer `j` (hash bits `1..=j` of
    /// `BOTTOM_LAYER_FINDER` all set), `H_j ⊆ S_j` for layer `j`'s heap, which
    /// holds `min(|S_j|, k)` keys for `k = heap_size`, and `M_j = S_j \ H_j`
    /// for the keys it misses. Let `L = layer_size` and let `b_{j+1}(i)` be
    /// bit `j+1` of key `i`'s finder hash. The estimate is
    /// `Y_{L−1} = |H_{L−1}|` and `Y_j = 2 Y_{j+1} + Σ_{i∈H_j} (1 − 2 b_{j+1}(i))`.
    ///
    /// When the top layer is complete ([`Self::top_layer_complete`],
    /// `H_{L−1} = S_{L−1}`), telescoping
    /// `|S_j| = 2 |S_{j+1}| + Σ_{i∈S_j} (1 − 2 b_{j+1}(i))` gives
    ///
    /// `Y_0 − F0 = −Σ_{j=0}^{L−2} 2^j Σ_{i∈M_j} (1 − 2 b_{j+1}(i))`.
    ///
    /// `M_j` is decided by layer `j`'s Count Sketch, which sees only `S_j`
    /// (bits `1..=j`) and hashes with its own seed, so it does not depend on
    /// bit `j+1`. Each term is then a fair ±1 independent of everything
    /// before it, the sum is a martingale, and the estimate is unbiased with
    ///
    /// `Var[Y_0] = Σ_{j=0}^{L−2} 4^j E|M_j|`, with `|M_j| = (|S_j| − k)^+`.
    ///
    /// Bounding `E(|S_j| − k)^+` by `E|S_j| = F0 / 2^j` on the low layers
    /// and by `E[X(X−1)(X−2)] / (5k²) ≤ (F0/2^j)³ / (5k²)` on the high ones
    /// (valid for `k ≥ 8`), and splitting where `2^j ≈ 0.45–0.9 · F0/k`,
    /// gives `Var[Y_0] ≤ 1.35 F0² / k ≤ 2 F0² / k` for `k ≥ 8`. Summed
    /// exactly, the identity stays below `0.83 F0² / k` for `k ∈ [8, 256]`.
    ///
    /// Hence, given a complete top layer,
    /// `P(|Y_0 − F0| > ε F0) ≤ 2 / (k ε²)` (Chebyshev, rigorous), and under
    /// a normal approximation of the martingale
    /// `P(|Y_0 − F0| > ε F0) ≈ 2 exp(−ε² k / 4)`. The latter is an
    /// approximation, not a bound.
    ///
    /// The top layer is complete when `|S_{L−1}| ≤ k`. With
    /// `L ≥ ⌈log2(F0_max / k)⌉ + 2`, `E|S_{L−1}| = F0 / 2^{L−1} ≤ k/2` and a
    /// Chernoff bound puts the chance of an incomplete top layer below
    /// `(e/4)^{k/2}`. Completeness is observed rather than assumed: read
    /// [`Self::top_layer_complete`] before relying on the bound.
    ///
    /// Terminal mode reconstructs its candidate sets from the strata, so the
    /// independence argument above does not carry over; it is not covered.
    pub fn calc_card(&self) -> f64 {
        self.calc_g_sum(|_| 1.0, true)
    }

    /// Resets the sketch to its initial state without reallocating.
    /// Zeroes all counters and clears all heaps, matching the Go `Free()` method.
    pub fn free(&mut self) {
        self.bucket_size = 0;
        self.update_mode = UnivMonUpdateMode::Unset;
        self.candidate_complete.fill(true);
        for i in 0..self.layer_size {
            self.l2_sketch_layers[i].clear();
            self.hh_layers[i].clear();
        }
    }

    /// Merges another compatible UnivMon into this one.
    ///
    /// Both sketches must use the same dimensions and update strategy.
    /// Counters are merged first, then candidate frequencies and cached L2
    /// values are rebuilt from the combined counter state.
    pub fn merge(&mut self, other: &UnivMon) {
        assert_eq!(
            self.layer_size, other.layer_size,
            "layer size should be equal to merge"
        );
        assert_eq!(
            (self.sketch_row, self.sketch_col, self.heap_size),
            (other.sketch_row, other.sketch_col, other.heap_size),
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
        self.bucket_size = self
            .bucket_size
            .checked_add(other.bucket_size)
            .expect("merged UnivMon total weight overflowed usize");
        for i in 0..self.layer_size {
            let sources_complete = self.candidate_complete[i] && other.candidate_complete[i];
            let candidate_keys: HashSet<HeapItem> = self.hh_layers[i]
                .heap()
                .iter()
                .chain(other.hh_layers[i].heap())
                .map(|item| item.key.clone())
                .collect();
            let merged_candidates_complete =
                sources_complete && candidate_keys.len() <= self.heap_size;
            self.l2_sketch_layers[i].merge(&other.l2_sketch_layers[i]);
            self.hh_layers[i].clear();
            for key in candidate_keys {
                let input = heap_item_to_sketch_input(&key);
                let count = self.l2_sketch_layers[i].estimate(&input) as i64;
                self.hh_layers[i].update(&input, count);
            }
            self.candidate_complete[i] = merged_candidates_complete;
        }
    }

    /// Returns the heap for one layer.
    pub fn heap_at_layer(&mut self, layer: usize) -> &mut HHHeap {
        &mut self.hh_layers[layer]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataInput, HeapItem};
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use std::collections::HashMap;

    /// Persisted terminal states must be distinguishable before standard ingestion.
    #[test]
    fn standard_update_compatibility_survives_roundtrip_and_reset() {
        let mut sketch = UnivMon::init_univmon(4, 3, 16, 2);
        assert!(sketch.accepts_standard_updates());
        sketch.insert(&DataInput::U64(1), 1);
        assert!(sketch.accepts_standard_updates());
        let standard =
            UnivMon::deserialize_from_bytes(&sketch.serialize_to_bytes().unwrap()).unwrap();
        assert!(standard.accepts_standard_updates());
        sketch.free();
        sketch.fast_insert(&DataInput::U64(1), 1);
        let mut terminal =
            UnivMon::deserialize_from_bytes(&sketch.serialize_to_bytes().unwrap()).unwrap();
        assert!(!terminal.accepts_standard_updates());
        terminal.free();
        assert!(terminal.accepts_standard_updates());
    }

    #[test]
    fn univmon_round_trip_serialization() {
        let mut um = UnivMon::init_univmon(12, 3, 64, 4);
        let flows = [
            ("alpha", 5),
            ("beta", 7),
            ("gamma", 9),
            ("alpha", 3),
            ("delta", 11),
        ];

        for (key, count) in flows {
            um.insert(&DataInput::String(key.to_string()), count);
        }

        let bucket_size_before = um.bucket_size;
        let l1_before = um.calc_l1();
        let l2_before = um.calc_l2();
        let entropy_before = um.calc_entropy();
        let card_before = um.calc_card();

        let encoded = um
            .serialize_to_bytes()
            .expect("serialize UnivMon into MessagePack");
        assert!(!encoded.is_empty(), "serialized bytes should not be empty");
        let data = encoded.clone();

        let decoded =
            UnivMon::deserialize_from_bytes(&data).expect("deserialize UnivMon from MessagePack");

        assert_eq!(um.layer_size, decoded.layer_size);
        assert_eq!(um.sketch_row, decoded.sketch_row);
        assert_eq!(um.sketch_col, decoded.sketch_col);
        assert_eq!(um.heap_size, decoded.heap_size);
        assert_eq!(bucket_size_before, decoded.bucket_size);
        assert!(
            (decoded.calc_l1() - l1_before).abs() < 1e-6,
            "L1 changed after round trip"
        );
        assert!(
            (decoded.calc_l2() - l2_before).abs() < 1e-6,
            "L2 changed after round trip"
        );
        assert!(
            (decoded.calc_entropy() - entropy_before).abs() < 1e-6,
            "entropy changed after round trip"
        );
        assert!(
            (decoded.calc_card() - card_before).abs() < f64::EPSILON,
            "cardinality changed after round trip"
        );
    }

    #[test]
    fn update_populates_bucket_size_and_heavy_hitters() {
        // processing a single hot key should record its weight in the heavy hitter layers
        let mut um = UnivMon::init_univmon(16, 3, 32, 4);
        let key = "alpha";

        for _ in 0..40 {
            um.insert(&DataInput::Str(key), 1);
        }

        assert_eq!(um.bucket_size, 40);

        let idx = um.hh_layers[0]
            .find_heap_item(&HeapItem::String(key.to_owned()))
            .expect("heavy hitter should track key");
        assert!(
            um.hh_layers[0].heap()[idx].count >= 20,
            "expected significant count for heavy hitter, got {}",
            um.hh_layers[0].heap()[idx].count
        );
        assert!(
            um.calc_l1() == 40.0,
            "L1 Norm: get {}, expecting 1",
            um.calc_l1()
        );
        assert!(
            um.calc_card() == 1.0,
            "Cardinality: get {}, expecting 1",
            um.calc_card()
        );
    }

    #[test]
    fn merge_with_combines_heavy_hitters() {
        // merging two sketches should keep contributions from both sides
        let mut left = UnivMon::init_univmon(16, 3, 32, 4);
        let mut right = UnivMon::init_univmon(16, 3, 32, 4);

        let key_left = "left";
        let key_right = "right";

        for _ in 0..25 {
            left.insert(&DataInput::Str(key_left), 1);
        }
        for _ in 0..30 {
            right.insert(&DataInput::Str(key_right), 1);
        }

        left.merge(&right);

        let left_heap = left.heap_at_layer(00);
        let right_heap = right.heap_at_layer(0);
        let idx_left = left_heap
            .find_heap_item(&HeapItem::String(key_left.to_owned()))
            .expect("left key present");
        let idx_right_in_left = left_heap
            .find_heap_item(&HeapItem::String(key_right.to_owned()))
            .expect("left key present");
        let idx_right = right_heap
            .find_heap_item(&HeapItem::String(key_right.to_owned()))
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
        let mut left = UnivMon::init_univmon(1, 3, 1024, 1);
        let mut right = UnivMon::init_univmon(1, 3, 1024, 1);
        left.insert(&DataInput::Str("x"), 100);
        right.insert(&DataInput::Str("x"), 5);
        right.insert(&DataInput::Str("y"), 10);

        left.merge(&right);

        assert_eq!(left.bucket_size, 115);
        let expected_l2 = (105.0_f64.powi(2) + 10.0_f64.powi(2)).sqrt();
        assert!((left.l2_sketch_layers[0].get_l2() - expected_l2).abs() < 1e-9);
        let x = HeapItem::String("x".to_owned());
        let index = left.hh_layers[0]
            .find_heap_item(&x)
            .expect("merged top candidate");
        assert_eq!(left.hh_layers[0].heap()[index].count, 105);
        assert!(left.calc_entropy().is_finite());
        assert!(left.calc_entropy() >= 0.0);
    }

    #[test]
    fn standard_and_terminal_merges_match_one_pass_with_complete_candidates() {
        for terminal_only in [false, true] {
            let mut one_pass = UnivMon::init_univmon(128, 5, 2048, 10);
            let mut left = UnivMon::init_univmon(128, 5, 2048, 10);
            let mut right = UnivMon::init_univmon(128, 5, 2048, 10);
            for observation in 0..2000_u64 {
                let key = DataInput::U64(observation % 64);
                if terminal_only {
                    one_pass.fast_insert(&key, 1);
                    if observation % 2 == 0 {
                        left.fast_insert(&key, 1);
                    } else {
                        right.fast_insert(&key, 1);
                    }
                } else {
                    one_pass.insert(&key, 1);
                    if observation % 2 == 0 {
                        left.insert(&key, 1);
                    } else {
                        right.insert(&key, 1);
                    }
                }
            }

            left.merge(&right);
            assert_eq!(left.bucket_size, one_pass.bucket_size);
            assert!((left.calc_l1() - one_pass.calc_l1()).abs() < 1e-9);
            assert!((left.calc_l2() - one_pass.calc_l2()).abs() < 1e-9);
            assert!((left.calc_card() - one_pass.calc_card()).abs() < 1e-9);
            assert!((left.calc_entropy() - one_pass.calc_entropy()).abs() < 1e-9);
        }
    }

    #[test]
    fn standard_updates_l2_for_every_sampled_layer() {
        let mut sketch = UnivMon::init_univmon(16, 3, 128, 6);
        let key = (0_u64..)
            .map(DataInput::U64)
            .find(|key| {
                let hash = hash64_seeded(BOTTOM_LAYER_FINDER, key);
                sketch.find_bottom_layer_num(hash, sketch.layer_size) >= 2
            })
            .expect("find a key sampled into multiple layers");
        let hash = hash64_seeded(BOTTOM_LAYER_FINDER, &key);
        let bottom = sketch.find_bottom_layer_num(hash, sketch.layer_size);
        sketch.insert(&key, 3);
        for level in 0..=bottom {
            assert_eq!(sketch.l2_sketch_layers[level].get_l2(), 3.0);
        }
    }

    #[test]
    fn terminal_update_touches_one_physical_layer_and_reconstructs_queries() {
        let mut sketch = UnivMon::init_univmon(16, 3, 128, 6);
        let key = (0_u64..)
            .map(DataInput::U64)
            .find(|key| {
                let hash = hash64_seeded(BOTTOM_LAYER_FINDER, key);
                sketch.find_bottom_layer_num(hash, sketch.layer_size) >= 2
            })
            .expect("find a key sampled into multiple layers");
        let hash = hash64_seeded(BOTTOM_LAYER_FINDER, &key);
        let bottom = sketch.find_bottom_layer_num(hash, sketch.layer_size);
        sketch.fast_insert(&key, 3);

        for level in 0..sketch.layer_size {
            let expected = if level == bottom { 3.0 } else { 0.0 };
            assert_eq!(sketch.l2_sketch_layers[level].get_l2(), expected);
        }
        assert_eq!(sketch.calc_l1(), 3.0);
        assert_eq!(sketch.calc_l2(), 3.0);
        assert_eq!(sketch.calc_card(), 1.0);
        assert_eq!(sketch.calc_entropy(), 0.0);
    }

    #[test]
    fn univmon_layers_use_different_seeds() {
        // Verify that different layers in UnivMon use different seeds
        // by checking they produce different hash values
        use crate::common::hash128_seeded;

        let _um = UnivMon::init_univmon(20, 3, 1024, 4);

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
        let mut um = UnivMon::init_univmon(20, 3, 2048, 8);

        for i in 0..20 {
            let key = format!("flow_{i}");
            um.insert(&DataInput::String(key), 1);
        }

        let card = um.calc_card();
        assert!(
            card == 20.0,
            "Cardinality should be positive after insertions, got {card}"
        );
    }

    #[test]
    fn univmon_bucket_size_tracked_correctly() {
        // Verify that bucket_size is correctly tracked with seed configuration
        let mut um = UnivMon::init_univmon(20, 3, 1024, 6);

        let flows = [("flow_a", 100), ("flow_b", 200), ("flow_c", 150)];
        let expected_total = 450;

        for (key, count) in &flows {
            um.insert(&DataInput::Str(key), *count);
        }

        assert_eq!(
            um.bucket_size, expected_total,
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

        let mut um = UnivMon::init_univmon(100, 3, 2048, 16);
        for case in cases {
            um.insert(&DataInput::String(case.0), case.1);
        }

        assert_eq!(um.calc_card(), 10.0, "Cardinality estimation incorrect");
        assert_eq!(um.calc_l1(), 131.0, "L1 estimation incorrect");
    }

    #[test]
    fn test_statistical_accuracy() {
        // 1. Setup: Larger sketch for statistical significance
        // k=50 (top-k size), r=5 (rows), c=1024 (cols), l=10 (layers)
        let mut um = UnivMon::init_univmon(50, 5, 1024, 10);

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
                um.insert(&DataInput::String(key), val);
            }
        }

        // 3. Calculate True Metrics
        let true_l2 = true_l2_sq.sqrt();
        let true_entropy = total_count.log2() - (true_entropy_term / total_count);

        // 4. Get Estimates
        let est_l2 = um.calc_l2();
        let est_entropy = um.calc_entropy();

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
        use crate::sketches::countsketch_topk::l2hh_cell_for_row;
        l2hh_cell_for_row(hash128_seeded(0, key), 0, cols, cols_mask_bits(cols)).1
    }

    /// A key whose Count Sketch estimate is not positive is still a key the
    /// stream inserted; a complete layer must count it.
    #[test]
    fn calc_card_counts_heap_keys_with_nonpositive_estimates() {
        let plus = (0_u64..)
            .map(DataInput::U64)
            .find(|key| layer0_sign(key, 1) == 1)
            .expect("a key with sign +1");
        let minus = (0_u64..)
            .map(DataInput::U64)
            .find(|key| layer0_sign(key, 1) == -1)
            .expect("a key with sign -1");
        // One row, one column: both keys share the cell with opposite signs,
        // so the light key reads 1 - 5 = -4.
        let mut sketch = UnivMon::init_univmon(4, 1, 1, 1);
        sketch.insert(&plus, 5);
        sketch.insert(&minus, 1);
        assert!(sketch.l2_sketch_layers[0].estimate(&minus) <= 0.0);
        assert!(sketch.top_layer_complete());
        assert_eq!(sketch.calc_card(), 2.0);
    }

    /// Standard mode feeds layer 0 every key, so its own Count Sketch L2 is the
    /// estimate; the heap recursion is a weaker estimator of the same value.
    #[test]
    fn calc_l2_reads_layer_zero_directly_in_standard_mode() {
        let mut sketch = UnivMon::init_univmon(16, 5, 256, 8);
        for key in 0..2000_u64 {
            sketch.insert(&DataInput::U64(key), (key % 7 + 1) as i64);
        }
        let direct = sketch.l2_sketch_layers[0].get_l2();
        assert!(
            (sketch.calc_l2() - direct).abs() < 1e-9,
            "calc_l2 {} differs from layer 0's direct estimate {direct}",
            sketch.calc_l2()
        );
    }

    /// Terminal mode keeps disjoint strata, so the squared L2 estimates add.
    #[test]
    fn calc_l2_sums_stratum_estimates_in_terminal_mode() {
        let mut sketch = UnivMon::init_univmon(16, 5, 256, 8);
        for key in 0..2000_u64 {
            sketch.fast_insert(&DataInput::U64(key), (key % 7 + 1) as i64);
        }
        let direct = (0..sketch.layer_size)
            .map(|level| sketch.l2_sketch_layers[level].get_l2_sqr())
            .sum::<f64>()
            .sqrt();
        assert!(
            (sketch.calc_l2() - direct).abs() < 1e-9,
            "calc_l2 {} differs from the summed strata {direct}",
            sketch.calc_l2()
        );
    }

    /// The top layer's completeness is the distinct-count bound's
    /// precondition, so it is readable on its own.
    #[test]
    fn top_layer_complete_tracks_the_top_heap() {
        let mut sketch = UnivMon::init_univmon(4, 3, 64, 1);
        for key in 0..4_u64 {
            sketch.insert(&DataInput::U64(key), 1);
        }
        assert!(sketch.top_layer_complete());
        sketch.insert(&DataInput::U64(4), 1);
        assert!(!sketch.top_layer_complete());
        sketch.free();
        assert!(sketch.top_layer_complete());
    }

    /// Monte Carlo checks of the error bounds documented on `calc_l2` and
    /// `calc_card`. The seed list is fixed, so each trial draws fresh random
    /// key ids instead; a uniformly random hash makes those trials independent.
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
            let mut ids = HashSet::with_capacity(n);
            while ids.len() < n {
                ids.insert(rng.random::<u64>());
            }
            ids.into_iter().zip(ws).collect()
        }

        /// `P(Binomial(r, p) ≥ m)`.
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
                    let (mut sq_err, mut var_bound) = (0.0, 0.0);
                    for _ in 0..trials {
                        let keys = stream(shape, KEYS, &mut rng);
                        let mut sketch = UnivMon::init_univmon(8, ROWS, w, 2);
                        for &(id, weight) in &keys {
                            sketch.insert(&DataInput::U64(id), weight);
                        }
                        let f2: f64 = keys.iter().map(|&(_, f)| (f as f64).powi(2)).sum();
                        let f4: f64 = keys.iter().map(|&(_, f)| (f as f64).powi(4)).sum();
                        let estimate = sketch.calc_l2().powi(2);
                        if (estimate - f2).abs() > EPS * f2 {
                            failures += 1;
                        }
                        // Per-row variance: Var[Z] ≤ 2q (F2² − F4).
                        let L2HH::COUNT(counter) = &sketch.l2_sketch_layers[0];
                        let storage = counter.as_storage();
                        for row in 0..ROWS {
                            let z: f64 = (0..w).map(|c| (storage[row][c] as f64).powi(2)).sum();
                            sq_err += ((z - f2) / f2).powi(2);
                            var_bound += 2.0 * q * (1.0 - f4 / (f2 * f2));
                        }
                    }
                    eprintln!(
                        "L2 {shape:?} w={w}: {failures}/{trials} misses (δ={delta:.4}), row variance {:.5} vs bound {:.5}",
                        sq_err / (trials * ROWS) as f64,
                        var_bound / (trials * ROWS) as f64
                    );
                    assert!(
                        failures as f64 <= allowed_failures(trials, delta),
                        "L2 {shape:?} w={w}: {failures}/{trials} misses beyond ε={EPS}, δ={delta:.4}"
                    );
                    assert!(
                        sq_err <= 1.25 * var_bound,
                        "L2 {shape:?} w={w}: per-row relative variance {:.5} above the bound {:.5}",
                        sq_err / (trials * ROWS) as f64,
                        var_bound / (trials * ROWS) as f64
                    );
                }
            }
        }

        fn check_card(trials: usize) {
            const HEAP: usize = 32;
            const KEYS: usize = 2000;
            const EPS: f64 = 0.5;
            // L ≥ ⌈log2(F0 / k)⌉ + 2.
            let layers = (KEYS as f64 / HEAP as f64).log2().ceil() as usize + 2;
            let chebyshev = 2.0 / (HEAP as f64 * EPS * EPS);
            let normal = 2.0 * (-EPS * EPS * HEAP as f64 / 4.0).exp();
            let mut rng = StdRng::seed_from_u64(0x5EED_00F0);
            let f0 = KEYS as f64;
            for shape in SHAPES {
                let (mut failures, mut incomplete, mut kept) = (0, 0, 0);
                let (mut sum, mut sum_sq) = (0.0, 0.0);
                for _ in 0..trials {
                    let mut sketch = UnivMon::init_univmon(HEAP, 3, 256, layers);
                    for (id, weight) in stream(shape, KEYS, &mut rng) {
                        sketch.insert(&DataInput::U64(id), weight);
                    }
                    if !sketch.top_layer_complete() {
                        incomplete += 1;
                        continue;
                    }
                    kept += 1;
                    let estimate = sketch.calc_card();
                    sum += estimate - f0;
                    sum_sq += (estimate - f0).powi(2);
                    if (estimate - f0).abs() > EPS * f0 {
                        failures += 1;
                    }
                }
                // (e/4)^{k/2} ≈ 2e-3 for k = 32.
                assert!(
                    incomplete as f64 <= allowed_failures(trials, 0.01),
                    "card {shape:?}: top layer incomplete in {incomplete}/{trials} trials"
                );
                let kept_f = kept as f64;
                let variance = sum_sq / kept_f;
                eprintln!(
                    "card {shape:?}: {failures}/{kept} misses (δ={:.4}), {incomplete} incomplete, variance {:.4}·F0²/k, mean error {:.2}",
                    chebyshev.min(normal),
                    variance / (f0 * f0 / HEAP as f64),
                    sum / kept_f
                );
                assert!(
                    failures as f64 <= allowed_failures(kept, chebyshev.min(normal)),
                    "card {shape:?}: {failures}/{kept} misses beyond ε={EPS}"
                );
                assert!(
                    variance <= 1.35 * f0 * f0 / HEAP as f64,
                    "card {shape:?}: variance {:.4}·F0²/k above 1.35",
                    variance / (f0 * f0 / HEAP as f64)
                );
                // Unbiased: the mean error is within four standard errors.
                assert!(
                    (sum / kept_f).abs() <= 4.0 * (variance / kept_f).sqrt(),
                    "card {shape:?}: mean error {:.2} looks biased",
                    sum / kept_f
                );
            }
        }

        #[test]
        fn calc_l2_meets_its_documented_bound() {
            check_l2(150);
        }

        #[test]
        #[ignore = "long Monte Carlo run; the fast variant runs by default"]
        fn calc_l2_meets_its_documented_bound_long() {
            check_l2(3000);
        }

        #[test]
        fn calc_card_meets_its_documented_bound() {
            check_card(60);
        }

        #[test]
        #[ignore = "long Monte Carlo run; the fast variant runs by default"]
        fn calc_card_meets_its_documented_bound_long() {
            check_card(2000);
        }
    }

    #[test]
    fn univmon_random_data_matches_ground_truth_within_configured_tolerance() {
        let mut rng = StdRng::seed_from_u64(0xDEADBEEF);
        let mut um = UnivMon::init_univmon(256, 6, 8192, 16);
        let mut truth: HashMap<String, i64> = HashMap::new();

        for _ in 0..10_000 {
            let key_id = rng.random::<u32>() % 5000;
            let key = format!("key_{key_id}");
            let value = (rng.random::<u32>() % 100 + 1) as i64;
            *truth.entry(key.clone()).or_insert(0) += value;
            um.insert(&DataInput::String(key), value);
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
            ("cardinality", um.calc_card(), true_card, 0.07),
            ("l1", um.calc_l1(), true_l1, 0.05),
            ("l2", um.calc_l2(), true_l2, 0.05),
            ("entropy", um.calc_entropy(), true_entropy, 0.05),
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
