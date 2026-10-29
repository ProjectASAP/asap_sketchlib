//! `UnivMonTwoTier`: pyramid-structured UnivMon with two-tier sketch
//! dimensions — larger "elephant" layers for heavy hitters and smaller
//! "mouse" layers for the long tail, matching the PromSketch design.

use crate::UnivMon;
use crate::common::Vector1D;
use crate::common::heap::{DEFAULT_HEAP_SIZE, TopKHeap};
use crate::common::{
    DataInput, OwnedDataInput, UNIVMON_BOTTOM_LAYER_SEED_INDEX, hash64_seeded, hash64_seeded_owned,
};
use crate::sketch_framework::univmon::{UNIVMON_MAX_LAYERS, UnivMonUpdateMode};
use crate::sketches::countsketch_l2_norm::CountSketchWithL2Norm;
use std::collections::{HashMap, HashSet};

mod wire;

// ---------------------------------------------------------------------------
// UnivMonTwoTier
// ---------------------------------------------------------------------------

pub(crate) const DEFAULT_ELEPHANT_LAYERS: usize = 8;
pub(crate) const DEFAULT_ELEPHANT_ROWS: usize = 3;
pub(crate) const DEFAULT_ELEPHANT_COLS: usize = 2048;
pub(crate) const DEFAULT_MOUSE_ROWS: usize = 3;
pub(crate) const DEFAULT_MOUSE_COLS: usize = 512;
pub(crate) const DEFAULT_LAYERS: usize = 16;

/// Pyramid-structured UnivMon with two-tier sketch dimensions.
///
/// Layers `0..elephant_layers` ("elephant") use larger sketches for accurate
/// heavy-hitter tracking. Layers `elephant_layers..layers` ("mouse") use
/// smaller sketches, saving memory since deeper layers sample exponentially
/// fewer items.
#[derive(Clone, Debug)]
/// Optimized UnivMon variant with separate elephant and mouse layers.
pub struct UnivMonTwoTier {
    /// Per-layer L2/heavy-hitter sketches.
    pub layer_sketches: Vector1D<CountSketchWithL2Norm>,
    /// Per-layer heavy-hitter heaps.
    pub layer_heaps: Vector1D<TopKHeap>,
    /// Total number of layers.
    pub layers: usize,
    /// Number of elephant layers.
    pub elephant_layers: usize,
    /// Row count for elephant layers.
    pub elephant_rows: usize,
    /// Column count for elephant layers.
    pub elephant_cols: usize,
    /// Row count for mouse layers.
    pub mouse_rows: usize,
    /// Column count for mouse layers.
    pub mouse_cols: usize,
    /// Heap capacity per layer.
    pub heap_size: usize,
    /// Total inserted weight (the exact L1 norm).
    pub total_weight: usize,
    update_mode: UnivMonUpdateMode,
    candidate_complete: Vec<bool>,
}

impl UnivMonTwoTier {
    /// Creates an optimized UnivMon pyramid.
    pub fn new(
        heap_size: usize,
        elephant_layers: usize,
        elephant_rows: usize,
        elephant_cols: usize,
        mouse_rows: usize,
        mouse_cols: usize,
        layers: usize,
    ) -> Self {
        assert!(heap_size > 0, "heap size must be positive");
        assert!(
            elephant_rows > 0 && mouse_rows > 0,
            "sketch row counts must be positive"
        );
        assert!(
            elephant_cols > 0 && mouse_cols > 0,
            "sketch column counts must be positive"
        );
        assert!(layers > 0, "layer count must be positive");
        assert!(
            layers <= UNIVMON_MAX_LAYERS,
            "layer count must be at most UNIVMON_MAX_LAYERS {UNIVMON_MAX_LAYERS}"
        );
        let sk_vec: Vec<CountSketchWithL2Norm> = if layers <= elephant_layers {
            (0..layers)
                .map(|i| {
                    CountSketchWithL2Norm::with_dimensions_and_seed_index(
                        elephant_rows,
                        elephant_cols,
                        i,
                    )
                })
                .collect()
        } else {
            (0..elephant_layers)
                .map(|i| {
                    CountSketchWithL2Norm::with_dimensions_and_seed_index(
                        elephant_rows,
                        elephant_cols,
                        i,
                    )
                })
                .chain((elephant_layers..layers).map(|i| {
                    CountSketchWithL2Norm::with_dimensions_and_seed_index(mouse_rows, mouse_cols, i)
                }))
                .collect()
        };

        let hh_vec: Vec<TopKHeap> = (0..layers).map(|_| TopKHeap::new(heap_size)).collect();

        UnivMonTwoTier {
            layer_sketches: Vector1D::from_vec(sk_vec),
            layer_heaps: Vector1D::from_vec(hh_vec),
            layers,
            elephant_layers,
            elephant_rows,
            elephant_cols,
            mouse_rows,
            mouse_cols,
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
        crate::analytics::ops::add(1);
        self.total_weight = self
            .total_weight
            .checked_add(value as usize)
            .expect("UnivMon total weight overflowed usize");
    }

    /// Creates a pyramid using built-in default dimensions.
    pub fn with_defaults() -> Self {
        Self::new(
            DEFAULT_HEAP_SIZE,
            DEFAULT_ELEPHANT_LAYERS,
            DEFAULT_ELEPHANT_ROWS,
            DEFAULT_ELEPHANT_COLS,
            DEFAULT_MOUSE_ROWS,
            DEFAULT_MOUSE_COLS,
            DEFAULT_LAYERS,
        )
    }

    /// Standard insert: updates sketch + heap at every layer 0..=bottom.
    pub fn update_weighted(&mut self, key: &DataInput, weight: i64) {
        self.begin_update(weight, UnivMonUpdateMode::Standard);
        let h = hash64_seeded(UNIVMON_BOTTOM_LAYER_SEED_INDEX, key);
        let bottom = UnivMon::bottom_layer_for_hash(h, self.layers);
        for i in 0..=bottom {
            let count = self.layer_sketches[i].update_weighted_and_estimate(key, weight);
            if !self.layer_heaps[i].update(key, count as i64) {
                crate::analytics::ops::add(1);
                self.candidate_complete[i] = false;
            }
        }
    }

    /// Joltik-style insert: updates only the terminal CountSketch and heap.
    ///
    /// Logical upper-layer candidate sets are reconstructed at query time.
    /// Do not mix this method with [`Self::update_weighted`] on the same sketch.
    pub fn update_weighted_terminal(&mut self, key: &DataInput, weight: i64) {
        self.begin_update(weight, UnivMonUpdateMode::Terminal);
        let h = hash64_seeded(UNIVMON_BOTTOM_LAYER_SEED_INDEX, key);
        let bottom = UnivMon::bottom_layer_for_hash(h, self.layers);
        let count = self.layer_sketches[bottom].update_weighted_and_estimate(key, weight);
        if !self.layer_heaps[bottom].update(key, count as i64) {
            self.candidate_complete[bottom] = false;
        }
    }

    // -- Query methods (identical to UnivMon) --------------------------------

    /// Computes a g-sum estimate with the UnivMon recurrence; `count_every_key`
    /// selects which heap keys contribute, as in [`UnivMon::estimate_g_sum`].
    pub fn estimate_g_sum<F>(&self, g: F, count_every_key: bool) -> f64
    where
        F: Fn(f64) -> f64,
    {
        crate::analytics::ops::add(1);
        if self.total_weight == 0 && !count_every_key {
            return 0.0;
        }
        if self.update_mode == UnivMonUpdateMode::Terminal {
            return self.calc_terminal_g_sum(g, count_every_key);
        }

        let counted = |count: i64| count_every_key || count > 0;
        // Each `y` slot is written once and read once.
        crate::analytics::ops::add(2 * self.layers);
        let mut y = vec![0.0; self.layers];

        let mut tmp = 0.0;
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
        crate::analytics::ops::add(1);
        self.total_weight as f64
    }

    /// Returns the estimated L2 norm: layer 0's Count Sketch F2 estimate in
    /// standard mode, the summed strata's F2 estimates in terminal mode.
    /// Error bounds: `docs/api/api_univmon_two_tier.md`.
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
        crate::analytics::ops::add(1);
        if self.total_weight == 0 {
            return 0.0;
        }
        let tmp = self.estimate_g_sum(|x| if x > 0.0 { x * x.log2() } else { 0.0 }, false);
        crate::analytics::ops::add(2);
        (self.total_weight as f64).log2() - tmp / (self.total_weight as f64)
    }

    /// Returns the estimated number of distinct keys, counting every heap key
    /// (zero-weight keys included) whatever its frequency estimate.
    /// Error bounds: `docs/api/api_univmon_two_tier.md`.
    pub fn estimate_cardinality(&self) -> f64 {
        self.estimate_g_sum(|_| 1.0, true)
    }

    /// Whether the top layer's heap still holds every key that layer received.
    pub fn top_layer_complete(&self) -> bool {
        self.candidate_complete[self.layers - 1]
    }

    // -- Lifecycle -----------------------------------------------------------

    /// Resets all counters and heaps without deallocating.
    pub fn clear(&mut self) {
        self.total_weight = 0;
        self.update_mode = UnivMonUpdateMode::Unset;
        self.candidate_complete.fill(true);
        for i in 0..self.layers {
            self.layer_sketches[i].clear();
            self.layer_heaps[i].clear();
        }
    }

    /// Merges another compatible pyramid and rebuilds its candidate heaps.
    pub fn merge(&mut self, other: &UnivMonTwoTier) {
        assert_eq!(self.layers, other.layers, "layer size must match for merge");
        assert_eq!(
            (
                self.elephant_layers,
                self.elephant_rows,
                self.elephant_cols,
                self.mouse_rows,
                self.mouse_cols,
                self.heap_size,
            ),
            (
                other.elephant_layers,
                other.elephant_rows,
                other.elephant_cols,
                other.mouse_rows,
                other.mouse_cols,
                other.heap_size,
            ),
            "UnivMon pyramid layouts must match for merge"
        );
        match (self.update_mode, other.update_mode) {
            (UnivMonUpdateMode::Unset, mode) => self.update_mode = mode,
            (_, UnivMonUpdateMode::Unset) => {}
            (left, right) => assert_eq!(
                left, right,
                "cannot merge standard and terminal-only UnivMon states"
            ),
        }
        crate::analytics::ops::add(2);
        self.total_weight = self
            .total_weight
            .checked_add(other.total_weight)
            .expect("merged UnivMon total weight overflowed usize");
        for i in 0..self.layers {
            crate::analytics::ops::add(2);
            let sources_complete = self.candidate_complete[i] && other.candidate_complete[i];
            let candidate_keys: HashSet<OwnedDataInput> = self.layer_heaps[i]
                .heap()
                .iter()
                .chain(other.layer_heaps[i].heap())
                .map(|item| {
                    crate::analytics::ops::add(1);
                    item.key.clone()
                })
                .collect();
            let merged_candidates_complete =
                sources_complete && candidate_keys.len() <= self.heap_size;
            self.layer_sketches[i].merge(&other.layer_sketches[i]);
            self.layer_heaps[i].clear();
            for key in candidate_keys {
                crate::analytics::ops::add(1);
                let input = key.as_data_input();
                let count = self.layer_sketches[i].estimate(&input) as i64;
                self.layer_heaps[i].update(&input, count);
            }
            crate::analytics::ops::add(1);
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

    /// Bytes a pyramid of that shape holds once keys shaped like `key` churn
    /// through every layer, as [`UnivMon::theoretical_memory_bytes`] counts
    /// them with each layer's own dimensions. Saturates at `usize::MAX`.
    #[allow(clippy::too_many_arguments)]
    pub fn theoretical_memory_bytes(
        heap_size: usize,
        elephant_layers: usize,
        elephant_rows: usize,
        elephant_cols: usize,
        mouse_rows: usize,
        mouse_cols: usize,
        layers: usize,
        key: &DataInput,
    ) -> usize {
        let per_layer = TopKHeap::<i64>::theoretical_memory_bytes(heap_size, key)
            .saturating_add(size_of::<bool>());
        let elephants = elephant_layers.min(layers);
        let elephant =
            CountSketchWithL2Norm::theoretical_memory_bytes(elephant_rows, elephant_cols)
                .saturating_add(per_layer);
        let mouse = CountSketchWithL2Norm::theoretical_memory_bytes(mouse_rows, mouse_cols)
            .saturating_add(per_layer);
        size_of::<Self>()
            .saturating_add(elephants.saturating_mul(elephant))
            .saturating_add((layers - elephants).saturating_mul(mouse))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataInput;
    use rand::{Rng, SeedableRng, rngs::StdRng};

    /// Sign layer 0 gives `key` in its single row (`+1` or `-1`).
    fn layer0_sign(key: &DataInput, cols: usize) -> i64 {
        use crate::common::hash128_seeded;
        use crate::common::structures::matrix_storage::cols_mask_bits;
        CountSketchWithL2Norm::cell_for_row(hash128_seeded(0, key), 0, cols, cols_mask_bits(cols)).1
    }

    #[test]
    fn two_tier_estimate_cardinality_counts_heap_keys_with_nonpositive_estimates() {
        let plus = (0_u64..)
            .map(DataInput::U64)
            .find(|key| layer0_sign(key, 1) == 1)
            .expect("a key with sign +1");
        let minus = (0_u64..)
            .map(DataInput::U64)
            .find(|key| layer0_sign(key, 1) == -1)
            .expect("a key with sign -1");
        let mut pyramid = UnivMonTwoTier::new(4, 1, 1, 1, 1, 1, 1);
        pyramid.update_weighted(&plus, 5);
        pyramid.update_weighted(&minus, 1);
        assert!(pyramid.layer_sketches[0].estimate(&minus) <= 0.0);
        assert!(pyramid.top_layer_complete());
        assert_eq!(pyramid.estimate_cardinality(), 2.0);
    }

    #[test]
    fn two_tier_estimate_cardinality_counts_zero_weight_only_streams() {
        for terminal in [false, true] {
            let mut pyramid = UnivMonTwoTier::new(8, 1, 3, 64, 2, 16, 4);
            for id in [1, 2, 1, 3] {
                if terminal {
                    pyramid.update_weighted_terminal(&DataInput::U64(id), 0);
                } else {
                    pyramid.update_weighted(&DataInput::U64(id), 0);
                }
            }
            assert_eq!(pyramid.estimate_l2(), 0.0);
            assert_eq!(pyramid.estimate_cardinality(), 3.0);
        }
    }

    #[test]
    fn two_tier_estimate_l2_reads_layer_zero_directly_in_standard_mode() {
        let mut pyramid = UnivMonTwoTier::new(16, 2, 5, 256, 3, 64, 8);
        for key in 0..2000_u64 {
            pyramid.update_weighted(&DataInput::U64(key), (key % 7 + 1) as i64);
        }
        let direct = pyramid.layer_sketches[0].l2();
        assert!(
            (pyramid.estimate_l2() - direct).abs() < 1e-9,
            "estimate_l2 {} differs from layer 0's direct estimate {direct}",
            pyramid.estimate_l2()
        );
    }

    #[test]
    fn two_tier_estimate_l2_sums_stratum_estimates_in_terminal_mode() {
        let mut pyramid = UnivMonTwoTier::new(16, 2, 5, 256, 3, 64, 8);
        for key in 0..2000_u64 {
            pyramid.update_weighted_terminal(&DataInput::U64(key), (key % 7 + 1) as i64);
        }
        let direct = (0..pyramid.layers)
            .map(|level| pyramid.layer_sketches[level].l2_squared())
            .sum::<f64>()
            .sqrt();
        assert!(
            (pyramid.estimate_l2() - direct).abs() < 1e-9,
            "estimate_l2 {} differs from the summed strata {direct}",
            pyramid.estimate_l2()
        );
    }

    #[test]
    fn two_tier_top_layer_complete_tracks_the_top_heap() {
        let mut pyramid = UnivMonTwoTier::new(4, 1, 3, 64, 2, 16, 1);
        for key in 0..4_u64 {
            pyramid.update_weighted(&DataInput::U64(key), 1);
        }
        assert!(pyramid.top_layer_complete());
        pyramid.update_weighted(&DataInput::U64(4), 1);
        assert!(!pyramid.top_layer_complete());
        pyramid.clear();
        assert!(pyramid.top_layer_complete());
    }

    /// One trial's stream: fresh random key ids with Zipf weights.
    fn zipf_stream(n: usize, rng: &mut StdRng) -> Vec<(u64, i64)> {
        let mut seen = HashSet::with_capacity(n);
        let mut ids = Vec::with_capacity(n);
        while ids.len() < n {
            let id = rng.random::<u64>();
            if seen.insert(id) {
                ids.push(id);
            }
        }
        ids.into_iter()
            .enumerate()
            .map(|(rank, id)| (id, (10_000.0 / ((rank + 1) as f64).powf(1.1)).ceil() as i64))
            .collect()
    }

    /// Failures allowed out of `trials` at a true rate of at most `delta`:
    /// the mean plus four standard deviations, plus one.
    fn allowed_failures(trials: usize, delta: f64) -> f64 {
        let n = trials as f64;
        n * delta + 4.0 * (n * delta * (1.0 - delta)).sqrt() + 1.0
    }

    /// Layer 0 is `5 x 64`, mouse layers `3 x 16`:
    /// `p = 2/(64 * 0.25) = 0.125`, `delta = P(Bin(5, p) >= 3) ~ 0.016`.
    fn check_l2(trials: usize) {
        const EPS: f64 = 0.5;
        const DELTA: f64 = 0.0161;
        let mut rng = StdRng::seed_from_u64(0x5EED_0013);
        let mut failures = 0;
        for _ in 0..trials {
            let keys = zipf_stream(400, &mut rng);
            let mut pyramid = UnivMonTwoTier::new(8, 1, 5, 64, 3, 16, 3);
            for &(id, weight) in &keys {
                pyramid.update_weighted(&DataInput::U64(id), weight);
            }
            let f2: f64 = keys.iter().map(|&(_, f)| (f as f64).powi(2)).sum();
            if (pyramid.estimate_l2().powi(2) - f2).abs() > EPS * f2 {
                failures += 1;
            }
        }
        assert!(
            failures as f64 <= allowed_failures(trials, DELTA),
            "{failures}/{trials} misses beyond eps={EPS}, delta={DELTA}"
        );
    }

    /// Mouse layers much narrower than the elephant ones: the joint bound
    /// `P(error > eps F0 AND complete) <= 1.35/(k eps^2)`, and the ideal estimator
    /// (exact top count restored) has mean `F0` and variance at most `1.35 F0^2/k`.
    fn check_cardinality(trials: usize) {
        const HEAP: usize = 32;
        const KEYS: usize = 2000;
        const EPS: f64 = 0.5;
        // L >= ceil(log2(F0 / k)) + 2.
        let layers = (KEYS as f64 / HEAP as f64).log2().ceil() as usize + 2;
        let mut rng = StdRng::seed_from_u64(0x5EED_00F1);
        let f0 = KEYS as f64;
        let (mut failures, mut incomplete, mut sum, mut sum_sq) = (0, 0, 0.0, 0.0);
        for _ in 0..trials {
            let mut pyramid = UnivMonTwoTier::new(HEAP, 2, 3, 256, 2, 32, layers);
            let mut top_keys = 0;
            for (id, weight) in zipf_stream(KEYS, &mut rng) {
                let key = DataInput::U64(id);
                let h = hash64_seeded(UNIVMON_BOTTOM_LAYER_SEED_INDEX, &key);
                top_keys += usize::from(UnivMon::bottom_layer_for_hash(h, layers) == layers - 1);
                pyramid.update_weighted(&key, weight);
            }
            let complete = pyramid.top_layer_complete();
            incomplete += usize::from(!complete);
            let estimate = pyramid.estimate_cardinality();
            let missing = top_keys - pyramid.layer_heaps[layers - 1].len();
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
            "top layer incomplete in {incomplete}/{trials} trials"
        );
        let n = trials as f64;
        let delta = 1.35 / (HEAP as f64 * EPS * EPS);
        assert!(
            failures as f64 <= allowed_failures(trials, delta),
            "{failures}/{trials} joint misses beyond eps={EPS}"
        );
        let variance = sum_sq / n;
        assert!(
            variance <= 1.35 * f0 * f0 / HEAP as f64,
            "variance {:.3} F0^2/k above 1.35",
            variance / (f0 * f0 / HEAP as f64)
        );
        assert!(
            (sum / n).abs() <= 4.0 * (variance / n).sqrt(),
            "ideal estimator mean error {:.1} looks biased",
            sum / n
        );
    }

    #[test]
    fn two_tier_estimate_l2_meets_its_documented_bound() {
        check_l2(150);
    }

    #[test]
    #[ignore = "long Monte Carlo run; the fast variant runs by default"]
    fn two_tier_estimate_l2_meets_its_documented_bound_long() {
        check_l2(3000);
    }

    #[test]
    fn two_tier_estimate_cardinality_meets_its_documented_bound() {
        check_cardinality(60);
    }

    #[test]
    #[ignore = "long Monte Carlo run; the fast variant runs by default"]
    fn two_tier_estimate_cardinality_meets_its_documented_bound_long() {
        check_cardinality(2000);
    }

    // =======================================================================
    //                    UnivMonTwoTier tests
    // =======================================================================

    #[test]
    fn univmon_two_tier_basic_update_and_query() {
        let mut um = UnivMonTwoTier::with_defaults();

        let cases: Vec<(&str, i64)> = vec![("hello", 10), ("world", 20), ("hello", 5), ("foo", 30)];

        for (key, val) in &cases {
            um.update_weighted(&DataInput::Str(key), *val);
        }

        assert_eq!(um.total_weight, 65);
        assert!(
            (um.estimate_l1() - 65.0).abs() < 1e-6,
            "L1 = {}",
            um.estimate_l1()
        );
        assert_eq!(um.estimate_cardinality(), 3.0);
    }

    #[test]
    fn univmon_two_tier_update_weighted_terminal_matches_standard() {
        // Both insert paths should produce identical sketches.
        // Retain the complete 100-key support so this tests construction
        // equivalence rather than independent candidate-sampling variance.
        let mut standard = UnivMonTwoTier::new(128, 8, 3, 2048, 3, 512, 16);
        let mut fast = UnivMonTwoTier::new(128, 8, 3, 2048, 3, 512, 16);

        for i in 0..500i64 {
            let key = DataInput::I64(i % 100);
            standard.update_weighted(&key, 1);
            fast.update_weighted_terminal(&key, 1);
        }

        assert_eq!(standard.total_weight, fast.total_weight);

        // Standard cumulative layers and Joltik terminal strata must yield
        // the same logical estimates when all candidates are retained.
        let l1_diff = (standard.estimate_l1() - fast.estimate_l1()).abs();
        let card_diff = (standard.estimate_cardinality() - fast.estimate_cardinality()).abs();
        assert!(
            l1_diff / standard.estimate_l1() < 0.10,
            "L1 diverged: std={}, fast={}",
            standard.estimate_l1(),
            fast.estimate_l1()
        );
        assert!(
            card_diff / standard.estimate_cardinality().max(1.0) < 0.15,
            "Card diverged: std={}, fast={}",
            standard.estimate_cardinality(),
            fast.estimate_cardinality()
        );
    }

    #[test]
    fn univmon_two_tier_dimensions() {
        // Verify elephant layers are larger than mouse layers.
        let um = UnivMonTwoTier::new(32, 4, 5, 2048, 3, 256, 8);

        // Layers 0..4 = elephant (5 rows × 2048 cols)
        // Layers 4..8 = mouse   (3 rows × 256 cols)
        assert_eq!(um.layers, 8);
        assert_eq!(um.elephant_layers, 4);
    }

    #[test]
    fn univmon_two_tier_clear_resets_state() {
        let mut um = UnivMonTwoTier::with_defaults();
        for i in 0..100i64 {
            um.update_weighted(&DataInput::I64(i), 10);
        }
        assert!(um.total_weight > 0);

        um.clear();
        assert_eq!(um.total_weight, 0);
        assert!((um.layer_sketches[0].l2()).abs() < 1e-9);
    }

    #[test]
    fn univmon_two_tier_merge_combines_data() {
        let mut one_pass = UnivMonTwoTier::new(128, 8, 3, 2048, 3, 512, 16);
        let mut left = UnivMonTwoTier::new(128, 8, 3, 2048, 3, 512, 16);
        let mut right = UnivMonTwoTier::new(128, 8, 3, 2048, 3, 512, 16);

        for i in 0..50i64 {
            left.update_weighted(&DataInput::I64(i), 10);
            one_pass.update_weighted(&DataInput::I64(i), 10);
        }
        for i in 50..100i64 {
            right.update_weighted(&DataInput::I64(i), 10);
            one_pass.update_weighted(&DataInput::I64(i), 10);
        }

        left.merge(&right);

        assert_eq!(left.total_weight, 1000);
        assert!(left.estimate_entropy().is_finite());
        for level in 0..left.layers {
            let merged_count = &left.layer_sketches[level];
            let one_pass_count = &one_pass.layer_sketches[level];
            assert_eq!(
                merged_count.as_storage().as_slice(),
                one_pass_count.as_storage().as_slice(),
                "counter mismatch at level {level}"
            );
        }
        assert!(
            (left.estimate_l1() - one_pass.estimate_l1()).abs() < 1e-9,
            "merged L1={}, one-pass L1={}",
            left.estimate_l1(),
            one_pass.estimate_l1()
        );
        assert!((left.estimate_l2() - one_pass.estimate_l2()).abs() < 1e-9);
        assert!((left.estimate_cardinality() - one_pass.estimate_cardinality()).abs() < 1e-9);
        assert!((left.estimate_entropy() - one_pass.estimate_entropy()).abs() < 1e-9);
    }

    fn ground_truth(freq: &std::collections::HashMap<i64, i64>) -> (f64, f64, f64, f64) {
        let l1: f64 = freq.values().map(|&v| v as f64).sum();
        let l2: f64 = freq
            .values()
            .map(|&v| (v as f64).powi(2))
            .sum::<f64>()
            .sqrt();
        let card = freq.len() as f64;
        let entropy = if l1 > 0.0 {
            let term: f64 = freq
                .values()
                .map(|&v| {
                    let f = v as f64;
                    if f > 0.0 { f * f.log2() } else { 0.0 }
                })
                .sum();
            l1.log2() - term / l1
        } else {
            0.0
        };
        (l1, l2, card, entropy)
    }

    #[test]
    fn univmon_two_tier_accuracy_zipf() {
        use std::collections::HashMap;

        let mut um = UnivMonTwoTier::new(64, 8, 5, 2048, 3, 512, 16);
        let mut freq: HashMap<i64, i64> = HashMap::new();

        // Heavy hitter
        for _ in 0..5000 {
            um.update_weighted(&DataInput::I64(0), 1);
            *freq.entry(0).or_insert(0) += 1;
        }
        // Medium flows
        for key in 1..=20i64 {
            for _ in 0..200 {
                um.update_weighted(&DataInput::I64(key), 1);
                *freq.entry(key).or_insert(0) += 1;
            }
        }
        // Light flows
        for key in 21..=500i64 {
            um.update_weighted(&DataInput::I64(key), 1);
            *freq.entry(key).or_insert(0) += 1;
        }

        let (true_l1, true_l2, true_card, true_entropy) = ground_truth(&freq);

        let err = |name: &str, est: f64, truth: f64| {
            let rel = (est - truth).abs() / truth.max(1e-12);
            assert!(
                rel < 0.15,
                "UnivMonTwoTier {name}: error {:.2}%, est={est:.2}, truth={truth:.2}",
                rel * 100.0
            );
        };

        err("L1", um.estimate_l1(), true_l1);
        err("L2", um.estimate_l2(), true_l2);
        err("Card", um.estimate_cardinality(), true_card);
        err("Entropy", um.estimate_entropy(), true_entropy);
    }

    #[test]
    fn univmon_two_tier_update_weighted_terminal_accuracy() {
        use std::collections::HashMap;

        let mut um = UnivMonTwoTier::new(64, 8, 5, 2048, 3, 512, 16);
        let mut freq: HashMap<i64, i64> = HashMap::new();

        // Use update_weighted_terminal for everything.
        for _ in 0..3000 {
            um.update_weighted_terminal(&DataInput::I64(0), 1);
            *freq.entry(0).or_insert(0) += 1;
        }
        for key in 1..=50i64 {
            for _ in 0..100 {
                um.update_weighted_terminal(&DataInput::I64(key), 1);
                *freq.entry(key).or_insert(0) += 1;
            }
        }

        let (true_l1, true_l2, true_card, true_entropy) = ground_truth(&freq);

        let err = |name: &str, est: f64, truth: f64| {
            let rel = (est - truth).abs() / truth.max(1e-12);
            assert!(
                rel < 0.15,
                "UnivMonTwoTier terminal {name}: error {:.2}%, est={est:.2}, truth={truth:.2}",
                rel * 100.0
            );
        };

        err("L1", um.estimate_l1(), true_l1);
        err("L2", um.estimate_l2(), true_l2);
        err("Card", um.estimate_cardinality(), true_card);
        err("Entropy", um.estimate_entropy(), true_entropy);
    }

    #[test]
    fn univmon_two_tier_memory_savings_vs_uniform() {
        // Pyramid with 16 layers should use less memory than a uniform
        // UnivMon with the same elephant dimensions at all 16 layers.
        // We verify this by comparing total sketch column counts.
        let elephant_cols = 2048;
        let mouse_cols = 512;
        let elephant_layers = 8;
        let layers = 16;

        let uniform_cols = elephant_cols * layers;
        let pyramid_cols =
            elephant_cols * elephant_layers + mouse_cols * (layers - elephant_layers);

        assert!(
            pyramid_cols < uniform_cols,
            "UnivMonTwoTier ({pyramid_cols}) should use fewer columns than uniform ({uniform_cols})"
        );
        // With defaults: pyramid = 8*2048 + 8*512 = 20480, uniform = 16*2048 = 32768
        // Savings = ~37.5%
        let savings = 1.0 - (pyramid_cols as f64 / uniform_cols as f64);
        assert!(
            savings > 0.30,
            "Expected >30% column savings, got {:.1}%",
            savings * 100.0
        );
    }
}
