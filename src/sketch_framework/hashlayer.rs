//! Hash-layer: group sketches that share a compatible hash so the hash is
//! computed once per insert and fanned out to every sketch in the layer.
//!
//! # Motivation
//!
//! If you have two Count-Min Sketches of the same shape, inserting a key into
//! both normally requires hashing the key twice — producing identical results.
//! `HashLayer` eliminates that redundancy: it hashes each input once and
//! forwards the result to every sketch it manages.
//!
//! # Which sketches can live in a `HashLayer`
//!
//! Only sketches with a true prehashed insertion path are accepted:
//!
//! * `CountMin<_, FastPath, _>` — Count-Min Sketch (fast path)
//! * `Count<_, FastPath, _>` — Count Sketch (fast path)
//! * `HyperLogLog<DataFusion>` / `HyperLogLog<Regular>` / `HyperLogLogHIP`
//!
//! All matrix-backed sketches (CMS / Count) in one layer must agree on the
//! same hash layout (determined by rows × cols dimensions).  HLL sketches can
//! coexist with them because they only consume the lower 64 bits of the shared
//! hash.
//!
//! # Querying
//!
//! Because frequency sketches (CMS, Count) and cardinality sketches (HLL)
//! answer fundamentally different questions, the query API is split:
//!
//! * [`HashLayer::estimate`] / [`HashLayer::estimate_with_hash`] — frequency
//!   estimate for a key (CMS / Count only).
//! * [`HashLayer::cardinality`] — distinct-count estimate (HLL only).

use crate::{
    Count, CountMin, DataFusion, DefaultXxHasher, FastPath, HyperLogLog, HyperLogLogHIP,
    MatrixHashMode, MatrixHashType, Regular, SketchHasher, SketchInput, Vector1D,
    hash_for_matrix_seeded_with_mode_generic, hash_mode_for_matrix,
    sketch_framework::sketch_catalog::{CountFastOps, CountMinFastOps},
};
use std::marker::PhantomData;

// Pre-computed hash configuration derived from matrix dimensions.
// Stored once so `hash_input` can avoid recomputing the mode on every call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HashConfig {
    mode: MatrixHashMode,
    rows: usize,
}

impl HashConfig {
    fn from_dimensions(rows: usize, cols: usize) -> Self {
        HashConfig {
            mode: hash_mode_for_matrix(rows, cols),
            rows,
        }
    }

    fn hash_for_input<H>(&self, input: &SketchInput) -> MatrixHashType
    where
        H: SketchHasher<HashType = MatrixHashType>,
    {
        hash_for_matrix_seeded_with_mode_generic::<H>(0, self.mode, self.rows, input)
    }
}

pub enum HashLayerSketch {
    CountMinFast(Box<dyn CountMinFastOps>),
    CountFast(Box<dyn CountFastOps>),
    HllDf(HyperLogLog<DataFusion>),
    HllRegular(HyperLogLog<Regular>),
    HllHip(HyperLogLogHIP),
}

impl HashLayerSketch {
    pub fn sketch_type(&self) -> &'static str {
        match self {
            HashLayerSketch::CountMinFast(_) => "CountMin",
            HashLayerSketch::CountFast(_) => "Count",
            HashLayerSketch::HllDf(_)
            | HashLayerSketch::HllRegular(_)
            | HashLayerSketch::HllHip(_) => "HLL",
        }
    }

    fn hash_config(&self) -> Option<HashConfig> {
        match self {
            HashLayerSketch::CountMinFast(s) => {
                Some(HashConfig::from_dimensions(s.rows(), s.cols()))
            }
            HashLayerSketch::CountFast(s) => Some(HashConfig::from_dimensions(s.rows(), s.cols())),
            HashLayerSketch::HllDf(_)
            | HashLayerSketch::HllRegular(_)
            | HashLayerSketch::HllHip(_) => None,
        }
    }

    pub fn insert_with_hash(&mut self, hash: &MatrixHashType) {
        match self {
            HashLayerSketch::CountMinFast(sketch) => sketch.fast_insert(hash),
            HashLayerSketch::CountFast(sketch) => sketch.fast_insert(hash),
            HashLayerSketch::HllDf(hll) => hll.insert_with_hash(hash.lower_64()),
            HashLayerSketch::HllRegular(hll) => hll.insert_with_hash(hash.lower_64()),
            HashLayerSketch::HllHip(hll) => hll.insert_with_hash(hash.lower_64()),
        }
    }

    /// Returns the frequency estimate for a key.
    /// `Some(f64)` for CMS / Count, `None` for HLL.
    pub fn estimate_with_hash(&self, hash: &MatrixHashType) -> Option<f64> {
        match self {
            HashLayerSketch::CountMinFast(sketch) => Some(sketch.fast_estimate(hash)),
            HashLayerSketch::CountFast(sketch) => Some(sketch.fast_estimate(hash)),
            HashLayerSketch::HllDf(_)
            | HashLayerSketch::HllRegular(_)
            | HashLayerSketch::HllHip(_) => None,
        }
    }

    /// Returns the cardinality (distinct-count) estimate.
    /// `Some(f64)` for HLL, `None` for CMS / Count.
    pub fn cardinality(&self) -> Option<f64> {
        match self {
            HashLayerSketch::HllDf(hll) => Some(hll.estimate() as f64),
            HashLayerSketch::HllRegular(hll) => Some(hll.estimate() as f64),
            HashLayerSketch::HllHip(hll) => Some(hll.estimate() as f64),
            HashLayerSketch::CountMinFast(_) | HashLayerSketch::CountFast(_) => None,
        }
    }
}

impl<S, H> From<CountMin<S, FastPath, H>> for HashLayerSketch
where
    H: SketchHasher<HashType = MatrixHashType> + 'static,
    S: crate::MatrixStorage + crate::FastPathHasher<H> + 'static,
    S::Counter: Copy
        + PartialOrd
        + From<i32>
        + std::ops::AddAssign
        + crate::common::structure_utils::ToF64
        + 'static,
{
    fn from(value: CountMin<S, FastPath, H>) -> Self {
        HashLayerSketch::CountMinFast(Box::new(value))
    }
}

impl<S, H> From<Count<S, FastPath, H>> for HashLayerSketch
where
    H: SketchHasher<HashType = MatrixHashType> + 'static,
    S: crate::MatrixStorage + crate::FastPathHasher<H> + 'static,
    S::Counter: crate::sketches::count::CountSketchCounter + 'static,
{
    fn from(value: Count<S, FastPath, H>) -> Self {
        HashLayerSketch::CountFast(Box::new(value))
    }
}

impl From<HyperLogLog<DataFusion>> for HashLayerSketch {
    fn from(value: HyperLogLog<DataFusion>) -> Self {
        HashLayerSketch::HllDf(value)
    }
}

impl From<HyperLogLog<Regular>> for HashLayerSketch {
    fn from(value: HyperLogLog<Regular>) -> Self {
        HashLayerSketch::HllRegular(value)
    }
}

impl From<HyperLogLogHIP> for HashLayerSketch {
    fn from(value: HyperLogLogHIP) -> Self {
        HashLayerSketch::HllHip(value)
    }
}

pub struct HashLayer<H = DefaultXxHasher>
where
    H: SketchHasher<HashType = MatrixHashType> + 'static,
{
    sketches: Vector1D<HashLayerSketch>,
    hash_config: Option<HashConfig>,
    _hasher: PhantomData<H>,
}

impl<H> HashLayer<H>
where
    H: SketchHasher<HashType = MatrixHashType> + 'static,
{
    pub fn new(sketches: Vec<HashLayerSketch>) -> Result<Self, &'static str> {
        let hash_config = Self::validate_sketches(&sketches)?;
        Ok(HashLayer {
            sketches: Vector1D::from_vec(sketches),
            hash_config,
            _hasher: PhantomData,
        })
    }

    pub fn push(&mut self, sketch: HashLayerSketch) -> Result<(), &'static str> {
        let sketch_cfg = sketch.hash_config();
        match (self.hash_config, sketch_cfg) {
            (Some(layer_cfg), Some(sketch_cfg)) if layer_cfg != sketch_cfg => {
                return Err("all matrix sketches in a HashLayer must share the same dimensions");
            }
            (None, Some(sketch_cfg)) => {
                self.hash_config = Some(sketch_cfg);
            }
            _ => {}
        }
        self.sketches.push(sketch);
        Ok(())
    }

    fn validate_sketches(sketches: &[HashLayerSketch]) -> Result<Option<HashConfig>, &'static str> {
        let mut layer_cfg = None;
        for sketch in sketches {
            if let Some(cfg) = sketch.hash_config() {
                match layer_cfg {
                    Some(existing) if existing != cfg => {
                        return Err(
                            "all matrix sketches in a HashLayer must share the same dimensions",
                        );
                    }
                    None => layer_cfg = Some(cfg),
                    _ => {}
                }
            }
        }
        Ok(layer_cfg)
    }

    // -- Hashing --------------------------------------------------------------

    /// Compute the shared hash for an input using this layer's hash
    /// configuration and hasher `H`.
    pub fn hash_input(&self, input: &SketchInput) -> H::HashType {
        if let Some(cfg) = self.hash_config {
            cfg.hash_for_input::<H>(input)
        } else {
            MatrixHashType::Packed64(H::hash64_seeded(crate::CANONICAL_HASH_SEED, input))
        }
    }

    // -- Insertion -------------------------------------------------------------

    /// Hash `val` once and insert into every sketch in the layer.
    pub fn insert(&mut self, val: &SketchInput) {
        let hash = self.hash_input(val);
        for i in 0..self.sketches.len() {
            self.sketches[i].insert_with_hash(&hash);
        }
    }

    /// Insert a pre-computed hash into every sketch in the layer.
    pub fn insert_with_hash(&mut self, hash: &H::HashType) {
        for i in 0..self.sketches.len() {
            self.sketches[i].insert_with_hash(hash);
        }
    }

    /// Hash `val` once and insert into the sketches at `indices` only.
    pub fn insert_at(&mut self, indices: &[usize], val: &SketchInput) {
        let hash = self.hash_input(val);
        for &idx in indices {
            if idx < self.sketches.len() {
                self.sketches[idx].insert_with_hash(&hash);
            }
        }
    }

    /// Insert a pre-computed hash into the sketches at `indices` only.
    pub fn insert_at_with_hash(&mut self, indices: &[usize], hash: &H::HashType) {
        for &idx in indices {
            if idx < self.sketches.len() {
                self.sketches[idx].insert_with_hash(hash);
            }
        }
    }

    /// Insert a batch of inputs into every sketch in the layer.
    pub fn bulk_insert(&mut self, values: &[SketchInput]) {
        for value in values {
            self.insert(value);
        }
    }

    /// Insert a batch of pre-computed hashes into every sketch in the layer.
    pub fn bulk_insert_with_hashes(&mut self, hashes: &[H::HashType]) {
        for hash in hashes {
            self.insert_with_hash(hash);
        }
    }

    /// Insert a batch of inputs into the sketches at `indices` only.
    pub fn bulk_insert_at(&mut self, indices: &[usize], values: &[SketchInput]) {
        for value in values {
            self.insert_at(indices, value);
        }
    }

    /// Insert a batch of pre-computed hashes into the sketches at `indices` only.
    pub fn bulk_insert_at_with_hashes(&mut self, indices: &[usize], hashes: &[H::HashType]) {
        for hash in hashes {
            self.insert_at_with_hash(indices, hash);
        }
    }

    // -- Querying: frequency --------------------------------------------------

    /// Frequency estimate for a key at the given sketch index.
    ///
    /// Returns an error if the index is out of bounds or the sketch is not a
    /// frequency sketch (CMS / Count).
    pub fn estimate(&self, index: usize, val: &SketchInput) -> Result<f64, &'static str> {
        if index >= self.sketches.len() {
            return Err("index out of bounds");
        }
        let hash = self.hash_input(val);
        self.sketches[index]
            .estimate_with_hash(&hash)
            .ok_or("sketch at this index is not a frequency sketch")
    }

    /// Frequency estimate using a pre-computed hash.
    pub fn estimate_with_hash(
        &self,
        index: usize,
        hash: &H::HashType,
    ) -> Result<f64, &'static str> {
        if index >= self.sketches.len() {
            return Err("index out of bounds");
        }
        self.sketches[index]
            .estimate_with_hash(hash)
            .ok_or("sketch at this index is not a frequency sketch")
    }

    // -- Querying: cardinality ------------------------------------------------

    /// Cardinality (distinct-count) estimate at the given sketch index.
    ///
    /// Returns an error if the index is out of bounds or the sketch is not an
    /// HLL variant.
    pub fn cardinality(&self, index: usize) -> Result<f64, &'static str> {
        if index >= self.sketches.len() {
            return Err("index out of bounds");
        }
        self.sketches[index]
            .cardinality()
            .ok_or("sketch at this index is not a cardinality sketch")
    }

    // -- Accessors ------------------------------------------------------------

    pub fn len(&self) -> usize {
        self.sketches.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sketches.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&HashLayerSketch> {
        if index < self.sketches.len() {
            Some(&self.sketches[index])
        } else {
            None
        }
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut HashLayerSketch> {
        if index < self.sketches.len() {
            Some(&mut self.sketches[index])
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::sample_zipf_u64;
    use crate::{DataFusion, HyperLogLog, Vector2D};
    use std::collections::HashMap;

    const SAMPLE_SIZE: usize = 10_000;
    const ZIPF_DOMAIN: usize = 1_000;
    const ZIPF_EXPONENT: f64 = 1.5;
    const SEED: u64 = 42;
    const ERROR_TOLERANCE: f64 = 0.1;

    fn create_baseline(data: &[u64]) -> HashMap<u64, i64> {
        let mut baseline = HashMap::new();
        for &value in data {
            *baseline.entry(value).or_insert(0) += 1;
        }
        baseline
    }

    fn relative_error(estimate: f64, truth: i64) -> f64 {
        if truth == 0 {
            if estimate == 0.0 { 0.0 } else { 1.0 }
        } else {
            ((estimate - truth as f64).abs()) / (truth as f64)
        }
    }

    fn default_layer() -> HashLayer<DefaultXxHasher> {
        HashLayer::new(vec![
            CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 4096).into(),
            Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 4096).into(),
        ])
        .expect("compatible sketches")
    }

    #[test]
    fn test_insert_and_estimate() {
        let data = sample_zipf_u64(ZIPF_DOMAIN, ZIPF_EXPONENT, SAMPLE_SIZE, SEED);
        let baseline = create_baseline(&data);

        let mut layer = default_layer();
        assert_eq!(layer.len(), 2);

        for &value in &data {
            layer.insert(&SketchInput::U64(value));
        }

        let mut cms_errors = Vec::new();
        let mut cs_errors = Vec::new();

        for (&key, &true_count) in baseline.iter().take(100) {
            let input = SketchInput::U64(key);

            let cms_est = layer.estimate(0, &input).expect("CMS estimate");
            cms_errors.push(relative_error(cms_est, true_count));

            let cs_est = layer.estimate(1, &input).expect("CS estimate");
            cs_errors.push(relative_error(cs_est, true_count));
        }

        let avg_cms = cms_errors.iter().sum::<f64>() / cms_errors.len() as f64;
        let avg_cs = cs_errors.iter().sum::<f64>() / cs_errors.len() as f64;

        assert!(avg_cms < ERROR_TOLERANCE, "CMS avg error {avg_cms:.4}");
        assert!(avg_cs < ERROR_TOLERANCE, "CS avg error {avg_cs:.4}");
    }

    #[test]
    fn test_insert_at() {
        let data = sample_zipf_u64(ZIPF_DOMAIN, ZIPF_EXPONENT, SAMPLE_SIZE, SEED);
        let baseline = create_baseline(&data);

        let mut layer = default_layer();

        for &value in &data {
            layer.insert_at(&[0], &SketchInput::U64(value));
        }

        let sample_key = *baseline.keys().next().unwrap();
        let input = SketchInput::U64(sample_key);

        let cms_est = layer.estimate(0, &input).expect("CMS estimate");
        assert!(cms_est > 0.0, "CMS at index 0 should have data");

        let cs_est = layer.estimate(1, &input).expect("CS estimate");
        assert_eq!(cs_est, 0.0, "CS at index 1 should be empty");
    }

    #[test]
    fn test_insert_with_hash_matches_insert() {
        let data = sample_zipf_u64(ZIPF_DOMAIN, ZIPF_EXPONENT, SAMPLE_SIZE, SEED);

        let mut layer_a = default_layer();
        let mut layer_b = default_layer();

        for &value in &data {
            let input = SketchInput::U64(value);
            layer_a.insert(&input);

            let hash = layer_b.hash_input(&input);
            layer_b.insert_with_hash(&hash);
        }

        let probe = SketchInput::U64(data[0]);
        let hash = layer_a.hash_input(&probe);

        let est_a = layer_a.estimate(0, &probe).unwrap();
        let est_b = layer_b.estimate_with_hash(0, &hash).unwrap();
        assert_eq!(est_a, est_b);
    }

    #[test]
    fn test_hll_cardinality() {
        let data = sample_zipf_u64(ZIPF_DOMAIN, ZIPF_EXPONENT, SAMPLE_SIZE, SEED);
        let baseline = create_baseline(&data);
        let true_cardinality = baseline.len();

        let mut layer: HashLayer<DefaultXxHasher> =
            HashLayer::new(vec![HyperLogLog::<DataFusion>::default().into()])
                .expect("HLL-only layer");

        for &value in &data {
            layer.insert(&SketchInput::U64(value));
        }

        let hll_est = layer.cardinality(0).expect("HLL cardinality");
        let err = relative_error(hll_est, true_cardinality as i64);

        assert!(
            err < 0.02,
            "HLL cardinality error {err:.4} too high \
             (true: {true_cardinality}, estimate: {hll_est:.0})"
        );
    }

    #[test]
    fn test_estimate_on_hll_returns_error() {
        let layer: HashLayer<DefaultXxHasher> =
            HashLayer::new(vec![HyperLogLog::<DataFusion>::default().into()])
                .expect("HLL-only layer");

        let result = layer.estimate(0, &SketchInput::U64(42));
        assert!(result.is_err());
    }

    #[test]
    fn test_cardinality_on_cms_returns_error() {
        let layer = default_layer();
        let result = layer.cardinality(0);
        assert!(result.is_err());
    }

    #[test]
    fn test_direct_access() {
        let mut layer = default_layer();

        assert!(layer.get(0).is_some());
        assert!(layer.get(1).is_some());
        assert!(layer.get(2).is_none());

        let sketch = layer.get_mut(0).expect("mutable ref");
        assert_eq!(sketch.sketch_type(), "CountMin");
    }

    #[test]
    fn test_bounds_checking() {
        let layer = default_layer();

        assert!(layer.estimate(999, &SketchInput::U64(0)).is_err());
        assert!(layer.cardinality(999).is_err());

        let hash = layer.hash_input(&SketchInput::U64(0));
        assert!(layer.estimate_with_hash(999, &hash).is_err());
    }

    #[test]
    fn test_custom_dimensions() {
        let mut layer: HashLayer<DefaultXxHasher> = HashLayer::new(vec![
            CountMin::<Vector2D<i32>, FastPath>::with_dimensions(5, 2048).into(),
            Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 2048).into(),
        ])
        .expect("compatible sketches");
        assert_eq!(layer.len(), 2);
        assert!(!layer.is_empty());

        let data = sample_zipf_u64(ZIPF_DOMAIN, ZIPF_EXPONENT, SAMPLE_SIZE, SEED);
        for &value in &data {
            layer.insert(&SketchInput::U64(value));
        }

        let input = SketchInput::U64(data[0]);
        assert!(layer.estimate(0, &input).unwrap() > 0.0);
        assert!(layer.estimate(1, &input).unwrap() > 0.0);
    }

    #[test]
    fn test_mixed_matrix_and_hll() {
        let mut layer = HashLayer::<DefaultXxHasher>::new(vec![
            CountMin::<Vector2D<i32>, FastPath>::default().into(),
            HyperLogLog::<DataFusion>::default().into(),
        ])
        .expect("CMS + HLL layer");

        let data = sample_zipf_u64(ZIPF_DOMAIN, ZIPF_EXPONENT, SAMPLE_SIZE, SEED);
        let baseline = create_baseline(&data);

        for &value in &data {
            layer.insert(&SketchInput::U64(value));
        }

        let cms_est = layer
            .estimate(0, &SketchInput::U64(data[0]))
            .expect("CMS estimate");
        assert!(cms_est > 0.0);

        let card = layer.cardinality(1).expect("HLL cardinality");
        let err = relative_error(card, baseline.len() as i64);
        assert!(err < 0.05, "HLL error {err:.4}");
    }

    #[test]
    fn test_push_compatible() {
        let mut layer: HashLayer<DefaultXxHasher> = HashLayer::new(vec![
            CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 4096).into(),
        ])
        .expect("single CMS");

        let result = layer.push(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 4096).into());
        assert!(result.is_ok());
        assert_eq!(layer.len(), 2);
    }

    #[test]
    fn test_push_incompatible_rejected() {
        let mut layer: HashLayer<DefaultXxHasher> = HashLayer::new(vec![
            CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 4096).into(),
        ])
        .expect("single CMS");

        let result = layer.push(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 2048).into());
        assert!(result.is_err());
    }
}
