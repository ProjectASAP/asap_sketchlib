//! Hash-layer orchestration for hash-reuse-capable sketches.
//! This module provides a small manager that reuses hashes across compatible sketches.

use crate::{
    Count, CountMin, DataFusion, DefaultXxHasher, FastPath, HyperLogLog, HyperLogLogHIP,
    MatrixHashMode, MatrixHashType, Regular, SketchHasher, SketchInput, Vector1D, Vector2D,
    hash_for_matrix_seeded_with_mode_generic, hash_mode_for_matrix,
    sketch_framework::sketch_catalog::{CountFastOps, CountMinFastOps},
};
use std::marker::PhantomData;

// Mirrors the matrix fast-path hash layout used by Count-Min / Count Sketch.
// HashLayer needs this plan because it computes the shared prehash from SketchInput
// before forwarding it to the sketches in the layer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MatrixHashPlan {
    Packed64,
    Packed128,
    Rows { rows: usize },
}

impl MatrixHashPlan {
    fn from_dimensions(rows: usize, cols: usize) -> Self {
        match hash_mode_for_matrix(rows, cols) {
            MatrixHashMode::Packed64 => MatrixHashPlan::Packed64,
            MatrixHashMode::Packed128 => MatrixHashPlan::Packed128,
            MatrixHashMode::Rows => MatrixHashPlan::Rows { rows },
        }
    }

    fn hash_for_input<H>(&self, input: &SketchInput) -> MatrixHashType
    where
        H: SketchHasher<HashType = MatrixHashType>,
    {
        match *self {
            MatrixHashPlan::Packed64 => {
                hash_for_matrix_seeded_with_mode_generic::<H>(0, MatrixHashMode::Packed64, 1, input)
            }
            MatrixHashPlan::Packed128 => hash_for_matrix_seeded_with_mode_generic::<H>(
                0,
                MatrixHashMode::Packed128,
                1,
                input,
            ),
            MatrixHashPlan::Rows { rows } => {
                hash_for_matrix_seeded_with_mode_generic::<H>(0, MatrixHashMode::Rows, rows, input)
            }
        }
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

    fn matrix_hash_plan(&self) -> Option<MatrixHashPlan> {
        match self {
            HashLayerSketch::CountMinFast(sketch) => Some(MatrixHashPlan::from_dimensions(
                sketch.rows(),
                sketch.cols(),
            )),
            HashLayerSketch::CountFast(sketch) => Some(MatrixHashPlan::from_dimensions(
                sketch.rows(),
                sketch.cols(),
            )),
            HashLayerSketch::HllDf(_)
            | HashLayerSketch::HllRegular(_)
            | HashLayerSketch::HllHip(_) => None,
        }
    }

    pub fn query_with_hash(&self, hash: &MatrixHashType) -> Result<f64, &'static str> {
        match self {
            HashLayerSketch::CountMinFast(sketch) => Ok(sketch.fast_estimate(hash)),
            HashLayerSketch::CountFast(sketch) => Ok(sketch.fast_estimate(hash)),
            HashLayerSketch::HllDf(hll) => Ok(hll.estimate() as f64),
            HashLayerSketch::HllRegular(hll) => Ok(hll.estimate() as f64),
            HashLayerSketch::HllHip(hll) => Ok(hll.estimate() as f64),
        }
    }

    pub fn insert_with_hash(&mut self, hash: &MatrixHashType) -> Result<(), &'static str> {
        match self {
            HashLayerSketch::CountMinFast(sketch) => {
                sketch.fast_insert(hash);
                Ok(())
            }
            HashLayerSketch::CountFast(sketch) => {
                sketch.fast_insert(hash);
                Ok(())
            }
            HashLayerSketch::HllDf(hll) => {
                hll.insert_with_hash(hash.lower_64());
                Ok(())
            }
            HashLayerSketch::HllRegular(hll) => {
                hll.insert_with_hash(hash.lower_64());
                Ok(())
            }
            HashLayerSketch::HllHip(hll) => {
                hll.insert_with_hash(hash.lower_64());
                Ok(())
            }
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
    matrix_hash_plan: Option<MatrixHashPlan>,
    _hasher: PhantomData<H>,
}

impl<H> Default for HashLayer<H>
where
    H: SketchHasher<HashType = MatrixHashType> + 'static,
{
    fn default() -> Self {
        Self::new(vec![
            CountMin::<Vector2D<i32>, FastPath, H>::with_dimensions(3, 4096).into(),
            Count::<Vector2D<i32>, FastPath, H>::with_dimensions(3, 4096).into(),
        ])
        .expect("default HashLayer sketches must share one hash reuse tag")
    }
}

impl<H> HashLayer<H>
where
    H: SketchHasher<HashType = MatrixHashType> + 'static,
{
    pub fn new(lst: Vec<HashLayerSketch>) -> Result<Self, &'static str> {
        let matrix_hash_plan = Self::validate_sketches(&lst)?;
        Ok(HashLayer {
            sketches: Vector1D::from_vec(lst),
            matrix_hash_plan,
            _hasher: PhantomData,
        })
    }

    pub fn push(&mut self, sketch: HashLayerSketch) -> Result<(), &'static str> {
        let sketch_plan = sketch.matrix_hash_plan();
        match (self.matrix_hash_plan, sketch_plan) {
            (Some(layer_plan), Some(sketch_plan)) if layer_plan != sketch_plan => {
                return Err("HashLayer matrix sketches must share the same matrix hash plan");
            }
            (None, Some(sketch_plan)) => {
                self.matrix_hash_plan = Some(sketch_plan);
            }
            _ => {}
        }
        self.sketches.push(sketch);
        Ok(())
    }

    fn validate_sketches(lst: &[HashLayerSketch]) -> Result<Option<MatrixHashPlan>, &'static str> {
        let mut layer_plan = None;
        for sketch in lst {
            if let Some(sketch_plan) = sketch.matrix_hash_plan() {
                match layer_plan {
                    Some(existing_plan) if existing_plan != sketch_plan => {
                        return Err(
                            "HashLayer matrix sketches must share the same matrix hash plan",
                        );
                    }
                    None => layer_plan = Some(sketch_plan),
                    _ => {}
                }
            }
        }
        Ok(layer_plan)
    }

    /// Insert to all sketches using the layer's shared hash computation.
    pub fn insert_all(&mut self, val: &SketchInput) {
        let hash = self.hash_input(val);
        for i in 0..self.sketches.len() {
            self.sketches[i]
                .insert_with_hash(&hash)
                .expect("HashLayer sketch must accept the layer hash type");
        }
    }

    /// Insert to specific sketch indices using the layer's shared hash computation.
    pub fn insert_at(&mut self, indices: &[usize], val: &SketchInput) {
        let hash = self.hash_input(val);
        for &idx in indices {
            if idx < self.sketches.len() {
                self.sketches[idx]
                    .insert_with_hash(&hash)
                    .expect("HashLayer sketch must accept the layer hash type");
            }
        }
    }

    /// Insert a batch of inputs to all sketches using the layer's shared hash computation.
    pub fn bulk_insert_all(&mut self, values: &[SketchInput]) {
        for value in values {
            self.insert_all(value);
        }
    }

    /// Insert a batch of inputs to specific sketch indices using the layer's shared hash computation.
    pub fn bulk_insert_at(&mut self, indices: &[usize], values: &[SketchInput]) {
        for value in values {
            self.insert_at(indices, value);
        }
    }

    /// Insert to all sketches using a pre-computed hash value
    pub fn insert_all_with_hash(&mut self, hash_value: &H::HashType) {
        for i in 0..self.sketches.len() {
            let _ = self.sketches[i].insert_with_hash(hash_value);
        }
    }

    /// Insert to specific sketch indices using a pre-computed hash value
    pub fn insert_at_with_hash(&mut self, indices: &[usize], hash_value: &H::HashType) {
        for &idx in indices {
            if idx < self.sketches.len() {
                let _ = self.sketches[idx].insert_with_hash(hash_value);
            }
        }
    }

    /// Insert a batch of pre-computed hash values to all sketches.
    pub fn bulk_insert_all_with_hashes(&mut self, hash_values: &[H::HashType]) {
        for hash_value in hash_values {
            self.insert_all_with_hash(hash_value);
        }
    }

    /// Insert a batch of pre-computed hash values to specific sketch indices.
    pub fn bulk_insert_at_with_hashes(&mut self, indices: &[usize], hash_values: &[H::HashType]) {
        for hash_value in hash_values {
            self.insert_at_with_hash(indices, hash_value);
        }
    }

    /// Query a specific sketch by index
    pub fn query_at(&self, index: usize, val: &SketchInput) -> Result<f64, &'static str> {
        if index >= self.sketches.len() {
            return Err("Index out of bounds");
        }
        let hash = self.hash_input(val);
        self.sketches[index].query_with_hash(&hash)
    }

    /// Query a specific sketch by index using a pre-computed hash value
    pub fn query_at_with_hash(
        &self,
        index: usize,
        hash_value: &H::HashType,
    ) -> Result<f64, &'static str> {
        if index >= self.sketches.len() {
            return Err("Index out of bounds");
        }
        self.sketches[index].query_with_hash(hash_value)
    }

    /// Query all sketches and return results as a vector
    pub fn query_all(&self, val: &SketchInput) -> Vec<Result<f64, &'static str>> {
        let hash = self.hash_input(val);
        (0..self.sketches.len())
            .map(|i| self.sketches[i].query_with_hash(&hash))
            .collect()
    }

    /// Query all sketches using a pre-computed hash value
    pub fn query_all_with_hash(&self, hash_value: &H::HashType) -> Vec<Result<f64, &'static str>> {
        (0..self.sketches.len())
            .map(|i| self.sketches[i].query_with_hash(hash_value))
            .collect()
    }

    /// Get the number of sketches in the layer
    pub fn len(&self) -> usize {
        self.sketches.len()
    }

    /// Check if the layer is empty
    pub fn is_empty(&self) -> bool {
        self.sketches.is_empty()
    }

    /// Get a reference to a specific sketch
    pub fn get(&self, index: usize) -> Option<&HashLayerSketch> {
        if index < self.sketches.len() {
            Some(&self.sketches[index])
        } else {
            None
        }
    }

    /// Get a mutable reference to a specific sketch
    pub fn get_mut(&mut self, index: usize) -> Option<&mut HashLayerSketch> {
        if index < self.sketches.len() {
            Some(&mut self.sketches[index])
        } else {
            None
        }
    }

    pub fn hash_input(&self, input: &SketchInput) -> H::HashType {
        if let Some(plan) = self.matrix_hash_plan {
            plan.hash_for_input::<H>(input)
        } else {
            MatrixHashType::Packed64(H::hash64_seeded(crate::CANONICAL_HASH_SEED, input))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::sample_zipf_u64;
    use crate::{DataFusion, HyperLogLog};
    use std::collections::HashMap;

    const SAMPLE_SIZE: usize = 10_000;
    const ZIPF_DOMAIN: usize = 1_000;
    const ZIPF_EXPONENT: f64 = 1.5;
    const SEED: u64 = 42;
    const ERROR_TOLERANCE: f64 = 0.1; // 10% error tolerance

    /// Create a baseline HashMap from zipf data
    fn create_baseline(data: &[u64]) -> HashMap<u64, i64> {
        let mut baseline = HashMap::new();
        for &value in data {
            *baseline.entry(value).or_insert(0) += 1;
        }
        baseline
    }

    /// Calculate relative error between estimate and truth
    fn relative_error(estimate: f64, truth: i64) -> f64 {
        if truth == 0 {
            if estimate == 0.0 {
                0.0
            } else {
                1.0 // Maximum error if truth is 0 but estimate is not
            }
        } else {
            ((estimate - truth as f64).abs()) / (truth as f64)
        }
    }

    #[test]
    fn test_hashlayer_insert_all() {
        // Generate zipf data
        let data = sample_zipf_u64(ZIPF_DOMAIN, ZIPF_EXPONENT, SAMPLE_SIZE, SEED);
        let baseline = create_baseline(&data);

        // Create HashLayer with default sketches
        let mut layer: HashLayer<DefaultXxHasher> = HashLayer::default();
        assert_eq!(layer.len(), 2); // CountMin, Count

        // Insert all data
        for &value in &data {
            let input = SketchInput::U64(value);
            layer.insert_all(&input);
        }

        // Test queries for CountMin (index 0) and Count (index 1)
        let mut countmin_errors = Vec::new();
        let mut count_errors = Vec::new();

        for (&key, &true_count) in baseline.iter().take(100) {
            let input = SketchInput::U64(key);

            // Query CountMin sketch (index 0)
            let countmin_est = layer.query_at(0, &input).expect("Query should succeed");
            let countmin_err = relative_error(countmin_est, true_count);
            countmin_errors.push(countmin_err);

            // Query Count sketch (index 1)
            let count_est = layer.query_at(1, &input).expect("Query should succeed");
            let count_err = relative_error(count_est, true_count);
            count_errors.push(count_err);
        }

        // Calculate average errors
        let avg_countmin_error: f64 =
            countmin_errors.iter().sum::<f64>() / countmin_errors.len() as f64;
        let avg_count_error: f64 = count_errors.iter().sum::<f64>() / count_errors.len() as f64;

        println!("Average CountMin error: {avg_countmin_error:.4}");
        println!("Average Count error: {avg_count_error:.4}");

        assert!(
            avg_countmin_error < ERROR_TOLERANCE,
            "CountMin average error {avg_countmin_error:.4} exceeded tolerance {ERROR_TOLERANCE:.4}"
        );
        assert!(
            avg_count_error < ERROR_TOLERANCE,
            "Count average error {avg_count_error:.4} exceeded tolerance {ERROR_TOLERANCE:.4}"
        );
    }

    #[test]
    fn test_hashlayer_insert_at_specific_indices() {
        let data = sample_zipf_u64(ZIPF_DOMAIN, ZIPF_EXPONENT, SAMPLE_SIZE, SEED);
        let baseline = create_baseline(&data);

        let mut layer: HashLayer<DefaultXxHasher> = HashLayer::default();

        // Insert only to CountMin (index 0) and Count (index 1), not HllDf
        for &value in &data {
            let input = SketchInput::U64(value);
            layer.insert_at(&[0, 1], &input);
        }

        // Test that CountMin and Count have data
        let sample_key = *baseline.keys().next().unwrap();
        let input = SketchInput::U64(sample_key);

        let countmin_result = layer.query_at(0, &input);
        assert!(countmin_result.is_ok());
        assert!(countmin_result.unwrap() > 0.0, "CountMin should have data");

        let count_result = layer.query_at(1, &input);
        assert!(count_result.is_ok());
        assert!(count_result.unwrap() > 0.0, "Count should have data");
    }

    #[test]
    fn test_hashlayer_query_all() {
        let data = sample_zipf_u64(ZIPF_DOMAIN, ZIPF_EXPONENT, SAMPLE_SIZE, SEED);

        let mut layer: HashLayer<DefaultXxHasher> = HashLayer::default();

        for &value in &data {
            let input = SketchInput::U64(value);
            layer.insert_all(&input);
        }

        // Query all sketches at once
        let test_value = data[0];
        let input = SketchInput::U64(test_value);
        let results = layer.query_all(&input);

        assert_eq!(results.len(), 2, "Should have 2 results");

        // CountMin and Count should return valid estimates
        assert!(results[0].is_ok(), "CountMin query should succeed");
        assert!(results[1].is_ok(), "Count query should succeed");
    }

    #[test]
    fn test_hashlayer_bulk_insert_variants() {
        let data = sample_zipf_u64(ZIPF_DOMAIN, ZIPF_EXPONENT, SAMPLE_SIZE, SEED);
        let inputs: Vec<SketchInput> = data.iter().copied().map(SketchInput::U64).collect();

        let mut bulk_layer: HashLayer<DefaultXxHasher> = HashLayer::default();
        bulk_layer.bulk_insert_all(&inputs);

        let mut single_layer: HashLayer<DefaultXxHasher> = HashLayer::default();
        for input in &inputs {
            single_layer.insert_all(input);
        }

        let probe = SketchInput::U64(data[0]);
        let bulk_countmin = bulk_layer
            .query_at(0, &probe)
            .expect("bulk query should succeed");
        let single_countmin = single_layer
            .query_at(0, &probe)
            .expect("single query should succeed");
        assert_eq!(bulk_countmin, single_countmin);

        let hashes: Vec<MatrixHashType> = inputs
            .iter()
            .map(|input| bulk_layer.hash_input(input))
            .collect();

        let mut bulk_hash_layer: HashLayer<DefaultXxHasher> = HashLayer::default();
        bulk_hash_layer.bulk_insert_all_with_hashes(&hashes);

        let mut single_hash_layer: HashLayer<DefaultXxHasher> = HashLayer::default();
        for hash in &hashes {
            single_hash_layer.insert_all_with_hash(hash);
        }

        let bulk_hash_count = bulk_hash_layer
            .query_at_with_hash(1, &hashes[0])
            .expect("bulk hash query should succeed");
        let single_hash_count = single_hash_layer
            .query_at_with_hash(1, &hashes[0])
            .expect("single hash query should succeed");
        assert_eq!(bulk_hash_count, single_hash_count);
    }

    #[test]
    fn test_hashlayer_with_hash_optimization() {
        let data = sample_zipf_u64(ZIPF_DOMAIN, ZIPF_EXPONENT, SAMPLE_SIZE, SEED);
        let baseline = create_baseline(&data);

        let mut layer: HashLayer<DefaultXxHasher> = HashLayer::default();

        // Insert using pre-computed hash (the key optimization)
        for &value in &data {
            let input = SketchInput::U64(value);
            let hash = layer.hash_input(&input);
            layer.insert_all_with_hash(&hash);
        }

        // Query using pre-computed hash
        let mut errors = Vec::new();
        for (&key, &true_count) in baseline.iter().take(50) {
            let input = SketchInput::U64(key);
            let hash = layer.hash_input(&input);

            let countmin_est = layer
                .query_at_with_hash(0, &hash)
                .expect("Query should succeed");
            let err = relative_error(countmin_est, true_count);
            errors.push(err);
        }

        let avg_error: f64 = errors.iter().sum::<f64>() / errors.len() as f64;
        println!("Average error with hash optimization: {avg_error:.4}");

        assert!(
            avg_error < ERROR_TOLERANCE,
            "Average error with hash {avg_error:.4} exceeded tolerance {ERROR_TOLERANCE:.4}"
        );
    }

    #[test]
    fn test_hashlayer_hll_cardinality() {
        let data = sample_zipf_u64(ZIPF_DOMAIN, ZIPF_EXPONENT, SAMPLE_SIZE, SEED);
        let baseline = create_baseline(&data);
        let true_cardinality = baseline.len();

        let mut layer: HashLayer<DefaultXxHasher> =
            HashLayer::new(vec![HyperLogLog::<DataFusion>::default().into()])
                .expect("single-family HLL layer should be valid");

        for &value in &data {
            let input = SketchInput::U64(value);
            layer.insert_all(&input);
        }

        // Query HllDf (index 0) for cardinality
        let dummy_input = SketchInput::U64(0); // Value doesn't matter for HLL
        let hll_estimate = layer
            .query_at(0, &dummy_input)
            .expect("HLL query should succeed");

        let cardinality_error = relative_error(hll_estimate, true_cardinality as i64);

        println!("True cardinality: {true_cardinality}");
        println!("HLL estimate: {hll_estimate:.0}");
        println!("Cardinality error: {cardinality_error:.4}");

        assert!(
            cardinality_error < 0.02, // HLL should have ~2% error
            "HLL cardinality error {cardinality_error:.4} too high (true: {true_cardinality}, estimate: {hll_estimate:.0})"
        );
    }

    #[test]
    fn test_hashlayer_direct_access() {
        let mut layer: HashLayer<DefaultXxHasher> = HashLayer::default();

        // Test direct access via get()
        assert!(layer.get(0).is_some(), "Should access sketch at index 0");
        assert!(layer.get(1).is_some(), "Should access sketch at index 1");
        assert!(
            layer.get(2).is_none(),
            "Should return None for out of bounds"
        );

        // Test mutable access via get_mut()
        let sketch = layer.get_mut(0).expect("Should get mutable reference");
        assert_eq!(sketch.sketch_type(), "CountMin");
    }

    #[test]
    fn test_hashlayer_bounds_checking() {
        let layer: HashLayer<DefaultXxHasher> = HashLayer::default();
        let input = SketchInput::U64(42);

        // Test query bounds checking
        let result = layer.query_at(999, &input);
        assert!(result.is_err(), "Should error on out of bounds query");
        assert_eq!(result.unwrap_err(), "Index out of bounds");

        // Test query_at_with_hash bounds checking
        let hash = layer.hash_input(&input);
        let result = layer.query_at_with_hash(999, &hash);
        assert!(result.is_err(), "Should error on out of bounds query");
        assert_eq!(result.unwrap_err(), "Index out of bounds");
    }

    #[test]
    fn test_hashlayer_custom_sketches() {
        // Create a custom HashLayer with specific sketch configurations
        let sketches = vec![
            CountMin::<Vector2D<i32>, FastPath>::with_dimensions(5, 2048).into(),
            Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 2048).into(),
        ];

        let mut layer: HashLayer<DefaultXxHasher> =
            HashLayer::new(sketches).expect("custom HashLayer should be valid");
        assert_eq!(layer.len(), 2);
        assert!(!layer.is_empty());

        let data = sample_zipf_u64(ZIPF_DOMAIN, ZIPF_EXPONENT, SAMPLE_SIZE, SEED);

        for &value in &data {
            let input = SketchInput::U64(value);
            layer.insert_all(&input);
        }

        // Verify both sketches have data
        let test_input = SketchInput::U64(data[0]);
        let result0 = layer.query_at(0, &test_input);
        let result1 = layer.query_at(1, &test_input);

        assert!(result0.is_ok() && result0.unwrap() > 0.0);
        assert!(result1.is_ok() && result1.unwrap() > 0.0);
    }

    #[test]
    fn test_hashlayer_supports_mixed_matrix_and_hll_sketches() {
        let mut layer = HashLayer::<DefaultXxHasher>::new(vec![
            CountMin::<Vector2D<i32>, FastPath>::default().into(),
            HyperLogLog::<DataFusion>::default().into(),
        ])
        .expect("matrix sketches and HLL should share the same layer hash");

        let data = sample_zipf_u64(ZIPF_DOMAIN, ZIPF_EXPONENT, SAMPLE_SIZE, SEED);
        for &value in &data {
            layer.insert_all(&SketchInput::U64(value));
        }

        let results = layer.query_all(&SketchInput::U64(data[0]));
        assert_eq!(results.len(), 2, "Should have 2 results");
        assert!(results[0].is_ok(), "CountMin query should succeed");
        assert!(results[1].is_ok(), "HLL query should succeed");
    }
}
