use crate::{
    Count, CountMin, HllDf, LASTSTATE, SketchInput, Vector1D, hash_it_to_128, input::AnySketch,
};

pub struct HashLayer {
    sketches: Vector1D<AnySketch>,
}

impl Default for HashLayer {
    fn default() -> Self {
        Self::new(vec![
            AnySketch::CountMin(CountMin::default()),
            AnySketch::Count(Count::default()),
            AnySketch::HllDf(HllDf::default()),
        ])
    }
}

impl HashLayer {
    pub fn new(lst: Vec<AnySketch>) -> Self {
        HashLayer {
            sketches: Vector1D::from_vec(lst),
        }
    }

    /// Insert to all sketches using a shared hash computation
    pub fn insert_all(&mut self, val: &SketchInput) {
        let hashed_val = hash_it_to_128(LASTSTATE, val);
        self.insert_all_with_hash(hashed_val);
    }

    /// Insert to specific sketch indices using a shared hash computation
    pub fn insert_at(&mut self, indices: &[usize], val: &SketchInput) {
        let hashed_val = hash_it_to_128(LASTSTATE, val);
        self.insert_at_with_hash(indices, hashed_val);
    }

    /// Insert to all sketches using a pre-computed hash value
    pub fn insert_all_with_hash(&mut self, hash_value: u128) {
        for i in 0..self.sketches.len() {
            self.sketches[i].insert_with_hash(hash_value);
        }
    }

    /// Insert to specific sketch indices using a pre-computed hash value
    pub fn insert_at_with_hash(&mut self, indices: &[usize], hash_value: u128) {
        for &idx in indices {
            if idx < self.sketches.len() {
                self.sketches[idx].insert_with_hash(hash_value);
            }
        }
    }

    /// Query a specific sketch by index
    pub fn query_at(&self, index: usize, val: &SketchInput) -> Result<f64, &'static str> {
        if index >= self.sketches.len() {
            return Err("Index out of bounds");
        }
        let hashed_val = hash_it_to_128(LASTSTATE, val);
        self.sketches[index].query_with_hash(hashed_val)
    }

    /// Query a specific sketch by index using a pre-computed hash value
    pub fn query_at_with_hash(&self, index: usize, hash_value: u128) -> Result<f64, &'static str> {
        if index >= self.sketches.len() {
            return Err("Index out of bounds");
        }
        self.sketches[index].query_with_hash(hash_value)
    }

    /// Query all sketches and return results as a vector
    pub fn query_all(&self, val: &SketchInput) -> Vec<Result<f64, &'static str>> {
        let hashed_val = hash_it_to_128(LASTSTATE, val);
        (0..self.sketches.len())
            .map(|i| self.sketches[i].query_with_hash(hashed_val))
            .collect()
    }

    /// Query all sketches using a pre-computed hash value
    pub fn query_all_with_hash(&self, hash_value: u128) -> Vec<Result<f64, &'static str>> {
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
    pub fn get(&self, index: usize) -> Option<&AnySketch> {
        if index < self.sketches.len() {
            Some(&self.sketches[index])
        } else {
            None
        }
    }

    /// Get a mutable reference to a specific sketch
    pub fn get_mut(&mut self, index: usize) -> Option<&mut AnySketch> {
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
        let mut layer = HashLayer::default();
        assert_eq!(layer.len(), 3); // CountMin, Count, HllDf

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

        let mut layer = HashLayer::default();

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

        let mut layer = HashLayer::default();

        for &value in &data {
            let input = SketchInput::U64(value);
            layer.insert_all(&input);
        }

        // Query all sketches at once
        let test_value = data[0];
        let input = SketchInput::U64(test_value);
        let results = layer.query_all(&input);

        assert_eq!(results.len(), 3, "Should have 3 results");

        // CountMin and Count should return valid estimates
        assert!(results[0].is_ok(), "CountMin query should succeed");
        assert!(results[1].is_ok(), "Count query should succeed");

        // HllDf returns cardinality (should also succeed)
        assert!(results[2].is_ok(), "HllDf query should succeed");
    }

    #[test]
    fn test_hashlayer_with_hash_optimization() {
        let data = sample_zipf_u64(ZIPF_DOMAIN, ZIPF_EXPONENT, SAMPLE_SIZE, SEED);
        let baseline = create_baseline(&data);

        let mut layer = HashLayer::default();

        // Insert using pre-computed hash (the key optimization)
        for &value in &data {
            let input = SketchInput::U64(value);
            let hash = hash_it_to_128(LASTSTATE, &input);
            layer.insert_all_with_hash(hash);
        }

        // Query using pre-computed hash
        let mut errors = Vec::new();
        for (&key, &true_count) in baseline.iter().take(50) {
            let input = SketchInput::U64(key);
            let hash = hash_it_to_128(LASTSTATE, &input);

            let countmin_est = layer
                .query_at_with_hash(0, hash)
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

        let mut layer = HashLayer::default();

        for &value in &data {
            let input = SketchInput::U64(value);
            layer.insert_all(&input);
        }

        // Query HllDf (index 2) for cardinality
        let dummy_input = SketchInput::U64(0); // Value doesn't matter for HLL
        let hll_estimate = layer
            .query_at(2, &dummy_input)
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
        let mut layer = HashLayer::default();

        // Test direct access via get()
        assert!(layer.get(0).is_some(), "Should access sketch at index 0");
        assert!(layer.get(1).is_some(), "Should access sketch at index 1");
        assert!(layer.get(2).is_some(), "Should access sketch at index 2");
        assert!(
            layer.get(3).is_none(),
            "Should return None for out of bounds"
        );

        // Test mutable access via get_mut()
        let sketch = layer.get_mut(0).expect("Should get mutable reference");
        assert_eq!(sketch.sketch_type(), "CountMin");
    }

    #[test]
    fn test_hashlayer_bounds_checking() {
        let layer = HashLayer::default();
        let input = SketchInput::U64(42);

        // Test query bounds checking
        let result = layer.query_at(999, &input);
        assert!(result.is_err(), "Should error on out of bounds query");
        assert_eq!(result.unwrap_err(), "Index out of bounds");

        // Test query_at_with_hash bounds checking
        let hash = hash_it_to_128(LASTSTATE, &input);
        let result = layer.query_at_with_hash(999, hash);
        assert!(result.is_err(), "Should error on out of bounds query");
        assert_eq!(result.unwrap_err(), "Index out of bounds");
    }

    #[test]
    fn test_hashlayer_custom_sketches() {
        // Create a custom HashLayer with specific sketch configurations
        let sketches = vec![
            AnySketch::CountMin(CountMin::with_dimensions(5, 2048)),
            AnySketch::Count(Count::with_dimensions(5, 2048)),
        ];

        let mut layer = HashLayer::new(sketches);
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
}
