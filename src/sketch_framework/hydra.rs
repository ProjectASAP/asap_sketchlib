use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::input::{HydraCounter, HydraQuery};
use crate::sketches::countmin::CountMin;
use crate::{SketchInput, Vector2D, hash_it_to_128};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hydra {
    pub row_num: usize,
    pub col_num: usize,
    pub sketches: Vector2D<HydraCounter>,
    pub type_to_clone: HydraCounter,
}

impl Default for Hydra {
    fn default() -> Self {
        Hydra::with_dimensions(3, 32, HydraCounter::CM(CountMin::default()))
    }
}

impl Hydra {
    pub fn with_dimensions(r: usize, c: usize, sketch_type: HydraCounter) -> Self {
        let mut h = Hydra {
            row_num: r,
            col_num: c,
            sketches: Vector2D::init(r, c),
            type_to_clone: sketch_type.clone(),
        };
        h.sketches.fill(sketch_type);
        h
    }

    /// Assume key is a string that aggregate different keys
    /// with ";" for now
    pub fn update(&mut self, key: &str, value: &SketchInput) {
        let parts: Vec<&str> = key.split(';').filter(|s| !s.is_empty()).collect();
        let n = parts.len();
        let mut result = Vec::new();
        for i in 1..(1 << n) {
            let mut current_combination: Vec<&str> = Vec::new();
            for j in 0..n {
                if (i >> j) & 1 == 1 {
                    current_combination.push(parts[j]);
                }
            }
            result.push(current_combination.join(";"));
        }
        // println!("UPDATE: generated subkeys: {:?}", result);
        for i in 0..self.row_num {
            for subkey in &result {
                let hash = hash_it_to_128(i, &SketchInput::String(subkey.to_string()));
                let bucket = (hash as usize) % self.col_num;
                // println!("UPDATE: row={}, subkey={}, bucket={}", i, subkey, bucket);
                self.sketches[i][bucket].insert(value);
            }
        }
    }

    /// Query the Hydra sketch for a specific subpopulation
    ///
    /// # Arguments
    /// * `key` - The subpopulation key as a vector of dimension values (e.g., ["city", "device"])
    /// * `query` - The query type (Frequency, Quantile, Cardinality, etc.)
    ///
    /// # Returns
    /// The estimated statistic (median of r row estimates)
    ///
    /// # Algorithm
    /// 1. Hash the key to r different sketch instances (one per row)
    /// 2. Query each sketch instance
    /// 3. Return the median of the r estimates
    ///
    /// This follows the Hydra paper's query algorithm for robust estimation.
    pub fn query_key(&self, key: Vec<&str>, query: &HydraQuery) -> f64 {
        let mut estimates = Vec::with_capacity(self.row_num);
        let key_string = key.join(";");

        // Query each row and collect estimates
        for i in 0..self.row_num {
            let hash_value = hash_it_to_128(i, &SketchInput::String(key_string.clone()));
            let col_index = (hash_value as usize) % self.col_num;
            match self.sketches[i][col_index].query(query) {
                Ok(v) => estimates.push(v),
                Err(_) => (), // Skip failed queries (type mismatch)
            }
        }

        // If all queries failed, return 0.0
        if estimates.is_empty() {
            return 0.0;
        }

        // Return median estimate for robustness (as per Hydra paper)
        estimates.sort_by(|a, b| match a.partial_cmp(b) {
            Some(ordering) => ordering,
            None => Ordering::Equal,
        });

        let mid = estimates.len() / 2;
        if estimates.len() % 2 == 0 {
            (estimates[mid - 1] + estimates[mid]) / 2.0
        } else {
            estimates[mid]
        }
    }

    /// Convenience method for querying frequency (for CountMin-based Hydra)
    /// This is a wrapper around query_key with HydraQuery::Frequency
    pub fn query_frequency(&self, key: Vec<&str>, value: &SketchInput) -> f64 {
        self.query_key(key, &HydraQuery::Frequency(value.clone()))
    }

    /// Convenience method for querying quantiles (for KLL-based Hydra in the future)
    /// This is a wrapper around query_key with HydraQuery::Quantile
    pub fn query_quantile(&self, key: Vec<&str>, threshold: f64) -> f64 {
        self.query_key(key, &HydraQuery::Quantile(threshold))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPSILON: f64 = 1e-6;

    // fn query_cdf(hydra: &Hydra<'_>, key_parts: &[&str], threshold: f64) -> f64 {
    //     let query_input = SketchInput::F64(threshold);
    //     hydra.query_key(key_parts.to_vec(), &query_input)
    // }

    // fn build_kll_test_hydra() -> Hydra<'static> {
    //     let template = Chapter::KLL(KLL::init_kll(200));
    //     let mut hydra = Hydra::new(3, 64, template);

    //     let dataset = [
    //         ("key1;key2;key3", 10.0),
    //         ("key1;key2;key3", 20.0),
    //         ("key1;key2;key3", 30.0),
    //         ("key4;key5;key6", 40.0),
    //         ("key4;key5;key6", 50.0),
    //         ("key4;key5;key6", 60.0),
    //         ("key7;key8;key9", 70.0),
    //         ("key7;key8;key9", 80.0),
    //         ("key7;key8;key9", 90.0),
    //     ];

    //     for (key, value) in dataset {
    //         let input = SketchInput::F64(value);
    //         hydra.update(key, &input);
    //     }

    //     hydra
    // }

    #[test]
    fn hydra_updates_countmin_frequency() {
        let mut hydra = Hydra::with_dimensions(3, 32, HydraCounter::CM(CountMin::default()));
        let value = SketchInput::String("event".to_string());

        for _ in 0..5 {
            hydra.update("user;session", &value);
        }

        let combined = hydra.query_frequency(vec!["user", "session"], &value);
        assert!(
            combined >= 5.0,
            "expected frequency at least 5, got {}",
            combined
        );

        let unrelated = hydra.query_frequency(vec!["other"], &value);
        assert_eq!(unrelated, 0.0);
    }

    #[test]
    fn hydra_updates_countmin_frequency_multiple_values() {
        let mut hydra = Hydra::with_dimensions(3, 32, HydraCounter::CM(CountMin::default()));

        for i in 0..5 {
            for _ in 0..i {
                let value = SketchInput::I64(i as i64);
                hydra.update("key1;key2;key3", &value);
            }
        }

        for i in 0..5 {
            let query_value = SketchInput::I64(i as i64);
            let combined = hydra.query_frequency(vec!["key1", "key3"], &query_value);
            assert!(
                combined >= i as f64,
                "expected frequency at least {}, got {}",
                i,
                combined
            );
        }

        let unrelated_value = SketchInput::I64(0);
        let unrelated = hydra.query_frequency(vec!["other"], &unrelated_value);
        assert_eq!(unrelated, 0.0);
    }

    // #[test]
    // fn hydra_tracks_kll_quantiles() {
    //     let mut hydra = Hydra::with_dimensions(3, 64, Chapter::KLL(KLL::init_kll(200)));
    //     let samples = [
    //         SketchInput::F64(10.0),
    //         SketchInput::F64(20.0),
    //         SketchInput::F64(30.0),
    //         SketchInput::F64(40.0),
    //         SketchInput::F64(50.0),
    //     ];

    //     for sample in &samples {
    //         hydra.update("metrics;latency", sample);
    //     }

    //     let query_value = SketchInput::F64(35.0);
    //     let quantile = hydra.query_key(vec!["metrics", "latency"], &query_value);
    //     assert!(
    //         (quantile - 0.6).abs() < 1e-9,
    //         "expected quantile near 0.6, got {}",
    //         quantile
    //     );

    //     let empty_bucket = hydra.query_key(vec!["other", "key"], &query_value);
    //     assert_eq!(empty_bucket, 0.0);
    // }

    // #[test]
    // fn hydra_kll_single_label_cdfs() {
    //     let hydra = build_kll_test_hydra();

    //     assert!((query_cdf(&hydra, &["key1"], 15.0) - (1.0 / 3.0)).abs() < EPSILON);
    //     assert!((query_cdf(&hydra, &["key1"], 25.0) - (2.0 / 3.0)).abs() < EPSILON);
    //     assert!((query_cdf(&hydra, &["key1"], 35.0) - 1.0).abs() < EPSILON);

    //     assert!((query_cdf(&hydra, &["key4"], 45.0) - (1.0 / 3.0)).abs() < EPSILON);
    //     assert!((query_cdf(&hydra, &["key4"], 55.0) - (2.0 / 3.0)).abs() < EPSILON);
    //     assert!((query_cdf(&hydra, &["key4"], 65.0) - 1.0).abs() < EPSILON);

    //     assert!((query_cdf(&hydra, &["key7"], 75.0) - (1.0 / 3.0)).abs() < EPSILON);
    //     assert!((query_cdf(&hydra, &["key7"], 85.0) - (2.0 / 3.0)).abs() < EPSILON);
    //     assert!((query_cdf(&hydra, &["key7"], 95.0) - 1.0).abs() < EPSILON);
    // }

    // #[test]
    // fn hydra_kll_multi_label_cdfs() {
    //     let hydra = build_kll_test_hydra();

    //     assert!((query_cdf(&hydra, &["key1", "key3"], 25.0) - (2.0 / 3.0)).abs() < EPSILON);
    //     assert!((query_cdf(&hydra, &["key1", "key2", "key3"], 30.0) - 1.0).abs() < EPSILON);
    //     assert!((query_cdf(&hydra, &["key4", "key5"], 55.0) - (2.0 / 3.0)).abs() < EPSILON);
    //     assert!((query_cdf(&hydra, &["key4", "key5", "key6"], 60.0) - 1.0).abs() < EPSILON);
    //     assert!((query_cdf(&hydra, &["key7", "key8", "key9"], 85.0) - (2.0 / 3.0)).abs() < EPSILON);
    //     assert!((query_cdf(&hydra, &["key1", "key7"], 50.0) - 0.0).abs() < EPSILON);
    // }

    // #[test]
    // fn hydra_kll_extreme_queries() {
    //     let hydra = build_kll_test_hydra();

    //     assert!((query_cdf(&hydra, &["key1"], 0.0) - 0.0).abs() < EPSILON);
    //     assert!((query_cdf(&hydra, &["key1"], 100.0) - 1.0).abs() < EPSILON);

    //     assert!((query_cdf(&hydra, &["key4", "key5", "key6"], 35.0) - 0.0).abs() < EPSILON);
    //     assert!((query_cdf(&hydra, &["key4", "key5", "key6"], 100.0) - 1.0).abs() < EPSILON);

    //     assert!((query_cdf(&hydra, &["unknown"], 50.0) - 0.0).abs() < EPSILON);
    // }
}
