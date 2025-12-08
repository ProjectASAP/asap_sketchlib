use serde::{Deserialize, Serialize};

use crate::input::{HydraCounter, HydraQuery};
use crate::sketches::countmin::CountMin;
use crate::{HYDRA_SEED, SketchInput, Vector2D, hash_it_to_128};

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
            // for j in 0..n {
            for (j, &part_item) in parts.iter().enumerate().take(n) {
                if (i >> j) & 1 == 1 {
                    current_combination.push(part_item);
                }
            }
            result.push(current_combination.join(";"));
        }

        for subkey in &result {
            let hash = hash_it_to_128(HYDRA_SEED, &SketchInput::String(subkey.to_string()));
            self.sketches
                .fast_insert(|a, b, _| a.insert(b), value, hash);
        }
    }

    /// Query the Hydra sketch for a specific subpopulation
    /// Assume `key` appears in-order
    ///
    /// # Arguments
    /// * `key` - The subpopulation key as a vector of dimension values (e.g., ["city", "device"])
    /// * `query` - The query type (Frequency, Quantile, Cardinality, etc.)
    ///
    /// # Returns
    /// The estimated statistic (median of r row estimates)
    pub fn query_key(&self, key: Vec<&str>, query: &HydraQuery) -> f64 {
        let key_string = key.join(";");
        let hashed_val = hash_it_to_128(HYDRA_SEED, &SketchInput::String(key_string.to_string()));
        self.sketches
            .fast_query_median_with_key(hashed_val, query, |counter, q, _, _| {
                counter.query(q).unwrap()
            })
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
    use crate::KLL;

    use super::*;

    const EPSILON: f64 = 1e-6;

    fn query_cdf(hydra: &Hydra, key_parts: &[&str], threshold: f64) -> f64 {
        hydra.query_quantile(key_parts.to_vec(), threshold)
    }

    fn build_kll_test_hydra() -> Hydra {
        let template = HydraCounter::KLL(KLL::default());
        let mut hydra = Hydra::with_dimensions(3, 64, template);

        let dataset = [
            ("key1;key2;key3", 10.0),
            ("key1;key2;key3", 20.0),
            ("key1;key2;key3", 30.0),
            ("key4;key5;key6", 40.0),
            ("key4;key5;key6", 50.0),
            ("key4;key5;key6", 60.0),
            ("key7;key8;key9", 70.0),
            ("key7;key8;key9", 80.0),
            ("key7;key8;key9", 90.0),
        ];

        for (key, value) in dataset {
            let input = SketchInput::F64(value);
            hydra.update(key, &input);
        }

        hydra
    }

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
            "expected frequency at least 5, got {combined}"
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
                "expected frequency at least {i}, got {combined}"
            );
        }

        let unrelated_value = SketchInput::I64(0);
        let unrelated = hydra.query_frequency(vec!["other"], &unrelated_value);
        assert_eq!(unrelated, 0.0);
    }

    #[test]
    fn hydra_subpopulation_frequency_test() {
        // Build test dataset using CountMin for frequency queries
        let mut hydra = Hydra::with_dimensions(3, 64, HydraCounter::CM(CountMin::default()));

        let dataset = [
            ("key1;key2;key3", 10.0),
            ("key1;key2;key4", 10.0),
            ("key1;key2;key3", 20.0),
            ("key1;key2;key3", 30.0),
            ("key4;key5;key6", 40.0),
            ("key4;key5;key6", 50.0),
            ("key4;key5;key6", 60.0),
            ("key7;key8;key9", 70.0),
            ("key7;key8;key9", 80.0),
            ("key7;key8;key9", 90.0),
        ];

        // Insert all data points
        for (key, value) in dataset {
            let input = SketchInput::F64(value);
            hydra.update(key, &input);
        }

        // Test single label subpopulation queries
        // key1 appears in 3 entries with values 10.0, 20.0, 30.0
        let freq_10 = hydra.query_frequency(vec!["key1"], &SketchInput::F64(10.0));
        assert_eq!(
            freq_10, 2.0,
            "expected frequency of 10.0 for key1 to be 2, got {freq_10}"
        );

        let freq_20 = hydra.query_frequency(vec!["key1"], &SketchInput::F64(20.0));
        assert_eq!(
            freq_20, 1.0,
            "expected frequency of 20.0 for key1 to be 1, got {freq_20}"
        );

        let freq_30 = hydra.query_frequency(vec!["key1"], &SketchInput::F64(30.0));
        assert_eq!(
            freq_30, 1.0,
            "expected frequency of 30.0 for key1 to be 1, got {freq_30}"
        );

        // key4 appears in 3 entries with values 40.0, 50.0, 60.0
        let freq_40 = hydra.query_frequency(vec!["key4"], &SketchInput::F64(40.0));
        assert_eq!(
            freq_40, 1.0,
            "expected frequency of 40.0 for key4 to be 1, got {freq_40}"
        );

        // Test multi-label subpopulation queries
        let freq_multi = hydra.query_frequency(vec!["key1", "key3"], &SketchInput::F64(10.0));
        assert_eq!(
            freq_multi, 1.0,
            "expected frequency of 10.0 for key1;key to be 1, got {freq_multi}"
        );

        // key1;key2;key3 is the full key appearing 3 times
        let freq_full =
            hydra.query_frequency(vec!["key1", "key2", "key3"], &SketchInput::F64(20.0));
        assert_eq!(
            freq_full, 1.0,
            "expected frequency of 20.0 for key1;key2;key3 to be 1, got {freq_full}"
        );

        // Test cross-population queries (should be 0 as key1 and key8 never appear together)
        let freq_cross = hydra.query_frequency(vec!["key1", "key8"], &SketchInput::F64(10.0));
        assert_eq!(
            freq_cross, 0.0,
            "expected frequency of 10.0 for key1;key8 to be 0/empty, got {freq_cross}"
        );
    }

    #[test]
    fn hydra_subpopulation_cardinality_test() {
        use crate::sketches::hll::HllDf;

        // Build test dataset using HyperLogLog for cardinality queries
        let mut hydra = Hydra::with_dimensions(5, 128, HydraCounter::HLL(HllDf::new()));

        let dataset = [
            ("key1;key2;key3", 10.0),
            ("key1;key2;key3", 20.0),
            ("key1;key2;key3", 30.0),
            ("key4;key5;key6", 40.0),
            ("key4;key5;key6", 50.0),
            ("key4;key5;key6", 60.0),
            ("key7;key8;key9", 70.0),
            ("key7;key8;key9", 80.0),
            ("key7;key8;key9", 90.0),
        ];

        // Insert all data points (HLL tracks distinct values)
        for (key, value) in dataset {
            let input = SketchInput::F64(value);
            hydra.update(key, &input);
        }

        // Test single label cardinality
        // key1 appears with 3 distinct values: 10.0, 20.0, 30.0
        let card_key1 = hydra.query_key(vec!["key1"], &HydraQuery::Cardinality);
        assert!(
            (card_key1 - 3.0).abs() < EPSILON,
            "expected cardinality near 3 for key1, got {card_key1}"
        );

        // key4 appears with 3 distinct values: 40.0, 50.0, 60.0
        let card_key4 = hydra.query_key(vec!["key4"], &HydraQuery::Cardinality);
        assert!(
            (card_key4 - 3.0).abs() < EPSILON,
            "expected cardinality near 3 for key4, got {card_key4}"
        );

        // key7 appears with 3 distinct values: 70.0, 80.0, 90.0
        let card_key7 = hydra.query_key(vec!["key7"], &HydraQuery::Cardinality);
        assert!(
            (card_key7 - 3.0).abs() < EPSILON,
            "expected cardinality near 3 for key7, got {card_key7}"
        );

        // Test multi-label cardinality
        // key1;key2 appears together with 3 distinct values
        let card_multi = hydra.query_key(vec!["key1", "key2"], &HydraQuery::Cardinality);
        assert!(
            (card_multi - 3.0).abs() < EPSILON,
            "expected cardinality near 3 for key1;key2, got {card_multi}"
        );

        // key1;key2;key3 is the full key with 3 distinct values
        let card_full = hydra.query_key(vec!["key1", "key2", "key3"], &HydraQuery::Cardinality);
        assert!(
            (card_full - 3.0).abs() < EPSILON,
            "expected cardinality near 3 for key1;key2;key3, got {card_full}"
        );

        // Test cross-population queries (should be 0 as key1 and key7 never appear together)
        let card_cross = hydra.query_key(vec!["key1", "key7"], &HydraQuery::Cardinality);
        assert_eq!(
            card_cross, 0.0,
            "expected cardinality 0 for non-overlapping keys"
        );

        // Test unrelated key (never inserted)
        let card_unrelated = hydra.query_key(vec!["unknown"], &HydraQuery::Cardinality);
        assert_eq!(
            card_unrelated, 0.0,
            "expected cardinality 0 for unknown key"
        );
    }

    #[test]
    fn hydra_tracks_kll_quantiles() {
        let mut hydra = Hydra::with_dimensions(3, 64, HydraCounter::KLL(KLL::default()));
        let samples = [
            SketchInput::F64(10.0),
            SketchInput::F64(20.0),
            SketchInput::F64(30.0),
            SketchInput::F64(40.0),
            SketchInput::F64(50.0),
        ];

        for sample in &samples {
            hydra.update("metrics;latency", sample);
        }

        // let query_value = SketchInput::F64(35.0);
        let quantile = hydra.query_key(vec!["metrics", "latency"], &HydraQuery::Quantile(50.0));
        assert!(
            (quantile - 0.6).abs() < 1e-9,
            "expected quantile near 0.6, got {}",
            quantile
        );

        let empty_bucket = hydra.query_key(vec!["other", "key"], &HydraQuery::Quantile(50.0));
        assert_eq!(empty_bucket, 0.0);
    }

    #[test]
    fn hydra_kll_single_label_cdfs() {
        let hydra = build_kll_test_hydra();

        assert!((query_cdf(&hydra, &["key1"], 15.0) - (1.0 / 3.0)).abs() < EPSILON);
        assert!((query_cdf(&hydra, &["key1"], 25.0) - (2.0 / 3.0)).abs() < EPSILON);
        assert!((query_cdf(&hydra, &["key1"], 35.0) - 1.0).abs() < EPSILON);

        assert!((query_cdf(&hydra, &["key4"], 45.0) - (1.0 / 3.0)).abs() < EPSILON);
        assert!((query_cdf(&hydra, &["key4"], 55.0) - (2.0 / 3.0)).abs() < EPSILON);
        assert!((query_cdf(&hydra, &["key4"], 65.0) - 1.0).abs() < EPSILON);

        assert!((query_cdf(&hydra, &["key7"], 75.0) - (1.0 / 3.0)).abs() < EPSILON);
        assert!((query_cdf(&hydra, &["key7"], 85.0) - (2.0 / 3.0)).abs() < EPSILON);
        assert!((query_cdf(&hydra, &["key7"], 95.0) - 1.0).abs() < EPSILON);
    }

    #[test]
    fn hydra_kll_multi_label_cdfs() {
        let hydra = build_kll_test_hydra();

        assert!((query_cdf(&hydra, &["key1", "key3"], 25.0) - (2.0 / 3.0)).abs() < EPSILON);
        assert!((query_cdf(&hydra, &["key1", "key2", "key3"], 30.0) - 1.0).abs() < EPSILON);
        assert!((query_cdf(&hydra, &["key4", "key5"], 55.0) - (2.0 / 3.0)).abs() < EPSILON);
        assert!((query_cdf(&hydra, &["key4", "key5", "key6"], 60.0) - 1.0).abs() < EPSILON);
        assert!((query_cdf(&hydra, &["key7", "key8", "key9"], 85.0) - (2.0 / 3.0)).abs() < EPSILON);
        assert!((query_cdf(&hydra, &["key1", "key7"], 50.0) - 0.0).abs() < EPSILON);
    }

    #[test]
    fn hydra_kll_extreme_queries() {
        let hydra = build_kll_test_hydra();

        assert!((query_cdf(&hydra, &["key1"], 0.0) - 0.0).abs() < EPSILON);
        assert!((query_cdf(&hydra, &["key1"], 100.0) - 1.0).abs() < EPSILON);

        assert!((query_cdf(&hydra, &["key4", "key5", "key6"], 35.0) - 0.0).abs() < EPSILON);
        assert!((query_cdf(&hydra, &["key4", "key5", "key6"], 100.0) - 1.0).abs() < EPSILON);

        assert!((query_cdf(&hydra, &["unknown"], 50.0) - 0.0).abs() < EPSILON);
    }
}
