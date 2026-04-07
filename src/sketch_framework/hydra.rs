//! Hydra hierarchical sketch framework.
//!
//! Reference:
//! - Manousis et al., VLDB 2022.
//!   https://vldb.org/pvldb/vol15/p3249-manousis.pdf

use rmp_serde::{
    decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice, to_vec_named,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::input::{HydraCounter, HydraQuery};
use crate::{CountMin, FastPath, Vector2D};
use crate::{HYDRA_SEED, SketchInput, hash_for_matrix_seeded};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hydra {
    pub row_num: usize,
    pub col_num: usize,
    pub sketches: Vector2D<HydraCounter>,
    pub type_to_clone: HydraCounter,
}

impl Default for Hydra {
    fn default() -> Self {
        Hydra::with_dimensions(
            3,
            32,
            HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default()),
        )
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
    pub fn update(&mut self, key: &str, value: &SketchInput, count: Option<i32>) {
        let parts: Vec<&str> = key.split(';').filter(|s| !s.is_empty()).collect();
        let n = parts.len();

        // Reuse a single buffer to minimize allocations
        let mut buffer = String::with_capacity(key.len());

        for i in 1..(1 << n) {
            buffer.clear();
            let mut first = true;
            for (j, &part_item) in parts.iter().enumerate() {
                if (i >> j) & 1 == 1 {
                    if !first {
                        buffer.push(';');
                    }
                    buffer.push_str(part_item);
                    first = false;
                }
            }

            // Insert immediately instead of collecting all combinations first
            // Use Str(&str) variant to avoid cloning the buffer
            let hash = hash_for_matrix_seeded(
                HYDRA_SEED,
                self.row_num,
                self.col_num,
                &SketchInput::Str(&buffer),
            );
            self.sketches
                .fast_insert(|a, b, _| a.insert(b, count), value, &hash);
        }

        // Original implementation (kept for reference):
        // let mut result = Vec::new();
        // for i in 1..(1 << n) {
        //     let mut current_combination: Vec<&str> = Vec::new();
        //     // for j in 0..n {
        //     for (j, &part_item) in parts.iter().enumerate().take(n) {
        //         if (i >> j) & 1 == 1 {
        //             current_combination.push(part_item);
        //         }
        //     }
        //     result.push(current_combination.join(";"));
        // }
        //
        // for subkey in &result {
        //     let hash = hash128_seeded(HYDRA_SEED, &SketchInput::String(subkey.to_string()));
        //     self.sketches
        //         .fast_insert(|a, b, _| a.insert(b, count), value, hash);
        // }
    }

    /// Merge another Hydra sketch into this one.
    pub fn merge(&mut self, other: &Hydra) -> Result<(), String> {
        if self.row_num != other.row_num || self.col_num != other.col_num {
            return Err("Hydra dimension mismatch while merging".to_string());
        }
        if std::mem::discriminant(&self.type_to_clone)
            != std::mem::discriminant(&other.type_to_clone)
        {
            return Err("Hydra counter type mismatch while merging".to_string());
        }
        let self_cells = self.sketches.as_mut_slice();
        let other_cells = other.sketches.as_slice();
        if self_cells.len() != other_cells.len() {
            return Err("Hydra storage length mismatch while merging".to_string());
        }
        for (self_counter, other_counter) in self_cells.iter_mut().zip(other_cells.iter()) {
            self_counter.merge(other_counter)?;
        }
        Ok(())
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
        let hashed_val = hash_for_matrix_seeded(
            HYDRA_SEED,
            self.row_num,
            self.col_num,
            &SketchInput::String(key_string.to_string()),
        );
        self.sketches
            .fast_query_median_with_key(&hashed_val, query, |counter, q, _, _| {
                counter.query(q).unwrap()
            })
    }

    /// Convenience method for querying frequency (for CountMin-based Hydra)
    /// This is a wrapper around query_key with HydraQuery::Frequency
    pub fn query_frequency(&self, key: Vec<&str>, value: &SketchInput) -> f64 {
        self.query_key(key, &HydraQuery::Frequency(value.clone()))
    }

    /// Convenience method for querying cumulative distribution for a tracked metric
    /// This is a wrapper around query_key with HydraQuery::Cdf
    pub fn query_quantile(&self, key: Vec<&str>, threshold: f64) -> f64 {
        self.query_key(key, &HydraQuery::Cdf(threshold))
    }

    /// Serializes the Hydra sketch (including all counters) into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    /// Deserializes a Hydra sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MultiHeadHydra {
    pub row_num: usize,
    pub col_num: usize,
    pub sketches: Vector2D<Vec<HydraCounter>>,
    pub dimensions: Vec<(String, HydraCounter)>,
}

impl MultiHeadHydra {
    pub fn dimension_index(&self, dimension: &str) -> Option<usize> {
        self.dimensions
            .iter()
            .position(|(name, _)| name == dimension)
    }

    pub fn with_dimensions(r: usize, c: usize, dimensions: Vec<(String, HydraCounter)>) -> Self {
        let template: Vec<HydraCounter> = dimensions
            .iter()
            .map(|(_, counter)| counter.clone())
            .collect();
        let sketches = Vector2D::from_fn(r, c, |_, _| template.clone());
        MultiHeadHydra {
            row_num: r,
            col_num: c,
            sketches,
            dimensions,
        }
    }

    /// Single fan-out, insert multiple values to different dimension sets
    pub fn update(&mut self, key: &str, values: &[(&SketchInput, &[&str])], count: Option<i32>) {
        let parts: Vec<&str> = key.split(';').filter(|s| !s.is_empty()).collect();
        let n = parts.len();

        let dim_name_to_idx: HashMap<&str, usize> = self
            .dimensions
            .iter()
            .enumerate()
            .map(|(idx, (name, _))| (name.as_str(), idx))
            .collect();
        let precomputed: Vec<Vec<usize>> = values
            .iter()
            .map(|(_, dims)| {
                dims.iter()
                    .filter_map(|dim_name| dim_name_to_idx.get(*dim_name).copied())
                    .collect()
            })
            .collect();
        let updates = (values, &precomputed);

        // Reuse a single buffer to minimize allocations
        let mut buffer = String::with_capacity(key.len());
        for i in 1..(1 << n) {
            buffer.clear();
            let mut first = true;
            for (j, &part_item) in parts.iter().enumerate() {
                if (i >> j) & 1 == 1 {
                    if !first {
                        buffer.push(';');
                    }
                    buffer.push_str(part_item);
                    first = false;
                }
            }

            // Insert immediately instead of collecting all combinations first
            // Use Str(&str) variant to avoid cloning the buffer
            let hash = hash_for_matrix_seeded(
                HYDRA_SEED,
                self.row_num,
                self.col_num,
                &SketchInput::Str(&buffer),
            );
            self.sketches.fast_insert(
                |cell_vec, dim_values, _| {
                    let (values, precomputed) = dim_values;
                    for ((value, _), indices) in values.iter().zip(precomputed.iter()) {
                        for &idx in indices.iter() {
                            if let Some(counter) = cell_vec.get_mut(idx) {
                                if let Some(hash) = counter.hash_for_value(value) {
                                    counter.insert_with_hash(value, &hash, count);
                                } else {
                                    counter.insert(value, count);
                                }
                            }
                        }
                    }
                },
                updates,
                &hash,
            );
        }
    }

    /// Merge another MultiHeadHydra into this one.
    pub fn merge(&mut self, other: &MultiHeadHydra) -> Result<(), String> {
        if self.row_num != other.row_num || self.col_num != other.col_num {
            return Err("MultiHeadHydra dimension mismatch while merging".to_string());
        }
        if self.dimensions.len() != other.dimensions.len() {
            return Err("MultiHeadHydra dimension list mismatch while merging".to_string());
        }
        for (idx, (name, counter)) in self.dimensions.iter().enumerate() {
            let (other_name, other_counter) = other.dimensions.get(idx).ok_or_else(|| {
                "MultiHeadHydra dimension list mismatch while merging".to_string()
            })?;
            if name != other_name {
                return Err(format!(
                    "MultiHeadHydra dimension order mismatch at index {idx}"
                ));
            }
            if std::mem::discriminant(counter) != std::mem::discriminant(other_counter) {
                return Err(format!(
                    "MultiHeadHydra counter type mismatch for dimension '{}'",
                    name
                ));
            }
        }

        let self_cells = self.sketches.as_mut_slice();
        let other_cells = other.sketches.as_slice();
        if self_cells.len() != other_cells.len() {
            return Err("MultiHeadHydra storage length mismatch while merging".to_string());
        }
        for (self_cell, other_cell) in self_cells.iter_mut().zip(other_cells.iter()) {
            if self_cell.len() != self.dimensions.len()
                || other_cell.len() != other.dimensions.len()
            {
                return Err("MultiHeadHydra cell dimension mismatch while merging".to_string());
            }
            for idx in 0..self.dimensions.len() {
                let self_counter = self_cell
                    .get_mut(idx)
                    .ok_or_else(|| "MultiHeadHydra missing dimension in target cell".to_string())?;
                let other_counter = other_cell
                    .get(idx)
                    .ok_or_else(|| "MultiHeadHydra missing dimension in source cell".to_string())?;
                self_counter.merge(other_counter)?;
            }
        }

        Ok(())
    }

    /// Query a specific dimension
    pub fn query_key(&self, key: Vec<&str>, dimension: &str, query: &HydraQuery) -> f64 {
        let key_string = key.join(";");
        let hashed_val = hash_for_matrix_seeded(
            HYDRA_SEED,
            self.row_num,
            self.col_num,
            &SketchInput::String(key_string),
        );

        let dim_idx = match self.dimension_index(dimension) {
            Some(idx) => idx,
            None => return 0.0,
        };
        self.sketches
            .fast_query_median_with_key(&hashed_val, query, |cell_vec, q, _, _| {
                cell_vec
                    .get(dim_idx)
                    .map(|counter| counter.query(q).unwrap())
                    .unwrap_or(0.0)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Count, CountMin, ErtlMLE, FastPath, HyperLogLog, KLL, UnivMon, Vector2D};

    const EPSILON: f64 = 1e-6;

    fn query_cdf(hydra: &Hydra, key_parts: &[&str], threshold: f64) -> f64 {
        hydra.query_quantile(key_parts.to_vec(), threshold)
    }

    fn build_kll_test_hydra() -> Hydra {
        let template = HydraCounter::KLL(KLL::default());
        let mut hydra = Hydra::with_dimensions(3, 1024, template);

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
            hydra.update(key, &input, None);
        }

        hydra
    }

    #[test]
    fn hydra_updates_countmin_frequency() {
        let mut hydra = Hydra::with_dimensions(
            3,
            32,
            HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default()),
        );
        let value = SketchInput::String("event".to_string());

        for _ in 0..5 {
            hydra.update("user;session", &value, None);
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
        let mut hydra = Hydra::with_dimensions(
            3,
            32,
            HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default()),
        );

        for i in 0..5 {
            for _ in 0..i {
                let value = SketchInput::I64(i as i64);
                hydra.update("key1;key2;key3", &value, None);
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
    fn hydra_round_trip_serialization() {
        let template =
            HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 64));
        let mut hydra = Hydra::with_dimensions(3, 64, template);

        let dataset = [
            ("city;device", "event_a"),
            ("city;device", "event_a"),
            ("city;browser", "event_b"),
            ("region;device", "event_c"),
            ("city;device;country", "event_a"),
        ];

        for (key, value) in dataset {
            hydra.update(key, &SketchInput::String(value.to_string()), None);
        }

        let hot_value = SketchInput::String("event_a".to_string());
        let cold_value = SketchInput::String("event_c".to_string());

        let freq_before = hydra.query_frequency(vec!["city", "device"], &hot_value);
        let region_before = hydra.query_frequency(vec!["region"], &cold_value);

        let encoded = hydra
            .serialize_to_bytes()
            .expect("serialize Hydra into MessagePack");
        assert!(!encoded.is_empty(), "serialized bytes should not be empty");
        let data = encoded.clone();

        let decoded =
            Hydra::deserialize_from_bytes(&data).expect("deserialize Hydra from MessagePack");

        assert_eq!(hydra.row_num, decoded.row_num);
        assert_eq!(hydra.col_num, decoded.col_num);
        assert_eq!(hydra.sketches.rows(), decoded.sketches.rows());
        assert_eq!(hydra.sketches.cols(), decoded.sketches.cols());
        match &decoded.type_to_clone {
            HydraCounter::CM(_) => {}
            other => panic!("expected CM template, got {other:?}"),
        }

        let freq_after = decoded.query_frequency(vec!["city", "device"], &hot_value);
        let region_after = decoded.query_frequency(vec!["region"], &cold_value);

        assert_eq!(freq_before, freq_after, "frequency changed after serde");
        assert_eq!(
            region_before, region_after,
            "region frequency changed after serde"
        );
    }

    #[test]
    fn multihead_hydra_updates_multiple_dimensions() {
        let dimensions = vec![
            (
                "events".to_string(),
                HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default()),
            ),
            (
                "latency".to_string(),
                HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default()),
            ),
        ];
        let mut hydra = MultiHeadHydra::with_dimensions(3, 32, dimensions);

        let event_value = SketchInput::String("event_a".to_string());
        let latency_value = SketchInput::I64(120);

        for _ in 0..3 {
            hydra.update(
                "user;session",
                &[(&event_value, &["events"]), (&latency_value, &["latency"])],
                None,
            );
        }

        let events_full = hydra.query_key(
            vec!["user", "session"],
            "events",
            &HydraQuery::Frequency(event_value.clone()),
        );
        assert!(
            events_full >= 3.0,
            "expected events count at least 3, got {events_full}"
        );

        let events_fanout = hydra.query_key(
            vec!["user"],
            "events",
            &HydraQuery::Frequency(event_value.clone()),
        );
        assert!(
            events_fanout >= 3.0,
            "expected fan-out events count at least 3, got {events_fanout}"
        );

        let latency_full = hydra.query_key(
            vec!["user", "session"],
            "latency",
            &HydraQuery::Frequency(latency_value.clone()),
        );
        assert!(
            latency_full >= 3.0,
            "expected latency count at least 3, got {latency_full}"
        );
    }

    #[test]
    fn hydra_subpopulation_frequency_test() {
        // Build test dataset using CountMin for frequency queries
        let mut hydra = Hydra::with_dimensions(
            3,
            64,
            HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default()),
        );

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
            hydra.update(key, &input, None);
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
        use crate::sketches::hll::{ErtlMLE, HyperLogLog};

        // Build test dataset using HyperLogLog for cardinality queries
        let mut hydra =
            Hydra::with_dimensions(5, 128, HydraCounter::HLL(HyperLogLog::<ErtlMLE>::new()));

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
            hydra.update(key, &input, None);
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
            hydra.update("metrics;latency", sample, None);
        }

        // let query_value = SketchInput::F64(35.0);
        let quantile = hydra.query_key(vec!["metrics", "latency"], &HydraQuery::Cdf(30.0));
        assert!(
            (quantile - 0.6).abs() < 1e-9,
            "expected CDF near 0.6, got {}",
            quantile
        );

        let empty_bucket = hydra.query_key(vec!["other", "key"], &HydraQuery::Cdf(50.0));
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

    // Helper to generate a default CountMin counter
    fn cm_counter() -> HydraCounter {
        HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default())
    }

    // Helper to generate a default Count Sketch counter
    fn count_counter() -> HydraCounter {
        HydraCounter::CS(Count::<Vector2D<i32>, FastPath>::default())
    }

    // Helper to generate a default UnivMon counter
    fn univmon_counter() -> HydraCounter {
        HydraCounter::UNIVERSAL(UnivMon::default())
    }

    #[test]
    fn test_count_min_frequency_query() {
        let mut counter = cm_counter();
        let key = SketchInput::I64(42);

        // 1. Insert data
        counter.insert(&key, None);
        counter.insert(&key, None);
        counter.insert(&key, None);

        // 2. Query Frequency (Valid)
        let query = HydraQuery::Frequency(key);
        let result = counter.query(&query);

        assert!(result.is_ok());
        // CountMin isn't always exact, but for small inputs/defaults it usually is
        assert_eq!(result.unwrap(), 3.0);
    }

    #[test]
    fn test_count_min_invalid_query_types() {
        let counter = cm_counter();

        // 1. Test Quantile query (Invalid for CM)
        let result = counter.query(&HydraQuery::Quantile(0.5));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Count-Min Sketch Counter does not support Quantile Query"
        );

        // 2. Test Cardinality query (Invalid for CM)
        let result = counter.query(&HydraQuery::Cardinality);
        assert!(result.is_err());
    }

    #[test]
    fn test_hll_cardinality_query() {
        let mut counter = HydraCounter::HLL(HyperLogLog::<ErtlMLE>::default());

        // 1. Insert unique items
        for i in 0..100 {
            counter.insert(&SketchInput::I64(i), None);
        }
        // Duplicate insertions shouldn't affect cardinality
        counter.insert(&SketchInput::I64(0), None);

        // 2. Query Cardinality (Valid)
        let result = counter.query(&HydraQuery::Cardinality);
        assert!(result.is_ok());

        // HLL is probabilistic, check for reasonable error margin (e.g., +/- 5%)
        let card = result.unwrap();
        assert!(
            card > 90.0 && card < 110.0,
            "Expected approx 100, got {}",
            card
        );
    }

    #[test]
    fn test_kll_quantile_query() {
        // Assuming KLL has a default implementation
        let mut counter = HydraCounter::KLL(KLL::default());

        // Insert numbers 1 to 100
        for i in 1..=100 {
            counter.insert(&SketchInput::F64(i as f64), None);
        }

        // Query Median (0.5)
        let result = counter.query(&HydraQuery::Quantile(0.5));
        assert!(result.is_ok());

        // Median of 1..100 is approx 50
        let median = result.unwrap();
        assert!(
            (median - 50.0).abs() < 5.0,
            "Expected approx 50, got {}",
            median
        );
    }

    #[test]
    fn test_univmon_universal_queries() {
        let mut counter = univmon_counter();

        // Insert distribution:
        // Item "A": 10 times
        // Item "B": 20 times
        let key_a = SketchInput::Str("A");
        let key_b = SketchInput::Str("B");

        for _ in 0..10 {
            counter.insert(&key_a, None);
        }
        for _ in 0..20 {
            counter.insert(&key_b, None);
        }

        // 1. Test L1 Norm (Total Sum of Weights)
        // Should be 10 + 20 = 30
        let l1 = counter.query(&HydraQuery::L1Norm).unwrap();
        assert_eq!(l1, 30.0);

        // 2. Test Cardinality
        // Should be 2 ("A" and "B")
        let card = counter.query(&HydraQuery::Cardinality).unwrap();
        assert!((card - 2.0).abs() < 0.5, "Cardinality should be approx 2");

        // 3. Test Entropy
        // UnivMon calculates entropy, should be > 0 for this distribution
        let entropy = counter.query(&HydraQuery::Entropy).unwrap();
        assert!(entropy > 0.0);
    }

    #[test]
    fn test_merge_counters() {
        // Test merging two CountMin sketches via the Hydra wrapper
        let mut c1 = cm_counter();
        let mut c2 = cm_counter();

        c1.insert(&SketchInput::I64(1), None);
        c2.insert(&SketchInput::I64(1), None);

        // Valid merge
        assert!(c1.merge(&c2).is_ok());

        let count = c1
            .query(&HydraQuery::Frequency(SketchInput::I64(1)))
            .unwrap();
        assert_eq!(count, 2.0, "Merge should sum the counts");

        // Invalid merge (Different types)
        let hll = HydraCounter::HLL(HyperLogLog::<ErtlMLE>::default());
        assert!(c1.merge(&hll).is_err());
    }

    #[test]
    fn test_count_frequency_query() {
        let mut counter = count_counter();
        let key = SketchInput::I64(7);

        for _ in 0..4 {
            counter.insert(&key, None);
        }

        let query = HydraQuery::Frequency(key);
        let result = counter.query(&query);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            4.0,
            "Count Sketch should track all inserts"
        );
    }

    #[test]
    fn test_count_invalid_query_types() {
        let counter = count_counter();

        let quantile = counter.query(&HydraQuery::Quantile(0.5));
        assert!(quantile.is_err());
        assert_eq!(
            quantile.unwrap_err(),
            "Count Sketch Counter does not support Quantile Query"
        );

        let cardinality = counter.query(&HydraQuery::Cardinality);
        assert!(cardinality.is_err());
    }
}
