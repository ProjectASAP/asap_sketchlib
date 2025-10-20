use std::cmp::Ordering;

use crate::{SketchInput, hash_it};

use super::super::sketches::countmin::CountMin;
use super::Chapter;

#[cfg(test)]
use super::super::sketches::kll::KLL;

#[derive(Clone, Debug)]
pub struct Hydra<'bucket> {
    pub row_num: usize,
    pub col_num: usize,
    pub sketches: Vec<Vec<Chapter<'bucket>>>,
    pub type_to_clone: Chapter<'bucket>,
}

impl<'bucket> Default for Hydra<'bucket> {
    fn default() -> Self {
        Hydra::new(3, 32, Chapter::CM(CountMin::default()))
    }
}

impl<'a> Hydra<'a> {
    pub fn new(r: usize, c: usize, sketch_type: Chapter<'a>) -> Self {
        let mut mat = Vec::with_capacity(r);
        for _ in 0..r {
            let mut row = Vec::with_capacity(c);
            for _ in 0..c {
                row.push(sketch_type.clone());
            }
            mat.push(row);
        }
        Hydra {
            row_num: r,
            col_num: c,
            sketches: mat,
            type_to_clone: sketch_type,
        }
    }

    /// Assume key is a string that aggregate different keys
    /// with ";" for now
    pub fn update(&mut self, key: &str, value: &SketchInput<'a>) {
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
        // println!("result: {:?}", result);
        for i in 0..self.row_num {
            for subkey in &result {
                // let hash = xxh32(subkey.as_bytes(), i as u32);
                // let hash = XxHash32::oneshot(i as u32, subkey.as_bytes());
                let hash = hash_it(i, &SketchInput::String(subkey.to_string()));
                let bucket = (hash as usize) % self.col_num;
                // println!("bucket: {}", bucket);
                self.sketches[i][bucket].insert(value);
            }
        }
    }

    pub fn query_key(&self, key: Vec<&str>, quantile: &SketchInput<'a>) -> f64 {
        let mut quantiles = Vec::with_capacity(self.row_num);
        let key_string = key.join(";");
        // let key_bytes = key_string.as_bytes();

        // Query each row and take the median
        for i in 0..self.row_num {
            // let hash_value = xxh32(key_bytes, i as u32);
            let hash_value = hash_it(i, &SketchInput::String(key_string.clone()));
            let col_index = (hash_value as usize) % self.col_num;
            match self.sketches[i][col_index].query(quantile) {
                Ok(v) => quantiles.push(v),
                Err(_) => (),
            }
            // quantiles.push(self.sketches[i][col_index].query(quantile)?);
        }

        if quantiles.is_empty() {
            return 0.0;
        }

        quantiles.sort_by(|a, b| match a.partial_cmp(b) {
            Some(ordering) => ordering,
            None => Ordering::Equal,
        });

        let mid = quantiles.len() / 2;
        if quantiles.len() % 2 == 0 {
            (quantiles[mid - 1] + quantiles[mid]) / 2.0
        } else {
            quantiles[mid]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPSILON: f64 = 1e-6;

    fn query_cdf(hydra: &Hydra<'_>, key_parts: &[&str], threshold: f64) -> f64 {
        let query_input = SketchInput::F64(threshold);
        hydra.query_key(key_parts.to_vec(), &query_input)
    }

    fn build_kll_test_hydra() -> Hydra<'static> {
        let template = Chapter::KLL(KLL::init_kll(200));
        let mut hydra = Hydra::new(3, 64, template);

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
        let mut hydra = Hydra::new(3, 32, Chapter::CM(CountMin::default()));
        let value = SketchInput::String("event".to_string());

        for _ in 0..5 {
            hydra.update("user;session", &value);
        }

        let combined = hydra.query_key(vec!["user", "session"], &value);
        assert!(
            combined >= 5.0,
            "expected frequency at least 5, got {}",
            combined
        );

        let unrelated = hydra.query_key(vec!["other"], &value);
        assert_eq!(unrelated, 0.0);
    }

    #[test]
    fn hydra_updates_countmin_frequency_multiple_values() {
        let mut hydra = Hydra::new(3, 32, Chapter::CM(CountMin::default()));

        for i in 0..5 {
            for _ in 0..i {
                let value = SketchInput::I64(i as i64);
                hydra.update("key1;key2;key3", &value);
            }
        }

        for i in 0..5 {
            let query_value = SketchInput::I64(i as i64);
            let combined = hydra.query_key(vec!["key1", "key3"], &query_value);
            assert!(
                combined >= i as f64,
                "expected frequency at least {}, got {}",
                i,
                combined
            );
        }

        let unrelated_value = SketchInput::I64(0);
        let unrelated = hydra.query_key(vec!["other"], &unrelated_value);
        assert_eq!(unrelated, 0.0);
    }

    #[test]
    fn hydra_tracks_kll_quantiles() {
        let mut hydra = Hydra::new(3, 64, Chapter::KLL(KLL::init_kll(200)));
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

        let query_value = SketchInput::F64(35.0);
        let quantile = hydra.query_key(vec!["metrics", "latency"], &query_value);
        assert!(
            (quantile - 0.6).abs() < 1e-9,
            "expected quantile near 0.6, got {}",
            quantile
        );

        let empty_bucket = hydra.query_key(vec!["other", "key"], &query_value);
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
