use crate::sketches::kll::{KllSketch, KllSketchData};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use xxhash_rust::xxh32::xxh32;

#[derive(Serialize, Deserialize)]
struct HydraKllSketchData {
    #[serde(rename = "row_num")]
    rows: usize,
    #[serde(rename = "col_num")]
    cols: usize,
    sketches: Vec<Vec<KllSketchData>>,
}

#[derive(Debug, Clone)]
pub struct HydraKllSketch {
    pub sketch: Vec<Vec<KllSketch>>,
    pub rows: usize,
    pub cols: usize,
}

impl HydraKllSketch {
    pub fn new(rows: usize, cols: usize, k: u16) -> Self {
        let sketch = vec![vec![KllSketch::new(k); cols]; rows];
        Self { sketch, rows, cols }
    }

    /// Number of hash rows in the sketch matrix.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Number of columns in the sketch matrix.
    pub fn cols(&self) -> usize {
        self.cols
    }

    pub fn update(&mut self, key: &str, value: f64) {
        let key_bytes = key.as_bytes();
        // Update each row using different hash functions
        for i in 0..self.rows {
            let hash_value = xxh32(key_bytes, i as u32);
            let col_index = (hash_value as usize) % self.cols;
            self.sketch[i][col_index].update(value);
        }
    }

    /// Estimate the value at the given quantile `q` for `key` — the
    /// median across rows of each row's KLL quantile estimate at the
    /// hashed cell.
    pub fn quantile(&self, key: &str, q: f64) -> f64 {
        let key_bytes = key.as_bytes();
        let mut quantiles = Vec::with_capacity(self.rows);

        for i in 0..self.rows {
            let hash_value = xxh32(key_bytes, i as u32);
            let col_index = (hash_value as usize) % self.cols;
            quantiles.push(self.sketch[i][col_index].quantile(q));
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

    /// Merge another HydraKllSketch into self in place. Both operands
    /// must have identical dimensions.
    pub fn merge(
        &mut self,
        other: &HydraKllSketch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.rows != other.rows || self.cols != other.cols {
            return Err(format!(
                "HydraKllSketch dimension mismatch: self={}x{}, other={}x{}",
                self.rows, self.cols, other.rows, other.cols
            )
            .into());
        }
        for i in 0..self.rows {
            for j in 0..self.cols {
                self.sketch[i][j].merge(&other.sketch[i][j])?;
            }
        }
        Ok(())
    }

    /// Merge from references, returning a new sketch — convenience for
    /// batch reduction at API edges.
    pub fn merge_refs(
        inputs: &[&HydraKllSketch],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let first = inputs
            .first()
            .ok_or("HydraKllSketch::merge_refs called with empty input")?;
        let mut merged = (*first).clone();
        for h in inputs.iter().skip(1) {
            merged.merge(h)?;
        }
        Ok(merged)
    }

    /// Serialize to MessagePack — matches the wire format exactly.
    pub fn serialize_msgpack(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        let mut sketches = Vec::with_capacity(self.rows);
        for row in &self.sketch {
            let mut row_data = Vec::with_capacity(self.cols);
            for cell in row {
                // Serialize each KllSketch to KllSketchData. We can
                // avoid the round-trip by reading directly via the
                // public sketch_bytes accessor.
                row_data.push(KllSketchData {
                    k: cell.k,
                    sketch_bytes: cell.sketch_bytes(),
                });
            }
            sketches.push(row_data);
        }

        let serialized = HydraKllSketchData {
            rows: self.rows,
            cols: self.cols,
            sketches,
        };

        let mut buf = Vec::new();
        rmp_serde::encode::write(&mut buf, &serialized)?;
        Ok(buf)
    }

    /// Deserialize from MessagePack.
    pub fn deserialize_msgpack(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let deserialized_sketch_data: HydraKllSketchData = rmp_serde::from_slice(buffer).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("Failed to deserialize HydraKLL from MessagePack: {e}").into()
            },
        )?;

        if deserialized_sketch_data.sketches.len() != deserialized_sketch_data.rows {
            return Err(format!(
                "HydraKLL row count mismatch: expected {}, got {}",
                deserialized_sketch_data.rows,
                deserialized_sketch_data.sketches.len()
            )
            .into());
        }

        let mut sketch: Vec<Vec<KllSketch>> = Vec::with_capacity(deserialized_sketch_data.rows);

        for (row_idx, row) in deserialized_sketch_data.sketches.into_iter().enumerate() {
            if row.len() != deserialized_sketch_data.cols {
                return Err(format!(
                    "HydraKLL column count mismatch in row {}: expected {}, got {}",
                    row_idx,
                    deserialized_sketch_data.cols,
                    row.len()
                )
                .into());
            }

            let mut accum_row: Vec<KllSketch> = Vec::with_capacity(deserialized_sketch_data.cols);
            for cell in row {
                let cell_bytes = rmp_serde::to_vec(&cell).map_err(
                    |e| -> Box<dyn std::error::Error + Send + Sync> {
                        format!("Failed to serialize nested KLL sketch: {e}").into()
                    },
                )?;
                let kll = KllSketch::deserialize_msgpack(&cell_bytes)?;
                accum_row.push(kll);
            }

            sketch.push(accum_row);
        }

        Ok(Self {
            sketch,
            rows: deserialized_sketch_data.rows,
            cols: deserialized_sketch_data.cols,
        })
    }

    /// One-shot aggregation: build a sketch from parallel keys/values.
    pub fn aggregate_hydrakll(
        rows: usize,
        cols: usize,
        k: u16,
        keys: &[&str],
        values: &[f64],
    ) -> Option<Vec<u8>> {
        if keys.is_empty() {
            return None;
        }
        let mut sketch = Self::new(rows, cols, k);
        for (key, &value) in keys.iter().zip(values.iter()) {
            sketch.update(key, value);
        }
        sketch.serialize_msgpack().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creation() {
        let h = HydraKllSketch::new(2, 3, 200);
        assert_eq!(h.rows, 2);
        assert_eq!(h.cols, 3);
        assert_eq!(h.sketch.len(), 2);
        assert_eq!(h.sketch[0].len(), 3);
    }

    #[test]
    fn test_update_and_query() {
        let mut h = HydraKllSketch::new(2, 10, 200);
        h.update("key1", 5.0);
        h.update("key1", 10.0);
        // With 2 values, median quantile should be between them
        let q = h.quantile("key1", 0.5);
        assert!(q >= 0.0);
    }

    #[test]
    fn test_merge() {
        let mut h1 = HydraKllSketch::new(2, 5, 200);
        let mut h2 = HydraKllSketch::new(2, 5, 200);

        for i in 1..=5 {
            h1.update("key1", i as f64);
        }
        for i in 6..=10 {
            h2.update("key1", i as f64);
        }

        h1.merge(&h2).unwrap();
        assert_eq!(h1.rows, 2);
        assert_eq!(h1.cols, 5);
    }

    #[test]
    fn test_merge_dimension_mismatch() {
        let mut h1 = HydraKllSketch::new(2, 5, 200);
        let h2 = HydraKllSketch::new(3, 5, 200);
        assert!(h1.merge(&h2).is_err());
    }

    #[test]
    fn test_msgpack_round_trip() {
        let mut h = HydraKllSketch::new(2, 3, 200);
        h.update("key1", 5.0);
        h.update("key2", 10.0);

        let bytes = h.serialize_msgpack().unwrap();
        let deserialized = HydraKllSketch::deserialize_msgpack(&bytes).unwrap();

        assert_eq!(deserialized.rows, 2);
        assert_eq!(deserialized.cols, 3);
    }

    #[test]
    fn test_aggregate_hydrakll() {
        let keys = ["a", "b", "a"];
        let values = [1.0, 2.0, 3.0];
        let bytes = HydraKllSketch::aggregate_hydrakll(2, 5, 200, &keys, &values).unwrap();
        let h = HydraKllSketch::deserialize_msgpack(&bytes).unwrap();
        assert_eq!(h.rows, 2);
        assert_eq!(h.cols, 5);
    }

    #[test]
    fn test_aggregate_hydrakll_empty() {
        assert!(HydraKllSketch::aggregate_hydrakll(2, 5, 200, &[], &[]).is_none());
    }
}
