use crate::common::{SketchInput, Vector1D, Vector2D, hash_it};
use crate::sketches::TopKHeap;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

fn median(values: &mut [f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let n = values.len();
    if n % 2 == 1 {
        values[n / 2]
    } else {
        0.5 * (values[n / 2 - 1] + values[n / 2])
    }
}

/// Locher sketch backed by the shared vector abstractions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocherSketch {
    rows: usize,
    cols: usize,
    heaps: Vector2D<TopKHeap>,
    row_sum: Vector1D<f64>,
}

impl LocherSketch {
    pub fn new(rows: usize, cols: usize, k: usize) -> Self {
        let heaps = Vector2D::from_fn(rows, cols, |_, _| TopKHeap::init_heap(k as u32));
        let mut row_sum = Vector1D::init(rows);
        row_sum.fill(0.0);

        Self {
            rows,
            cols,
            heaps,
            row_sum,
        }
    }

    pub fn insert(&mut self, key: &String, value: u64) {
        for row in 0..self.rows {
            let idx = hash_it(row, &SketchInput::String(key.clone())) as usize % self.cols;
            let heap = self.heaps.get_mut(row, idx).expect("indices in bounds");

            let before = heap
                .find(key)
                .map(|position| heap.heap[position].count)
                .unwrap_or(0);
            self.row_sum.as_mut_slice()[row] -= before as f64;

            heap.update(key, before + value as i64);

            let after = heap
                .find(key)
                .map(|position| heap.heap[position].count)
                .unwrap_or(0);
            self.row_sum.as_mut_slice()[row] += after as f64;
        }
    }

    pub fn estimate(&self, key: &str) -> f64 {
        let mut per_row = Vec::with_capacity(self.rows);
        for row in 0..self.rows {
            let idx = hash_it(row, &SketchInput::Str(key)) as usize % self.cols;
            let heap = self.heaps.get(row, idx).expect("indices in bounds");
            let estimate = heap
                .find(key)
                .map(|position| heap.heap[position].count)
                .unwrap_or(0) as f64;
            let others = self.row_sum.as_slice()[row] - estimate;
            let denom = (self.cols - 1) as f64;
            let adjusted = if denom > 0.0 {
                estimate - others / denom
            } else {
                estimate
            };
            per_row.push(adjusted.max(0.0));
        }
        median(&mut per_row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locher_estimate_tracks_inserted_frequency() {
        let mut sketch = LocherSketch::new(3, 32, 5);
        let key = "service::a".to_string();

        for _ in 0..30 {
            sketch.insert(&key, 1);
        }

        let estimate = sketch.estimate(&key);
        assert!(
            estimate >= 15.0,
            "expected estimate to be close to inserted frequency, got {}",
            estimate
        );
        assert_eq!(sketch.estimate("missing"), 0.0);
    }
}
