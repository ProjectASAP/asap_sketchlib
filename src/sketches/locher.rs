use crate::HHHeap;
use crate::{SketchInput, hash_it};
use crate::{Vector1D, Vector2D};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LocherSketch {
    pub r: usize,
    pub l: usize,
    pub rows: Vector2D<HHHeap>,
    pub row_sum: Vector1D<f64>,
}

impl LocherSketch {
    pub fn new(r: usize, l: usize, k: usize) -> Self {
        let rows = Vector2D::from_fn(r, l, |_row, _col| HHHeap::new(k));
        let row_sum = Vector1D::filled(r, 0.0);

        Self {
            r,
            l,
            rows,
            row_sum,
        }
    }

    pub fn insert(&mut self, e: &String, _v: u64) {
        for i in 0..self.r {
            let idx = hash_it(i, &&SketchInput::String(e.clone())) as usize % self.l;
            let cell = &mut self.rows[i][idx];
            let before = match cell.find(e) {
                Some(heap_idx) => cell.heap()[heap_idx].count,
                None => 0,
            };
            // println!("check e: {}", e);
            // println!("before is: {}", before);
            self.row_sum[i] -= before as f64;
            cell.update(e, before + 1);
            let after = match cell.find(e) {
                Some(heap_idx) => cell.heap()[heap_idx].count,
                None => 0,
            };
            // println!("after is: {}", after);
            self.row_sum[i] += after as f64;
        }
    }

    pub fn estimate(&self, e: &str) -> f64 {
        let mut per_row = Vec::with_capacity(self.r);
        for i in 0..self.r {
            let idx = hash_it(i, &SketchInput::Str(e)) as usize % self.l;
            // let est = self.rows[i][idx].find(e).unwrap_or(0);
            let est = match self.rows[i][idx].find(e) {
                Some(v) => self.rows[i][idx].heap()[v].count,
                None => 0,
            };
            let others = self.row_sum[i] - est as f64;
            let denom = (self.l - 1) as f64;
            let adj = if denom > 0.0 {
                est as f64 - others / denom
            } else {
                est as f64
            };
            per_row.push(adj.max(0.0));
        }
        median(&mut per_row)
    }
}

fn median(xs: &mut [f64]) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let n = xs.len();
    if n % 2 == 1 {
        xs[n / 2]
    } else {
        0.5 * (xs[n / 2 - 1] + xs[n / 2])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locher_estimate_tracks_inserted_frequency() {
        // inserting the same key multiple times should result in a strong estimate
        let mut sketch = LocherSketch::new(3, 32, 5);
        let key = "service::a".to_string();

        for _ in 0..30 {
            sketch.insert(&key, 1);
        }

        let estimate = sketch.estimate(&key);
        assert!(
            estimate >= 15.0,
            "expected estimate to be close to 30, got {}",
            estimate
        );
        assert_eq!(sketch.estimate("missing"), 0.0);
    }

    #[test]
    fn median_handles_even_and_empty_inputs() {
        // exercise the helper median function for the edge cases it supports
        assert_eq!(median(&mut []), 0.0);
        let mut even = [4.0, 1.0, 3.0, 2.0];
        assert_eq!(median(&mut even), 2.5);
        let mut odd = [9.0, 1.0, 5.0];
        assert_eq!(median(&mut odd), 5.0);
    }
}
