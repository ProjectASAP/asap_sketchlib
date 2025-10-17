use super::heap::TopKHeap;
use super::utils::{SketchInput, hash_it};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LocherSketch {
    pub r: usize,
    pub l: usize,
    pub rows: Vec<Vec<TopKHeap>>,
    pub row_sum: Vec<f64>,
}

impl LocherSketch {
    pub fn new(r: usize, l: usize, k: usize) -> Self {
        let mut rows = Vec::with_capacity(r);
        for _ in 0..r {
            let mut v = Vec::with_capacity(l);
            for _ in 0..l {
                v.push(TopKHeap::init_heap(k as u32));
            }
            rows.push(v);
        }
        let row_sum = vec![0.0; r];

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
                Some(idx) => cell.heap[idx].count,
                None => 0,
            };
            // println!("check e: {}", e);
            // println!("before is: {}", before);
            self.row_sum[i] -= before as f64;
            cell.update(e, before + 1);
            let after = match cell.find(e) {
                Some(idx) => cell.heap[idx].count,
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
                Some(v) => self.rows[i][idx].heap[v].count,
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
