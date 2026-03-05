//! # CocoSketch (SIGCOMM '21)
//!
//! A Rust implementation of the CocoSketch algorithm for high-performance
//! network measurement over arbitrary key spaces.
//!
//! ## Key Features
//! * **Arbitrary Keys**: Supports variable-length strings via `full_key` storage.
//! * **Subset Queries**: Enables prefix and UDF-based matching through table scans.
//! * **Biased Replacement**: Uses a probabilistic strategy to retain Heavy Hitters.
//!
//! ## Reference
//! * "CocoSketch: High-Performance Sketch-based Measurement over Arbitrary Key Spaces"

use crate::{DefaultXxHasher, SketchHasher, SketchInput, Vector2D};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CocoBucket {
    pub full_key: Option<String>,
    pub val: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(bound = "")]
pub struct Coco<H: SketchHasher = DefaultXxHasher> {
    pub w: usize,
    pub d: usize,
    pub table: Vector2D<CocoBucket>,
    #[serde(skip)]
    _hasher: PhantomData<H>,
}

const DEFAULT_WIDTH: usize = 64;
const DEFAULT_DEPTH: usize = 5;
const DEFAULT_ROW_IDX: usize = 0;

impl Default for CocoBucket {
    fn default() -> Self {
        Self::new()
    }
}

impl CocoBucket {
    pub fn new() -> Self {
        CocoBucket {
            full_key: None,
            val: 0,
        }
    }

    pub fn update_key(&mut self, key: &str) {
        self.full_key = Some(key.to_string());
    }

    /// Checks if partial_key is a substring of the stored full key.
    pub fn is_partial_key(&mut self, partial_key: &str) -> bool {
        match &self.full_key {
            Some(full) => full.contains(partial_key),
            None => false,
        }
    }
    /// the function should take in full key first, then partial key
    pub fn is_partial_key_with_udf<F>(&mut self, partial_key: &str, udf: F) -> bool
    where
        F: Fn(&str, &str) -> bool,
    {
        match &self.full_key {
            Some(k) => udf(k.as_str(), partial_key),
            None => false,
        }
    }

    pub fn debug(&mut self) {
        match &self.full_key {
            Some(k) => print!(" <String::{}, {}> ", k, self.val),
            None => print!(" <None, {}> ", self.val),
        }
    }

    pub fn add_v(&mut self, v: u64) {
        self.val += v;
    }
}

impl Default for Coco {
    fn default() -> Self {
        Self::new()
    }
}

impl<H: SketchHasher> Coco<H> {
    pub fn new() -> Self {
        Coco::init_with_size(DEFAULT_WIDTH, DEFAULT_DEPTH)
    }

    pub fn debug(&mut self) {
        println!("w: {}", self.w);
        println!("d: {}", self.d);
        for i in 0..self.d {
            print!("[ ");
            for j in 0..self.w {
                self.table[i][j].debug();
            }
            println!(" ]");
        }
    }

    pub fn init_with_size(w: usize, d: usize) -> Self {
        Coco {
            w,
            d,
            table: Vector2D::from_fn(d, w, |_, _| CocoBucket::default()),
            _hasher: PhantomData,
        }
    }

    pub fn insert(&mut self, key: &str, v: u64) {
        if self.d == 0 || self.w == 0 {
            return;
        }
        let key_input = SketchInput::Str(key);
        let mut min_val_row = usize::MAX;
        let mut min_val = u64::MAX;
        for i in 0..self.d {
            // let idx = STATELIST[i].hash_one(&key) as usize % self.w;
            // let idx = hash64_seeded(i, &key) as usize % self.w;
            let idx = H::hash64_seeded(i, &key_input) as usize % self.w;
            match &self.table[i][idx].full_key {
                Some(k) => {
                    if k == key {
                        self.table[i][idx].val += v;
                        return;
                    }
                    if self.table[i][idx].val < min_val {
                        min_val_row = i;
                        min_val = self.table[i][idx].val;
                    }
                }
                None => {
                    // seems like if nothing there, I should just update, and return
                    self.table[i][idx].val += v;
                    self.table[i][idx].update_key(key);
                    return;
                }
            }
            // println!("i: {}", i);
        }
        // all empty
        // println!("min val row: {}", min_val_row);
        if min_val_row >= self.d {
            min_val_row = DEFAULT_ROW_IDX;
        }
        // let idx = STATELIST[min_val_row].hash_one(&key) as usize % self.w;
        // let idx = hash64_seeded(min_val_row, &key) as usize % self.w;
        let idx = H::hash64_seeded(min_val_row, &key_input) as usize % self.w;
        self.table[min_val_row][idx].val += v;
        match self.table[min_val_row][idx].full_key {
            Some(_) => {
                let mut name_decider = rand::rng();
                let random_float = name_decider.random_range(0.0..=1.0_f64);
                if (v as f64 / self.table[min_val_row][idx].val as f64) > random_float {
                    // self.table[min_val_row][idx].full_key = Some(key.clone());
                    // to make lifetime happy
                    self.table[min_val_row][idx].update_key(key);
                }
            }
            // None => self.table[min_val_row][idx].full_key = Some(key.clone()),
            // to make lifetime happy
            None => self.table[min_val_row][idx].update_key(key),
        }
    }

    /// the udf parameter takes in full key first, and then partial key
    pub fn estimate_with_udf<F>(&mut self, partial_key: &str, udf: F) -> u64
    where
        F: Fn(&str, &str) -> bool,
    {
        let mut total = 0;
        for i in 0..self.d {
            for j in 0..self.w {
                if self.table[i][j].is_partial_key_with_udf(partial_key, &udf) {
                    total += self.table[i][j].val;
                    // println!("partial: {:?}, full: {:?}, val: {}, total: {}", partial_key, self.table[i][j].full_key, self.table[i][j].val, total);
                }
            }
        }
        total
    }

    pub fn estimate(&mut self, partial_key: &str) -> u64 {
        let mut total = 0;
        for i in 0..self.d {
            for j in 0..self.w {
                if self.table[i][j].is_partial_key(partial_key) {
                    total += self.table[i][j].val;
                }
            }
        }
        total
    }

    pub fn merge(&mut self, other: &Coco<H>) {
        assert_eq!(self.d, other.d, "Different depth, do nothing");
        assert_eq!(self.w, other.w, "Different width, do nothing");
        for i in 0..self.d {
            for j in 0..self.w {
                if let Some(k) = &other.table[i][j].full_key {
                    self.insert(k.as_str(), other.table[i][j].val);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_W: usize = 32;
    const TEST_D: usize = 4;

    #[test]
    fn insert_then_estimate_matches_full_value_for_partial_key() {
        // cover end-to-end flow of inserting a key and querying with a substring
        let mut coco: Coco = Coco::init_with_size(TEST_W, TEST_D);
        let key = "user:1234";

        coco.insert(key, 3);
        coco.insert(key, 2);

        let estimate = coco.estimate("user");
        assert_eq!(estimate, 5);
    }

    #[test]
    fn estimate_with_udf_allows_custom_partial_matching() {
        // ensure custom UDF matching logic aggregates only intended buckets
        let mut coco: Coco = Coco::init_with_size(TEST_W, TEST_D);
        coco.insert("region=us|id=1", 4);
        coco.insert("region=eu|id=2", 6);

        fn matcher(full: &str, partial: &str) -> bool {
            full.contains(partial)
        }

        let total_us = coco.estimate_with_udf("us", matcher);
        assert_eq!(total_us, 4);

        let total_all = coco.estimate_with_udf("region", matcher);
        assert_eq!(total_all, 10);
    }

    #[test]
    fn merge_combines_tables_without_losing_counts() {
        // verify merging replays entries so both sketches contribute to totals
        let mut left: Coco = Coco::init_with_size(TEST_W, TEST_D);
        let mut right: Coco = Coco::init_with_size(TEST_W, TEST_D);

        left.insert("alpha:key", 7);
        right.insert("beta:key", 11);

        left.merge(&right);

        assert_eq!(left.estimate("alpha"), 7);
        assert_eq!(left.estimate("beta"), 11);
    }
}
