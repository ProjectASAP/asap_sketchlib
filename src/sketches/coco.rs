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
//! * <https://dl.acm.org/doi/10.1145/3452296.3472892>

use crate::{DataInput, DefaultXxHasher, SketchHasher, Vector2D};
use rand::Rng;
use rand::rngs::ThreadRng;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

/// One table slot: the key it currently represents and the mass attributed to it.
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

/// Buckets per array in a default `Coco`. Width sets the collision rate, and
/// `w * d` buckets at 32 bytes each set the table footprint.
const DEFAULT_WIDTH: usize = 1024;

/// Arrays in a default `Coco`, inside the paper's recommended 2..=4. Every
/// insert scans the `d` buckets its key maps to.
const DEFAULT_DEPTH: usize = 4;

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
    pub fn is_partial_key(&self, partial_key: &str) -> bool {
        match &self.full_key {
            Some(full) => full.contains(partial_key),
            None => false,
        }
    }
    /// the function should take in full key first, then partial key
    pub fn is_partial_key_with_udf<F>(&self, partial_key: &str, udf: F) -> bool
    where
        F: Fn(&str, &str) -> bool,
    {
        match &self.full_key {
            Some(k) => udf(k.as_str(), partial_key),
            None => false,
        }
    }

    pub fn debug(&self) {
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

    pub fn debug(&self) {
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

    /// Adds `v` to `key` using the paper's stochastic variance-optimized update.
    ///
    /// The `d` mapped buckets are scanned for `key` first; a match absorbs `v`
    /// directly. Otherwise the whole increment lands in the smallest of them,
    /// drawn uniformly at random when several share that smallest value, and
    /// that bucket's key is replaced with `key` with probability `v / val`.
    pub fn insert(&mut self, key: &str, v: u64) {
        if self.d == 0 || self.w == 0 {
            return;
        }
        let key_input = DataInput::Str(key);
        let mut rng: Option<ThreadRng> = None;
        let mut victim = (0usize, 0usize);
        let mut victim_val = u64::MAX;
        let mut tied = 0u32;

        for i in 0..self.d {
            let idx = H::hash64_seeded(i, &key_input) as usize % self.w;
            let bucket = &self.table[i][idx];
            if bucket.full_key.as_deref() == Some(key) {
                self.table[i][idx].val += v;
                return;
            }
            if bucket.val < victim_val {
                victim_val = bucket.val;
                victim = (i, idx);
                tied = 1;
            } else if bucket.val == victim_val {
                // reservoir sampling: the n-th tie takes the slot with probability 1/n
                tied += 1;
                if rng.get_or_insert_with(rand::rng).random_range(0..tied) == 0 {
                    victim = (i, idx);
                }
            }
        }

        let bucket = &mut self.table[victim.0][victim.1];
        bucket.val += v;
        let elected = match bucket.full_key {
            None => true,
            Some(_) => {
                let draw = rng
                    .get_or_insert_with(rand::rng)
                    .random_range(0.0..=1.0_f64);
                v as f64 > draw * bucket.val as f64
            }
        };
        if elected {
            bucket.update_key(key);
        }
    }

    /// Frequency estimate for `key` as defined by the paper: the sum of the `d`
    /// mapped buckets that currently hold `key`.
    pub fn estimate_key(&self, key: &str) -> u64 {
        if self.d == 0 || self.w == 0 {
            return 0;
        }
        let key_input = DataInput::Str(key);
        let mut total = 0;
        for i in 0..self.d {
            let idx = H::hash64_seeded(i, &key_input) as usize % self.w;
            if self.table[i][idx].full_key.as_deref() == Some(key) {
                total += self.table[i][idx].val;
            }
        }
        total
    }

    /// the udf parameter takes in full key first, and then partial key
    pub fn estimate_with_udf<F>(&self, partial_key: &str, udf: F) -> u64
    where
        F: Fn(&str, &str) -> bool,
    {
        let mut total = 0;
        for i in 0..self.d {
            for j in 0..self.w {
                if self.table[i][j].is_partial_key_with_udf(partial_key, &udf) {
                    total += self.table[i][j].val;
                }
            }
        }
        total
    }

    /// Partial-key query in the shape of the paper's `GROUP BY g(k_F)`: sums
    /// every occupied bucket whose stored full key projects to `partial_key`.
    pub fn estimate_projected<F>(&self, partial_key: &str, project: F) -> u64
    where
        F: for<'a> Fn(&'a str) -> &'a str,
    {
        let mut total = 0;
        for i in 0..self.d {
            for j in 0..self.w {
                if let Some(full) = &self.table[i][j].full_key {
                    if project(full.as_str()) == partial_key {
                        total += self.table[i][j].val;
                    }
                }
            }
        }
        total
    }

    /// Sums every bucket whose stored key contains `partial_key` anywhere.
    /// Containment is not a key projection: `"k1"` also collects `k10` and
    /// `k100`. Use [`Self::estimate_projected`] or [`Self::estimate_key`].
    pub fn estimate_substring(&self, partial_key: &str) -> u64 {
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

        let estimate = coco.estimate_substring("user");
        assert_eq!(estimate, 5);
        assert_eq!(coco.estimate_key(key), 5);
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
    fn tied_minimum_buckets_are_chosen_uniformly_at_random() {
        // every mapped bucket of a fresh table holds 0, so all TEST_D of them tie
        const TRIALS: usize = 2_000;
        let mut landings = [0usize; TEST_D];

        for _ in 0..TRIALS {
            let mut coco: Coco = Coco::init_with_size(TEST_W, TEST_D);
            coco.insert("flow::tie-probe", 1);
            let row = (0..TEST_D)
                .find(|i| {
                    (0..TEST_W)
                        .any(|j| coco.table[*i][j].full_key.as_deref() == Some("flow::tie-probe"))
                })
                .expect("the probe key must land somewhere");
            landings[row] += 1;
        }

        // uniform over 4 rows puts 500 in each; the band is ~15 sigma wide either way
        for (row, count) in landings.iter().enumerate() {
            assert!(
                *count > TRIALS / 10 && *count < TRIALS * 2 / 5,
                "row {row} took {count} of {TRIALS} landings, expected 200..800"
            );
        }
    }

    #[test]
    fn the_three_queries_disagree_on_a_key_that_prefixes_another() {
        // substring containment is not a key projection: "k1" also matches "k10"
        let mut coco: Coco = Coco::init_with_size(TEST_W, TEST_D);
        coco.insert("k1", 7);
        coco.insert("k10", 5);

        assert_eq!(coco.estimate_substring("k1"), 12);
        assert_eq!(coco.estimate_key("k1"), 7);
        assert_eq!(coco.estimate_projected("k1", |full| full), 7);
    }

    #[test]
    fn estimate_projected_aggregates_full_keys_sharing_a_partial_key() {
        // the paper's figure 7: two full keys on one srcip sum to that srcip
        let mut coco: Coco = Coco::init_with_size(TEST_W, TEST_D);
        coco.insert("19.98.10.26|80", 521);
        coco.insert("19.98.10.26|443", 520);
        coco.insert("34.52.73.17|118", 856);

        fn srcip(full: &str) -> &str {
            full.split('|').next().unwrap_or(full)
        }

        assert_eq!(coco.estimate_projected("19.98.10.26", srcip), 1041);
        assert_eq!(coco.estimate_projected("34.52.73.17", srcip), 856);
    }

    #[test]
    fn a_key_occupies_at_most_one_bucket_per_row() {
        // the match scan covers every row before a victim is chosen, so a key
        // already resident in a later row never gains a second home
        let mut coco: Coco = Coco::init_with_size(TEST_W, TEST_D);
        let key = "flow::single-home";

        for _ in 0..64 {
            coco.insert(key, 1);
        }

        let homes = (0..TEST_D)
            .flat_map(|i| (0..TEST_W).map(move |j| (i, j)))
            .filter(|(i, j)| coco.table[*i][*j].full_key.as_deref() == Some(key))
            .count();
        assert_eq!(homes, 1, "key must live in exactly one bucket");
        assert_eq!(coco.estimate_key(key), 64);
    }

    #[test]
    fn estimate_key_never_exceeds_the_inserted_mass() {
        // biased replacement is unbiased in expectation but never invents mass
        // beyond what the whole table holds
        let mut coco: Coco = Coco::init_with_size(8, 2);
        let mut total = 0u64;
        for i in 0..500u64 {
            coco.insert(&format!("k{}", i % 40), 3);
            total += 3;
        }

        let table_mass: u64 = (0..2)
            .flat_map(|i| (0..8).map(move |j| (i, j)))
            .map(|(i, j)| coco.table[i][j].val)
            .sum();
        assert_eq!(table_mass, total, "the table conserves the inserted mass");
        for i in 0..40u64 {
            assert!(coco.estimate_key(&format!("k{i}")) <= total);
        }
    }

    #[test]
    fn merge_combines_tables_without_losing_counts() {
        // verify merging replays entries so both sketches contribute to totals
        let mut left: Coco = Coco::init_with_size(TEST_W, TEST_D);
        let mut right: Coco = Coco::init_with_size(TEST_W, TEST_D);

        left.insert("alpha:key", 7);
        right.insert("beta:key", 11);

        left.merge(&right);

        assert_eq!(left.estimate_substring("alpha"), 7);
        assert_eq!(left.estimate_substring("beta"), 11);
    }
}
