//! Nitro Sketch, with batch processing
//! Assume the Nitro Sketch can get a batch of input
//! For streaming Nitro, please refers to Nitro struct in structure_utils.rs
//!
//! Reference:
//! - NitroSketch paper.
//!   <https://dl.acm.org/doi/10.1145/3341302.3342076>

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng, rng};
use serde::{Deserialize, Serialize};

use crate::{
    Count, CountMin, DataInput, FastPath, PRECOMPUTED_SAMPLE_RATE_1PERCENT, Vector2D,
    hash128_seeded,
};

/// Trait for sketch backends that support Nitro row updates.
pub trait NitroTarget {
    /// Returns the number of rows in the target sketch.
    fn rows(&self) -> usize;
    /// Applies a sampled update to one row.
    fn update_row(&mut self, row: usize, hashed: u128, delta: u64);
}

/// Trait for Nitro targets that can be merged.
pub trait NitroMerge {
    /// Merges another target into this one.
    fn merge(&mut self, other: &Self);
}

/// Trait for Nitro targets that support median-style estimation.
pub trait NitroEstimate {
    /// Returns the target's estimate for `value`.
    fn estimate_median(&self, value: &DataInput) -> f64;
}

impl NitroTarget for Vector2D<u32> {
    #[inline(always)]
    fn rows(&self) -> usize {
        self.rows()
    }

    #[inline(always)]
    fn update_row(&mut self, row: usize, hashed: u128, delta: u64) {
        self.update_by_row(row, hashed, |a, b| *a += b as u32, delta);
    }
}

impl NitroMerge for CountMin<Vector2D<i32>, FastPath> {
    #[inline(always)]
    fn merge(&mut self, other: &Self) {
        CountMin::merge(self, other);
    }
}

impl NitroEstimate for CountMin<Vector2D<i32>, FastPath> {
    #[inline(always)]
    fn estimate_median(&self, value: &DataInput) -> f64 {
        self.nitro_estimate(value)
    }
}

impl NitroMerge for Count<Vector2D<i32>, FastPath> {
    #[inline(always)]
    fn merge(&mut self, other: &Self) {
        Count::merge(self, other);
    }
}

impl NitroEstimate for Count<Vector2D<i32>, FastPath> {
    #[inline(always)]
    fn estimate_median(&self, value: &DataInput) -> f64 {
        self.estimate(value)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NitroBatch<S: NitroTarget> {
    sampling_rate: f64,
    /// Remaining items to skip before the next sampled update.
    pub to_skip: usize,
    inv_ln_one_minus_p: f64,
    /// Weight applied to each sampled update.
    pub delta: u64,
    #[serde(skip)]
    #[serde(default = "new_small_rng")]
    generator: SmallRng,
    idx: usize,
    mask: usize,
    sk: S,
}

fn new_small_rng() -> SmallRng {
    let mut seed_rng = rng();
    SmallRng::from_rng(&mut seed_rng)
}

impl Default for NitroBatch<Vector2D<u32>> {
    fn default() -> Self {
        let mut n = NitroBatch {
            sampling_rate: 0.0,
            to_skip: 0,
            inv_ln_one_minus_p: 0.0,
            delta: 0,
            generator: new_small_rng(),
            idx: 0,
            mask: 0x10000,
            sk: Vector2D::init(5, 2048),
        };
        n.sk.fill(0);
        n
    }
}

impl NitroBatch<Vector2D<u32>> {
    /// Creates a Nitro sketch with the given sampling rate.
    pub fn init_nitro(rate: f64) -> Self {
        assert!(
            !rate.is_nan() && rate > 0.0 && rate <= 1.0,
            "sample_rate must be within (0.0, 1.0]"
        );
        let inv_ln = if (rate - 1.0).abs() <= f64::EPSILON {
            0.0 // Not used for full sampling
        } else {
            1.0 / (1.0 - rate).ln()
        };
        let mut nitro = Self {
            sampling_rate: rate,
            to_skip: 0,
            inv_ln_one_minus_p: inv_ln,
            generator: new_small_rng(),
            delta: 0,
            idx: 0,
            mask: 0x10000,
            sk: Vector2D::init(5, 2048),
        };
        nitro.sk.fill(0);
        nitro.delta = nitro.scaled_increment(1);
        nitro
    }
}

impl<S: NitroTarget> NitroBatch<S> {
    /// Returns the wrapped target sketch.
    pub fn target(&self) -> &S {
        &self.sk
    }

    /// Returns the wrapped target sketch mutably.
    pub fn target_mut(&mut self) -> &mut S {
        &mut self.sk
    }

    /// Consumes the wrapper and returns the target sketch.
    pub fn into_target(self) -> S {
        self.sk
    }

    /// Wraps an existing target sketch with Nitro sampling.
    pub fn with_target(rate: f64, sk: S) -> Self {
        assert!(
            !rate.is_nan() && rate > 0.0 && rate <= 1.0,
            "sample_rate must be within (0.0, 1.0]"
        );
        let inv_ln = if (rate - 1.0).abs() <= f64::EPSILON {
            0.0 // Not used for full sampling
        } else {
            1.0 / (1.0 - rate).ln()
        };
        let mut nitro = Self {
            sampling_rate: rate,
            to_skip: 0,
            inv_ln_one_minus_p: inv_ln,
            generator: new_small_rng(),
            delta: 0,
            idx: 0,
            mask: 0x10000,
            sk,
        };
        nitro.delta = nitro.scaled_increment(1);
        nitro
    }

    // for profiling
    #[inline(always)]
    /// Draws the next geometric skip distance.
    pub fn draw_geometric(&mut self) {
        if self.is_full_sampling() {
            self.to_skip = 0;
            return;
        }
        let k = loop {
            let r = self.generator.random::<f64>();
            if r != 0.0_f64 && r != 1.0_f64 {
                break r;
            }
        };
        self.to_skip = ((1.0 - k).ln() * self.inv_ln_one_minus_p).ceil() as usize;
        self.idx = (self.idx + 1) & self.mask;
    }

    #[inline(always)]
    /// Decrements the current skip counter by one.
    pub fn reduce_to_skip(&mut self) {
        self.to_skip -= 1;
    }

    #[inline(always)]
    /// Decrements the current skip counter by `c`.
    pub fn reduce_to_skip_by_count(&mut self, c: usize) {
        self.to_skip -= c;
    }

    #[inline(always)]
    /// Returns the configured sampling rate.
    pub fn get_sampling_rate(&self) -> f64 {
        self.sampling_rate
    }

    // #[inline]
    #[inline(always)]
    /// Scales an update weight by the sampling rate.
    pub fn scaled_increment(&self, weight: u64) -> u64 {
        if self.is_full_sampling() {
            weight
        } else {
            ((weight as f64) / self.sampling_rate).ceil() as u64
        }
    }

    // #[inline]
    #[inline(always)]
    fn is_full_sampling(&self) -> bool {
        (self.sampling_rate - 1.0).abs() <= f64::EPSILON
    }

    #[inline(always)]
    /// Returns the current cached Nitro sampling state.
    pub fn get_ctx(&self) -> (usize, f64, usize, usize) {
        (self.idx, self.inv_ln_one_minus_p, self.to_skip, self.mask)
    }

    #[inline(always)]
    /// Restores the cached Nitro sampling state.
    pub fn commit_ctx(&mut self, idx: usize, to_skip: usize) {
        self.idx = idx;
        self.to_skip = to_skip;
    }

    /// Inserts a batch of values using geometric skipping.
    pub fn insert(&mut self, data: &[i64]) {
        let rows = self.sk.rows();
        self.draw_geometric();
        let mut position = self.to_skip;
        while position < data.len() {
            let row_to_update = position % rows;
            let hashed = hash128_seeded(0, &DataInput::I64(data[position]));
            self.sk.update_row(row_to_update, hashed, self.delta);
            self.draw_geometric();
            position += self.to_skip + 1;
        }
    }

    /// Inserts a batch using the precomputed skip table.
    pub fn insert_cached_step(&mut self, data: &[i64]) {
        let rows = self.sk.rows();
        self.to_skip = PRECOMPUTED_SAMPLE_RATE_1PERCENT[self.idx].ceil() as usize;
        self.idx = (self.idx + 1) & self.mask;
        let mut position = self.to_skip;
        while position < data.len() {
            let row_to_update = position % rows;
            let hashed = hash128_seeded(0, &DataInput::I64(data[position]));
            self.sk.update_row(row_to_update, hashed, self.delta);
            self.to_skip = PRECOMPUTED_SAMPLE_RATE_1PERCENT[self.idx].ceil() as usize;
            self.idx = (self.idx + 1) & self.mask;
            position += self.to_skip + 1;
        }
    }
}

impl<S: NitroTarget + NitroMerge> NitroBatch<S> {
    /// Merges another Nitro sketch with the same sampling rate.
    pub fn merge(&mut self, other: &Self) {
        assert!(
            (self.sampling_rate - other.sampling_rate).abs() <= f64::EPSILON,
            "nitro merge requires matching sampling rates"
        );
        self.sk.merge(&other.sk);
    }
}

impl<S: NitroTarget + NitroEstimate> NitroBatch<S> {
    /// Returns the wrapped sketch's estimate for `value`.
    pub fn estimate_median(&self, value: &DataInput) -> f64 {
        self.sk.estimate_median(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::sample_zipf_u64;
    use crate::{DataInput, compute_median_inline_f64};
    use std::collections::HashMap;

    fn nitro_countmin_estimate(storage: &Vector2D<i32>, key: &DataInput) -> f64 {
        let rows = storage.rows();
        let mask_bits = storage.get_mask_bits() as usize;
        let mask = (1u128 << mask_bits) - 1;
        let hashed = hash128_seeded(0, key);
        let mut min = i32::MAX;
        for row in 0..rows {
            let col = ((hashed >> (mask_bits * row)) & mask) as usize;
            let val = storage.query_one_counter(row, col);
            if val < min {
                min = val;
            }
        }
        min as f64
    }

    fn nitro_count_estimate(storage: &Vector2D<i32>, key: &DataInput) -> f64 {
        let rows = storage.rows();
        let mask_bits = storage.get_mask_bits() as usize;
        let mask = (1u128 << mask_bits) - 1;
        let hashed = hash128_seeded(0, key);
        let mut estimates = Vec::with_capacity(rows);
        for row in 0..rows {
            let col = ((hashed >> (mask_bits * row)) & mask) as usize;
            let val = storage.query_one_counter(row, col) as f64;
            let bit = (hashed >> (127 - row)) & 1;
            let sign = (bit as i32 * 2 - 1) as f64;
            estimates.push(sign * val);
        }
        compute_median_inline_f64(&mut estimates)
    }

    #[test]
    fn nitro_batch_countmin_error_bound_zipf() {
        let rows = 3;
        let cols = 4096;
        let domain = 8192;
        let exponent = 1.1;
        let samples = 200_000;
        let seed = 0x5eed_c0de;

        let mut truth = HashMap::<i64, u64>::new();
        let data: Vec<i64> = sample_zipf_u64(domain, exponent, samples, seed)
            .into_iter()
            .map(|v| {
                let key = v as i64;
                *truth.entry(key).or_insert(0) += 1;
                key
            })
            .collect();

        let cm = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(rows, cols);
        let mut batch = NitroBatch::with_target(1.0, cm);
        batch.insert(&data);

        let epsilon = std::f64::consts::E / cols as f64;
        let delta = 1.0 / std::f64::consts::E.powi(rows as i32);
        let error_bound = epsilon * samples as f64;
        let correct_lower_bound = truth.len() as f64 * (1.0 - delta);
        let storage = batch.target().as_storage();
        let mut within_count = 0;
        for key in truth.keys() {
            let est = nitro_countmin_estimate(storage, &DataInput::I64(*key));
            if (est - (*truth.get(key).unwrap() as f64)).abs() < error_bound {
                within_count += 1;
            }
        }
        assert!(
            within_count as f64 > correct_lower_bound,
            "in-bound items number {within_count} not greater than expected amount {correct_lower_bound}"
        );
    }

    #[test]
    fn nitro_batch_count_error_bound_zipf() {
        let rows = 3;
        let cols = 4096;
        let domain = 8192;
        let exponent = 1.1;
        let samples = 200_000;
        let seed = 0x5eed_c0de;

        let mut truth = HashMap::<i64, u64>::new();
        let data: Vec<i64> = sample_zipf_u64(domain, exponent, samples, seed)
            .into_iter()
            .map(|v| {
                let key = v as i64;
                *truth.entry(key).or_insert(0) += 1;
                key
            })
            .collect();

        let cs = Count::<Vector2D<i32>, FastPath>::with_dimensions(rows, cols);
        let mut batch = NitroBatch::with_target(1.0, cs);
        batch.insert(&data);

        let epsilon = std::f64::consts::E / cols as f64;
        let delta = 1.0 / std::f64::consts::E.powi(rows as i32);
        let error_bound = epsilon * samples as f64;
        let correct_lower_bound = truth.len() as f64 * (1.0 - delta);
        let storage = batch.target().as_storage();
        let mut within_count = 0;
        for key in truth.keys() {
            let est = nitro_count_estimate(storage, &DataInput::I64(*key));
            if (est - (*truth.get(key).unwrap() as f64)).abs() < error_bound {
                within_count += 1;
            }
        }
        assert!(
            within_count as f64 > correct_lower_bound,
            "in-bound items number {within_count} not greater than expected amount {correct_lower_bound}"
        );
    }
}
