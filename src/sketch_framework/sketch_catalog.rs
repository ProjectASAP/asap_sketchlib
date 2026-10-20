//! Erased fast-path adapter traits for matrix-backed sketches.
//!
//! These traits let `HashSketchEnsemble` hold type-erased `Box<dyn CountMinFastOps>`
//! and `Box<dyn CountSketchFastOps>` without knowing the concrete storage or hasher.

use crate::common::structure_utils::ToF64;
use crate::sketches::countsketch::CountSketchCounter;
use crate::{Count, CountMinSketch, DataInput, FastPath, MatrixHash, RegularPath, SketchHasher};
use std::ops::AddAssign;

/// Type-erased adapter for regular-path Count-Min sketches.
pub trait CountMinRegularOps {
    /// Inserts one value into the sketch.
    fn update(&mut self, val: &DataInput);
    /// Returns the estimate as `f64`.
    fn estimate_f64(&self, val: &DataInput) -> f64;
}

/// Type-erased adapter for fast-path Count-Min sketches.
pub trait CountMinFastOps {
    /// Inserts one value into the sketch.
    fn update(&mut self, val: &DataInput);
    /// Returns the estimate as `f64`.
    fn estimate_f64(&self, val: &DataInput) -> f64;
    /// Returns the row count.
    fn rows(&self) -> usize;
    /// Returns the column count.
    fn cols(&self) -> usize;
    /// Inserts one precomputed hash.
    fn fast_insert(&mut self, hash: &MatrixHash);
    /// Returns the estimate for one precomputed hash.
    fn fast_estimate(&self, hash: &MatrixHash) -> f64;
}

/// Type-erased adapter for regular-path Count Sketches.
pub trait CountSketchRegularOps {
    /// Inserts one value into the sketch.
    fn update(&mut self, val: &DataInput);
    /// Returns the estimate as `f64`.
    fn estimate_f64(&self, val: &DataInput) -> f64;
}

/// Type-erased adapter for fast-path Count Sketches.
pub trait CountSketchFastOps {
    /// Inserts one value into the sketch.
    fn update(&mut self, val: &DataInput);
    /// Returns the estimate as `f64`.
    fn estimate_f64(&self, val: &DataInput) -> f64;
    /// Returns the row count.
    fn rows(&self) -> usize;
    /// Returns the column count.
    fn cols(&self) -> usize;
    /// Inserts one precomputed hash.
    fn fast_insert(&mut self, hash: &MatrixHash);
    /// Returns the estimate for one precomputed hash.
    fn fast_estimate(&self, hash: &MatrixHash) -> f64;
}

impl<S> CountMinRegularOps for CountMinSketch<S, RegularPath>
where
    S: crate::MatrixStorage + 'static,
    S::Counter: Copy + PartialOrd + From<i32> + AddAssign + ToF64 + 'static,
{
    fn update(&mut self, val: &DataInput) {
        self.update(val);
    }

    fn estimate_f64(&self, val: &DataInput) -> f64 {
        self.estimate(val).to_f64()
    }
}

impl<S, H> CountMinFastOps for CountMinSketch<S, FastPath, H>
where
    H: SketchHasher<HashType = MatrixHash> + 'static,
    S: crate::MatrixStorage + crate::FastPathHasher<H> + 'static,
    S::Counter: Copy + PartialOrd + From<i32> + AddAssign + ToF64 + 'static,
{
    fn update(&mut self, val: &DataInput) {
        self.update(val);
    }

    fn estimate_f64(&self, val: &DataInput) -> f64 {
        self.estimate(val).to_f64()
    }

    fn rows(&self) -> usize {
        self.rows()
    }

    fn cols(&self) -> usize {
        self.cols()
    }

    fn fast_insert(&mut self, hash: &MatrixHash) {
        self.update_with_hash(hash);
    }

    fn fast_estimate(&self, hash: &MatrixHash) -> f64 {
        self.estimate_with_hash(hash).to_f64()
    }
}

impl<S> CountSketchRegularOps for Count<S, RegularPath>
where
    S: crate::MatrixStorage + 'static,
    S::Counter: CountSketchCounter + 'static,
{
    fn update(&mut self, val: &DataInput) {
        self.update(val);
    }

    fn estimate_f64(&self, val: &DataInput) -> f64 {
        self.estimate(val)
    }
}

impl<S, H> CountSketchFastOps for Count<S, FastPath, H>
where
    H: SketchHasher<HashType = MatrixHash> + 'static,
    S: crate::MatrixStorage + crate::FastPathHasher<H> + 'static,
    S::Counter: CountSketchCounter + 'static,
{
    fn update(&mut self, val: &DataInput) {
        self.update(val);
    }

    fn estimate_f64(&self, val: &DataInput) -> f64 {
        self.estimate(val)
    }

    fn rows(&self) -> usize {
        self.rows()
    }

    fn cols(&self) -> usize {
        self.cols()
    }

    fn fast_insert(&mut self, hash: &MatrixHash) {
        self.update_with_hash(hash);
    }

    fn fast_estimate(&self, hash: &MatrixHash) -> f64 {
        self.estimate_with_hash(hash)
    }
}
