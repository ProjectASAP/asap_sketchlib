//! Erased fast-path adapter traits for matrix-backed sketches.
//!
//! These traits let `HashSketchEnsemble` hold type-erased `Box<dyn CountMinFastOps>`
//! and `Box<dyn CountFastOps>` without knowing the concrete storage or hasher.

use crate::common::structure_utils::ToF64;
use crate::sketches::countsketch::CountSketchCounter;
use crate::{Count, CountMin, DataInput, FastPath, MatrixHashType, RegularPath, SketchHasher};
use std::ops::AddAssign;

/// Type-erased adapter for regular-path Count-Min sketches.
pub trait CountMinRegularOps {
    /// Inserts one value into the sketch.
    fn insert(&mut self, val: &DataInput);
    /// Returns the estimate as `f64`.
    fn estimate_f64(&self, val: &DataInput) -> f64;
}

/// Type-erased adapter for fast-path Count-Min sketches.
pub trait CountMinFastOps {
    /// Inserts one value into the sketch.
    fn insert(&mut self, val: &DataInput);
    /// Returns the estimate as `f64`.
    fn estimate_f64(&self, val: &DataInput) -> f64;
    /// Returns the row count.
    fn rows(&self) -> usize;
    /// Returns the column count.
    fn cols(&self) -> usize;
    /// Inserts one precomputed hash.
    fn fast_insert(&mut self, hash: &MatrixHashType);
    /// Returns the estimate for one precomputed hash.
    fn fast_estimate(&self, hash: &MatrixHashType) -> f64;
}

/// Type-erased adapter for regular-path Count Sketches.
pub trait CountRegularOps {
    /// Inserts one value into the sketch.
    fn insert(&mut self, val: &DataInput);
    /// Returns the estimate as `f64`.
    fn estimate_f64(&self, val: &DataInput) -> f64;
}

/// Type-erased adapter for fast-path Count Sketches.
pub trait CountFastOps {
    /// Inserts one value into the sketch.
    fn insert(&mut self, val: &DataInput);
    /// Returns the estimate as `f64`.
    fn estimate_f64(&self, val: &DataInput) -> f64;
    /// Returns the row count.
    fn rows(&self) -> usize;
    /// Returns the column count.
    fn cols(&self) -> usize;
    /// Inserts one precomputed hash.
    fn fast_insert(&mut self, hash: &MatrixHashType);
    /// Returns the estimate for one precomputed hash.
    fn fast_estimate(&self, hash: &MatrixHashType) -> f64;
}

impl<S> CountMinRegularOps for CountMin<S, RegularPath>
where
    S: crate::MatrixStorage + 'static,
    S::Counter: Copy + PartialOrd + From<i32> + AddAssign + ToF64 + 'static,
{
    fn insert(&mut self, val: &DataInput) {
        self.insert(val);
    }

    fn estimate_f64(&self, val: &DataInput) -> f64 {
        self.estimate(val).to_f64()
    }
}

impl<S, H> CountMinFastOps for CountMin<S, FastPath, H>
where
    H: SketchHasher<HashType = MatrixHashType> + 'static,
    S: crate::MatrixStorage + crate::FastPathHasher<H> + 'static,
    S::Counter: Copy + PartialOrd + From<i32> + AddAssign + ToF64 + 'static,
{
    fn insert(&mut self, val: &DataInput) {
        self.insert(val);
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

    fn fast_insert(&mut self, hash: &MatrixHashType) {
        self.fast_insert_with_hash_value(hash);
    }

    fn fast_estimate(&self, hash: &MatrixHashType) -> f64 {
        self.fast_estimate_with_hash(hash).to_f64()
    }
}

impl<S> CountRegularOps for Count<S, RegularPath>
where
    S: crate::MatrixStorage + 'static,
    S::Counter: CountSketchCounter + 'static,
{
    fn insert(&mut self, val: &DataInput) {
        self.insert(val);
    }

    fn estimate_f64(&self, val: &DataInput) -> f64 {
        self.estimate(val)
    }
}

impl<S, H> CountFastOps for Count<S, FastPath, H>
where
    H: SketchHasher<HashType = MatrixHashType> + 'static,
    S: crate::MatrixStorage + crate::FastPathHasher<H> + 'static,
    S::Counter: CountSketchCounter + 'static,
{
    fn insert(&mut self, val: &DataInput) {
        self.insert(val);
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

    fn fast_insert(&mut self, hash: &MatrixHashType) {
        self.fast_insert_with_hash_value(hash);
    }

    fn fast_estimate(&self, hash: &MatrixHashType) -> f64 {
        self.fast_estimate_with_hash(hash)
    }
}
