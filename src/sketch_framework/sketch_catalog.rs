//! Erased fast-path adapter traits for matrix-backed sketches.
//!
//! These traits let `HashSketchEnsemble` hold type-erased `Box<dyn CountMinFastOps>`
//! and `Box<dyn CountFastOps>` without knowing the concrete storage or hasher.

use crate::common::structure_utils::ToF64;
use crate::sketches::count::CountSketchCounter;
use crate::{Count, CountMin, DataInput, FastPath, MatrixHashType, RegularPath, SketchHasher};
use std::ops::AddAssign;

pub trait CountMinRegularOps {
    fn insert(&mut self, val: &DataInput);
    fn estimate_f64(&self, val: &DataInput) -> f64;
}

pub trait CountMinFastOps {
    fn insert(&mut self, val: &DataInput);
    fn estimate_f64(&self, val: &DataInput) -> f64;
    fn rows(&self) -> usize;
    fn cols(&self) -> usize;
    fn fast_insert(&mut self, hash: &MatrixHashType);
    fn fast_estimate(&self, hash: &MatrixHashType) -> f64;
}

pub trait CountRegularOps {
    fn insert(&mut self, val: &DataInput);
    fn estimate_f64(&self, val: &DataInput) -> f64;
}

pub trait CountFastOps {
    fn insert(&mut self, val: &DataInput);
    fn estimate_f64(&self, val: &DataInput) -> f64;
    fn rows(&self) -> usize;
    fn cols(&self) -> usize;
    fn fast_insert(&mut self, hash: &MatrixHashType);
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
