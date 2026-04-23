//! `NumericalValue` trait: the minimal numeric contract for generic numeric sketches
//! (e.g., the generic `KLL<T>`).
//!
//! Implementors must be cheaply copyable, have a meaningful total ordering (even for
//! floats, where `PartialOrd` isn't total), a zero-like default, and a lossless-enough
//! projection into `f64` for quantile math.

use std::cmp::Ordering;

/// Minimal numeric trait used by generic sketch implementations.
pub trait NumericalValue: Copy + Default + Send + Sync + 'static {
    /// Compares two values using a total ordering.
    fn total_cmp(&self, other: &Self) -> Ordering;
    /// Converts the value into `f64` for sketch math.
    fn to_f64(self) -> f64;
}

macro_rules! impl_numerical_int {
    ($($t:ty),*) => {$(
        impl NumericalValue for $t {
            #[inline(always)]
            fn total_cmp(&self, other: &Self) -> Ordering { Ord::cmp(self, other) }
            #[inline(always)]
            fn to_f64(self) -> f64 { self as f64 }
        }
    )*};
}

macro_rules! impl_numerical_float {
    ($($t:ty),*) => {$(
        impl NumericalValue for $t {
            #[inline(always)]
            fn total_cmp(&self, other: &Self) -> Ordering { <$t>::total_cmp(self, other) }
            #[inline(always)]
            fn to_f64(self) -> f64 { self as f64 }
        }
    )*};
}

impl_numerical_int!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize
);
impl_numerical_float!(f32, f64);
