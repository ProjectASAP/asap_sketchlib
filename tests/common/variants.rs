#![allow(dead_code, unused_imports)]

use std::any::Any;

use asap_sketchlib::DataInput;
use asap_sketchlib::{impl_fixed_matrix, impl_hll_bucket_list};

use super::specs::CardinalityModel;

pub mod cms;
pub mod cs;
pub mod hll;

pub use cms::{countminsketch_topk_variants, countminsketch_variants};
pub use cs::{countsketch_topk_variants, countsketch_variants};
pub use hll::{hyperloglog_variants, portable_hll_variants};

impl_fixed_matrix!(Matrix3X512I32, i32, 3, 512);
impl_fixed_matrix!(Matrix3X512I64, i64, 3, 512);
impl_fixed_matrix!(Matrix3X512I128, i128, 3, 512);

impl_fixed_matrix!(Matrix3X1024I32, i32, 3, 1024);
impl_fixed_matrix!(Matrix3X1024I64, i64, 3, 1024);
impl_fixed_matrix!(Matrix3X1024I128, i128, 3, 1024);

impl_fixed_matrix!(Matrix3X2048I32, i32, 3, 2048);
impl_fixed_matrix!(Matrix3X2048I64, i64, 3, 2048);
impl_fixed_matrix!(Matrix3X2048I128, i128, 3, 2048);

impl_fixed_matrix!(Matrix3X4096I32, i32, 3, 4096);
impl_fixed_matrix!(Matrix3X4096I64, i64, 3, 4096);
impl_fixed_matrix!(Matrix3X4096I128, i128, 3, 4096);

impl_fixed_matrix!(Matrix3X8192I32, i32, 3, 8192);
impl_fixed_matrix!(Matrix3X8192I64, i64, 3, 8192);
impl_fixed_matrix!(Matrix3X8192I128, i128, 3, 8192);

impl_fixed_matrix!(Matrix3X16384I32, i32, 3, 16384);
impl_fixed_matrix!(Matrix3X16384I64, i64, 3, 16384);
impl_fixed_matrix!(Matrix3X16384I128, i128, 3, 16384);

impl_fixed_matrix!(Matrix3X32768I32, i32, 3, 32768);
impl_fixed_matrix!(Matrix3X32768I64, i64, 3, 32768);
impl_fixed_matrix!(Matrix3X32768I128, i128, 3, 32768);

impl_fixed_matrix!(Matrix5X512I32, i32, 5, 512);
impl_fixed_matrix!(Matrix5X512I64, i64, 5, 512);
impl_fixed_matrix!(Matrix5X512I128, i128, 5, 512);

impl_fixed_matrix!(Matrix5X1024I32, i32, 5, 1024);
impl_fixed_matrix!(Matrix5X1024I64, i64, 5, 1024);
impl_fixed_matrix!(Matrix5X1024I128, i128, 5, 1024);

impl_fixed_matrix!(Matrix5X2048I32, i32, 5, 2048);
impl_fixed_matrix!(Matrix5X2048I64, i64, 5, 2048);
impl_fixed_matrix!(Matrix5X2048I128, i128, 5, 2048);

impl_fixed_matrix!(Matrix5X4096I32, i32, 5, 4096);
impl_fixed_matrix!(Matrix5X4096I64, i64, 5, 4096);
impl_fixed_matrix!(Matrix5X4096I128, i128, 5, 4096);

impl_fixed_matrix!(Matrix5X8192I32, i32, 5, 8192);
impl_fixed_matrix!(Matrix5X8192I64, i64, 5, 8192);
impl_fixed_matrix!(Matrix5X8192I128, i128, 5, 8192);

impl_fixed_matrix!(Matrix5X16384I32, i32, 5, 16384);
impl_fixed_matrix!(Matrix5X16384I64, i64, 5, 16384);
impl_fixed_matrix!(Matrix5X16384I128, i128, 5, 16384);

impl_fixed_matrix!(Matrix5X32768I32, i32, 5, 32768);
impl_fixed_matrix!(Matrix5X32768I64, i64, 5, 32768);
impl_fixed_matrix!(Matrix5X32768I128, i128, 5, 32768);

impl_fixed_matrix!(Matrix7X512I32, i32, 7, 512);
impl_fixed_matrix!(Matrix7X512I64, i64, 7, 512);
impl_fixed_matrix!(Matrix7X512I128, i128, 7, 512);

impl_fixed_matrix!(Matrix7X1024I32, i32, 7, 1024);
impl_fixed_matrix!(Matrix7X1024I64, i64, 7, 1024);
impl_fixed_matrix!(Matrix7X1024I128, i128, 7, 1024);

impl_fixed_matrix!(Matrix7X2048I32, i32, 7, 2048);
impl_fixed_matrix!(Matrix7X2048I64, i64, 7, 2048);
impl_fixed_matrix!(Matrix7X2048I128, i128, 7, 2048);

impl_fixed_matrix!(Matrix7X4096I32, i32, 7, 4096);
impl_fixed_matrix!(Matrix7X4096I64, i64, 7, 4096);
impl_fixed_matrix!(Matrix7X4096I128, i128, 7, 4096);

impl_fixed_matrix!(Matrix7X8192I32, i32, 7, 8192);
impl_fixed_matrix!(Matrix7X8192I64, i64, 7, 8192);
impl_fixed_matrix!(Matrix7X8192I128, i128, 7, 8192);

impl_fixed_matrix!(Matrix7X16384I32, i32, 7, 16384);
impl_fixed_matrix!(Matrix7X16384I64, i64, 7, 16384);
impl_fixed_matrix!(Matrix7X16384I128, i128, 7, 16384);

impl_fixed_matrix!(Matrix7X32768I32, i32, 7, 32768);
impl_fixed_matrix!(Matrix7X32768I64, i64, 7, 32768);
impl_fixed_matrix!(Matrix7X32768I128, i128, 7, 32768);

pub trait VariantCounter: Copy {
    fn as_f64(self) -> f64;
}
impl VariantCounter for i32 {
    fn as_f64(self) -> f64 {
        self as f64
    }
}
impl VariantCounter for i64 {
    fn as_f64(self) -> f64 {
        self as f64
    }
}
impl VariantCounter for i128 {
    fn as_f64(self) -> f64 {
        self as f64
    }
}
impl VariantCounter for f64 {
    fn as_f64(self) -> f64 {
        self
    }
}

pub trait FrequencyVariant: Any {
    fn insert(&mut self, key: &DataInput);
    fn merge(&mut self, other: &dyn FrequencyVariant);
    fn query(&self, key: &DataInput) -> f64;
    fn dims(&self) -> (usize, usize);
}

pub type VariantList = Vec<(&'static str, Box<dyn FrequencyVariant>)>;

impl_hll_bucket_list!(HllBucketListP8, 8, 1_usize << 8);
impl_hll_bucket_list!(HllBucketListP9, 9, 1_usize << 9);
impl_hll_bucket_list!(HllBucketListP10, 10, 1_usize << 10);
impl_hll_bucket_list!(HllBucketListP11, 11, 1_usize << 11);
impl_hll_bucket_list!(HllBucketListP12, 12, 1_usize << 12);
impl_hll_bucket_list!(HllBucketListP13, 13, 1_usize << 13);
impl_hll_bucket_list!(HllBucketListP14, 14, 1_usize << 14);
impl_hll_bucket_list!(HllBucketListP15, 15, 1_usize << 15);
impl_hll_bucket_list!(HllBucketListP16, 16, 1_usize << 16);
impl_hll_bucket_list!(HllBucketListP17, 17, 1_usize << 17);
impl_hll_bucket_list!(HllBucketListP18, 18, 1_usize << 18);

pub trait CardinalityVariant: Any {
    fn insert(&mut self, key: &DataInput);
    fn merge(&mut self, other: &dyn CardinalityVariant);
    fn estimate(&self) -> f64;
    fn registers(&self) -> usize;
    fn sigma_rel(&self) -> f64;
}

pub type CardinalityVariantList = Vec<(&'static str, Box<dyn CardinalityVariant>)>;
