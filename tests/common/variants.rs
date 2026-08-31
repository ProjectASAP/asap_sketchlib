#![allow(dead_code)]

use std::any::Any;

use asap_sketchlib::sketches::countsketch::CountSketchCounter;
use asap_sketchlib::{
    CMSHeap, CSHeap, Count, CountMin, DataInput, DefaultXxHasher, FastPath, FastPathHasher,
    FixedMatrix, MatrixStorage, QuickMatrixI64, QuickMatrixI128, RegularPath, Vector2D,
    impl_fixed_matrix,
};

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
    fn insert(&mut self, key: u64);
    fn merge(&mut self, other: &dyn FrequencyVariant);
    fn query(&self, key: u64) -> f64;
    fn dims(&self) -> (usize, usize);
}

pub type VariantList = Vec<(&'static str, Box<dyn FrequencyVariant>)>;

impl<S> FrequencyVariant for CountMin<S, RegularPath, DefaultXxHasher>
where
    S: MatrixStorage + 'static,
    S::Counter: VariantCounter + Copy + PartialOrd + From<i32> + std::ops::AddAssign,
{
    fn insert(&mut self, key: u64) {
        CountMin::<S, RegularPath, DefaultXxHasher>::insert(self, &DataInput::U64(key));
    }
    fn merge(&mut self, other: &dyn FrequencyVariant) {
        let other = (other as &dyn Any)
            .downcast_ref::<Self>()
            .expect("merge requires both variants to be the same concrete sketch type");
        CountMin::<S, RegularPath, DefaultXxHasher>::merge(self, other);
    }
    fn query(&self, key: u64) -> f64 {
        self.estimate(&DataInput::U64(key)).as_f64()
    }
    fn dims(&self) -> (usize, usize) {
        (self.rows(), self.cols())
    }
}

impl<S> FrequencyVariant for CountMin<S, FastPath, DefaultXxHasher>
where
    S: MatrixStorage + FastPathHasher<DefaultXxHasher> + 'static,
    S::Counter: VariantCounter + Copy + PartialOrd + From<i32> + std::ops::AddAssign,
{
    fn insert(&mut self, key: u64) {
        CountMin::<S, FastPath, DefaultXxHasher>::insert(self, &DataInput::U64(key));
    }
    fn merge(&mut self, other: &dyn FrequencyVariant) {
        let other = (other as &dyn Any)
            .downcast_ref::<Self>()
            .expect("merge requires both variants to be the same concrete sketch type");
        CountMin::<S, FastPath, DefaultXxHasher>::merge(self, other);
    }
    fn query(&self, key: u64) -> f64 {
        self.estimate(&DataInput::U64(key)).as_f64()
    }
    fn dims(&self) -> (usize, usize) {
        (self.rows(), self.cols())
    }
}

impl<S> FrequencyVariant for Count<S, RegularPath, DefaultXxHasher>
where
    S: MatrixStorage + 'static,
    S::Counter: CountSketchCounter,
{
    fn insert(&mut self, key: u64) {
        Count::<S, RegularPath, DefaultXxHasher>::insert(self, &DataInput::U64(key));
    }
    fn merge(&mut self, other: &dyn FrequencyVariant) {
        let other = (other as &dyn Any)
            .downcast_ref::<Self>()
            .expect("merge requires both variants to be the same concrete sketch type");
        Count::<S, RegularPath, DefaultXxHasher>::merge(self, other);
    }
    fn query(&self, key: u64) -> f64 {
        self.estimate(&DataInput::U64(key))
    }
    fn dims(&self) -> (usize, usize) {
        (self.rows(), self.cols())
    }
}

impl<S> FrequencyVariant for Count<S, FastPath, DefaultXxHasher>
where
    S: MatrixStorage + FastPathHasher<DefaultXxHasher> + 'static,
    S::Counter: CountSketchCounter,
{
    fn insert(&mut self, key: u64) {
        Count::<S, FastPath, DefaultXxHasher>::insert(self, &DataInput::U64(key));
    }
    fn merge(&mut self, other: &dyn FrequencyVariant) {
        let other = (other as &dyn Any)
            .downcast_ref::<Self>()
            .expect("merge requires both variants to be the same concrete sketch type");
        Count::<S, FastPath, DefaultXxHasher>::merge(self, other);
    }
    fn query(&self, key: u64) -> f64 {
        self.estimate(&DataInput::U64(key))
    }
    fn dims(&self) -> (usize, usize) {
        (self.rows(), self.cols())
    }
}

impl<S> FrequencyVariant for CMSHeap<S, RegularPath, DefaultXxHasher>
where
    S: MatrixStorage + 'static,
    S::Counter: VariantCounter + Copy + Ord + From<i32> + Into<i64> + std::ops::AddAssign,
{
    fn insert(&mut self, key: u64) {
        CMSHeap::<S, RegularPath, DefaultXxHasher>::insert(self, &DataInput::U64(key));
    }
    fn merge(&mut self, other: &dyn FrequencyVariant) {
        let other = (other as &dyn Any)
            .downcast_ref::<Self>()
            .expect("merge requires both variants to be the same concrete sketch type");
        CMSHeap::<S, RegularPath, DefaultXxHasher>::merge(self, other);
    }
    fn query(&self, key: u64) -> f64 {
        self.estimate(&DataInput::U64(key)).as_f64()
    }
    fn dims(&self) -> (usize, usize) {
        (self.rows(), self.cols())
    }
}

impl<S> FrequencyVariant for CMSHeap<S, FastPath, DefaultXxHasher>
where
    S: MatrixStorage + FastPathHasher<DefaultXxHasher> + 'static,
    S::Counter: VariantCounter + Copy + Ord + From<i32> + Into<i64> + std::ops::AddAssign,
{
    fn insert(&mut self, key: u64) {
        CMSHeap::<S, FastPath, DefaultXxHasher>::insert(self, &DataInput::U64(key));
    }
    fn merge(&mut self, other: &dyn FrequencyVariant) {
        let other = (other as &dyn Any)
            .downcast_ref::<Self>()
            .expect("merge requires both variants to be the same concrete sketch type");
        CMSHeap::<S, FastPath, DefaultXxHasher>::merge(self, other);
    }
    fn query(&self, key: u64) -> f64 {
        self.estimate(&DataInput::U64(key)).as_f64()
    }
    fn dims(&self) -> (usize, usize) {
        (self.rows(), self.cols())
    }
}

impl<S> FrequencyVariant for CSHeap<S, RegularPath, DefaultXxHasher>
where
    S: MatrixStorage + 'static,
    S::Counter: CountSketchCounter,
{
    fn insert(&mut self, key: u64) {
        CSHeap::<S, RegularPath, DefaultXxHasher>::insert(self, &DataInput::U64(key));
    }
    fn merge(&mut self, other: &dyn FrequencyVariant) {
        let other = (other as &dyn Any)
            .downcast_ref::<Self>()
            .expect("merge requires both variants to be the same concrete sketch type");
        CSHeap::<S, RegularPath, DefaultXxHasher>::merge(self, other);
    }
    fn query(&self, key: u64) -> f64 {
        self.estimate(&DataInput::U64(key))
    }
    fn dims(&self) -> (usize, usize) {
        (self.rows(), self.cols())
    }
}

impl<S> FrequencyVariant for CSHeap<S, FastPath, DefaultXxHasher>
where
    S: MatrixStorage + FastPathHasher<DefaultXxHasher> + 'static,
    S::Counter: CountSketchCounter,
{
    fn insert(&mut self, key: u64) {
        CSHeap::<S, FastPath, DefaultXxHasher>::insert(self, &DataInput::U64(key));
    }
    fn merge(&mut self, other: &dyn FrequencyVariant) {
        let other = (other as &dyn Any)
            .downcast_ref::<Self>()
            .expect("merge requires both variants to be the same concrete sketch type");
        CSHeap::<S, FastPath, DefaultXxHasher>::merge(self, other);
    }
    fn query(&self, key: u64) -> f64 {
        self.estimate(&DataInput::U64(key))
    }
    fn dims(&self) -> (usize, usize) {
        (self.rows(), self.cols())
    }
}

pub fn countminsketch_variants() -> VariantList {
    vec![
        (
            "CountMin<Vector2D<i32>, RegularPath> 3x512",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                3, 512,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 3x512",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 512)),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 3x512",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                3, 512,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 3x512",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(3, 512)),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 3x512",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                3, 512,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 3x512",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                3, 512,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 3x512",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                3, 512,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 3x512",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(3, 512)),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 3x1024",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                3, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 3x1024",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                3, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 3x1024",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                3, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 3x1024",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                3, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 3x1024",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                3, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 3x1024",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                3, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 3x1024",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                3, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 3x1024",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                3, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 3x2048",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                3, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 3x2048",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                3, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 3x2048",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                3, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 3x2048",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                3, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 3x2048",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                3, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 3x2048",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                3, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 3x2048",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                3, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 3x2048",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                3, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 3x4096",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                3, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 3x4096",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                3, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 3x4096",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                3, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 3x4096",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                3, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 3x4096",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                3, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 3x4096",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                3, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 3x4096",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                3, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 3x4096",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                3, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 3x8192",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                3, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 3x8192",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                3, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 3x8192",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                3, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 3x8192",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                3, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 3x8192",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                3, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 3x8192",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                3, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 3x8192",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                3, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 3x8192",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                3, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 3x16384",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                3, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 3x16384",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                3, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 3x16384",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                3, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 3x16384",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                3, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 3x16384",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                3, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 3x16384",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                3, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 3x16384",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                3, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 3x16384",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                3, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 3x32768",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                3, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 3x32768",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                3, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 3x32768",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                3, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 3x32768",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                3, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 3x32768",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                3, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 3x32768",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                3, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 3x32768",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                3, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 3x32768",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                3, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 5x512",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                5, 512,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 5x512",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(5, 512)),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 5x512",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                5, 512,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 5x512",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(5, 512)),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 5x512",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                5, 512,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 5x512",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                5, 512,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 5x512",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                5, 512,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 5x512",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(5, 512)),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 5x1024",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                5, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 5x1024",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                5, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 5x1024",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                5, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 5x1024",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                5, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 5x1024",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                5, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 5x1024",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                5, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 5x1024",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                5, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 5x1024",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                5, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 5x2048",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                5, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 5x2048",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                5, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 5x2048",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                5, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 5x2048",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                5, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 5x2048",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                5, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 5x2048",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                5, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 5x2048",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                5, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 5x2048",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                5, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 5x4096",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                5, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 5x4096",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                5, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 5x4096",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                5, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 5x4096",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                5, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 5x4096",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                5, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 5x4096",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                5, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 5x4096",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                5, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 5x4096",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                5, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 5x8192",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                5, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 5x8192",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                5, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 5x8192",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                5, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 5x8192",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                5, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 5x8192",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                5, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 5x8192",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                5, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 5x8192",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                5, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 5x8192",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                5, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 5x16384",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                5, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 5x16384",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                5, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 5x16384",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                5, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 5x16384",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                5, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 5x16384",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                5, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 5x16384",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                5, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 5x16384",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                5, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 5x16384",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                5, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 5x32768",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                5, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 5x32768",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                5, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 5x32768",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                5, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 5x32768",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                5, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 5x32768",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                5, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 5x32768",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                5, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 5x32768",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                5, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 5x32768",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                5, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 7x512",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                7, 512,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 7x512",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(7, 512)),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 7x512",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                7, 512,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 7x512",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(7, 512)),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 7x512",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                7, 512,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 7x512",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                7, 512,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 7x512",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                7, 512,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 7x512",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(7, 512)),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 7x1024",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                7, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 7x1024",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                7, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 7x1024",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                7, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 7x1024",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                7, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 7x1024",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                7, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 7x1024",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                7, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 7x1024",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                7, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 7x1024",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                7, 1024,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 7x2048",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                7, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 7x2048",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                7, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 7x2048",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                7, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 7x2048",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                7, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 7x2048",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                7, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 7x2048",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                7, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 7x2048",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                7, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 7x2048",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                7, 2048,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 7x4096",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                7, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 7x4096",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                7, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 7x4096",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                7, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 7x4096",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                7, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 7x4096",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                7, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 7x4096",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                7, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 7x4096",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                7, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 7x4096",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                7, 4096,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 7x8192",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                7, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 7x8192",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                7, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 7x8192",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                7, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 7x8192",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                7, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 7x8192",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                7, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 7x8192",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                7, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 7x8192",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                7, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 7x8192",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                7, 8192,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 7x16384",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                7, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 7x16384",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                7, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 7x16384",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                7, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 7x16384",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                7, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 7x16384",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                7, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 7x16384",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                7, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 7x16384",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                7, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 7x16384",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                7, 16384,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, RegularPath> 7x32768",
            Box::new(CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(
                7, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i32>, FastPath> 7x32768",
            Box::new(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(
                7, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, RegularPath> 7x32768",
            Box::new(CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(
                7, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i64>, FastPath> 7x32768",
            Box::new(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                7, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, RegularPath> 7x32768",
            Box::new(CountMin::<Vector2D<i128>, RegularPath>::with_dimensions(
                7, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<i128>, FastPath> 7x32768",
            Box::new(CountMin::<Vector2D<i128>, FastPath>::with_dimensions(
                7, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, RegularPath> 7x32768",
            Box::new(CountMin::<Vector2D<f64>, RegularPath>::with_dimensions(
                7, 32768,
            )),
        ),
        (
            "CountMin<Vector2D<f64>, FastPath> 7x32768",
            Box::new(CountMin::<Vector2D<f64>, FastPath>::with_dimensions(
                7, 32768,
            )),
        ),
        (
            "CountMin<Matrix3X512I32, RegularPath>",
            Box::new(CountMin::<Matrix3X512I32, RegularPath>::from_storage(
                Matrix3X512I32::default(),
            )),
        ),
        (
            "CountMin<Matrix3X512I32, FastPath>",
            Box::new(CountMin::<Matrix3X512I32, FastPath>::from_storage(
                Matrix3X512I32::default(),
            )),
        ),
        (
            "CountMin<Matrix3X512I64, RegularPath>",
            Box::new(CountMin::<Matrix3X512I64, RegularPath>::from_storage(
                Matrix3X512I64::default(),
            )),
        ),
        (
            "CountMin<Matrix3X512I64, FastPath>",
            Box::new(CountMin::<Matrix3X512I64, FastPath>::from_storage(
                Matrix3X512I64::default(),
            )),
        ),
        (
            "CountMin<Matrix3X512I128, RegularPath>",
            Box::new(CountMin::<Matrix3X512I128, RegularPath>::from_storage(
                Matrix3X512I128::default(),
            )),
        ),
        (
            "CountMin<Matrix3X512I128, FastPath>",
            Box::new(CountMin::<Matrix3X512I128, FastPath>::from_storage(
                Matrix3X512I128::default(),
            )),
        ),
        (
            "CountMin<Matrix3X1024I32, RegularPath>",
            Box::new(CountMin::<Matrix3X1024I32, RegularPath>::from_storage(
                Matrix3X1024I32::default(),
            )),
        ),
        (
            "CountMin<Matrix3X1024I32, FastPath>",
            Box::new(CountMin::<Matrix3X1024I32, FastPath>::from_storage(
                Matrix3X1024I32::default(),
            )),
        ),
        (
            "CountMin<Matrix3X1024I64, RegularPath>",
            Box::new(CountMin::<Matrix3X1024I64, RegularPath>::from_storage(
                Matrix3X1024I64::default(),
            )),
        ),
        (
            "CountMin<Matrix3X1024I64, FastPath>",
            Box::new(CountMin::<Matrix3X1024I64, FastPath>::from_storage(
                Matrix3X1024I64::default(),
            )),
        ),
        (
            "CountMin<Matrix3X1024I128, RegularPath>",
            Box::new(CountMin::<Matrix3X1024I128, RegularPath>::from_storage(
                Matrix3X1024I128::default(),
            )),
        ),
        (
            "CountMin<Matrix3X1024I128, FastPath>",
            Box::new(CountMin::<Matrix3X1024I128, FastPath>::from_storage(
                Matrix3X1024I128::default(),
            )),
        ),
        (
            "CountMin<Matrix3X2048I32, RegularPath>",
            Box::new(CountMin::<Matrix3X2048I32, RegularPath>::from_storage(
                Matrix3X2048I32::default(),
            )),
        ),
        (
            "CountMin<Matrix3X2048I32, FastPath>",
            Box::new(CountMin::<Matrix3X2048I32, FastPath>::from_storage(
                Matrix3X2048I32::default(),
            )),
        ),
        (
            "CountMin<Matrix3X2048I64, RegularPath>",
            Box::new(CountMin::<Matrix3X2048I64, RegularPath>::from_storage(
                Matrix3X2048I64::default(),
            )),
        ),
        (
            "CountMin<Matrix3X2048I64, FastPath>",
            Box::new(CountMin::<Matrix3X2048I64, FastPath>::from_storage(
                Matrix3X2048I64::default(),
            )),
        ),
        (
            "CountMin<Matrix3X2048I128, RegularPath>",
            Box::new(CountMin::<Matrix3X2048I128, RegularPath>::from_storage(
                Matrix3X2048I128::default(),
            )),
        ),
        (
            "CountMin<Matrix3X2048I128, FastPath>",
            Box::new(CountMin::<Matrix3X2048I128, FastPath>::from_storage(
                Matrix3X2048I128::default(),
            )),
        ),
        (
            "CountMin<Matrix3X4096I32, RegularPath>",
            Box::new(CountMin::<Matrix3X4096I32, RegularPath>::from_storage(
                Matrix3X4096I32::default(),
            )),
        ),
        (
            "CountMin<Matrix3X4096I32, FastPath>",
            Box::new(CountMin::<Matrix3X4096I32, FastPath>::from_storage(
                Matrix3X4096I32::default(),
            )),
        ),
        (
            "CountMin<Matrix3X4096I64, RegularPath>",
            Box::new(CountMin::<Matrix3X4096I64, RegularPath>::from_storage(
                Matrix3X4096I64::default(),
            )),
        ),
        (
            "CountMin<Matrix3X4096I64, FastPath>",
            Box::new(CountMin::<Matrix3X4096I64, FastPath>::from_storage(
                Matrix3X4096I64::default(),
            )),
        ),
        (
            "CountMin<Matrix3X4096I128, RegularPath>",
            Box::new(CountMin::<Matrix3X4096I128, RegularPath>::from_storage(
                Matrix3X4096I128::default(),
            )),
        ),
        (
            "CountMin<Matrix3X4096I128, FastPath>",
            Box::new(CountMin::<Matrix3X4096I128, FastPath>::from_storage(
                Matrix3X4096I128::default(),
            )),
        ),
        (
            "CountMin<Matrix3X8192I32, RegularPath>",
            Box::new(CountMin::<Matrix3X8192I32, RegularPath>::from_storage(
                Matrix3X8192I32::default(),
            )),
        ),
        (
            "CountMin<Matrix3X8192I32, FastPath>",
            Box::new(CountMin::<Matrix3X8192I32, FastPath>::from_storage(
                Matrix3X8192I32::default(),
            )),
        ),
        (
            "CountMin<Matrix3X8192I64, RegularPath>",
            Box::new(CountMin::<Matrix3X8192I64, RegularPath>::from_storage(
                Matrix3X8192I64::default(),
            )),
        ),
        (
            "CountMin<Matrix3X8192I64, FastPath>",
            Box::new(CountMin::<Matrix3X8192I64, FastPath>::from_storage(
                Matrix3X8192I64::default(),
            )),
        ),
        (
            "CountMin<Matrix3X8192I128, RegularPath>",
            Box::new(CountMin::<Matrix3X8192I128, RegularPath>::from_storage(
                Matrix3X8192I128::default(),
            )),
        ),
        (
            "CountMin<Matrix3X8192I128, FastPath>",
            Box::new(CountMin::<Matrix3X8192I128, FastPath>::from_storage(
                Matrix3X8192I128::default(),
            )),
        ),
        (
            "CountMin<Matrix3X16384I32, RegularPath>",
            Box::new(CountMin::<Matrix3X16384I32, RegularPath>::from_storage(
                Matrix3X16384I32::default(),
            )),
        ),
        (
            "CountMin<Matrix3X16384I32, FastPath>",
            Box::new(CountMin::<Matrix3X16384I32, FastPath>::from_storage(
                Matrix3X16384I32::default(),
            )),
        ),
        (
            "CountMin<Matrix3X16384I64, RegularPath>",
            Box::new(CountMin::<Matrix3X16384I64, RegularPath>::from_storage(
                Matrix3X16384I64::default(),
            )),
        ),
        (
            "CountMin<Matrix3X16384I64, FastPath>",
            Box::new(CountMin::<Matrix3X16384I64, FastPath>::from_storage(
                Matrix3X16384I64::default(),
            )),
        ),
        (
            "CountMin<Matrix3X16384I128, RegularPath>",
            Box::new(CountMin::<Matrix3X16384I128, RegularPath>::from_storage(
                Matrix3X16384I128::default(),
            )),
        ),
        (
            "CountMin<Matrix3X16384I128, FastPath>",
            Box::new(CountMin::<Matrix3X16384I128, FastPath>::from_storage(
                Matrix3X16384I128::default(),
            )),
        ),
        (
            "CountMin<Matrix3X32768I32, RegularPath>",
            Box::new(CountMin::<Matrix3X32768I32, RegularPath>::from_storage(
                Matrix3X32768I32::default(),
            )),
        ),
        (
            "CountMin<Matrix3X32768I32, FastPath>",
            Box::new(CountMin::<Matrix3X32768I32, FastPath>::from_storage(
                Matrix3X32768I32::default(),
            )),
        ),
        (
            "CountMin<Matrix3X32768I64, RegularPath>",
            Box::new(CountMin::<Matrix3X32768I64, RegularPath>::from_storage(
                Matrix3X32768I64::default(),
            )),
        ),
        (
            "CountMin<Matrix3X32768I64, FastPath>",
            Box::new(CountMin::<Matrix3X32768I64, FastPath>::from_storage(
                Matrix3X32768I64::default(),
            )),
        ),
        (
            "CountMin<Matrix3X32768I128, RegularPath>",
            Box::new(CountMin::<Matrix3X32768I128, RegularPath>::from_storage(
                Matrix3X32768I128::default(),
            )),
        ),
        (
            "CountMin<Matrix3X32768I128, FastPath>",
            Box::new(CountMin::<Matrix3X32768I128, FastPath>::from_storage(
                Matrix3X32768I128::default(),
            )),
        ),
        (
            "CountMin<Matrix5X512I32, RegularPath>",
            Box::new(CountMin::<Matrix5X512I32, RegularPath>::from_storage(
                Matrix5X512I32::default(),
            )),
        ),
        (
            "CountMin<Matrix5X512I32, FastPath>",
            Box::new(CountMin::<Matrix5X512I32, FastPath>::from_storage(
                Matrix5X512I32::default(),
            )),
        ),
        (
            "CountMin<Matrix5X512I64, RegularPath>",
            Box::new(CountMin::<Matrix5X512I64, RegularPath>::from_storage(
                Matrix5X512I64::default(),
            )),
        ),
        (
            "CountMin<Matrix5X512I64, FastPath>",
            Box::new(CountMin::<Matrix5X512I64, FastPath>::from_storage(
                Matrix5X512I64::default(),
            )),
        ),
        (
            "CountMin<Matrix5X512I128, RegularPath>",
            Box::new(CountMin::<Matrix5X512I128, RegularPath>::from_storage(
                Matrix5X512I128::default(),
            )),
        ),
        (
            "CountMin<Matrix5X512I128, FastPath>",
            Box::new(CountMin::<Matrix5X512I128, FastPath>::from_storage(
                Matrix5X512I128::default(),
            )),
        ),
        (
            "CountMin<Matrix5X1024I32, RegularPath>",
            Box::new(CountMin::<Matrix5X1024I32, RegularPath>::from_storage(
                Matrix5X1024I32::default(),
            )),
        ),
        (
            "CountMin<Matrix5X1024I32, FastPath>",
            Box::new(CountMin::<Matrix5X1024I32, FastPath>::from_storage(
                Matrix5X1024I32::default(),
            )),
        ),
        (
            "CountMin<Matrix5X1024I64, RegularPath>",
            Box::new(CountMin::<Matrix5X1024I64, RegularPath>::from_storage(
                Matrix5X1024I64::default(),
            )),
        ),
        (
            "CountMin<Matrix5X1024I64, FastPath>",
            Box::new(CountMin::<Matrix5X1024I64, FastPath>::from_storage(
                Matrix5X1024I64::default(),
            )),
        ),
        (
            "CountMin<Matrix5X1024I128, RegularPath>",
            Box::new(CountMin::<Matrix5X1024I128, RegularPath>::from_storage(
                Matrix5X1024I128::default(),
            )),
        ),
        (
            "CountMin<Matrix5X1024I128, FastPath>",
            Box::new(CountMin::<Matrix5X1024I128, FastPath>::from_storage(
                Matrix5X1024I128::default(),
            )),
        ),
        (
            "CountMin<Matrix5X2048I32, RegularPath>",
            Box::new(CountMin::<Matrix5X2048I32, RegularPath>::from_storage(
                Matrix5X2048I32::default(),
            )),
        ),
        (
            "CountMin<Matrix5X2048I32, FastPath>",
            Box::new(CountMin::<Matrix5X2048I32, FastPath>::from_storage(
                Matrix5X2048I32::default(),
            )),
        ),
        (
            "CountMin<Matrix5X2048I64, RegularPath>",
            Box::new(CountMin::<Matrix5X2048I64, RegularPath>::from_storage(
                Matrix5X2048I64::default(),
            )),
        ),
        (
            "CountMin<Matrix5X2048I64, FastPath>",
            Box::new(CountMin::<Matrix5X2048I64, FastPath>::from_storage(
                Matrix5X2048I64::default(),
            )),
        ),
        (
            "CountMin<Matrix5X2048I128, RegularPath>",
            Box::new(CountMin::<Matrix5X2048I128, RegularPath>::from_storage(
                Matrix5X2048I128::default(),
            )),
        ),
        (
            "CountMin<Matrix5X2048I128, FastPath>",
            Box::new(CountMin::<Matrix5X2048I128, FastPath>::from_storage(
                Matrix5X2048I128::default(),
            )),
        ),
        (
            "CountMin<Matrix5X4096I32, RegularPath>",
            Box::new(CountMin::<Matrix5X4096I32, RegularPath>::from_storage(
                Matrix5X4096I32::default(),
            )),
        ),
        (
            "CountMin<Matrix5X4096I32, FastPath>",
            Box::new(CountMin::<Matrix5X4096I32, FastPath>::from_storage(
                Matrix5X4096I32::default(),
            )),
        ),
        (
            "CountMin<Matrix5X4096I64, RegularPath>",
            Box::new(CountMin::<Matrix5X4096I64, RegularPath>::from_storage(
                Matrix5X4096I64::default(),
            )),
        ),
        (
            "CountMin<Matrix5X4096I64, FastPath>",
            Box::new(CountMin::<Matrix5X4096I64, FastPath>::from_storage(
                Matrix5X4096I64::default(),
            )),
        ),
        (
            "CountMin<Matrix5X4096I128, RegularPath>",
            Box::new(CountMin::<Matrix5X4096I128, RegularPath>::from_storage(
                Matrix5X4096I128::default(),
            )),
        ),
        (
            "CountMin<Matrix5X4096I128, FastPath>",
            Box::new(CountMin::<Matrix5X4096I128, FastPath>::from_storage(
                Matrix5X4096I128::default(),
            )),
        ),
        (
            "CountMin<Matrix5X8192I32, RegularPath>",
            Box::new(CountMin::<Matrix5X8192I32, RegularPath>::from_storage(
                Matrix5X8192I32::default(),
            )),
        ),
        (
            "CountMin<Matrix5X8192I32, FastPath>",
            Box::new(CountMin::<Matrix5X8192I32, FastPath>::from_storage(
                Matrix5X8192I32::default(),
            )),
        ),
        (
            "CountMin<Matrix5X8192I64, RegularPath>",
            Box::new(CountMin::<Matrix5X8192I64, RegularPath>::from_storage(
                Matrix5X8192I64::default(),
            )),
        ),
        (
            "CountMin<Matrix5X8192I64, FastPath>",
            Box::new(CountMin::<Matrix5X8192I64, FastPath>::from_storage(
                Matrix5X8192I64::default(),
            )),
        ),
        (
            "CountMin<Matrix5X8192I128, RegularPath>",
            Box::new(CountMin::<Matrix5X8192I128, RegularPath>::from_storage(
                Matrix5X8192I128::default(),
            )),
        ),
        (
            "CountMin<Matrix5X8192I128, FastPath>",
            Box::new(CountMin::<Matrix5X8192I128, FastPath>::from_storage(
                Matrix5X8192I128::default(),
            )),
        ),
        (
            "CountMin<Matrix5X16384I32, RegularPath>",
            Box::new(CountMin::<Matrix5X16384I32, RegularPath>::from_storage(
                Matrix5X16384I32::default(),
            )),
        ),
        (
            "CountMin<Matrix5X16384I32, FastPath>",
            Box::new(CountMin::<Matrix5X16384I32, FastPath>::from_storage(
                Matrix5X16384I32::default(),
            )),
        ),
        (
            "CountMin<Matrix5X16384I64, RegularPath>",
            Box::new(CountMin::<Matrix5X16384I64, RegularPath>::from_storage(
                Matrix5X16384I64::default(),
            )),
        ),
        (
            "CountMin<Matrix5X16384I64, FastPath>",
            Box::new(CountMin::<Matrix5X16384I64, FastPath>::from_storage(
                Matrix5X16384I64::default(),
            )),
        ),
        (
            "CountMin<Matrix5X16384I128, RegularPath>",
            Box::new(CountMin::<Matrix5X16384I128, RegularPath>::from_storage(
                Matrix5X16384I128::default(),
            )),
        ),
        (
            "CountMin<Matrix5X16384I128, FastPath>",
            Box::new(CountMin::<Matrix5X16384I128, FastPath>::from_storage(
                Matrix5X16384I128::default(),
            )),
        ),
        (
            "CountMin<Matrix5X32768I32, RegularPath>",
            Box::new(CountMin::<Matrix5X32768I32, RegularPath>::from_storage(
                Matrix5X32768I32::default(),
            )),
        ),
        (
            "CountMin<Matrix5X32768I32, FastPath>",
            Box::new(CountMin::<Matrix5X32768I32, FastPath>::from_storage(
                Matrix5X32768I32::default(),
            )),
        ),
        (
            "CountMin<Matrix5X32768I64, RegularPath>",
            Box::new(CountMin::<Matrix5X32768I64, RegularPath>::from_storage(
                Matrix5X32768I64::default(),
            )),
        ),
        (
            "CountMin<Matrix5X32768I64, FastPath>",
            Box::new(CountMin::<Matrix5X32768I64, FastPath>::from_storage(
                Matrix5X32768I64::default(),
            )),
        ),
        (
            "CountMin<Matrix5X32768I128, RegularPath>",
            Box::new(CountMin::<Matrix5X32768I128, RegularPath>::from_storage(
                Matrix5X32768I128::default(),
            )),
        ),
        (
            "CountMin<Matrix5X32768I128, FastPath>",
            Box::new(CountMin::<Matrix5X32768I128, FastPath>::from_storage(
                Matrix5X32768I128::default(),
            )),
        ),
        (
            "CountMin<Matrix7X512I32, RegularPath>",
            Box::new(CountMin::<Matrix7X512I32, RegularPath>::from_storage(
                Matrix7X512I32::default(),
            )),
        ),
        (
            "CountMin<Matrix7X512I32, FastPath>",
            Box::new(CountMin::<Matrix7X512I32, FastPath>::from_storage(
                Matrix7X512I32::default(),
            )),
        ),
        (
            "CountMin<Matrix7X512I64, RegularPath>",
            Box::new(CountMin::<Matrix7X512I64, RegularPath>::from_storage(
                Matrix7X512I64::default(),
            )),
        ),
        (
            "CountMin<Matrix7X512I64, FastPath>",
            Box::new(CountMin::<Matrix7X512I64, FastPath>::from_storage(
                Matrix7X512I64::default(),
            )),
        ),
        (
            "CountMin<Matrix7X512I128, RegularPath>",
            Box::new(CountMin::<Matrix7X512I128, RegularPath>::from_storage(
                Matrix7X512I128::default(),
            )),
        ),
        (
            "CountMin<Matrix7X512I128, FastPath>",
            Box::new(CountMin::<Matrix7X512I128, FastPath>::from_storage(
                Matrix7X512I128::default(),
            )),
        ),
        (
            "CountMin<Matrix7X1024I32, RegularPath>",
            Box::new(CountMin::<Matrix7X1024I32, RegularPath>::from_storage(
                Matrix7X1024I32::default(),
            )),
        ),
        (
            "CountMin<Matrix7X1024I32, FastPath>",
            Box::new(CountMin::<Matrix7X1024I32, FastPath>::from_storage(
                Matrix7X1024I32::default(),
            )),
        ),
        (
            "CountMin<Matrix7X1024I64, RegularPath>",
            Box::new(CountMin::<Matrix7X1024I64, RegularPath>::from_storage(
                Matrix7X1024I64::default(),
            )),
        ),
        (
            "CountMin<Matrix7X1024I64, FastPath>",
            Box::new(CountMin::<Matrix7X1024I64, FastPath>::from_storage(
                Matrix7X1024I64::default(),
            )),
        ),
        (
            "CountMin<Matrix7X1024I128, RegularPath>",
            Box::new(CountMin::<Matrix7X1024I128, RegularPath>::from_storage(
                Matrix7X1024I128::default(),
            )),
        ),
        (
            "CountMin<Matrix7X1024I128, FastPath>",
            Box::new(CountMin::<Matrix7X1024I128, FastPath>::from_storage(
                Matrix7X1024I128::default(),
            )),
        ),
        (
            "CountMin<Matrix7X2048I32, RegularPath>",
            Box::new(CountMin::<Matrix7X2048I32, RegularPath>::from_storage(
                Matrix7X2048I32::default(),
            )),
        ),
        (
            "CountMin<Matrix7X2048I32, FastPath>",
            Box::new(CountMin::<Matrix7X2048I32, FastPath>::from_storage(
                Matrix7X2048I32::default(),
            )),
        ),
        (
            "CountMin<Matrix7X2048I64, RegularPath>",
            Box::new(CountMin::<Matrix7X2048I64, RegularPath>::from_storage(
                Matrix7X2048I64::default(),
            )),
        ),
        (
            "CountMin<Matrix7X2048I64, FastPath>",
            Box::new(CountMin::<Matrix7X2048I64, FastPath>::from_storage(
                Matrix7X2048I64::default(),
            )),
        ),
        (
            "CountMin<Matrix7X2048I128, RegularPath>",
            Box::new(CountMin::<Matrix7X2048I128, RegularPath>::from_storage(
                Matrix7X2048I128::default(),
            )),
        ),
        (
            "CountMin<Matrix7X2048I128, FastPath>",
            Box::new(CountMin::<Matrix7X2048I128, FastPath>::from_storage(
                Matrix7X2048I128::default(),
            )),
        ),
        (
            "CountMin<Matrix7X4096I32, RegularPath>",
            Box::new(CountMin::<Matrix7X4096I32, RegularPath>::from_storage(
                Matrix7X4096I32::default(),
            )),
        ),
        (
            "CountMin<Matrix7X4096I32, FastPath>",
            Box::new(CountMin::<Matrix7X4096I32, FastPath>::from_storage(
                Matrix7X4096I32::default(),
            )),
        ),
        (
            "CountMin<Matrix7X4096I64, RegularPath>",
            Box::new(CountMin::<Matrix7X4096I64, RegularPath>::from_storage(
                Matrix7X4096I64::default(),
            )),
        ),
        (
            "CountMin<Matrix7X4096I64, FastPath>",
            Box::new(CountMin::<Matrix7X4096I64, FastPath>::from_storage(
                Matrix7X4096I64::default(),
            )),
        ),
        (
            "CountMin<Matrix7X4096I128, RegularPath>",
            Box::new(CountMin::<Matrix7X4096I128, RegularPath>::from_storage(
                Matrix7X4096I128::default(),
            )),
        ),
        (
            "CountMin<Matrix7X4096I128, FastPath>",
            Box::new(CountMin::<Matrix7X4096I128, FastPath>::from_storage(
                Matrix7X4096I128::default(),
            )),
        ),
        (
            "CountMin<Matrix7X8192I32, RegularPath>",
            Box::new(CountMin::<Matrix7X8192I32, RegularPath>::from_storage(
                Matrix7X8192I32::default(),
            )),
        ),
        (
            "CountMin<Matrix7X8192I32, FastPath>",
            Box::new(CountMin::<Matrix7X8192I32, FastPath>::from_storage(
                Matrix7X8192I32::default(),
            )),
        ),
        (
            "CountMin<Matrix7X8192I64, RegularPath>",
            Box::new(CountMin::<Matrix7X8192I64, RegularPath>::from_storage(
                Matrix7X8192I64::default(),
            )),
        ),
        (
            "CountMin<Matrix7X8192I64, FastPath>",
            Box::new(CountMin::<Matrix7X8192I64, FastPath>::from_storage(
                Matrix7X8192I64::default(),
            )),
        ),
        (
            "CountMin<Matrix7X8192I128, RegularPath>",
            Box::new(CountMin::<Matrix7X8192I128, RegularPath>::from_storage(
                Matrix7X8192I128::default(),
            )),
        ),
        (
            "CountMin<Matrix7X8192I128, FastPath>",
            Box::new(CountMin::<Matrix7X8192I128, FastPath>::from_storage(
                Matrix7X8192I128::default(),
            )),
        ),
        (
            "CountMin<Matrix7X16384I32, RegularPath>",
            Box::new(CountMin::<Matrix7X16384I32, RegularPath>::from_storage(
                Matrix7X16384I32::default(),
            )),
        ),
        (
            "CountMin<Matrix7X16384I32, FastPath>",
            Box::new(CountMin::<Matrix7X16384I32, FastPath>::from_storage(
                Matrix7X16384I32::default(),
            )),
        ),
        (
            "CountMin<Matrix7X16384I64, RegularPath>",
            Box::new(CountMin::<Matrix7X16384I64, RegularPath>::from_storage(
                Matrix7X16384I64::default(),
            )),
        ),
        (
            "CountMin<Matrix7X16384I64, FastPath>",
            Box::new(CountMin::<Matrix7X16384I64, FastPath>::from_storage(
                Matrix7X16384I64::default(),
            )),
        ),
        (
            "CountMin<Matrix7X16384I128, RegularPath>",
            Box::new(CountMin::<Matrix7X16384I128, RegularPath>::from_storage(
                Matrix7X16384I128::default(),
            )),
        ),
        (
            "CountMin<Matrix7X16384I128, FastPath>",
            Box::new(CountMin::<Matrix7X16384I128, FastPath>::from_storage(
                Matrix7X16384I128::default(),
            )),
        ),
        (
            "CountMin<Matrix7X32768I32, RegularPath>",
            Box::new(CountMin::<Matrix7X32768I32, RegularPath>::from_storage(
                Matrix7X32768I32::default(),
            )),
        ),
        (
            "CountMin<Matrix7X32768I32, FastPath>",
            Box::new(CountMin::<Matrix7X32768I32, FastPath>::from_storage(
                Matrix7X32768I32::default(),
            )),
        ),
        (
            "CountMin<Matrix7X32768I64, RegularPath>",
            Box::new(CountMin::<Matrix7X32768I64, RegularPath>::from_storage(
                Matrix7X32768I64::default(),
            )),
        ),
        (
            "CountMin<Matrix7X32768I64, FastPath>",
            Box::new(CountMin::<Matrix7X32768I64, FastPath>::from_storage(
                Matrix7X32768I64::default(),
            )),
        ),
        (
            "CountMin<Matrix7X32768I128, RegularPath>",
            Box::new(CountMin::<Matrix7X32768I128, RegularPath>::from_storage(
                Matrix7X32768I128::default(),
            )),
        ),
        (
            "CountMin<Matrix7X32768I128, FastPath>",
            Box::new(CountMin::<Matrix7X32768I128, FastPath>::from_storage(
                Matrix7X32768I128::default(),
            )),
        ),
    ]
}

pub fn countsketch_variants() -> VariantList {
    vec![
        (
            "Count<Vector2D<i32>, RegularPath> 3x512",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(3, 512)),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 3x512",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 512)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 3x512",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(3, 512)),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 3x512",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 512)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 3x512",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                3, 512,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 3x512",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 512)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 3x1024",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                3, 1024,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 3x1024",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 1024)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 3x1024",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                3, 1024,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 3x1024",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 1024)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 3x1024",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                3, 1024,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 3x1024",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 1024)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 3x2048",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                3, 2048,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 3x2048",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 2048)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 3x2048",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                3, 2048,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 3x2048",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 2048)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 3x2048",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                3, 2048,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 3x2048",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 2048)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 3x4096",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                3, 4096,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 3x4096",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 4096)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 3x4096",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                3, 4096,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 3x4096",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 4096)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 3x4096",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                3, 4096,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 3x4096",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 4096)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 3x8192",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                3, 8192,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 3x8192",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 8192)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 3x8192",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                3, 8192,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 3x8192",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 8192)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 3x8192",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                3, 8192,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 3x8192",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 8192)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 3x16384",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                3, 16384,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 3x16384",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 16384)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 3x16384",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                3, 16384,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 3x16384",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 16384)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 3x16384",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                3, 16384,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 3x16384",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 16384)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 3x32768",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                3, 32768,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 3x32768",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(3, 32768)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 3x32768",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                3, 32768,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 3x32768",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(3, 32768)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 3x32768",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                3, 32768,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 3x32768",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(3, 32768)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 5x512",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(5, 512)),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 5x512",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 512)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 5x512",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(5, 512)),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 5x512",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 512)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 5x512",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                5, 512,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 5x512",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 512)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 5x1024",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                5, 1024,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 5x1024",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 1024)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 5x1024",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                5, 1024,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 5x1024",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 1024)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 5x1024",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                5, 1024,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 5x1024",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 1024)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 5x2048",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                5, 2048,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 5x2048",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 2048)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 5x2048",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                5, 2048,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 5x2048",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 2048)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 5x2048",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                5, 2048,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 5x2048",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 2048)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 5x4096",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                5, 4096,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 5x4096",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 4096)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 5x4096",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                5, 4096,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 5x4096",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 4096)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 5x4096",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                5, 4096,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 5x4096",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 4096)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 5x8192",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                5, 8192,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 5x8192",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 8192)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 5x8192",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                5, 8192,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 5x8192",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 8192)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 5x8192",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                5, 8192,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 5x8192",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 8192)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 5x16384",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                5, 16384,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 5x16384",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 16384)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 5x16384",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                5, 16384,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 5x16384",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 16384)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 5x16384",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                5, 16384,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 5x16384",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 16384)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 5x32768",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                5, 32768,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 5x32768",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(5, 32768)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 5x32768",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                5, 32768,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 5x32768",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(5, 32768)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 5x32768",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                5, 32768,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 5x32768",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(5, 32768)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 7x512",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(7, 512)),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 7x512",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 512)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 7x512",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(7, 512)),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 7x512",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 512)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 7x512",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                7, 512,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 7x512",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 512)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 7x1024",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                7, 1024,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 7x1024",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 1024)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 7x1024",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                7, 1024,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 7x1024",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 1024)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 7x1024",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                7, 1024,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 7x1024",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 1024)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 7x2048",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                7, 2048,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 7x2048",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 2048)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 7x2048",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                7, 2048,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 7x2048",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 2048)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 7x2048",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                7, 2048,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 7x2048",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 2048)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 7x4096",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                7, 4096,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 7x4096",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 4096)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 7x4096",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                7, 4096,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 7x4096",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 4096)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 7x4096",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                7, 4096,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 7x4096",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 4096)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 7x8192",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                7, 8192,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 7x8192",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 8192)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 7x8192",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                7, 8192,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 7x8192",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 8192)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 7x8192",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                7, 8192,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 7x8192",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 8192)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 7x16384",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                7, 16384,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 7x16384",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 16384)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 7x16384",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                7, 16384,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 7x16384",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 16384)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 7x16384",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                7, 16384,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 7x16384",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 16384)),
        ),
        (
            "Count<Vector2D<i32>, RegularPath> 7x32768",
            Box::new(Count::<Vector2D<i32>, RegularPath>::with_dimensions(
                7, 32768,
            )),
        ),
        (
            "Count<Vector2D<i32>, FastPath> 7x32768",
            Box::new(Count::<Vector2D<i32>, FastPath>::with_dimensions(7, 32768)),
        ),
        (
            "Count<Vector2D<i64>, RegularPath> 7x32768",
            Box::new(Count::<Vector2D<i64>, RegularPath>::with_dimensions(
                7, 32768,
            )),
        ),
        (
            "Count<Vector2D<i64>, FastPath> 7x32768",
            Box::new(Count::<Vector2D<i64>, FastPath>::with_dimensions(7, 32768)),
        ),
        (
            "Count<Vector2D<i128>, RegularPath> 7x32768",
            Box::new(Count::<Vector2D<i128>, RegularPath>::with_dimensions(
                7, 32768,
            )),
        ),
        (
            "Count<Vector2D<i128>, FastPath> 7x32768",
            Box::new(Count::<Vector2D<i128>, FastPath>::with_dimensions(7, 32768)),
        ),
        (
            "Count<Matrix3X512I32, RegularPath>",
            Box::new(Count::<Matrix3X512I32, RegularPath>::from_storage(
                Matrix3X512I32::default(),
            )),
        ),
        (
            "Count<Matrix3X512I32, FastPath>",
            Box::new(Count::<Matrix3X512I32, FastPath>::from_storage(
                Matrix3X512I32::default(),
            )),
        ),
        (
            "Count<Matrix3X512I64, RegularPath>",
            Box::new(Count::<Matrix3X512I64, RegularPath>::from_storage(
                Matrix3X512I64::default(),
            )),
        ),
        (
            "Count<Matrix3X512I64, FastPath>",
            Box::new(Count::<Matrix3X512I64, FastPath>::from_storage(
                Matrix3X512I64::default(),
            )),
        ),
        (
            "Count<Matrix3X512I128, RegularPath>",
            Box::new(Count::<Matrix3X512I128, RegularPath>::from_storage(
                Matrix3X512I128::default(),
            )),
        ),
        (
            "Count<Matrix3X512I128, FastPath>",
            Box::new(Count::<Matrix3X512I128, FastPath>::from_storage(
                Matrix3X512I128::default(),
            )),
        ),
        (
            "Count<Matrix3X1024I32, RegularPath>",
            Box::new(Count::<Matrix3X1024I32, RegularPath>::from_storage(
                Matrix3X1024I32::default(),
            )),
        ),
        (
            "Count<Matrix3X1024I32, FastPath>",
            Box::new(Count::<Matrix3X1024I32, FastPath>::from_storage(
                Matrix3X1024I32::default(),
            )),
        ),
        (
            "Count<Matrix3X1024I64, RegularPath>",
            Box::new(Count::<Matrix3X1024I64, RegularPath>::from_storage(
                Matrix3X1024I64::default(),
            )),
        ),
        (
            "Count<Matrix3X1024I64, FastPath>",
            Box::new(Count::<Matrix3X1024I64, FastPath>::from_storage(
                Matrix3X1024I64::default(),
            )),
        ),
        (
            "Count<Matrix3X1024I128, RegularPath>",
            Box::new(Count::<Matrix3X1024I128, RegularPath>::from_storage(
                Matrix3X1024I128::default(),
            )),
        ),
        (
            "Count<Matrix3X1024I128, FastPath>",
            Box::new(Count::<Matrix3X1024I128, FastPath>::from_storage(
                Matrix3X1024I128::default(),
            )),
        ),
        (
            "Count<Matrix3X2048I32, RegularPath>",
            Box::new(Count::<Matrix3X2048I32, RegularPath>::from_storage(
                Matrix3X2048I32::default(),
            )),
        ),
        (
            "Count<Matrix3X2048I32, FastPath>",
            Box::new(Count::<Matrix3X2048I32, FastPath>::from_storage(
                Matrix3X2048I32::default(),
            )),
        ),
        (
            "Count<Matrix3X2048I64, RegularPath>",
            Box::new(Count::<Matrix3X2048I64, RegularPath>::from_storage(
                Matrix3X2048I64::default(),
            )),
        ),
        (
            "Count<Matrix3X2048I64, FastPath>",
            Box::new(Count::<Matrix3X2048I64, FastPath>::from_storage(
                Matrix3X2048I64::default(),
            )),
        ),
        (
            "Count<Matrix3X2048I128, RegularPath>",
            Box::new(Count::<Matrix3X2048I128, RegularPath>::from_storage(
                Matrix3X2048I128::default(),
            )),
        ),
        (
            "Count<Matrix3X2048I128, FastPath>",
            Box::new(Count::<Matrix3X2048I128, FastPath>::from_storage(
                Matrix3X2048I128::default(),
            )),
        ),
        (
            "Count<Matrix3X4096I32, RegularPath>",
            Box::new(Count::<Matrix3X4096I32, RegularPath>::from_storage(
                Matrix3X4096I32::default(),
            )),
        ),
        (
            "Count<Matrix3X4096I32, FastPath>",
            Box::new(Count::<Matrix3X4096I32, FastPath>::from_storage(
                Matrix3X4096I32::default(),
            )),
        ),
        (
            "Count<Matrix3X4096I64, RegularPath>",
            Box::new(Count::<Matrix3X4096I64, RegularPath>::from_storage(
                Matrix3X4096I64::default(),
            )),
        ),
        (
            "Count<Matrix3X4096I64, FastPath>",
            Box::new(Count::<Matrix3X4096I64, FastPath>::from_storage(
                Matrix3X4096I64::default(),
            )),
        ),
        (
            "Count<Matrix3X4096I128, RegularPath>",
            Box::new(Count::<Matrix3X4096I128, RegularPath>::from_storage(
                Matrix3X4096I128::default(),
            )),
        ),
        (
            "Count<Matrix3X4096I128, FastPath>",
            Box::new(Count::<Matrix3X4096I128, FastPath>::from_storage(
                Matrix3X4096I128::default(),
            )),
        ),
        (
            "Count<Matrix3X8192I32, RegularPath>",
            Box::new(Count::<Matrix3X8192I32, RegularPath>::from_storage(
                Matrix3X8192I32::default(),
            )),
        ),
        (
            "Count<Matrix3X8192I32, FastPath>",
            Box::new(Count::<Matrix3X8192I32, FastPath>::from_storage(
                Matrix3X8192I32::default(),
            )),
        ),
        (
            "Count<Matrix3X8192I64, RegularPath>",
            Box::new(Count::<Matrix3X8192I64, RegularPath>::from_storage(
                Matrix3X8192I64::default(),
            )),
        ),
        (
            "Count<Matrix3X8192I64, FastPath>",
            Box::new(Count::<Matrix3X8192I64, FastPath>::from_storage(
                Matrix3X8192I64::default(),
            )),
        ),
        (
            "Count<Matrix3X8192I128, RegularPath>",
            Box::new(Count::<Matrix3X8192I128, RegularPath>::from_storage(
                Matrix3X8192I128::default(),
            )),
        ),
        (
            "Count<Matrix3X8192I128, FastPath>",
            Box::new(Count::<Matrix3X8192I128, FastPath>::from_storage(
                Matrix3X8192I128::default(),
            )),
        ),
        (
            "Count<Matrix3X16384I32, RegularPath>",
            Box::new(Count::<Matrix3X16384I32, RegularPath>::from_storage(
                Matrix3X16384I32::default(),
            )),
        ),
        (
            "Count<Matrix3X16384I32, FastPath>",
            Box::new(Count::<Matrix3X16384I32, FastPath>::from_storage(
                Matrix3X16384I32::default(),
            )),
        ),
        (
            "Count<Matrix3X16384I64, RegularPath>",
            Box::new(Count::<Matrix3X16384I64, RegularPath>::from_storage(
                Matrix3X16384I64::default(),
            )),
        ),
        (
            "Count<Matrix3X16384I64, FastPath>",
            Box::new(Count::<Matrix3X16384I64, FastPath>::from_storage(
                Matrix3X16384I64::default(),
            )),
        ),
        (
            "Count<Matrix3X16384I128, RegularPath>",
            Box::new(Count::<Matrix3X16384I128, RegularPath>::from_storage(
                Matrix3X16384I128::default(),
            )),
        ),
        (
            "Count<Matrix3X16384I128, FastPath>",
            Box::new(Count::<Matrix3X16384I128, FastPath>::from_storage(
                Matrix3X16384I128::default(),
            )),
        ),
        (
            "Count<Matrix3X32768I32, RegularPath>",
            Box::new(Count::<Matrix3X32768I32, RegularPath>::from_storage(
                Matrix3X32768I32::default(),
            )),
        ),
        (
            "Count<Matrix3X32768I32, FastPath>",
            Box::new(Count::<Matrix3X32768I32, FastPath>::from_storage(
                Matrix3X32768I32::default(),
            )),
        ),
        (
            "Count<Matrix3X32768I64, RegularPath>",
            Box::new(Count::<Matrix3X32768I64, RegularPath>::from_storage(
                Matrix3X32768I64::default(),
            )),
        ),
        (
            "Count<Matrix3X32768I64, FastPath>",
            Box::new(Count::<Matrix3X32768I64, FastPath>::from_storage(
                Matrix3X32768I64::default(),
            )),
        ),
        (
            "Count<Matrix3X32768I128, RegularPath>",
            Box::new(Count::<Matrix3X32768I128, RegularPath>::from_storage(
                Matrix3X32768I128::default(),
            )),
        ),
        (
            "Count<Matrix3X32768I128, FastPath>",
            Box::new(Count::<Matrix3X32768I128, FastPath>::from_storage(
                Matrix3X32768I128::default(),
            )),
        ),
        (
            "Count<Matrix5X512I32, RegularPath>",
            Box::new(Count::<Matrix5X512I32, RegularPath>::from_storage(
                Matrix5X512I32::default(),
            )),
        ),
        (
            "Count<Matrix5X512I32, FastPath>",
            Box::new(Count::<Matrix5X512I32, FastPath>::from_storage(
                Matrix5X512I32::default(),
            )),
        ),
        (
            "Count<Matrix5X512I64, RegularPath>",
            Box::new(Count::<Matrix5X512I64, RegularPath>::from_storage(
                Matrix5X512I64::default(),
            )),
        ),
        (
            "Count<Matrix5X512I64, FastPath>",
            Box::new(Count::<Matrix5X512I64, FastPath>::from_storage(
                Matrix5X512I64::default(),
            )),
        ),
        (
            "Count<Matrix5X512I128, RegularPath>",
            Box::new(Count::<Matrix5X512I128, RegularPath>::from_storage(
                Matrix5X512I128::default(),
            )),
        ),
        (
            "Count<Matrix5X512I128, FastPath>",
            Box::new(Count::<Matrix5X512I128, FastPath>::from_storage(
                Matrix5X512I128::default(),
            )),
        ),
        (
            "Count<Matrix5X1024I32, RegularPath>",
            Box::new(Count::<Matrix5X1024I32, RegularPath>::from_storage(
                Matrix5X1024I32::default(),
            )),
        ),
        (
            "Count<Matrix5X1024I32, FastPath>",
            Box::new(Count::<Matrix5X1024I32, FastPath>::from_storage(
                Matrix5X1024I32::default(),
            )),
        ),
        (
            "Count<Matrix5X1024I64, RegularPath>",
            Box::new(Count::<Matrix5X1024I64, RegularPath>::from_storage(
                Matrix5X1024I64::default(),
            )),
        ),
        (
            "Count<Matrix5X1024I64, FastPath>",
            Box::new(Count::<Matrix5X1024I64, FastPath>::from_storage(
                Matrix5X1024I64::default(),
            )),
        ),
        (
            "Count<Matrix5X1024I128, RegularPath>",
            Box::new(Count::<Matrix5X1024I128, RegularPath>::from_storage(
                Matrix5X1024I128::default(),
            )),
        ),
        (
            "Count<Matrix5X1024I128, FastPath>",
            Box::new(Count::<Matrix5X1024I128, FastPath>::from_storage(
                Matrix5X1024I128::default(),
            )),
        ),
        (
            "Count<Matrix5X2048I32, RegularPath>",
            Box::new(Count::<Matrix5X2048I32, RegularPath>::from_storage(
                Matrix5X2048I32::default(),
            )),
        ),
        (
            "Count<Matrix5X2048I32, FastPath>",
            Box::new(Count::<Matrix5X2048I32, FastPath>::from_storage(
                Matrix5X2048I32::default(),
            )),
        ),
        (
            "Count<Matrix5X2048I64, RegularPath>",
            Box::new(Count::<Matrix5X2048I64, RegularPath>::from_storage(
                Matrix5X2048I64::default(),
            )),
        ),
        (
            "Count<Matrix5X2048I64, FastPath>",
            Box::new(Count::<Matrix5X2048I64, FastPath>::from_storage(
                Matrix5X2048I64::default(),
            )),
        ),
        (
            "Count<Matrix5X2048I128, RegularPath>",
            Box::new(Count::<Matrix5X2048I128, RegularPath>::from_storage(
                Matrix5X2048I128::default(),
            )),
        ),
        (
            "Count<Matrix5X2048I128, FastPath>",
            Box::new(Count::<Matrix5X2048I128, FastPath>::from_storage(
                Matrix5X2048I128::default(),
            )),
        ),
        (
            "Count<Matrix5X4096I32, RegularPath>",
            Box::new(Count::<Matrix5X4096I32, RegularPath>::from_storage(
                Matrix5X4096I32::default(),
            )),
        ),
        (
            "Count<Matrix5X4096I32, FastPath>",
            Box::new(Count::<Matrix5X4096I32, FastPath>::from_storage(
                Matrix5X4096I32::default(),
            )),
        ),
        (
            "Count<Matrix5X4096I64, RegularPath>",
            Box::new(Count::<Matrix5X4096I64, RegularPath>::from_storage(
                Matrix5X4096I64::default(),
            )),
        ),
        (
            "Count<Matrix5X4096I64, FastPath>",
            Box::new(Count::<Matrix5X4096I64, FastPath>::from_storage(
                Matrix5X4096I64::default(),
            )),
        ),
        (
            "Count<Matrix5X4096I128, RegularPath>",
            Box::new(Count::<Matrix5X4096I128, RegularPath>::from_storage(
                Matrix5X4096I128::default(),
            )),
        ),
        (
            "Count<Matrix5X4096I128, FastPath>",
            Box::new(Count::<Matrix5X4096I128, FastPath>::from_storage(
                Matrix5X4096I128::default(),
            )),
        ),
        (
            "Count<Matrix5X8192I32, RegularPath>",
            Box::new(Count::<Matrix5X8192I32, RegularPath>::from_storage(
                Matrix5X8192I32::default(),
            )),
        ),
        (
            "Count<Matrix5X8192I32, FastPath>",
            Box::new(Count::<Matrix5X8192I32, FastPath>::from_storage(
                Matrix5X8192I32::default(),
            )),
        ),
        (
            "Count<Matrix5X8192I64, RegularPath>",
            Box::new(Count::<Matrix5X8192I64, RegularPath>::from_storage(
                Matrix5X8192I64::default(),
            )),
        ),
        (
            "Count<Matrix5X8192I64, FastPath>",
            Box::new(Count::<Matrix5X8192I64, FastPath>::from_storage(
                Matrix5X8192I64::default(),
            )),
        ),
        (
            "Count<Matrix5X8192I128, RegularPath>",
            Box::new(Count::<Matrix5X8192I128, RegularPath>::from_storage(
                Matrix5X8192I128::default(),
            )),
        ),
        (
            "Count<Matrix5X8192I128, FastPath>",
            Box::new(Count::<Matrix5X8192I128, FastPath>::from_storage(
                Matrix5X8192I128::default(),
            )),
        ),
        (
            "Count<Matrix5X16384I32, RegularPath>",
            Box::new(Count::<Matrix5X16384I32, RegularPath>::from_storage(
                Matrix5X16384I32::default(),
            )),
        ),
        (
            "Count<Matrix5X16384I32, FastPath>",
            Box::new(Count::<Matrix5X16384I32, FastPath>::from_storage(
                Matrix5X16384I32::default(),
            )),
        ),
        (
            "Count<Matrix5X16384I64, RegularPath>",
            Box::new(Count::<Matrix5X16384I64, RegularPath>::from_storage(
                Matrix5X16384I64::default(),
            )),
        ),
        (
            "Count<Matrix5X16384I64, FastPath>",
            Box::new(Count::<Matrix5X16384I64, FastPath>::from_storage(
                Matrix5X16384I64::default(),
            )),
        ),
        (
            "Count<Matrix5X16384I128, RegularPath>",
            Box::new(Count::<Matrix5X16384I128, RegularPath>::from_storage(
                Matrix5X16384I128::default(),
            )),
        ),
        (
            "Count<Matrix5X16384I128, FastPath>",
            Box::new(Count::<Matrix5X16384I128, FastPath>::from_storage(
                Matrix5X16384I128::default(),
            )),
        ),
        (
            "Count<Matrix5X32768I32, RegularPath>",
            Box::new(Count::<Matrix5X32768I32, RegularPath>::from_storage(
                Matrix5X32768I32::default(),
            )),
        ),
        (
            "Count<Matrix5X32768I32, FastPath>",
            Box::new(Count::<Matrix5X32768I32, FastPath>::from_storage(
                Matrix5X32768I32::default(),
            )),
        ),
        (
            "Count<Matrix5X32768I64, RegularPath>",
            Box::new(Count::<Matrix5X32768I64, RegularPath>::from_storage(
                Matrix5X32768I64::default(),
            )),
        ),
        (
            "Count<Matrix5X32768I64, FastPath>",
            Box::new(Count::<Matrix5X32768I64, FastPath>::from_storage(
                Matrix5X32768I64::default(),
            )),
        ),
        (
            "Count<Matrix5X32768I128, RegularPath>",
            Box::new(Count::<Matrix5X32768I128, RegularPath>::from_storage(
                Matrix5X32768I128::default(),
            )),
        ),
        (
            "Count<Matrix5X32768I128, FastPath>",
            Box::new(Count::<Matrix5X32768I128, FastPath>::from_storage(
                Matrix5X32768I128::default(),
            )),
        ),
        (
            "Count<Matrix7X512I32, RegularPath>",
            Box::new(Count::<Matrix7X512I32, RegularPath>::from_storage(
                Matrix7X512I32::default(),
            )),
        ),
        (
            "Count<Matrix7X512I32, FastPath>",
            Box::new(Count::<Matrix7X512I32, FastPath>::from_storage(
                Matrix7X512I32::default(),
            )),
        ),
        (
            "Count<Matrix7X512I64, RegularPath>",
            Box::new(Count::<Matrix7X512I64, RegularPath>::from_storage(
                Matrix7X512I64::default(),
            )),
        ),
        (
            "Count<Matrix7X512I64, FastPath>",
            Box::new(Count::<Matrix7X512I64, FastPath>::from_storage(
                Matrix7X512I64::default(),
            )),
        ),
        (
            "Count<Matrix7X512I128, RegularPath>",
            Box::new(Count::<Matrix7X512I128, RegularPath>::from_storage(
                Matrix7X512I128::default(),
            )),
        ),
        (
            "Count<Matrix7X512I128, FastPath>",
            Box::new(Count::<Matrix7X512I128, FastPath>::from_storage(
                Matrix7X512I128::default(),
            )),
        ),
        (
            "Count<Matrix7X1024I32, RegularPath>",
            Box::new(Count::<Matrix7X1024I32, RegularPath>::from_storage(
                Matrix7X1024I32::default(),
            )),
        ),
        (
            "Count<Matrix7X1024I32, FastPath>",
            Box::new(Count::<Matrix7X1024I32, FastPath>::from_storage(
                Matrix7X1024I32::default(),
            )),
        ),
        (
            "Count<Matrix7X1024I64, RegularPath>",
            Box::new(Count::<Matrix7X1024I64, RegularPath>::from_storage(
                Matrix7X1024I64::default(),
            )),
        ),
        (
            "Count<Matrix7X1024I64, FastPath>",
            Box::new(Count::<Matrix7X1024I64, FastPath>::from_storage(
                Matrix7X1024I64::default(),
            )),
        ),
        (
            "Count<Matrix7X1024I128, RegularPath>",
            Box::new(Count::<Matrix7X1024I128, RegularPath>::from_storage(
                Matrix7X1024I128::default(),
            )),
        ),
        (
            "Count<Matrix7X1024I128, FastPath>",
            Box::new(Count::<Matrix7X1024I128, FastPath>::from_storage(
                Matrix7X1024I128::default(),
            )),
        ),
        (
            "Count<Matrix7X2048I32, RegularPath>",
            Box::new(Count::<Matrix7X2048I32, RegularPath>::from_storage(
                Matrix7X2048I32::default(),
            )),
        ),
        (
            "Count<Matrix7X2048I32, FastPath>",
            Box::new(Count::<Matrix7X2048I32, FastPath>::from_storage(
                Matrix7X2048I32::default(),
            )),
        ),
        (
            "Count<Matrix7X2048I64, RegularPath>",
            Box::new(Count::<Matrix7X2048I64, RegularPath>::from_storage(
                Matrix7X2048I64::default(),
            )),
        ),
        (
            "Count<Matrix7X2048I64, FastPath>",
            Box::new(Count::<Matrix7X2048I64, FastPath>::from_storage(
                Matrix7X2048I64::default(),
            )),
        ),
        (
            "Count<Matrix7X2048I128, RegularPath>",
            Box::new(Count::<Matrix7X2048I128, RegularPath>::from_storage(
                Matrix7X2048I128::default(),
            )),
        ),
        (
            "Count<Matrix7X2048I128, FastPath>",
            Box::new(Count::<Matrix7X2048I128, FastPath>::from_storage(
                Matrix7X2048I128::default(),
            )),
        ),
        (
            "Count<Matrix7X4096I32, RegularPath>",
            Box::new(Count::<Matrix7X4096I32, RegularPath>::from_storage(
                Matrix7X4096I32::default(),
            )),
        ),
        (
            "Count<Matrix7X4096I32, FastPath>",
            Box::new(Count::<Matrix7X4096I32, FastPath>::from_storage(
                Matrix7X4096I32::default(),
            )),
        ),
        (
            "Count<Matrix7X4096I64, RegularPath>",
            Box::new(Count::<Matrix7X4096I64, RegularPath>::from_storage(
                Matrix7X4096I64::default(),
            )),
        ),
        (
            "Count<Matrix7X4096I64, FastPath>",
            Box::new(Count::<Matrix7X4096I64, FastPath>::from_storage(
                Matrix7X4096I64::default(),
            )),
        ),
        (
            "Count<Matrix7X4096I128, RegularPath>",
            Box::new(Count::<Matrix7X4096I128, RegularPath>::from_storage(
                Matrix7X4096I128::default(),
            )),
        ),
        (
            "Count<Matrix7X4096I128, FastPath>",
            Box::new(Count::<Matrix7X4096I128, FastPath>::from_storage(
                Matrix7X4096I128::default(),
            )),
        ),
        (
            "Count<Matrix7X8192I32, RegularPath>",
            Box::new(Count::<Matrix7X8192I32, RegularPath>::from_storage(
                Matrix7X8192I32::default(),
            )),
        ),
        (
            "Count<Matrix7X8192I32, FastPath>",
            Box::new(Count::<Matrix7X8192I32, FastPath>::from_storage(
                Matrix7X8192I32::default(),
            )),
        ),
        (
            "Count<Matrix7X8192I64, RegularPath>",
            Box::new(Count::<Matrix7X8192I64, RegularPath>::from_storage(
                Matrix7X8192I64::default(),
            )),
        ),
        (
            "Count<Matrix7X8192I64, FastPath>",
            Box::new(Count::<Matrix7X8192I64, FastPath>::from_storage(
                Matrix7X8192I64::default(),
            )),
        ),
        (
            "Count<Matrix7X8192I128, RegularPath>",
            Box::new(Count::<Matrix7X8192I128, RegularPath>::from_storage(
                Matrix7X8192I128::default(),
            )),
        ),
        (
            "Count<Matrix7X8192I128, FastPath>",
            Box::new(Count::<Matrix7X8192I128, FastPath>::from_storage(
                Matrix7X8192I128::default(),
            )),
        ),
        (
            "Count<Matrix7X16384I32, RegularPath>",
            Box::new(Count::<Matrix7X16384I32, RegularPath>::from_storage(
                Matrix7X16384I32::default(),
            )),
        ),
        (
            "Count<Matrix7X16384I32, FastPath>",
            Box::new(Count::<Matrix7X16384I32, FastPath>::from_storage(
                Matrix7X16384I32::default(),
            )),
        ),
        (
            "Count<Matrix7X16384I64, RegularPath>",
            Box::new(Count::<Matrix7X16384I64, RegularPath>::from_storage(
                Matrix7X16384I64::default(),
            )),
        ),
        (
            "Count<Matrix7X16384I64, FastPath>",
            Box::new(Count::<Matrix7X16384I64, FastPath>::from_storage(
                Matrix7X16384I64::default(),
            )),
        ),
        (
            "Count<Matrix7X16384I128, RegularPath>",
            Box::new(Count::<Matrix7X16384I128, RegularPath>::from_storage(
                Matrix7X16384I128::default(),
            )),
        ),
        (
            "Count<Matrix7X16384I128, FastPath>",
            Box::new(Count::<Matrix7X16384I128, FastPath>::from_storage(
                Matrix7X16384I128::default(),
            )),
        ),
        (
            "Count<Matrix7X32768I32, RegularPath>",
            Box::new(Count::<Matrix7X32768I32, RegularPath>::from_storage(
                Matrix7X32768I32::default(),
            )),
        ),
        (
            "Count<Matrix7X32768I32, FastPath>",
            Box::new(Count::<Matrix7X32768I32, FastPath>::from_storage(
                Matrix7X32768I32::default(),
            )),
        ),
        (
            "Count<Matrix7X32768I64, RegularPath>",
            Box::new(Count::<Matrix7X32768I64, RegularPath>::from_storage(
                Matrix7X32768I64::default(),
            )),
        ),
        (
            "Count<Matrix7X32768I64, FastPath>",
            Box::new(Count::<Matrix7X32768I64, FastPath>::from_storage(
                Matrix7X32768I64::default(),
            )),
        ),
        (
            "Count<Matrix7X32768I128, RegularPath>",
            Box::new(Count::<Matrix7X32768I128, RegularPath>::from_storage(
                Matrix7X32768I128::default(),
            )),
        ),
        (
            "Count<Matrix7X32768I128, FastPath>",
            Box::new(Count::<Matrix7X32768I128, FastPath>::from_storage(
                Matrix7X32768I128::default(),
            )),
        ),
    ]
}

pub fn countminsketch_topk_variants() -> VariantList {
    vec![
        (
            "CMSHeap<Vector2D<i32>, RegularPath>",
            Box::new(CMSHeap::<Vector2D<i32>, RegularPath>::default()),
        ),
        (
            "CMSHeap<Vector2D<i32>, FastPath>",
            Box::new(CMSHeap::<Vector2D<i32>, FastPath>::default()),
        ),
        (
            "CMSHeap<Vector2D<i64>, RegularPath>",
            Box::new(CMSHeap::<Vector2D<i64>, RegularPath>::default()),
        ),
        (
            "CMSHeap<Vector2D<i64>, FastPath>",
            Box::new(CMSHeap::<Vector2D<i64>, FastPath>::default()),
        ),
        (
            "CMSHeap<FixedMatrix, RegularPath>",
            Box::new(CMSHeap::<FixedMatrix, RegularPath>::default()),
        ),
        (
            "CMSHeap<FixedMatrix, FastPath>",
            Box::new(CMSHeap::<FixedMatrix, FastPath>::default()),
        ),
        (
            "CMSHeap<QuickMatrixI64, RegularPath>",
            Box::new(CMSHeap::<QuickMatrixI64, RegularPath>::default()),
        ),
        (
            "CMSHeap<QuickMatrixI64, FastPath>",
            Box::new(CMSHeap::<QuickMatrixI64, FastPath>::default()),
        ),
    ]
}

pub fn countsketch_topk_variants() -> VariantList {
    vec![
        (
            "CSHeap<Vector2D<i32>, RegularPath>",
            Box::new(CSHeap::<Vector2D<i32>, RegularPath>::default()),
        ),
        (
            "CSHeap<Vector2D<i32>, FastPath>",
            Box::new(CSHeap::<Vector2D<i32>, FastPath>::default()),
        ),
        (
            "CSHeap<Vector2D<i64>, RegularPath>",
            Box::new(CSHeap::<Vector2D<i64>, RegularPath>::default()),
        ),
        (
            "CSHeap<Vector2D<i64>, FastPath>",
            Box::new(CSHeap::<Vector2D<i64>, FastPath>::default()),
        ),
        (
            "CSHeap<Vector2D<i128>, RegularPath>",
            Box::new(CSHeap::<Vector2D<i128>, RegularPath>::new(3, 4096, 32)),
        ),
        (
            "CSHeap<Vector2D<i128>, FastPath>",
            Box::new(CSHeap::<Vector2D<i128>, FastPath>::new(3, 4096, 32)),
        ),
        (
            "CSHeap<FixedMatrix, RegularPath>",
            Box::new(CSHeap::<FixedMatrix, RegularPath>::default()),
        ),
        (
            "CSHeap<FixedMatrix, FastPath>",
            Box::new(CSHeap::<FixedMatrix, FastPath>::default()),
        ),
        (
            "CSHeap<QuickMatrixI64, RegularPath>",
            Box::new(CSHeap::<QuickMatrixI64, RegularPath>::default()),
        ),
        (
            "CSHeap<QuickMatrixI64, FastPath>",
            Box::new(CSHeap::<QuickMatrixI64, FastPath>::default()),
        ),
        (
            "CSHeap<QuickMatrixI128, RegularPath>",
            Box::new(CSHeap::<QuickMatrixI128, RegularPath>::default()),
        ),
        (
            "CSHeap<QuickMatrixI128, FastPath>",
            Box::new(CSHeap::<QuickMatrixI128, FastPath>::default()),
        ),
    ]
}
