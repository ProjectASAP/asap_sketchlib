use asap_sketchlib::{
    CMSHeap, CountMin, DataInput, DefaultXxHasher, FastPath, FastPathHasher, FixedMatrix,
    MatrixStorage, QuickMatrixI64, RegularPath, Vector2D,
};

use super::*;

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
