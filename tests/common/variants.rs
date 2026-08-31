#![allow(dead_code)]

use super::FreqTruth;
use super::specs::{CountMinSpec, CountSketchSpec};
use super::streams::zipf_u64;

use asap_sketchlib::sketches::countsketch::CountSketchCounter;
use asap_sketchlib::{
    CMSHeap, CSHeap, Count, CountMin, DataInput, DefaultXxHasher, FastPath, FastPathHasher,
    MatrixStorage, RegularPath,
};

const VARIANT_N: usize = 40_000;
const VARIANT_DOMAIN: usize = 4_096;
const VARIANT_SEED: u64 = 0x10BE_C700;

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

pub trait FrequencyVariant {
    fn feed(&mut self, key: u64);
    fn query(&self, key: u64) -> f64;
    fn dims(&self) -> (usize, usize);
}

pub type VariantList = Vec<(&'static str, Box<dyn FrequencyVariant>)>;

impl<S> FrequencyVariant for CountMin<S, RegularPath, DefaultXxHasher>
where
    S: MatrixStorage,
    S::Counter: VariantCounter + Copy + PartialOrd + From<i32> + std::ops::AddAssign,
{
    fn feed(&mut self, key: u64) {
        self.insert(&DataInput::U64(key));
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
    S: MatrixStorage + FastPathHasher<DefaultXxHasher>,
    S::Counter: VariantCounter + Copy + PartialOrd + From<i32> + std::ops::AddAssign,
{
    fn feed(&mut self, key: u64) {
        self.insert(&DataInput::U64(key));
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
    S: MatrixStorage,
    S::Counter: CountSketchCounter,
{
    fn feed(&mut self, key: u64) {
        self.insert(&DataInput::U64(key));
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
    S: MatrixStorage + FastPathHasher<DefaultXxHasher>,
    S::Counter: CountSketchCounter,
{
    fn feed(&mut self, key: u64) {
        self.insert(&DataInput::U64(key));
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
    S: MatrixStorage,
    S::Counter: VariantCounter + Copy + Ord + From<i32> + Into<i64> + std::ops::AddAssign,
{
    fn feed(&mut self, key: u64) {
        self.insert(&DataInput::U64(key));
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
    S: MatrixStorage + FastPathHasher<DefaultXxHasher>,
    S::Counter: VariantCounter + Copy + Ord + From<i32> + Into<i64> + std::ops::AddAssign,
{
    fn feed(&mut self, key: u64) {
        self.insert(&DataInput::U64(key));
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
    S: MatrixStorage,
    S::Counter: CountSketchCounter,
{
    fn feed(&mut self, key: u64) {
        self.insert(&DataInput::U64(key));
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
    S: MatrixStorage + FastPathHasher<DefaultXxHasher>,
    S::Counter: CountSketchCounter,
{
    fn feed(&mut self, key: u64) {
        self.insert(&DataInput::U64(key));
    }
    fn query(&self, key: u64) -> f64 {
        self.estimate(&DataInput::U64(key))
    }
    fn dims(&self) -> (usize, usize) {
        (self.rows(), self.cols())
    }
}

pub fn variant_stream() -> (Vec<u64>, FreqTruth) {
    let stream = zipf_u64(VARIANT_N, VARIANT_DOMAIN, 1.1, VARIANT_SEED);
    let mut truth = FreqTruth::default();
    for k in &stream {
        truth.observe(*k as i64);
    }
    (stream, truth)
}

fn variant_context(label: &str, rows: usize, cols: usize) -> String {
    format!(
        "{label} rows={rows} cols={cols} zipf(1.1) domain={VARIANT_DOMAIN} n={VARIANT_N} \
         seed={VARIANT_SEED:#x}"
    )
}

pub fn assert_count_min_bound(variants: VariantList) {
    let (stream, truth) = variant_stream();
    for (label, mut sketch) in variants {
        for k in &stream {
            sketch.feed(*k);
        }
        let (rows, cols) = sketch.dims();
        let ctx = variant_context(label, rows, cols);
        CountMinSpec::new(rows, cols).assert_contract(
            label,
            &truth,
            |k| sketch.query(k as u64),
            &ctx,
        );
    }
}

pub fn assert_l2_bound(variants: VariantList) {
    let (stream, truth) = variant_stream();
    for (label, mut sketch) in variants {
        for k in &stream {
            sketch.feed(*k);
        }
        let (rows, cols) = sketch.dims();
        let ctx = variant_context(label, rows, cols);
        CountSketchSpec::new(rows, cols).assert_contract(
            label,
            &truth,
            |k| sketch.query(k as u64),
            &ctx,
        );
    }
}
