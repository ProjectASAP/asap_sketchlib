//! Sketch family enums and shared adapter traits.
//! Groups sketches by capability and provides erased fast-path interfaces used by the framework.

use crate::common::structure_utils::ToF64;
use crate::sketches::count::CountSketchCounter;
use crate::{
    Coco, Count, CountMin, DDSketch, DataFusion, FastPath, HyperLogLog, HyperLogLogHIP, KLL,
    MatrixHashType, Regular, RegularPath, SketchHasher, SketchInput, UnivMon,
    hydra::MultiHeadHydra, sketch_framework::Hydra,
};
use std::ops::AddAssign;

#[derive(Clone, Copy, Debug)]
pub enum UnivMonQuery {
    Cardinality,
    L1Norm,
    L2Norm,
    Entropy,
}

// Shared hash wrapper for the broader sketch catalog.
// Some sketches consume matrix fast-path hashes, while others only need a 64-bit hash.
#[derive(Clone, Debug)]
pub enum HashValue {
    Matrix(MatrixHashType),
    Fast64(u64),
}

impl From<MatrixHashType> for HashValue {
    fn from(value: MatrixHashType) -> Self {
        HashValue::Matrix(value)
    }
}

impl From<u64> for HashValue {
    fn from(value: u64) -> Self {
        HashValue::Fast64(value)
    }
}

pub trait CountMinRegularOps {
    fn insert(&mut self, val: &SketchInput);
    fn estimate_f64(&self, val: &SketchInput) -> f64;
}

pub trait CountMinFastOps {
    fn insert(&mut self, val: &SketchInput);
    fn estimate_f64(&self, val: &SketchInput) -> f64;
    fn rows(&self) -> usize;
    fn cols(&self) -> usize;
    fn fast_insert(&mut self, hash: &MatrixHashType);
    fn fast_estimate(&self, hash: &MatrixHashType) -> f64;
}

pub trait CountRegularOps {
    fn insert(&mut self, val: &SketchInput);
    fn estimate_f64(&self, val: &SketchInput) -> f64;
}

pub trait CountFastOps {
    fn insert(&mut self, val: &SketchInput);
    fn estimate_f64(&self, val: &SketchInput) -> f64;
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
    fn insert(&mut self, val: &SketchInput) {
        self.insert(val);
    }

    fn estimate_f64(&self, val: &SketchInput) -> f64 {
        self.estimate(val).to_f64()
    }
}

impl<S, H> CountMinFastOps for CountMin<S, FastPath, H>
where
    H: SketchHasher<HashType = MatrixHashType> + 'static,
    S: crate::MatrixStorage + crate::FastPathHasher<H> + 'static,
    S::Counter: Copy + PartialOrd + From<i32> + AddAssign + ToF64 + 'static,
{
    fn insert(&mut self, val: &SketchInput) {
        self.insert(val);
    }

    fn estimate_f64(&self, val: &SketchInput) -> f64 {
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
    fn insert(&mut self, val: &SketchInput) {
        self.insert(val);
    }

    fn estimate_f64(&self, val: &SketchInput) -> f64 {
        self.estimate(val)
    }
}

impl<S, H> CountFastOps for Count<S, FastPath, H>
where
    H: SketchHasher<HashType = MatrixHashType> + 'static,
    S: crate::MatrixStorage + crate::FastPathHasher<H> + 'static,
    S::Counter: CountSketchCounter + 'static,
{
    fn insert(&mut self, val: &SketchInput) {
        self.insert(val);
    }

    fn estimate_f64(&self, val: &SketchInput) -> f64 {
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

pub enum FreqSketch {
    CountMinFast(Box<dyn CountMinFastOps>),
    CountMinRegular(Box<dyn CountMinRegularOps>),
    CountFast(Box<dyn CountFastOps>),
    CountRegular(Box<dyn CountRegularOps>),
}

impl<S, H> From<CountMin<S, FastPath, H>> for FreqSketch
where
    H: SketchHasher<HashType = MatrixHashType> + 'static,
    S: crate::MatrixStorage + crate::FastPathHasher<H> + 'static,
    S::Counter: Copy + PartialOrd + From<i32> + AddAssign + ToF64 + 'static,
{
    fn from(value: CountMin<S, FastPath, H>) -> Self {
        FreqSketch::CountMinFast(Box::new(value))
    }
}

impl<S> From<CountMin<S, RegularPath>> for FreqSketch
where
    S: crate::MatrixStorage + 'static,
    S::Counter: Copy + PartialOrd + From<i32> + AddAssign + ToF64 + 'static,
{
    fn from(value: CountMin<S, RegularPath>) -> Self {
        FreqSketch::CountMinRegular(Box::new(value))
    }
}

impl<S, H> From<Count<S, FastPath, H>> for FreqSketch
where
    H: SketchHasher<HashType = MatrixHashType> + 'static,
    S: crate::MatrixStorage + crate::FastPathHasher<H> + 'static,
    S::Counter: CountSketchCounter + 'static,
{
    fn from(value: Count<S, FastPath, H>) -> Self {
        FreqSketch::CountFast(Box::new(value))
    }
}

impl<S> From<Count<S, RegularPath>> for FreqSketch
where
    S: crate::MatrixStorage + 'static,
    S::Counter: CountSketchCounter + 'static,
{
    fn from(value: Count<S, RegularPath>) -> Self {
        FreqSketch::CountRegular(Box::new(value))
    }
}

pub enum CardinalitySketch {
    HllDf(HyperLogLog<DataFusion>),
    HllRegular(HyperLogLog<Regular>),
    HllHip(HyperLogLogHIP),
}

pub enum QuantileSketch {
    Kll(KLL),
    Dd(DDSketch),
}

pub enum SubpopulationSketch {
    Hydra(Hydra),
    MultiHydra(MultiHeadHydra),
}

pub enum SubquerySketch {
    Coco(Coco),
}

pub enum GSumSketch {
    UnivMon(UnivMon),
}

impl FreqSketch {
    pub fn sketch_type(&self) -> &'static str {
        match self {
            FreqSketch::CountMinFast(_) | FreqSketch::CountMinRegular(_) => "CountMin",
            FreqSketch::CountFast(_) | FreqSketch::CountRegular(_) => "Count",
        }
    }

    pub fn insert(&mut self, val: &SketchInput) {
        match self {
            FreqSketch::CountMinRegular(sketch) => sketch.insert(val),
            FreqSketch::CountMinFast(sketch) => sketch.insert(val),
            FreqSketch::CountRegular(sketch) => sketch.insert(val),
            FreqSketch::CountFast(sketch) => sketch.insert(val),
        }
    }

    pub fn query(&self, val: &SketchInput) -> Result<f64, &'static str> {
        match self {
            FreqSketch::CountMinFast(sketch) => Ok(sketch.estimate_f64(val)),
            FreqSketch::CountMinRegular(sketch) => Ok(sketch.estimate_f64(val)),
            FreqSketch::CountFast(sketch) => Ok(sketch.estimate_f64(val)),
            FreqSketch::CountRegular(sketch) => Ok(sketch.estimate_f64(val)),
        }
    }

    pub fn query_with_hash_value(&self, hash: &HashValue) -> Result<f64, &'static str> {
        match (self, hash) {
            (FreqSketch::CountMinFast(sketch), HashValue::Matrix(h)) => Ok(sketch.fast_estimate(h)),
            (FreqSketch::CountFast(sketch), HashValue::Matrix(h)) => Ok(sketch.fast_estimate(h)),
            _ => Err("Hash value type not supported"),
        }
    }

    pub fn try_insert_with_hash_value(&mut self, hash: &HashValue, _val: &SketchInput) -> bool {
        match (self, hash) {
            (FreqSketch::CountMinFast(sketch), HashValue::Matrix(h)) => {
                sketch.fast_insert(h);
                true
            }
            (FreqSketch::CountFast(sketch), HashValue::Matrix(h)) => {
                sketch.fast_insert(h);
                true
            }
            _ => false,
        }
    }

    pub fn insert_with_hash_only(&mut self, hash: &HashValue) -> Result<(), &'static str> {
        match (self, hash) {
            (FreqSketch::CountMinFast(sketch), HashValue::Matrix(h)) => {
                sketch.fast_insert(h);
                Ok(())
            }
            (FreqSketch::CountFast(sketch), HashValue::Matrix(h)) => {
                sketch.fast_insert(h);
                Ok(())
            }
            _ => Err("Hash value type not supported"),
        }
    }
}

impl CardinalitySketch {
    pub fn sketch_type(&self) -> &'static str {
        "HLL"
    }

    pub fn insert(&mut self, val: &SketchInput) {
        match self {
            CardinalitySketch::HllDf(sketch) => sketch.insert(val),
            CardinalitySketch::HllRegular(sketch) => sketch.insert(val),
            CardinalitySketch::HllHip(sketch) => sketch.insert(val),
        }
    }

    pub fn query(&self, _val: &SketchInput) -> Result<f64, &'static str> {
        match self {
            CardinalitySketch::HllDf(hll_df) => Ok(hll_df.estimate() as f64),
            CardinalitySketch::HllRegular(hll) => Ok(hll.estimate() as f64),
            CardinalitySketch::HllHip(hll) => Ok(hll.estimate() as f64),
        }
    }

    pub fn query_with_hash_value(&self, _hash: &HashValue) -> Result<f64, &'static str> {
        match self {
            CardinalitySketch::HllDf(hll_df) => Ok(hll_df.estimate() as f64),
            CardinalitySketch::HllRegular(hll) => Ok(hll.estimate() as f64),
            CardinalitySketch::HllHip(hll) => Ok(hll.estimate() as f64),
        }
    }

    pub fn try_insert_with_hash_value(&mut self, hash: &HashValue) -> bool {
        match (self, hash) {
            (CardinalitySketch::HllDf(hll_df), HashValue::Fast64(h)) => {
                hll_df.insert_with_hash(*h);
                true
            }
            (CardinalitySketch::HllRegular(hll_df), HashValue::Fast64(h)) => {
                hll_df.insert_with_hash(*h);
                true
            }
            (CardinalitySketch::HllHip(hll_df), HashValue::Fast64(h)) => {
                hll_df.insert_with_hash(*h);
                true
            }
            _ => false,
        }
    }

    pub fn insert_with_hash_only(&mut self, hash: &HashValue) -> Result<(), &'static str> {
        match (self, hash) {
            (CardinalitySketch::HllDf(hll_df), HashValue::Fast64(h)) => {
                hll_df.insert_with_hash(*h);
                Ok(())
            }
            (CardinalitySketch::HllRegular(hll_df), HashValue::Fast64(h)) => {
                hll_df.insert_with_hash(*h);
                Ok(())
            }
            (CardinalitySketch::HllHip(hll_df), HashValue::Fast64(h)) => {
                hll_df.insert_with_hash(*h);
                Ok(())
            }
            _ => Err("Hash value type not supported"),
        }
    }
}

impl QuantileSketch {
    pub fn sketch_type(&self) -> &'static str {
        match self {
            QuantileSketch::Kll(_) => "KLL",
            QuantileSketch::Dd(_) => "DDSketch",
        }
    }

    pub fn insert(&mut self, val: &SketchInput) {
        match self {
            QuantileSketch::Kll(sketch) => {
                let _ = sketch.update(val);
            }
            QuantileSketch::Dd(sketch) => {
                let _ = sketch.add_input(val);
            }
        }
    }

    pub fn query(&self, _val: &SketchInput) -> Result<f64, &'static str> {
        match self {
            QuantileSketch::Kll(_) => Err("KLL requires quantile/CDF queries"),
            QuantileSketch::Dd(_) => Err("DDSketch requires quantile/CDF queries"),
        }
    }

    pub fn query_with_hash_value(&self, _hash: &HashValue) -> Result<f64, &'static str> {
        Err("Hash value type not supported")
    }

    pub fn try_insert_with_hash_value(&mut self, _hash: &HashValue) -> bool {
        false
    }

    pub fn insert_with_hash_only(&mut self, _hash: &HashValue) -> Result<(), &'static str> {
        Err("Hash value type not supported")
    }
}

impl SubpopulationSketch {
    pub fn sketch_type(&self) -> &'static str {
        match self {
            SubpopulationSketch::Hydra(_) => "Hydra",
            SubpopulationSketch::MultiHydra(_) => "MultiHydra",
        }
    }

    pub fn insert(&mut self, _val: &SketchInput) {
        match self {
            SubpopulationSketch::Hydra(_) | SubpopulationSketch::MultiHydra(_) => {}
        }
    }

    pub fn query(&self, _val: &SketchInput) -> Result<f64, &'static str> {
        match self {
            SubpopulationSketch::Hydra(_) | SubpopulationSketch::MultiHydra(_) => {
                Err("Hydra requires HydraQuery")
            }
        }
    }

    pub fn query_with_hash_value(&self, _hash: &HashValue) -> Result<f64, &'static str> {
        Err("Hash value type not supported")
    }

    pub fn try_insert_with_hash_value(&mut self, hash: &HashValue, val: &SketchInput) -> bool {
        let _ = (hash, val);
        false
    }

    pub fn insert_with_hash_only(&mut self, _hash: &HashValue) -> Result<(), &'static str> {
        Err("Hash value type not supported")
    }
}

impl SubquerySketch {
    pub fn sketch_type(&self) -> &'static str {
        match self {
            SubquerySketch::Coco(_) => "Coco",
        }
    }

    pub fn insert(&mut self, val: &SketchInput) {
        match (self, val) {
            (SubquerySketch::Coco(sketch), SketchInput::Str(key)) => sketch.insert(key, 1),
            (SubquerySketch::Coco(sketch), SketchInput::String(key)) => {
                sketch.insert(key.as_str(), 1)
            }
            (SubquerySketch::Coco(sketch), SketchInput::Bytes(bytes)) => {
                if let Ok(key) = std::str::from_utf8(bytes) {
                    sketch.insert(key, 1);
                }
            }
            _ => {}
        }
    }

    pub fn query(&self, _val: &SketchInput) -> Result<f64, &'static str> {
        Err("Subquery requires a subquery-specific query type")
    }

    pub fn query_with_hash_value(&self, _hash: &HashValue) -> Result<f64, &'static str> {
        Err("Hash value type not supported")
    }

    pub fn try_insert_with_hash_value(&mut self, _hash: &HashValue, _val: &SketchInput) -> bool {
        false
    }

    pub fn insert_with_hash_only(&mut self, _hash: &HashValue) -> Result<(), &'static str> {
        Err("Hash value type not supported")
    }
}

impl GSumSketch {
    pub fn sketch_type(&self) -> &'static str {
        "UnivMon"
    }

    pub fn insert(&mut self, val: &SketchInput) {
        match self {
            GSumSketch::UnivMon(sketch) => sketch.insert(val, 1),
        }
    }

    pub fn query(&self, _val: &SketchInput) -> Result<f64, &'static str> {
        Err("UnivMon requires a query type")
    }

    pub fn query_with_hash_value(&self, _hash: &HashValue) -> Result<f64, &'static str> {
        Err("Hash value type not supported")
    }

    pub fn try_insert_with_hash_value(&mut self, _hash: &HashValue, _val: &SketchInput) -> bool {
        false
    }

    pub fn insert_with_hash_only(&mut self, _hash: &HashValue) -> Result<(), &'static str> {
        Err("Hash value type not supported")
    }
}
