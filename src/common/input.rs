use serde::{Deserialize, Serialize};
use std::{
    fmt,
    hash::{Hash, Hasher},
};

use crate::{Count, CountL2HH, CountMin, HllDf, KLL, UnivMon};

/// enum that can be any sketch type
/// Provides a unified interface for different sketch implementations
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AnySketch {
    CountMin(CountMin),
    Count(Count),
    HllDf(HllDf),
}

impl AnySketch {
    /// Insert a value into the sketch
    pub fn insert(&mut self, val: &SketchInput) {
        match self {
            AnySketch::CountMin(sketch) => sketch.insert_cm(val),
            AnySketch::Count(sketch) => sketch.fast_insert(val),
            AnySketch::HllDf(sketch) => sketch.insert(val),
        }
    }

    /// Insert a value into the sketch
    pub fn insert_with_hash(&mut self, hashed_val: u128) {
        match self {
            AnySketch::CountMin(sketch) => sketch.fast_insert_with_hash_value(hashed_val),
            AnySketch::Count(sketch) => sketch.fast_insert_with_hash_value(hashed_val),
            AnySketch::HllDf(sketch) => sketch.insert_with_hash(hashed_val as u64),
        }
    }

    /// Merge another sketch of the same type into this one
    pub fn merge(&mut self, other: &AnySketch) -> Result<(), &'static str> {
        match (self, other) {
            (AnySketch::CountMin(s), AnySketch::CountMin(o)) => {
                s.merge(o);
                Ok(())
            }
            (AnySketch::Count(s), AnySketch::Count(o)) => {
                s.merge(o);
                Ok(())
            }
            (AnySketch::HllDf(s), AnySketch::HllDf(o)) => {
                s.merge(o);
                Ok(())
            }
            _ => Err("Cannot merge sketches of different types"),
        }
    }

    /// Query the sketch for an estimate
    pub fn query(&self, key: &SketchInput) -> Result<f64, &'static str> {
        match self {
            AnySketch::CountMin(cm) => Ok(cm.get_est(key) as f64),
            AnySketch::Count(cs) => Ok(cs.fast_estimate(key)),
            AnySketch::HllDf(hll_df) => Ok(hll_df.get_est() as f64),
        }
    }

    /// Query using a pre-computed hash value
    /// Note: For HllDf (cardinality sketch), the hash_value is ignored and total cardinality is returned
    pub fn query_with_hash(&self, hash_value: u128) -> Result<f64, &'static str> {
        match self {
            AnySketch::CountMin(cm) => Ok(cm.fast_estimate_with_hash(hash_value) as f64),
            AnySketch::Count(cs) => Ok(cs.fast_estimate_with_hash(hash_value)),
            AnySketch::HllDf(hll_df) => Ok(hll_df.get_est() as f64),
        }
    }

    /// Get the type of sketch as a string
    pub fn sketch_type(&self) -> &'static str {
        match self {
            AnySketch::CountMin(_) => "CountMin",
            AnySketch::Count(_) => "Count",
            AnySketch::HllDf(_) => "HllDf",
        }
    }
}

/// enum to wrap input for sketch
/// mainly supports primitive type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SketchInput<'a> {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    ISIZE(isize),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    USIZE(usize),
    F32(f32),
    F64(f64),
    Str(&'a str),
    String(String),
    Bytes(&'a [u8]),
}

/// enum to wrap items heap can hold
/// mainly supports primitive type
/// borrowed type is not suitable here
/// user may insert some value to the heap and get rid of the value
/// however, the heap may live longer than user holding the value
/// in other words, sketch input can borrow the value
/// but when it comes to heap, the value should be owned, not borrowed
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, PartialOrd)]
pub enum HeapItem {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    ISIZE(isize),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    USIZE(usize),
    F32(f32),
    F64(f64),
    String(String),
}

pub fn input_to_owned<'a>(input: &SketchInput<'a>) -> HeapItem {
    match input {
        SketchInput::I8(i) => HeapItem::I8(*i),
        SketchInput::I16(i) => HeapItem::I16(*i),
        SketchInput::I32(i) => HeapItem::I32(*i),
        SketchInput::I64(i) => HeapItem::I64(*i),
        SketchInput::I128(i) => HeapItem::I128(*i),
        SketchInput::ISIZE(i) => HeapItem::ISIZE(*i),
        SketchInput::U8(u) => HeapItem::U8(*u),
        SketchInput::U16(u) => HeapItem::U16(*u),
        SketchInput::U32(u) => HeapItem::U32(*u),
        SketchInput::U64(u) => HeapItem::U64(*u),
        SketchInput::U128(u) => HeapItem::U128(*u),
        SketchInput::USIZE(u) => HeapItem::USIZE(*u),
        SketchInput::F32(f) => HeapItem::F32(*f),
        SketchInput::F64(f) => HeapItem::F64(*f),
        SketchInput::Str(s) => HeapItem::String((*s).to_owned()),
        SketchInput::String(s) => HeapItem::String((*s).to_owned()),
        SketchInput::Bytes(items) => {
            let byte_array = (*items).to_owned();
            let s = String::from_utf8(byte_array).unwrap();
            HeapItem::String(s)
        }
    }
}

impl<'a> PartialEq for SketchInput<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::I8(l0), Self::I8(r0)) => l0 == r0,
            (Self::I16(l0), Self::I16(r0)) => l0 == r0,
            (Self::I32(l0), Self::I32(r0)) => l0 == r0,
            (Self::I64(l0), Self::I64(r0)) => l0 == r0,
            (Self::I128(l0), Self::I128(r0)) => l0 == r0,
            (Self::ISIZE(l0), Self::ISIZE(r0)) => l0 == r0,
            (Self::U8(l0), Self::U8(r0)) => l0 == r0,
            (Self::U16(l0), Self::U16(r0)) => l0 == r0,
            (Self::U32(l0), Self::U32(r0)) => l0 == r0,
            (Self::U64(l0), Self::U64(r0)) => l0 == r0,
            (Self::U128(l0), Self::U128(r0)) => l0 == r0,
            (Self::USIZE(l0), Self::USIZE(r0)) => l0 == r0,
            (Self::F32(l0), Self::F32(r0)) => l0 == r0,
            (Self::F64(l0), Self::F64(r0)) => l0 == r0,
            (Self::Str(l0), Self::Str(r0)) => l0 == r0,
            (Self::String(l0), Self::String(r0)) => l0 == r0,
            (Self::Bytes(l0), Self::Bytes(r0)) => l0 == r0,
            _ => false,
        }
    }
}

impl<'a> Eq for SketchInput<'a> {}

impl Hash for SketchInput<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            SketchInput::I8(v) => v.hash(state),
            SketchInput::I16(v) => v.hash(state),
            SketchInput::I32(v) => v.hash(state),
            SketchInput::I64(v) => v.hash(state),
            SketchInput::I128(v) => v.hash(state),
            SketchInput::ISIZE(v) => v.hash(state),
            SketchInput::U8(v) => v.hash(state),
            SketchInput::U16(v) => v.hash(state),
            SketchInput::U32(v) => v.hash(state),
            SketchInput::U64(v) => v.hash(state),
            SketchInput::U128(v) => v.hash(state),
            SketchInput::USIZE(v) => v.hash(state),
            SketchInput::F32(v) => state.write_u32(v.to_bits()),
            SketchInput::F64(v) => state.write_u64(v.to_bits()),
            SketchInput::Str(s) => s.hash(state),
            SketchInput::String(s) => s.hash(state),
            SketchInput::Bytes(bytes) => {
                let str_repr = std::str::from_utf8(bytes)
                    .expect("HeapItem only supports UTF-8 bytes for hashing");
                str_repr.hash(state);
            }
        }
    }
}

impl PartialEq<SketchInput<'_>> for HeapItem {
    fn eq(&self, other: &SketchInput<'_>) -> bool {
        match (self, other) {
            (HeapItem::I8(l), SketchInput::I8(r)) => l == r,
            (HeapItem::I16(l), SketchInput::I16(r)) => l == r,
            (HeapItem::I32(l), SketchInput::I32(r)) => l == r,
            (HeapItem::I64(l), SketchInput::I64(r)) => l == r,
            (HeapItem::I128(l), SketchInput::I128(r)) => l == r,
            (HeapItem::ISIZE(l), SketchInput::ISIZE(r)) => l == r,
            (HeapItem::U8(l), SketchInput::U8(r)) => l == r,
            (HeapItem::U16(l), SketchInput::U16(r)) => l == r,
            (HeapItem::U32(l), SketchInput::U32(r)) => l == r,
            (HeapItem::U64(l), SketchInput::U64(r)) => l == r,
            (HeapItem::U128(l), SketchInput::U128(r)) => l == r,
            (HeapItem::USIZE(l), SketchInput::USIZE(r)) => l == r,
            (HeapItem::F32(l), SketchInput::F32(r)) => l == r,
            (HeapItem::F64(l), SketchInput::F64(r)) => l == r,
            (HeapItem::String(l), SketchInput::Str(r)) => l == r,
            (HeapItem::String(l), SketchInput::String(r)) => l == r,
            (HeapItem::String(l), SketchInput::Bytes(bytes)) => {
                std::str::from_utf8(bytes).map(|s| l == s).unwrap_or(false)
            }
            _ => false,
        }
    }
}

impl PartialEq<&SketchInput<'_>> for HeapItem {
    fn eq(&self, other: &&SketchInput<'_>) -> bool {
        self == *other
    }
}

impl<'a> PartialEq<HeapItem> for SketchInput<'a> {
    fn eq(&self, other: &HeapItem) -> bool {
        other == self
    }
}

impl<'a> PartialEq<&HeapItem> for SketchInput<'a> {
    fn eq(&self, other: &&HeapItem) -> bool {
        self == *other
    }
}

impl Eq for HeapItem {}

impl Hash for HeapItem {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            HeapItem::I8(val) => val.hash(state),
            HeapItem::I16(val) => val.hash(state),
            HeapItem::I32(val) => val.hash(state),
            HeapItem::I64(val) => val.hash(state),
            HeapItem::I128(val) => val.hash(state),
            HeapItem::ISIZE(val) => val.hash(state),
            HeapItem::U8(val) => val.hash(state),
            HeapItem::U16(val) => val.hash(state),
            HeapItem::U32(val) => val.hash(state),
            HeapItem::U64(val) => val.hash(state),
            HeapItem::U128(val) => val.hash(state),
            HeapItem::USIZE(val) => val.hash(state),
            HeapItem::F32(val) => state.write_u32(val.to_bits()),
            HeapItem::F64(val) => state.write_u64(val.to_bits()),
            HeapItem::String(val) => val.hash(state),
        }
    }
}

/// enum that can be used by UnivMon
/// using CountL2HH as State-Of-Art example
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum L2HH {
    COUNT(CountL2HH),
}

impl L2HH {
    pub fn update_and_est(&mut self, key: &SketchInput, value: i64) -> f64 {
        match self {
            L2HH::COUNT(count_l2hh) => count_l2hh.fast_update_and_est(key, value),
        }
    }

    pub fn update_and_est_without_l2(&mut self, key: &SketchInput, value: i64) -> f64 {
        match self {
            L2HH::COUNT(count_l2hh) => count_l2hh.fast_update_and_est_without_l2(key, value),
        }
    }

    pub fn get_l2(&self) -> f64 {
        match self {
            L2HH::COUNT(count_l2hh) => count_l2hh.get_l2(),
        }
    }

    pub fn merge(&mut self, other: &L2HH) {
        match (self, other) {
            (L2HH::COUNT(self_count), L2HH::COUNT(other_count)) => {
                self_count.merge(other_count);
            }
        }
    }
}

/// Query type for Hydra sketches
/// Different sketches support different query semantics
#[derive(Clone, Debug)]
pub enum HydraQuery<'a> {
    /// Query for frequency of a specific item (for CountMin, Count, etc.)
    Frequency(SketchInput<'a>),
    /// Query for quantile/CDF at a threshold (for KLL, DDSketch, etc.)
    Quantile(f64),
    /// Query cumulative distribution up to a threshold value
    Cdf(f64),
    /// Query for cardinality (for HyperLogLog, etc.)
    Cardinality,
    L1Norm,
    L2Norm,
    Entropy,
    // whether adding rank needs mroe consideration
    // Rank(f64),
}

impl<'a> fmt::Display for HydraQuery<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HydraQuery::Frequency(_) => write!(f, "Frequency Query"),
            HydraQuery::Quantile(_) => write!(f, "Quantile Query"),
            HydraQuery::Cdf(_) => write!(f, "CDF Query"),
            HydraQuery::Cardinality => write!(f, "Cardinality Query"),
            HydraQuery::L1Norm => write!(f, "L1Norm Query"),
            HydraQuery::L2Norm => write!(f, "L2Norm Query"),
            HydraQuery::Entropy => write!(f, "Entropy Query"),
        }
    }
}

/// enum that can be used as counter in Hydra
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HydraCounter {
    CM(CountMin),
    HLL(HllDf),
    CS(Count),
    KLL(KLL),
    UNIVERSAL(UnivMon),
}

impl fmt::Display for HydraCounter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HydraCounter::CM(_) => write!(f, "Count-Min Sketch Counter"),
            HydraCounter::HLL(_) => write!(f, "HyperLogLog Counter"),
            HydraCounter::CS(_) => write!(f, "Count Sketch Counter"),
            HydraCounter::KLL(_) => write!(f, "KLL Counter"),
            HydraCounter::UNIVERSAL(_) => write!(f, "UnivMon Counter"),
        }
    }
}

impl HydraCounter {
    /// Insert a value into the counter sketch
    /// This updates the underlying sketch with the given value
    pub fn insert(&mut self, value: &SketchInput) {
        match self {
            HydraCounter::CM(cm) => cm.fast_insert(value),
            HydraCounter::HLL(hll) => hll.insert(value),
            HydraCounter::CS(count) => count.fast_insert(value),
            HydraCounter::KLL(kll) => kll.update(value).unwrap(),
            HydraCounter::UNIVERSAL(u) => u.insert(value, 1),
        }
    }

    /// Query the counter sketch with the appropriate query type
    /// Returns the estimated statistic as f64
    ///
    /// # Arguments
    /// * `query` - The query type (Frequency, Quantile, Cardinality, etc.)
    ///
    /// # Returns
    /// * `Ok(f64)` - The estimated statistic
    /// * `Err(String)` - Error message if query type is incompatible with sketch type
    ///
    /// # Examples
    /// ```
    /// // For CountMin, only Frequency queries are valid
    /// use sketchlib_rust::input::HydraCounter;
    /// use sketchlib_rust::input::HydraQuery;
    /// use sketchlib_rust::CountMin;
    /// use sketchlib_rust::SketchInput;
    /// let counter = HydraCounter::CM(CountMin::default());
    /// let result = counter.query(&HydraQuery::Frequency(SketchInput::I64(42)));
    ///
    /// // For KLL, only Quantile queries would be valid
    /// let result = counter.query(&HydraQuery::Quantile(0.5)); // median
    /// ```
    pub fn query(&self, query: &HydraQuery) -> Result<f64, String> {
        match (self, query) {
            (HydraCounter::CM(cm), HydraQuery::Frequency(value)) => {
                Ok(cm.fast_estimate(value) as f64)
            }
            (HydraCounter::HLL(hll_df), HydraQuery::Cardinality) => Ok(hll_df.get_est() as f64),
            (HydraCounter::CS(count), HydraQuery::Frequency(value)) => {
                Ok(count.fast_estimate(value) as f64)
            }
            (HydraCounter::KLL(kll), HydraQuery::Quantile(q)) => Ok(kll.quantile(*q)),
            (HydraCounter::KLL(kll), HydraQuery::Cdf(value)) => Ok(kll.cdf().quantile(*value)),
            (HydraCounter::UNIVERSAL(um), HydraQuery::Cardinality) => Ok(um.calc_card()),
            (HydraCounter::UNIVERSAL(um), HydraQuery::L1Norm) => Ok(um.calc_l1()),
            (HydraCounter::UNIVERSAL(um), HydraQuery::L2Norm) => Ok(um.calc_l2()),
            (HydraCounter::UNIVERSAL(um), HydraQuery::Entropy) => Ok(um.calc_entropy()),
            (c, q) => Err(format!(
                "{} does not support {}",
                c.to_string(),
                q.to_string()
            )),
        }
    }

    /// Merge another HydraCounter into this one
    /// Both counters must be of the same type
    pub fn merge(&mut self, other: &HydraCounter) -> Result<(), String> {
        match (self, other) {
            (HydraCounter::CM(self_cm), HydraCounter::CM(other_cm)) => {
                self_cm.merge(other_cm);
                Ok(())
            }
            (HydraCounter::HLL(h1), HydraCounter::HLL(h2)) => {
                h1.merge(h2);
                Ok(())
            }
            (_, _) => Err("Sketch Type in Hydra Counter different, cannot merge".to_string()),
        }
    }
}

/// A key-count pair used in heap-based sketches for tracking heavy hitters.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HHItem {
    pub key: HeapItem,
    pub count: i64,
}

impl HHItem {
    /// Creates a new Item with the given key and count.
    pub fn new(k: SketchInput, count: i64) -> Self {
        HHItem {
            key: input_to_owned(&k),
            count,
        }
    }

    pub fn create_item(k: HeapItem, count: i64) -> Self {
        HHItem { key: k, count }
    }

    /// Legacy constructor for compatibility.
    pub fn init_item(k: SketchInput, count: i64) -> Self {
        HHItem {
            key: input_to_owned(&k),
            count,
        }
    }

    /// Prints the item in a human-readable format.
    pub fn print_item(&self) {
        println!("key: {:?} with count: {}", self.key, self.count);
    }
}

// Implement Ord and PartialOrd to compare by count
impl Ord for HHItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.count.cmp(&other.count)
    }
}

impl PartialOrd for HHItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HHItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.count == other.count
    }
}

impl Eq for HHItem {}
