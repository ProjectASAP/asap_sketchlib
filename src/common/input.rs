//! Shared input, query, and counter types used across sketches.
//! This module defines value wrappers, heap-friendly items, and Hydra/UnivMon helpers.

use crate::message_pack_format::admission_rate;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    hash::{Hash, Hasher},
};

use crate::{
    CountSketch, CountMinSketch, CountSketchWithL2Norm, Ertl, FastPath, HeapCount, HyperLogLog, Kll,
    MatrixHash, UnivMon, Vector2D,
};

/// Input wrapper for sketch APIs (supports primitive and borrowed values).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataInput<'a> {
    /// A signed 8-bit integer.
    I8(i8),
    /// A signed 16-bit integer.
    I16(i16),
    /// A signed 32-bit integer.
    I32(i32),
    /// A signed 64-bit integer.
    I64(i64),
    /// A signed 128-bit integer.
    I128(i128),
    /// A signed pointer-sized integer.
    Isize(isize),
    /// An unsigned 8-bit integer.
    U8(u8),
    /// An unsigned 16-bit integer.
    U16(u16),
    /// An unsigned 32-bit integer.
    U32(u32),
    /// An unsigned 64-bit integer.
    U64(u64),
    /// An unsigned 128-bit integer.
    U128(u128),
    /// An unsigned pointer-sized integer.
    Usize(usize),
    /// A 32-bit floating-point value.
    F32(f32),
    /// A 64-bit floating-point value.
    F64(f64),
    /// A borrowed UTF-8 string slice.
    Str(&'a str),
    /// An owned UTF-8 string.
    String(String),
    /// Borrowed raw bytes.
    Bytes(&'a [u8]),
}

/// Owned counterpart to `DataInput` for heap storage.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, PartialOrd)]
pub enum OwnedDataInput {
    /// A signed 8-bit integer.
    I8(i8),
    /// A signed 16-bit integer.
    I16(i16),
    /// A signed 32-bit integer.
    I32(i32),
    /// A signed 64-bit integer.
    I64(i64),
    /// A signed 128-bit integer.
    I128(i128),
    /// A signed pointer-sized integer.
    Isize(isize),
    /// An unsigned 8-bit integer.
    U8(u8),
    /// An unsigned 16-bit integer.
    U16(u16),
    /// An unsigned 32-bit integer.
    U32(u32),
    /// An unsigned 64-bit integer.
    U64(u64),
    /// An unsigned 128-bit integer.
    U128(u128),
    /// An unsigned pointer-sized integer.
    Usize(usize),
    /// A 32-bit floating-point value.
    F32(f32),
    /// A 64-bit floating-point value.
    F64(f64),
    /// An owned UTF-8 string.
    String(String),
    /// Owned raw bytes.
    Bytes(Vec<u8>),
}

/// Converts a heap-owned key into a borrowed sketch input.
pub fn heap_item_to_sketch_input(item: &OwnedDataInput) -> DataInput<'_> {
    match item {
        OwnedDataInput::I8(v) => DataInput::I8(*v),
        OwnedDataInput::I16(v) => DataInput::I16(*v),
        OwnedDataInput::I32(v) => DataInput::I32(*v),
        OwnedDataInput::I64(v) => DataInput::I64(*v),
        OwnedDataInput::I128(v) => DataInput::I128(*v),
        OwnedDataInput::Isize(v) => DataInput::Isize(*v),
        OwnedDataInput::U8(v) => DataInput::U8(*v),
        OwnedDataInput::U16(v) => DataInput::U16(*v),
        OwnedDataInput::U32(v) => DataInput::U32(*v),
        OwnedDataInput::U64(v) => DataInput::U64(*v),
        OwnedDataInput::U128(v) => DataInput::U128(*v),
        OwnedDataInput::Usize(v) => DataInput::Usize(*v),
        OwnedDataInput::F32(v) => DataInput::F32(*v),
        OwnedDataInput::F64(v) => DataInput::F64(*v),
        OwnedDataInput::String(s) => DataInput::Str(s),
        OwnedDataInput::Bytes(b) => DataInput::Bytes(b.as_slice()),
    }
}

/// Converts a sketch input into an owned heap item.
pub fn input_to_owned<'a>(input: &DataInput<'a>) -> OwnedDataInput {
    match input {
        DataInput::I8(i) => OwnedDataInput::I8(*i),
        DataInput::I16(i) => OwnedDataInput::I16(*i),
        DataInput::I32(i) => OwnedDataInput::I32(*i),
        DataInput::I64(i) => OwnedDataInput::I64(*i),
        DataInput::I128(i) => OwnedDataInput::I128(*i),
        DataInput::Isize(i) => OwnedDataInput::Isize(*i),
        DataInput::U8(u) => OwnedDataInput::U8(*u),
        DataInput::U16(u) => OwnedDataInput::U16(*u),
        DataInput::U32(u) => OwnedDataInput::U32(*u),
        DataInput::U64(u) => OwnedDataInput::U64(*u),
        DataInput::U128(u) => OwnedDataInput::U128(*u),
        DataInput::Usize(u) => OwnedDataInput::Usize(*u),
        DataInput::F32(f) => OwnedDataInput::F32(*f),
        DataInput::F64(f) => OwnedDataInput::F64(*f),
        DataInput::Str(s) => OwnedDataInput::String((*s).to_owned()),
        DataInput::String(s) => OwnedDataInput::String((*s).to_owned()),
        DataInput::Bytes(b) => OwnedDataInput::Bytes((*b).to_vec()),
    }
}

/// Converts DataInput to f64 for numeric-only sketches.
/// Returns an error when the input is not numeric.
#[inline(always)]
pub(crate) fn data_input_to_f64(input: &DataInput) -> Result<f64, &'static str> {
    match input {
        DataInput::I8(v) => Ok(*v as f64),
        DataInput::I16(v) => Ok(*v as f64),
        DataInput::I32(v) => Ok(*v as f64),
        DataInput::I64(v) => Ok(*v as f64),
        DataInput::I128(v) => Ok(*v as f64),
        DataInput::Isize(v) => Ok(*v as f64),
        DataInput::U8(v) => Ok(*v as f64),
        DataInput::U16(v) => Ok(*v as f64),
        DataInput::U32(v) => Ok(*v as f64),
        DataInput::U64(v) => Ok(*v as f64),
        DataInput::U128(v) => Ok(*v as f64),
        DataInput::Usize(v) => Ok(*v as f64),
        DataInput::F32(v) => Ok(*v as f64),
        DataInput::F64(v) => Ok(*v),
        DataInput::Str(_) | DataInput::String(_) | DataInput::Bytes(_) => {
            Err("KLL sketch only accepts numeric inputs")
        }
    }
}

impl<'a> PartialEq for DataInput<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::I8(l0), Self::I8(r0)) => l0 == r0,
            (Self::I16(l0), Self::I16(r0)) => l0 == r0,
            (Self::I32(l0), Self::I32(r0)) => l0 == r0,
            (Self::I64(l0), Self::I64(r0)) => l0 == r0,
            (Self::I128(l0), Self::I128(r0)) => l0 == r0,
            (Self::Isize(l0), Self::Isize(r0)) => l0 == r0,
            (Self::U8(l0), Self::U8(r0)) => l0 == r0,
            (Self::U16(l0), Self::U16(r0)) => l0 == r0,
            (Self::U32(l0), Self::U32(r0)) => l0 == r0,
            (Self::U64(l0), Self::U64(r0)) => l0 == r0,
            (Self::U128(l0), Self::U128(r0)) => l0 == r0,
            (Self::Usize(l0), Self::Usize(r0)) => l0 == r0,
            (Self::F32(l0), Self::F32(r0)) => l0 == r0,
            (Self::F64(l0), Self::F64(r0)) => l0 == r0,
            (Self::Str(l0), Self::Str(r0)) => l0 == r0,
            (Self::String(l0), Self::String(r0)) => l0 == r0,
            (Self::Bytes(l0), Self::Bytes(r0)) => l0 == r0,
            _ => false,
        }
    }
}

impl<'a> Eq for DataInput<'a> {}

impl Hash for DataInput<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            DataInput::I8(v) => v.hash(state),
            DataInput::I16(v) => v.hash(state),
            DataInput::I32(v) => v.hash(state),
            DataInput::I64(v) => v.hash(state),
            DataInput::I128(v) => v.hash(state),
            DataInput::Isize(v) => v.hash(state),
            DataInput::U8(v) => v.hash(state),
            DataInput::U16(v) => v.hash(state),
            DataInput::U32(v) => v.hash(state),
            DataInput::U64(v) => v.hash(state),
            DataInput::U128(v) => v.hash(state),
            DataInput::Usize(v) => v.hash(state),
            DataInput::F32(v) => state.write_u32(v.to_bits()),
            DataInput::F64(v) => state.write_u64(v.to_bits()),
            DataInput::Str(s) => s.hash(state),
            DataInput::String(s) => s.hash(state),
            DataInput::Bytes(bytes) => bytes.hash(state),
        }
    }
}

impl PartialEq<DataInput<'_>> for OwnedDataInput {
    fn eq(&self, other: &DataInput<'_>) -> bool {
        match (self, other) {
            (OwnedDataInput::I8(l), DataInput::I8(r)) => l == r,
            (OwnedDataInput::I16(l), DataInput::I16(r)) => l == r,
            (OwnedDataInput::I32(l), DataInput::I32(r)) => l == r,
            (OwnedDataInput::I64(l), DataInput::I64(r)) => l == r,
            (OwnedDataInput::I128(l), DataInput::I128(r)) => l == r,
            (OwnedDataInput::Isize(l), DataInput::Isize(r)) => l == r,
            (OwnedDataInput::U8(l), DataInput::U8(r)) => l == r,
            (OwnedDataInput::U16(l), DataInput::U16(r)) => l == r,
            (OwnedDataInput::U32(l), DataInput::U32(r)) => l == r,
            (OwnedDataInput::U64(l), DataInput::U64(r)) => l == r,
            (OwnedDataInput::U128(l), DataInput::U128(r)) => l == r,
            (OwnedDataInput::Usize(l), DataInput::Usize(r)) => l == r,
            (OwnedDataInput::F32(l), DataInput::F32(r)) => l == r,
            (OwnedDataInput::F64(l), DataInput::F64(r)) => l == r,
            (OwnedDataInput::String(l), DataInput::Str(r)) => l == r,
            (OwnedDataInput::String(l), DataInput::String(r)) => l == r,
            (OwnedDataInput::Bytes(l), DataInput::Bytes(r)) => l.as_slice() == *r,
            _ => false,
        }
    }
}

impl PartialEq<&DataInput<'_>> for OwnedDataInput {
    fn eq(&self, other: &&DataInput<'_>) -> bool {
        self == *other
    }
}

impl<'a> PartialEq<OwnedDataInput> for DataInput<'a> {
    fn eq(&self, other: &OwnedDataInput) -> bool {
        other == self
    }
}

impl<'a> PartialEq<&OwnedDataInput> for DataInput<'a> {
    fn eq(&self, other: &&OwnedDataInput) -> bool {
        self == *other
    }
}

impl Eq for OwnedDataInput {}

impl Hash for OwnedDataInput {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            OwnedDataInput::I8(val) => val.hash(state),
            OwnedDataInput::I16(val) => val.hash(state),
            OwnedDataInput::I32(val) => val.hash(state),
            OwnedDataInput::I64(val) => val.hash(state),
            OwnedDataInput::I128(val) => val.hash(state),
            OwnedDataInput::Isize(val) => val.hash(state),
            OwnedDataInput::U8(val) => val.hash(state),
            OwnedDataInput::U16(val) => val.hash(state),
            OwnedDataInput::U32(val) => val.hash(state),
            OwnedDataInput::U64(val) => val.hash(state),
            OwnedDataInput::U128(val) => val.hash(state),
            OwnedDataInput::Usize(val) => val.hash(state),
            OwnedDataInput::F32(val) => state.write_u32(val.to_bits()),
            OwnedDataInput::F64(val) => state.write_u64(val.to_bits()),
            OwnedDataInput::String(val) => val.hash(state),
            OwnedDataInput::Bytes(val) => val.hash(state),
        }
    }
}

/// Counter wrapper for UnivMon (currently backed by CountSketchWithL2Norm).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum L2HH {
    /// Count-sketch-based heavy-hitter tracker with L2 support.
    COUNT(CountSketchWithL2Norm),
}

impl L2HH {
    /// Updates the counter and returns the current estimate.
    pub fn update_and_est(&mut self, key: &DataInput, value: i64) -> f64 {
        match self {
            L2HH::COUNT(count_l2hh) => count_l2hh.update_weighted_and_estimate(key, value),
        }
    }

    /// Updates the counter without refreshing the cached L2 value.
    pub fn update_and_est_without_l2(&mut self, key: &DataInput, value: i64) -> f64 {
        match self {
            L2HH::COUNT(count_l2hh) => count_l2hh.fast_update_and_est_without_l2(key, value),
        }
    }

    /// Returns the current L2 estimate.
    pub fn l2(&self) -> f64 {
        match self {
            L2HH::COUNT(count_l2hh) => count_l2hh.l2(),
        }
    }

    /// Returns the current frequency estimate without changing the sketch.
    pub fn estimate(&self, key: &DataInput) -> f64 {
        match self {
            L2HH::COUNT(count_l2hh) => count_l2hh.estimate(key),
        }
    }

    /// Applies a counter delta promoted by an OctoSketch worker.
    pub fn apply_delta(&mut self, delta: crate::octo_delta::CountSketchCellDelta) {
        match self {
            L2HH::COUNT(count_l2hh) => count_l2hh.apply_delta(delta),
        }
    }

    /// Merges another counter of the same kind into this one.
    pub fn merge(&mut self, other: &L2HH) {
        match (self, other) {
            (L2HH::COUNT(self_count), L2HH::COUNT(other_count)) => {
                self_count.merge(other_count);
            }
        }
    }

    /// Resets all counters to zero without reallocating.
    pub fn clear(&mut self) {
        match self {
            L2HH::COUNT(count_l2hh) => count_l2hh.clear(),
        }
    }
}

/// Query type for Hydra sketches.
#[derive(Clone, Debug)]
pub enum HydraQuery<'a> {
    /// Query for frequency of a specific item (for CountMinSketch, Count, etc.)
    Frequency(DataInput<'a>),
    /// Query for quantile/CDF at a threshold (for KLL, DDSketch, etc.)
    Quantile(f64),
    /// Query cumulative distribution up to a threshold value
    Cdf(f64),
    /// Query for cardinality (for HyperLogLog, etc.)
    Cardinality,
    /// Query for the first frequency moment.
    L1Norm,
    /// Query for the second frequency moment.
    L2Norm,
    /// Query for Shannon entropy.
    Entropy,
    // whether adding rank needs more consideration
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

/// Counter variants supported by Hydra.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HydraCounter {
    /// Count-Min-backed counter.
    CountMinSketch(CountMinSketch<Vector2D<i32>, FastPath>),
    /// HyperLogLog-backed counter.
    Hll(HyperLogLog<Ertl>),
    /// Count Sketch-backed counter.
    CountSketch(CountSketch<Vector2D<i32>, FastPath>),
    /// KLL-backed counter.
    Kll(Kll),
    /// UnivMon-backed counter.
    UnivMon(UnivMon),
}

impl fmt::Display for HydraCounter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HydraCounter::CountMinSketch(_) => write!(f, "Count-Min Sketch Counter"),
            HydraCounter::Hll(_) => write!(f, "HyperLogLog Counter"),
            HydraCounter::CountSketch(_) => write!(f, "Count Sketch Counter"),
            HydraCounter::Kll(_) => write!(f, "KLL Counter"),
            HydraCounter::UnivMon(_) => write!(f, "UnivMon Counter"),
        }
    }
}

impl HydraCounter {
    /// Insert a value into the counter sketch
    /// This updates the underlying sketch with the given value
    pub fn update(&mut self, value: &DataInput, weight: Option<i32>) {
        match (self, weight) {
            (HydraCounter::CountMinSketch(cm), None) => cm.update(value),
            (HydraCounter::CountMinSketch(cm), Some(i)) => cm.update_weighted(value, i),
            (HydraCounter::Hll(hll), _) => hll.update(value), // for cardinality, insert once or many times make no difference
            (HydraCounter::CountSketch(weight), None) => weight.update(value),
            (HydraCounter::CountSketch(weight), Some(i)) => weight.update_weighted(value, i),
            (HydraCounter::Kll(kll), None) => kll.update_data_input(value).unwrap(),
            (HydraCounter::Kll(kll), Some(i)) => {
                for _ in 0..i as usize {
                    kll.update_data_input(value).unwrap()
                }
            }
            (HydraCounter::UnivMon(u), None) => u.update_weighted(value, 1),
            (HydraCounter::UnivMon(u), Some(i)) => u.update_weighted(value, i as i64),
        }
    }

    /// Batch insert for `update_batch` paths. For `Kll` this delegates to
    /// the new `update_batch_data_input` (single CDF invalidation); for
    /// other counters it loops over the existing per-item APIs.
    pub fn update_batch(&mut self, values: &[DataInput]) -> Result<(), String> {
        match self {
            HydraCounter::Kll(kll) => kll
                .update_batch_data_input(values)
                .map_err(|e| e.to_string()),
            HydraCounter::CountMinSketch(cm) => {
                cm.update_batch(values);
                Ok(())
            }
            HydraCounter::Hll(hll) => {
                for v in values {
                    hll.update(v);
                }
                Ok(())
            }
            HydraCounter::CountSketch(cs) => {
                for v in values {
                    cs.update(v);
                }
                Ok(())
            }
            HydraCounter::UnivMon(u) => {
                for v in values {
                    u.update_weighted(v, 1);
                }
                Ok(())
            }
        }
    }

    /// Insert a value using a pre-computed hash when supported.
    /// For sketches that require full values (e.g., KLL, UnivMon), this falls back to `update`.
    pub fn update_with_hash(
        &mut self,
        value: &DataInput,
        hashed_val: &MatrixHash,
        weight: Option<i32>,
    ) {
        match (self, weight) {
            (HydraCounter::CountMinSketch(cm), None) => cm.update_with_hash(hashed_val),
            (HydraCounter::CountMinSketch(cm), Some(i)) => {
                cm.update_weighted_with_hash(hashed_val, i)
            }
            (HydraCounter::Hll(hll), _) => hll.update(value),
            (HydraCounter::CountSketch(count), None) => count.update_with_hash(hashed_val),
            (HydraCounter::CountSketch(count), Some(i)) => {
                count.update_weighted_with_hash(hashed_val, i)
            }
            (HydraCounter::Kll(kll), None) => kll.update_data_input(value).unwrap(),
            (HydraCounter::Kll(kll), Some(i)) => {
                for _ in 0..i as usize {
                    kll.update_data_input(value).unwrap()
                }
            }
            (HydraCounter::UnivMon(u), None) => u.update_weighted(value, 1),
            (HydraCounter::UnivMon(u), Some(i)) => u.update_weighted(value, i as i64),
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
    /// // For CountMinSketch, only Frequency queries are valid
    /// use asap_sketchlib::input::HydraCounter;
    /// use asap_sketchlib::input::HydraQuery;
    /// use asap_sketchlib::{CountMinSketch, FastPath, Vector2D};
    /// use asap_sketchlib::DataInput;
    /// let counter = HydraCounter::CountMinSketch(CountMinSketch::<Vector2D<i32>, FastPath>::default());
    /// let result = counter.query(&HydraQuery::Frequency(DataInput::I64(42)));
    ///
    /// // For KLL, only Quantile queries would be valid
    /// let result = counter.query(&HydraQuery::Quantile(0.5)); // median
    /// ```
    pub fn query(&self, query: &HydraQuery) -> Result<f64, String> {
        match (self, query) {
            (HydraCounter::CountMinSketch(cm), HydraQuery::Frequency(value)) => {
                Ok(cm.estimate(value) as f64)
            }
            (HydraCounter::Hll(hll_df), HydraQuery::Cardinality) => Ok(hll_df.estimate() as f64),
            (HydraCounter::CountSketch(count), HydraQuery::Frequency(value)) => {
                Ok(count.estimate(value))
            }
            (HydraCounter::Kll(kll), HydraQuery::Quantile(q)) => Ok(kll.quantile(*q)),
            (HydraCounter::Kll(kll), HydraQuery::Cdf(value)) => Ok(kll.cdf().quantile(*value)),
            (HydraCounter::UnivMon(um), HydraQuery::Cardinality) => Ok(um.estimate_cardinality()),
            (HydraCounter::UnivMon(um), HydraQuery::L1Norm) => Ok(um.estimate_l1()),
            (HydraCounter::UnivMon(um), HydraQuery::L2Norm) => Ok(um.estimate_l2()),
            (HydraCounter::UnivMon(um), HydraQuery::Entropy) => Ok(um.estimate_entropy()),
            (c, q) => Err(format!("{c} does not support {q}")),
        }
    }

    /// Merge another HydraCounter into this one
    /// Both counters must be of the same type
    pub fn merge(&mut self, other: &HydraCounter) -> Result<(), String> {
        match (self, other) {
            (HydraCounter::CountMinSketch(self_cm), HydraCounter::CountMinSketch(other_cm)) => {
                admission_rate::check_equal(self_cm.admission_rate(), other_cm.admission_rate())?;
                self_cm.merge(other_cm);
                Ok(())
            }
            (HydraCounter::Hll(h1), HydraCounter::Hll(h2)) => {
                admission_rate::check_equal(h1.admission_rate(), h2.admission_rate())?;
                h1.merge(h2);
                Ok(())
            }
            (HydraCounter::CountSketch(self_count), HydraCounter::CountSketch(other_count)) => {
                admission_rate::check_equal(
                    self_count.admission_rate(),
                    other_count.admission_rate(),
                )?;
                self_count.merge(other_count);
                Ok(())
            }
            (HydraCounter::Kll(self_kll), HydraCounter::Kll(other_kll)) => {
                self_kll.merge(other_kll);
                Ok(())
            }
            (HydraCounter::UnivMon(self_um), HydraCounter::UnivMon(other_um)) => {
                self_um.merge(other_um);
                Ok(())
            }
            (_, _) => Err("Sketch Type in Hydra Counter different, cannot merge".to_string()),
        }
    }
}

/// A key-count pair used in heap-based sketches for tracking heavy hitters.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(bound = "")]
pub struct HeapEntry<C: HeapCount = i64> {
    /// Stored key.
    pub key: OwnedDataInput,
    /// Estimated count associated with `key`.
    pub count: C,
}

impl<C: HeapCount> HeapEntry<C> {
    /// Creates a new Item with the given key and count.
    pub fn new(k: DataInput, count: C) -> Self {
        HeapEntry {
            key: input_to_owned(&k),
            count,
        }
    }

    /// Creates an item from an already-owned key.
    pub fn create_item(k: OwnedDataInput, count: C) -> Self {
        HeapEntry { key: k, count }
    }

    /// Legacy constructor for compatibility.
    pub fn init_item(k: DataInput, count: C) -> Self {
        HeapEntry {
            key: input_to_owned(&k),
            count,
        }
    }

    /// Prints the item in a human-readable format.
    pub fn print_entry(&self) {
        println!("key: {:?} with count: {}", self.key, self.count);
    }
}

// Implement Ord and PartialOrd to compare by count
impl<C: HeapCount> Ord for HeapEntry<C> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.count.order(&other.count)
    }
}

impl<C: HeapCount> PartialOrd for HeapEntry<C> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<C: HeapCount> PartialEq for HeapEntry<C> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.count.order(&other.count).is_eq()
    }
}

impl<C: HeapCount> Eq for HeapEntry<C> {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bytes that are not UTF-8 at all, so any conversion through `String`
    /// would either fail or replace them.
    const RAW: &[u8] = &[0xff, 0x00, 0xfe];

    /// A borrowed byte array owns as a byte array and borrows back as the same
    /// bytes, whatever they are.
    #[test]
    fn a_byte_array_owns_and_borrows_back_unchanged() {
        let owned = input_to_owned(&DataInput::Bytes(RAW));
        assert_eq!(owned, OwnedDataInput::Bytes(RAW.to_vec()));
        assert_eq!(heap_item_to_sketch_input(&owned), DataInput::Bytes(RAW));
        assert_eq!(owned, DataInput::Bytes(RAW));
    }

    /// `Bytes` and `Str` are different keys even where the bytes are valid
    /// UTF-8, so a sketch holding one never answers for the other.
    #[test]
    fn a_byte_key_is_not_the_string_of_the_same_bytes() {
        let bytes = input_to_owned(&DataInput::Bytes(b"abc"));
        let string = input_to_owned(&DataInput::Str("abc"));
        assert_ne!(bytes, string);

        assert_eq!(bytes, DataInput::Bytes(b"abc"));
        assert_ne!(bytes, DataInput::Str("abc"));
        assert_ne!(bytes, DataInput::String("abc".to_string()));

        assert_eq!(string, DataInput::Str("abc"));
        assert_eq!(string, DataInput::String("abc".to_string()));
        assert_ne!(string, DataInput::Bytes(b"abc"));
    }
}
