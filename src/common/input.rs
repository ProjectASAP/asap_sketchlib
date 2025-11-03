use serde::{Deserialize, Serialize};

use crate::{Count, CountL2HH, CountMin, HllDf};

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
            AnySketch::HllDf(sketch) => sketch.insert_with_hash(hashed_val),
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
    /// Query for cardinality (for HyperLogLog, etc.)
    Cardinality,
}

/// enum that can be used as counter in Hydra
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HydraCounter {
    CM(CountMin),
    HLL(HllDf),
}

impl HydraCounter {
    /// Insert a value into the counter sketch
    /// This updates the underlying sketch with the given value
    pub fn insert(&mut self, value: &SketchInput) {
        match self {
            HydraCounter::CM(cm) => cm.fast_insert(value),
            HydraCounter::HLL(hll) => hll.insert(value),
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
    pub fn query(&self, query: &HydraQuery) -> Result<i64, String> {
        match (self, query) {
            (HydraCounter::CM(cm), HydraQuery::Frequency(value)) => {
                Ok(cm.fast_estimate(value) as i64)
            }
            (HydraCounter::CM(_), HydraQuery::Quantile(_)) => {
                Err("CountMin does not support quantile queries. Use a quantile sketch like KLL instead.".to_string())
            }
            (HydraCounter::CM(_), HydraQuery::Cardinality) => {
                Err("CountMin does not support cardinality queries. Use HyperLogLog instead.".to_string())
            }
            (HydraCounter::HLL(_), HydraQuery::Frequency(_)) => {
                Err("HyperLogLog does not support frequency queries. Use a frequency sketch like CM instead.".to_string())
            },
            (HydraCounter::HLL(_), HydraQuery::Quantile(_)) => {
                Err("HyperLogLog does not support quantile queries. Use a quantile sketch like KLL instead.".to_string())
            },
            (HydraCounter::HLL(hll_df), HydraQuery::Cardinality) => {
                Ok(hll_df.get_est() as i64)
            },
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
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct HHItem {
    pub key: String,
    pub count: i64,
}

impl HHItem {
    /// Creates a new Item with the given key and count.
    pub fn new(key: String, count: i64) -> Self {
        HHItem { key, count }
    }

    /// Legacy constructor for compatibility.
    pub fn init_item(key: String, count: i64) -> Self {
        HHItem { key, count }
    }

    /// Prints the item in a human-readable format.
    pub fn print_item(&self) {
        println!("key: {} with count: {}", self.key, self.count);
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
