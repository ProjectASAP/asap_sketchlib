use serde::{Deserialize, Serialize};

use crate::CountL2HH;

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
