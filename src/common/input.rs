use serde::{Deserialize, Serialize};

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

/// A key-count pair used in heap-based sketches for tracking heavy hitters.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Item {
    pub key: String,
    pub count: i64,
}

impl Item {
    /// Creates a new Item with the given key and count.
    pub fn new(key: String, count: i64) -> Self {
        Item { key, count }
    }

    /// Legacy constructor for compatibility.
    pub fn init_item(key: String, count: i64) -> Self {
        Item { key, count }
    }

    /// Prints the item in a human-readable format.
    pub fn print_item(&self) {
        println!("key: {} with count: {}", self.key, self.count);
    }
}

// Implement Ord and PartialOrd to compare by count
impl Ord for Item {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.count.cmp(&other.count)
    }
}

impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
