//! Zero-copy view layouts: a sketch written as little-endian fixed-width
//! fields and 8-byte-aligned arrays, so a reader can use it where it lies
//! (a memory-mapped file, a buffer) through a read-only view, without
//! rebuilding the sketch.
//!
//! Each sketch family with a view has `to_view_bytes` on the owned sketch
//! (the writer), a `…View<'a>` type with `from_bytes` (validates lengths and
//! alignment; never panics on bad bytes), the family's estimates, and a
//! `merge_view` on the owned sketch that folds a view into it in place, so
//! a query merging many stored windows allocates only its accumulator.
//!
//! Layout of every view: a 4-byte family magic, a `u32` layout version, the
//! family's fixed fields (each 8 bytes, or `i32`/`u32` in pairs), then its
//! arrays, each starting 8-byte aligned. The bytes handed to `from_bytes`
//! must start 8-byte aligned. Views are read only on little-endian hosts.

use std::fmt;

/// Why bytes are not a view of the expected family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewError(pub String);

impl fmt::Display for ViewError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sketch view: {}", self.0)
    }
}

impl std::error::Error for ViewError {}

/// The layout version every view writes.
pub(crate) const VIEW_LAYOUT_VERSION: u32 = 1;

/// Builds a view's bytes.
pub(crate) struct ViewWriter {
    bytes: Vec<u8>,
}

impl ViewWriter {
    pub(crate) fn new(magic: [u8; 4]) -> Self {
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(&magic);
        bytes.extend_from_slice(&VIEW_LAYOUT_VERSION.to_le_bytes());
        Self { bytes }
    }

    pub(crate) fn u32(&mut self, value: u32) -> &mut Self {
        self.bytes.extend_from_slice(&value.to_le_bytes());
        self
    }

    pub(crate) fn i32(&mut self, value: i32) -> &mut Self {
        self.bytes.extend_from_slice(&value.to_le_bytes());
        self
    }

    pub(crate) fn u64(&mut self, value: u64) -> &mut Self {
        self.bytes.extend_from_slice(&value.to_le_bytes());
        self
    }

    pub(crate) fn f64(&mut self, value: f64) -> &mut Self {
        self.bytes.extend_from_slice(&value.to_le_bytes());
        self
    }

    fn pad8(&mut self) {
        let padded = self.bytes.len().div_ceil(8) * 8;
        self.bytes.resize(padded, 0);
    }

    pub(crate) fn u64s(&mut self, values: &[u64]) -> &mut Self {
        self.pad8();
        for value in values {
            self.bytes.extend_from_slice(&value.to_le_bytes());
        }
        self
    }

    pub(crate) fn f64s(&mut self, values: &[f64]) -> &mut Self {
        self.pad8();
        for value in values {
            self.bytes.extend_from_slice(&value.to_le_bytes());
        }
        self
    }

    pub(crate) fn u32s(&mut self, values: &[u32]) -> &mut Self {
        self.pad8();
        for value in values {
            self.bytes.extend_from_slice(&value.to_le_bytes());
        }
        self
    }

    pub(crate) fn finish(mut self) -> Vec<u8> {
        self.pad8();
        self.bytes
    }
}

/// Reads a view's bytes in place.
pub(crate) struct ViewReader<'a> {
    bytes: &'a [u8],
    at: usize,
}

macro_rules! fixed {
    ($name:ident, $ty:ty) => {
        pub(crate) fn $name(&mut self) -> Result<$ty, ViewError> {
            const N: usize = std::mem::size_of::<$ty>();
            let raw = self
                .bytes
                .get(self.at..self.at + N)
                .ok_or_else(|| ViewError("truncated".into()))?;
            self.at += N;
            Ok(<$ty>::from_le_bytes(raw.try_into().expect("N bytes")))
        }
    };
}

macro_rules! array {
    ($name:ident, $ty:ty) => {
        /// `n` values borrowed in place, starting at the next 8-byte boundary.
        pub(crate) fn $name(&mut self, n: usize) -> Result<&'a [$ty], ViewError> {
            self.at = self.at.div_ceil(8) * 8;
            let len = n
                .checked_mul(std::mem::size_of::<$ty>())
                .ok_or_else(|| ViewError("array too long".into()))?;
            let raw = self
                .bytes
                .get(self.at..self.at + len)
                .ok_or_else(|| ViewError("truncated array".into()))?;
            if raw.as_ptr() as usize % std::mem::align_of::<$ty>() != 0 {
                return Err(ViewError("misaligned array".into()));
            }
            self.at += len;
            // SAFETY: `raw` is `n * size_of::<T>()` initialized bytes, aligned
            // for `T`, which is valid for every bit pattern; the host is
            // little-endian (checked in `new`), the layout's byte order.
            Ok(unsafe { std::slice::from_raw_parts(raw.as_ptr().cast::<$ty>(), n) })
        }
    };
}

impl<'a> ViewReader<'a> {
    pub(crate) fn new(bytes: &'a [u8], magic: [u8; 4]) -> Result<Self, ViewError> {
        if cfg!(target_endian = "big") {
            return Err(ViewError("views are read on little-endian hosts only".into()));
        }
        if bytes.as_ptr() as usize % 8 != 0 {
            return Err(ViewError("view bytes are not 8-byte aligned".into()));
        }
        if bytes.get(..4) != Some(&magic[..]) {
            return Err(ViewError(format!(
                "not a {} view",
                String::from_utf8_lossy(&magic)
            )));
        }
        let mut reader = Self { bytes, at: 4 };
        let version = reader.u32()?;
        if version != VIEW_LAYOUT_VERSION {
            return Err(ViewError(format!("unknown view layout version {version}")));
        }
        Ok(reader)
    }

    fixed!(u32, u32);
    fixed!(i32, i32);
    fixed!(u64, u64);
    fixed!(f64, f64);
    array!(u64s, u64);
    array!(f64s, f64);
    array!(u32s, u32);
}

/// Copies `bytes` into an 8-byte-aligned buffer, for tests and callers whose
/// bytes are not aligned.
pub fn aligned_copy(bytes: &[u8]) -> Vec<u64> {
    let mut words = vec![0u64; bytes.len().div_ceil(8)];
    // SAFETY: `words` holds at least `bytes.len()` writable bytes.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), words.as_mut_ptr().cast::<u8>(), bytes.len())
    };
    words
}

/// The bytes of `words` (from [`aligned_copy`]), `len` long.
pub fn aligned_bytes(words: &[u64], len: usize) -> &[u8] {
    assert!(len <= words.len() * 8);
    // SAFETY: `words` is at least `len` initialized bytes.
    unsafe { std::slice::from_raw_parts(words.as_ptr().cast::<u8>(), len) }
}
