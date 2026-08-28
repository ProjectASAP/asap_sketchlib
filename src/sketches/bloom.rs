//! # Bloom filter
//!
//! Approximate set membership: `contains` never says no about a key that was
//! inserted, and says yes about some keys that were not.
//!
//! This is the *partitioned* variant. The filter is a [`BitMatrix`](crate::BitMatrix) of `rows`
//! slices by `cols` bits, one slice per hash function, which is the same
//! `rows x cols` shape [`CountMin`](crate::CountMin) probes — one cell per row,
//! folded from the same seeded hashes. A membership query is the minimum across
//! rows, which over single bits is their AND.
//!
//! ## Reference
//! * Bloom, "Space/Time Trade-offs in Hash Coding with Allowable Errors",
//!   CACM 1970.
//! * Kirsch and Mitzenmacher, "Less Hashing, Same Performance", ESA 2006, for
//!   the per-slice partitioning.

use crate::{
    BitMatrix, DataInput, DefaultXxHasher, FastPath, FastPathHasher, MatrixStorage, RegularPath,
    SEEDLIST, SketchHasher,
};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

mod wire;

const LOWER_32_MASK: u64 = (1u64 << 32) - 1;

/// Hash functions in a default filter.
pub const BLOOM_DEFAULT_ROWS: usize = 7;
/// Bits per hash function in a default filter.
pub const BLOOM_DEFAULT_COLS: usize = 1 << 16;

/// Slices that can hash independently.
///
/// Row `r` hashes with seed index `r % SEEDLIST.len()` on both paths, so slice
/// `r` and slice `r + BLOOM_MAX_SLICES` receive the same seed and hold the same
/// bits. Selectivity therefore stops growing past this many rows.
pub const BLOOM_MAX_SLICES: usize = SEEDLIST.len();

/// Ceiling on the bits [`Bloom::with_capacity`] will size to, 256 MiB packed.
///
/// A target the ceiling cannot reach yields the widest slices that fit, and
/// [`Bloom::predicted_fpp`] then reports the rate those slices deliver.
pub const BLOOM_MAX_BITS: usize = 1 << 31;

/// Wire tag identifying the hash path a filter was built on.
///
/// The two paths fold different bits for the same key on most geometries, so a
/// filter decoded into the wrong path answers no about its own members. The tag
/// is serialized and checked so that decode fails instead.
pub trait BloomMode {
    /// Tag written into the serialized form.
    const MODE_TAG: &'static str;
}

impl BloomMode for RegularPath {
    const MODE_TAG: &'static str = "regular";
}

impl BloomMode for FastPath {
    const MODE_TAG: &'static str = "fast";
}

/// A partitioned Bloom filter over a packed bit grid.
#[derive(Clone, Debug)]
pub struct Bloom<Mode = RegularPath, H: SketchHasher = DefaultXxHasher> {
    bits: BitMatrix,
    inserted: u64,
    _mode: PhantomData<Mode>,
    _hasher: PhantomData<H>,
}

#[derive(Serialize)]
#[serde(rename = "Bloom")]
struct BloomSer<'a> {
    bits: &'a BitMatrix,
    inserted: u64,
    mode: &'static str,
}

#[derive(Deserialize)]
#[serde(rename = "Bloom")]
struct BloomDe {
    bits: BitMatrix,
    inserted: u64,
    mode: String,
}

impl<Mode: BloomMode, H: SketchHasher> Serialize for Bloom<Mode, H> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        BloomSer {
            bits: &self.bits,
            inserted: self.inserted,
            mode: Mode::MODE_TAG,
        }
        .serialize(serializer)
    }
}

impl<'de, Mode: BloomMode, H: SketchHasher> Deserialize<'de> for Bloom<Mode, H> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let input = BloomDe::deserialize(deserializer)?;
        if input.mode != Mode::MODE_TAG {
            return Err(serde::de::Error::custom(format!(
                "bloom filter was built on the {} hash path, cannot decode as {}",
                input.mode,
                Mode::MODE_TAG
            )));
        }
        Ok(Self {
            bits: input.bits,
            inserted: input.inserted,
            _mode: PhantomData,
            _hasher: PhantomData,
        })
    }
}

impl<Mode, H: SketchHasher> Default for Bloom<Mode, H> {
    fn default() -> Self {
        Self::with_dimensions(BLOOM_DEFAULT_ROWS, BLOOM_DEFAULT_COLS)
    }
}

impl<Mode, H: SketchHasher> Bloom<Mode, H> {
    /// Creates a filter of `rows` hash functions over `cols` bits each.
    ///
    /// Rows beyond [`BLOOM_MAX_SLICES`] are allowed but repeat an earlier row's
    /// seed and so hold identical bits: they cost storage and a hash each
    /// without sharpening the filter. [`Self::effective_rows`] reports the
    /// count that actually discriminates, and both rate estimates use it.
    pub fn with_dimensions(rows: usize, cols: usize) -> Self {
        Self {
            bits: BitMatrix::new(rows, cols),
            inserted: 0,
            _mode: PhantomData,
            _hasher: PhantomData,
        }
    }

    /// Creates a filter sized for `expected_items` at a target false-positive
    /// rate.
    ///
    /// See [`Self::dimensions_for`] for the sizing and its bounds.
    ///
    /// # Panics
    /// If `target_fpp` is NaN or infinite.
    pub fn with_capacity(expected_items: usize, target_fpp: f64) -> Self {
        let (rows, cols) = Self::dimensions_for(expected_items, target_fpp);
        Self::with_dimensions(rows, cols)
    }

    /// The `(rows, cols)` [`Self::with_capacity`] would choose.
    ///
    /// `k = round(log2(1/p))` slices, capped at [`BLOOM_MAX_SLICES`], then the
    /// slice width that hits `p` with exactly that many slices,
    /// `cols = -n / ln(1 - p^(1/k))`, rounded up to a power of two. Solving for
    /// the capped `k` rather than assuming the `k`-optimal split is what keeps
    /// a small target reachable: the cap costs bits, not accuracy, until
    /// [`BLOOM_MAX_BITS`] binds.
    ///
    /// The power-of-two rounding keeps the column fold free of modulo bias, so
    /// the measured rate lands at or under the target rather than above it.
    /// `target_fpp` is clamped into `(0, 1)` and both dimensions floor at 1. A
    /// target needing more than [`BLOOM_MAX_BITS`] gets the widest slices that
    /// fit, and [`Self::predicted_fpp`] reports the rate they deliver.
    ///
    /// # Panics
    /// If `target_fpp` is NaN or infinite. Clamping cannot order those, and a
    /// silently degenerate filter answers yes to everything.
    pub fn dimensions_for(expected_items: usize, target_fpp: f64) -> (usize, usize) {
        assert!(
            target_fpp.is_finite(),
            "target false-positive rate must be finite, got {target_fpp}"
        );
        let n = expected_items.max(1) as f64;
        let p = target_fpp.clamp(f64::MIN_POSITIVE, 1.0 - f64::EPSILON);
        let ln2 = std::f64::consts::LN_2;
        let m = (-n * p.ln() / (ln2 * ln2)).ceil().max(1.0);
        let rows = ((m / n) * ln2).round().clamp(1.0, BLOOM_MAX_SLICES as f64) as usize;

        let per_slice_fill = p.powf(1.0 / rows as f64);
        let wanted = (-n / (1.0 - per_slice_fill).ln()).ceil().max(1.0);
        let widest = 1usize << (BLOOM_MAX_BITS / rows).max(1).ilog2();
        let cols = if wanted >= widest as f64 {
            widest
        } else {
            (wanted as usize).next_power_of_two().min(widest)
        };
        (rows, cols)
    }

    /// Number of hash functions.
    #[inline(always)]
    pub fn rows(&self) -> usize {
        self.bits.rows()
    }

    /// Slices that hash independently: [`Self::rows`] capped at
    /// [`BLOOM_MAX_SLICES`].
    ///
    /// Rows past the seed list duplicate an earlier slice bit for bit, so they
    /// are not counted. On the fast path a packed hash layout can carry more
    /// independent windows than the seed list has entries; the cap is applied
    /// regardless, which under-reports rather than over-reports.
    #[inline(always)]
    pub fn effective_rows(&self) -> usize {
        self.rows().min(BLOOM_MAX_SLICES)
    }

    /// Bits per hash function.
    #[inline(always)]
    pub fn cols(&self) -> usize {
        self.bits.cols()
    }

    /// Total bits across every slice.
    pub fn bit_capacity(&self) -> usize {
        self.rows() * self.cols()
    }

    /// Bytes of packed storage.
    pub fn size_in_bytes(&self) -> usize {
        self.bits.size_in_bytes()
    }

    /// Number of `insert` calls this filter has seen, duplicates included.
    #[inline(always)]
    pub fn inserted(&self) -> u64 {
        self.inserted
    }

    /// Fraction of bits set.
    pub fn fill_ratio(&self) -> f64 {
        self.bits.fill_ratio()
    }

    /// True while no bit is set.
    pub fn is_empty(&self) -> bool {
        self.bits.count_ones() == 0
    }

    /// Clears every bit and the insert counter.
    pub fn clear(&mut self) {
        self.bits.clear();
        self.inserted = 0;
    }

    /// Read access to the underlying bit grid.
    pub fn as_bits(&self) -> &BitMatrix {
        &self.bits
    }

    /// False-positive rate implied by the bits actually set.
    ///
    /// Each slice contributes its own fill, so the rate is the fill ratio
    /// raised to the number of independent slices.
    pub fn estimated_fpp(&self) -> f64 {
        self.fill_ratio().powi(self.effective_rows() as i32)
    }

    /// False-positive rate the sizing formula predicts for `distinct_items`
    /// distinct keys, `(1 - e^(-n/cols))^effective_rows`.
    pub fn predicted_fpp(&self, distinct_items: usize) -> f64 {
        let n = distinct_items as f64;
        let per_slice = 1.0 - (-n / self.cols() as f64).exp();
        per_slice.powi(self.effective_rows() as i32)
    }

    /// Unions `other` into `self`.
    ///
    /// Both filters must have the same dimensions and the same hasher; the
    /// result is exactly the filter the concatenated streams would have built.
    pub fn merge_from(&mut self, other: &Self) {
        self.bits.union_from(&other.bits);
        self.inserted = self.inserted.saturating_add(other.inserted);
    }
}
// Regular-path membership: one seeded hash per slice.
impl<H: SketchHasher> Bloom<RegularPath, H> {
    /// Records `value` as a member.
    #[inline(always)]
    pub fn insert(&mut self, value: &DataInput) {
        let rows = self.bits.rows();
        let cols = self.bits.cols();
        for r in 0..rows {
            let hashed = H::hash64_seeded(r, value);
            let col = ((hashed & LOWER_32_MASK) as usize) % cols;
            self.bits.set(r, col);
        }
        self.inserted = self.inserted.saturating_add(1);
    }

    /// Records every value in `values`.
    pub fn bulk_insert(&mut self, values: &[DataInput]) {
        for value in values {
            self.insert(value);
        }
    }

    /// Returns false only if `value` was definitely never inserted.
    #[inline(always)]
    pub fn contains(&self, value: &DataInput) -> bool {
        let rows = self.bits.rows();
        let cols = self.bits.cols();
        for r in 0..rows {
            let hashed = H::hash64_seeded(r, value);
            let col = ((hashed & LOWER_32_MASK) as usize) % cols;
            if !self.bits.get(r, col) {
                return false;
            }
        }
        true
    }
}

// Fast-path membership: one combined hash decoded per slice.
impl<H: SketchHasher> Bloom<FastPath, H> {
    /// Records `value` as a member.
    #[inline(always)]
    pub fn insert(&mut self, value: &DataInput) {
        let hashed_val = <BitMatrix as FastPathHasher<H>>::hash_for_matrix(&self.bits, value);
        self.bits
            .fast_insert(|cell, _, _| *cell = true, (), &hashed_val);
        self.inserted = self.inserted.saturating_add(1);
    }

    /// Records every value in `values`.
    pub fn bulk_insert(&mut self, values: &[DataInput]) {
        for value in values {
            self.insert(value);
        }
    }

    /// Returns false only if `value` was definitely never inserted.
    #[inline(always)]
    pub fn contains(&self, value: &DataInput) -> bool {
        let hashed_val = <BitMatrix as FastPathHasher<H>>::hash_for_matrix(&self.bits, value);
        self.bits.fast_query_min(&hashed_val, |cell, _, _| *cell)
    }
}
