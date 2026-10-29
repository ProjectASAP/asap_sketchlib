//! # Bloom filter
//!
//! Approximate set membership: `contains` never says no about a key that was
//! inserted, and says yes about some keys that were not.
//!
//! This is the *partitioned* variant. The filter is a [`BitMatrix`](crate::BitMatrix) of `rows`
//! slices by `cols` bits, one slice per hash function, which is the same
//! `rows x cols` shape [`CountMinSketch`] probes — one cell per row,
//! folded from the same seeded hashes. A membership query is the minimum across
//! rows, which over single bits is their AND.
//!
//! ## Reference
//! * Bloom, "Space/Time Trade-offs in Hash Coding with Allowable Errors",
//!   CACM 1970.
//! * Kirsch and Mitzenmacher, "Less Hashing, Same Performance", ESA 2006, for
//!   the per-slice partitioning.

use crate::{
    BitMatrix, DataInput, DefaultXxHasher, FastPath, FastPathHasher, MATRIX_MAX_ROWS,
    MatrixStorage, RegularPath, SketchHasher,
};
use std::marker::PhantomData;

mod wire;

const LOWER_32_MASK: u64 = (1u64 << 32) - 1;

/// Hash functions in a default filter.
pub const BLOOM_DEFAULT_ROWS: usize = 7;
/// Bits per hash function in a default filter.
pub const BLOOM_DEFAULT_COLS: usize = 1 << 16;

/// Most slices a filter has: [`MATRIX_MAX_ROWS`], the ASAPv1 matrix row bound
/// under Bloom's name.
pub const BLOOM_MAX_SLICES: usize = MATRIX_MAX_ROWS;

/// Ceiling on the bits [`Bloom::with_capacity`] will size to, 256 MiB packed.
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
    _mode: PhantomData<Mode>,
    _hasher: PhantomData<H>,
}

impl<Mode, H: SketchHasher> Default for Bloom<Mode, H> {
    fn default() -> Self {
        Self::with_dimensions(BLOOM_DEFAULT_ROWS, BLOOM_DEFAULT_COLS)
    }
}

impl<Mode, H: SketchHasher> Bloom<Mode, H> {
    /// Creates a filter of `rows` hash functions over `cols` bits each.
    ///
    /// # Panics
    /// If `rows` exceeds [`BLOOM_MAX_SLICES`], or if either dimension is zero.
    pub fn with_dimensions(rows: usize, cols: usize) -> Self {
        assert!(
            rows <= BLOOM_MAX_SLICES,
            "a Bloom filter has at most BLOOM_MAX_SLICES ({BLOOM_MAX_SLICES}) slices, got {rows}"
        );
        Self {
            bits: BitMatrix::new(rows, cols),
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
    /// If `target_fpp` is NaN or infinite, or no filter within
    /// [`BLOOM_MAX_BITS`] meets it.
    pub fn with_capacity(expected_items: usize, target_fpp: f64) -> Self {
        let Some((rows, cols)) = Self::dimensions_for(expected_items, target_fpp) else {
            panic!(
                "no Bloom filter within BLOOM_MAX_BITS ({BLOOM_MAX_BITS}) bits holds \
                 {expected_items} items at false-positive rate {target_fpp}"
            );
        };
        Self::with_dimensions(rows, cols)
    }

    /// The `(rows, cols)` [`Self::with_capacity`] would choose.
    ///
    /// `k = round(ln(2) * m / n)` slices, capped at [`BLOOM_MAX_SLICES`], over
    /// the bit budget `m = ceil(-n * ln(p) / ln(2)^2)` for `n = expected_items`
    /// (at least 1) at target `p`. Then the slice width that hits `p` with
    /// exactly that many slices, `cols = -n / ln(1 - p^(1/k))`, rounded up to a
    /// power of two, then doubled until [`Self::predicted_fpp`] for `n` items is
    /// at most `p`. Solving for the capped `k` rather than assuming the
    /// `k`-optimal split is what keeps a small target reachable: the cap costs
    /// bits, not accuracy.
    ///
    /// The power-of-two width keeps the column fold free of modulo bias.
    /// `target_fpp` is clamped into `(0, 1)` and both dimensions floor at 1.
    /// `None` when meeting the target needs more than [`BLOOM_MAX_BITS`] bits.
    ///
    /// # Panics
    /// If `target_fpp` is NaN or infinite. Clamping cannot order those, and a
    /// silently degenerate filter answers yes to everything.
    pub fn dimensions_for(expected_items: usize, target_fpp: f64) -> Option<(usize, usize)> {
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
        let mut cols = if wanted >= widest as f64 {
            widest
        } else {
            (wanted as usize).next_power_of_two().min(widest)
        };
        while partitioned_fpp(rows, cols, n) > p {
            if cols >= widest {
                return None;
            }
            cols *= 2;
        }
        Some((rows, cols))
    }

    /// Number of hash functions.
    #[inline(always)]
    pub fn rows(&self) -> usize {
        self.bits.rows()
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

    /// Fraction of bits set.
    pub fn fill_ratio(&self) -> f64 {
        self.bits.fill_ratio()
    }

    /// True while no bit is set.
    pub fn is_empty(&self) -> bool {
        self.bits.count_ones() == 0
    }

    /// Clears every bit.
    pub fn clear(&mut self) {
        self.bits.clear();
    }

    /// Read access to the underlying bit grid.
    pub fn as_bits(&self) -> &BitMatrix {
        &self.bits
    }

    /// False-positive rate implied by the bits actually set.
    ///
    /// Each slice contributes its own fill, so the rate is the fill ratio
    /// raised to the slice count.
    pub fn estimated_fpp(&self) -> f64 {
        self.fill_ratio().powi(self.rows() as i32)
    }

    /// False-positive rate for `distinct_items` distinct keys,
    /// `(1 - (1 - 1/cols)^n)^rows`, exact for a power-of-two `cols`.
    pub fn predicted_fpp(&self, distinct_items: usize) -> f64 {
        partitioned_fpp(self.rows(), self.cols(), distinct_items as f64)
    }

    /// Unions `other` into `self`.
    ///
    /// Both filters must have the same dimensions and the same hasher; the
    /// result is exactly the filter the concatenated streams would have built.
    pub fn merge(&mut self, other: &Self) {
        self.bits.union_from(&other.bits);
    }
}
fn partitioned_fpp(rows: usize, cols: usize, items: f64) -> f64 {
    let per_slice = -(items * (-1.0 / cols as f64).ln_1p()).exp_m1();
    per_slice.powi(rows as i32)
}

// Regular-path membership: one seeded hash per slice.
impl<H: SketchHasher> Bloom<RegularPath, H> {
    /// Records `value` as a member.
    #[inline(always)]
    pub fn update(&mut self, value: &DataInput) {
        let rows = self.bits.rows();
        let cols = self.bits.cols();
        for r in 0..rows {
            let hashed = H::hash64_seeded(r, value);
            let col = ((hashed & LOWER_32_MASK) as usize) % cols;
            self.bits.set(r, col);
        }
    }

    /// Records every value in `values`.
    pub fn update_batch(&mut self, values: &[DataInput]) {
        for value in values {
            self.update(value);
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
    pub fn update(&mut self, value: &DataInput) {
        let hashed_val = <BitMatrix as FastPathHasher<H>>::hash_for_matrix(&self.bits, value);
        self.bits
            .fast_insert(|cell, _, _| *cell = true, (), &hashed_val);
    }

    /// Records every value in `values`.
    pub fn update_batch(&mut self, values: &[DataInput]) {
        for value in values {
            self.update(value);
        }
    }

    /// Returns false only if `value` was definitely never inserted.
    #[inline(always)]
    pub fn contains(&self, value: &DataInput) -> bool {
        let hashed_val = <BitMatrix as FastPathHasher<H>>::hash_for_matrix(&self.bits, value);
        self.bits.fast_query_min(&hashed_val, |cell, _, _| *cell)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WIDE_COLS: usize = 1 << 10;

    fn keys(count: u64) -> Vec<DataInput<'static>> {
        (0..count).map(DataInput::U64).collect()
    }

    /// The sizing helper is the only thing a caller can inspect before paying
    /// for the allocation, so it has to describe the filter `with_capacity`
    /// actually builds — a drift between the two would make every
    /// capacity-planning decision wrong.
    #[test]
    fn with_capacity_builds_exactly_the_geometry_dimensions_for_reports() {
        for expected in [0usize, 1, 17, 1_000, 250_000] {
            for target in [0.5, 0.1, 0.01, 1e-4, 1e-6] {
                let (rows, cols) = Bloom::<RegularPath>::dimensions_for(expected, target).unwrap();
                let built = Bloom::<RegularPath>::with_capacity(expected, target);
                assert_eq!(
                    (built.rows(), built.cols()),
                    (rows, cols),
                    "with_capacity({expected}, {target}) built {}x{} but dimensions_for reported {rows}x{cols}",
                    built.rows(),
                    built.cols()
                );
            }
        }
    }

    /// The slice count is split out of the bit budget for the expected load, so
    /// it tracks `expected_items` as well as the target: rounding the budget up
    /// buys one slice more than `round(log2(1/p))` for the smallest loads, and
    /// the two agree once the load is larger. The cap binds either way.
    #[test]
    fn the_slice_count_comes_from_the_bit_budget_not_the_target_alone() {
        for (expected, target, slices) in
            [(1usize, 0.05, 5usize), (1, 1.0 / 128.0, 8), (2, 1e-4, 14)]
        {
            let exponent = (1.0f64 / target).log2().round() as usize;
            let (rows, _) = Bloom::<RegularPath>::dimensions_for(expected, target).unwrap();
            assert_eq!(
                rows, slices,
                "dimensions_for({expected}, {target}) chose {rows} slices, not {slices}"
            );
            assert_eq!(
                rows,
                exponent + 1,
                "the budget for {expected} items no longer costs a slice over \
                 round(log2(1/{target})) = {exponent}"
            );
        }

        for target in [0.5, 0.05, 0.01, 1.0 / 128.0, 1e-4, 3e-6] {
            let exponent = (1.0f64 / target)
                .log2()
                .round()
                .clamp(1.0, BLOOM_MAX_SLICES as f64) as usize;
            for expected in [1_000usize, 100_000, 10_000_000] {
                let (rows, _) = Bloom::<RegularPath>::dimensions_for(expected, target).unwrap();
                assert_eq!(
                    rows, exponent,
                    "dimensions_for({expected}, {target}) chose {rows} slices, \
                     round(log2(1/p)) is {exponent}"
                );
            }
        }

        let (capped, _) = Bloom::<RegularPath>::dimensions_for(1_000_000, 1e-12).unwrap();
        assert_eq!(
            capped, BLOOM_MAX_SLICES,
            "a target needing more slices than the seed list holds chose {capped}"
        );
    }

    /// A target outside `(0, 1)` is clamped to the interval's own endpoints,
    /// and the lowest one needs more bits than any filter has.
    #[test]
    fn a_target_outside_the_open_unit_interval_is_clamped_to_the_endpoints() {
        let lowest = Bloom::<RegularPath>::dimensions_for(1_000, f64::MIN_POSITIVE);
        let highest = Bloom::<RegularPath>::dimensions_for(1_000, 1.0 - f64::EPSILON);
        assert_eq!(lowest, None, "a target of 2^-1022 was met");
        for below in [0.0, -0.0, -1.0, f64::MIN] {
            assert_eq!(
                Bloom::<RegularPath>::dimensions_for(1_000, below),
                lowest,
                "target {below} did not clamp up to the smallest positive rate {lowest:?}"
            );
        }
        for above in [1.0, 1.5, f64::MAX] {
            assert_eq!(
                Bloom::<RegularPath>::dimensions_for(1_000, above),
                highest,
                "target {above} did not clamp down to the largest rate below one {highest:?}"
            );
        }
        let (rows, cols) = highest.unwrap();
        assert!(rows >= 1 && cols >= 1, "clamping produced {rows}x{cols}");
        assert!(
            cols.is_power_of_two(),
            "clamped cols {cols} is not a power of two"
        );
    }

    #[test]
    fn a_target_past_the_bit_ceiling_is_unreachable() {
        assert_eq!(Bloom::<RegularPath>::dimensions_for(1 << 30, 1e-12), None);
    }

    #[test]
    #[should_panic(expected = "no Bloom filter within BLOOM_MAX_BITS")]
    fn with_capacity_panics_on_an_unreachable_target() {
        Bloom::<RegularPath>::with_capacity(1 << 30, 1e-12);
    }

    /// A NaN target cannot be ordered, so clamping it would silently produce a
    /// filter that answers yes to everything; sizing panics instead.
    #[test]
    #[should_panic(expected = "target false-positive rate must be finite")]
    fn a_target_that_is_not_a_number_panics_instead_of_sizing_a_degenerate_filter() {
        Bloom::<RegularPath>::dimensions_for(1_000, f64::NAN);
    }

    /// Same for an infinite target, and the panic has to come from
    /// `with_capacity` too, not only from the helper it delegates to.
    #[test]
    #[should_panic(expected = "target false-positive rate must be finite")]
    fn an_infinite_target_panics_instead_of_sizing_a_degenerate_filter() {
        Bloom::<RegularPath>::with_capacity(1_000, f64::INFINITY);
    }

    /// Sizing for nothing must still produce a filter, not a zero dimension
    /// that would panic the moment a key arrives.
    #[test]
    fn sizing_for_zero_expected_items_still_yields_a_filter_that_answers() {
        let (rows, cols) = Bloom::<RegularPath>::dimensions_for(0, 0.01).unwrap();
        assert!(
            rows >= 1 && cols >= 1,
            "sizing for nothing gave {rows}x{cols}"
        );
        assert_eq!(
            (rows, cols),
            Bloom::<RegularPath>::dimensions_for(1, 0.01).unwrap(),
            "sizing for zero items differs from sizing for one"
        );

        let mut filter = Bloom::<RegularPath>::with_capacity(0, 0.01);
        let key = DataInput::Str("only");
        filter.update(&key);
        assert!(
            filter.contains(&key),
            "a minimally sized filter lost its only member"
        );
    }

    /// `Default` is what every sketch built without a size gets, so its geometry
    /// is pinned to the two public constants and those must name a filter the
    /// hash family can actually serve.
    #[test]
    fn the_default_geometry_is_the_documented_pair() {
        let filter = Bloom::<RegularPath>::default();
        assert_eq!(
            (filter.rows(), filter.cols()),
            (BLOOM_DEFAULT_ROWS, BLOOM_DEFAULT_COLS),
            "Default built {}x{}",
            filter.rows(),
            filter.cols()
        );
        assert!(
            filter.rows() <= BLOOM_MAX_SLICES,
            "the default {} rows exceed the {BLOOM_MAX_SLICES} independent slices",
            filter.rows()
        );
        assert!(
            filter.cols().is_power_of_two(),
            "the default slice width {} is not a power of two",
            filter.cols()
        );
        assert!(
            filter.bit_capacity() <= BLOOM_MAX_BITS,
            "the default geometry wants {} bits, past the {BLOOM_MAX_BITS} ceiling",
            filter.bit_capacity()
        );
        assert!(filter.is_empty(), "a fresh default filter has bits set");
    }

    /// The seed list bounds how many slices can hash independently, so a wider
    /// filter is refused at construction rather than at serialization time.
    #[test]
    #[should_panic(expected = "a Bloom filter has at most BLOOM_MAX_SLICES")]
    fn more_slices_than_the_seed_list_has_is_rejected_at_construction() {
        Bloom::<RegularPath>::with_dimensions(BLOOM_MAX_SLICES + 1, WIDE_COLS);
    }

    /// The bound itself is legal: the assert fires past it, not at it.
    #[test]
    fn the_seed_list_length_itself_is_a_legal_slice_count() {
        let filter = Bloom::<RegularPath>::with_dimensions(BLOOM_MAX_SLICES, WIDE_COLS);
        assert_eq!(
            filter.rows(),
            BLOOM_MAX_SLICES,
            "the boundary geometry built {} rows",
            filter.rows()
        );
    }

    /// Both rate reporters raise a per-slice probability to the slice count, so
    /// a reporter reading the wrong count would report a rate the filter cannot
    /// deliver.
    #[test]
    fn both_rate_reporters_raise_the_per_slice_rate_to_the_slice_count() {
        let rows = BLOOM_MAX_SLICES;
        let mut filter = Bloom::<RegularPath>::with_dimensions(rows, WIDE_COLS);
        for key in keys(400) {
            filter.update(&key);
        }
        let fill = filter.fill_ratio();
        assert!(
            fill > 0.0 && fill < 1.0,
            "fill ratio {fill} leaves the exponent unobservable"
        );

        assert_eq!(
            filter.estimated_fpp(),
            fill.powi(rows as i32),
            "estimated_fpp did not use the slice count"
        );

        let per_slice = 1.0 - (1.0 - 1.0 / WIDE_COLS as f64).powi(400);
        let expected = per_slice.powi(rows as i32);
        assert!(
            (filter.predicted_fpp(400) / expected - 1.0).abs() < 1e-12,
            "predicted_fpp did not use the slice count"
        );
    }

    /// The two ends of the rate scale are what a caller watches for: an empty
    /// filter must report no false positives, and one whose every bit is set
    /// must report certainty rather than a comfortable-looking fraction.
    #[test]
    fn an_empty_filter_predicts_nothing_and_a_saturated_one_predicts_everything() {
        let mut filter = Bloom::<RegularPath>::with_dimensions(3, 8);
        assert_eq!(filter.fill_ratio(), 0.0, "a fresh filter has bits set");
        assert_eq!(
            filter.estimated_fpp(),
            0.0,
            "an empty filter claims false positives"
        );
        assert_eq!(
            filter.predicted_fpp(0),
            0.0,
            "predicting for no items claims false positives"
        );

        let mut saturating = 0;
        for key in keys(1_000) {
            filter.update(&key);
            saturating += 1;
            if filter.fill_ratio() == 1.0 {
                break;
            }
        }
        assert_eq!(
            filter.fill_ratio(),
            1.0,
            "a 3x8 filter was still not full after {saturating} inserts"
        );
        assert_eq!(
            filter.estimated_fpp(),
            1.0,
            "a saturated filter reported a rate below certainty"
        );
    }

    /// The filter is partitioned: each slice owns its own columns, so one key
    /// touches exactly one bit per row whatever the slice width. A fold that
    /// escaped its slice, or a slice width that is not a power of two folding
    /// wrong, shows up here as a bit count that is not the row count.
    #[test]
    fn a_single_update_sets_exactly_one_bit_in_every_slice() {
        for cols in [1usize, 2, 7, 64, 100, WIDE_COLS] {
            for rows in [1usize, 5, BLOOM_MAX_SLICES] {
                let key = DataInput::Str("solo");

                let mut regular = Bloom::<RegularPath>::with_dimensions(rows, cols);
                regular.update(&key);
                assert_eq!(
                    regular.as_bits().count_ones(),
                    rows,
                    "regular path set {} bits in a {rows}x{cols} filter",
                    regular.as_bits().count_ones()
                );
                assert_eq!(
                    regular.fill_ratio(),
                    rows as f64 / (rows * cols) as f64,
                    "regular path fill ratio disagrees with the bits set in {rows}x{cols}"
                );
                assert!(regular.contains(&key), "regular path lost its only member");

                let mut fast = Bloom::<FastPath>::with_dimensions(rows, cols);
                fast.update(&key);
                assert_eq!(
                    fast.as_bits().count_ones(),
                    rows,
                    "fast path set {} bits in a {rows}x{cols} filter",
                    fast.as_bits().count_ones()
                );
                assert!(fast.contains(&key), "fast path lost its only member");
            }
        }
    }

    /// `clear` takes the filter back to the empty state: no bit set, a zero
    /// fill ratio, and no former member reported present.
    #[test]
    fn clear_returns_the_filter_to_empty() {
        let mut filter = Bloom::<RegularPath>::with_dimensions(4, 256);
        assert!(filter.is_empty(), "a fresh filter has bits set");

        let repeated = DataInput::Str("same");
        for _ in 0..5 {
            filter.update(&repeated);
        }
        filter.update_batch(&keys(3));
        assert!(!filter.is_empty(), "an inserted-into filter reports empty");

        filter.clear();
        assert!(filter.is_empty(), "clear left bits set");
        assert_eq!(filter.fill_ratio(), 0.0, "clear left a non-zero fill ratio");
        assert!(!filter.contains(&repeated), "clear left a member present");
    }

    /// A merge unions the bits, so the merged filter answers yes for every
    /// member of either side and keeps its geometry.
    #[test]
    fn merging_identical_geometries_unions_the_members() {
        let mut left = Bloom::<RegularPath>::with_dimensions(5, 512);
        let mut right = Bloom::<RegularPath>::with_dimensions(5, 512);
        for value in 0..30u64 {
            left.update(&DataInput::U64(value));
        }
        for value in 30..70u64 {
            right.update(&DataInput::U64(value));
        }

        left.merge(&right);
        for value in 0..70u64 {
            assert!(
                left.contains(&DataInput::U64(value)),
                "merged filter lost member {value}"
            );
        }
        assert_eq!(
            (left.rows(), left.cols()),
            (5, 512),
            "merging changed the geometry"
        );
    }

    /// Two geometries index the same key at different columns, so unioning them
    /// word by word would produce a filter that answers no about its own
    /// members. It must panic instead of returning that quietly.
    #[test]
    #[should_panic(expected = "bit matrices must have the same dimensions to be unioned")]
    fn merging_mismatched_geometries_panics_instead_of_unioning_a_prefix() {
        let mut left = Bloom::<RegularPath>::with_dimensions(5, 512);
        let right = Bloom::<RegularPath>::with_dimensions(5, 256);
        left.merge(&right);
    }

    /// A zero dimension is a filter with no bits to probe; construction rejects
    /// it rather than handing back something every query would panic on.
    #[test]
    #[should_panic(expected = "a bit matrix needs both dimensions")]
    fn a_zero_slice_width_is_rejected_at_construction() {
        Bloom::<RegularPath>::with_dimensions(4, 0);
    }

    /// `bit_capacity` is the addressable grid and `size_in_bytes` is the packed
    /// storage behind it; the second rounds each row up to whole words, so it
    /// must cover the first without ever being confused for it.
    #[test]
    fn bit_capacity_and_packed_size_describe_the_same_grid() {
        for (rows, cols) in [
            (1usize, 1usize),
            (3, 7),
            (5, 64),
            (7, 65),
            (BLOOM_MAX_SLICES, WIDE_COLS),
        ] {
            let filter = Bloom::<RegularPath>::with_dimensions(rows, cols);
            assert_eq!(
                filter.bit_capacity(),
                rows * cols,
                "{rows}x{cols} reported {} addressable bits",
                filter.bit_capacity()
            );
            assert_eq!(
                filter.size_in_bytes(),
                rows * cols.div_ceil(64) * 8,
                "{rows}x{cols} reported {} bytes of storage",
                filter.size_in_bytes()
            );
            assert!(
                filter.size_in_bytes() * 8 >= filter.bit_capacity(),
                "{rows}x{cols}: {} bytes cannot hold {} bits",
                filter.size_in_bytes(),
                filter.bit_capacity()
            );
        }
    }
}
