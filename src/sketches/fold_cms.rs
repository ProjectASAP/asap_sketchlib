//! Folding Count-Min Sketch (FoldCMS)
//!
//! A memory-efficient CMS variant for sub-window aggregation. Instead of
//! allocating the full W columns required by the final merged query, each
//! sub-window uses only W/2^k physical columns (where k is the fold level).
//!
//! Cells lazily expand: a cell starts as [`FoldCell::Empty`], becomes
//! [`FoldCell::Single`] on the first insert, and only upgrades to
//! [`FoldCell::Collided`] when a *second distinct* `full_col` actually
//! collides into the same physical column. This ensures zero overhead for
//! non-colliding cells.
//!
//! When sub-window sketches are merged, columns are progressively "unfolded"
//! until reaching the full CMS resolution. Folding introduces **zero**
//! additional approximation error — the accuracy is identical to a full-width
//! CMS with W columns.

use serde::{Deserialize, Serialize};

use crate::{HHHeap, SketchInput, hash64_seeded, heap_item_to_sketch_input};

const LOWER_32_MASK: u64 = (1u64 << 32) - 1;

// ---------------------------------------------------------------------------
// FoldEntry / FoldCell
// ---------------------------------------------------------------------------

/// A single tagged counter in a folded cell.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FoldEntry {
    /// Column index in the target full-width CMS (permanent address).
    pub full_col: u16,
    /// Accumulated counter value.
    pub count: i64,
}

/// Cell in a FoldCMS. Lazily expands only when a real column collision occurs.
///
/// - `Empty`    — no key has hashed to this physical column yet (zero memory).
/// - `Single`   — exactly one `full_col` present (no heap allocation).
/// - `Collided` — two or more distinct `full_col` values share this physical
///                column; entries are stored in a `Vec`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FoldCell {
    Empty,
    Single { full_col: u16, count: i64 },
    Collided(Vec<FoldEntry>),
}

impl Default for FoldCell {
    fn default() -> Self {
        FoldCell::Empty
    }
}

impl FoldCell {
    /// Insert `delta` for the given `full_col`. Upgrades the cell
    /// representation only when a genuine collision is detected.
    #[inline]
    pub fn insert(&mut self, full_col: u16, delta: i64) {
        match self {
            FoldCell::Empty => {
                *self = FoldCell::Single { full_col, count: delta };
            }
            FoldCell::Single {
                full_col: existing_col,
                count,
            } => {
                if *existing_col == full_col {
                    *count += delta;
                } else {
                    // Real collision — upgrade to Collided.
                    let existing = FoldEntry {
                        full_col: *existing_col,
                        count: *count,
                    };
                    let new_entry = FoldEntry {
                        full_col,
                        count: delta,
                    };
                    *self = FoldCell::Collided(vec![existing, new_entry]);
                }
            }
            FoldCell::Collided(entries) => {
                for entry in entries.iter_mut() {
                    if entry.full_col == full_col {
                        entry.count += delta;
                        return;
                    }
                }
                entries.push(FoldEntry {
                    full_col,
                    count: delta,
                });
            }
        }
    }

    /// Look up the counter for a specific `full_col`. Returns 0 when absent.
    #[inline]
    pub fn query(&self, full_col: u16) -> i64 {
        match self {
            FoldCell::Empty => 0,
            FoldCell::Single {
                full_col: col,
                count,
            } => {
                if *col == full_col {
                    *count
                } else {
                    0
                }
            }
            FoldCell::Collided(entries) => {
                for entry in entries {
                    if entry.full_col == full_col {
                        return entry.count;
                    }
                }
                0
            }
        }
    }

    /// Merge another cell's entries into this cell (same fold level).
    pub fn merge_from(&mut self, other: &FoldCell) {
        match other {
            FoldCell::Empty => {}
            FoldCell::Single { full_col, count } => {
                self.insert(*full_col, *count);
            }
            FoldCell::Collided(entries) => {
                for entry in entries {
                    self.insert(entry.full_col, entry.count);
                }
            }
        }
    }

    /// Returns the number of distinct `full_col` entries stored in this cell.
    pub fn entry_count(&self) -> usize {
        match self {
            FoldCell::Empty => 0,
            FoldCell::Single { .. } => 1,
            FoldCell::Collided(entries) => entries.len(),
        }
    }

    /// Returns true if no entries are stored.
    pub fn is_empty(&self) -> bool {
        matches!(self, FoldCell::Empty)
    }

    /// Iterate over all `(full_col, count)` pairs in this cell.
    pub fn iter(&self) -> FoldCellIter<'_> {
        match self {
            FoldCell::Empty => FoldCellIter::Empty,
            FoldCell::Single { full_col, count } => FoldCellIter::Single(Some((*full_col, *count))),
            FoldCell::Collided(entries) => FoldCellIter::Multi(entries.iter()),
        }
    }
}

/// Iterator over `(full_col, count)` pairs in a [`FoldCell`].
pub enum FoldCellIter<'a> {
    Empty,
    Single(Option<(u16, i64)>),
    Multi(std::slice::Iter<'a, FoldEntry>),
}

impl<'a> Iterator for FoldCellIter<'a> {
    type Item = (u16, i64);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            FoldCellIter::Empty => None,
            FoldCellIter::Single(opt) => opt.take(),
            FoldCellIter::Multi(iter) => iter.next().map(|e| (e.full_col, e.count)),
        }
    }
}

// ---------------------------------------------------------------------------
// FoldCMS
// ---------------------------------------------------------------------------

/// Folding Count-Min Sketch.
///
/// A sub-window CMS that uses `full_cols / 2^fold_level` physical columns.
/// Each physical cell lazily tracks which full-CMS column(s) it holds,
/// expanding only on real collisions. When sub-windows are merged the columns
/// are "unfolded" back towards the full-width CMS.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FoldCMS {
    /// Number of hash functions (rows). Same across all fold levels.
    rows: usize,
    /// Number of physical columns = `full_cols >> fold_level`.
    fold_cols: usize,
    /// Target full-width CMS column count (invariant across merges).
    full_cols: usize,
    /// Folding level: 0 = full-width CMS, k = folded by 2^k.
    fold_level: u32,
    /// Flat storage: `cells[row * fold_cols + col]`.
    cells: Vec<FoldCell>,
    /// Top-K heavy-hitter tracking heap.
    heap: HHHeap,
}

impl FoldCMS {
    // -- Construction -------------------------------------------------------

    /// Creates a new FoldCMS.
    ///
    /// * `rows`      — number of hash functions (typically 3–5).
    /// * `full_cols`  — target full-width CMS column count (must be power of 2).
    /// * `fold_level` — folding depth; physical columns = `full_cols / 2^fold_level`.
    /// * `top_k`      — capacity of the heavy-hitter heap.
    ///
    /// # Panics
    ///
    /// Panics if `full_cols` is not a power of two or `fold_level` is too large.
    pub fn new(rows: usize, full_cols: usize, fold_level: u32, top_k: usize) -> Self {
        assert!(
            full_cols.is_power_of_two(),
            "full_cols must be a power of two, got {full_cols}"
        );
        assert!(
            fold_level <= full_cols.trailing_zeros(),
            "fold_level {fold_level} too large for full_cols {full_cols}"
        );

        let fold_cols = full_cols >> fold_level;
        let total_cells = rows * fold_cols;
        let cells = vec![FoldCell::Empty; total_cells];

        FoldCMS {
            rows,
            fold_cols,
            full_cols,
            fold_level,
            cells,
            heap: HHHeap::new(top_k),
        }
    }

    /// Creates a FoldCMS equivalent to a full-width CMS (fold_level = 0).
    pub fn new_full(rows: usize, full_cols: usize, top_k: usize) -> Self {
        Self::new(rows, full_cols, 0, top_k)
    }

    // -- Accessors ----------------------------------------------------------

    #[inline(always)]
    pub fn rows(&self) -> usize {
        self.rows
    }

    #[inline(always)]
    pub fn fold_cols(&self) -> usize {
        self.fold_cols
    }

    #[inline(always)]
    pub fn full_cols(&self) -> usize {
        self.full_cols
    }

    #[inline(always)]
    pub fn fold_level(&self) -> u32 {
        self.fold_level
    }

    /// Returns a reference to the internal cell grid.
    pub fn cells(&self) -> &[FoldCell] {
        &self.cells
    }

    /// Returns a reference to the heavy-hitter heap.
    pub fn heap(&self) -> &HHHeap {
        &self.heap
    }

    /// Returns a mutable reference to the heavy-hitter heap.
    pub fn heap_mut(&mut self) -> &mut HHHeap {
        &mut self.heap
    }

    /// Returns the cell at `(row, fold_col)`.
    #[inline(always)]
    pub fn cell(&self, row: usize, fold_col: usize) -> &FoldCell {
        &self.cells[row * self.fold_cols + fold_col]
    }

    /// Total number of `(full_col, count)` entries across all cells.
    pub fn total_entries(&self) -> usize {
        self.cells.iter().map(|c| c.entry_count()).sum()
    }

    /// Number of cells that contain more than one entry (real collisions).
    pub fn collided_cells(&self) -> usize {
        self.cells
            .iter()
            .filter(|c| c.entry_count() > 1)
            .count()
    }

    // -- Hashing helpers ----------------------------------------------------

    /// Compute the full-width column for `(row, key)`.
    #[inline(always)]
    fn full_col_for(&self, row: usize, key: &SketchInput) -> u16 {
        let hashed = hash64_seeded(row, key);
        ((hashed & LOWER_32_MASK) as usize % self.full_cols) as u16
    }

    /// Compute the physical (folded) column from a full column.
    #[inline(always)]
    fn fold_col_of(&self, full_col: u16) -> usize {
        (full_col as usize) & (self.fold_cols - 1)
    }

    // -- Insert -------------------------------------------------------------

    /// Insert `key` with count `delta`.
    pub fn insert(&mut self, key: &SketchInput, delta: i64) {
        for r in 0..self.rows {
            let full_col = self.full_col_for(r, key);
            let fc = self.fold_col_of(full_col);
            self.cells[r * self.fold_cols + fc].insert(full_col, delta);
        }
        // Update top-k heap with current estimate.
        let est = self.query(key);
        self.heap.update(key, est);
    }

    /// Insert `key` once (delta = 1).
    #[inline]
    pub fn insert_one(&mut self, key: &SketchInput) {
        self.insert(key, 1);
    }

    // -- Point Query --------------------------------------------------------

    /// Returns the CMS frequency estimate for `key` (minimum across rows).
    pub fn query(&self, key: &SketchInput) -> i64 {
        let mut min_count = i64::MAX;
        for r in 0..self.rows {
            let full_col = self.full_col_for(r, key);
            let fc = self.fold_col_of(full_col);
            let row_count = self.cells[r * self.fold_cols + fc].query(full_col);
            if row_count < min_count {
                min_count = row_count;
            }
        }
        min_count
    }

    // -- Same-level merge ---------------------------------------------------

    /// Merge `other` into `self` without unfolding. Both must share the same
    /// `full_cols`, `rows`, and `fold_level`.
    ///
    /// After merging, the top-k heap is reconciled by re-querying all heap
    /// items from both sources against the merged sketch.
    pub fn merge_same_level(&mut self, other: &FoldCMS) {
        assert_eq!(self.rows, other.rows, "row count mismatch");
        assert_eq!(self.full_cols, other.full_cols, "full_cols mismatch");
        assert_eq!(self.fold_level, other.fold_level, "fold_level mismatch");
        assert_eq!(self.fold_cols, other.fold_cols, "fold_cols mismatch");

        for idx in 0..self.cells.len() {
            self.cells[idx].merge_from(&other.cells[idx]);
        }

        self.reconcile_heap_from(other);
    }

    // -- Unfold merge -------------------------------------------------------

    /// Merge two **same-level** FoldCMS sketches into a new sketch one fold
    /// level lower (doubled physical columns).
    ///
    /// Both `a` and `b` must be at fold level k > 0. The result is at level k-1.
    pub fn unfold_merge(a: &FoldCMS, b: &FoldCMS) -> FoldCMS {
        assert_eq!(a.rows, b.rows, "row count mismatch");
        assert_eq!(a.full_cols, b.full_cols, "full_cols mismatch");
        assert_eq!(a.fold_level, b.fold_level, "fold_level mismatch");
        assert!(a.fold_level > 0, "cannot unfold from fold_level 0");

        let new_level = a.fold_level - 1;
        let new_fold_cols = a.full_cols >> new_level;
        let heap_k = a.heap.capacity().max(b.heap.capacity());

        let mut result = FoldCMS {
            rows: a.rows,
            fold_cols: new_fold_cols,
            full_cols: a.full_cols,
            fold_level: new_level,
            cells: vec![FoldCell::Empty; a.rows * new_fold_cols],
            heap: HHHeap::new(heap_k),
        };

        // Scatter entries from both sources into the wider grid.
        for source in [a, b] {
            for r in 0..source.rows {
                for c in 0..source.fold_cols {
                    let cell = &source.cells[r * source.fold_cols + c];
                    for (full_col, count) in cell.iter() {
                        let new_fc = (full_col as usize) & (new_fold_cols - 1);
                        result.cells[r * new_fold_cols + new_fc].insert(full_col, count);
                    }
                }
            }
        }

        // Reconcile top-k heaps from both sources.
        for source in [a, b] {
            for item in source.heap.heap() {
                let key_ref = heap_item_to_sketch_input(&item.key);
                let est = result.query(&key_ref);
                result.heap.update(&key_ref, est);
            }
        }

        result
    }

    /// Fully unfold a FoldCMS to fold_level 0 (equivalent to a standard CMS).
    /// If already at level 0 this returns a clone.
    pub fn unfold_full(&self) -> FoldCMS {
        if self.fold_level == 0 {
            return self.clone();
        }

        // Iteratively unfold one level at a time.
        let mut current = self.clone();
        while current.fold_level > 0 {
            let empty = FoldCMS::new(
                current.rows,
                current.full_cols,
                current.fold_level,
                current.heap.capacity(),
            );
            current = FoldCMS::unfold_merge(&current, &empty);
        }
        current
    }

    // -- Hierarchical merge -------------------------------------------------

    /// Unfold `self` down to the target fold level (must be ≤ current level).
    /// If already at the target level, returns a clone.
    pub fn unfold_to(&self, target_level: u32) -> FoldCMS {
        assert!(
            target_level <= self.fold_level,
            "target_level {target_level} > current fold_level {}",
            self.fold_level
        );
        let mut current = self.clone();
        while current.fold_level > target_level {
            let empty = FoldCMS::new(
                current.rows,
                current.full_cols,
                current.fold_level,
                current.heap.capacity(),
            );
            current = FoldCMS::unfold_merge(&current, &empty);
        }
        current
    }

    // -- Hierarchical merge -------------------------------------------------

    /// Merge a sequence of FoldCMS sketches via pairwise unfolding.
    ///
    /// Adjacent pairs are unfold-merged, then the results are paired again,
    /// until one sketch remains. Handles non-power-of-two lengths and
    /// mixed fold levels (the higher-level sketch is unfolded to match).
    pub fn hierarchical_merge(sketches: &[FoldCMS]) -> FoldCMS {
        assert!(
            !sketches.is_empty(),
            "need at least one sketch to merge"
        );
        if sketches.len() == 1 {
            return sketches[0].clone();
        }

        let mut current: Vec<FoldCMS> = sketches.to_vec();
        while current.len() > 1 {
            let mut next = Vec::with_capacity((current.len() + 1) / 2);
            let mut i = 0;
            while i + 1 < current.len() {
                let left = &current[i];
                let right = &current[i + 1];
                if left.fold_level == 0 && right.fold_level == 0 {
                    let mut merged = left.clone();
                    merged.merge_same_level(right);
                    next.push(merged);
                } else if left.fold_level == right.fold_level {
                    next.push(FoldCMS::unfold_merge(left, right));
                } else {
                    // Different levels: unfold the higher one to match the lower.
                    let target = left.fold_level.min(right.fold_level);
                    let a = left.unfold_to(target);
                    let b = right.unfold_to(target);
                    if target == 0 {
                        let mut merged = a;
                        merged.merge_same_level(&b);
                        next.push(merged);
                    } else {
                        next.push(FoldCMS::unfold_merge(&a, &b));
                    }
                }
                i += 2;
            }
            // Odd one out — carry forward.
            if i < current.len() {
                next.push(current[i].clone());
            }
            current = next;
        }
        current.into_iter().next().unwrap()
    }

    // -- Conversion ---------------------------------------------------------

    /// Extract the flat i64 counter array equivalent to a standard CMS.
    ///
    /// Returns a `rows × full_cols` row-major vector where each element is
    /// the accumulated count for that `(row, full_col)` cell.
    ///
    /// Works at *any* fold level — the full_col stored in each entry maps
    /// directly to the output position.
    pub fn to_flat_counters(&self) -> Vec<i64> {
        let mut out = vec![0i64; self.rows * self.full_cols];
        for r in 0..self.rows {
            for c in 0..self.fold_cols {
                let cell = &self.cells[r * self.fold_cols + c];
                for (full_col, count) in cell.iter() {
                    out[r * self.full_cols + full_col as usize] += count;
                }
            }
        }
        out
    }

    // -- Clear --------------------------------------------------------------

    /// Reset all cells to [`FoldCell::Empty`] and clear the heap.
    ///
    /// The outer `Vec<FoldCell>` allocation is preserved; inner `Collided(Vec)`
    /// data is dropped.
    pub fn clear(&mut self) {
        for cell in &mut self.cells {
            *cell = FoldCell::Empty;
        }
        self.heap.clear();
    }

    // -- Heap helpers -------------------------------------------------------

    /// Re-query all heap items from `other` against `self` and update our heap.
    fn reconcile_heap_from(&mut self, other: &FoldCMS) {
        for item in other.heap.heap() {
            let key_ref = heap_item_to_sketch_input(&item.key);
            let est = self.query(&key_ref);
            self.heap.update(&key_ref, est);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::sample_zipf_u64;
    use crate::{CountMin, HeapItem, RegularPath, Vector2D};
    use std::collections::HashMap;

    // -- FoldCell unit tests ------------------------------------------------

    #[test]
    fn cell_starts_empty() {
        let cell = FoldCell::Empty;
        assert_eq!(cell.entry_count(), 0);
        assert!(cell.is_empty());
        assert_eq!(cell.query(42), 0);
    }

    #[test]
    fn cell_single_insert() {
        let mut cell = FoldCell::Empty;
        cell.insert(10, 5);
        assert_eq!(cell.entry_count(), 1);
        assert_eq!(cell.query(10), 5);
        assert_eq!(cell.query(11), 0);
        assert!(matches!(cell, FoldCell::Single { .. }));
    }

    #[test]
    fn cell_single_accumulates() {
        let mut cell = FoldCell::Empty;
        cell.insert(10, 5);
        cell.insert(10, 3);
        assert_eq!(cell.entry_count(), 1);
        assert_eq!(cell.query(10), 8);
        // Still Single — no collision occurred.
        assert!(matches!(cell, FoldCell::Single { .. }));
    }

    #[test]
    fn cell_collision_upgrades_to_collided() {
        let mut cell = FoldCell::Empty;
        cell.insert(10, 5);
        cell.insert(42, 3); // different full_col → real collision
        assert_eq!(cell.entry_count(), 2);
        assert!(matches!(cell, FoldCell::Collided(_)));
        assert_eq!(cell.query(10), 5);
        assert_eq!(cell.query(42), 3);
    }

    #[test]
    fn cell_collided_accumulates() {
        let mut cell = FoldCell::Empty;
        cell.insert(10, 5);
        cell.insert(42, 3);
        cell.insert(10, 2);
        cell.insert(42, 7);
        assert_eq!(cell.query(10), 7);
        assert_eq!(cell.query(42), 10);
        assert_eq!(cell.entry_count(), 2);
    }

    #[test]
    fn cell_collided_third_entry() {
        let mut cell = FoldCell::Empty;
        cell.insert(10, 1);
        cell.insert(42, 2);
        cell.insert(99, 3);
        assert_eq!(cell.entry_count(), 3);
        assert_eq!(cell.query(10), 1);
        assert_eq!(cell.query(42), 2);
        assert_eq!(cell.query(99), 3);
    }

    #[test]
    fn cell_merge_from_empty() {
        let mut a = FoldCell::Empty;
        a.insert(10, 5);
        let b = FoldCell::Empty;
        a.merge_from(&b);
        assert_eq!(a.query(10), 5);
    }

    #[test]
    fn cell_merge_from_single() {
        let mut a = FoldCell::Empty;
        a.insert(10, 5);
        let mut b = FoldCell::Empty;
        b.insert(10, 3);
        a.merge_from(&b);
        assert_eq!(a.query(10), 8);
        assert!(matches!(a, FoldCell::Single { .. })); // still no collision
    }

    #[test]
    fn cell_merge_from_collision() {
        let mut a = FoldCell::Empty;
        a.insert(10, 5);
        let mut b = FoldCell::Empty;
        b.insert(42, 3);
        a.merge_from(&b);
        assert_eq!(a.query(10), 5);
        assert_eq!(a.query(42), 3);
        assert!(matches!(a, FoldCell::Collided(_)));
    }

    #[test]
    fn cell_iter_empty() {
        let cell = FoldCell::Empty;
        assert_eq!(cell.iter().count(), 0);
    }

    #[test]
    fn cell_iter_single() {
        let mut cell = FoldCell::Empty;
        cell.insert(7, 99);
        let items: Vec<_> = cell.iter().collect();
        assert_eq!(items, vec![(7, 99)]);
    }

    #[test]
    fn cell_iter_collided() {
        let mut cell = FoldCell::Empty;
        cell.insert(7, 10);
        cell.insert(15, 20);
        let mut items: Vec<_> = cell.iter().collect();
        items.sort();
        assert_eq!(items, vec![(7, 10), (15, 20)]);
    }

    // -- FoldCMS basic tests ------------------------------------------------

    #[test]
    fn fold_cms_dimensions() {
        let sketch = FoldCMS::new(3, 4096, 4, 10);
        assert_eq!(sketch.rows(), 3);
        assert_eq!(sketch.full_cols(), 4096);
        assert_eq!(sketch.fold_cols(), 256); // 4096 / 2^4
        assert_eq!(sketch.fold_level(), 4);
    }

    #[test]
    fn fold_cms_level_zero_is_full() {
        let sketch = FoldCMS::new_full(3, 1024, 10);
        assert_eq!(sketch.fold_cols(), 1024);
        assert_eq!(sketch.fold_level(), 0);
    }

    #[test]
    #[should_panic(expected = "full_cols must be a power of two")]
    fn fold_cms_rejects_non_power_of_two() {
        FoldCMS::new(3, 1000, 0, 10);
    }

    #[test]
    #[should_panic(expected = "fold_level")]
    fn fold_cms_rejects_excessive_fold_level() {
        FoldCMS::new(3, 256, 9, 10); // 256 = 2^8, fold_level 9 is too big
    }

    #[test]
    fn fold_cms_insert_query_single_key() {
        let mut sketch = FoldCMS::new(3, 1024, 4, 10);
        let key = SketchInput::Str("hello");
        sketch.insert(&key, 7);
        assert_eq!(sketch.query(&key), 7);
    }

    #[test]
    fn fold_cms_insert_accumulates() {
        let mut sketch = FoldCMS::new(3, 1024, 4, 10);
        let key = SketchInput::Str("hello");
        sketch.insert(&key, 3);
        sketch.insert(&key, 4);
        assert_eq!(sketch.query(&key), 7);
    }

    #[test]
    fn fold_cms_absent_key_returns_zero() {
        let mut sketch = FoldCMS::new(3, 1024, 4, 10);
        sketch.insert(&SketchInput::Str("present"), 10);
        assert_eq!(sketch.query(&SketchInput::Str("absent")), 0);
    }

    #[test]
    fn fold_cms_multiple_keys() {
        let mut sketch = FoldCMS::new(3, 4096, 4, 10);
        for i in 0..100u64 {
            sketch.insert(&SketchInput::U64(i), i as i64);
        }
        for i in 0..100u64 {
            let est = sketch.query(&SketchInput::U64(i));
            // CMS only over-estimates, and FoldCMS is exact w.r.t. the full CMS.
            assert!(
                est >= i as i64,
                "estimate {est} < true count {i} for key {i}"
            );
        }
    }

    // -- Exact match with standard CountMin ---------------------------------

    #[test]
    fn fold_cms_matches_standard_cms_exact() {
        let rows = 3;
        let cols = 256; // small for deterministic testing
        let fold_level = 3; // 256/8 = 32 physical columns

        let mut fold = FoldCMS::new(rows, cols, fold_level, 10);
        let mut standard = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(rows, cols);

        let keys: Vec<SketchInput> = (0..50).map(|i| SketchInput::I32(i)).collect();
        for key in &keys {
            fold.insert(key, 1);
            standard.insert(key);
        }

        // Every single query must match exactly.
        for key in &keys {
            let fold_est = fold.query(key);
            let std_est = standard.estimate(key) as i64;
            assert_eq!(
                fold_est, std_est,
                "mismatch for {key:?}: fold={fold_est}, std={std_est}"
            );
        }

        // Also verify via flat counter extraction.
        let flat = fold.to_flat_counters();
        let std_flat = standard.as_storage().as_slice();
        assert_eq!(flat.len(), std_flat.len());
        for (i, (f, s)) in flat.iter().zip(std_flat.iter()).enumerate() {
            assert_eq!(
                *f, *s as i64,
                "flat counter mismatch at index {i}: fold={f}, std={s}"
            );
        }
    }

    #[test]
    fn fold_cms_matches_standard_cms_insert_many() {
        let rows = 3;
        let cols = 512;
        let fold_level = 4;

        let mut fold = FoldCMS::new(rows, cols, fold_level, 10);
        let mut standard = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(rows, cols);

        // Insert keys with varying counts.
        for i in 0..30 {
            let key = SketchInput::U64(i);
            let count = (i + 1) as i64;
            fold.insert(&key, count);
            standard.insert_many(&key, count);
        }

        for i in 0..30 {
            let key = SketchInput::U64(i);
            assert_eq!(fold.query(&key), standard.estimate(&key) as i64);
        }
    }

    // -- Same-level merge ---------------------------------------------------

    #[test]
    fn same_level_merge_adds_counts() {
        let rows = 3;
        let cols = 1024;
        let fold_level = 3;

        let mut a = FoldCMS::new(rows, cols, fold_level, 10);
        let mut b = FoldCMS::new(rows, cols, fold_level, 10);

        let key = SketchInput::Str("user_001");
        a.insert(&key, 100);
        b.insert(&key, 200);

        a.merge_same_level(&b);
        assert_eq!(a.query(&key), 300);
    }

    #[test]
    fn same_level_merge_matches_standard_cms_merge() {
        let rows = 3;
        let cols = 512;
        let fold_level = 4;

        let mut fa = FoldCMS::new(rows, cols, fold_level, 10);
        let mut fb = FoldCMS::new(rows, cols, fold_level, 10);
        let mut sa = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(rows, cols);
        let mut sb = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(rows, cols);

        for i in 0..20 {
            let key = SketchInput::U64(i);
            fa.insert(&key, 1);
            sa.insert(&key);
        }
        for i in 10..30 {
            let key = SketchInput::U64(i);
            fb.insert(&key, 1);
            sb.insert(&key);
        }

        fa.merge_same_level(&fb);
        sa.merge(&sb);

        for i in 0..30 {
            let key = SketchInput::U64(i);
            assert_eq!(
                fa.query(&key),
                sa.estimate(&key) as i64,
                "mismatch after same-level merge for key {i}"
            );
        }
    }

    // -- Unfold merge -------------------------------------------------------

    #[test]
    fn unfold_merge_reduces_level() {
        let rows = 3;
        let cols = 1024;
        let fold_level = 3;

        let a = FoldCMS::new(rows, cols, fold_level, 10);
        let b = FoldCMS::new(rows, cols, fold_level, 10);

        let result = FoldCMS::unfold_merge(&a, &b);
        assert_eq!(result.fold_level(), 2);
        assert_eq!(result.fold_cols(), cols >> 2);
    }

    #[test]
    fn unfold_merge_preserves_counts() {
        let rows = 3;
        let cols = 256;
        let fold_level = 2;

        let mut a = FoldCMS::new(rows, cols, fold_level, 10);
        let mut b = FoldCMS::new(rows, cols, fold_level, 10);

        let key_a = SketchInput::Str("alpha");
        let key_b = SketchInput::Str("beta");
        a.insert(&key_a, 10);
        b.insert(&key_b, 20);

        let merged = FoldCMS::unfold_merge(&a, &b);
        assert_eq!(merged.fold_level(), 1);
        assert_eq!(merged.query(&key_a), 10);
        assert_eq!(merged.query(&key_b), 20);
    }

    #[test]
    fn unfold_merge_matches_standard_cms_merge() {
        let rows = 3;
        let cols = 512;
        let fold_level = 2;

        let mut fa = FoldCMS::new(rows, cols, fold_level, 10);
        let mut fb = FoldCMS::new(rows, cols, fold_level, 10);
        let mut sa = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(rows, cols);
        let mut sb = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(rows, cols);

        for i in 0..40 {
            let key = SketchInput::U64(i);
            fa.insert(&key, (i + 1) as i64);
            sa.insert_many(&key, (i + 1) as i64);
        }
        for i in 20..60 {
            let key = SketchInput::U64(i);
            fb.insert(&key, (i + 1) as i64);
            sb.insert_many(&key, (i + 1) as i64);
        }

        let merged_fold = FoldCMS::unfold_merge(&fa, &fb);
        sa.merge(&sb);

        for i in 0..60 {
            let key = SketchInput::U64(i);
            assert_eq!(
                merged_fold.query(&key),
                sa.estimate(&key) as i64,
                "unfold merge mismatch for key {i}"
            );
        }
    }

    // -- Hierarchical merge -------------------------------------------------

    #[test]
    fn hierarchical_merge_four_sketches() {
        let rows = 3;
        let cols = 1024;
        let fold_level = 2; // 1024/4 = 256 physical cols

        let mut sketches = Vec::new();
        let mut standard = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(rows, cols);

        for epoch in 0..4u64 {
            let mut sk = FoldCMS::new(rows, cols, fold_level, 10);
            for i in (epoch * 10)..((epoch + 1) * 10) {
                let key = SketchInput::U64(i);
                sk.insert(&key, 1);
                standard.insert(&key);
            }
            sketches.push(sk);
        }

        let merged = FoldCMS::hierarchical_merge(&sketches);
        assert_eq!(merged.fold_level(), 0);

        for i in 0..40u64 {
            let key = SketchInput::U64(i);
            assert_eq!(
                merged.query(&key),
                standard.estimate(&key) as i64,
                "hierarchical merge mismatch for key {i}"
            );
        }
    }

    // -- unfold_full --------------------------------------------------------

    #[test]
    fn unfold_full_matches_flat_counters() {
        let rows = 3;
        let cols = 256;
        let fold_level = 4;

        let mut sk = FoldCMS::new(rows, cols, fold_level, 10);
        for i in 0..30 {
            sk.insert(&SketchInput::I32(i), 1);
        }

        let flat_before = sk.to_flat_counters();
        let full = sk.unfold_full();
        assert_eq!(full.fold_level(), 0);
        assert_eq!(full.fold_cols(), cols);

        let flat_after = full.to_flat_counters();
        assert_eq!(flat_before, flat_after);
    }

    // -- to_flat_counters ---------------------------------------------------

    #[test]
    fn to_flat_counters_matches_standard_cms() {
        let rows = 3;
        let cols = 128;
        let fold_level = 3;

        let mut fold = FoldCMS::new(rows, cols, fold_level, 10);
        let mut standard = CountMin::<Vector2D<i64>, RegularPath>::with_dimensions(rows, cols);

        for i in 0..20 {
            let key = SketchInput::I32(i);
            fold.insert(&key, 1);
            standard.insert(&key);
        }

        let flat = fold.to_flat_counters();
        let std_flat = standard.as_storage().as_slice();
        for (i, (f, s)) in flat.iter().zip(std_flat.iter()).enumerate() {
            assert_eq!(
                *f, *s as i64,
                "flat counter mismatch at [{i}]: fold={f}, std={s}"
            );
        }
    }

    // -- Memory efficiency --------------------------------------------------

    #[test]
    fn sparse_subwindow_has_few_collisions() {
        let rows = 3;
        let cols = 4096;
        let fold_level = 4; // 256 physical cols

        let mut sk = FoldCMS::new(rows, cols, fold_level, 10);
        // Insert only 50 distinct keys into a 256-column folded sketch.
        for i in 0..50u64 {
            sk.insert(&SketchInput::U64(i), 1);
        }

        let total_entries = sk.total_entries();
        let collided = sk.collided_cells();

        // With 50 keys across 256 columns, total entries ≈ 50 * rows.
        // Some keys may hash-collide (same full_col, different keys), so
        // total_entries ≤ 50 * rows. Very few fold-collisions expected.
        assert!(
            total_entries <= rows * 50,
            "total_entries={total_entries} should be <= {} (rows*distinct_keys)",
            rows * 50
        );
        assert!(
            total_entries >= rows * 45,
            "total_entries={total_entries} unexpectedly low"
        );
        // Very few fold-collisions expected with 50 keys in 256 physical columns.
        assert!(
            collided < 30,
            "expected few collided cells, got {collided}"
        );
    }

    // -- Top-K heap integration ---------------------------------------------

    #[test]
    fn heap_tracks_heavy_hitters() {
        let mut sk = FoldCMS::new(3, 1024, 3, 5);

        // Insert keys with different frequencies.
        for _ in 0..100 {
            sk.insert(&SketchInput::Str("heavy"), 1);
        }
        for _ in 0..10 {
            sk.insert(&SketchInput::Str("medium"), 1);
        }
        sk.insert(&SketchInput::Str("light"), 1);

        let heap_items = sk.heap().heap();
        assert!(!heap_items.is_empty());

        // "heavy" should be in the heap with the highest count.
        let heavy = heap_items
            .iter()
            .find(|item| item.key == HeapItem::String("heavy".to_owned()));
        assert!(heavy.is_some(), "heavy hitter should be in heap");
        assert_eq!(heavy.unwrap().count, 100);
    }

    #[test]
    fn heap_survives_same_level_merge() {
        let mut a = FoldCMS::new(3, 1024, 3, 5);
        let mut b = FoldCMS::new(3, 1024, 3, 5);

        for _ in 0..50 {
            a.insert(&SketchInput::Str("user_x"), 1);
        }
        for _ in 0..70 {
            b.insert(&SketchInput::Str("user_x"), 1);
        }

        a.merge_same_level(&b);

        let found = a
            .heap()
            .heap()
            .iter()
            .find(|item| item.key == HeapItem::String("user_x".to_owned()));
        assert!(found.is_some());
        assert_eq!(found.unwrap().count, 120);
    }

    #[test]
    fn heap_survives_unfold_merge() {
        let mut a = FoldCMS::new(3, 512, 2, 5);
        let mut b = FoldCMS::new(3, 512, 2, 5);

        for _ in 0..40 {
            a.insert(&SketchInput::Str("endpoint_a"), 1);
        }
        for _ in 0..60 {
            b.insert(&SketchInput::Str("endpoint_a"), 1);
        }

        let merged = FoldCMS::unfold_merge(&a, &b);
        let found = merged
            .heap()
            .heap()
            .iter()
            .find(|item| item.key == HeapItem::String("endpoint_a".to_owned()));
        assert!(found.is_some());
        assert_eq!(found.unwrap().count, 100);
    }

    // -- Error bound (statistical) ------------------------------------------

    #[test]
    fn fold_cms_error_bound_zipf() {
        let rows = 3;
        let cols = 4096;
        let fold_level = 4;
        let domain = 8192;
        let exponent = 1.1;
        let samples = 200_000;

        let mut fold = FoldCMS::new(rows, cols, fold_level, 20);
        let mut truth = HashMap::<u64, i64>::new();

        for value in sample_zipf_u64(domain, exponent, samples, 0x5eed_c0de) {
            fold.insert(&SketchInput::U64(value), 1);
            *truth.entry(value).or_insert(0) += 1;
        }

        let epsilon = std::f64::consts::E / cols as f64;
        let delta = 1.0 / std::f64::consts::E.powi(rows as i32);
        let error_bound = epsilon * samples as f64;
        let correct_lower_bound = truth.len() as f64 * (1.0 - delta);

        let mut within_count = 0;
        for (key, true_count) in &truth {
            let est = fold.query(&SketchInput::U64(*key));
            if ((est - true_count).unsigned_abs() as f64) < error_bound {
                within_count += 1;
            }
        }

        assert!(
            within_count as f64 > correct_lower_bound,
            "in-bound items {within_count} not > expected {correct_lower_bound}"
        );
    }

    // -- Motivation scenario tests ------------------------------------------

    #[test]
    fn scenario_rate_limiting() {
        // Per-User Request Counting (from motivation Example 1)
        let rows = 3;
        let cols = 4096;
        let fold_level = 4;

        // Epoch 1: 10:00-10:01
        let mut epoch1 = FoldCMS::new(rows, cols, fold_level, 5);
        epoch1.insert(&SketchInput::Str("user_001"), 350);
        epoch1.insert(&SketchInput::Str("user_002"), 10);
        epoch1.insert(&SketchInput::Str("user_003"), 600);

        // Epoch 2: 10:01-10:02
        let mut epoch2 = FoldCMS::new(rows, cols, fold_level, 5);
        epoch2.insert(&SketchInput::Str("user_001"), 350);
        epoch2.insert(&SketchInput::Str("user_002"), 5);
        epoch2.insert(&SketchInput::Str("user_003"), 700);

        // Merge via same-level (both at fold_level 4)
        epoch1.merge_same_level(&epoch2);

        assert_eq!(epoch1.query(&SketchInput::Str("user_001")), 700);
        assert_eq!(epoch1.query(&SketchInput::Str("user_002")), 15);
        assert_eq!(epoch1.query(&SketchInput::Str("user_003")), 1300);
    }

    #[test]
    fn scenario_error_frequency() {
        // Per-Endpoint Error Frequency (from motivation Example 2)
        let rows = 3;
        let cols = 4096;
        let fold_level = 4;

        let mut epoch1 = FoldCMS::new(rows, cols, fold_level, 5);
        epoch1.insert(&SketchInput::Str("/api/v1/search"), 300);
        epoch1.insert(&SketchInput::Str("/api/v1/checkout"), 5);
        epoch1.insert(&SketchInput::Str("/api/v1/login"), 200);
        epoch1.insert(&SketchInput::Str("/api/v2/recommend"), 1);

        let mut epoch2 = FoldCMS::new(rows, cols, fold_level, 5);
        epoch2.insert(&SketchInput::Str("/api/v1/search"), 50);
        epoch2.insert(&SketchInput::Str("/api/v1/checkout"), 5);
        epoch2.insert(&SketchInput::Str("/api/v1/login"), 10);
        epoch2.insert(&SketchInput::Str("/api/v2/recommend"), 100);

        epoch1.merge_same_level(&epoch2);

        assert_eq!(
            epoch1.query(&SketchInput::Str("/api/v1/search")),
            350
        );
        assert_eq!(
            epoch1.query(&SketchInput::Str("/api/v1/login")),
            210
        );
        assert_eq!(
            epoch1.query(&SketchInput::Str("/api/v2/recommend")),
            101
        );
        assert_eq!(
            epoch1.query(&SketchInput::Str("/api/v1/checkout")),
            10
        );
    }

    // -- Large-window merge benchmark ---------------------------------------

    #[test]
    fn large_window_merge_benchmark_cms() {
        let rows = 3;
        let full_cols = 4096;
        let fold_level = 4; // 256 physical cols per sub-window
        let top_k = 20;
        let domain = 10_000;
        let exponent = 1.1;
        let total_samples = 500_000;
        let num_subwindows = 16;
        let samples_per_window = total_samples / num_subwindows;

        // Generate full stream and split into sub-windows.
        let stream = sample_zipf_u64(domain, exponent, total_samples, 0xBEEF_CAFE);

        let mut truth = HashMap::<u64, i64>::new();
        let mut subwindow_sketches = Vec::with_capacity(num_subwindows);

        for w in 0..num_subwindows {
            let start = w * samples_per_window;
            let end = start + samples_per_window;
            let mut sk = FoldCMS::new(rows, full_cols, fold_level, top_k);

            for &value in &stream[start..end] {
                sk.insert(&SketchInput::U64(value), 1);
                *truth.entry(value).or_insert(0) += 1;
            }
            subwindow_sketches.push(sk);
        }

        // Print per sub-window stats.
        eprintln!("\n=== FoldCMS Large-Window Merge Benchmark ===");
        eprintln!(
            "Config: rows={rows}, full_cols={full_cols}, fold_level={fold_level}, \
             sub-windows={num_subwindows}, samples/window={samples_per_window}"
        );
        for (w, sk) in subwindow_sketches.iter().enumerate() {
            eprintln!(
                "  Sub-window {w:>2}: cells={:<6} entries={:<6} collided={}",
                sk.cells().len(),
                sk.total_entries(),
                sk.collided_cells()
            );
        }

        // Hierarchical merge.
        let merged = FoldCMS::hierarchical_merge(&subwindow_sketches);
        eprintln!(
            "  Merged:        cells={:<6} entries={:<6} collided={} fold_level={}",
            merged.cells().len(),
            merged.total_entries(),
            merged.collided_cells(),
            merged.fold_level()
        );

        // Standard CMS for comparison.
        let std_memory = rows * full_cols * std::mem::size_of::<i64>();
        eprintln!(
            "  Standard CMS memory (per sketch): {} bytes ({} counters)",
            std_memory,
            rows * full_cols
        );

        // Error statistics.
        let epsilon = std::f64::consts::E / full_cols as f64;
        let error_bound = epsilon * total_samples as f64;

        let mut total_abs_error: f64 = 0.0;
        let mut max_abs_error: i64 = 0;
        let mut within_bound = 0usize;

        for (&key, &true_count) in &truth {
            let est = merged.query(&SketchInput::U64(key));
            let abs_err = (est - true_count).abs();
            total_abs_error += abs_err as f64;
            if abs_err > max_abs_error {
                max_abs_error = abs_err;
            }
            if (abs_err as f64) < error_bound {
                within_bound += 1;
            }
        }

        let mean_abs_error = total_abs_error / truth.len() as f64;
        let pct_within = within_bound as f64 / truth.len() as f64 * 100.0;

        eprintln!("\n  Error Distribution:");
        eprintln!("    Mean absolute error:  {mean_abs_error:.2}");
        eprintln!("    Max absolute error:   {max_abs_error}");
        eprintln!("    CMS error bound (eps*N): {error_bound:.2}");
        eprintln!(
            "    Within bound:         {within_bound}/{} ({pct_within:.1}%)",
            truth.len()
        );

        // Assertions: CMS guarantee — at least (1-delta) fraction within bound.
        let delta = 1.0 / std::f64::consts::E.powi(rows as i32);
        let expected_fraction = 1.0 - delta;
        assert!(
            pct_within / 100.0 > expected_fraction,
            "only {pct_within:.1}% within bound, expected > {:.1}%",
            expected_fraction * 100.0
        );
    }

    // -- Motivation scenario tests ------------------------------------------

    #[test]
    fn scenario_ddos_detection() {
        // Per-Source-IP Packet Counting (from motivation Example 3)
        let rows = 3;
        let cols = 4096;
        let fold_level = 4;

        let mut epoch1 = FoldCMS::new(rows, cols, fold_level, 5);
        epoch1.insert(&SketchInput::Str("192.168.1.1"), 50);
        epoch1.insert(&SketchInput::Str("10.0.0.42"), 10_000);
        epoch1.insert(&SketchInput::Str("172.16.5.99"), 30);
        epoch1.insert(&SketchInput::Str("10.0.0.43"), 8_000);

        let mut epoch2 = FoldCMS::new(rows, cols, fold_level, 5);
        epoch2.insert(&SketchInput::Str("192.168.1.1"), 45);
        epoch2.insert(&SketchInput::Str("10.0.0.42"), 15_000);
        epoch2.insert(&SketchInput::Str("172.16.5.99"), 25);
        epoch2.insert(&SketchInput::Str("10.0.0.43"), 200);

        let mut epoch3 = FoldCMS::new(rows, cols, fold_level, 5);
        epoch3.insert(&SketchInput::Str("192.168.1.1"), 60);
        epoch3.insert(&SketchInput::Str("10.0.0.42"), 12_000);
        epoch3.insert(&SketchInput::Str("172.16.5.99"), 9_000);
        epoch3.insert(&SketchInput::Str("10.0.0.43"), 100);

        // Hierarchical merge of 3 epochs (not a power of 2, tests carry-forward).
        let merged = FoldCMS::hierarchical_merge(&[epoch1, epoch2, epoch3]);

        let threshold = 15_000;
        let ip_42 = merged.query(&SketchInput::Str("10.0.0.42"));
        let ip_99 = merged.query(&SketchInput::Str("172.16.5.99"));
        let ip_43 = merged.query(&SketchInput::Str("10.0.0.43"));

        assert_eq!(ip_42, 37_000);
        assert!(ip_42 > threshold, "10.0.0.42 should exceed threshold");
        assert_eq!(ip_99, 9_055);
        assert!(ip_99 < threshold);
        assert_eq!(ip_43, 8_300);
        assert!(ip_43 < threshold);
    }
}
