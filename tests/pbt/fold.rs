//! Property tests for `FoldCMS` and `FoldCS`.
//!
//! The family's claim is an exact equality: folding introduces no additional
//! approximation error, so a query must not depend on the fold level.
//!
//! The point-query oracle is the sketch definition over an exact `HashMap` of
//! the stream: a counter holds the summed frequency of the keys addressing it,
//! the Count-Min estimate is the smallest counter across the rows, the Count
//! Sketch estimate the median of the sign-corrected ones. The address a key
//! takes in a row is read off a level-0 sketch carrying that key alone, through
//! `cell` and `iter` rather than through any of the folding paths under test.
//!
//! The `FoldCell` laws run against a `Vec` of the distinct columns inserted:
//! the count of those columns is `entry_count`, and their number fixes which
//! of the three representations the cell is allowed to be in.
//!
//! Cormode and Muthukrishnan, J. Algorithms '05; Charikar, Chen and
//! Farach-Colton, ICALP '02.

use crate::support::grid;
use asap_sketchlib::{DataInput, FoldCMS, FoldCS, FoldCell};
use proptest::prelude::*;
use std::collections::HashMap;

/// Heap capacity every sketch here is built with. The heavy-hitter heap rides
/// along on insert and merge; these laws read the counters.
const TOP_K: usize = 8;

/// A key domain far wider than the narrowest folded grid, so distinct full
/// columns share a physical cell and the `Collided` representation is reached.
fn updates(max: usize) -> impl Strategy<Value = Vec<(u64, i64)>> {
    prop::collection::vec((0u64..48, 1i64..20), 0..max)
}

/// Rows, a power-of-two full width, and a fold level the width admits.
fn geometry() -> impl Strategy<Value = (usize, usize, u32)> {
    (2usize..=4, 3u32..=6)
        .prop_flat_map(|(rows, exponent)| (Just(rows), Just(1usize << exponent), 0u32..=exponent))
}

/// The same, with the level held above zero: what `unfold_merge` needs.
fn folded_geometry() -> impl Strategy<Value = (usize, usize, u32)> {
    (2usize..=4, 3u32..=6)
        .prop_flat_map(|(rows, exponent)| (Just(rows), Just(1usize << exponent), 1u32..=exponent))
}

/// A geometry at fold level `k` paired with `2^k` sub-window streams, the shape
/// a balanced tree of `unfold_merge` calls consumes.
type Windows = (usize, usize, u32, Vec<Vec<(u64, i64)>>);

fn windows() -> impl Strategy<Value = Windows> {
    (2usize..=4, 3u32..=6)
        .prop_flat_map(|(rows, exponent)| {
            (Just(rows), Just(1usize << exponent), 1u32..=exponent.min(3))
        })
        .prop_flat_map(|(rows, full_cols, level)| {
            (
                Just(rows),
                Just(full_cols),
                Just(level),
                prop::collection::vec(updates(24), 1usize << level),
            )
        })
}

/// Every key the stream carried, and three it never did.
fn probes(stream: &[(u64, i64)]) -> Vec<u64> {
    let mut keys: Vec<u64> = stream.iter().map(|(k, _)| *k).collect();
    keys.extend([1_000u64, 1_001, 1_002]);
    keys.sort_unstable();
    keys.dedup();
    keys
}

fn truth_of(stream: &[(u64, i64)]) -> HashMap<u64, i64> {
    let mut counts: HashMap<u64, i64> = HashMap::new();
    for (key, delta) in stream {
        *counts.entry(*key).or_default() += delta;
    }
    counts
}

fn cms_of(rows: usize, full_cols: usize, level: u32, stream: &[(u64, i64)]) -> FoldCMS {
    let mut sketch = FoldCMS::new(rows, full_cols, level, TOP_K);
    for (key, delta) in stream {
        sketch.insert(&DataInput::U64(*key), *delta);
    }
    sketch
}

fn cs_of(rows: usize, full_cols: usize, level: u32, stream: &[(u64, i64)]) -> FoldCS {
    let mut sketch = FoldCS::new(rows, full_cols, level, TOP_K);
    for (key, delta) in stream {
        sketch.insert(&DataInput::U64(*key), *delta);
    }
    sketch
}

/// A cell's `(full_col, count)` pairs, sorted, so two cells compare as the
/// multisets they are: the order entries sit in is the implementation's.
fn cell_entries(cell: &FoldCell) -> Vec<(u16, i64)> {
    let mut entries: Vec<(u16, i64)> = cell.iter().collect();
    entries.sort_unstable();
    entries
}

fn cms_cells(sketch: &FoldCMS) -> Vec<Vec<(u16, i64)>> {
    grid(sketch.rows(), sketch.fold_cols(), |r, c| {
        cell_entries(sketch.cell(r, c))
    })
}

fn cs_cells(sketch: &FoldCS) -> Vec<Vec<(u16, i64)>> {
    grid(sketch.rows(), sketch.fold_cols(), |r, c| {
        cell_entries(sketch.cell(r, c))
    })
}

fn same_cells(
    left: &[Vec<(u16, i64)>],
    right: &[Vec<(u16, i64)>],
    cols: usize,
    what: &str,
) -> Result<(), TestCaseError> {
    prop_assert_eq!(left.len(), right.len(), "{}: grid shape", what);
    for (i, (a, b)) in left.iter().zip(right).enumerate() {
        prop_assert_eq!(a, b, "{}: cell at row {} col {}", what, i / cols, i % cols);
    }
    Ok(())
}

/// The single entry each row of a one-key level-0 sketch holds: the column that
/// key addresses, and the value the row signs it with.
fn row_marks(cells: &[Vec<(u16, i64)>], full_cols: usize) -> Vec<(u16, i64)> {
    cells
        .chunks(full_cols)
        .map(|row| {
            let marked: Vec<(u16, i64)> = row.iter().flatten().copied().collect();
            assert_eq!(marked.len(), 1, "one key marks one column per row");
            marked[0]
        })
        .collect()
}

fn cms_marks(rows: usize, full_cols: usize, key: u64) -> Vec<(u16, i64)> {
    row_marks(
        &cms_cells(&cms_of(rows, full_cols, 0, &[(key, 1)])),
        full_cols,
    )
}

fn cs_marks(rows: usize, full_cols: usize, key: u64) -> Vec<(u16, i64)> {
    row_marks(
        &cs_cells(&cs_of(rows, full_cols, 0, &[(key, 1)])),
        full_cols,
    )
}

/// Counter `(r, c)` is the summed frequency of the keys row `r` sends to `c`.
fn cms_column_sums(rows: usize, full_cols: usize, truth: &HashMap<u64, i64>) -> Vec<i64> {
    let mut out = vec![0i64; rows * full_cols];
    for (key, count) in truth {
        for (r, (col, _)) in cms_marks(rows, full_cols, *key).into_iter().enumerate() {
            out[r * full_cols + col as usize] += count;
        }
    }
    out
}

/// The same sum, each term carrying the row's sign for its key.
fn cs_column_sums(rows: usize, full_cols: usize, truth: &HashMap<u64, i64>) -> Vec<i64> {
    let mut out = vec![0i64; rows * full_cols];
    for (key, count) in truth {
        for (r, (col, sign)) in cs_marks(rows, full_cols, *key).into_iter().enumerate() {
            out[r * full_cols + col as usize] += sign * count;
        }
    }
    out
}

// ===== Laws shared by both fold families =====

macro_rules! fold_laws {
    ($name:ident, $ty:ident, $sketch_of:ident, $cells_of:ident) => {
        mod $name {
            use super::*;

            proptest! {
                // The documented claim. A folded sub-window answers what the
                // full-width sketch over the same stream answers, at every
                // level the width admits.
                #[test]
                fn every_fold_level_answers_the_same_query(
                    (rows, full_cols, _) in geometry(),
                    stream in updates(60),
                ) {
                    let flat = $sketch_of(rows, full_cols, 0, &stream);
                    let asked = probes(&stream);

                    for level in 1..=full_cols.trailing_zeros() {
                        let folded = $sketch_of(rows, full_cols, level, &stream);
                        prop_assert_eq!(folded.fold_cols(), full_cols >> level);
                        for key in &asked {
                            prop_assert_eq!(
                                folded.query(&DataInput::U64(*key)),
                                flat.query(&DataInput::U64(*key)),
                                "key {} at fold level {}", key, level
                            );
                        }
                    }
                }

                #[test]
                fn unfolding_preserves_every_query(
                    (rows, full_cols, level) in geometry(),
                    stream in updates(60),
                ) {
                    let sketch = $sketch_of(rows, full_cols, level, &stream);
                    let asked = probes(&stream);

                    for target in 0..=level {
                        let unfolded = sketch.unfold_to(target);
                        prop_assert_eq!(unfolded.fold_level(), target);
                        prop_assert_eq!(unfolded.fold_cols(), full_cols >> target);
                        for key in &asked {
                            prop_assert_eq!(
                                unfolded.query(&DataInput::U64(*key)),
                                sketch.query(&DataInput::U64(*key)),
                                "key {} unfolded from {} to {}", key, level, target
                            );
                        }
                    }
                }

                // A row stores one counter per full column, wherever folding
                // put it. This is what makes the fold lossless: an entry is an
                // address, so no two of them can be added together by mistake.
                #[test]
                fn a_full_column_is_stored_once_per_row(
                    (rows, full_cols, level) in geometry(),
                    stream in updates(60),
                ) {
                    let sketch = $sketch_of(rows, full_cols, level, &stream);
                    let cells = $cells_of(&sketch);

                    for (r, row) in cells.chunks(sketch.fold_cols()).enumerate() {
                        let mut columns: Vec<u16> =
                            row.iter().flatten().map(|(col, _)| *col).collect();
                        let held = columns.len();
                        columns.sort_unstable();
                        columns.dedup();
                        prop_assert_eq!(
                            columns.len(), held,
                            "row {} holds {} entries over {} columns at fold level {}",
                            r, held, columns.len(), level
                        );
                    }
                }
            }

            proptest! {
                #![proptest_config(ProptestConfig::with_cases(64))]

                // Every entry carries its own full-width address, so a descent
                // through an intermediate level lands where the direct one
                // does. `unfold_full` is the descent to zero.
                #[test]
                fn unfolding_in_two_steps_lands_where_one_step_does(
                    (rows, full_cols, level) in geometry(),
                    stream in updates(40),
                ) {
                    let sketch = $sketch_of(rows, full_cols, level, &stream);

                    for high in 0..=level {
                        for low in 0..=high {
                            let stepwise = sketch.unfold_to(high).unfold_to(low);
                            let direct = sketch.unfold_to(low);
                            same_cells(
                                &$cells_of(&stepwise),
                                &$cells_of(&direct),
                                full_cols >> low,
                                &format!("unfolded from {level} via {high} to {low}"),
                            )?;
                        }
                    }

                    same_cells(
                        &$cells_of(&sketch.unfold_full()),
                        &$cells_of(&sketch.unfold_to(0)),
                        full_cols,
                        "unfold_full against unfold_to(0)",
                    )?;
                }

                // `unfold_merge` is the same-level merge followed by one level
                // of widening, fused into a single scatter.
                #[test]
                fn unfold_merge_is_a_same_level_merge_then_one_unfold(
                    (rows, full_cols, level) in folded_geometry(),
                    left in updates(40),
                    right in updates(40),
                ) {
                    let a = $sketch_of(rows, full_cols, level, &left);
                    let b = $sketch_of(rows, full_cols, level, &right);

                    let fused = $ty::unfold_merge(&a, &b);
                    prop_assert_eq!(fused.fold_level(), level - 1);
                    prop_assert_eq!(fused.fold_cols(), full_cols >> (level - 1));

                    let mut stepwise = a.clone();
                    stepwise.merge_same_level(&b);
                    let stepwise = stepwise.unfold_to(level - 1);

                    same_cells(
                        &$cells_of(&fused),
                        &$cells_of(&stepwise),
                        full_cols >> (level - 1),
                        &format!("unfold_merge at level {level}"),
                    )?;
                }

                // The N-way scatter and the balanced tree of pairwise unfolds
                // reach the same level-0 grid, and that grid answers what one
                // full-width sketch over the concatenated sub-windows answers.
                #[test]
                fn hierarchical_merge_matches_a_tree_of_unfold_merges(
                    (rows, full_cols, level, sub_windows) in windows(),
                ) {
                    let sketches: Vec<_> = sub_windows
                        .iter()
                        .map(|w| $sketch_of(rows, full_cols, level, w))
                        .collect();

                    let merged = $ty::hierarchical_merge(&sketches);
                    prop_assert_eq!(merged.fold_level(), 0);
                    prop_assert_eq!(merged.fold_cols(), full_cols);

                    let mut tier = sketches;
                    while tier.len() > 1 {
                        tier = tier
                            .chunks(2)
                            .map(|pair| $ty::unfold_merge(&pair[0], &pair[1]))
                            .collect();
                    }
                    same_cells(
                        &$cells_of(&merged),
                        &$cells_of(&tier[0]),
                        full_cols,
                        &format!("{} sub-windows from level {level}", sub_windows.len()),
                    )?;

                    let concatenated: Vec<(u64, i64)> =
                        sub_windows.iter().flatten().copied().collect();
                    let whole = $sketch_of(rows, full_cols, 0, &concatenated);
                    prop_assert_eq!(merged.to_flat_counters(), whole.to_flat_counters());
                    for key in probes(&concatenated) {
                        prop_assert_eq!(
                            merged.query(&DataInput::U64(key)),
                            whole.query(&DataInput::U64(key)),
                            "key {} after merging {} sub-windows", key, sub_windows.len()
                        );
                    }
                }
            }
        }
    };
}

fold_laws!(cms, FoldCMS, cms_of, cms_cells);
fold_laws!(cs, FoldCS, cs_of, cs_cells);

// ===== Count-Min: the counters and the minimum over them =====

proptest! {
    // The counter matrix a folded sketch flattens to is the one a Count-Min
    // Sketch of that width holds: each cell the summed frequency of the keys
    // the row addresses to it.
    #[test]
    fn the_flat_counters_are_the_column_sums_of_the_stream(
        (rows, full_cols, level) in geometry(),
        stream in updates(60),
    ) {
        let sketch = cms_of(rows, full_cols, level, &stream);
        let expected = cms_column_sums(rows, full_cols, &truth_of(&stream));
        let flat = sketch.to_flat_counters();

        prop_assert_eq!(flat.len(), expected.len());
        for (i, (got, want)) in flat.iter().zip(&expected).enumerate() {
            prop_assert_eq!(
                got, want,
                "counter at row {} col {} from fold level {}",
                i / full_cols, i % full_cols, level
            );
        }
    }

    // The Count-Min point query: the smallest of the counters the key addresses.
    #[test]
    fn the_estimate_is_the_smallest_counter_the_key_addresses(
        (rows, full_cols, level) in geometry(),
        stream in updates(60),
    ) {
        let sketch = cms_of(rows, full_cols, level, &stream);
        let counters = cms_column_sums(rows, full_cols, &truth_of(&stream));

        for key in probes(&stream) {
            let marks = cms_marks(rows, full_cols, key);
            let expected = (0..rows)
                .map(|r| counters[r * full_cols + marks[r].0 as usize])
                .min()
                .expect("at least two rows");
            prop_assert_eq!(
                sketch.query(&DataInput::U64(key)),
                expected,
                "key {} at columns {:?} from fold level {}",
                key, marks.iter().map(|m| m.0).collect::<Vec<_>>(), level
            );
        }
    }

    // A non-negative stream only ever piles other keys' weight onto a counter,
    // so the estimate is one-sided.
    #[test]
    fn the_estimate_never_falls_below_the_true_count(
        (rows, full_cols, level) in geometry(),
        stream in updates(60),
    ) {
        let sketch = cms_of(rows, full_cols, level, &stream);

        for (key, count) in truth_of(&stream) {
            prop_assert!(
                sketch.query(&DataInput::U64(key)) >= count,
                "key {}: estimate {} below the true {} at fold level {}",
                key, sketch.query(&DataInput::U64(key)), count, level
            );
        }
    }
}

// ===== Count Sketch: the signed counters and the median over them =====

/// Odd row counts, where the median is an element of the row estimates rather
/// than a midpoint between two.
fn odd_geometry() -> impl Strategy<Value = (usize, usize, u32)> {
    (prop_oneof![Just(3usize), Just(5usize)], 3u32..=6)
        .prop_flat_map(|(rows, exponent)| (Just(rows), Just(1usize << exponent), 0u32..=exponent))
}

/// What each row estimates for `key`: the counter it addresses, corrected by
/// the sign that row gave the key.
fn cs_row_estimates(
    rows: usize,
    full_cols: usize,
    counters: &[i64],
    key: u64,
) -> Vec<(u16, i64, i64)> {
    cs_marks(rows, full_cols, key)
        .into_iter()
        .enumerate()
        .map(|(r, (col, sign))| (col, sign, sign * counters[r * full_cols + col as usize]))
        .collect()
}

proptest! {
    #[test]
    fn the_flat_counters_are_the_signed_column_sums_of_the_stream(
        (rows, full_cols, level) in geometry(),
        stream in updates(60),
    ) {
        let sketch = cs_of(rows, full_cols, level, &stream);
        let expected = cs_column_sums(rows, full_cols, &truth_of(&stream));
        let flat = sketch.to_flat_counters();

        prop_assert_eq!(flat.len(), expected.len());
        for (i, (got, want)) in flat.iter().zip(&expected).enumerate() {
            prop_assert_eq!(
                got, want,
                "counter at row {} col {} from fold level {}",
                i / full_cols, i % full_cols, level
            );
        }
    }

    // A median sits inside the range of what it summarises, whatever the row
    // count's parity leaves the midpoint to.
    #[test]
    fn the_estimate_stays_within_the_row_estimates(
        (rows, full_cols, level) in geometry(),
        stream in updates(60),
    ) {
        let sketch = cs_of(rows, full_cols, level, &stream);
        let counters = cs_column_sums(rows, full_cols, &truth_of(&stream));

        for key in probes(&stream) {
            let per_row = cs_row_estimates(rows, full_cols, &counters, key);
            let values: Vec<i64> = per_row.iter().map(|(_, _, est)| *est).collect();
            let estimate = sketch.query(&DataInput::U64(key));
            prop_assert!(
                estimate >= *values.iter().min().expect("at least two rows")
                    && estimate <= *values.iter().max().expect("at least two rows"),
                "key {}: estimate {} outside the row estimates {:?} at fold level {}",
                key, estimate, per_row, level
            );
        }
    }

    // With an odd row count the median is the middle row estimate exactly.
    #[test]
    fn the_estimate_is_the_middle_row_estimate(
        (rows, full_cols, level) in odd_geometry(),
        stream in updates(60),
    ) {
        let sketch = cs_of(rows, full_cols, level, &stream);
        let counters = cs_column_sums(rows, full_cols, &truth_of(&stream));

        for key in probes(&stream) {
            let per_row = cs_row_estimates(rows, full_cols, &counters, key);
            let mut values: Vec<i64> = per_row.iter().map(|(_, _, est)| *est).collect();
            values.sort_unstable();
            prop_assert_eq!(
                sketch.query(&DataInput::U64(key)),
                values[rows / 2],
                "key {}: row estimates {:?} at fold level {}",
                key, per_row, level
            );
        }
    }

    // The column and the sign come from the key and the row alone. Folding
    // chooses the physical cell an entry sits in and nothing else, so an entry
    // reads the same at every level and after any unfold.
    #[test]
    fn a_key_keeps_its_columns_and_signs_at_every_fold_level(
        (rows, full_cols, level) in geometry(),
        key in 0u64..48,
    ) {
        let expected = cs_marks(rows, full_cols, key);
        let folded = cs_of(rows, full_cols, level, &[(key, 1)]);

        prop_assert_eq!(
            row_marks(&cs_cells(&folded), full_cols >> level),
            expected.clone(),
            "key {} at fold level {}", key, level
        );
        prop_assert_eq!(
            row_marks(&cs_cells(&folded.unfold_to(0)), full_cols),
            expected,
            "key {} unfolded from level {}", key, level
        );
    }
}

// ===== FoldCell =====

/// Columns drawn from a domain of four, so a handful of inserts reaches the
/// second distinct one and the representation has to upgrade.
fn cell_inserts(max: usize) -> impl Strategy<Value = Vec<(u16, i64)>> {
    prop::collection::vec((0u16..4, -20i64..20), 0..max)
}

/// The distinct columns a cell was handed, each with its running sum.
fn cell_model(inserts: &[(u16, i64)]) -> Vec<(u16, i64)> {
    let mut model: Vec<(u16, i64)> = Vec::new();
    for (col, delta) in inserts {
        match model.iter_mut().find(|(c, _)| c == col) {
            Some(entry) => entry.1 += delta,
            None => model.push((*col, *delta)),
        }
    }
    model
}

fn holds_model(cell: &FoldCell, model: &[(u16, i64)], what: &str) -> Result<(), TestCaseError> {
    prop_assert_eq!(
        cell.entry_count(),
        model.len(),
        "{}: {} entries over {} distinct columns",
        what,
        cell.entry_count(),
        model.len()
    );
    prop_assert_eq!(cell.is_empty(), model.is_empty(), "{}: emptiness", what);

    let upgraded = match (cell, model.len()) {
        (FoldCell::Empty, 0) => true,
        (FoldCell::Single { .. }, 1) => true,
        (FoldCell::Collided(_), n) => n >= 2,
        _ => false,
    };
    prop_assert!(
        upgraded,
        "{}: {} distinct columns held as {:?}",
        what,
        model.len(),
        cell
    );

    for (col, count) in model {
        prop_assert_eq!(cell.query(*col), *count, "{}: column {}", what, col);
    }
    prop_assert_eq!(cell.query(9), 0, "{}: a column never inserted", what);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    // The representation is a function of how many distinct columns the cell
    // holds: a repeat of the column already there accumulates in place and
    // leaves the cell `Single`.
    #[test]
    fn a_cell_upgrades_only_on_a_second_distinct_column(inserts in cell_inserts(40)) {
        let mut cell = FoldCell::Empty;
        prop_assert!(cell.is_empty());
        prop_assert_eq!(cell.entry_count(), 0);

        for (step, (col, delta)) in inserts.iter().enumerate() {
            cell.insert(*col, *delta);
            let model = cell_model(&inserts[..=step]);
            holds_model(&cell, &model, &format!("after {} inserts", step + 1))?;
        }
    }

    // Merging is addition on the columns: nothing dropped, nothing counted
    // twice, and the two sides reach the same cell either way round.
    #[test]
    fn merging_cells_sums_them_column_by_column(
        left in cell_inserts(20),
        right in cell_inserts(20),
    ) {
        let mut merged = FoldCell::Empty;
        for (col, delta) in &left {
            merged.insert(*col, *delta);
        }
        let mut other = FoldCell::Empty;
        for (col, delta) in &right {
            other.insert(*col, *delta);
        }
        merged.merge_from(&other);

        let concatenated: Vec<(u16, i64)> = left.iter().chain(&right).copied().collect();
        holds_model(&merged, &cell_model(&concatenated), "left merged with right")?;

        let mut reversed = other;
        let mut first = FoldCell::Empty;
        for (col, delta) in &left {
            first.insert(*col, *delta);
        }
        reversed.merge_from(&first);
        holds_model(&reversed, &cell_model(&concatenated), "right merged with left")?;
    }
}
