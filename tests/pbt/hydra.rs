//! Property tests for Hydra: a multi-dimensional subpopulation sketch.
//!
//! Manousis et al., VLDB 2022.
//!
//! A record supplies one value per key column and fans out into the `2^D - 1`
//! non-empty subpopulations it belongs to; each subpopulation is hashed to one
//! column per row and a query takes the median of the row estimates.
//!
//! The oracle rebuilds that map from two things the sketch does not own: the
//! documented subkey encoding (`label ":" value` joined by `";"`, separators
//! escaped) and the public matrix hash at `HYDRA_SEED`. The counts it predicts
//! are a grouping of the generated stream, not a reading of the grid.
//!
//! Every record carries the same payload value, so each cell's Count-Min holds
//! a single key and its estimate is the exact mass the cell received. That
//! turns the grid laws into equalities instead of bounds.
//!
//! Two laws pin the algorithm between them: the grid holds exactly the writes
//! the encoding places, and a query answers a median of the cells its own
//! subkey names. Everything else about a subpopulation answer — that it never
//! falls below the records it counts, that a generalization never reads below
//! its specialization — follows from those two.
//!
//! `HydraKllSketch` is a second, unrelated grid in the portable module: no
//! schema and no fan-out, one string key routed by `xxh32(key, row)` and a
//! median of the row KLLs. Its accuracy is characterized in the end-to-end
//! quantile battery; the two laws here pin its routing and its reduction.

use asap_sketchlib::common::input::{HydraCounter, HydraQuery};
use asap_sketchlib::{
    CountMin, DataInput, FastPath, HYDRA_SEED, Hydra, HydraKllSketch, MatrixFastHash, Vector2D,
    hash_for_matrix_seeded,
};
use proptest::prelude::*;
use xxhash_rust::xxh32::xxh32;

/// The value every record carries.
const PAYLOAD: &str = "pkt";

/// A value no record carries.
const ABSENT: &str = "zzz";

/// Column labels, carrying the structural characters the encoding escapes.
const LABELS: [&str; 4] = ["src", "d;t", "a:b", "e\\f"];

/// One value domain shared by every column, so a value appears under several
/// labels and records nest inside each other's subpopulations. Four values
/// over up to four columns also guarantees repeats within a short stream.
const DOMAIN: [&str; 4] = ["x", "x;y", "", "p:q"];

fn payload() -> DataInput<'static> {
    DataInput::Str(PAYLOAD)
}

/// A per-cell counter small enough that a whole grid of them is cheap: the
/// default Count-Min is ~49 KB per cell.
fn counter() -> HydraCounter {
    HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::with_dimensions(2, 64))
}

fn escaped(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if matches!(ch, '\\' | ':' | ';') {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// The canonical subkey of `{ labels[i] = values[i] : bit i of mask is set }`,
/// in declaration order.
fn subkey(labels: &[String], values: &[&str], mask: u32) -> String {
    (0..labels.len())
        .filter(|col| (mask >> col) & 1 == 1)
        .map(|col| format!("{}:{}", escaped(&labels[col]), escaped(values[col])))
        .collect::<Vec<_>>()
        .join(";")
}

/// The column a subkey occupies on each row.
fn columns(rows: usize, cols: usize, key: &str) -> Vec<usize> {
    let hashed = hash_for_matrix_seeded(HYDRA_SEED, rows, cols, &DataInput::Str(key));
    (0..rows).map(|r| hashed.col_for_row(r, cols)).collect()
}

/// Writes every record makes, per cell, each weighted by the count its update
/// carried.
fn cell_mass<'a, I>(rows: usize, cols: usize, labels: &[String], records: I) -> Vec<Vec<i64>>
where
    I: IntoIterator<Item = (&'a [String], i64)>,
{
    let mut counts = vec![vec![0i64; cols]; rows];
    for (record, weight) in records {
        let values = borrow(record);
        for mask in 1u32..(1 << labels.len()) {
            for (row, col) in columns(rows, cols, &subkey(labels, &values, mask))
                .into_iter()
                .enumerate()
            {
                counts[row][col] += weight;
            }
        }
    }
    counts
}

/// Writes every record makes, per cell, one per update.
fn cell_counts(
    rows: usize,
    cols: usize,
    labels: &[String],
    records: &[Vec<String>],
) -> Vec<Vec<i64>> {
    cell_mass(
        rows,
        cols,
        labels,
        records.iter().map(|r| (r.as_slice(), 1)),
    )
}

fn borrow(record: &[String]) -> Vec<&str> {
    record.iter().map(String::as_str).collect()
}

/// One cell's exact write count, read through the Count-Min it holds.
fn cell(hydra: &Hydra, row: usize, col: usize) -> i64 {
    hydra
        .sketches
        .get(row, col)
        .expect("a cell inside the declared grid")
        .query(&HydraQuery::Frequency(payload()))
        .expect("Count-Min answers a frequency query") as i64
}

/// Asks for `{ labels[i] = values[i] : bit i of mask is set }`.
fn ask(hydra: &Hydra, values: &[&str], mask: u32) -> f64 {
    let key: Vec<Option<&str>> = (0..values.len())
        .map(|col| ((mask >> col) & 1 == 1).then_some(values[col]))
        .collect();
    hydra
        .query_frequency(&key, &payload())
        .expect("well-formed query")
}

fn filled(rows: usize, cols: usize, labels: &[String], records: &[Vec<String>]) -> Hydra {
    let mut hydra =
        Hydra::with_schema(rows, cols, labels.to_vec(), counter()).expect("valid schema");
    for record in records {
        hydra
            .update(&borrow(record), &payload(), None)
            .expect("schema arity");
    }
    hydra
}

/// Streams the records, each update carrying its own repeat count.
fn filled_weighted(
    rows: usize,
    cols: usize,
    labels: &[String],
    records: &[(Vec<String>, i32)],
) -> Hydra {
    let mut hydra =
        Hydra::with_schema(rows, cols, labels.to_vec(), counter()).expect("valid schema");
    for (record, count) in records {
        hydra
            .update(&borrow(record), &payload(), Some(*count))
            .expect("schema arity");
    }
    hydra
}

/// The subpopulations a query is asked for: every subset of every record's own
/// equalities, and every single-column equality over the shared domain — the
/// same value under each label in turn, plus one value no record carries.
fn probes(records: &[Vec<String>], arity: usize) -> Vec<(Vec<String>, u32)> {
    let mut probes = Vec::new();
    for record in records {
        for mask in 1u32..(1 << arity) {
            probes.push((record.clone(), mask));
        }
    }
    for col in 0..arity {
        for value in DOMAIN.iter().chain([&ABSENT]) {
            let mut values = vec![String::new(); arity];
            values[col] = (*value).to_string();
            probes.push((values, 1u32 << col));
        }
    }
    probes
}

/// A stream of records over `arity` key columns, drawn from the shared domain.
fn stream(arity: usize, max: usize) -> impl Strategy<Value = Vec<Vec<String>>> {
    prop::collection::vec(
        prop::collection::vec(
            prop::sample::select(DOMAIN.as_slice()).prop_map(|s| s.to_string()),
            arity,
        ),
        1..max,
    )
}

/// Two to four key columns and a stream over one shared value domain.
fn schema_and_records(max: usize) -> impl Strategy<Value = (Vec<String>, Vec<Vec<String>>)> {
    (2usize..=LABELS.len()).prop_flat_map(move |arity| {
        let labels: Vec<String> = LABELS[..arity].iter().map(|s| (*s).to_string()).collect();
        (Just(labels), stream(arity, max))
    })
}

/// A stream of records, each carrying the repeat count its update is given.
/// Counts stay positive so the per-cell Count-Min reading stays exact.
type WeightedRecords = Vec<(Vec<String>, i32)>;

fn schema_and_weighted_records(
    max: usize,
) -> impl Strategy<Value = (Vec<String>, WeightedRecords)> {
    (2usize..=LABELS.len()).prop_flat_map(move |arity| {
        let labels: Vec<String> = LABELS[..arity].iter().map(|s| (*s).to_string()).collect();
        let records = prop::collection::vec(
            (
                prop::collection::vec(
                    prop::sample::select(DOMAIN.as_slice()).prop_map(|s| s.to_string()),
                    arity,
                ),
                1i32..7,
            ),
            1..max,
        );
        (Just(labels), records)
    })
}

/// Two streams over one schema, so a merge and a single pass see the same key
/// columns.
type SchemaAndStreams = (Vec<String>, Vec<Vec<String>>, Vec<Vec<String>>);

fn schema_and_two_streams(max: usize) -> impl Strategy<Value = SchemaAndStreams> {
    (2usize..=LABELS.len()).prop_flat_map(move |arity| {
        let labels: Vec<String> = LABELS[..arity].iter().map(|s| (*s).to_string()).collect();
        (Just(labels), stream(arity, max), stream(arity, max))
    })
}

/// Column counts on both sides of the collision regime: a handful of columns
/// forces subkeys to share cells, a few hundred usually keeps them apart.
fn grid_cols() -> impl Strategy<Value = usize> {
    prop_oneof![
        Just(1usize),
        Just(2),
        Just(3),
        Just(8),
        Just(64),
        Just(251),
        Just(256),
    ]
}

/// The compaction-coin seed every portable grid is built with, so a cell's
/// answer is a function of its stream alone.
const KLL_SEED: u64 = 0x5EED_2000;

/// Wide enough that the streams below are retained whole, so no cell compacts.
const KLL_K: u16 = 256;

/// Keys the portable grid is streamed and probed with. Few and short, so a
/// narrow grid makes them share cells.
const KLL_KEYS: [&str; 5] = ["a", "b", "c", "", "a:b"];

/// The column a portable key occupies on each row.
fn kll_columns(rows: usize, cols: usize, key: &str) -> Vec<usize> {
    (0..rows)
        .map(|row| xxh32(key.as_bytes(), row as u32) as usize % cols)
        .collect()
}

/// A stream of `(key index, value)` for the portable grid.
fn kll_stream(max: usize) -> impl Strategy<Value = Vec<(usize, f64)>> {
    prop::collection::vec((0usize..KLL_KEYS.len(), 0.0f64..1000.0), 1..max)
}

fn kll_filled(rows: usize, cols: usize, stream: &[(usize, f64)]) -> HydraKllSketch {
    let mut grid = HydraKllSketch::with_seed(rows, cols, KLL_K, KLL_SEED);
    for (key, value) in stream {
        grid.update(KLL_KEYS[*key], *value);
    }
    grid
}

proptest! {
    // ===== Each subpopulation lands in the cell its labelled subkey names =====
    //
    // The whole write side at once: each record fans out into the `2^D - 1`
    // non-empty subsets of its equalities, each subset is hashed under its own
    // column labels, and nothing else reaches the grid. A row's total is
    // `N * (2^D - 1)`, the mass the accuracy note's collision term is drawn
    // from, and it is the sum of this law's own cells.

    #[test]
    fn hydra_writes_each_subpopulation_into_the_cell_its_labelled_subkey_names(
        rows in 1usize..6,
        cols in grid_cols(),
        (labels, records) in schema_and_records(10),
    ) {
        let hydra = filled(rows, cols, &labels, &records);
        let oracle = cell_counts(rows, cols, &labels, &records);

        for (r, row) in oracle.iter().enumerate() {
            for (c, want) in row.iter().enumerate() {
                prop_assert_eq!(
                    cell(&hydra, r, c), *want,
                    "cell ({}, {}) of a {}x{} grid over {:?}", r, c, rows, cols, labels
                );
            }
        }
    }

    // ===== An update's count is the weight it adds, not a write of one =======
    //
    // The law above streams every record once, so it holds equally for an
    // implementation that discards `count` and writes one per update. Counts
    // are positive and the cell reading stays exact, so this is the same
    // equality over a weighted stream.

    #[test]
    fn hydra_adds_the_count_each_update_carries(
        rows in 1usize..6,
        cols in grid_cols(),
        (labels, records) in schema_and_weighted_records(8),
    ) {
        let hydra = filled_weighted(rows, cols, &labels, &records);
        let oracle = cell_mass(
            rows, cols, &labels,
            records.iter().map(|(record, count)| (record.as_slice(), i64::from(*count))),
        );

        for (r, row) in oracle.iter().enumerate() {
            for (c, want) in row.iter().enumerate() {
                prop_assert_eq!(
                    cell(&hydra, r, c), *want,
                    "cell ({}, {}) of a {}x{} grid over {:?}", r, c, rows, cols, labels
                );
            }
        }
    }

    // ===== An answer is a median of the cells the query's own subkey names ====
    //
    // The read side at once: which cells a subpopulation is read from, and how
    // the row estimates are reduced. A median is asserted as an order
    // statistic — at least half the rows at or above the answer and at least
    // half at or below — which is what "median" fixes; the tie rule for an even
    // row count is not part of the contract. Taking the minimum or the maximum
    // of the rows instead satisfies every bound a subpopulation answer obeys,
    // so nothing but this catches it.

    #[test]
    fn hydra_answers_a_median_of_the_cells_its_labelled_subkey_names(
        rows in 1usize..6,
        cols in grid_cols(),
        (labels, records) in schema_and_records(8),
    ) {
        let hydra = filled(rows, cols, &labels, &records);

        for (values, mask) in probes(&records, labels.len()) {
            let values = borrow(&values);
            let cols_of = columns(rows, cols, &subkey(&labels, &values, mask));
            let row_estimates: Vec<i64> = cols_of
                .iter()
                .enumerate()
                .map(|(r, c)| cell(&hydra, r, *c))
                .collect();

            let seen = ask(&hydra, &values, mask);
            let at_or_below = row_estimates.iter().filter(|e| **e as f64 <= seen).count();
            let at_or_above = row_estimates.iter().filter(|e| **e as f64 >= seen).count();
            prop_assert!(
                2 * at_or_below >= rows && 2 * at_or_above >= rows,
                "mask {:#b} over {:?} read {}, no median of its row estimates {:?}",
                mask, values, seen, row_estimates
            );
        }
    }

    // ===== Merging two grids is streaming their concatenation =====
    //
    // Cell by cell, not query by query: a query reads a median over rows, so a
    // row left unmerged can still be outvoted.

    #[test]
    fn hydra_merge_matches_streaming_the_concatenation(
        rows in 1usize..6,
        cols in grid_cols(),
        (labels, left, right) in schema_and_two_streams(6),
    ) {
        let mut merged = filled(rows, cols, &labels, &left);
        merged
            .merge(&filled(rows, cols, &labels, &right))
            .expect("one schema and one geometry");

        let concatenated: Vec<Vec<String>> = left.iter().chain(&right).cloned().collect();
        let streamed = filled(rows, cols, &labels, &concatenated);

        for r in 0..rows {
            for c in 0..cols {
                prop_assert_eq!(
                    cell(&merged, r, c), cell(&streamed, r, c),
                    "cell ({}, {}) of a {}x{} grid over {:?}", r, c, rows, cols, labels
                );
            }
        }
    }

    // ===== The portable grid routes each row by its own seeded hash =========
    //
    // `HydraKllSketch` carries no schema and does not fan out: one key reaches
    // one cell per row, at `xxh32(key, row) % cols`. The oracle counts the
    // stream into that map and reads each cell's retained count back, so a row
    // seeded alike as its neighbours — or a routing that ignores the row — is
    // an unequal count rather than a merely worse estimate.

    #[test]
    fn portable_hydra_kll_routes_each_update_to_the_cell_its_row_hash_names(
        rows in 1usize..5,
        cols in 1usize..9,
        stream in kll_stream(24),
    ) {
        let grid = kll_filled(rows, cols, &stream);

        let mut oracle = vec![vec![0u64; cols]; rows];
        for (key, _) in &stream {
            for (row, col) in kll_columns(rows, cols, KLL_KEYS[*key]).into_iter().enumerate() {
                oracle[row][col] += 1;
            }
        }

        for (r, row) in oracle.iter().enumerate() {
            for (c, want) in row.iter().enumerate() {
                prop_assert_eq!(
                    grid.sketch[r][c].count(), *want,
                    "cell ({}, {}) of a {}x{} portable grid", r, c, rows, cols
                );
            }
        }
    }

    // ===== The portable grid answers a median of its row cells ==============
    //
    // As above, an order statistic: the tie rule for an even row count is not
    // part of the contract. An empty cell answers 0, which is below every value
    // the stream carries, so a query for an absent key is a real probe of the
    // reduction rather than a vacuous one.

    #[test]
    fn portable_hydra_kll_answers_a_median_of_its_row_cells(
        rows in 1usize..5,
        cols in 1usize..9,
        stream in kll_stream(24),
        q in 0.0f64..=1.0,
    ) {
        let grid = kll_filled(rows, cols, &stream);

        for key in KLL_KEYS {
            let row_estimates: Vec<f64> = kll_columns(rows, cols, key)
                .into_iter()
                .enumerate()
                .map(|(r, c)| grid.sketch[r][c].quantile(q))
                .collect();

            let seen = grid.quantile(key, q);
            let at_or_below = row_estimates.iter().filter(|e| **e <= seen).count();
            let at_or_above = row_estimates.iter().filter(|e| **e >= seen).count();
            prop_assert!(
                2 * at_or_below >= rows && 2 * at_or_above >= rows,
                "key {:?} at q {} read {}, no median of its row estimates {:?}",
                key, q, seen, row_estimates
            );
        }
    }

    // ===== Wire =====
    //
    // A grid of counters cut to size costs no more to build than any other
    // sketch here; the default per-cell Count-Min is what is expensive.

    #[test]
    fn hydra_round_trips_through_its_asapv1_envelope(
        rows in 1usize..6,
        cols in grid_cols(),
        (labels, records) in schema_and_records(8),
    ) {
        let hydra = filled(rows, cols, &labels, &records);
        let arity = labels.len();

        let mut probes: Vec<Vec<Option<String>>> = Vec::new();
        for record in &records {
            for mask in 1u32..(1 << arity) {
                probes.push(
                    (0..arity)
                        .map(|col| ((mask >> col) & 1 == 1).then(|| record[col].clone()))
                        .collect(),
                );
            }
        }
        probes.push((0..arity).map(|col| (col == 0).then(|| ABSENT.to_string())).collect());

        round_trip!(
            Hydra,
            hydra,
            |h: &Hydra| probes
                .iter()
                .map(|probe| {
                    let key: Vec<Option<&str>> = probe.iter().map(Option::as_deref).collect();
                    h.query_frequency(&key, &payload()).expect("well-formed query")
                })
                .collect::<Vec<f64>>(),
            |h: &Hydra| h.schema().to_vec(),
        );
    }
}
