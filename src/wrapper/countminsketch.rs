//! Wire-format-aligned Count-Min sketch types.

use rmp_serde::encode::Error as RmpEncodeError;

use crate::message_pack_format::{Error as MsgPackError, MessagePackCodec};
use crate::sketches::countminsketch::CountMin;
use crate::{DataInput, MatrixStorage, RegularPath, Vector2D};

// =====================================================================
// asap_sketchlib wire-format-aligned variant.
//
// `CountMinSketch` and `CountMinDelta` below are the public-field,
// proto-decode-friendly types consumed by the ASAP query engine
// accumulators, backed by `asap_sketchlib`'s in-tree CountMin. The
// high-throughput in-process variant above (`CountMin`) keeps its
// original design.
// =====================================================================

// (de-duplicated) use serde::{Deserialize, Serialize};

// ----- asap_sketchlib-backed Count-Min helpers -----
// Used below by `CountMinSketch`. Lives in this file so the wire-format
// type and its backend share a single home.

/// Concrete Count-Min type backing the wire-format `CountMinSketch`.
/// Uses f64 counters (`Vector2D<f64>`) for weighted updates without integer rounding.
pub type SketchlibCms = CountMin<Vector2D<f64>, RegularPath>;

/// Creates a fresh sketchlib Count-Min sketch with the given dimensions.
pub fn new_sketchlib_cms(row_num: usize, col_num: usize) -> SketchlibCms {
    SketchlibCms::with_dimensions(row_num, col_num)
}

/// Builds a sketchlib Count-Min sketch from an existing `sketch` matrix.
pub fn sketchlib_cms_from_matrix(
    row_num: usize,
    col_num: usize,
    sketch: &[Vec<f64>],
) -> SketchlibCms {
    let matrix = Vector2D::from_fn(row_num, col_num, |r, c| {
        sketch
            .get(r)
            .and_then(|row| row.get(c))
            .copied()
            .unwrap_or(0.0)
    });
    SketchlibCms::from_storage(matrix)
}

/// Converts a sketchlib Count-Min sketch into a `Vec<Vec<f64>>` matrix.
pub fn matrix_from_sketchlib_cms(inner: &SketchlibCms) -> Vec<Vec<f64>> {
    let storage: &Vector2D<f64> = inner.as_storage();
    let rows = storage.rows();
    let cols = storage.cols();
    let mut sketch = vec![vec![0.0; cols]; rows];

    for (r, row) in sketch.iter_mut().enumerate().take(rows) {
        for (c, cell) in row.iter_mut().enumerate().take(cols) {
            if let Some(v) = storage.get(r, c) {
                *cell = *v;
            }
        }
    }

    sketch
}

/// Helper to update a sketchlib Count-Min with a weighted key.
pub fn sketchlib_cms_update(inner: &mut SketchlibCms, key: &str, value: f64) {
    if value <= 0.0 {
        return;
    }
    let input = DataInput::String(key.to_owned());
    inner.insert_many(&input, value);
}

/// Helper to query a sketchlib Count-Min for a key, returning f64.
pub fn sketchlib_cms_query(inner: &SketchlibCms, key: &str) -> f64 {
    let input = DataInput::String(key.to_owned());
    inner.estimate(&input)
}

/// Sparse delta between two consecutive CountMinSketch snapshots —
/// the input shape for [`CountMinSketch::apply_delta`]. Mirrors the
/// `CountMinSketchDelta` proto in
/// `sketchlib-go/proto/countminsketch/countminsketch.proto` (packed
/// encoding only).
///
/// Cells apply additively: `matrix[row][col] += d_count`. Per-row
/// L1 and L2 norm deltas are carried for downstream error-accounting
/// but are not consumed by `apply_delta` itself.
#[derive(Debug, Clone, Default)]
pub struct CountMinSketchDelta {
    pub rows: u32,
    pub cols: u32,
    pub cells: Vec<(u32, u32, i64)>,
    pub l1: Vec<f64>,
    pub l2: Vec<f64>,
}

/// Provides approximate frequency counts with error bounds.
/// The msgpack wire format is the contract between sketch producers and
/// the query engine consumer.
#[derive(Debug, Clone)]
pub struct CountMinSketch {
    pub rows: usize,
    pub cols: usize,
    pub(crate) backend: SketchlibCms,
}

impl CountMinSketch {
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            rows,
            cols,
            backend: new_sketchlib_cms(rows, cols),
        }
    }

    /// Number of hash rows in the sketch matrix.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Number of columns (width) in the sketch matrix.
    pub fn cols(&self) -> usize {
        self.cols
    }

    /// Returns the sketch matrix (for wire format, serialization, tests).
    pub fn sketch(&self) -> Vec<Vec<f64>> {
        matrix_from_sketchlib_cms(&self.backend)
    }

    /// Construct from a `Vec<Vec<f64>>` matrix (used by deserialization and query engine).
    pub fn from_legacy_matrix(sketch: Vec<Vec<f64>>, rows: usize, cols: usize) -> Self {
        Self {
            rows,
            cols,
            backend: sketchlib_cms_from_matrix(rows, cols, &sketch),
        }
    }

    /// Insert a single weighted observation. Routes the key through the
    /// shared [`crate::common::hashspec`] pipeline so the matrix-cell
    /// layout matches `sketchlib-go::CountMinSketch.InsertWithHash`
    /// bit-for-bit:
    ///
    /// 1. `hash = Hash64(key) = XXH3-64-with-seed(seed_list[0], key)`
    ///    (Go's `common.Hash64`, mirrored by
    ///    [`hash_with_spec`](crate::common::hashspec::hash_with_spec))
    /// 2. for each row `r`:
    ///    - `col_raw = derive_index(spec, r, hash, mask_width)` — Go's
    ///      `MatrixHashType.RowHash` slice on the packed `u64`, where
    ///      `mask_width = next_power_of_two(cols)` (mirrors Go's
    ///      `hashLayoutForCols`)
    ///    - `col = col_raw % cols` — fold for non-pow2 widths,
    ///      mirroring Go's `if c >= s.Cols { c %= s.Cols }`
    /// 3. `matrix[r][col] += value`
    ///
    /// Constraints inherited from Go's Packed64 mode (the only mode
    /// the wire-format CountMinSketch exercises today):
    /// - `rows * log2(next_pow2(cols)) ≤ 64` so the packed hash holds
    ///   all row indices. The
    ///   `asap-precompute-rs::CMSWrapper` default (4×2048 → 4×11 = 44
    ///   bits) and Go's `DefaultRowNum=3, DefaultColNum=4096` (3×12 =
    ///   36 bits) both fit comfortably.
    ///
    /// Negative or zero values are skipped, mirroring the prior helper
    /// behavior (Go's `UpdateWeight` no-ops on `many == 0`).
    pub fn update(&mut self, key: &str, value: f64) {
        if value <= 0.0 || self.rows == 0 || self.cols == 0 {
            return;
        }
        let spec = crate::common::hashspec::HashSpec::default();
        let hash = crate::common::hashspec::hash_with_spec(&spec, key.as_bytes());
        let mask_width = (self.cols as u32).next_power_of_two();
        let storage = self.backend.as_storage_mut();
        for r in 0..self.rows {
            let col_raw = crate::common::hashspec::derive_index(&spec, r, hash, mask_width);
            let col = col_raw % self.cols;
            storage.increment_by_row(r, col, value);
        }
    }

    /// Estimate the frequency of `key` (CountMin point query). Mirrors
    /// `sketchlib-go::CountMinSketch.estimateMatrixHash` /
    /// `queryFrequencyFast`: derives the same per-row column index that
    /// [`Self::update`] used and returns the minimum cell across rows.
    pub fn estimate(&self, key: &str) -> f64 {
        if self.rows == 0 || self.cols == 0 {
            return 0.0;
        }
        let spec = crate::common::hashspec::HashSpec::default();
        let hash = crate::common::hashspec::hash_with_spec(&spec, key.as_bytes());
        let mask_width = (self.cols as u32).next_power_of_two();
        let storage = self.backend.as_storage();
        let mut min = f64::INFINITY;
        for r in 0..self.rows {
            let col_raw = crate::common::hashspec::derive_index(&spec, r, hash, mask_width);
            let col = col_raw % self.cols;
            let v = storage.query_one_counter(r, col);
            if v < min {
                min = v;
            }
        }
        if min.is_infinite() { 0.0 } else { min }
    }

    /// Merge another CountMinSketch into self in place. Both operands
    /// must have identical dimensions.
    pub fn merge(
        &mut self,
        other: &CountMinSketch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.rows != other.rows || self.cols != other.cols {
            return Err(format!(
                "CountMinSketch dimension mismatch: self={}x{}, other={}x{}",
                self.rows, self.cols, other.rows, other.cols
            )
            .into());
        }
        self.backend.merge(&other.backend);
        Ok(())
    }

    /// Merge from references, allocating only the output — no input clones.
    pub fn merge_refs(
        accumulators: &[&Self],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let rows = accumulators[0].rows;
        let cols = accumulators[0].cols;

        for acc in accumulators {
            if acc.rows != rows || acc.cols != cols {
                return Err(
                    "Cannot merge CountMinSketch accumulators with different dimensions".into(),
                );
            }
        }

        let mut merged = CountMinSketch::new(rows, cols);
        for acc in accumulators {
            merged.backend.merge(&acc.backend);
        }
        Ok(merged)
    }

    /// Apply a sparse delta in place. Matches the `ApplyDelta`
    /// semantics in `sketchlib-go/sketches/CountMinSketch/delta.go`:
    /// `matrix[row][col] += d_count` for each cell in the delta.
    ///
    /// The FFI handle is opaque, so we snapshot the matrix, apply
    /// cell updates, and rebuild the backend. The rebuild is
    /// O(rows × cols) per delta and is acceptable for ingest-side
    /// reconstitution — no delta should fire more than once per
    /// window (10s–300s in the paper's B3 / B4 configs).
    pub fn apply_delta(
        &mut self,
        delta: &CountMinSketchDelta,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (row, col, _) in &delta.cells {
            let r = *row as usize;
            let c = *col as usize;
            if r >= self.rows || c >= self.cols {
                return Err(format!(
                    "CountMinSketchDelta cell ({r},{c}) out of range (matrix={}x{})",
                    self.rows, self.cols
                )
                .into());
            }
        }
        let mut matrix = self.sketch();
        for (row, col, d_count) in &delta.cells {
            matrix[*row as usize][*col as usize] += *d_count as f64;
        }
        self.backend = sketchlib_cms_from_matrix(self.rows, self.cols, &matrix);
        Ok(())
    }

    /// Serialize to MessagePack — matches the wire format exactly.
    /// Thin shim over [`MessagePackCodec::to_msgpack`] kept for
    /// backwards compatibility.
    pub fn serialize_msgpack(&self) -> Result<Vec<u8>, RmpEncodeError> {
        self.to_msgpack().map_err(MsgPackError::into_encode)
    }

    /// Deserialize from MessagePack. Thin shim over
    /// [`MessagePackCodec::from_msgpack`].
    pub fn deserialize_msgpack(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::from_msgpack(buffer).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("Failed to deserialize CountMinSketch from MessagePack: {e}").into()
        })
    }

    /// One-shot aggregation: build a sketch from parallel key/value slices
    /// and return the msgpack bytes.
    pub fn aggregate_count(
        depth: usize,
        width: usize,
        keys: &[&str],
        values: &[f64],
    ) -> Option<Vec<u8>> {
        if keys.is_empty() {
            return None;
        }
        let mut sketch = Self::new(depth, width);
        for (key, &value) in keys.iter().zip(values.iter()) {
            sketch.update(key, value);
        }
        sketch.serialize_msgpack().ok()
    }

    /// Same as aggregate_count — CMS accumulates sums by construction.
    pub fn aggregate_sum(
        depth: usize,
        width: usize,
        keys: &[&str],
        values: &[f64],
    ) -> Option<Vec<u8>> {
        Self::aggregate_count(depth, width, keys, values)
    }
}

#[cfg(test)]
mod tests_wire_countmin {
    use super::*;

    #[test]
    fn test_count_min_sketch_creation() {
        let cms = CountMinSketch::new(4, 1000);
        assert_eq!(cms.rows, 4);
        assert_eq!(cms.cols, 1000);
        let sketch = cms.sketch();
        assert_eq!(sketch.len(), 4);
        assert_eq!(sketch[0].len(), 1000);

        // Check all values are initialized to 0
        for row in &sketch {
            for &value in row {
                assert_eq!(value, 0.0);
            }
        }
    }

    #[test]
    fn test_count_min_sketch_update() {
        let mut cms = CountMinSketch::new(2, 10);
        cms.update("key1", 1.0);
        // Query should return at least the updated value
        let result = cms.estimate("key1");
        assert!(result >= 1.0);
    }

    #[test]
    fn test_count_min_sketch_query_empty() {
        let cms = CountMinSketch::new(2, 10);
        assert_eq!(cms.estimate("anything"), 0.0);
    }

    #[test]
    fn test_count_min_sketch_merge() {
        // Use from_legacy_matrix so the test works regardless of sketchlib/legacy config
        let mut sketch1 = vec![vec![0.0; 3]; 2];
        sketch1[0][0] = 5.0;
        sketch1[1][2] = 10.0;
        let mut cms1 = CountMinSketch::from_legacy_matrix(sketch1, 2, 3);

        let mut sketch2 = vec![vec![0.0; 3]; 2];
        sketch2[0][0] = 3.0;
        sketch2[0][1] = 7.0;
        let cms2 = CountMinSketch::from_legacy_matrix(sketch2, 2, 3);

        cms1.merge(&cms2).unwrap();
        let merged_sketch = cms1.sketch();

        assert_eq!(merged_sketch[0][0], 8.0); // 5 + 3
        assert_eq!(merged_sketch[0][1], 7.0); // 0 + 7
        assert_eq!(merged_sketch[1][2], 10.0); // 10 + 0
    }

    #[test]
    fn test_count_min_sketch_merge_dimension_mismatch() {
        let mut cms1 = CountMinSketch::new(2, 3);
        let cms2 = CountMinSketch::new(3, 3);
        assert!(cms1.merge(&cms2).is_err());
    }

    #[test]
    fn test_count_min_sketch_msgpack_round_trip() {
        let mut cms = CountMinSketch::new(4, 256);
        cms.update("apple", 5.0);
        cms.update("banana", 3.0);
        cms.update("apple", 2.0); // total "apple" = 7

        let bytes = cms.serialize_msgpack().unwrap();
        let deserialized = CountMinSketch::deserialize_msgpack(&bytes).unwrap();

        assert_eq!(deserialized.rows, 4);
        assert_eq!(deserialized.cols, 256);
        assert!(deserialized.estimate("apple") >= 7.0);
        assert!(deserialized.estimate("banana") >= 3.0);
    }

    #[test]
    fn test_aggregate_count() {
        let keys = ["a", "b", "a"];
        let values = [1.0, 2.0, 3.0];
        let bytes = CountMinSketch::aggregate_count(4, 100, &keys, &values).unwrap();
        let cms = CountMinSketch::deserialize_msgpack(&bytes).unwrap();
        // "a" was updated twice (1.0 + 3.0 = 4.0), "b" once (2.0)
        assert!(cms.estimate("a") >= 4.0);
        assert!(cms.estimate("b") >= 2.0);
    }

    #[test]
    fn test_aggregate_count_empty() {
        assert!(CountMinSketch::aggregate_count(4, 100, &[], &[]).is_none());
    }

    #[test]
    fn test_apply_delta_additive() {
        let mut cms = CountMinSketch::from_legacy_matrix(
            vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]],
            2,
            3,
        );
        let delta = CountMinSketchDelta {
            rows: 2,
            cols: 3,
            cells: vec![(0, 0, 10), (1, 2, 100)],
            l1: vec![],
            l2: vec![],
        };
        cms.apply_delta(&delta).unwrap();
        assert_eq!(
            cms.sketch(),
            vec![vec![11.0, 2.0, 3.0], vec![4.0, 5.0, 106.0]]
        );
    }

    #[test]
    fn test_apply_delta_matches_full_merge() {
        let base = CountMinSketch::from_legacy_matrix(vec![vec![1.0, 2.0], vec![3.0, 4.0]], 2, 2);
        let addition =
            CountMinSketch::from_legacy_matrix(vec![vec![10.0, 0.0], vec![0.0, 20.0]], 2, 2);
        let mut via_merge = base.clone();
        via_merge.merge(&addition).unwrap();

        let delta = CountMinSketchDelta {
            rows: 2,
            cols: 2,
            cells: vec![(0, 0, 10), (1, 1, 20)],
            l1: vec![],
            l2: vec![],
        };
        let mut via_delta = base;
        via_delta.apply_delta(&delta).unwrap();
        assert_eq!(via_delta.sketch(), via_merge.sketch());
    }

    #[test]
    fn test_apply_delta_out_of_range() {
        let mut cms = CountMinSketch::new(2, 3);
        let delta = CountMinSketchDelta {
            rows: 2,
            cols: 3,
            cells: vec![(5, 0, 1)],
            l1: vec![],
            l2: vec![],
        };
        assert!(cms.apply_delta(&delta).is_err());
    }

    /// Cross-language byte-parity guard against `sketchlib-go`'s
    /// `CountMinSketch.SerializeProtoBytesFO` output for the
    /// deterministic input `goldenCmsKeys()` (10 keys "flow-0"..."flow-9",
    /// each repeated 5×, 50 unweighted updates total) at dimensions
    /// `(rows=4, cols=2048)` — the
    /// `asap-precompute-rs::CMSWrapper::new(4, 2048)` default. The hex
    /// blob below was captured from `proto.Marshal` of the Go envelope
    /// with `Producer` and `HashSpec` cleared, matching the
    /// `integration/parity/golden_test.go::TestGenerateGoldenFixtures`
    /// recipe (and the byte-payload that the Rust wrapper's
    /// `CMSWrapper::encode_envelope` emits to satisfy
    /// `cross_language_parity::cms_byte_parity_with_go`).
    ///
    /// Frequency-Only payload structure (matches Go's
    /// `SerializePortableFO`):
    /// - `rows = 4`, `cols = 2048`, `counter_type = INT64`
    /// - `counts_int` = packed sint64 row-major, `4*2048 = 8192` cells
    /// - `sum_counts` and `sum2_counts` deliberately omitted
    /// - `l1[r] = 50` (each row sees 50 unweighted inserts)
    /// - `l2[r] = 250` (10 unique cells per row each holding count 5,
    ///   so `Σ count^2 = 10 * 25 = 250`)
    ///
    /// Any drift in [`CountMinSketch::update`]'s hash → column-index
    /// derivation breaks this test cell-for-cell; that is the contract
    /// `cross_language_parity::cms_byte_parity_with_go` in ASAPCollector
    /// relies on. Closes part of ProjectASAP/ASAPCollector#243.
    #[test]
    fn test_update_then_envelope_matches_sketchlib_go_bytes() {
        use crate::proto::sketchlib::{
            CountMinState, CounterType, SketchEnvelope, sketch_envelope::SketchState,
        };
        use prost::Message;

        let rows = 4usize;
        let cols = 2048usize;
        let mut sk = CountMinSketch::new(rows, cols);
        for i in 0..50u64 {
            let key = format!("flow-{}", i % 10);
            sk.update(&key, 1.0);
        }

        // Build the Frequency-Only envelope mirroring sketchlib-go's
        // `CountMinSketch.SerializePortableFO`:
        //   - counter_type = INT64
        //   - counts_int = packed sint64 row-major, len = rows * cols
        //   - sum_counts / sum2_counts deliberately omitted
        //   - l1[r] = Σ_c count[r][c]   (Go maintains this incrementally
        //     in InsertWithHash; equals `weight * Σ inserts in row r`,
        //     which collapses to 50 for an unweighted 50-insert stream)
        //   - l2[r] = Σ_c count[r][c]^2 (Go maintains this as
        //     `L2[r] += curr*curr - prev*prev`, telescoping to the same
        //     sum-of-squares for unweighted streams)
        let matrix = sk.sketch();
        let mut counts_int: Vec<i64> = Vec::with_capacity(rows * cols);
        let mut l1: Vec<f64> = Vec::with_capacity(rows);
        let mut l2: Vec<f64> = Vec::with_capacity(rows);
        for row in matrix.iter().take(rows) {
            let mut row_l1 = 0.0f64;
            let mut row_l2 = 0.0f64;
            for &cell in row.iter().take(cols) {
                counts_int.push(cell as i64);
                row_l1 += cell;
                row_l2 += cell * cell;
            }
            l1.push(row_l1);
            l2.push(row_l2);
        }

        let state = CountMinState {
            rows: rows as u32,
            cols: cols as u32,
            counter_type: CounterType::Int64 as i32,
            counts_int,
            counts_float: Vec::new(),
            sum_counts: Vec::new(),
            sum2_counts: Vec::new(),
            l1,
            l2,
        };
        let envelope = SketchEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            sketch_state: Some(SketchState::CountMin(state)),
        };
        let mut got = Vec::with_capacity(envelope.encoded_len());
        envelope.encode(&mut got).expect("prost encode");

        // 8275-byte hex blob captured from
        // `sketchlib-go::CountMinSketch.SerializeProtoBytesFO` for the
        // same `(4, 2048) × goldenCmsKeys()` input — see
        // `integration/parity/golden_test.go` and
        // `cross_language_parity::cms_byte_parity_with_go` in
        // ASAPCollector.
        const GOLDEN_HEX: &str = include_str!("../sketches/testdata/cms_envelope_golden.hex");
        let want = decode_hex_cms(GOLDEN_HEX);
        assert_eq!(
            got.len(),
            want.len(),
            "CMS envelope length differs: got {} bytes, want {} bytes",
            got.len(),
            want.len(),
        );
        assert_eq!(
            got, want,
            "CMS envelope bytes diverge from sketchlib-go golden"
        );
    }

    fn decode_hex_cms(s: &str) -> Vec<u8> {
        let s = s.trim();
        s.as_bytes()
            .chunks(2)
            .map(|pair| {
                let high = hex_nibble_cms(pair[0]);
                let low = hex_nibble_cms(pair[1]);
                (high << 4) | low
            })
            .collect()
    }

    fn hex_nibble_cms(c: u8) -> u8 {
        match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            b'A'..=b'F' => c - b'A' + 10,
            _ => panic!("non-hex byte {}", c as char),
        }
    }
}
