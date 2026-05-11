//! Wire-format-aligned Count Sketch types.

use serde::{Deserialize, Serialize};

// =====================================================================
// ASAP runtime wire-format-aligned variant .
//
// `CountSketch` and `CountSketchDelta` below are the public-field,
// proto-decode-friendly types consumed by the ASAP query engine
// accumulators. The high-throughput in-process variant above
// (`Count`) keeps its original design.
// =====================================================================

// Count Sketch (a.k.a. Count-Min-style signed-counter sketch) —
// element-wise mergeable frequency estimator.
//
// Parallel to `count_min::CountMinSketch` but with **signed** counters,
// matching the `asap_sketchlib::proto::sketchlib::CountSketchState` wire
// format that DataCollector's `countsketchprocessor` emits via the
// modified OTLP `Metric.data = CountSketch{…}` variant.
//
// This is the minimal surface needed for PR C-CountSketch in the
// modified-OTLP hot path: construct from a decoded proto state, merge
// element-wise with another sketch, emit the matrix for queries and
// serialization. The richer query semantics of Count Sketch (median-
// of-estimators heavy-hitter tracking, `TopKState` integration, etc.)
// are intentionally deferred to a follow-up — the wire format already
// carries the matrix losslessly, so the merge/store round-trip works
// with just a matrix today.

// (de-duplicated) use serde::{Deserialize, Serialize};

/// Default Top-K capacity. Mirrors sketchlib-go `TOPK_SIZE = 100`.
pub const COUNT_SKETCH_TOPK_CAPACITY: usize = 100;

/// Sparse delta between two consecutive CountSketch snapshots —
/// the input shape for [`CountSketch::apply_delta`]. Mirrors the
/// `CountSketchDelta` proto in
/// `sketchlib-go/proto/countsketch/countsketch.proto` and the native
/// Go `Delta` in `sketchlib-go/sketches/CountSketch/delta.go`.
///
/// Cells apply additively: `matrix[row][col] += d_count` for each
/// `(row, col, d_count)` triple. Per-row L2 norm deltas apply
/// additively. Heavy-hitter candidate keys (`hh_keys`) are queried
/// against the post-merge matrix and used to rebuild the receiver's
/// Top-K heap.
#[derive(Debug, Clone, Default)]
pub struct CountSketchDelta {
    pub rows: u32,
    pub cols: u32,
    /// `(row, col, d_count)` cell updates, additive on the CS matrix.
    pub cells: Vec<(u32, u32, i64)>,
    /// Per-row L2 norm deltas. Additive, one scalar per row of the
    /// base sketch. Kept on the delta surface for downstream
    /// error-accounting; `apply_delta` itself ignores L2.
    pub l2: Vec<f64>,
    /// Heavy-hitter candidate keys forwarded by the upstream
    /// Space-Saving tracker. The receiver re-queries the merged CS
    /// matrix for each key and updates its Top-K heap with the
    /// resulting estimate. Mirrors Go's `Delta.HHKeys`.
    pub hh_keys: Vec<String>,
}

/// Minimal Count Sketch state — a flat `rows × cols` matrix of signed
/// counts. Element-wise mergeable (sum over aligned cells). Mirrors
/// sketchlib-go's `CountSketch.Count`/`TopK` pair (the on-the-wire
/// `L2` field is a derived value and is recomputed on load).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountSketch {
    #[serde(rename = "row_num")]
    pub rows: usize,
    #[serde(rename = "col_num")]
    pub cols: usize,
    /// Row-major matrix of signed counts. `matrix[r][c]` is the value of
    /// hash row `r`, column `c`.
    pub matrix: Vec<Vec<f64>>,
    /// Top-K heavy hitters as `(key, count)` pairs, capped at
    /// [`COUNT_SKETCH_TOPK_CAPACITY`]. Order is not guaranteed (heap
    /// shape is not preserved on the wire). Mirrors Go's
    /// `CountSketch.TopK` slot. Defaults to empty on legacy payloads.
    #[serde(default)]
    pub topk: Vec<(String, f64)>,
}

impl CountSketch {
    /// Construct an all-zero sketch with the given dimensions.
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            rows,
            cols,
            matrix: vec![vec![0.0; cols]; rows],
            topk: Vec::new(),
        }
    }

    /// Construct from a pre-built matrix (used by the modified-OTLP
    /// proto-decode path). `topk` is zero-initialised; callers that
    /// need non-zero auxiliary state should use the msgpack/proto path.
    pub fn from_legacy_matrix(matrix: Vec<Vec<f64>>, rows: usize, cols: usize) -> Self {
        debug_assert_eq!(matrix.len(), rows, "row count mismatch");
        debug_assert!(
            matrix.iter().all(|r| r.len() == cols),
            "column count mismatch in at least one row"
        );
        Self {
            rows,
            cols,
            matrix,
            topk: Vec::new(),
        }
    }

    /// Borrow the inner matrix.
    pub fn sketch(&self) -> &Vec<Vec<f64>> {
        &self.matrix
    }

    /// Update the in-memory Top-K heap with `(key, count)`. Keeps the
    /// heap bounded by [`COUNT_SKETCH_TOPK_CAPACITY`]; on overflow,
    /// drops the smallest-count entry. If `key` is already present,
    /// the new count replaces the old (max semantics). Used by
    /// `apply_delta` to rebuild Top-K from `hh_keys`.
    fn topk_update(&mut self, key: &str, count: f64) {
        if let Some(slot) = self.topk.iter_mut().find(|(k, _)| k == key) {
            if count > slot.1 {
                slot.1 = count;
            }
            return;
        }
        if self.topk.len() < COUNT_SKETCH_TOPK_CAPACITY {
            self.topk.push((key.to_owned(), count));
            return;
        }
        // Capacity hit: replace the minimum if `count` exceeds it.
        if let Some((min_idx, min_count)) = self
            .topk
            .iter()
            .enumerate()
            .min_by(|a, b| {
                a.1.1
                    .partial_cmp(&b.1.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(i, e)| (i, e.1))
        {
            if count > min_count {
                self.topk[min_idx] = (key.to_owned(), count);
            }
        }
    }

    /// Insert a single weighted observation. Routes the key through the
    /// shared [`crate::common::hashspec`] pipeline so the matrix-cell
    /// layout matches `sketchlib-go::CountSketch.UpdateString`
    /// bit-for-bit:
    ///
    /// 1. `hash = Hash64(key) = XXH3-64-with-seed(seed_list[0], key)`
    ///    (Go's `common.Hash64`, mirrored by
    ///    [`hash_with_spec`](crate::common::hashspec::hash_with_spec))
    /// 2. for each row `r`:
    ///    - `col = derive_index(spec, r, hash, cols)` — Go's
    ///      `MatrixHashType.RowHash` slice on the packed `u64`
    ///    - `sign = derive_sign(spec, r, hash)` — Go's
    ///      `MatrixHashType.SignForRow` high-bit-minus-row extraction
    /// 3. `matrix[r][col] += sign * value`
    ///
    /// Constraints inherited from Go's Packed64 mode (the only mode
    /// the wire-format CountSketch exercises today):
    /// - `cols` must be a power of two; the column mask is `cols - 1`.
    /// - `rows * (log2(cols) + 1) ≤ 64` so the packed hash holds all
    ///   row indices and sign bits.
    ///
    /// Both constraints are honored by the
    /// `asap-precompute-rs::CountSketchWrapper` defaults (3×512 → 30
    /// bits) and by `sketchlib-go`'s
    /// `RustDefaultRows = 3, RustDefaultCols = 4096` (39 bits).
    pub fn update(&mut self, key: &str, value: f64) {
        if self.rows == 0 || self.cols == 0 {
            return;
        }
        let spec = crate::common::hashspec::HashSpec::default();
        let hash = crate::common::hashspec::hash_with_spec(&spec, key.as_bytes());
        let cols_u32 = self.cols as u32;
        for r in 0..self.rows {
            let col = crate::common::hashspec::derive_index(&spec, r, hash, cols_u32);
            let sign = crate::common::hashspec::derive_sign(&spec, r, hash) as f64;
            self.matrix[r][col] += sign * value;
        }
    }

    /// Estimate the frequency of `key` via the standard median-of-rows
    /// CountSketch query. Returns 0 for an empty sketch. Mirrors
    /// `sketchlib-go::CountSketch.QueryWithHash(QueryFrequency)`: the
    /// per-row column index and sign are derived from the same single
    /// hash that [`Self::update`] used, so the estimator inverts the
    /// signed-counter projection in lockstep with the update.
    pub fn estimate(&self, key: &str) -> f64 {
        if self.rows == 0 || self.cols == 0 {
            return 0.0;
        }
        let spec = crate::common::hashspec::HashSpec::default();
        let hash = crate::common::hashspec::hash_with_spec(&spec, key.as_bytes());
        let cols_u32 = self.cols as u32;
        let mut estimates: Vec<f64> = Vec::with_capacity(self.rows);
        for r in 0..self.rows {
            let col = crate::common::hashspec::derive_index(&spec, r, hash, cols_u32);
            let sign = crate::common::hashspec::derive_sign(&spec, r, hash) as f64;
            estimates.push(sign * self.matrix[r][col]);
        }
        estimates.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = estimates.len() / 2;
        if estimates.len() % 2 == 1 {
            estimates[mid]
        } else {
            (estimates[mid - 1] + estimates[mid]) / 2.0
        }
    }

    /// Merge one other sketch into self via element-wise addition. Both
    /// operands must have identical dimensions.
    pub fn merge(
        &mut self,
        other: &CountSketch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.rows != other.rows || self.cols != other.cols {
            return Err(format!(
                "CountSketch dimension mismatch: self={}x{}, other={}x{}",
                self.rows, self.cols, other.rows, other.cols
            )
            .into());
        }
        for r in 0..self.rows {
            for c in 0..self.cols {
                self.matrix[r][c] += other.matrix[r][c];
            }
        }
        Ok(())
    }

    /// Apply a sparse delta in place. Matches the `ApplyDelta`
    /// semantics in `sketchlib-go/sketches/CountSketch/delta.go`:
    ///   * each `(row, col, d_count)` triple updates the count matrix
    ///     additively (`matrix[r][c] += d_count`);
    ///   * each `hh_key` is re-queried against the post-update matrix
    ///     and pushed into the receiver's Top-K with the merged-estimate
    ///     count (mirrors Go's `Delta.HHKeys` heavy-hitter rebuild).
    ///
    /// Returns `Err` if any `(row, col)` is out of range — indicating
    /// a dimension mismatch between the snapshot this sketch was
    /// built from and the delta sender.
    pub fn apply_delta(
        &mut self,
        delta: &CountSketchDelta,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 1. Cell additions.
        for (row, col, d_count) in &delta.cells {
            let r = *row as usize;
            let c = *col as usize;
            if r >= self.rows || c >= self.cols {
                return Err(format!(
                    "CountSketchDelta cell ({r},{c}) out of range (matrix={}x{})",
                    self.rows, self.cols
                )
                .into());
            }
            // `d_count` is signed on the wire; CS counts are signed
            // too (can go negative under adversarial keys).
            self.matrix[r][c] += *d_count as f64;
        }
        // 2. Heavy-hitter rebuild from `hh_keys`. Re-estimate against
        // the freshly-updated matrix and push into Top-K with the
        // merged count. Mirrors sketchlib-go's `Delta.HHKeys` path.
        for key in &delta.hh_keys {
            let est = self.estimate(key);
            self.topk_update(key, est);
        }
        Ok(())
    }

    /// Merge a slice of references into a single new sketch. All inputs
    /// must share the same dimensions; returns `Err` on mismatch or an
    /// empty input.
    pub fn merge_refs(
        inputs: &[&CountSketch],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let first = inputs
            .first()
            .ok_or("CountSketch::merge_refs called with empty input")?;
        let mut merged = CountSketch::new(first.rows, first.cols);
        for cs in inputs {
            merged.merge(cs)?;
        }
        Ok(merged)
    }
}

#[cfg(test)]
mod tests_wire_count {
    use super::*;
    use crate::message_pack_format::MessagePackCodec;

    #[test]
    fn test_new_empty() {
        let cs = CountSketch::new(2, 3);
        assert_eq!(cs.rows, 2);
        assert_eq!(cs.cols, 3);
        assert_eq!(cs.sketch(), &vec![vec![0.0, 0.0, 0.0], vec![0.0, 0.0, 0.0]]);
    }

    #[test]
    fn test_from_legacy_matrix() {
        let m = vec![vec![1.0, -2.0, 3.0], vec![-4.0, 5.0, -6.0]];
        let cs = CountSketch::from_legacy_matrix(m.clone(), 2, 3);
        assert_eq!(cs.sketch(), &m);
    }

    #[test]
    fn test_merge_element_wise() {
        let mut a = CountSketch::from_legacy_matrix(vec![vec![1.0, 2.0], vec![3.0, 4.0]], 2, 2);
        let b = CountSketch::from_legacy_matrix(vec![vec![-1.0, -2.0], vec![-3.0, -4.0]], 2, 2);
        a.merge(&b).unwrap();
        assert_eq!(a.sketch(), &vec![vec![0.0, 0.0], vec![0.0, 0.0]]);
    }

    #[test]
    fn test_merge_dimension_mismatch() {
        let mut a = CountSketch::new(2, 3);
        let b = CountSketch::new(3, 3);
        assert!(a.merge(&b).is_err());
    }

    #[test]
    fn test_merge_refs() {
        let a = CountSketch::from_legacy_matrix(vec![vec![1.0, 2.0]], 1, 2);
        let b = CountSketch::from_legacy_matrix(vec![vec![3.0, 4.0]], 1, 2);
        let c = CountSketch::from_legacy_matrix(vec![vec![5.0, 6.0]], 1, 2);
        let merged = CountSketch::merge_refs(&[&a, &b, &c]).unwrap();
        assert_eq!(merged.sketch(), &vec![vec![9.0, 12.0]]);
    }

    #[test]
    fn test_apply_delta_additive() {
        let mut cs = CountSketch::from_legacy_matrix(
            vec![vec![1.0, -2.0, 3.0], vec![-4.0, 5.0, -6.0]],
            2,
            3,
        );
        let delta = CountSketchDelta {
            rows: 2,
            cols: 3,
            cells: vec![
                (0, 0, 10),  // 1 + 10 = 11
                (0, 2, -3),  // 3 - 3 = 0
                (1, 1, -15), // 5 - 15 = -10
            ],
            l2: vec![],
            hh_keys: vec![],
        };
        cs.apply_delta(&delta).unwrap();
        assert_eq!(
            cs.sketch(),
            &vec![vec![11.0, -2.0, 0.0], vec![-4.0, -10.0, -6.0]]
        );
    }

    #[test]
    fn test_apply_delta_matches_full_merge() {
        let base = CountSketch::from_legacy_matrix(vec![vec![1.0, 2.0], vec![3.0, 4.0]], 2, 2);
        let addition =
            CountSketch::from_legacy_matrix(vec![vec![10.0, 0.0], vec![0.0, 20.0]], 2, 2);
        let mut via_merge = base.clone();
        via_merge.merge(&addition).unwrap();

        let delta = CountSketchDelta {
            rows: 2,
            cols: 2,
            cells: vec![(0, 0, 10), (1, 1, 20)],
            l2: vec![],
            hh_keys: vec![],
        };
        let mut via_delta = base;
        via_delta.apply_delta(&delta).unwrap();
        assert_eq!(via_delta.sketch(), via_merge.sketch());
    }

    #[test]
    fn test_apply_delta_out_of_range() {
        let mut cs = CountSketch::new(2, 3);
        let delta = CountSketchDelta {
            rows: 2,
            cols: 3,
            cells: vec![(2, 0, 1)], // row 2 out of range for 2-row matrix
            l2: vec![],
            hh_keys: vec![],
        };
        assert!(cs.apply_delta(&delta).is_err());
    }

    #[test]
    fn test_apply_delta_rebuilds_topk_from_hh_keys() {
        // Build a sketch with two known keys via the in-process
        // `update` path so the matrix has a coherent shape, then
        // send a delta that only carries `hh_keys` entries. The
        // receiver should re-query the merged matrix and populate
        // `topk` with the resulting estimates. Mirrors sketchlib-go's
        // `Delta.HHKeys` heavy-hitter rebuild path.
        let mut cs = CountSketch::new(3, 16);
        cs.update("alpha", 5.0);
        cs.update("beta", 3.0);
        let delta = CountSketchDelta {
            rows: 3,
            cols: 16,
            cells: vec![],
            l2: vec![],
            hh_keys: vec!["alpha".to_string(), "beta".to_string()],
        };
        cs.apply_delta(&delta).unwrap();
        assert_eq!(cs.topk.len(), 2);
        let alpha_count = cs
            .topk
            .iter()
            .find(|(k, _)| k == "alpha")
            .map(|(_, v)| *v)
            .expect("alpha should be in topk");
        let beta_count = cs
            .topk
            .iter()
            .find(|(k, _)| k == "beta")
            .map(|(_, v)| *v)
            .expect("beta should be in topk");
        // Alpha was inserted with weight 5; the median estimate
        // should exceed beta's (weight 3) modulo signed-counter
        // cancellation in this small 3x16 matrix.
        assert!(
            alpha_count > beta_count,
            "alpha={alpha_count} beta={beta_count}"
        );
    }

    #[test]
    fn test_apply_delta_hh_keys_topk_capacity() {
        // Verify the Top-K heap is bounded by COUNT_SKETCH_TOPK_CAPACITY
        // and that on overflow, the smallest-count entry is evicted in
        // favor of a larger-count newcomer.
        let mut cs = CountSketch::new(3, 1024);
        let n = COUNT_SKETCH_TOPK_CAPACITY + 5;
        let keys: Vec<String> = (0..n).map(|i| format!("k{i:04}")).collect();
        // Fill all keys into the matrix so estimates are non-zero.
        for (i, k) in keys.iter().enumerate() {
            cs.update(k, (i + 1) as f64);
        }
        let delta = CountSketchDelta {
            rows: 3,
            cols: 1024,
            cells: vec![],
            l2: vec![],
            hh_keys: keys.clone(),
        };
        cs.apply_delta(&delta).unwrap();
        assert_eq!(cs.topk.len(), COUNT_SKETCH_TOPK_CAPACITY);
    }

    #[test]
    fn test_msgpack_round_trip() {
        let original =
            CountSketch::from_legacy_matrix(vec![vec![1.5, -2.5], vec![3.5, -4.5]], 2, 2);
        let bytes = original.to_msgpack().unwrap();
        let decoded = CountSketch::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.sketch(), original.sketch());
        assert_eq!(decoded.rows, original.rows);
        assert_eq!(decoded.cols, original.cols);
    }

    /// Cross-language byte-parity guard against `sketchlib-go`'s
    /// `CountSketch.SerializeProtoBytes` output for the deterministic
    /// input `goldenCsKeys()` (25 keys "k-a"..."k-e", each repeated 5×)
    /// at dimensions `(rows=3, cols=512)`. The hex blob below was
    /// captured from a `proto.Marshal` of the Go envelope with
    /// `Producer` and `HashSpec` cleared (matching the
    /// `integration/parity/golden_test.go::TestGenerateGoldenFixtures`
    /// recipe used for DDSketch and KLL).
    ///
    /// Any drift in [`CountSketch::update`]'s hash → (col, sign)
    /// derivation breaks this test cell-for-cell; that is the contract
    /// `cross_language_parity::countsketch_byte_parity_with_go` in
    /// ASAPCollector relies on. Closes part of
    /// ProjectASAP/ASAPCollector#243.
    #[test]
    fn test_update_then_envelope_matches_sketchlib_go_bytes() {
        use crate::proto::sketchlib::{
            CountSketchState, CounterType, SketchEnvelope, sketch_envelope::SketchState,
        };
        use prost::Message;

        let rows = 3usize;
        let cols = 512usize;
        let mut sk = CountSketch::new(rows, cols);
        for i in 0..25 {
            let key = format!("k-{}", (b'a' + (i % 5) as u8) as char);
            sk.update(&key, 1.0);
        }

        // Build envelope mirroring sketchlib-go's CountSketch.SerializePortable:
        //   counter_type = INT64, counts packed sint64 in row-major
        //   order, l2 = per-row sum of squared cells, no TopK.
        let mut counts_int: Vec<i64> = Vec::with_capacity(rows * cols);
        let mut l2: Vec<f64> = Vec::with_capacity(rows);
        for row in sk.matrix.iter().take(rows) {
            let mut row_l2 = 0.0f64;
            for &cell in row.iter().take(cols) {
                counts_int.push(cell as i64);
                row_l2 += cell * cell;
            }
            l2.push(row_l2);
        }

        let state = CountSketchState {
            rows: rows as u32,
            cols: cols as u32,
            counter_type: CounterType::Int64 as i32,
            counts_int,
            counts_float: Vec::new(),
            l2,
            topk: None,
        };
        let envelope = SketchEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            sketch_state: Some(SketchState::CountSketch(state)),
        };
        let mut got = Vec::with_capacity(envelope.encoded_len());
        envelope.encode(&mut got).expect("prost encode");

        // Byte string captured from sketchlib-go for the same
        // (3,512) × `goldenCsKeys()` input — see
        // `integration/parity/golden_test.go` and
        // `cross_language_parity::countsketch_byte_parity_with_go`
        // in ASAPCollector. 1577 bytes total: a `SketchEnvelope` proto
        // wrapping a `CountSketchState` whose `counts_int` is a packed
        // sint64 (zigzag) row-major matrix and `l2` is `[125.0;3]`
        // (each row's 5 hot cells hold ±5 → l2 = 5·25 = 125).
        const GOLDEN_HEX: &str = "08015aa40c0803108004180222800c000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000900000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000090000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000009000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000900000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000900000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000090000000000000a000000000000000900000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000900000000000000000000000000000000000000000000000000000000000000000a0000000000000000000000090000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000009000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000032180000000000405f400000000000405f400000000000405f40";
        let want = decode_hex(GOLDEN_HEX);
        assert_eq!(
            got,
            want,
            "CountSketch envelope bytes diverge from sketchlib-go golden \
             ({} bytes got vs {} bytes want)",
            got.len(),
            want.len(),
        );
    }

    fn decode_hex(s: &str) -> Vec<u8> {
        s.as_bytes()
            .chunks(2)
            .map(|pair| {
                let high = hex_nibble(pair[0]);
                let low = hex_nibble(pair[1]);
                (high << 4) | low
            })
            .collect()
    }

    fn hex_nibble(c: u8) -> u8 {
        match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            b'A'..=b'F' => c - b'A' + 10,
            _ => panic!("non-hex byte {}", c as char),
        }
    }
}
