//! Wire-format-aligned DDSketch types.
//!
//! Moved from `crate::wrapper::ddsketch`; the wire DTO + runtime ops
//! live together here.

use serde::{Deserialize, Serialize};

use crate::message_pack_format::{Error as MsgPackError, MessagePackCodec};

/// Bucket-store growth chunk for the wire-format-aligned [`DdSketch`]
/// variant. Matches `sketchlib-go/sketches/DDSketch.GrowChunk` so the
/// `store_counts` / `store_offset` layout written by
/// [`DdSketch::update`] is byte-identical to Go's `SerializePortable`
/// output for the same input stream.
pub const DDSKETCH_GROW_CHUNK: usize = 128;

// =====================================================================
// ASAP runtime wire-format-aligned variant .
//
// `DdSketch` and `DdSketchDelta` below are the public-field,
// proto-decode-friendly types consumed by the ASAP query engine
// accumulators. The high-throughput in-process variant above
// (`DDSketch`) keeps its original design.
// =====================================================================

// DDSketch — log-bucketed quantile sketch, mergeable by store-index alignment.
//
// Parallel to `count_sketch::CountSketch`: the minimum viable surface
// needed for the modified-OTLP `Metric.data = DDSketch{…}` hot path
// (PR C-CountSketch follow-up). Holds the bucket counts, their
// absolute-index base offset, and the aggregate `{count, sum, min, max}`.
//
// Merge semantics: two sketches with the same relative-accuracy
// parameter `alpha` are merged by aligning bucket arrays along their
// `store_offset` and summing counts element-wise, with `min`/`max`
// combined via min/max and `count`/`sum` added.
//
// The wire format is the protobuf-encoded
// `asap_sketchlib::proto::sketchlib::DDSketchState` emitted by
// DataCollector's `ddsketchprocessor`. Quantile estimation against
// stored data is intentionally deferred — queries currently return
// a placeholder error and fall through to the §5.2 fallback.

// (de-duplicated) use serde::{Deserialize, Serialize};

/// Sparse delta between two consecutive DDSketch snapshots — the
/// input shape for [`DdSketch::apply_delta`]. Mirrors the
/// `DDSketchDelta` proto in `sketchlib-go/proto/ddsketch/ddsketch.proto`
/// (and its Rust bindings vendored in `asap_otel_proto::sketchlib::v1`).
/// Kept as a plain struct so this crate doesn't need a tonic/prost
/// dependency; proto decode lives in the accumulator.
#[derive(Debug, Clone, Default)]
pub struct DdSketchDelta {
    /// `(absolute_bucket_index, Δcount)` pairs, additive.
    pub buckets: Vec<(i32, u64)>,
    /// Δ total count. May be negative (signed on the wire).
    pub d_count: i64,
    /// Δ sum.
    pub d_sum: f64,
    /// Whether `new_min` carries a meaningful value. Min can only
    /// decrease; a delta that didn't lower min sends `false`.
    pub min_changed: bool,
    pub new_min: f64,
    /// Whether `new_max` carries a meaningful value. Max can only
    /// increase.
    pub max_changed: bool,
    pub new_max: f64,
}

/// Minimal DDSketch state — bucket counts + alpha + aggregates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DdSketch {
    /// Relative accuracy parameter; must satisfy `0 < alpha < 1`.
    pub alpha: f64,
    /// Bucket counts in absolute-index order. The absolute index of
    /// `store_counts[i]` is `i + store_offset`.
    pub store_counts: Vec<u64>,
    /// Absolute bucket index corresponding to `store_counts[0]`. May
    /// be negative.
    pub store_offset: i32,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl DdSketch {
    /// Construct an empty sketch.
    pub fn new(alpha: f64) -> Self {
        Self {
            alpha,
            store_counts: Vec::new(),
            store_offset: 0,
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    /// Construct from the decoded wire fields.
    #[allow(clippy::too_many_arguments)]
    pub fn from_raw(
        alpha: f64,
        store_counts: Vec<u64>,
        store_offset: i32,
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    ) -> Self {
        Self {
            alpha,
            store_counts,
            store_offset,
            count,
            sum,
            min,
            max,
        }
    }

    /// Merge one other sketch into self by aligning bucket arrays on
    /// absolute indices. Both operands must share the same `alpha`.
    pub fn merge(
        &mut self,
        other: &DdSketch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if (self.alpha - other.alpha).abs() > f64::EPSILON {
            return Err(format!(
                "DdSketch alpha mismatch: self={}, other={}",
                self.alpha, other.alpha
            )
            .into());
        }

        if other.store_counts.is_empty() {
            self.count += other.count;
            self.sum += other.sum;
            if other.min < self.min {
                self.min = other.min;
            }
            if other.max > self.max {
                self.max = other.max;
            }
            return Ok(());
        }
        if self.store_counts.is_empty() {
            self.store_counts = other.store_counts.clone();
            self.store_offset = other.store_offset;
        } else {
            let self_start = self.store_offset as i64;
            let self_end = self_start + self.store_counts.len() as i64;
            let other_start = other.store_offset as i64;
            let other_end = other_start + other.store_counts.len() as i64;
            let new_start = self_start.min(other_start);
            let new_end = self_end.max(other_end);
            let new_len = (new_end - new_start) as usize;
            let mut merged = vec![0u64; new_len];
            for (i, c) in self.store_counts.iter().enumerate() {
                let idx = (self_start + i as i64 - new_start) as usize;
                merged[idx] = merged[idx].saturating_add(*c);
            }
            for (i, c) in other.store_counts.iter().enumerate() {
                let idx = (other_start + i as i64 - new_start) as usize;
                merged[idx] = merged[idx].saturating_add(*c);
            }
            self.store_counts = merged;
            self.store_offset = new_start as i32;
        }
        self.count += other.count;
        self.sum += other.sum;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
        Ok(())
    }

    /// Apply a sparse delta to this sketch in place. Matches the
    /// `ApplyDelta` logic in `sketchlib-go/sketches/DDSketch/delta.go`:
    /// bucket counts add, total count + sum add, min can only decrease
    /// and max can only increase. Used by the backend ingest path to
    /// reconstitute a full sketch from a base snapshot + subsequent
    /// delta-transmission frames (paper §6.2 B3 / B4 baselines).
    pub fn apply_delta(&mut self, delta: &DdSketchDelta) {
        for (abs_idx, d_count) in &delta.buckets {
            if self.store_counts.is_empty() {
                self.store_counts = vec![0u64; 1];
                self.store_offset = *abs_idx;
            }
            let cur_start = self.store_offset as i64;
            let cur_end = cur_start + self.store_counts.len() as i64;
            let k = *abs_idx as i64;
            if k < cur_start {
                // Prepend zeros.
                let pad = (cur_start - k) as usize;
                let mut buf = vec![0u64; pad];
                buf.append(&mut self.store_counts);
                self.store_counts = buf;
                self.store_offset = *abs_idx;
            } else if k >= cur_end {
                let pad = (k - cur_end + 1) as usize;
                self.store_counts.extend(std::iter::repeat_n(0u64, pad));
            }
            let arr_idx = (k - self.store_offset as i64) as usize;
            self.store_counts[arr_idx] = self.store_counts[arr_idx].saturating_add(*d_count);
        }
        if delta.d_count >= 0 {
            self.count = self.count.saturating_add(delta.d_count as u64);
        } else {
            self.count = self.count.saturating_sub((-delta.d_count) as u64);
        }
        self.sum += delta.d_sum;
        if delta.min_changed && delta.new_min < self.min {
            self.min = delta.new_min;
        }
        if delta.max_changed && delta.new_max > self.max {
            self.max = delta.new_max;
        }
    }

    /// Merge a slice of references into a single new sketch. Returns
    /// `Err` on alpha mismatch or an empty input.
    pub fn merge_refs(
        inputs: &[&DdSketch],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let first = inputs
            .first()
            .ok_or("DdSketch::merge_refs called with empty input")?;
        let mut merged = DdSketch::new(first.alpha);
        for d in inputs {
            merged.merge(d)?;
        }
        Ok(merged)
    }

    /// Insert a single positive value. Updates count, sum, min/max
    /// and increments the bucket for `floor(ln(v) / ln(gamma))`
    /// where `gamma = (1+α)/(1-α)`. Provided primarily so tests can
    /// build a ground-truth sketch to compare delta-apply output
    /// against.
    ///
    /// Bucket-store growth mirrors `sketchlib-go`'s `Buckets.ensure`:
    /// the first allocation is a half-chunk-centered `GROW_CHUNK` of
    /// zeros, and subsequent expansions extend by `max(needed,
    /// GROW_CHUNK)` so the on-the-wire `store_counts` / `store_offset`
    /// layout is byte-identical to Go's `SerializePortable` output.
    /// Without this chunked layout the `DDSketchState` proto bytes
    /// emitted by `asap-precompute-rs::DDSketchWrapper` diverge from
    /// the legacy OTel `ddsketchprocessor` payload (see
    /// ProjectASAP/ASAPCollector#243).
    pub fn update(&mut self, value: f64) {
        if value <= 0.0 {
            // DDSketch is defined for positive reals; the paper's
            // sketchlib-go rejects non-positive values silently.
            return;
        }
        let gamma = (1.0 + self.alpha) / (1.0 - self.alpha);
        let ln_gamma = gamma.ln();
        let idx = (value.ln() / ln_gamma).floor() as i32;
        self.ensure_bucket(idx);
        let arr_idx = (idx as i64 - self.store_offset as i64) as usize;
        self.store_counts[arr_idx] = self.store_counts[arr_idx].saturating_add(1);
        self.count = self.count.saturating_add(1);
        self.sum += value;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
    }

    /// Ensure bucket `k` is addressable in `store_counts`, growing in
    /// chunks of [`DDSKETCH_GROW_CHUNK`] to match `sketchlib-go`'s
    /// `Buckets.ensure`. Empty stores are seeded with a half-chunk
    /// of zeros centered on `k` (`store_offset = k - GROW_CHUNK/2`);
    /// out-of-range expansions extend by `max(needed, GROW_CHUNK)`
    /// on either side. The on-the-wire byte layout is therefore
    /// identical between Go and Rust producers fed the same input
    /// stream — required for the cross-language byte-parity test
    /// in `ASAPCollector::cross_language_parity::ddsketch_byte_parity_with_go`.
    fn ensure_bucket(&mut self, k: i32) {
        if self.store_counts.is_empty() {
            self.store_counts = vec![0u64; DDSKETCH_GROW_CHUNK];
            self.store_offset = k - (DDSKETCH_GROW_CHUNK as i32 / 2);
            return;
        }
        let cur_start = self.store_offset as i64;
        let cur_end = cur_start + self.store_counts.len() as i64;
        let kk = k as i64;
        if kk < cur_start {
            let needed = (cur_start - kk) as usize;
            let grow = needed.max(DDSKETCH_GROW_CHUNK);
            let mut buf = vec![0u64; grow];
            buf.extend_from_slice(&self.store_counts);
            self.store_counts = buf;
            self.store_offset -= grow as i32;
        } else if kk >= cur_end {
            let needed = (kk - cur_end + 1) as usize;
            let grow = needed.max(DDSKETCH_GROW_CHUNK);
            self.store_counts.extend(std::iter::repeat_n(0u64, grow));
        }
    }

    /// Estimate the quantile at rank `q` ∈ [0, 1]. Walks the bucket
    /// array in ascending absolute-index order, accumulating counts
    /// until the target rank; returns the bucket's representative
    /// value `gamma^(k + 0.5)` where `k` is the bucket's absolute
    /// index. Returns `None` if the sketch is empty.
    ///
    /// Accuracy: bounded by DDSketch's α parameter — the estimated
    /// quantile value is within `(1+α)/(1-α)` relative error of the
    /// true quantile.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 || self.store_counts.is_empty() {
            return None;
        }
        let target = (q * (self.count.saturating_sub(1)) as f64).floor() as u64;
        let mut cumulative: u64 = 0;
        let gamma = (1.0 + self.alpha) / (1.0 - self.alpha);
        for (i, &c) in self.store_counts.iter().enumerate() {
            cumulative = cumulative.saturating_add(c);
            if cumulative > target {
                let k = (self.store_offset as i64 + i as i64) as f64;
                // Bucket midpoint: gamma^(k + 0.5) — centers the
                // estimate in the logarithmic bucket.
                return Some(gamma.powf(k + 0.5));
            }
        }
        // Numerical edge case: if we fall off the end, return max.
        Some(self.max)
    }

    /// Return the alpha value as it appears on the wire — round-tripped
    /// through gamma exactly the way `sketchlib-go::DDSketch.SerializePortable`
    /// does it (`alpha = (gamma - 1) / (gamma + 1)`, where
    /// `gamma = (1+α)/(1-α)`). The roundtrip introduces a small
    /// floating-point drift; without applying it the Rust producer's
    /// `DDSketchState.alpha` field bytes diverge from the Go
    /// producer's, and the cross-language byte-parity test fails on
    /// the very first proto field. Closes part of
    /// ProjectASAP/ASAPCollector#243.
    #[inline]
    pub fn wire_alpha(&self) -> f64 {
        let gamma = (1.0 + self.alpha) / (1.0 - self.alpha);
        (gamma - 1.0) / (gamma + 1.0)
    }
}

impl MessagePackCodec for DdSketch {
    fn to_msgpack(&self) -> Result<Vec<u8>, MsgPackError> {
        Ok(rmp_serde::to_vec(self)?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, MsgPackError> {
        Ok(rmp_serde::from_slice(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_pack_format::MessagePackCodec;

    #[test]
    fn test_new_empty() {
        let d = DdSketch::new(0.01);
        assert_eq!(d.count, 0);
        assert!(d.store_counts.is_empty());
        assert_eq!(d.min, f64::INFINITY);
        assert_eq!(d.max, f64::NEG_INFINITY);
    }

    #[test]
    fn test_merge_aligned_same_offset() {
        let mut a = DdSketch::from_raw(0.01, vec![1, 2, 3], -1, 6, 30.0, 1.0, 5.0);
        let b = DdSketch::from_raw(0.01, vec![10, 20, 30], -1, 60, 300.0, 0.5, 6.0);
        a.merge(&b).unwrap();
        assert_eq!(a.store_counts, vec![11, 22, 33]);
        assert_eq!(a.store_offset, -1);
        assert_eq!(a.count, 66);
        assert_eq!(a.sum, 330.0);
        assert_eq!(a.min, 0.5);
        assert_eq!(a.max, 6.0);
    }

    #[test]
    fn test_merge_overlapping_offsets() {
        // a covers indices [-1, 0, 1]; b covers indices [0, 1, 2]
        let mut a = DdSketch::from_raw(0.01, vec![1, 1, 1], -1, 3, 3.0, 1.0, 3.0);
        let b = DdSketch::from_raw(0.01, vec![10, 10, 10], 0, 30, 30.0, 1.0, 3.0);
        a.merge(&b).unwrap();
        // Merged window is [-1, 0, 1, 2] → [1, 11, 11, 10]
        assert_eq!(a.store_counts, vec![1, 11, 11, 10]);
        assert_eq!(a.store_offset, -1);
        assert_eq!(a.count, 33);
    }

    #[test]
    fn test_merge_disjoint_offsets() {
        let mut a = DdSketch::from_raw(0.01, vec![1, 2], 0, 3, 3.0, 1.0, 2.0);
        let b = DdSketch::from_raw(0.01, vec![3, 4], 5, 7, 7.0, 5.0, 6.0);
        a.merge(&b).unwrap();
        // Window [0..7) → [1,2,0,0,0,3,4]
        assert_eq!(a.store_counts, vec![1, 2, 0, 0, 0, 3, 4]);
        assert_eq!(a.store_offset, 0);
    }

    #[test]
    fn test_apply_delta_additive_inside_store() {
        let mut base = DdSketch::from_raw(0.01, vec![1, 2, 3], -1, 6, 30.0, 1.0, 5.0);
        let delta = DdSketchDelta {
            buckets: vec![(-1, 4), (0, 8), (1, 12)],
            d_count: 24,
            d_sum: 120.0,
            min_changed: false,
            new_min: 0.0,
            max_changed: true,
            new_max: 9.0,
        };
        base.apply_delta(&delta);
        assert_eq!(base.store_counts, vec![5, 10, 15]);
        assert_eq!(base.count, 30);
        assert_eq!(base.sum, 150.0);
        assert_eq!(base.min, 1.0);
        assert_eq!(base.max, 9.0);
    }

    #[test]
    fn test_apply_delta_expands_store_on_new_bucket() {
        // Base covers [0..2]; delta adds a bucket at absolute index 4.
        let mut base = DdSketch::from_raw(0.01, vec![1, 2], 0, 3, 3.0, 1.0, 2.0);
        let delta = DdSketchDelta {
            buckets: vec![(4, 7)],
            d_count: 7,
            d_sum: 35.0,
            min_changed: false,
            new_min: 0.0,
            max_changed: true,
            new_max: 6.0,
        };
        base.apply_delta(&delta);
        assert_eq!(base.store_counts, vec![1, 2, 0, 0, 7]);
        assert_eq!(base.store_offset, 0);
        assert_eq!(base.count, 10);
        assert_eq!(base.max, 6.0);
    }

    #[test]
    fn test_apply_delta_matches_full_merge() {
        // Snapshot the sketch, add more samples via a merge, and confirm
        // the delta+apply path lands at the same state.
        let base = DdSketch::from_raw(0.01, vec![1, 2, 3], 0, 6, 12.0, 1.0, 3.0);
        let addition = DdSketch::from_raw(0.01, vec![10, 0, 20], 0, 30, 70.0, 0.5, 5.0);
        let mut via_merge = base.clone();
        via_merge.merge(&addition).unwrap();

        let delta = DdSketchDelta {
            buckets: vec![(0, 10), (2, 20)],
            d_count: 30,
            d_sum: 70.0,
            min_changed: true,
            new_min: 0.5,
            max_changed: true,
            new_max: 5.0,
        };
        let mut via_delta = base;
        via_delta.apply_delta(&delta);

        assert_eq!(via_delta.store_counts, via_merge.store_counts);
        assert_eq!(via_delta.count, via_merge.count);
        assert_eq!(via_delta.sum, via_merge.sum);
        assert_eq!(via_delta.min, via_merge.min);
        assert_eq!(via_delta.max, via_merge.max);
    }

    #[test]
    fn test_merge_alpha_mismatch() {
        let mut a = DdSketch::new(0.01);
        let b = DdSketch::new(0.02);
        assert!(a.merge(&b).is_err());
    }

    #[test]
    fn test_msgpack_round_trip() {
        let original = DdSketch::from_raw(0.01, vec![1, 2, 3], -2, 6, 30.0, 1.0, 5.0);
        let bytes = original.to_msgpack().unwrap();
        let decoded = DdSketch::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.store_counts, original.store_counts);
        assert_eq!(decoded.store_offset, original.store_offset);
        assert_eq!(decoded.count, original.count);
    }

    #[test]
    fn test_insert_and_quantile_lognormal() {
        // Ground-truth: insert a large i.i.d. log-normal sample into
        // a full sketch and sanity-check P50 / P90 / P99.
        let mut gt = DdSketch::new(0.01);
        let mut rng = 0xdead_beefu64;
        let mut next = || {
            // xorshift64*
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        // Box-Muller normal → log-normal(mu=3, sigma=0.7).
        let lognormal = |u: u64, v: u64| -> f64 {
            let r1 = (u as f64) / (u64::MAX as f64).max(1.0);
            let r2 = (v as f64) / (u64::MAX as f64).max(1.0);
            let z = (-2.0 * r1.max(1e-12).ln()).sqrt() * (2.0 * std::f64::consts::PI * r2).cos();
            (3.0 + 0.7 * z).exp()
        };
        for _ in 0..100_000 {
            gt.update(lognormal(next(), next()));
        }
        let p50 = gt.quantile(0.5).unwrap();
        let p99 = gt.quantile(0.99).unwrap();
        // Analytical P50 = exp(mu) = e^3 ≈ 20.09;
        // P99 ≈ exp(mu + sigma * Φ⁻¹(0.99)) = e^(3 + 0.7×2.326) ≈ 102.4.
        assert!(
            (p50 / 20.09).ln().abs() < 0.05,
            "P50 {} not close to 20.09",
            p50
        );
        assert!(
            (p99 / 102.4).ln().abs() < 0.05,
            "P99 {} not close to 102.4",
            p99
        );
    }

    /// Core accuracy claim for PRs #60-#63 end-to-end: building a
    /// sketch via `base + apply_delta()` produces quantile estimates
    /// within DDSketch's α bound of the ground-truth full-sketch
    /// path. If this fails, the paper's delta-reconstitution story
    /// is broken.
    #[test]
    fn test_delta_chain_preserves_quantile_accuracy() {
        let alpha = 0.01;
        let mut rng = 0xcafe_babeu64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let lognormal = |u: u64, v: u64| -> f64 {
            let r1 = (u as f64) / (u64::MAX as f64).max(1.0);
            let r2 = (v as f64) / (u64::MAX as f64).max(1.0);
            let z = (-2.0 * r1.max(1e-12).ln()).sqrt() * (2.0 * std::f64::consts::PI * r2).cos();
            (3.0 + 0.7 * z).exp()
        };

        // Path A (ground truth): one sketch, 50k samples inserted
        // directly.
        let mut full = DdSketch::new(alpha);
        // Path B (delta chain): a base sketch from the first 10k
        // samples, then 4 incremental "flushes" of 10k samples each,
        // each transmitted as a delta computed against the previous
        // snapshot.
        let mut reconstituted = DdSketch::new(alpha);
        let mut prev_snapshot = DdSketch::new(alpha); // what the "receiver" has cached

        let batch = 10_000;
        let batches = 5;
        for b in 0..batches {
            let mut this_batch = prev_snapshot.clone();
            for _ in 0..batch {
                let v = lognormal(next(), next());
                full.update(v);
                this_batch.update(v);
            }
            // Compute a "delta" = diff of this_batch vs prev_snapshot
            // in our in-memory struct shape. Matches what
            // `sketchlib-go/sketches/DDSketch/delta.go::ComputeDelta`
            // would put on the wire.
            let delta = compute_dd_delta(&prev_snapshot, &this_batch);
            if b == 0 {
                // First batch seeds the reconstituted sketch.
                reconstituted = this_batch.clone();
            } else {
                reconstituted.apply_delta(&delta);
            }
            prev_snapshot = this_batch;
        }

        // Both paths should see the same total count + sum (exact).
        assert_eq!(reconstituted.count, full.count, "count diverged");
        assert!(
            (reconstituted.sum - full.sum).abs() < 1e-6,
            "sum diverged: recon={} full={}",
            reconstituted.sum,
            full.sum
        );

        // And P50 / P90 / P99 should agree within α bound.
        for q in [0.5, 0.9, 0.99] {
            let got = reconstituted.quantile(q).unwrap();
            let want = full.quantile(q).unwrap();
            let rel_err = (got / want - 1.0).abs();
            assert!(
                rel_err <= alpha,
                "q={} rel_err={:.4} exceeds α={}: reconstituted={}, full={}",
                q,
                rel_err,
                alpha,
                got,
                want,
            );
        }
    }

    /// Helper used only in the delta-chain test: computes a
    /// `DdSketchDelta` from two snapshots. Mirrors the sketchlib-go
    /// `ComputeDelta` logic so the test exercises the wire-format
    /// path end-to-end.
    fn compute_dd_delta(snapshot: &DdSketch, current: &DdSketch) -> DdSketchDelta {
        let mut cells = Vec::new();
        if !current.store_counts.is_empty() {
            for (i, &c) in current.store_counts.iter().enumerate() {
                if c == 0 {
                    continue;
                }
                let k = current.store_offset + i as i32;
                let snap_count: u64 = if !snapshot.store_counts.is_empty() {
                    let idx = k as i64 - snapshot.store_offset as i64;
                    if idx >= 0 && (idx as usize) < snapshot.store_counts.len() {
                        snapshot.store_counts[idx as usize]
                    } else {
                        0
                    }
                } else {
                    0
                };
                let dc = c.saturating_sub(snap_count);
                if dc > 0 {
                    cells.push((k, dc));
                }
            }
        }
        let d_count = current.count as i64 - snapshot.count as i64;
        let d_sum = current.sum - snapshot.sum;
        let min_changed = current.count > 0 && (snapshot.count == 0 || current.min < snapshot.min);
        let max_changed = current.count > 0 && (snapshot.count == 0 || current.max > snapshot.max);
        DdSketchDelta {
            buckets: cells,
            d_count,
            d_sum,
            min_changed,
            new_min: current.min,
            max_changed,
            new_max: current.max,
        }
    }

    /// Cross-language byte-parity guard against `sketchlib-go`'s
    /// `DDSketch.SerializePortable` output for the deterministic input
    /// `(1..=50)` with `α = 0.01`. The hex blob below was captured
    /// from a `proto.Marshal` of the Go envelope (with `Producer` and
    /// `HashSpec` cleared, matching the
    /// `integration/parity/golden_test.go::TestGenerateGoldenFixtures`
    /// recipe). Any change to [`DdSketch::update`]'s bucket-store
    /// growth that breaks parity will surface here. Closes part of
    /// ProjectASAP/ASAPCollector#243.
    #[test]
    fn test_update_then_envelope_matches_sketchlib_go_bytes() {
        use crate::proto::sketchlib::{
            DdSketchState, SketchEnvelope, sketch_envelope::SketchState,
        };
        use prost::Message;

        let mut sk = DdSketch::new(0.01);
        for i in 1..=50 {
            sk.update(i as f64);
        }

        let state = DdSketchState {
            alpha: sk.wire_alpha(),
            store_counts: sk.store_counts.clone(),
            store_offset: sk.store_offset,
            count: sk.count,
            sum: sk.sum,
            min: if sk.count == 0 { f64::INFINITY } else { sk.min },
            max: if sk.count == 0 {
                f64::NEG_INFINITY
            } else {
                sk.max
            },
        };
        let envelope = SketchEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            sketch_state: Some(SketchState::Ddsketch(state)),
        };
        let mut got = Vec::with_capacity(envelope.encoded_len());
        envelope.encode(&mut got).expect("prost encode");

        // Byte string captured from sketchlib-go for the same input —
        // see `integration/parity/golden_test.go` and
        // `cross_language_parity::ddsketch_byte_parity_with_go` in
        // ASAPCollector. 432 bytes total: a `SketchEnvelope` proto
        // wrapping a `DDSketchState` whose `store_counts` is the Go
        // chunk-128 padded layout (offset = -64, len = 384).
        const GOLDEN_HEX: &str = "080172ab03096214ae47e17a843f128003000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000100000000000000000000000000000100000000000000000000010000000000000000010000000000000001000000000001000000000001000000000001000000010000000001000000010000010000000100000100000100000100000100010000010001000100010001000100010001000100010100010100010100010101000101010100010101010101010100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000187f2032290000000000ec934031000000000000f03f390000000000004940";
        let want = decode_hex(GOLDEN_HEX);
        assert_eq!(
            got,
            want,
            "DDSketch envelope bytes diverge from sketchlib-go golden \
             ({} bytes got vs {} bytes want)",
            got.len(),
            want.len(),
        );
    }

    fn decode_hex(s: &str) -> Vec<u8> {
        let bytes: Vec<u8> = s
            .as_bytes()
            .chunks(2)
            .map(|pair| {
                let high = hex_nibble(pair[0]);
                let low = hex_nibble(pair[1]);
                (high << 4) | low
            })
            .collect();
        bytes
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
