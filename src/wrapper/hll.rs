//! Wire-format-aligned HyperLogLog types.

use crate::message_pack_format::{Error as MsgPackError, MessagePackCodec};
use crate::{CANONICAL_HASH_SEED, DataInput, hash64_seeded};
use rmp_serde::encode::Error as RmpEncodeError;
use serde::{Deserialize, Serialize};

// =====================================================================
// ASAP runtime wire-format-aligned variant .
//
// `HllSketch` and `HllSketchDelta` below are the public-field,
// proto-decode-friendly types consumed by the ASAP query engine
// accumulators. The high-throughput in-process variant above
// (`HyperLogLogImpl`/`HyperLogLog`) keeps its original design. Note:
// the wire-format delta type was renamed `HllDelta` -> `HllSketchDelta`
// to avoid collision with `octo_delta::HllDelta` (single-register,
// octo-runtime path).
// =====================================================================

// HyperLogLog sketch — register-wise mergeable cardinality estimator.
//
// Parallel to `count_sketch::CountSketch`: the minimum viable surface
// needed for the modified-OTLP `Metric.data = HLLSketch{…}` hot path
// (PR C-CountSketch follow-up). Wraps a flat `Vec<u8>` of register
// values (length = `2^precision`) and merges element-wise by taking
// the maximum across aligned registers, which is the standard HLL
// merge semantics.
//
// The wire format is the protobuf-encoded
// `asap_sketchlib::proto::sketchlib::HyperLogLogState` emitted by
// DataCollector's `hllprocessor`. This type carries the register
// bytes and the variant/precision metadata losslessly, so the
// merge + store round-trip works end-to-end. Cardinality estimation
// against stored HLL data is intentionally deferred to a follow-up
// — queries currently return a placeholder error and fall through
// to the §5.2 fallback.

// (de-duplicated) use serde::{Deserialize, Serialize};

/// HLL estimator variant. Mirrors `asap_sketchlib::proto::sketchlib::HllVariant`
/// so the proto round-trip preserves the algorithm identity — the three
/// variants are not mutually compatible on register contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HllVariant {
    Unspecified,
    Regular,
    Datafusion,
    Hip,
}

/// Sparse delta between two consecutive HLL snapshots — the input
/// shape for [`HllSketch::apply_delta`]. Mirrors the `HLLDelta` proto
/// in `sketchlib-go/proto/hll/hll.proto` (and its Rust bindings
/// vendored in `asap_otel_proto::sketchlib::v1`). HLL registers merge
/// with max semantics, so a delta carries only the register indices
/// whose value increased since the last snapshot.
#[derive(Debug, Clone, Default)]
pub struct HllSketchDelta {
    /// `(register_index, new_value)` pairs. `new_value` is the full
    /// post-update register value; `apply_delta` does
    /// `registers[i] = max(registers[i], new_value)`.
    pub updates: Vec<(u32, u8)>,
}

/// Minimal HLL state — registers + variant + precision. Register-wise
/// mergeable (max over aligned cells).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HllSketch {
    pub variant: HllVariant,
    pub precision: u32,
    /// Flat register array, length = `2^precision`.
    pub registers: Vec<u8>,
    /// HIP accumulator components — populated only when `variant == Hip`.
    pub hip_kxq0: f64,
    pub hip_kxq1: f64,
    pub hip_est: f64,
}

impl HllSketch {
    /// Construct an empty sketch at the given precision.
    pub fn new(variant: HllVariant, precision: u32) -> Self {
        let n = 1usize << precision;
        Self {
            variant,
            precision,
            registers: vec![0u8; n],
            hip_kxq0: 0.0,
            hip_kxq1: 0.0,
            hip_est: 0.0,
        }
    }

    /// Construct from pre-built register bytes (used by the modified-OTLP
    /// proto-decode path).
    pub fn from_raw(
        variant: HllVariant,
        precision: u32,
        registers: Vec<u8>,
        hip_kxq0: f64,
        hip_kxq1: f64,
        hip_est: f64,
    ) -> Self {
        Self {
            variant,
            precision,
            registers,
            hip_kxq0,
            hip_kxq1,
            hip_est,
        }
    }

    /// Merge one other sketch into self via register-wise max. Both
    /// operands must have identical variant and precision.
    pub fn merge(
        &mut self,
        other: &HllSketch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.variant != other.variant {
            return Err(format!(
                "HllSketch variant mismatch: self={:?}, other={:?}",
                self.variant, other.variant
            )
            .into());
        }
        if self.precision != other.precision {
            return Err(format!(
                "HllSketch precision mismatch: self={}, other={}",
                self.precision, other.precision
            )
            .into());
        }
        if self.registers.len() != other.registers.len() {
            return Err(format!(
                "HllSketch register-length mismatch: self={}, other={}",
                self.registers.len(),
                other.registers.len()
            )
            .into());
        }
        for (s, o) in self.registers.iter_mut().zip(other.registers.iter()) {
            if *o > *s {
                *s = *o;
            }
        }
        // HIP accumulators add on merge (each source carried its own
        // running estimate; merged state inherits the combined
        // components).
        if self.variant == HllVariant::Hip {
            self.hip_kxq0 += other.hip_kxq0;
            self.hip_kxq1 += other.hip_kxq1;
            self.hip_est += other.hip_est;
        }
        Ok(())
    }

    /// Apply a sparse register delta in place. Matches the
    /// `registers[i] = max(registers[i], new_value)` logic in
    /// `sketchlib-go/sketches/HLL/delta.go::ApplyRegisterDelta`. Used
    /// by the backend ingest path to reconstitute a full sketch from
    /// a base snapshot + subsequent delta-transmission frames (paper
    /// §6.2 B3 / B4 baselines).
    ///
    /// Returns `Err` if any delta index is out of range for the
    /// sketch's precision — indicating a precision mismatch between
    /// the snapshot this sketch was built from and the delta sender.
    pub fn apply_delta(
        &mut self,
        delta: &HllSketchDelta,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let n = self.registers.len();
        for (idx, new_val) in &delta.updates {
            let i = *idx as usize;
            if i >= n {
                return Err(format!(
                    "HllSketchDelta index {i} out of range (precision={} → {n} registers)",
                    self.precision
                )
                .into());
            }
            if *new_val > self.registers[i] {
                self.registers[i] = *new_val;
            }
        }
        Ok(())
    }

    /// Merge a slice of references into a single new sketch. All inputs
    /// must share the same variant and precision; returns `Err` on
    /// mismatch or an empty input.
    pub fn merge_refs(
        inputs: &[&HllSketch],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let first = inputs
            .first()
            .ok_or("HllSketch::merge_refs called with empty input")?;
        let mut merged = HllSketch::new(first.variant, first.precision);
        for hll in inputs {
            merged.merge(hll)?;
        }
        Ok(merged)
    }

    /// Insert a value into the sketch. Hashes the bytes with the
    /// canonical seed, takes the leading `precision` bits as the
    /// register index, then `1 + leading_zeros` of the remaining
    /// bits as the candidate register value, applied with max
    /// semantics. Mirrors `HyperLogLogImpl::insert_with_hash` (line
    /// 131) — re-stated here so the wire-format type doesn't need
    /// to construct a parameterized typed sketch on every insert.
    pub fn update(&mut self, value: &[u8]) {
        let hashed_val = hash64_seeded(CANONICAL_HASH_SEED, &DataInput::Bytes(value));
        let p = self.precision as usize;
        let register_bits = (u64::BITS as usize) - p;
        let p_mask: u64 = (1u64 << p) - 1;
        let bucket_num = ((hashed_val >> register_bits) & p_mask) as usize;
        let leading_zero = ((hashed_val << p) + p_mask).leading_zeros() as u8 + 1;
        if bucket_num < self.registers.len() && leading_zero > self.registers[bucket_num] {
            self.registers[bucket_num] = leading_zero;
        }
    }

    /// Estimate the cardinality represented by this sketch.
    /// Re-implements the Classic HLL estimator from
    /// `HyperLogLogImpl::<Classic, _, _>::estimate` (line 203) with
    /// small/large range corrections, returning `f64` for parity with
    /// the other wire-format estimates.
    pub fn estimate(&self) -> f64 {
        let m = self.registers.len() as f64;
        if m == 0.0 {
            return 0.0;
        }
        // Indicator function: sum 2^-reg_val.
        let mut z = 0.0_f64;
        let mut zero_count = 0usize;
        for &reg_val in &self.registers {
            if reg_val == 0 {
                zero_count += 1;
            }
            z += 2f64.powi(-(reg_val as i32));
        }
        let indicator = 1.0 / z;

        let alpha_m = 0.7213 / (1.0 + 1.079 / m);
        let mut est = alpha_m * m * m * indicator;

        // Small-range correction (linear counting).
        if est <= m * 5.0 / 2.0 && zero_count != 0 {
            est = m * (m / zero_count as f64).ln();
        } else if est > 143_165_576.533 {
            // Large-range correction.
            let aux = i32::MAX as f64;
            est = -aux * (1.0 - est / aux).ln();
        }
        est
    }

    /// Return the proto enum value Go's `HyperLogLog.SerializePortable`
    /// emits on the wire for this sketch's [`HllVariant`]. Mirrors the
    /// `sketchlib-go::sketches/HLL/portable.go::SerializePortable` mapping
    /// (`HyperLogLog -> HLL_VARIANT_DATAFUSION`,
    /// `HyperLogLogVariant{Regular} -> HLL_VARIANT_REGULAR`,
    /// `HyperLogLogHIP -> HLL_VARIANT_HIP`). The returned `i32` is byte-
    /// identical to Go's emitted `HyperLogLogState.variant` field, so a
    /// wire-format-aligned producer can encode a `HyperLogLogState`
    /// matching `sketchlib-go::HyperLogLog.SerializePortable` byte-for-
    /// byte. Closes part of ProjectASAP/ASAPCollector#243.
    #[inline]
    pub fn wire_proto_variant(&self) -> i32 {
        // Numeric values match the proto enum (sketchlib.v1.HLLVariant in
        // both repos): UNSPECIFIED=0, REGULAR=1, ERTL_MLE/DATAFUSION=2,
        // HIP=3. Go's `HyperLogLog.SerializePortable` (the high-throughput
        // DataFusion-style sketch) emits value 2; we mirror it here so a
        // Rust producer fed the same input stream lands the same variant
        // byte on the wire.
        match self.variant {
            HllVariant::Unspecified => 0,
            HllVariant::Regular => 1,
            HllVariant::Datafusion => 2,
            HllVariant::Hip => 3,
        }
    }

    /// Serialize to MessagePack bytes. Thin shim over
    /// [`MessagePackCodec::to_msgpack`].
    pub fn serialize_msgpack(&self) -> Result<Vec<u8>, RmpEncodeError> {
        self.to_msgpack().map_err(MsgPackError::into_encode)
    }

    /// Thin shim over [`MessagePackCodec::from_msgpack`].
    pub fn deserialize_msgpack(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::from_msgpack(buffer).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("Failed to deserialize HllSketch from MessagePack: {e}").into()
        })
    }
}

#[cfg(test)]
mod tests_wire_hll {
    use super::*;

    #[test]
    fn test_new_empty() {
        let h = HllSketch::new(HllVariant::Regular, 4);
        assert_eq!(h.registers.len(), 16);
        assert!(h.registers.iter().all(|&r| r == 0));
    }

    #[test]
    fn test_merge_register_wise_max() {
        let mut a = HllSketch::from_raw(HllVariant::Regular, 2, vec![1, 5, 3, 7], 0.0, 0.0, 0.0);
        let b = HllSketch::from_raw(HllVariant::Regular, 2, vec![4, 2, 6, 0], 0.0, 0.0, 0.0);
        a.merge(&b).unwrap();
        assert_eq!(a.registers, vec![4, 5, 6, 7]);
    }

    #[test]
    fn test_apply_delta_max_semantics() {
        let mut h = HllSketch::from_raw(HllVariant::Regular, 2, vec![1, 5, 3, 7], 0.0, 0.0, 0.0);
        let delta = HllSketchDelta {
            updates: vec![(0, 4), (1, 2), (2, 6), (3, 0)],
        };
        h.apply_delta(&delta).unwrap();
        // reg[0]: max(1,4)=4, reg[1]: max(5,2)=5, reg[2]: max(3,6)=6,
        // reg[3]: max(7,0)=7.
        assert_eq!(h.registers, vec![4, 5, 6, 7]);
    }

    #[test]
    fn test_apply_delta_out_of_range() {
        let mut h = HllSketch::new(HllVariant::Regular, 2); // 4 registers
        let delta = HllSketchDelta {
            updates: vec![(7, 3)],
        };
        assert!(h.apply_delta(&delta).is_err());
    }

    #[test]
    fn test_apply_delta_matches_full_merge() {
        let base = HllSketch::from_raw(HllVariant::Regular, 2, vec![1, 5, 3, 7], 0.0, 0.0, 0.0);
        let addition = HllSketch::from_raw(HllVariant::Regular, 2, vec![4, 0, 6, 0], 0.0, 0.0, 0.0);
        let mut via_merge = base.clone();
        via_merge.merge(&addition).unwrap();

        let delta = HllSketchDelta {
            updates: vec![(0, 4), (2, 6)],
        };
        let mut via_delta = base;
        via_delta.apply_delta(&delta).unwrap();
        assert_eq!(via_delta.registers, via_merge.registers);
    }

    #[test]
    fn test_merge_variant_mismatch() {
        let mut a = HllSketch::new(HllVariant::Regular, 4);
        let b = HllSketch::new(HllVariant::Datafusion, 4);
        assert!(a.merge(&b).is_err());
    }

    #[test]
    fn test_merge_precision_mismatch() {
        let mut a = HllSketch::new(HllVariant::Regular, 4);
        let b = HllSketch::new(HllVariant::Regular, 5);
        assert!(a.merge(&b).is_err());
    }

    #[test]
    fn test_merge_refs() {
        let a = HllSketch::from_raw(HllVariant::Regular, 1, vec![1, 0], 0.0, 0.0, 0.0);
        let b = HllSketch::from_raw(HllVariant::Regular, 1, vec![0, 3], 0.0, 0.0, 0.0);
        let c = HllSketch::from_raw(HllVariant::Regular, 1, vec![2, 2], 0.0, 0.0, 0.0);
        let merged = HllSketch::merge_refs(&[&a, &b, &c]).unwrap();
        assert_eq!(merged.registers, vec![2, 3]);
    }

    #[test]
    fn test_update_then_estimate_within_2pct() {
        // Insert N distinct keys; the HLL estimate should be within
        // ~2% of N for precision=12 (4096 registers, std err ≈ 1.6%).
        let n: usize = 50_000;
        let mut h = HllSketch::new(HllVariant::Regular, 12);
        for i in 0..n {
            let key = format!("key-{i}");
            h.update(key.as_bytes());
        }
        let est = h.estimate();
        let rel_err = (est - n as f64).abs() / n as f64;
        assert!(
            rel_err < 0.02,
            "HLL estimate {est} not within 2% of {n} (rel_err {rel_err:.4})",
        );
    }

    #[test]
    fn test_estimate_empty_is_zero() {
        let h = HllSketch::new(HllVariant::Regular, 4);
        assert_eq!(h.estimate(), 0.0);
    }

    #[test]
    fn test_msgpack_round_trip() {
        let original = HllSketch::from_raw(
            HllVariant::Hip,
            3,
            vec![0, 1, 2, 3, 4, 5, 6, 7],
            1.0,
            2.0,
            3.0,
        );
        let bytes = original.serialize_msgpack().unwrap();
        let decoded = HllSketch::deserialize_msgpack(&bytes).unwrap();
        assert_eq!(decoded.registers, original.registers);
        assert_eq!(decoded.precision, original.precision);
        assert_eq!(decoded.hip_kxq0, 1.0);
    }

    /// Cross-language byte-parity guard against
    /// `sketchlib-go::HyperLogLog.SerializePortable` for the
    /// deterministic input stream `(1..=50i32).map(|i| (i as f64).
    /// to_le_bytes())`. The hex blob below was captured from a
    /// `proto.Marshal` of the Go envelope (with `Producer` and
    /// `HashSpec` cleared, matching the
    /// `integration/parity/golden_test.go::TestGenerateGoldenFixtures`
    /// recipe).
    ///
    /// Hash alignment: both producers reach `InsertWithHash` /
    /// `insert_with_hash` with the same `u64` because
    /// `sketchlib-go::common.FromBytes` does
    /// `HashIt(CanonicalHashSeed=5, key)` and
    /// `asap_sketchlib::HllSketch::update` does
    /// `hash64_seeded(CANONICAL_HASH_SEED=5, &DataInput::Bytes(key))`,
    /// both of which call `xxh3_64(seed=seedList[5]=0x6a09e667, key)`.
    ///
    /// Variant alignment: Go's free-`HyperLogLog`
    /// `SerializePortable` emits `HLL_VARIANT_DATAFUSION = 2`. Mirror
    /// it via [`HllVariant::Datafusion`] (the Rust proto enum value
    /// `ErtlMle = 2` is the same wire byte). Any future change to
    /// [`HllSketch::update`]'s hash path or to
    /// [`HllSketch::wire_proto_variant`] that breaks parity will
    /// surface here. Closes part of ProjectASAP/ASAPCollector#243.
    #[test]
    fn test_update_then_envelope_matches_sketchlib_go_bytes() {
        use crate::proto::sketchlib::{
            HyperLogLogState, SketchEnvelope, sketch_envelope::SketchState,
        };
        use prost::Message;

        let mut sk = HllSketch::new(HllVariant::Datafusion, 14);
        for i in 1..=50i32 {
            let v = i as f64;
            sk.update(&v.to_le_bytes());
        }

        let state = HyperLogLogState {
            variant: sk.wire_proto_variant(),
            precision: sk.precision,
            registers: sk.registers.clone(),
            hip_kxq0: sk.hip_kxq0,
            hip_kxq1: sk.hip_kxq1,
            hip_est: sk.hip_est,
        };
        let envelope = SketchEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            sketch_state: Some(SketchState::Hll(state)),
        };
        let mut got = Vec::with_capacity(envelope.encoded_len());
        envelope.encode(&mut got).expect("prost encode");

        // Hex blob captured from `sketchlib-go::HyperLogLog.SerializePortable`
        // for the (1..=50) IEEE-754-LE byte-key input — see
        // `integration/parity/golden_test.go` and
        // `cross_language_parity::hll_byte_parity_with_go` in
        // ASAPCollector. 16 398 bytes total: a `SketchEnvelope` proto
        // wrapping a `HyperLogLogState{variant=DATAFUSION,
        // precision=14, registers=<16 384 bytes, 50 nonzero>}`.
        const GOLDEN_HEX: &str = include_str!("../sketches/testdata/hll_envelope_golden.hex");
        let want = decode_hex(GOLDEN_HEX);
        assert_eq!(
            got.len(),
            want.len(),
            "HLL envelope length differs: got {} bytes, want {} bytes",
            got.len(),
            want.len(),
        );
        assert_eq!(
            got, want,
            "HLL envelope bytes diverge from sketchlib-go golden"
        );
    }

    fn decode_hex(s: &str) -> Vec<u8> {
        let s = s.trim();
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
