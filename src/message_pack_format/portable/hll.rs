//! Wire-format-aligned HyperLogLog types.
//!
//! Self-contained: maintains its own `Vec<u8>` register array. Parity
//! with Go's `HyperLogLog.SerializePortable` (and equivalence with
//! `sketches::HyperLogLogImpl<.., ErtlMLE, P14>`) is locked in by the
//! `test_update_then_envelope_matches_sketchlib_go_bytes` test below and
//! by `tests/sketches_go_parity_probe.rs`.

use serde::{Deserialize, Serialize};

use crate::message_pack_format::{Error as MsgPackError, MessagePackCodec};
use crate::{CANONICAL_HASH_SEED, DataInput, hash64_seeded};

/// HLL estimator variant. Mirrors `asap_sketchlib::proto::sketchlib::HllVariant`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HllVariant {
    Unspecified,
    Regular,
    Datafusion,
    Hip,
}

/// Sparse delta between two consecutive HLL snapshots — input shape for
/// [`HllSketch::apply_delta`]. Mirrors the `HLLDelta` proto in
/// `sketchlib-go/proto/hll/hll.proto`.
#[derive(Debug, Clone, Default)]
pub struct HllSketchDelta {
    pub updates: Vec<(u32, u8)>,
}

/// Minimal HLL state — registers + variant + precision. Register-wise
/// mergeable (max over aligned cells).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HllSketch {
    pub variant: HllVariant,
    pub precision: u32,
    pub registers: Vec<u8>,
    pub hip_kxq0: f64,
    pub hip_kxq1: f64,
    pub hip_est: f64,
}

impl HllSketch {
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
        if self.variant == HllVariant::Hip {
            self.hip_kxq0 += other.hip_kxq0;
            self.hip_kxq1 += other.hip_kxq1;
            self.hip_est += other.hip_est;
        }
        Ok(())
    }

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

    /// Insert a value. Math mirrors `sketches::HyperLogLogImpl::insert_with_hash`
    /// (CANONICAL_HASH_SEED, packed bucket/leading-zero); locked in by
    /// `tests/sketches_go_parity_probe.rs`.
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

    /// Estimate cardinality (Classic HLL estimator with small/large-range corrections).
    pub fn estimate(&self) -> f64 {
        let m = self.registers.len() as f64;
        if m == 0.0 {
            return 0.0;
        }
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

        if est <= m * 5.0 / 2.0 && zero_count != 0 {
            est = m * (m / zero_count as f64).ln();
        } else if est > 143_165_576.533 {
            let aux = i32::MAX as f64;
            est = -aux * (1.0 - est / aux).ln();
        }
        est
    }

    /// Return the proto-enum value Go's `HyperLogLog.SerializePortable`
    /// emits for this sketch's [`HllVariant`].
    #[inline]
    pub fn wire_proto_variant(&self) -> i32 {
        match self.variant {
            HllVariant::Unspecified => 0,
            HllVariant::Regular => 1,
            HllVariant::Datafusion => 2,
            HllVariant::Hip => 3,
        }
    }
}

impl MessagePackCodec for HllSketch {
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
        assert_eq!(h.registers, vec![4, 5, 6, 7]);
    }

    #[test]
    fn test_apply_delta_out_of_range() {
        let mut h = HllSketch::new(HllVariant::Regular, 2);
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
        let bytes = original.to_msgpack().unwrap();
        let decoded = HllSketch::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.registers, original.registers);
        assert_eq!(decoded.precision, original.precision);
        assert_eq!(decoded.hip_kxq0, 1.0);
    }

    /// Cross-language byte-parity guard against
    /// `sketchlib-go::HyperLogLog.SerializePortable` for the
    /// deterministic input stream `(1..=50i32).map(|i| (i as f64)
    /// .to_le_bytes())`.
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

        const GOLDEN_HEX: &str = include_str!("../../sketches/testdata/hll_envelope_golden.hex");
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
