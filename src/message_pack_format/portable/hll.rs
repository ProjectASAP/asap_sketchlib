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

    /// Compute a sparse, proto-marshalled `HLLDelta` of `self` against a
    /// `snapshot`. A register update is included when `self`'s register
    /// value increased over the snapshot's (`self[i] > snapshot[i]`); its
    /// carried value is the new (larger) register value.
    ///
    /// This is the Rust twin of the Go reference implementation's
    /// `ComputeRegisterDelta` + `SerializeRegisterDelta`. HLL uses max
    /// semantics, so only increases are meaningful and the delta is
    /// **lossless** — every increased register is carried, regardless of
    /// `threshold` (the parameter is accepted for a uniform delta API and
    /// has no effect here; an HLL register update is never dropped). The
    /// returned bytes are a `prost`-encoded
    /// [`crate::proto::sketchlib::HllDelta`], byte-identical to the Go
    /// `proto.Marshal(HLLDelta)` output for the same inputs (cross-language
    /// byte parity).
    ///
    /// Delta-against-empty: when `snapshot` is the all-zero sketch, every
    /// non-zero register of `self` is carried, so the result is this
    /// window's full register state encoded as a delta (no cross-window
    /// subtraction). HLL deltas carry only register max-updates — there are
    /// no DataPoint-level metric scalars to drop.
    pub fn compute_delta(&self, snapshot: &HllSketch, _threshold: u64) -> Vec<u8> {
        use crate::proto::sketchlib::{HllDelta as ProtoDelta, HllRegisterUpdate};
        use prost::Message;

        let cur = &self.registers;
        let snap = &snapshot.registers;
        let n = cur.len().min(snap.len());

        let mut updates: Vec<HllRegisterUpdate> = Vec::new();
        for i in 0..n {
            if cur[i] > snap[i] {
                updates.push(HllRegisterUpdate {
                    index: i as u32,
                    value: cur[i] as u32,
                });
            }
        }
        // Guard: if `self` has more registers than the snapshot (should not
        // happen at a fixed precision), carry all non-zero extras. Matches
        // the Go reference's trailing-register guard.
        for (i, &v) in cur.iter().enumerate().take(cur.len()).skip(n) {
            if v > 0 {
                updates.push(HllRegisterUpdate {
                    index: i as u32,
                    value: v as u32,
                });
            }
        }

        ProtoDelta { updates }.encode_to_vec()
    }

    /// Apply a `prost`-encoded [`crate::proto::sketchlib::HllDelta`] to this
    /// sketch in place (register max-merge). The Rust twin of the Go
    /// reference implementation's `DeserializeRegisterDelta` +
    /// `ApplyRegisterDelta`: each update sets
    /// `register[index] = max(register[index], value)`.
    ///
    /// Returns `Err` if `bytes` is not a valid `HLLDelta` proto or a
    /// register index is out of range for this sketch's precision.
    pub fn apply_delta_bytes(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use crate::proto::sketchlib::HllDelta as ProtoDelta;
        use prost::Message;

        let proto = ProtoDelta::decode(bytes)?;
        let delta = HllSketchDelta {
            updates: proto
                .updates
                .into_iter()
                .map(|u| (u.index, u.value as u8))
                .collect(),
        };
        self.apply_delta(&delta)
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

// ---------------------------------------------------------------------------
// Proto dual-read decode (DENSE tag 3 + SPARSE tag 7)
// ---------------------------------------------------------------------------

use crate::proto::sketchlib::{HllSparseRegisters, HyperLogLogState};

/// Decode the full dense register array from a `HyperLogLogState`, accepting
/// BOTH wire encodings emitted by sketchlib-go:
///   - DENSE (`registers`, tag 3): raw 1-byte-per-register array, or
///   - SPARSE (`registers_sparse`, tag 7): HLL++ style varint-packed
///     (index_delta, value) pairs for non-zero registers only.
///
/// The SPARSE field takes priority when present; otherwise the DENSE field is
/// used. Both reconstruct the identical `2^precision`-byte register array, so
/// downstream estimation / merge is encoding-agnostic. This is the Rust half of
/// the backend-decode-first rollout: it must ship before any producer emits
/// sparse. Mirrors the Go decoder in
/// `sketchlib-go/sketches/HLL/portable.go::registersFromState` +
/// `sparse.go::decodeSparseRegisters`.
pub fn registers_from_state(
    state: &HyperLogLogState,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(sp) = &state.registers_sparse {
        return decode_sparse_registers(sp);
    }
    Ok(state.registers.clone())
}

/// Reconstruct the dense register array from a sparse message. Inverse of the
/// Go encoder `sparse.go::encodeSparseRegisters`.
pub fn decode_sparse_registers(
    sp: &HllSparseRegisters,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let n = sp.num_registers as usize;
    let mut regs = vec![0u8; n];

    let packed = &sp.packed;
    let mut off = 0usize;
    let mut prev: usize = 0;
    let mut first = true;
    while off < packed.len() {
        let (delta, used) =
            read_uvarint(&packed[off..]).ok_or("hll: sparse index-delta varint corrupt")?;
        off += used;
        let (val, used) = read_uvarint(&packed[off..]).ok_or("hll: sparse value varint corrupt")?;
        off += used;

        let idx = prev + delta as usize;
        // First delta is an absolute index (prev=0); later deltas must be > 0
        // so indices stay strictly increasing and unique.
        if !first && delta == 0 {
            return Err(format!("hll: sparse non-increasing index at idx {idx}").into());
        }
        first = false;
        if idx >= n {
            return Err(format!("hll: sparse index {idx} out of range [0,{n})").into());
        }
        if val > 0xff {
            return Err(format!("hll: sparse value {val} exceeds u8 at idx {idx}").into());
        }
        regs[idx] = val as u8;
        prev = idx;
    }
    Ok(regs)
}

/// Encode the non-zero registers of `regs` into an [`HllSparseRegisters`]
/// message, byte-identical to the Go encoder `sparse.go::encodeSparseRegisters`.
/// Provided so a Rust producer (or the reverse-direction golden test) can emit
/// the same sparse wire form Go does.
pub fn encode_sparse_registers(regs: &[u8]) -> HllSparseRegisters {
    let mut packed: Vec<u8> = Vec::new();
    let mut prev: usize = 0;
    for (i, &v) in regs.iter().enumerate() {
        if v == 0 {
            continue;
        }
        write_uvarint(&mut packed, (i - prev) as u64);
        write_uvarint(&mut packed, v as u64);
        prev = i;
    }
    HllSparseRegisters {
        num_registers: regs.len() as u32,
        packed,
    }
}

/// Append one unsigned LEB128 varint to `out`. Matches Go's
/// `encoding/binary.PutUvarint`.
fn write_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Read one unsigned LEB128 varint, returning (value, bytes_consumed). Matches
/// Go's `encoding/binary.Uvarint`. Returns None on truncation / overflow.
fn read_uvarint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    for (i, &b) in buf.iter().enumerate() {
        if i >= 10 {
            return None; // varint too long for u64
        }
        if shift == 63 && b > 1 {
            return None; // overflow
        }
        result |= ((b & 0x7f) as u64) << shift;
        if b < 0x80 {
            return Some((result, i + 1));
        }
        shift += 7;
    }
    None
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
            // This parity golden predates the sparse field and intentionally
            // pins the DENSE wire form, so leave the sparse field unset.
            registers_sparse: None,
        };
        let envelope = SketchEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            // Golden pins the pre-sampling wire form; 0.0 is the proto3 default
            // (dual-read as 1.0) so the encoded bytes are unchanged.
            sample_p: 0.0,
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

    // ----- sparse full-state encoding -----

    use crate::proto::sketchlib::HyperLogLogState;

    /// A handful of (index, value) pairs that exercise small and large index
    /// deltas plus the maximum register value.
    fn sample_regs(n: usize) -> Vec<u8> {
        let mut regs = vec![0u8; n];
        regs[0] = 1; // first delta is absolute index 0
        regs[1] = 7; // adjacent index (delta 1)
        regs[300] = 51; // max p=14 register value (Q+1)
        regs[8191] = 12; // mid-range, multi-byte index delta
        regs[n - 1] = 3; // last register
        regs
    }

    #[test]
    fn sparse_round_trip_exact() {
        let regs = sample_regs(16384);
        let sp = encode_sparse_registers(&regs);
        assert_eq!(sp.num_registers, 16384);
        let decoded = decode_sparse_registers(&sp).unwrap();
        assert_eq!(decoded, regs, "sparse round trip must be exact");
    }

    #[test]
    fn registers_from_state_reads_both() {
        let regs = sample_regs(16384);

        // SPARSE: registers_sparse set, registers empty.
        let sparse_state = HyperLogLogState {
            variant: 2,
            precision: 14,
            registers: Vec::new(),
            hip_kxq0: 0.0,
            hip_kxq1: 0.0,
            hip_est: 0.0,
            registers_sparse: Some(encode_sparse_registers(&regs)),
        };
        assert_eq!(registers_from_state(&sparse_state).unwrap(), regs);

        // DENSE: registers set, registers_sparse None.
        let dense_state = HyperLogLogState {
            variant: 2,
            precision: 14,
            registers: regs.clone(),
            hip_kxq0: 0.0,
            hip_kxq1: 0.0,
            hip_est: 0.0,
            registers_sparse: None,
        };
        assert_eq!(registers_from_state(&dense_state).unwrap(), regs);
    }

    #[test]
    fn sparse_empty_is_all_zero() {
        let regs = vec![0u8; 16384];
        let sp = encode_sparse_registers(&regs);
        assert!(sp.packed.is_empty());
        assert_eq!(decode_sparse_registers(&sp).unwrap(), regs);
    }

    #[test]
    fn sparse_rejects_out_of_range_index() {
        // packed = uvarint(delta=5) , uvarint(value=3) but num_registers=4.
        let sp = HllSparseRegisters {
            num_registers: 4,
            packed: vec![5, 3],
        };
        assert!(decode_sparse_registers(&sp).is_err());
    }

    /// `compute_delta` against an EMPTY snapshot reconstructs the window's
    /// full register state when its bytes are applied to a fresh empty
    /// sketch (register max-merge round-trip). HLL deltas are lossless —
    /// every increased register is carried.
    #[test]
    fn test_compute_delta_against_empty_round_trips() {
        let mut window = HllSketch::new(HllVariant::Datafusion, 14);
        for i in 0..5000u64 {
            window.update(&i.to_le_bytes());
        }
        let empty = HllSketch::new(HllVariant::Datafusion, 14);

        let delta_bytes = window.compute_delta(&empty, 0);

        let mut reconstructed = HllSketch::new(HllVariant::Datafusion, 14);
        reconstructed.apply_delta_bytes(&delta_bytes).unwrap();

        assert_eq!(reconstructed.registers, window.registers);
    }

    /// A delta computed between two non-empty snapshots reconstructs the
    /// current sketch when applied to the base (max-merge of the registers
    /// that increased).
    #[test]
    fn test_compute_delta_then_apply_matches_current() {
        let mut base = HllSketch::new(HllVariant::Datafusion, 12);
        for i in 0..2000u64 {
            base.update(&i.to_le_bytes());
        }
        let mut current = base.clone();
        for i in 2000..5000u64 {
            current.update(&i.to_le_bytes());
        }

        let delta_bytes = current.compute_delta(&base, 0);
        let mut reconstructed = base.clone();
        reconstructed.apply_delta_bytes(&delta_bytes).unwrap();
        assert_eq!(reconstructed.registers, current.registers);
    }

    /// Cross-language byte-parity guard: `compute_delta` against an empty
    /// snapshot must emit bytes identical to the Go reference
    /// implementation's `SerializeRegisterDelta(ComputeRegisterDelta(empty,
    /// current))` for the same precision-14 sketch fed `(1..=50)` as f64
    /// little-endian byte values. A delta-against-empty carries every
    /// non-zero register as an `(index, value)` update; the golden hex was
    /// captured from a `proto.Marshal` of the Go reference's `HLLDelta`.
    #[test]
    fn test_compute_delta_matches_go_golden_bytes() {
        let mut current = HllSketch::new(HllVariant::Datafusion, 14);
        for i in 1..=50i32 {
            let v = i as f64;
            current.update(&v.to_le_bytes());
        }
        let empty = HllSketch::new(HllVariant::Datafusion, 14);
        let got = current.compute_delta(&empty, 0);

        // Captured from the Go reference implementation's
        // SerializeRegisterDelta(ComputeRegisterDelta(empty, current)) for the
        // same input.
        const GOLDEN_HEX: &str = "0a04085810010a0508930510020a0508931110010a0508e31110010a0508fa1510010a0508d71910010a0508881b10010a0508ba2310010a0508ec2310010a0508d62410010a0508ff2510020a0508ae2610010a0508b22810020a0508bb2810020a0508dc3210020a0508f63510020a0508eb3610010a0508b23910040a0508da3910040a0508853a10050a0508f14110020a0508974310020a05089c4510050a0508de4810020a0508b64910020a0508c74910010a0508c54d10010a0508ed4d10020a05088d4e10020a0508b35210030a0508805410020a0508e75a10020a0508f85a10060a0508835b10030a0508b75b10020a0508c25b10060a0508de5c10010a0508dd6310010a0508876410010a0508e56410050a0508e66610010a0508d66710040a0508e86710010a0508e06c10010a0508877110040a0508ad7210010a0508e97510010a0508877710030a0508d37910010a0508db7b1003";
        let want = decode_hex(GOLDEN_HEX);
        assert_eq!(
            got,
            want,
            "HLL delta bytes diverge from the Go reference golden \
             ({} bytes got vs {} bytes want)",
            got.len(),
            want.len(),
        );
    }
}
