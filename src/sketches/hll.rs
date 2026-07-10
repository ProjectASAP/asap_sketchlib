/*
 * Copyright The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// ----------------------------------------------------------------
// This file contains code derived from multiple Apache Software Foundation projects.
//
// 1. HyperLogLog<ErtlMLE> Implementation:
//    - Originally derived from Apache DataFusion's HyperLogLog component
//    - Source: https://github.com/apache/datafusion/blob/main/datafusion/functions-aggregate/src/hyperloglog.rs
//    - Algorithm: Otmar Ertl's MLE estimator (arXiv:1702.01284)
//
// 2. HyperLogLogHIP Implementation:
//    - Ported from: Apache DataSketches (Java)
//    - Source: https://github.com/apache/datasketches-java
//    - Algorithm: HIP (Kevin J. Lang, arXiv:1708.06839)
//    - Note: This Rust implementation is a port based on the original Java logic.
//
// Modifications:
// - Adapted both implementations to use a unified `HllBucketList` storage.
// - Refactored into a generic `HyperLogLog<Variant>` structure.
// ----------------------------------------------------------------

use crate::message_pack_format::envelope;
use crate::structures::fixed_structure::{
    HllBucketListP12, HllBucketListP14, HllBucketListP16, HllRegisterStorage,
};
use crate::{CANONICAL_HASH_SEED, DataInput, DefaultXxHasher, SketchHasher, hash64_seeded};
use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

/// Generic HyperLogLog sketch parameterized by estimation variant, register storage, and hasher.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct HyperLogLogImpl<
    Variant,
    Registers: HllRegisterStorage,
    H: SketchHasher = DefaultXxHasher,
> {
    registers: Registers,
    #[serde(skip)]
    _marker: PhantomData<Variant>,
    #[serde(skip)]
    _hasher: PhantomData<H>,
}

/// Marker type selecting the classic HyperLogLog estimation algorithm.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct Classic;
/// Marker type selecting the Ertl MLE estimation algorithm (arXiv:1702.01284).
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct ErtlMLE;

/// HyperLogLog variant using the Historic Inverse Probability (HIP) estimator for improved accuracy.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct HyperLogLogHIPImpl<Registers: HllRegisterStorage> {
    registers: Registers,
    kxq0: f64,
    kxq1: f64,
    est: f64,
}

/// HyperLogLog with 12-bit precision.
pub type HyperLogLogP12<Variant, H = DefaultXxHasher> =
    HyperLogLogImpl<Variant, HllBucketListP12, H>;
/// HyperLogLog with 14-bit precision.
pub type HyperLogLogP14<Variant, H = DefaultXxHasher> =
    HyperLogLogImpl<Variant, HllBucketListP14, H>;
/// HyperLogLog with 16-bit precision.
pub type HyperLogLogP16<Variant, H = DefaultXxHasher> =
    HyperLogLogImpl<Variant, HllBucketListP16, H>;
/// Default HyperLogLog alias using 14-bit precision.
pub type HyperLogLog<Variant, H = DefaultXxHasher> = HyperLogLogP14<Variant, H>;

/// HIP HyperLogLog with 12-bit precision.
pub type HyperLogLogHIPP12 = HyperLogLogHIPImpl<HllBucketListP12>;
/// HIP HyperLogLog with 14-bit precision.
pub type HyperLogLogHIPP14 = HyperLogLogHIPImpl<HllBucketListP14>;
/// HIP HyperLogLog with 16-bit precision.
pub type HyperLogLogHIPP16 = HyperLogLogHIPImpl<HllBucketListP16>;
/// Default HIP HyperLogLog alias using 14-bit precision.
pub type HyperLogLogHIP = HyperLogLogHIPP14;

/// Maps an in-memory HLL estimator variant to its ASAPv1 `kind_id`.
pub trait HllWireVariant {
    /// The `[family, variant]` kind_id for this estimator.
    const WIRE_KIND_ID: &'static [u8];
}

impl HllWireVariant for Classic {
    const WIRE_KIND_ID: &'static [u8] = HLL_KIND_CLASSIC;
}

impl HllWireVariant for ErtlMLE {
    const WIRE_KIND_ID: &'static [u8] = HLL_KIND_ERTL_MLE;
}

/// HLL payload (ASAPv1 §3.1), serialized as a msgpack **array** (`to_vec`,
/// positional). `registers` is a msgpack `bin` (one byte per register, matching
/// Go's `[]byte`) via `serde_bytes` rather than serde's default `array<u8>`.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct HllPayloadPlain {
    #[serde(with = "serde_bytes")]
    pub(crate) registers: Vec<u8>,
}

/// HIP payload: the register bin plus the three HIP running scalars.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct HllPayloadHip {
    #[serde(with = "serde_bytes")]
    pub(crate) registers: Vec<u8>,
    pub(crate) hip_kxq0: f64,
    pub(crate) hip_kxq1: f64,
    pub(crate) hip_est: f64,
}

const HLL_KIND_FAMILY: u8 = 0x01;
pub(crate) const HLL_KIND_CLASSIC: &[u8] = &[HLL_KIND_FAMILY, 0x01];
pub(crate) const HLL_KIND_ERTL_MLE: &[u8] = &[HLL_KIND_FAMILY, 0x02];
pub(crate) const HLL_KIND_HIP: &[u8] = &[HLL_KIND_FAMILY, 0x03];

/// Descriptor metadata for an HLL sketch (ASAPv1 §2), serialized as a msgpack
/// **map** (`to_vec_named`) with keys in this declaration order — the canonical
/// order the wire spec fixes. HLL carries the hash spec (minus the registered
/// profile's `seed_list`, resolved from `hash_profile_id`) plus its one
/// structural param, `precision`.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct HllMetadata {
    pub(crate) metadata_version: u8,
    pub(crate) hash_profile_id: String,
    pub(crate) hash_algorithm: String,
    pub(crate) seed_derivation: String,
    pub(crate) input_encoding: String,
    pub(crate) canonical_seed_index: u32,
    pub(crate) precision: u32,
}

pub(crate) fn standard_hll_metadata(precision: u32) -> HllMetadata {
    HllMetadata {
        metadata_version: 1,
        hash_profile_id: envelope::HASH_PROFILE_PROJECTASAP_XXH3_V1.to_string(),
        hash_algorithm: envelope::HASH_ALGORITHM_XXH3_64_128.to_string(),
        seed_derivation: envelope::HASH_SEED_DERIVATION_INDEX_WRAP.to_string(),
        input_encoding: envelope::HASH_INPUT_ENCODING_PROJECTASAP_V1.to_string(),
        canonical_seed_index: crate::CANONICAL_HASH_SEED as u32,
        precision,
    }
}

/// Validate the envelope for a known target and return the raw payload bytes.
fn validated_hll_payload<'a>(
    bytes: &'a [u8],
    expected_kind_id: &[u8],
    expected_precision: u32,
) -> Result<&'a [u8], RmpDecodeError> {
    let (kind_id, metadata, payload) =
        envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
    if kind_id != expected_kind_id {
        return Err(RmpDecodeError::Uncategorized(format!(
            "HLL kind_id mismatch: stored {kind_id:?}, expected {expected_kind_id:?}"
        )));
    }
    let meta: HllMetadata = from_slice(metadata)?;
    if meta != standard_hll_metadata(expected_precision) {
        return Err(RmpDecodeError::Uncategorized(
            "ASAPv1 HLL envelope: metadata mismatch".to_string(),
        ));
    }
    Ok(payload)
}

/// Rebuild register storage from the payload's register bin.
fn registers_from_bytes<Registers: HllRegisterStorage>(
    registers: &[u8],
) -> Result<Registers, RmpDecodeError> {
    if registers.len() != Registers::NUM_REGISTERS {
        return Err(RmpDecodeError::Uncategorized(format!(
            "HLL register length mismatch: stored {}, expected {}",
            registers.len(),
            Registers::NUM_REGISTERS
        )));
    }
    let mut out = Registers::default();
    out.as_mut_slice().copy_from_slice(registers);
    Ok(out)
}

impl<Variant, Registers: HllRegisterStorage, H: SketchHasher> Default
    for HyperLogLogImpl<Variant, Registers, H>
{
    fn default() -> Self {
        Self::new_base()
    }
}

// Core HyperLogLog logic (hash-based operations + serialization).
impl<Variant, Registers: HllRegisterStorage, H: SketchHasher>
    HyperLogLogImpl<Variant, Registers, H>
{
    fn new_base() -> Self {
        Self {
            registers: Registers::default(),
            _marker: PhantomData,
            _hasher: PhantomData,
        }
    }

    /// Serializes the sketch into an ASAPv1 MessagePack envelope.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
    where
        Variant: HllWireVariant,
    {
        let metadata =
            rmp_serde::to_vec_named(&standard_hll_metadata(Registers::PRECISION as u32))?;
        let payload = rmp_serde::to_vec(&HllPayloadPlain {
            registers: self.registers.as_slice().to_vec(),
        })?;
        Ok(envelope::encode(Variant::WIRE_KIND_ID, &metadata, &payload))
    }

    /// Deserializes a sketch from an ASAPv1 MessagePack envelope.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
    where
        Variant: HllWireVariant,
    {
        let payload =
            validated_hll_payload(bytes, Variant::WIRE_KIND_ID, Registers::PRECISION as u32)?;
        let payload: HllPayloadPlain = from_slice(payload)?;
        let registers = registers_from_bytes::<Registers>(&payload.registers)?;
        Ok(Self {
            registers,
            _marker: PhantomData,
            _hasher: PhantomData,
        })
    }

    /// Borrow the raw register byte slice (one byte per register).
    pub fn registers_as_slice(&self) -> &[u8] {
        self.registers.as_slice()
    }

    /// Inserts a pre-hashed value into the sketch.
    #[inline(always)]
    pub fn insert_with_hash(&mut self, hashed_val: u64) {
        let bucket_num = ((hashed_val >> Registers::REGISTER_BITS) & Registers::P_MASK) as usize;
        let leading_zero =
            ((hashed_val << Registers::PRECISION) + Registers::P_MASK).leading_zeros() as u8 + 1;
        let registers = self.registers.as_mut_slice();
        if leading_zero > registers[bucket_num] {
            registers[bucket_num] = leading_zero;
        }
    }

    /// Inserts multiple pre-hashed values into the sketch.
    #[inline(always)]
    pub fn insert_many_with_hashes(&mut self, hashes: &[u64]) {
        for &hashed in hashes {
            self.insert_with_hash(hashed);
        }
    }

    /// Merges another sketch into this one by taking the element-wise max of registers.
    pub fn merge(&mut self, other: &Self) {
        assert!(
            self.registers.len() == other.registers.len(),
            "Different register length, should not merge"
        );
        for (reg, other_val) in self
            .registers
            .as_mut_slice()
            .iter_mut()
            .zip(other.registers.as_slice().iter().copied())
        {
            if other_val > *reg {
                *reg = other_val;
            }
        }
    }
}

// DataInput adapters (hashing + batch helpers).
impl<Variant, Registers: HllRegisterStorage, H: SketchHasher>
    HyperLogLogImpl<Variant, Registers, H>
{
    /// Hashes and inserts a single input value into the sketch.
    pub fn insert(&mut self, obj: &DataInput) {
        let hashed_val = H::hash64_seeded(CANONICAL_HASH_SEED, obj);
        self.insert_with_hash(hashed_val);
    }

    /// Hashes and inserts multiple input values into the sketch.
    pub fn insert_many(&mut self, items: &[DataInput]) {
        for item in items {
            self.insert(item);
        }
    }
}

impl<Registers: HllRegisterStorage, H: SketchHasher> HyperLogLogImpl<Classic, Registers, H> {
    /// Creates a new HyperLogLog sketch with the Classic estimator.
    pub fn new() -> Self {
        Self::new_base()
    }
    /// indicator function in the original HyperLogLog paper
    /// <https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf>
    pub fn indicator(&self) -> f64 {
        let mut z = 0.0;
        for &reg_val in self.registers.as_slice() {
            let inv_pow2 = 2f64.powi(-(reg_val as i32));
            z += inv_pow2;
        }
        1.0 / z
    }

    /// Returns the estimated cardinality using the classic HyperLogLog algorithm with small/large range corrections.
    pub fn estimate(&self) -> usize {
        let m = Registers::NUM_REGISTERS as f64;
        let alpha_m = 0.7213 / (1.0 + 1.079 / m);
        let mut est = alpha_m * m * m * self.indicator();
        if est <= m * 5.0 / 2.0 {
            let mut zero_count = 0;
            for &reg_val in self.registers.as_slice() {
                if reg_val == 0 {
                    zero_count += 1;
                }
            }
            if zero_count != 0 {
                est = m * (m / zero_count as f64).ln();
            }
        } else if est > 143165576.533 {
            let correction_aux = i32::MAX as f64;
            est = 1.0 * -correction_aux * (1.0 - est / correction_aux).ln();
        }
        est as usize
    }
}

impl<Registers: HllRegisterStorage, H: SketchHasher> HyperLogLogImpl<ErtlMLE, Registers, H> {
    /// Creates a new HyperLogLog sketch with the Ertl MLE estimator.
    pub fn new() -> Self {
        Self::new_base()
    }
    /// "New cardinality estimation algorithms for HyperLogLog sketches"
    /// Otmar Ertl, arXiv:1702.01284
    #[inline]
    fn hll_ertl_sigma(&self, x: f64) -> f64 {
        if x == 1. {
            f64::INFINITY
        } else {
            let mut y = 1.0;
            let mut z = x;
            let mut x = x;
            loop {
                x *= x;
                let z_prime = z;
                z += x * y;
                y += y;
                if z_prime == z {
                    break;
                }
            }
            z
        }
    }
    /// "New cardinality estimation algorithms for HyperLogLog sketches"
    /// Otmar Ertl, arXiv:1702.01284
    #[inline]
    fn hll_ertl_tau(&self, x: f64) -> f64 {
        if x == 0.0 || x == 1.0 {
            0.0
        } else {
            let mut y = 1.0;
            let mut z = 1.0 - x;
            let mut x = x;
            loop {
                x = x.sqrt();
                let z_prime = z;
                y *= 0.5;
                z -= (1.0 - x).powi(2) * y;
                if z_prime == z {
                    break;
                }
            }
            z / 3.0
        }
    }
}

macro_rules! impl_ertl_mle_estimate {
    ($storage:ty) => {
        impl<H: SketchHasher> HyperLogLogImpl<ErtlMLE, $storage, H> {
            /// "New cardinality estimation algorithms for HyperLogLog sketches"
            /// Otmar Ertl, arXiv:1702.01284
            #[inline]
            fn get_histogram(&self) -> [u32; { <$storage>::REGISTER_BITS + 2 }] {
                let mut histogram = [0; { <$storage>::REGISTER_BITS + 2 }];
                for &register in self.registers.as_slice() {
                    histogram[register as usize] += 1;
                }
                histogram
            }

            /// Returns the estimated cardinality using the Ertl MLE algorithm.
            pub fn estimate(&self) -> usize {
                let histogram = self.get_histogram();
                let m: f64 = <$storage>::NUM_REGISTERS as f64;
                let mut z = m * self
                    .hll_ertl_tau((m - histogram[<$storage>::REGISTER_BITS + 1] as f64) / m);
                for i in histogram[1..=<$storage>::REGISTER_BITS].iter().rev() {
                    z += *i as f64;
                    z *= 0.5;
                }
                z += m * self.hll_ertl_sigma(histogram[0] as f64 / m);
                (0.5 / 2_f64.ln() * m * m / z).round() as usize
            }
        }
    };
}

impl_ertl_mle_estimate!(HllBucketListP12);
impl_ertl_mle_estimate!(HllBucketListP14);
impl_ertl_mle_estimate!(HllBucketListP16);

impl<Registers: HllRegisterStorage> Default for HyperLogLogHIPImpl<Registers> {
    fn default() -> Self {
        Self::new()
    }
}

// Core HIP logic (hash-based operations + serialization).
impl<Registers: HllRegisterStorage> HyperLogLogHIPImpl<Registers> {
    /// Creates a new HyperLogLog HIP sketch.
    pub fn new() -> Self {
        Self {
            registers: Registers::default(),
            kxq0: Registers::NUM_REGISTERS as f64,
            kxq1: 0.0,
            est: 0.0,
        }
    }
    /// Inserts a pre-hashed value, updating both the register and the HIP running estimate.
    #[inline(always)]
    pub fn insert_with_hash(&mut self, hashed: u64) {
        let hashed_val = hashed;
        let bucket_num = ((hashed_val >> Registers::REGISTER_BITS) & Registers::P_MASK) as usize;
        let leading_zero =
            ((hashed_val << Registers::PRECISION) + Registers::P_MASK).leading_zeros() as u8 + 1;
        let registers = self.registers.as_mut_slice();
        let old_value = registers[bucket_num];
        let new_value = leading_zero;
        if new_value > old_value {
            registers[bucket_num] = leading_zero;
            self.est += Registers::NUM_REGISTERS as f64 / (self.kxq0 + self.kxq1);
            if old_value < 32 {
                self.kxq0 -= 1.0 / ((1_u64 << old_value) as f64);
            } else {
                self.kxq1 -= 1.0 / ((1_u64 << old_value) as f64);
            }
            if new_value < 32 {
                self.kxq0 += 1.0 / ((1_u64 << new_value) as f64);
            } else {
                self.kxq1 += 1.0 / ((1_u64 << new_value) as f64);
            }
        }
    }

    /// Inserts multiple pre-hashed values into the HIP sketch.
    #[inline(always)]
    pub fn insert_many_with_hashes(&mut self, hashes: &[u64]) {
        for &hashed in hashes {
            self.insert_with_hash(hashed);
        }
    }

    /// Returns the estimated cardinality from the HIP running estimate.
    pub fn estimate(&self) -> usize {
        self.est as usize
    }

    /// Serializes the sketch into an ASAPv1 MessagePack envelope.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let metadata =
            rmp_serde::to_vec_named(&standard_hll_metadata(Registers::PRECISION as u32))?;
        let payload = rmp_serde::to_vec(&HllPayloadHip {
            registers: self.registers.as_slice().to_vec(),
            hip_kxq0: self.kxq0,
            hip_kxq1: self.kxq1,
            hip_est: self.est,
        })?;
        Ok(envelope::encode(HLL_KIND_HIP, &metadata, &payload))
    }

    /// Deserializes a sketch from an ASAPv1 MessagePack envelope.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let payload = validated_hll_payload(bytes, HLL_KIND_HIP, Registers::PRECISION as u32)?;
        let payload: HllPayloadHip = from_slice(payload)?;
        let registers = registers_from_bytes::<Registers>(&payload.registers)?;
        Ok(Self {
            registers,
            kxq0: payload.hip_kxq0,
            kxq1: payload.hip_kxq1,
            est: payload.hip_est,
        })
    }
}

// DataInput adapters for HIP (hashing + batch helpers).
// Note: HyperLogLogHIP is not parameterized by H since it is a separate,
// self-contained struct. It uses the free-function wrapper (DefaultXxHasher).
impl<Registers: HllRegisterStorage> HyperLogLogHIPImpl<Registers> {
    /// "Back to the Future: an Even More Nearly Optimal Cardinality Estimation Algorithm"
    /// Kevin J. Lang, <https://arxiv.org/pdf/1708.06839>
    pub fn insert(&mut self, obj: &DataInput) {
        let hashed_val = hash64_seeded(CANONICAL_HASH_SEED, obj);
        self.insert_with_hash(hashed_val);
    }

    /// Hashes and inserts multiple input values into the HIP sketch.
    pub fn insert_many(&mut self, items: &[DataInput]) {
        for item in items {
            self.insert(item);
        }
    }
}

use crate::octo_delta::HllDelta;

impl<Variant, Registers: HllRegisterStorage, H: SketchHasher>
    HyperLogLogImpl<Variant, Registers, H>
{
    #[inline(always)]
    /// Inserts a hashed value and emits a delta when a register increases.
    pub fn insert_emit_delta_with_hash(
        &mut self,
        hashed_val: u64,
        emit: &mut impl FnMut(HllDelta),
    ) {
        let bucket_num = ((hashed_val >> Registers::REGISTER_BITS) & Registers::P_MASK) as usize;
        let leading_zero =
            ((hashed_val << Registers::PRECISION) + Registers::P_MASK).leading_zeros() as u8 + 1;
        let regs = self.registers.as_mut_slice();
        if leading_zero > regs[bucket_num] {
            regs[bucket_num] = leading_zero;
            emit(HllDelta {
                pos: bucket_num as u16,
                value: leading_zero,
            });
        }
    }

    #[inline(always)]
    /// Hashes an input, inserts it, and emits a delta when needed.
    pub fn insert_emit_delta(&mut self, obj: &DataInput, emit: &mut impl FnMut(HllDelta)) {
        let hashed_val = H::hash64_seeded(CANONICAL_HASH_SEED, obj);
        self.insert_emit_delta_with_hash(hashed_val, emit);
    }

    /// Applies one externally emitted HLL delta.
    pub fn apply_delta(&mut self, delta: HllDelta) {
        let pos = delta.pos as usize;
        let regs = self.registers.as_mut_slice();
        if delta.value > regs[pos] {
            regs[pos] = delta.value;
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{DataInput, HllBucketList};

    const TARGETS: [usize; 7] = [10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000];
    const ERROR_TOLERANCE: f64 = 0.02;
    const P12_ERROR_TOLERANCE: f64 = 0.03;
    const SERDE_SAMPLE: usize = 100_000;

    #[test]
    fn hll_child_insert_emits_on_improvement() {
        let mut child = HyperLogLog::<Classic>::default();
        let mut deltas: Vec<HllDelta> = Vec::new();

        child.insert_emit_delta(&DataInput::U64(1), &mut |d| deltas.push(d));
        assert_eq!(deltas.len(), 1, "first insert should improve one register");

        let before = deltas.len();
        child.insert_emit_delta(&DataInput::U64(1), &mut |d| deltas.push(d));
        assert_eq!(deltas.len(), before, "duplicate should not emit");
    }

    trait HllEstimator: Default {
        fn push(&mut self, input: &DataInput);
        fn insert_with_hash(&mut self, hashed: u64);
        fn estimate(&self) -> f64;
        fn index(&self, i: usize) -> u8;
    }

    trait HllMerge: HllEstimator + Clone {
        fn merge_into(&mut self, other: &Self);
    }

    trait HllSerializable: HllEstimator {
        fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>;
        fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
        where
            Self: Sized;
    }

    impl<Registers: HllRegisterStorage, H: SketchHasher> HllEstimator
        for HyperLogLogImpl<Classic, Registers, H>
    {
        fn push(&mut self, input: &DataInput) {
            self.insert(input);
        }

        fn insert_with_hash(&mut self, hashed: u64) {
            HyperLogLogImpl::<Classic, Registers, H>::insert_with_hash(self, hashed);
        }

        fn estimate(&self) -> f64 {
            HyperLogLogImpl::<Classic, Registers, H>::estimate(self) as f64
        }

        fn index(&self, i: usize) -> u8 {
            self.registers.as_slice()[i]
        }
    }

    impl<Registers: HllRegisterStorage, H: SketchHasher> HllMerge
        for HyperLogLogImpl<Classic, Registers, H>
    {
        fn merge_into(&mut self, other: &Self) {
            self.merge(other);
        }
    }

    impl<Registers: HllRegisterStorage, H: SketchHasher> HllSerializable
        for HyperLogLogImpl<Classic, Registers, H>
    {
        fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
            HyperLogLogImpl::<Classic, Registers, H>::serialize_to_bytes(self)
        }

        fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
            HyperLogLogImpl::<Classic, Registers, H>::deserialize_from_bytes(bytes)
        }
    }

    macro_rules! impl_ertl_mle_test_traits {
        ($storage:ty) => {
            impl<H: SketchHasher> HllEstimator for HyperLogLogImpl<ErtlMLE, $storage, H> {
                fn push(&mut self, input: &DataInput) {
                    self.insert(input);
                }

                fn insert_with_hash(&mut self, hashed: u64) {
                    HyperLogLogImpl::<ErtlMLE, $storage, H>::insert_with_hash(self, hashed);
                }

                fn estimate(&self) -> f64 {
                    HyperLogLogImpl::<ErtlMLE, $storage, H>::estimate(self) as f64
                }

                fn index(&self, i: usize) -> u8 {
                    self.registers.as_slice()[i]
                }
            }

            impl<H: SketchHasher> HllMerge for HyperLogLogImpl<ErtlMLE, $storage, H> {
                fn merge_into(&mut self, other: &Self) {
                    self.merge(other);
                }
            }

            impl<H: SketchHasher> HllSerializable for HyperLogLogImpl<ErtlMLE, $storage, H> {
                fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
                    HyperLogLogImpl::<ErtlMLE, $storage, H>::serialize_to_bytes(self)
                }

                fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
                    HyperLogLogImpl::<ErtlMLE, $storage, H>::deserialize_from_bytes(bytes)
                }
            }
        };
    }

    impl_ertl_mle_test_traits!(HllBucketListP12);
    impl_ertl_mle_test_traits!(HllBucketListP14);
    impl_ertl_mle_test_traits!(HllBucketListP16);

    impl<Registers: HllRegisterStorage> HllEstimator for HyperLogLogHIPImpl<Registers> {
        fn push(&mut self, input: &DataInput) {
            self.insert(input);
        }

        fn insert_with_hash(&mut self, hashed: u64) {
            HyperLogLogHIPImpl::<Registers>::insert_with_hash(self, hashed);
        }

        fn estimate(&self) -> f64 {
            HyperLogLogHIPImpl::<Registers>::estimate(self) as f64
        }
        fn index(&self, i: usize) -> u8 {
            self.registers.as_slice()[i]
        }
    }

    impl<Registers: HllRegisterStorage> HllSerializable for HyperLogLogHIPImpl<Registers> {
        fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
            HyperLogLogHIPImpl::<Registers>::serialize_to_bytes(self)
        }

        fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
            HyperLogLogHIPImpl::<Registers>::deserialize_from_bytes(bytes)
        }
    }

    #[test]
    fn hyperloglog_accuracy_within_two_percent() {
        assert_accuracy::<HyperLogLog<Classic>>("HyperLogLog");
    }

    #[test]
    fn hll_ertl_accuracy_within_two_percent() {
        assert_accuracy::<HyperLogLog<ErtlMLE>>("HllErtl");
    }

    #[test]
    fn hllds_accuracy_within_two_percent() {
        assert_accuracy::<HyperLogLogHIP>("HllDs");
    }

    #[test]
    fn hyperloglog_p12_accuracy_within_two_percent() {
        assert_accuracy_within::<HyperLogLogP12<Classic>>("HyperLogLogP12", P12_ERROR_TOLERANCE);
    }

    #[test]
    fn hll_ertl_p12_accuracy_within_two_percent() {
        assert_accuracy_within::<HyperLogLogP12<ErtlMLE>>("HllErtlP12", P12_ERROR_TOLERANCE);
    }

    #[test]
    fn hllds_p12_accuracy_within_two_percent() {
        assert_accuracy_within::<HyperLogLogHIPP12>("HllDsP12", P12_ERROR_TOLERANCE);
    }

    #[test]
    fn hyperloglog_merge_within_two_percent() {
        assert_merge_accuracy::<HyperLogLog<Classic>>("HyperLogLog");
    }

    #[test]
    fn hll_ertl_merge_within_two_percent() {
        assert_merge_accuracy::<HyperLogLog<ErtlMLE>>("HllErtl");
    }

    #[test]
    fn hyperloglog_p12_merge_within_two_percent() {
        assert_merge_accuracy_within::<HyperLogLogP12<Classic>>(
            "HyperLogLogP12",
            P12_ERROR_TOLERANCE,
        );
    }

    #[test]
    fn hll_ertl_p12_merge_within_two_percent() {
        assert_merge_accuracy_within::<HyperLogLogP12<ErtlMLE>>("HllErtlP12", P12_ERROR_TOLERANCE);
    }

    #[test]
    fn hyperloglog_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLog<Classic>>("HyperLogLog");
    }

    #[test]
    fn hll_ertl_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLog<ErtlMLE>>("HllErtl");
    }

    #[test]
    fn hllds_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLogHIP>("HllDs");
    }

    #[test]
    fn hyperloglog_p12_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLogP12<Classic>>("HyperLogLogP12");
    }

    #[test]
    fn hll_ertl_p12_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLogP12<ErtlMLE>>("HllErtlP12");
    }

    #[test]
    fn hllds_p12_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLogHIPP12>("HllDsP12");
    }

    // insert 10 values and check corresponding counter is updated
    #[test]
    fn hll_correctness_test() {
        let mut hll = HyperLogLog::<Classic>::default();
        hll_correctness_test_helper::<HyperLogLog<Classic>>(&mut hll);
        let mut hll_ertl = HyperLogLog::<ErtlMLE>::default();
        hll_correctness_test_helper::<HyperLogLog<ErtlMLE>>(&mut hll_ertl);
        let mut hllds = HyperLogLogHIP::default();
        hll_correctness_test_helper(&mut hllds);
    }

    // insert 10 values and check corresponding counter is updated
    fn hll_correctness_test_helper<T>(hll: &mut T)
    where
        T: HllEstimator,
    {
        hll.insert_with_hash(0x0002_0000_0000_0000);
        assert_eq!(
            hll.index(0),
            1,
            "the first bucket should be 1, but get {}",
            hll.index(0)
        );
        hll.insert_with_hash(0x0000_0000_0000_0000);
        assert_eq!(
            hll.index(0),
            51,
            "the first bucket should be 51, but get {}",
            hll.index(0)
        );
        hll.insert_with_hash(0xfffc_3000_0000_0000);
        assert_eq!(
            hll.index(HllBucketList::P_MASK as usize),
            5,
            "the last bucket should be 5, but get {}",
            hll.index(HllBucketList::P_MASK as usize)
        );
        hll.insert_with_hash(0xcafe_0000_0000_0000);
        assert_eq!(
            hll.index(12991),
            1,
            "the 12991th bucket should be 1, but get {}",
            hll.index(12991)
        );
        hll.insert_with_hash(0xcafc_00ce_cafe_face);
        assert_eq!(
            hll.index(12991),
            11,
            "the 12991th bucket should be 11, but get {}",
            hll.index(12991)
        );
        hll.insert_with_hash(0xface_cafe_face_cafe);
        assert_eq!(
            hll.index(16051),
            1,
            "the 16051th bucket should be 1, but get {}",
            hll.index(16051)
        );
        hll.insert_with_hash(0xfacc_ca00_0000_cafe);
        assert_eq!(
            hll.index(16051),
            3,
            "the 16051th bucket should be 3, but get {}",
            hll.index(16051)
        );
        hll.insert_with_hash(0x0831_8310_0000_0000);
        assert_eq!(
            hll.index(524),
            2,
            "the 524th bucket should be 2, but get {}",
            hll.index(524)
        );
        hll.insert_with_hash(0x3014_1592_6535_8000);
        assert_eq!(
            hll.index(3077),
            6,
            "the 3077th bucket should be 6, but get {}",
            hll.index(3077)
        );
        hll.insert_with_hash(0xcafc_0ace_cafe_face);
        assert_eq!(
            hll.index(12991),
            11,
            "the 12991th bucket should still be 11, but get {}",
            hll.index(12991)
        );
        assert_eq!(
            hll.index(1000),
            0,
            "no unintended changes, but get {} at bucket 1000",
            hll.index(1000)
        );
    }

    fn assert_accuracy<S>(name: &str)
    where
        S: HllEstimator,
    {
        assert_accuracy_within::<S>(name, ERROR_TOLERANCE);
    }

    fn assert_accuracy_within<S>(name: &str, tolerance: f64)
    where
        S: HllEstimator,
    {
        let mut sketch = S::default();
        let mut inserted: usize = 0;

        for &target in TARGETS.iter() {
            while inserted < target {
                let input = DataInput::U64(inserted as u64);
                sketch.push(&input);
                inserted += 1;
            }

            let truth = target as f64;
            let estimate = sketch.estimate();
            let error = if truth == 0.0 {
                0.0
            } else {
                (estimate - truth).abs() / truth
            };
            assert!(
                error <= tolerance,
                "{name} accuracy error {error:.4} exceeded {tolerance} (truth {truth}, estimate {estimate})"
            );
        }
    }

    fn assert_merge_accuracy<S>(name: &str)
    where
        S: HllMerge,
    {
        assert_merge_accuracy_within::<S>(name, ERROR_TOLERANCE);
    }

    fn assert_merge_accuracy_within<S>(name: &str, tolerance: f64)
    where
        S: HllMerge,
    {
        let mut left = S::default();
        let mut right = S::default();
        let mut next_even: usize = 0;
        let mut next_odd: usize = 1;

        for &target in TARGETS.iter() {
            while next_even < target {
                let input = DataInput::U64(next_even as u64);
                left.push(&input);
                next_even += 2;
            }

            while next_odd < target {
                let input = DataInput::U64(next_odd as u64);
                right.push(&input);
                next_odd += 2;
            }

            let mut merged = left.clone();
            merged.merge_into(&right);

            let truth = target as f64;
            let estimate = merged.estimate();
            let error = if truth == 0.0 {
                0.0
            } else {
                (estimate - truth).abs() / truth
            };
            assert!(
                error <= tolerance,
                "{name} merge error {error:.4} exceeded {tolerance} (truth {truth}, estimate {estimate})"
            );
        }
    }

    fn assert_serialization_round_trip<S>(name: &str)
    where
        S: HllSerializable,
    {
        let mut sketch = S::default();
        for value in 0..SERDE_SAMPLE {
            let input = DataInput::U64(value as u64);
            sketch.push(&input);
        }

        let encoded = sketch
            .serialize_to_bytes()
            .unwrap_or_else(|err| panic!("{name} serialize_to_bytes failed: {err}"));
        assert!(
            !encoded.is_empty(),
            "{name} serialization output should not be empty"
        );

        let decoded = S::deserialize_from_bytes(&encoded)
            .unwrap_or_else(|err| panic!("{name} deserialize_from_bytes failed: {err}"));

        let reencoded = decoded
            .serialize_to_bytes()
            .unwrap_or_else(|err| panic!("{name} re-serialize failed: {err}"));

        assert_eq!(
            encoded, reencoded,
            "{name} serialized bytes differed after round trip"
        );

        let original_est = sketch.estimate();
        let decoded_est = decoded.estimate();
        assert!(
            (original_est - decoded_est).abs() <= ERROR_TOLERANCE * original_est.max(1.0),
            "{name} estimate mismatch after round trip: before {original_est}, after {decoded_est}"
        );
    }

    #[test]
    fn hll_envelope_structure_and_kind_id_guard() {
        // ASAPv1 envelope header for an Ertl-MLE sketch: magic, version 0x01,
        // kind_id_len 2, kind_id [0x01, 0x02].
        let mut sketch = HyperLogLog::<ErtlMLE>::default();
        for value in 0..1000 {
            sketch.insert(&DataInput::U64(value));
        }
        let bytes = sketch.serialize_to_bytes().expect("serialize");

        assert!(bytes.starts_with(envelope::MAGIC));
        assert_eq!(bytes[6], envelope::VERSION);
        assert_eq!(bytes[7], 2, "kind_id_len");
        assert_eq!(&bytes[8..10], HLL_KIND_ERTL_MLE);

        let decoded = HyperLogLog::<ErtlMLE>::deserialize_from_bytes(&bytes).expect("decode");
        assert_eq!(decoded.registers_as_slice(), sketch.registers_as_slice());

        // A Classic decoder must reject Ertl-MLE bytes (kind_id mismatch).
        assert!(HyperLogLog::<Classic>::deserialize_from_bytes(&bytes).is_err());
    }

    #[test]
    fn hll_hip_round_trip_preserves_state() {
        let mut sketch = HyperLogLogHIP::default();
        for value in 0..1000 {
            sketch.insert(&DataInput::U64(value));
        }
        let bytes = sketch.serialize_to_bytes().expect("serialize");
        assert!(bytes.starts_with(envelope::MAGIC));
        assert_eq!(&bytes[8..10], HLL_KIND_HIP);

        let decoded = HyperLogLogHIP::deserialize_from_bytes(&bytes).expect("decode");
        assert_eq!(decoded.registers.as_slice(), sketch.registers.as_slice());
        assert_eq!(decoded.kxq0, sketch.kxq0);
        assert_eq!(decoded.kxq1, sketch.kxq1);
        assert_eq!(decoded.est, sketch.est);
    }

    /// Drift guard: the native path and the (deprecated) portable path must emit
    /// byte-identical ASAPv1 envelopes. Keep until golden byte-vectors exist.
    #[test]
    fn native_and_portable_hll_bytes_match() {
        use crate::message_pack_format::MessagePackCodec;
        use crate::message_pack_format::portable::hll::{HllSketch, HllVariant};

        // Ertl-MLE / Datafusion: registers-only payload.
        let mut native = HyperLogLog::<ErtlMLE>::default();
        for v in 0..1000 {
            native.insert(&DataInput::U64(v));
        }
        let native_bytes = native.serialize_to_bytes().expect("native serialize");
        let portable = HllSketch::from_raw(
            HllVariant::Datafusion,
            HllBucketList::PRECISION as u32,
            native.registers_as_slice().to_vec(),
            0.0,
            0.0,
            0.0,
        );
        assert_eq!(
            native_bytes,
            portable.to_msgpack().expect("portable serialize")
        );

        // HIP: registers + three running scalars.
        let mut hip = HyperLogLogHIP::default();
        for v in 0..1000 {
            hip.insert(&DataInput::U64(v));
        }
        let hip_bytes = hip.serialize_to_bytes().expect("native serialize");
        let portable_hip = HllSketch::from_raw(
            HllVariant::Hip,
            HllBucketList::PRECISION as u32,
            hip.registers.as_slice().to_vec(),
            hip.kxq0,
            hip.kxq1,
            hip.est,
        );
        assert_eq!(
            hip_bytes,
            portable_hip.to_msgpack().expect("portable serialize")
        );
    }
}
