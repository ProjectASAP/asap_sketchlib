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

use crate::structures::fixed_structure::{
    HllBucketListP12, HllBucketListP14, HllBucketListP16, HllRegisterStorage,
};
use crate::{CANONICAL_HASH_SEED, DataInput, DefaultXxHasher, SketchHasher, hash64_seeded};
use rmp_serde::{
    decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice, to_vec_named,
};
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

    /// Serializes the sketch into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    /// Deserializes a sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
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

    /// Serializes the sketch into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    /// Deserializes a sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
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
}

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
        let hashed_val = crate::hash64_seeded(crate::CANONICAL_HASH_SEED, &DataInput::Bytes(value));
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

    /// Serialize to MessagePack bytes (used by the legacy wire path
    /// and by PR I's `_ENCODING_MSGPACK` variant when that lands).
    pub fn serialize_msgpack(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec(self)
    }

    /// Deserialize from MessagePack bytes.
    pub fn deserialize_msgpack(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(rmp_serde::from_slice(buffer)?)
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
}
