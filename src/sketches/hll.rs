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
use crate::{CANONICAL_HASH_SEED, DefaultXxHasher, SketchHasher, SketchInput, hash64_seeded};
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
pub struct Regular;
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

pub type HyperLogLogP12<Variant, H = DefaultXxHasher> =
    HyperLogLogImpl<Variant, HllBucketListP12, H>;
pub type HyperLogLogP14<Variant, H = DefaultXxHasher> =
    HyperLogLogImpl<Variant, HllBucketListP14, H>;
pub type HyperLogLogP16<Variant, H = DefaultXxHasher> =
    HyperLogLogImpl<Variant, HllBucketListP16, H>;
pub type HyperLogLog<Variant, H = DefaultXxHasher> = HyperLogLogP14<Variant, H>;

pub type HyperLogLogHIPP12 = HyperLogLogHIPImpl<HllBucketListP12>;
pub type HyperLogLogHIPP14 = HyperLogLogHIPImpl<HllBucketListP14>;
pub type HyperLogLogHIPP16 = HyperLogLogHIPImpl<HllBucketListP16>;
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

// SketchInput adapters (hashing + batch helpers).
impl<Variant, Registers: HllRegisterStorage, H: SketchHasher>
    HyperLogLogImpl<Variant, Registers, H>
{
    /// Hashes and inserts a single input value into the sketch.
    pub fn insert(&mut self, obj: &SketchInput) {
        let hashed_val = H::hash64_seeded(CANONICAL_HASH_SEED, obj);
        self.insert_with_hash(hashed_val);
    }

    /// Hashes and inserts multiple input values into the sketch.
    pub fn insert_many(&mut self, items: &[SketchInput]) {
        for item in items {
            self.insert(item);
        }
    }
}

impl<Registers: HllRegisterStorage, H: SketchHasher> HyperLogLogImpl<Regular, Registers, H> {
    /// Creates a new HyperLogLog sketch with the classic (Regular) estimator.
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
        let hashed_val = hashed as u64;
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

// SketchInput adapters for HIP (hashing + batch helpers).
// Note: HyperLogLogHIP is not parameterized by H since it is a separate,
// self-contained struct. It uses the free-function wrapper (DefaultXxHasher).
impl<Registers: HllRegisterStorage> HyperLogLogHIPImpl<Registers> {
    /// "Back to the Future: an Even More Nearly Optimal Cardinality Estimation Algorithm"
    /// Kevin J. Lang, <https://arxiv.org/pdf/1708.06839>
    pub fn insert(&mut self, obj: &SketchInput) {
        let hashed_val = hash64_seeded(CANONICAL_HASH_SEED, obj);
        self.insert_with_hash(hashed_val);
    }

    /// Hashes and inserts multiple input values into the HIP sketch.
    pub fn insert_many(&mut self, items: &[SketchInput]) {
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
    pub fn insert_emit_delta(&mut self, obj: &SketchInput, emit: &mut impl FnMut(HllDelta)) {
        let hashed_val = H::hash64_seeded(CANONICAL_HASH_SEED, obj);
        self.insert_emit_delta_with_hash(hashed_val, emit);
    }

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
    use crate::{HllBucketList, SketchInput};

    const TARGETS: [usize; 7] = [10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000];
    const ERROR_TOLERANCE: f64 = 0.02;
    const P12_ERROR_TOLERANCE: f64 = 0.03;
    const SERDE_SAMPLE: usize = 100_000;

    #[test]
    fn hll_child_insert_emits_on_improvement() {
        let mut child = HyperLogLog::<Regular>::default();
        let mut deltas: Vec<HllDelta> = Vec::new();

        child.insert_emit_delta(&SketchInput::U64(1), &mut |d| deltas.push(d));
        assert_eq!(deltas.len(), 1, "first insert should improve one register");

        let before = deltas.len();
        child.insert_emit_delta(&SketchInput::U64(1), &mut |d| deltas.push(d));
        assert_eq!(deltas.len(), before, "duplicate should not emit");
    }

    trait HllEstimator: Default {
        fn push(&mut self, input: &SketchInput);
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
        for HyperLogLogImpl<Regular, Registers, H>
    {
        fn push(&mut self, input: &SketchInput) {
            self.insert(input);
        }

        fn insert_with_hash(&mut self, hashed: u64) {
            HyperLogLogImpl::<Regular, Registers, H>::insert_with_hash(self, hashed);
        }

        fn estimate(&self) -> f64 {
            HyperLogLogImpl::<Regular, Registers, H>::estimate(self) as f64
        }

        fn index(&self, i: usize) -> u8 {
            self.registers.as_slice()[i]
        }
    }

    impl<Registers: HllRegisterStorage, H: SketchHasher> HllMerge
        for HyperLogLogImpl<Regular, Registers, H>
    {
        fn merge_into(&mut self, other: &Self) {
            self.merge(other);
        }
    }

    impl<Registers: HllRegisterStorage, H: SketchHasher> HllSerializable
        for HyperLogLogImpl<Regular, Registers, H>
    {
        fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
            HyperLogLogImpl::<Regular, Registers, H>::serialize_to_bytes(self)
        }

        fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
            HyperLogLogImpl::<Regular, Registers, H>::deserialize_from_bytes(bytes)
        }
    }

    macro_rules! impl_ertl_mle_test_traits {
        ($storage:ty) => {
            impl<H: SketchHasher> HllEstimator for HyperLogLogImpl<ErtlMLE, $storage, H> {
                fn push(&mut self, input: &SketchInput) {
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
        fn push(&mut self, input: &SketchInput) {
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
        assert_accuracy::<HyperLogLog<Regular>>("HyperLogLog");
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
        assert_accuracy_within::<HyperLogLogP12<Regular>>("HyperLogLogP12", P12_ERROR_TOLERANCE);
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
        assert_merge_accuracy::<HyperLogLog<Regular>>("HyperLogLog");
    }

    #[test]
    fn hll_ertl_merge_within_two_percent() {
        assert_merge_accuracy::<HyperLogLog<ErtlMLE>>("HllErtl");
    }

    #[test]
    fn hyperloglog_p12_merge_within_two_percent() {
        assert_merge_accuracy_within::<HyperLogLogP12<Regular>>(
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
        assert_serialization_round_trip::<HyperLogLog<Regular>>("HyperLogLog");
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
        assert_serialization_round_trip::<HyperLogLogP12<Regular>>("HyperLogLogP12");
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
        let mut hll = HyperLogLog::<Regular>::default();
        hll_correctness_test_helper::<HyperLogLog<Regular>>(&mut hll);
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
                let input = SketchInput::U64(inserted as u64);
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
                let input = SketchInput::U64(next_even as u64);
                left.push(&input);
                next_even += 2;
            }

            while next_odd < target {
                let input = SketchInput::U64(next_odd as u64);
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
            let input = SketchInput::U64(value as u64);
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
