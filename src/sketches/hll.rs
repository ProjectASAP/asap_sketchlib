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
// 1. HyperLogLog<Ertl> Implementation:
//    - Originally derived from Apache DataFusion's HyperLogLog component
//    - Source: https://github.com/apache/datafusion/blob/main/datafusion/functions-aggregate/src/hyperloglog.rs
//    - Algorithm: Otmar Ertl's MLE estimator (arXiv:1702.01284)
//
// 2. HyperLogLogHip Implementation:
//    - Ported from: Apache DataSketches (Java)
//    - Source: https://github.com/apache/datasketches-java
//    - Algorithm: HIP (Kevin J. Lang, arXiv:1708.06839)
//    - Note: This Rust implementation is a port based on the original Java logic.
//
// Modifications:
// - Adapted both implementations to use a unified `HllRegisters` storage.
// - Refactored into a generic `HyperLogLog<Variant>` structure.
// ----------------------------------------------------------------

use crate::asapv1::admission_rate::admit_all;
use crate::structures::fixed_structure::{
    HllRegisterStorage, HllRegistersP12, HllRegistersP14, HllRegistersP16,
};
use crate::{
    DEFAULT_CANONICAL_SEED_INDEX, DataInput, DefaultXxHasher, SketchHasher, hash64_seeded,
};
use std::marker::PhantomData;

mod delta;
mod wire;
pub use delta::HyperLogLogDelta;
pub use wire::HllWireVariant;

/// Generic HyperLogLog sketch parameterized by estimation variant, register storage, and hasher.
#[derive(Clone, Debug)]
pub struct HyperLogLogImpl<
    Variant,
    Registers: HllRegisterStorage,
    H: SketchHasher = DefaultXxHasher,
> {
    registers: Registers,
    _marker: PhantomData<Variant>,
    _hasher: PhantomData<H>,
    admission_rate: f64,
}

/// One byte per register: `2^precision`, saturating at `usize::MAX`.
fn register_bytes(precision: usize) -> usize {
    u32::try_from(precision)
        .ok()
        .and_then(|precision| 1usize.checked_shl(precision))
        .unwrap_or(usize::MAX)
}

/// Classic HyperLogLog cardinality estimate over a raw register slice.
///
/// This is the estimator body shared by [`HyperLogLogImpl<Classic, _, _>`] and
/// by any sketch that keeps HyperLogLog registers in storage of its own. The
/// harmonic sum and the zero-register count are accumulated in one pass.
///
/// Ranks are drawn from a 64-bit hash, so only the small-range (linear
/// counting) correction applies; there is no 32-bit large-range correction.
#[inline]
pub(crate) fn classic_estimate(registers: &[u8]) -> f64 {
    let m = registers.len() as f64;
    let alpha_m = 0.7213 / (1.0 + 1.079 / m);
    let mut z = 0.0;
    let mut zero_count: usize = 0;
    for &reg_val in registers {
        z += 2f64.powi(-(reg_val as i32));
        if reg_val == 0 {
            zero_count += 1;
        }
    }
    let mut est = alpha_m * m * m / z;
    if est <= m * 5.0 / 2.0 && zero_count != 0 {
        est = m * (m / zero_count as f64).ln();
    }
    est
}

/// `2^-exponent`, built from its bits so it is exact by construction: biased
/// exponent `1023 - exponent`, zero mantissa. A register holds at most
/// `65 - p` (at most 64), well inside the normal range.
#[inline]
fn inverse_power_of_two(exponent: u8) -> f64 {
    f64::from_bits((1023 - u64::from(exponent)) << 52)
}

/// Marker type selecting the classic HyperLogLog estimation algorithm.
#[derive(Clone, Copy, Debug, Default)]
pub struct Classic;
/// Marker type selecting the Ertl MLE estimation algorithm (arXiv:1702.01284).
#[derive(Clone, Copy, Debug, Default)]
pub struct Ertl;

/// HyperLogLog variant using the Historic Inverse Probability (HIP) estimator for improved accuracy.
#[derive(Clone, Debug)]
pub struct HyperLogLogHipImpl<Registers: HllRegisterStorage> {
    registers: Registers,
    /// `sum(2^-r)` over the registers `r` below 32. Derived from the
    /// registers ([`hip_kxq`]) and kept so an update costs O(1).
    kxq0: f64,
    /// `sum(2^-r)` over the registers `r` of 32 or more; see `kxq0`.
    kxq1: f64,
    /// The running HIP estimate. It depends on the arrival order, so it is
    /// state, not derivable from the registers.
    est: f64,
    admission_rate: f64,
}

/// `(kxq0, kxq1)` for a register slice: the sums of `2^-r` over the registers
/// `r` below 32 and over those of 32 or more, the split
/// `HyperLogLogHipImpl::update_with_hash` keeps them in.
///
/// Every term is a power of two, so every partial sum, and every step of the
/// incremental subtract-then-add, is a multiple of the smallest term. `kxq0`
/// (terms `2^0..2^-31`, sum at most `2^p`) is then exact for `p <= 22`, and
/// `kxq1` (terms `2^-32..2^-(65 - p)`, sum at most `2^(p - 32)`) at every
/// precision. Within that bound this equals the incrementally kept values bit
/// for bit, whatever the order.
fn hip_kxq(registers: &[u8]) -> (f64, f64) {
    let mut kxq0 = 0.0;
    let mut kxq1 = 0.0;
    for &register in registers {
        let term = inverse_power_of_two(register);
        if register < 32 {
            kxq0 += term;
        } else {
            kxq1 += term;
        }
    }
    (kxq0, kxq1)
}

/// HyperLogLog with 12-bit precision.
pub type HyperLogLogP12<Variant, H = DefaultXxHasher> =
    HyperLogLogImpl<Variant, HllRegistersP12, H>;
/// HyperLogLog with 14-bit precision.
pub type HyperLogLogP14<Variant, H = DefaultXxHasher> =
    HyperLogLogImpl<Variant, HllRegistersP14, H>;
/// HyperLogLog with 16-bit precision.
pub type HyperLogLogP16<Variant, H = DefaultXxHasher> =
    HyperLogLogImpl<Variant, HllRegistersP16, H>;
/// Default HyperLogLog alias using 14-bit precision.
pub type HyperLogLog<Variant, H = DefaultXxHasher> = HyperLogLogP14<Variant, H>;

/// HIP HyperLogLog with 12-bit precision.
pub type HyperLogLogHipP12 = HyperLogLogHipImpl<HllRegistersP12>;
/// HIP HyperLogLog with 14-bit precision.
pub type HyperLogLogHipP14 = HyperLogLogHipImpl<HllRegistersP14>;
/// HIP HyperLogLog with 16-bit precision.
pub type HyperLogLogHipP16 = HyperLogLogHipImpl<HllRegistersP16>;
/// Default HIP HyperLogLog alias using 14-bit precision.
pub type HyperLogLogHip = HyperLogLogHipP14;

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
            admission_rate: admit_all(),
        }
    }

    /// Borrow the raw register byte slice (one byte per register).
    pub fn registers_as_slice(&self) -> &[u8] {
        self.registers.as_slice()
    }

    /// Sets the probability `p` with which each update was admitted. The
    /// estimate counts admitted keys alone; a consumer divides it by `p`.
    pub fn with_admission_rate(mut self, p: f64) -> Self {
        self.admission_rate = p;
        self
    }

    /// The probability with which each update was admitted; `1` unless set.
    pub fn admission_rate(&self) -> f64 {
        self.admission_rate
    }

    /// Inserts a pre-hashed value into the sketch.
    #[inline(always)]
    pub fn update_with_hash(&mut self, hashed_val: u64) {
        let bucket_num =
            ((hashed_val >> Registers::RANK_HASH_BITS) & Registers::INDEX_MASK) as usize;
        let leading_zero = ((hashed_val << Registers::PRECISION) + Registers::INDEX_MASK)
            .leading_zeros() as u8
            + 1;
        let registers = self.registers.as_mut_slice();
        if leading_zero > registers[bucket_num] {
            registers[bucket_num] = leading_zero;
        }
    }

    /// Inserts multiple pre-hashed values into the sketch.
    #[inline(always)]
    pub fn update_batch_with_hashes(&mut self, hashes: &[u64]) {
        for &hashed in hashes {
            self.update_with_hash(hashed);
        }
    }

    /// Merges another sketch into this one by taking the element-wise max of
    /// registers. Panics when the two admission rates differ.
    pub fn merge(&mut self, other: &Self) {
        assert!(
            self.registers.len() == other.registers.len(),
            "Different register length, should not merge"
        );
        assert_eq!(
            self.admission_rate, other.admission_rate,
            "admission rate mismatch while merging HyperLogLog sketches"
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

    /// Bytes this sketch holds: the struct and one byte per register.
    pub fn memory_bytes(&self) -> usize {
        size_of::<Self>() + Registers::NUM_REGISTERS
    }

    /// Bytes a sketch of `2^precision` registers holds: the struct and one
    /// byte per register. `precision` alone decides it, whatever alias this is
    /// called through; the crate builds 12, 14 and 16, and
    /// [`impl_hll_registers!`](crate::impl_hll_registers) others. Saturates at
    /// `usize::MAX`.
    pub fn estimated_memory_bytes(precision: usize) -> usize {
        size_of::<Self>().saturating_add(register_bytes(precision))
    }
}

// DataInput adapters (hashing + batch helpers).
impl<Variant, Registers: HllRegisterStorage, H: SketchHasher>
    HyperLogLogImpl<Variant, Registers, H>
{
    /// The hash this sketch indexes `obj` by.
    ///
    /// Exposed so a caller can hash on its own thread and hand a worker the
    /// result, leaving nothing borrowed to cross the boundary.
    #[inline(always)]
    pub fn canonical_hash(obj: &DataInput) -> u64 {
        H::hash64_seeded(DEFAULT_CANONICAL_SEED_INDEX, obj)
    }

    /// Hashes and inserts a single input value into the sketch.
    pub fn update(&mut self, obj: &DataInput) {
        self.update_with_hash(Self::canonical_hash(obj));
    }

    /// Hashes and inserts multiple input values into the sketch.
    pub fn update_batch(&mut self, items: &[DataInput]) {
        for item in items {
            self.update(item);
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

    /// Returns the estimated cardinality using the classic HyperLogLog
    /// algorithm with the small-range correction. Ranks are drawn from a
    /// 64-bit hash, so the estimate needs no large-range correction.
    pub fn estimate(&self) -> usize {
        classic_estimate(self.registers.as_slice()) as usize
    }
}

impl<Registers: HllRegisterStorage, H: SketchHasher> HyperLogLogImpl<Ertl, Registers, H> {
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

/// Upper bound on the Ertl histogram length. `RANK_HASH_BITS + 2` peaks at
/// `64 + 2` for `precision = 0`, so one array covers every precision and the
/// per-precision `[u32; RANK_HASH_BITS + 2]` sizing is not needed.
const ERTL_HISTOGRAM_CAP: usize = 66;

impl<Registers: HllRegisterStorage, H: SketchHasher> HyperLogLogImpl<Ertl, Registers, H> {
    /// "New cardinality estimation algorithms for HyperLogLog sketches"
    /// Otmar Ertl, arXiv:1702.01284
    #[inline]
    fn get_histogram(&self) -> [u32; ERTL_HISTOGRAM_CAP] {
        let mut histogram = [0; ERTL_HISTOGRAM_CAP];
        for &register in self.registers.as_slice() {
            histogram[register as usize] += 1;
        }
        histogram
    }

    /// Returns the estimated cardinality using the Ertl MLE algorithm.
    pub fn estimate(&self) -> usize {
        let histogram = self.get_histogram();
        let m: f64 = Registers::NUM_REGISTERS as f64;
        let mut z =
            m * self.hll_ertl_tau((m - histogram[Registers::RANK_HASH_BITS + 1] as f64) / m);
        for i in histogram[1..=Registers::RANK_HASH_BITS].iter().rev() {
            z += *i as f64;
            z *= 0.5;
        }
        z += m * self.hll_ertl_sigma(histogram[0] as f64 / m);
        (0.5 / 2_f64.ln() * m * m / z).round() as usize
    }
}

impl<Registers: HllRegisterStorage> Default for HyperLogLogHipImpl<Registers> {
    fn default() -> Self {
        Self::new()
    }
}

// Core HIP logic (hash-based operations + serialization).
impl<Registers: HllRegisterStorage> HyperLogLogHipImpl<Registers> {
    /// Creates a new HyperLogLog HIP sketch.
    pub fn new() -> Self {
        Self {
            registers: Registers::default(),
            kxq0: Registers::NUM_REGISTERS as f64,
            kxq1: 0.0,
            est: 0.0,
            admission_rate: admit_all(),
        }
    }

    /// Sets the probability `p` with which each update was admitted. The
    /// estimate counts admitted keys alone; a consumer divides it by `p`.
    pub fn with_admission_rate(mut self, p: f64) -> Self {
        self.admission_rate = p;
        self
    }

    /// The probability with which each update was admitted; `1` unless set.
    pub fn admission_rate(&self) -> f64 {
        self.admission_rate
    }

    /// Inserts a pre-hashed value, updating both the register and the HIP running estimate.
    #[inline(always)]
    pub fn update_with_hash(&mut self, hashed: u64) {
        let hashed_val = hashed;
        let bucket_num =
            ((hashed_val >> Registers::RANK_HASH_BITS) & Registers::INDEX_MASK) as usize;
        let leading_zero = ((hashed_val << Registers::PRECISION) + Registers::INDEX_MASK)
            .leading_zeros() as u8
            + 1;
        let registers = self.registers.as_mut_slice();
        let old_value = registers[bucket_num];
        let new_value = leading_zero;
        if new_value > old_value {
            registers[bucket_num] = leading_zero;
            self.est += Registers::NUM_REGISTERS as f64 / (self.kxq0 + self.kxq1);
            if old_value < 32 {
                self.kxq0 -= inverse_power_of_two(old_value);
            } else {
                self.kxq1 -= inverse_power_of_two(old_value);
            }
            if new_value < 32 {
                self.kxq0 += inverse_power_of_two(new_value);
            } else {
                self.kxq1 += inverse_power_of_two(new_value);
            }
        }
    }

    /// Inserts multiple pre-hashed values into the HIP sketch.
    #[inline(always)]
    pub fn update_batch_with_hashes(&mut self, hashes: &[u64]) {
        for &hashed in hashes {
            self.update_with_hash(hashed);
        }
    }

    /// Returns the estimated cardinality from the HIP running estimate.
    pub fn estimate(&self) -> usize {
        self.est as usize
    }

    /// Bytes this sketch holds: the struct and one byte per register.
    pub fn memory_bytes(&self) -> usize {
        size_of::<Self>() + Registers::NUM_REGISTERS
    }

    /// Bytes a sketch of `2^precision` registers holds: the struct and one
    /// byte per register. `precision` alone decides it, whatever alias this is
    /// called through; the crate builds 12, 14 and 16, and
    /// [`impl_hll_registers!`](crate::impl_hll_registers) others. Saturates at
    /// `usize::MAX`.
    pub fn estimated_memory_bytes(precision: usize) -> usize {
        size_of::<Self>().saturating_add(register_bytes(precision))
    }
}

// DataInput adapters for HIP (hashing + batch helpers).
// Note: HyperLogLogHip is not parameterized by H since it is a separate,
// self-contained struct. It uses the free-function wrapper (DefaultXxHasher).
impl<Registers: HllRegisterStorage> HyperLogLogHipImpl<Registers> {
    /// "Back to the Future: an Even More Nearly Optimal Cardinality Estimation Algorithm"
    /// Kevin J. Lang, <https://arxiv.org/pdf/1708.06839>
    pub fn update(&mut self, obj: &DataInput) {
        let hashed_val = hash64_seeded(DEFAULT_CANONICAL_SEED_INDEX, obj);
        self.update_with_hash(hashed_val);
    }

    /// Hashes and inserts multiple input values into the HIP sketch.
    pub fn update_batch(&mut self, items: &[DataInput]) {
        for item in items {
            self.update(item);
        }
    }
}

use crate::octo_delta::{HllRegisterDelta, OCTO_HLL_DEFAULT_THRESHOLD};
use crate::sketch_framework::octo::max_hll_threshold;

impl<Variant, Registers: HllRegisterStorage, H: SketchHasher>
    HyperLogLogImpl<Variant, Registers, H>
{
    #[inline(always)]
    /// Inserts a hashed value, promoting register improvements at the default
    /// threshold `OCTO_HLL_DEFAULT_THRESHOLD`.
    pub fn update_emit_delta_with_hash(
        &mut self,
        hashed_val: u64,
        emit: &mut impl FnMut(HllRegisterDelta),
    ) {
        self.update_emit_delta_with_hash_and_threshold(
            hashed_val,
            OCTO_HLL_DEFAULT_THRESHOLD,
            emit,
        );
    }

    #[inline(always)]
    /// Inserts a hashed value and promotes the register when the improvement is
    /// large enough.
    ///
    /// Cardinality sketches merge by `max`, so a worker never clears a
    /// register; it promotes one only when `|2^C' - 2^C| >= 2^threshold`, the
    /// rule the paper gives for HyperLogLog (§4.4). A threshold of 0 promotes
    /// every improvement and makes the aggregator exactly equal to a
    /// single-threaded sketch.
    pub fn update_emit_delta_with_hash_and_threshold(
        &mut self,
        hashed_val: u64,
        threshold: u8,
        emit: &mut impl FnMut(HllRegisterDelta),
    ) {
        // A register holds a leading-zero count of at most `64 - PRECISION + 1`,
        // so the gain `2^C' - 2^C` never reaches `2^(64 - PRECISION)`. Above
        // that a threshold is unsatisfiable and the parent would stay empty
        // rather than merely lag, so cap it at the largest one that can fire.
        let threshold = threshold.min(max_hll_threshold(Registers::PRECISION as u8));
        let bucket_num =
            ((hashed_val >> Registers::RANK_HASH_BITS) & Registers::INDEX_MASK) as usize;
        let leading_zero = ((hashed_val << Registers::PRECISION) + Registers::INDEX_MASK)
            .leading_zeros() as u8
            + 1;
        let regs = self.registers.as_mut_slice();
        let previous = regs[bucket_num];
        if leading_zero > previous {
            regs[bucket_num] = leading_zero;
            if pow2_saturating(leading_zero) - pow2_saturating(previous)
                >= pow2_saturating(threshold)
            {
                emit(HllRegisterDelta {
                    index: bucket_num as u32,
                    register_value: leading_zero,
                });
            }
        }
    }

    #[inline(always)]
    /// Hashes an input, inserts it, and emits a delta at the default threshold.
    pub fn update_emit_delta(&mut self, obj: &DataInput, emit: &mut impl FnMut(HllRegisterDelta)) {
        self.update_emit_delta_with_threshold(obj, OCTO_HLL_DEFAULT_THRESHOLD, emit);
    }

    #[inline(always)]
    /// Hashes an input, inserts it, and promotes the register when the
    /// improvement clears `threshold`.
    pub fn update_emit_delta_with_threshold(
        &mut self,
        obj: &DataInput,
        threshold: u8,
        emit: &mut impl FnMut(HllRegisterDelta),
    ) {
        let hashed_val = H::hash64_seeded(DEFAULT_CANONICAL_SEED_INDEX, obj);
        self.update_emit_delta_with_hash_and_threshold(hashed_val, threshold, emit);
    }

    /// Applies one externally emitted HLL delta.
    pub fn apply_delta(&mut self, delta: HllRegisterDelta) {
        let pos = delta.index as usize;
        let regs = self.registers.as_mut_slice();
        if delta.register_value > regs[pos] {
            regs[pos] = delta.register_value;
        }
    }
}

/// `2^exp`, saturating rather than overflowing for out-of-range registers.
#[inline(always)]
fn pow2_saturating(exp: u8) -> u128 {
    if exp >= 127 { u128::MAX } else { 1u128 << exp }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{DataInput, HllRegisters};

    const TARGETS: [usize; 7] = [10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000];
    const P12_ERROR_TOLERANCE: f64 = 0.03;

    #[test]
    fn hll_child_update_emits_on_improvement() {
        let mut child = HyperLogLog::<Classic>::default();
        let mut deltas: Vec<HllRegisterDelta> = Vec::new();

        child.update_emit_delta(&DataInput::U64(1), &mut |d| deltas.push(d));
        assert_eq!(deltas.len(), 1, "first insert should improve one register");

        let before = deltas.len();
        child.update_emit_delta(&DataInput::U64(1), &mut |d| deltas.push(d));
        assert_eq!(deltas.len(), before, "duplicate should not emit");
    }

    trait HllEstimator: Default {
        fn push(&mut self, input: &DataInput);
        fn update_with_hash(&mut self, hashed: u64);
        fn estimate(&self) -> f64;
        fn index(&self, i: usize) -> u8;
    }

    trait HllMerge: HllEstimator + Clone {
        fn merge_into(&mut self, other: &Self);
    }

    impl<Registers: HllRegisterStorage, H: SketchHasher> HllEstimator
        for HyperLogLogImpl<Classic, Registers, H>
    {
        fn push(&mut self, input: &DataInput) {
            self.update(input);
        }

        fn update_with_hash(&mut self, hashed: u64) {
            HyperLogLogImpl::<Classic, Registers, H>::update_with_hash(self, hashed);
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

    impl<Registers: HllRegisterStorage, H: SketchHasher> HllEstimator
        for HyperLogLogImpl<Ertl, Registers, H>
    {
        fn push(&mut self, input: &DataInput) {
            self.update(input);
        }

        fn update_with_hash(&mut self, hashed: u64) {
            HyperLogLogImpl::<Ertl, Registers, H>::update_with_hash(self, hashed);
        }

        fn estimate(&self) -> f64 {
            HyperLogLogImpl::<Ertl, Registers, H>::estimate(self) as f64
        }

        fn index(&self, i: usize) -> u8 {
            self.registers.as_slice()[i]
        }
    }

    impl<Registers: HllRegisterStorage, H: SketchHasher> HllMerge
        for HyperLogLogImpl<Ertl, Registers, H>
    {
        fn merge_into(&mut self, other: &Self) {
            self.merge(other);
        }
    }

    impl<Registers: HllRegisterStorage> HllEstimator for HyperLogLogHipImpl<Registers> {
        fn push(&mut self, input: &DataInput) {
            self.update(input);
        }

        fn update_with_hash(&mut self, hashed: u64) {
            HyperLogLogHipImpl::<Registers>::update_with_hash(self, hashed);
        }

        fn estimate(&self) -> f64 {
            HyperLogLogHipImpl::<Registers>::estimate(self) as f64
        }
        fn index(&self, i: usize) -> u8 {
            self.registers.as_slice()[i]
        }
    }

    #[test]
    fn hyperloglog_p12_accuracy_within_two_percent() {
        assert_accuracy_within::<HyperLogLogP12<Classic>>("HyperLogLogP12", P12_ERROR_TOLERANCE);
    }

    #[test]
    fn hll_ertl_p12_accuracy_within_two_percent() {
        assert_accuracy_within::<HyperLogLogP12<Ertl>>("HllErtlP12", P12_ERROR_TOLERANCE);
    }

    #[test]
    fn hllds_p12_accuracy_within_two_percent() {
        assert_accuracy_within::<HyperLogLogHipP12>("HllDsP12", P12_ERROR_TOLERANCE);
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
        assert_merge_accuracy_within::<HyperLogLogP12<Ertl>>("HllErtlP12", P12_ERROR_TOLERANCE);
    }

    /// Ranks are drawn from a 64-bit hash, so a register state whose estimate
    /// runs past the 32-bit range must still follow the HyperLogLog formula
    /// rather than saturate or collapse to zero.
    #[test]
    fn classic_estimate_holds_past_the_32_bit_range() {
        const RANK_HASH_BITS: usize = HllRegisters::RANK_HASH_BITS;
        let m = HllRegisters::NUM_REGISTERS as f64;
        let alpha_m = 0.7213 / (1.0 + 1.079 / m);

        // A hash selecting `bucket` whose leading run gives every register the
        // same rank, so the estimate is `alpha_m * m * 2^rank` in closed form.
        for rank in [12u32, 16, 18, 20] {
            let mut hll = HyperLogLog::<Classic>::default();
            for bucket in 0..HllRegisters::NUM_REGISTERS as u64 {
                let hashed = (bucket << RANK_HASH_BITS) | (1 << (RANK_HASH_BITS - rank as usize));
                HyperLogLog::<Classic>::update_with_hash(&mut hll, hashed);
            }
            assert!(
                hll.registers_as_slice().iter().all(|&r| r == rank as u8),
                "rank {rank} state was not built: got {:?}",
                &hll.registers_as_slice()[..4]
            );

            let expected = alpha_m * m * 2f64.powi(rank as i32);
            let estimate = hll.estimate() as f64;
            // The only slack is `estimate`'s truncation to a whole count.
            let error = (estimate - expected).abs() / expected;
            assert!(
                error <= 1e-6,
                "rank {rank} estimate {estimate} deviates from {expected} by {error:.6}"
            );
        }
    }

    // insert 10 values and check corresponding counter is updated
    #[test]
    fn hll_correctness_test() {
        let mut hll = HyperLogLog::<Classic>::default();
        hll_correctness_test_helper::<HyperLogLog<Classic>>(&mut hll);
        let mut hll_ertl = HyperLogLog::<Ertl>::default();
        hll_correctness_test_helper::<HyperLogLog<Ertl>>(&mut hll_ertl);
        let mut hllds = HyperLogLogHip::default();
        hll_correctness_test_helper(&mut hllds);
    }

    // insert 10 values and check corresponding counter is updated
    fn hll_correctness_test_helper<T>(hll: &mut T)
    where
        T: HllEstimator,
    {
        hll.update_with_hash(0x0002_0000_0000_0000);
        assert_eq!(
            hll.index(0),
            1,
            "the first bucket should be 1, but get {}",
            hll.index(0)
        );
        hll.update_with_hash(0x0000_0000_0000_0000);
        assert_eq!(
            hll.index(0),
            51,
            "the first bucket should be 51, but get {}",
            hll.index(0)
        );
        hll.update_with_hash(0xfffc_3000_0000_0000);
        assert_eq!(
            hll.index(HllRegisters::INDEX_MASK as usize),
            5,
            "the last bucket should be 5, but get {}",
            hll.index(HllRegisters::INDEX_MASK as usize)
        );
        hll.update_with_hash(0xcafe_0000_0000_0000);
        assert_eq!(
            hll.index(12991),
            1,
            "the 12991th bucket should be 1, but get {}",
            hll.index(12991)
        );
        hll.update_with_hash(0xcafc_00ce_cafe_face);
        assert_eq!(
            hll.index(12991),
            11,
            "the 12991th bucket should be 11, but get {}",
            hll.index(12991)
        );
        hll.update_with_hash(0xface_cafe_face_cafe);
        assert_eq!(
            hll.index(16051),
            1,
            "the 16051th bucket should be 1, but get {}",
            hll.index(16051)
        );
        hll.update_with_hash(0xfacc_ca00_0000_cafe);
        assert_eq!(
            hll.index(16051),
            3,
            "the 16051th bucket should be 3, but get {}",
            hll.index(16051)
        );
        hll.update_with_hash(0x0831_8310_0000_0000);
        assert_eq!(
            hll.index(524),
            2,
            "the 524th bucket should be 2, but get {}",
            hll.index(524)
        );
        hll.update_with_hash(0x3014_1592_6535_8000);
        assert_eq!(
            hll.index(3077),
            6,
            "the 3077th bucket should be 6, but get {}",
            hll.index(3077)
        );
        hll.update_with_hash(0xcafc_0ace_cafe_face);
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
}
