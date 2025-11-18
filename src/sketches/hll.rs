use crate::hash_it_to_128;
use crate::{LASTSTATE, SketchInput, Vector1D};
use rmp_serde::{
    decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice, to_vec_named,
};
use serde::{Deserialize, Serialize};

/// The greater is P, the smaller the error.
const HLL_P: usize = 14_usize;
/// The number of bits of the hash value used determining the number of leading zeros
const HLL_Q: usize = 64_usize - HLL_P;
const NUM_REGISTERS: usize = 1_usize << HLL_P;
/// Mask to obtain index into the registers
const HLL_P_MASK: u64 = (NUM_REGISTERS as u64) - 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HyperLogLog {
    registers: Vector1D<u8>,
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperLogLog {
    pub fn new() -> Self {
        HyperLogLog {
            registers: Vector1D::filled(NUM_REGISTERS, 0_u8),
        }
    }

    pub fn insert(&mut self, obj: &SketchInput) {
        let hashed_val = hash_it_to_128(LASTSTATE, obj);
        self.insert_with_hash(hashed_val);
    }

    #[inline(always)]
    pub fn insert_with_hash(&mut self, hashed: u128) {
        let hashed_val = hashed as u64;
        let bucket_num = ((hashed_val >> HLL_Q) & HLL_P_MASK) as usize;
        let leading_zero = ((hashed_val << HLL_P) + HLL_P_MASK).leading_zeros() as u8 + 1;
        self.registers.update_if_greater(bucket_num, leading_zero);
    }

    pub fn merge(&mut self, other: &HyperLogLog) {
        assert!(
            self.registers.len() == other.registers.len(),
            "Different register length, should not merge"
        );
        for i in 0..NUM_REGISTERS {
            self.registers
                .update_if_greater(i, *other.registers.get(i).unwrap());
        }
    }
    /// indicator function in the original HyperLogLog paper
    /// https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
    pub fn indicator(&self) -> f64 {
        let mut z = 0.0;
        for i in 0..NUM_REGISTERS {
            // let pow2 = 0x1 << self.registers[i];
            // let inv_pow2 = 1.0 / (pow2 as f64);
            let reg_val = *self.registers.get(i).unwrap();
            let inv_pow2 = 2f64.powi(-(reg_val as i32));
            z += inv_pow2;
        }
        1.0 / z
    }

    pub fn get_est(&self) -> usize {
        let m = NUM_REGISTERS as f64;
        let alpha_m = 0.7213 / (1.0 + 1.079 / m);
        let mut est = alpha_m * m * m * self.indicator();
        // println!("raw est {} with indicator: {}", est, self.indicator());
        // perform correction
        if est <= m * 5.0 / 2.0 {
            let mut zero_count = 0;
            for i in 0..NUM_REGISTERS {
                let reg_val = *self.registers.get(i).unwrap();
                if reg_val == 0 {
                    zero_count += 1;
                }
            }
            if zero_count != 0 {
                est = m * (m / zero_count as f64).ln();
            }
        } else if est > 143165576.533 {
            let correction_aux = i32::MAX as f64;
            // est = -1.0 * correction_aux * (1.0 - est / correction_aux).ln();
            est = 1.0 * -correction_aux * (1.0 - est / correction_aux).ln();
        }
        est as usize
    }

    /// Serializes the sketch into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    /// Convenience alias matching the previous API shape used elsewhere.
    pub fn serialize(&self) -> Result<Vec<u8>, RmpEncodeError> {
        self.serialize_to_bytes()
    }

    /// Deserializes a sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
    }

    /// Convenience alias matching the previous API shape used elsewhere.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        Self::deserialize_from_bytes(bytes)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HllDf {
    registers: Vector1D<u8>,
}

impl Default for HllDf {
    fn default() -> Self {
        Self::new()
    }
}

impl HllDf {
    pub fn new() -> Self {
        HllDf {
            registers: Vector1D::filled(NUM_REGISTERS, 0_u8),
        }
    }

    pub fn insert(&mut self, obj: &SketchInput) {
        let hashed_val = hash_it_to_128(LASTSTATE, obj);
        self.insert_with_hash(hashed_val);
    }

    #[inline(always)]
    pub fn insert_with_hash(&mut self, h: u128) {
        let hashed_val = h as u64;
        let bucket_num = ((hashed_val >> HLL_Q) & HLL_P_MASK) as usize;
        let leading_zero = ((hashed_val << HLL_P) + HLL_P_MASK).leading_zeros() as u8 + 1;
        self.registers.update_if_greater(bucket_num, leading_zero);
    }

    pub fn merge(&mut self, other: &HllDf) {
        assert!(
            self.registers.len() == other.registers.len(),
            "Different register length, should not merge"
        );
        for i in 0..NUM_REGISTERS {
            self.registers
                .update_if_greater(i, *other.registers.get(i).unwrap());
        }
    }
    /// "New cardinality estimation algorithms for HyperLogLog sketches"
    /// Otmar Ertl, arXiv:1702.01284
    #[inline]
    fn get_histogram(&self) -> [u32; HLL_Q + 2] {
        let mut histogram = [0; HLL_Q + 2];
        for r in self.registers.as_slice() {
            histogram[*r as usize] += 1;
        }
        histogram
    }
    /// "New cardinality estimation algorithms for HyperLogLog sketches"
    /// Otmar Ertl, arXiv:1702.01284
    #[inline]
    fn hlldf_sigma(&self, x: f64) -> f64 {
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
    fn hlldf_tau(&self, x: f64) -> f64 {
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
    pub fn get_est(&self) -> usize {
        let histogram = self.get_histogram();
        let m: f64 = NUM_REGISTERS as f64;
        let mut z = m * self.hlldf_tau((m - histogram[HLL_Q + 1] as f64) / m);
        for i in histogram[1..=HLL_Q].iter().rev() {
            z += *i as f64;
            z *= 0.5;
        }
        z += m * self.hlldf_sigma(histogram[0] as f64 / m);
        (0.5 / 2_f64.ln() * m * m / z).round() as usize
    }

    /// Serializes the sketch into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    /// Convenience alias matching previous APIs.
    pub fn serialize(&self) -> Result<Vec<u8>, RmpEncodeError> {
        self.serialize_to_bytes()
    }

    /// Deserializes a sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
    }

    /// Convenience alias matching previous APIs.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        Self::deserialize_from_bytes(bytes)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HllDs {
    registers: Vector1D<u8>,
    kxq0: f64,
    kxq1: f64,
    est: f64,
}

impl Default for HllDs {
    fn default() -> Self {
        Self::new()
    }
}

impl HllDs {
    pub fn new() -> Self {
        HllDs {
            registers: Vector1D::filled(NUM_REGISTERS, 0_u8),
            kxq0: NUM_REGISTERS as f64,
            kxq1: 0.0,
            est: 0.0,
        }
    }
    /// "Back to the Future: an Even More Nearly Optimal Cardinality Estimation Algorithm"
    /// Kevin J. Lang, https://arxiv.org/pdf/1708.06839
    pub fn insert(&mut self, obj: &SketchInput) {
        let hashed_val = hash_it_to_128(LASTSTATE, obj);
        self.insert_with_hash(hashed_val);
    }

    #[inline(always)]
    pub fn insert_with_hash(&mut self, hashed: u128) {
        let hashed_val = hashed as u64;
        let bucket_num = ((hashed_val >> HLL_Q) & HLL_P_MASK) as usize;
        let leading_zero = ((hashed_val << HLL_P) + HLL_P_MASK).leading_zeros() as u8 + 1;
        let old_value = *self.registers.get(bucket_num).unwrap();
        let new_value = leading_zero;
        if new_value > old_value {
            self.registers.update_if_greater(bucket_num, leading_zero);
            self.est += NUM_REGISTERS as f64 / (self.kxq0 + self.kxq1);
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

    pub fn merge(&mut self, _: &HllDs) {
        panic!("Hll with HIP Estimator should not be merged");
    }

    pub fn get_est(&self) -> usize {
        self.est as usize
    }

    /// Serializes the sketch into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    /// Convenience alias matching previous APIs.
    pub fn serialize(&self) -> Result<Vec<u8>, RmpEncodeError> {
        self.serialize_to_bytes()
    }

    /// Deserializes a sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
    }

    /// Convenience alias matching previous APIs.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        Self::deserialize_from_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::SketchInput;

    const TARGETS: [usize; 7] = [10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000];
    const ERROR_TOLERANCE: f64 = 0.02;
    const SERDE_SAMPLE: usize = 100_000;

    trait HllEstimator: Default {
        fn push(&mut self, input: &SketchInput);
        fn insert_with_hash(&mut self, hashed: u128);
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

    impl HllEstimator for HyperLogLog {
        fn push(&mut self, input: &SketchInput) {
            self.insert(input);
        }

        fn insert_with_hash(&mut self, hashed: u128) {
            self.insert_with_hash(hashed);
        }

        fn estimate(&self) -> f64 {
            self.get_est() as f64
        }

        fn index(&self, i: usize) -> u8 {
            self.registers[i]
        }
    }

    impl HllMerge for HyperLogLog {
        fn merge_into(&mut self, other: &Self) {
            self.merge(other);
        }
    }

    impl HllSerializable for HyperLogLog {
        fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
            HyperLogLog::serialize_to_bytes(self)
        }

        fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
            HyperLogLog::deserialize_from_bytes(bytes)
        }
    }

    impl HllEstimator for HllDf {
        fn push(&mut self, input: &SketchInput) {
            self.insert(input);
        }

        fn insert_with_hash(&mut self, hashed: u128) {
            self.insert_with_hash(hashed);
        }
        fn estimate(&self) -> f64 {
            self.get_est() as f64
        }
        fn index(&self, i: usize) -> u8 {
            self.registers[i]
        }
    }

    impl HllMerge for HllDf {
        fn merge_into(&mut self, other: &Self) {
            self.merge(other);
        }
    }

    impl HllSerializable for HllDf {
        fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
            HllDf::serialize_to_bytes(self)
        }

        fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
            HllDf::deserialize_from_bytes(bytes)
        }
    }

    impl HllEstimator for HllDs {
        fn push(&mut self, input: &SketchInput) {
            self.insert(input);
        }

        fn insert_with_hash(&mut self, hashed: u128) {
            self.insert_with_hash(hashed);
        }

        fn estimate(&self) -> f64 {
            self.get_est() as f64
        }
        fn index(&self, i: usize) -> u8 {
            self.registers[i]
        }
    }

    impl HllSerializable for HllDs {
        fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
            HllDs::serialize_to_bytes(self)
        }

        fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
            HllDs::deserialize_from_bytes(bytes)
        }
    }

    #[test]
    fn hyperloglog_accuracy_within_two_percent() {
        assert_accuracy::<HyperLogLog>("HyperLogLog");
    }

    #[test]
    fn hlldf_accuracy_within_two_percent() {
        assert_accuracy::<HllDf>("HllDf");
    }

    #[test]
    fn hllds_accuracy_within_two_percent() {
        assert_accuracy::<HllDs>("HllDs");
    }

    #[test]
    fn hyperloglog_merge_within_two_percent() {
        assert_merge_accuracy::<HyperLogLog>("HyperLogLog");
    }

    #[test]
    fn hlldf_merge_within_two_percent() {
        assert_merge_accuracy::<HllDf>("HllDf");
    }

    #[test]
    #[should_panic(expected = "Hll with HIP Estimator should not be merged")]
    fn hllds_merge_panics() {
        let mut left = HllDs::default();
        let right = HllDs::default();
        left.merge(&right);
    }

    #[test]
    fn hyperloglog_round_trip_serialization() {
        assert_serialization_round_trip::<HyperLogLog>("HyperLogLog");
    }

    #[test]
    fn hlldf_round_trip_serialization() {
        assert_serialization_round_trip::<HllDf>("HllDf");
    }

    #[test]
    fn hllds_round_trip_serialization() {
        assert_serialization_round_trip::<HllDs>("HllDs");
    }

    // insert 10 values and check corresponding counter is updated
    #[test]
    fn hll_correctness_test() {
        let mut hll = HyperLogLog::default();
        hll_correctness_test_helper::<HyperLogLog>(&mut hll);
        let mut hlldf = HllDf::default();
        hll_correctness_test_helper::<HllDf>(&mut hlldf);
        let mut hllds = HllDs::default();
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
            hll.index(HLL_P_MASK as usize),
            5,
            "the last bucket should be 5, but get {}",
            hll.index(HLL_P_MASK as usize)
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
                error <= ERROR_TOLERANCE,
                "{name} accuracy error {error:.4} exceeded {ERROR_TOLERANCE} (truth {truth}, estimate {estimate})"
            );
        }
    }

    fn assert_merge_accuracy<S>(name: &str)
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
                error <= ERROR_TOLERANCE,
                "{name} merge error {error:.4} exceeded {ERROR_TOLERANCE} (truth {truth}, estimate {estimate})"
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
