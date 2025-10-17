use crate::utils::{LASTSTATE, SketchInput};
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::marker::PhantomData;

use super::utils::hash_it;

use super::utils::SEED;

#[derive(Clone, Debug)]
pub struct HLL<T>
where
    T: Hash + ?Sized,
{
    pub registers: [u8; 16384],
    phantom: PhantomData<T>,
}

// #[derive(Clone, Debug, Serialize, Deserialize)]
#[derive(Clone, Debug)]
pub struct HLLHIP<T>
where
    T: Hash + ?Sized,
{
    pub registers: [u8; 16384], // 2**14
    pub kxq0: f64,
    pub kxq1: f64,
    pub est: f64,
    phantom: PhantomData<T>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HllDfModified {
    #[serde(with = "serde_bytes")]
    pub registers: Vec<u8>,
}

impl Default for HllDfModified {
    fn default() -> Self {
        Self::new()
    }
}

impl HllDfModified {
    pub fn new() -> Self {
        let mut r = Vec::with_capacity(16384);
        for _i in 0..16384 {
            r.push(0);
        }
        HllDfModified { registers: r }
    }

    // pub fn insert<T: Hash+?Sized>(&mut self, obj: &T) {
    //     let hashed_val = hash_it(LASTSTATE, obj);
    //     let left_14_bits = (hashed_val >> 50) & ((0b1 << 14) - 1);
    //     let leading_zero = ((hashed_val << 14) + ((0b1 << 14) - 1)).leading_zeros() as u8 + 1;
    //     self.registers[left_14_bits as usize] = self.registers[left_14_bits as usize].max(leading_zero);
    // }
    pub fn insert(&mut self, obj: &SketchInput) {
        let hashed_val = hash_it(LASTSTATE, obj);
        let left_14_bits = (hashed_val >> 50) & ((0b1 << 14) - 1);
        let leading_zero = ((hashed_val << 14) + ((0b1 << 14) - 1)).leading_zeros() as u8 + 1;
        self.registers[left_14_bits as usize] =
            self.registers[left_14_bits as usize].max(leading_zero);
    }

    pub fn merge(&mut self, other: &HllDfModified) {
        assert!(
            self.registers.len() == other.registers.len(),
            "Different register length, should not merge"
        );
        for i in 0..16384 {
            let temp = self.registers[i].max(other.registers[i]);
            self.registers[i] = temp;
        }
    }

    #[inline]
    fn get_histogram(&self) -> [u32; 52] {
        let mut histogram = [0; 52];
        for r in &self.registers {
            histogram[*r as usize] += 1;
        }
        histogram
    }
    /// Helper function sigma as defined in
    /// /// "New cardinality estimation algorithms for HyperLogLog sketches"
    /// /// Otmar Ertl, arXiv:1702.01284
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
    /// Helper function tau as defined in
    /// /// "New cardinality estimation algorithms for HyperLogLog sketches"
    /// /// Otmar Ertl, arXiv:1702.01284
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
        let m: f64 = 16384.0;
        let mut z = m * self.hlldf_tau((m - histogram[51] as f64) / m);
        for i in histogram[1..=50].iter().rev() {
            z += *i as f64;
            z *= 0.5;
        }
        z += m * self.hlldf_sigma(histogram[0] as f64 / m);
        (0.5 / 2_f64.ln() * m * m / z).round() as usize
    }
}

// this is the HLL from DataFusion

/// The greater is P, the smaller the error.
const HLL_P: usize = 14_usize;
/// The number of bits of the hash value used determining the number of leading zeros
const HLL_Q: usize = 64_usize - HLL_P;
const NUM_REGISTERS: usize = 1_usize << HLL_P;
/// Mask to obtain index into the registers
const HLL_P_MASK: u64 = (NUM_REGISTERS as u64) - 1;
#[derive(Clone, Debug)]
pub struct HLLDataFusion<T>
where
    T: Hash + ?Sized,
{
    registers: [u8; NUM_REGISTERS],
    phantom: PhantomData<T>,
}

impl<T> Default for HLL<T>
where
    T: Hash + ?Sized,
{
    fn default() -> Self {
        Self::init_hll()
    }
}

impl<T> HLL<T>
where
    T: Hash + ?Sized,
{
    pub fn debug(&self) -> () {
        println!("registers: {:?}", self.registers);
    }

    pub fn init_hll() -> Self {
        HLL {
            registers: [0; 16384],
            phantom: PhantomData,
        }
    }

    // copied from datafusion
    /// choice of hash function: ahash is already an dependency
    /// and it fits the requirements of being a 64bit hash with
    /// reasonable performance.
    #[inline]
    fn hash_value(&self, obj: &T) -> u64 {
        SEED.hash_one(obj)
    }

    pub fn insert_hll(&mut self, val: &T) {
        let hashed_val = self.hash_value(val);
        let left_14_bits = (hashed_val >> 50) & ((0b1 << 14) - 1);
        let leading_zero = ((hashed_val << 14) + ((0b1 << 14) - 1)).leading_zeros() as u8 + 1;
        self.registers[left_14_bits as usize] =
            self.registers[left_14_bits as usize].max(leading_zero);
    }

    pub fn merge_hll(&mut self, other: &HLL<T>) {
        assert!(
            self.registers.len() == other.registers.len(),
            "Different register length, should not merge"
        );
        for i in 0..16384 {
            let temp = self.registers[i].max(other.registers[i]);
            self.registers[i] = temp;
        }
    }

    pub fn indicator(&self) -> f64 {
        // the precision could be a problem, sometimes?
        let mut z = 0.0;
        for i in 0..16384 {
            // let pow2 = 0x1 << self.registers[i];
            // let inv_pow2 = 1.0 / (pow2 as f64);
            let inv_pow2 = 2f64.powi(-(self.registers[i] as i32));
            z += inv_pow2;
        }
        1.0 / z
    }

    pub fn calculate_est(&self) -> f64 {
        // println!("registers: {:?}", self.registers);
        let alpha_m = 0.7213 / (1.0 + 1.079 / 16384.0);
        let mut est = alpha_m * 16384.0 * 16384.0 * self.indicator();
        // println!("raw est {} with indicator: {}", est, self.indicator());
        // perform correction
        if est <= 16384.0 * 5.0 / 2.0 {
            let mut zero_count = 0;
            for i in 0..16384 {
                if self.registers[i] == 0 {
                    zero_count += 1;
                }
            }
            if zero_count != 0 {
                est = 16384.0 * f64::log2(16384.0 / zero_count as f64);
            }
        } else if est > 143165576.533 {
            let correction_aux = i32::MAX as f64;
            est = -1.0 * correction_aux * f64::log2(1.0 - est / correction_aux);
        }
        est
    }
}

impl<T> Default for HLLHIP<T>
where
    T: Hash + ?Sized,
{
    fn default() -> Self {
        Self::init_hll()
    }
}

impl<T> HLLHIP<T>
where
    T: Hash + ?Sized,
{
    pub fn debug(&self) -> () {
        println!("registers: {:?}", self.registers);
        println!(
            "kxq0: {}; kxq1: {}; current est: {}",
            self.kxq0, self.kxq1, self.est
        );
    }

    pub fn init_hll() -> Self {
        HLLHIP {
            registers: [0; 16384],
            kxq0: 16384.0,
            kxq1: 0.0,
            est: 0.0,
            phantom: PhantomData,
        }
    }

    // copied from datafusion
    /// choice of hash function: ahash is already an dependency
    /// and it fits the requirements of being a 64bit hash with
    /// reasonable performance.
    #[inline]
    fn hash_value(&self, obj: &T) -> u64 {
        SEED.hash_one(obj)
    }

    pub fn insert_hll(&mut self, val: &T) {
        let hashed_val = self.hash_value(val);
        // stupid mistake: use 0x instead of 0b... lol
        let left_14_bits = (hashed_val >> 50) & ((0b1 << 14) - 1);
        let leading_zero = ((hashed_val << 14) + ((0b1 << 14) - 1)).leading_zeros() as u8 + 1;
        let old_value = self.registers[left_14_bits as usize];
        let new_value = leading_zero;
        // println!("hased: {:b}", hashed_val);
        // println!("left 14: {:b}, or move only {:b}", left_14_bits, hashed_val >> 50);
        // println!("leading_zero: {}", leading_zero);
        if new_value > old_value {
            self.registers[left_14_bits as usize] = leading_zero;
            self.est += 16384.0 / (self.kxq0 + self.kxq1);
            if old_value < 32 {
                self.kxq0 -= 1.0 / ((1 << old_value) as f64);
            } else {
                self.kxq1 -= 1.0 / ((1 << old_value) as f64);
            }
            if new_value < 32 {
                self.kxq0 += 1.0 / ((1 << new_value) as f64);
            } else {
                self.kxq1 += 1.0 / ((1 << new_value) as f64);
            }
        }
    }

    pub fn merge_hll(&mut self, other: &HLLHIP<T>) {
        // the merge seems to be incomplete
        assert!(
            self.registers.len() == other.registers.len(),
            "Different register length, should not merge"
        );
        for i in 0..32 {
            let temp = self.registers[i].max(other.registers[i]);
            self.registers[i] = temp;
        }
    }

    pub fn calculate_est(&self) -> f64 {
        self.est
    }
}

impl<T> Default for HLLDataFusion<T>
where
    T: Hash + ?Sized,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> HLLDataFusion<T>
where
    T: Hash + ?Sized,
{
    /// Creates a new, empty HyperLogLog.
    pub fn new() -> Self {
        let registers = [0; NUM_REGISTERS];
        Self::new_with_registers(registers)
    }

    /// Creates a HyperLogLog from already populated registers
    /// note that this method should not be invoked in untrusted environment
    /// because the internal structure of registers are not examined.
    pub(crate) fn new_with_registers(registers: [u8; NUM_REGISTERS]) -> Self {
        Self {
            registers,
            phantom: PhantomData,
        }
    }

    /// choice of hash function: ahash is already an dependency
    /// and it fits the requirements of being a 64bit hash with
    /// reasonable performance.
    #[inline]
    fn hash_value(&self, obj: &T) -> u64 {
        SEED.hash_one(obj)
    }

    /// Adds an element to the HyperLogLog.
    pub fn add(&mut self, obj: &T) {
        let hash = self.hash_value(obj);
        let index = (hash & HLL_P_MASK) as usize;
        let p = ((hash >> HLL_P) | (1_u64 << HLL_Q)).trailing_zeros() + 1;
        self.registers[index] = self.registers[index].max(p as u8);
    }

    /// Get the register histogram (each value in register index into
    /// the histogram; u32 is enough because we only have 2**14=16384 registers
    #[inline]
    fn get_histogram(&self) -> [u32; HLL_Q + 2] {
        let mut histogram = [0; HLL_Q + 2];
        // hopefully this can be unrolled
        for r in self.registers {
            histogram[r as usize] += 1;
        }
        histogram
    }

    /// Merge the other [`HyperLogLog`] into this one
    pub fn merge(&mut self, other: &HLLDataFusion<T>) {
        assert!(
            self.registers.len() == other.registers.len(),
            "unexpected got unequal register size, expect {}, got {}",
            self.registers.len(),
            other.registers.len()
        );
        for i in 0..self.registers.len() {
            self.registers[i] = self.registers[i].max(other.registers[i]);
        }
    }

    /// Guess the number of unique elements seen by the HyperLogLog.
    pub fn count(&self) -> usize {
        let histogram = self.get_histogram();
        let m = NUM_REGISTERS as f64;
        let mut z = m * hll_tau((m - histogram[HLL_Q + 1] as f64) / m);
        for i in histogram[1..=HLL_Q].iter().rev() {
            z += *i as f64;
            z *= 0.5;
        }
        z += m * hll_sigma(histogram[0] as f64 / m);
        (0.5 / 2_f64.ln() * m * m / z).round() as usize
    }
}

/// Helper function sigma as defined in
/// "New cardinality estimation algorithms for HyperLogLog sketches"
/// Otmar Ertl, arXiv:1702.01284
#[inline]
fn hll_sigma(x: f64) -> f64 {
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

/// Helper function tau as defined in
/// "New cardinality estimation algorithms for HyperLogLog sketches"
/// Otmar Ertl, arXiv:1702.01284
#[inline]
fn hll_tau(x: f64) -> f64 {
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

impl<T> AsRef<[u8]> for HLLDataFusion<T>
where
    T: Hash + ?Sized,
{
    fn as_ref(&self) -> &[u8] {
        &self.registers
    }
}

impl<T> Extend<T> for HLLDataFusion<T>
where
    T: Hash,
{
    fn extend<S: IntoIterator<Item = T>>(&mut self, iter: S) {
        for elem in iter {
            self.add(&elem);
        }
    }
}

impl<'a, T> Extend<&'a T> for HLLDataFusion<T>
where
    T: 'a + Hash + ?Sized,
{
    fn extend<S: IntoIterator<Item = &'a T>>(&mut self, iter: S) {
        for elem in iter {
            self.add(elem);
        }
    }
}
