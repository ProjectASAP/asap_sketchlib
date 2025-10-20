use crate::common::{LASTSTATE, SketchInput, SketchList, hash_it};

const NUM_REGISTERS: usize = 1 << 14;
const HISTOGRAM_SIZE: usize = 52;

/// HyperLogLog sketch backed by the shared `SketchList` abstraction.
#[derive(Clone, Debug)]
pub struct HyperLogLog {
    registers: SketchList<u8>,
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperLogLog {
    /// Creates a new sketch with zeroed registers.
    pub fn new() -> Self {
        Self {
            registers: SketchList::filled(NUM_REGISTERS, 0),
        }
    }

    /// Inserts an element into the sketch.
    pub fn insert(&mut self, value: &SketchInput) {
        let hashed = hash_it(LASTSTATE, value);
        let register_index = ((hashed >> 50) & ((NUM_REGISTERS as u64) - 1)) as usize;
        let leading_zeros = ((hashed << 14) + (((1u64) << 14) - 1)).leading_zeros() as u8 + 1u8;

        if let Some(register) = self.registers.get_mut(register_index) {
            *register = (*register).max(leading_zeros);
        }
    }

    /// Merges another sketch into this one, taking the per-register maxima.
    pub fn merge(&mut self, other: &Self) {
        assert_eq!(
            self.registers.len(),
            other.registers.len(),
            "cannot merge HyperLogLog sketches with different register counts"
        );

        for (dst, src) in self.registers.iter_mut().zip(other.registers.iter()) {
            *dst = (*dst).max(*src);
        }
    }

    /// Returns the estimated cardinality.
    pub fn estimate(&self) -> usize {
        let histogram = self.histogram();
        let m = NUM_REGISTERS as f64;
        let mut z = m * self.tau((m - histogram[HISTOGRAM_SIZE - 1] as f64) / m);
        for count in histogram[1..HISTOGRAM_SIZE - 1].iter().rev() {
            z += *count as f64;
            z *= 0.5;
        }
        z += m * self.sigma(histogram[0] as f64 / m);
        (0.5 / f64::ln(2.0) * m * m / z).round() as usize
    }

    /// Exposes the registers, primarily for testing.
    pub fn registers(&self) -> &SketchList<u8> {
        &self.registers
    }

    fn histogram(&self) -> [u32; HISTOGRAM_SIZE] {
        let mut histogram = [0u32; HISTOGRAM_SIZE];
        for register in self.registers.iter() {
            let bucket = (*register as usize).min(HISTOGRAM_SIZE - 1);
            histogram[bucket] += 1;
        }
        histogram
    }

    fn sigma(&self, mut x: f64) -> f64 {
        if x == 1.0 {
            return f64::INFINITY;
        }
        let mut y = 1.0;
        let mut z = x;
        loop {
            x *= x;
            let z_prev = z;
            z += x * y;
            y += y;
            if z_prev == z {
                break;
            }
        }
        z
    }

    fn tau(&self, mut x: f64) -> f64 {
        if x == 0.0 || x == 1.0 {
            return 0.0;
        }
        let mut y = 1.0;
        let mut z = 1.0 - x;
        loop {
            x = x.sqrt();
            let z_prev = z;
            y *= 0.5;
            z -= (1.0 - x).powi(2) * y;
            if z_prev == z {
                break;
            }
        }
        z / 3.0
    }
}
