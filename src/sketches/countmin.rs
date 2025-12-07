use rmp_serde::{
    decode::Error as RmpDecodeError, encode::Error as RmpEncodeError, from_slice, to_vec_named,
};
use serde::{Deserialize, Serialize};

use crate::Vector2D;
use crate::{SketchInput, hash_it_to_128};

const DEFAULT_ROW_NUM: usize = 3;
const DEFAULT_COL_NUM: usize = 4096;
const LOWER_32_MASK: u64 = (1u64 << 32) - 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CountMin {
    counts: Vector2D<u64>,
    row: usize,
    col: usize,
}

impl Default for CountMin {
    fn default() -> Self {
        Self::with_dimensions(DEFAULT_ROW_NUM, DEFAULT_COL_NUM)
    }
}

impl CountMin {
    /// Creates a sketch with the requested number of rows and columns.
    pub fn with_dimensions(rows: usize, cols: usize) -> Self {
        let mut sk = CountMin {
            counts: Vector2D::init(rows, cols),
            row: rows,
            col: cols,
        };
        sk.counts.fill(0);
        sk
    }

    /// Legacy constructor retaining the historic naming scheme.
    pub fn init_cm_with_row_col(rows: usize, cols: usize) -> Self {
        Self::with_dimensions(rows, cols)
    }

    /// Legacy constructor retaining the historic naming scheme.
    pub fn init_count_min() -> Self {
        Self::default()
    }

    /// Number of rows in the sketch.
    pub fn rows(&self) -> usize {
        self.row
    }

    /// Number of columns in the sketch.
    pub fn cols(&self) -> usize {
        self.col
    }

    /// Enables Nitro sampling with the provided rate.
    pub fn enable_nitro(&mut self, sampling_rate: f64) {
        self.counts.enable_nitro(sampling_rate);
    }

    /// Disables Nitro sampling and resets its internal state.
    pub fn disable_nitro(&mut self) {
        self.counts.disable_nitro();
    }

    /// Inserts an observation while using the standard Count-Min minimum row update rule.
    pub fn insert(&mut self, value: &SketchInput) {
        for r in 0..self.row {
            let hashed = hash_it_to_128(r, value);
            let col = ((hashed as u64 & LOWER_32_MASK) as usize) % self.col;
            self.counts[r][col] += 1;
        }
    }

    /// Inserts an observation using the combined hash optimization.
    pub fn fast_insert(&mut self, value: &SketchInput) {
        let hashed_val = hash_it_to_128(0, value);
        self.fast_insert_with_hash_value(hashed_val);
    }

    /// Inserts an observation using the combined hash optimization.
    /// Hash value can be reused with other sketches.
    pub fn fast_insert_with_hash_value(&mut self, hashed_val: u128) {
        self.counts
            .fast_insert(|a, b, _| *a += *b, 1_u64, hashed_val);
    }

    /// Inserts an observation using Nitro-aware sampling logic.
    #[inline(always)]
    pub fn fast_insert_nitro(&mut self, value: &SketchInput) {
        // if self.counts.nitro().to_skip > 0 {
        //     self.counts.reduce_to_skip();
        // } else {
        //     let hashed_val = hash_it_to_128(0, value);
        //     self.fast_insert_nitro_with_hash_value(hashed_val);
        // }
        let delta = self.counts.nitro().delta;
        // let nitro = self.counts.nitro_mut();
        if self.counts.nitro().to_skip >= self.row {
            self.counts.reduce_nitro_skip(self.row);
        } else {
            let hashed = hash_it_to_128(0, value);
            let mut r = self.counts.nitro().to_skip;
            loop {
                self.counts.update_by_row(r, hashed, |a, b| *a += b, delta);
                self.counts.nitro_mut().draw_geometric();
                if r + self.counts.nitro_mut().to_skip + 1 >= self.row {
                    break;
                }
                r += self.counts.nitro_mut().to_skip + 1;
            }
            let temp = self.counts.get_nitro_skip();
            self.counts.update_nitro_skip((r + temp + 1) - self.row);
        }
    }

    /// Returns the frequency estimate for the provided value.
    pub fn estimate(&self, value: &SketchInput) -> u64 {
        let mut min = u64::MAX;
        for r in 0..self.row {
            let hashed = hash_it_to_128(r, value);
            let col = ((hashed as u64 & LOWER_32_MASK) as usize) % self.col;
            // let idx = row * cols + col;
            // min = min.min(self.counts.query_one_counter(r, col));
            min = min.min(self.counts[r][col]);
        }
        min
    }

    /// Returns the frequency estimate for the provided value, with hash optimization.
    pub fn fast_estimate(&self, value: &SketchInput) -> u64 {
        let hashed_val = hash_it_to_128(0, value);
        self.counts.fast_query_min(hashed_val, |val, _, _| *val)
    }

    /// Returns the frequency estimate using a pre-computed hash value.
    pub fn fast_estimate_with_hash(&self, hashed_val: u128) -> u64 {
        self.counts.fast_query_min(hashed_val, |val, _, _| *val)
    }

    pub fn nitro_estimate(&self, value: &SketchInput) -> f64 {
        let hashed_val = hash_it_to_128(0, value);
        self.counts
            .fast_query_median(hashed_val, |val, _, _| (*val) as f64)
    }

    /// Merges another sketch while asserting compatible dimensions.
    pub fn merge(&mut self, other: &Self) {
        assert_eq!(
            (self.row, self.col),
            (other.row, other.col),
            "dimension mismatch while merging CountMin sketches"
        );

        for i in 0..self.row {
            for j in 0..self.col {
                self.counts[i][j] += other.counts[i][j];
            }
        }
    }

    /// Exposes the backing matrix for inspection/testing.
    pub fn as_storage(&self) -> &Vector2D<u64> {
        &self.counts
    }

    /// Mutable access used internally for testing scenarios.
    pub fn as_storage_mut(&mut self) -> &mut Vector2D<u64> {
        &mut self.counts
    }

    /// Human-friendly helper used by the serializer demo binaries.
    pub fn debug(&self) {
        for row in 0..self.row {
            println!("row {}: {:?}", row, &self.counts.row_slice(row));
        }
    }

    /// Serializes the sketch into MessagePack bytes.
    pub fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError> {
        to_vec_named(self)
    }

    /// Convenience alias matching the previous API.
    pub fn serialize(&self) -> Result<Vec<u8>, RmpEncodeError> {
        self.serialize_to_bytes()
    }

    /// Deserializes a sketch from MessagePack bytes.
    pub fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        from_slice(bytes)
    }

    /// Convenience alias matching the previous API.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        Self::deserialize_from_bytes(bytes)
    }

    /// Legacy helper retaining the historic naming scheme.
    pub fn insert_cm(&mut self, value: &SketchInput) {
        self.insert(value);
    }

    /// Legacy helper retaining the historic naming scheme.
    pub fn get_est(&self, value: &SketchInput) -> u64 {
        self.estimate(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SketchInput;
    use crate::test_utils::{
        all_counter_zero_u64, all_zero_except_u64, counter_index, sample_uniform_f64,
        sample_zipf_u64,
    };
    use core::f64;
    use std::collections::HashMap;

    fn run_zipf_stream(
        rows: usize,
        cols: usize,
        domain: usize,
        exponent: f64,
        samples: usize,
        seed: u64,
    ) -> (CountMin, HashMap<u64, u64>) {
        let mut truth = HashMap::<u64, u64>::new();
        let mut sketch = CountMin::with_dimensions(rows, cols);

        for value in sample_zipf_u64(domain, exponent, samples, seed) {
            let key = SketchInput::U64(value);
            sketch.insert(&key);
            *truth.entry(value).or_insert(0) += 1;
        }

        (sketch, truth)
    }

    fn run_zipf_stream_fast(
        rows: usize,
        cols: usize,
        domain: usize,
        exponent: f64,
        samples: usize,
        seed: u64,
    ) -> (CountMin, HashMap<u64, u64>) {
        let mut truth = HashMap::<u64, u64>::new();
        let mut sketch = CountMin::with_dimensions(rows, cols);

        for value in sample_zipf_u64(domain, exponent, samples, seed) {
            let key = SketchInput::U64(value);
            sketch.fast_insert(&key);
            *truth.entry(value).or_insert(0) += 1;
        }

        (sketch, truth)
    }

    fn run_uniform_stream(
        rows: usize,
        cols: usize,
        min: f64,
        max: f64,
        samples: usize,
        seed: u64,
    ) -> (CountMin, HashMap<u64, u64>) {
        let mut truth = HashMap::<u64, u64>::new();
        let mut sketch = CountMin::with_dimensions(rows, cols);

        for value in sample_uniform_f64(min, max, samples, seed) {
            let key = SketchInput::F64(value);
            sketch.insert(&key);
            *truth.entry(value.to_bits() as u64).or_insert(0) += 1;
        }

        (sketch, truth)
    }

    fn run_uniform_stream_fast(
        rows: usize,
        cols: usize,
        min: f64,
        max: f64,
        samples: usize,
        seed: u64,
    ) -> (CountMin, HashMap<u64, u64>) {
        let mut truth = HashMap::<u64, u64>::new();
        let mut sketch = CountMin::with_dimensions(rows, cols);

        for value in sample_uniform_f64(min, max, samples, seed) {
            let key = SketchInput::F64(value);
            sketch.fast_insert(&key);
            *truth.entry(value.to_bits() as u64).or_insert(0) += 1;
        }

        (sketch, truth)
    }

    // test for dimension of CMS after initialization
    #[test]
    fn dimension_test() {
        // test default sketch dimension
        let cm = CountMin::default();
        assert_eq!(cm.rows(), 3);
        assert_eq!(cm.cols(), 4096);
        let storage = cm.as_storage();
        all_counter_zero_u64(storage);

        // test for custom dimension size
        let cm_customize = CountMin::with_dimensions(3, 17);
        assert_eq!(cm_customize.rows(), 3);
        assert_eq!(cm_customize.cols(), 17);

        let storage_customize = cm_customize.as_storage();
        all_counter_zero_u64(storage_customize);
    }

    #[test]
    fn fast_insert_same_estimate() {
        let mut slow = CountMin::with_dimensions(3, 64);
        let mut fast = CountMin::with_dimensions(3, 64);

        let keys = vec![
            SketchInput::Str("alpha"),
            SketchInput::Str("beta"),
            SketchInput::Str("gamma"),
            SketchInput::Str("delta"),
            SketchInput::Str("epsilon"),
        ];

        for key in &keys {
            slow.insert(key);
            fast.fast_insert(key);
        }

        for key in &keys {
            assert_eq!(
                slow.estimate(key),
                fast.fast_estimate(key),
                "fast path should match standard insert for key {key:?}"
            );
        }
    }

    #[test]
    fn merge_adds_counters_element_wise() {
        let mut left = CountMin::with_dimensions(2, 32);
        let mut right = CountMin::with_dimensions(2, 32);
        let key = SketchInput::Str("delta");

        left.insert(&key);
        right.insert(&key);
        right.insert(&key);

        let left_indices: Vec<_> = (0..left.row)
            .map(|row| counter_index(row, &key, left.col))
            .collect();

        left.merge(&right);

        for (row, idx) in left_indices.into_iter().enumerate() {
            assert_eq!(left.as_storage().query_one_counter(row, idx), 3);
        }
    }

    #[test]
    #[should_panic(expected = "dimension mismatch while merging CountMin sketches")]
    fn merge_requires_matching_dimensions() {
        let mut left = CountMin::with_dimensions(2, 32);
        let right = CountMin::with_dimensions(3, 32);
        left.merge(&right);
    }

    // 0: AD840DF6E50083D93BF66518E6FF7E3D
    // 1: 6E17A626B9D8F6F4573FA3775F50F1B9
    // 2: 172A557358ABF80D6511148F7D5AA0F6
    // end of 0
    // 0: DD835708A07623292C138892AEB1F547
    // 1: F9B0D7EF1C2ED935F5F989B41B09FD89
    // 2: 7A0C88C68F4531C51BAC589557E8F18C
    // end of 1
    // 0: DD61C694F39B506F09D6B97F9CEBC585
    // 1: 301ADA0437C870752BFB3D0D0B5DA30D
    // 2: D21D14E3363A1092FFB15D675C083154
    // end of 2
    // 0: B5BEF3BF53A585FF1B2B5F62AAD24BED
    // 1: 7D6CA4F84CB7D09C6D1784F7D218F5CB
    // 2: CEFB01A00A53ABEC3BA981A6520EC1B8
    // end of 3
    // 0: 16D521D000A0D47C3F48F6337A57083F
    // 1: A66ED8D7F3BF42C301D95F41A176676A
    // 2: 37B2A6706E35FE54C157F0BB1C615FE6
    // end of 4
    // 0: 2BAE2A5230668C4AEADCEF720D5CED80
    // 1: B14095E9638EC368F72C7C42E0F56DDE
    // 2: 7FF0D70FE7F7724764106A10B7891FBC
    // end of 5
    // 0: 6C6614EE2E5470FBEF10E25F5699AC8B
    // 1: 96D75ECE5AC8EC01D28D283523CC5645
    // 2: 347E41D9E0FEE82CA99948EBF1EF4197
    // end of 6
    // 0: 6E47C6AF273700D4DF8A5CDAF096CEE8
    // 1: E688315FBE5FCA36A23BB2AC87FD72C3
    // 2: 218DCB5B3795D9F43769F64B6A145021
    // end of 7
    // 0: EB956D6716CF14B4CAE830DF0405ED5
    // 1: 321A4D0D0F8AD2DB5957FA84B0F8D249
    // 2: 6C931E49EA89960760D2E223598066DE
    // end of 8
    // 0: EA9B0E7B47192AB557372F968739F6CC
    // 1: 3053BA71E31A27332644EBD80DB20C55
    // 2: 3D95B11B7BD867ED20CE28729B869EC0
    // end of 9
    #[test]
    fn cm_regular_path_correctness() {
        let mut sk = CountMin::default();
        // insert 0~9
        for i in 0..10 {
            sk.insert(&SketchInput::I32(i));
        }
        let data = sk.as_storage().as_slice();
        // some counter is 1 now
        assert_eq!(
            data[0xE3D], 1,
            "incorrect value {} for row 0 of insertion i32 0",
            data[0xE3D]
        );
        assert_eq!(
            data[0x1B9 + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 0",
            data[0x1B9 + sk.col]
        );
        assert_eq!(
            data[0x0F6 + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 0",
            data[0x0F6 + sk.col * 2]
        );
        assert_eq!(
            data[0x547], 1,
            "incorrect value {} for row 0 of insertion i32 1",
            data[0x547]
        );
        assert_eq!(
            data[0xD89 + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 1",
            data[0xD89 + sk.col]
        );
        assert_eq!(
            data[0x18C + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 1",
            data[0x18C + sk.col * 2]
        );
        assert_eq!(
            data[0x585], 1,
            "incorrect value {} for row 0 of insertion i32 2",
            data[0x585]
        );
        assert_eq!(
            data[0x30D + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 2",
            data[0x30D + sk.col]
        );
        assert_eq!(
            data[0x154 + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 2",
            data[0x154 + sk.col * 2]
        );
        assert_eq!(
            data[0xBED], 1,
            "incorrect value {} for row 0 of insertion i32 3",
            data[0xBED]
        );
        assert_eq!(
            data[0x5CB + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 3",
            data[0x5CB + sk.col]
        );
        assert_eq!(
            data[0x1B8 + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 3",
            data[0x1B8 + sk.col * 2]
        );
        assert_eq!(
            data[0x83F], 1,
            "incorrect value {} for row 0 of insertion i32 4",
            data[0x83F]
        );
        assert_eq!(
            data[0x76A + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 4",
            data[0x76A + sk.col]
        );
        assert_eq!(
            data[0xFE6 + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 4",
            data[0xFE6 + sk.col * 2]
        );
        assert_eq!(
            data[0xD80], 1,
            "incorrect value {} for row 0 of insertion i32 5",
            data[0xD80]
        );
        assert_eq!(
            data[0xDDE + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 5",
            data[0xDDE + sk.col]
        );
        assert_eq!(
            data[0xFBC + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 5",
            data[0xFBC + sk.col * 2]
        );
        assert_eq!(
            data[0xC8B], 1,
            "incorrect value {} for row 0 of insertion i32 6",
            data[0xC8B]
        );
        assert_eq!(
            data[0x645 + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 6",
            data[0x645 + sk.col]
        );
        assert_eq!(
            data[0x197 + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 6",
            data[0x197 + sk.col * 2]
        );
        assert_eq!(
            data[0xEE8], 1,
            "incorrect value {} for row 0 of insertion i32 7",
            data[0xEE8]
        );
        assert_eq!(
            data[0x2C3 + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 7",
            data[0x2C3 + sk.col]
        );
        assert_eq!(
            data[0x021 + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 7",
            data[0x021 + sk.col * 2]
        );
        assert_eq!(
            data[0xED5], 1,
            "incorrect value {} for row 0 of insertion i32 8",
            data[0xED5]
        );
        assert_eq!(
            data[0x249 + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 8",
            data[0x249 + sk.col]
        );
        assert_eq!(
            data[0x6DE + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 8",
            data[0x6DE + sk.col * 2]
        );
        assert_eq!(
            data[0x6CC], 1,
            "incorrect value {} for row 0 of insertion i32 9",
            data[0x6CC]
        );
        assert_eq!(
            data[0xC55 + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 9",
            data[0xC55 + sk.col]
        );
        assert_eq!(
            data[0xEC0 + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 9",
            data[0xEC0 + sk.col * 2]
        );
        // other remains zero
        all_zero_except_u64(
            sk.as_storage(),
            vec![
                0xE3D,
                0x1B9 + sk.col,
                0x0F6 + sk.col * 2, // 0
                0x547,
                0xD89 + sk.col,
                0x18C + sk.col * 2, // 1
                0x585,
                0x30D + sk.col,
                0x154 + sk.col * 2, // 2
                0xBED,
                0x5CB + sk.col,
                0x1B8 + sk.col * 2, // 3
                0x83F,
                0x76A + sk.col,
                0xFE6 + sk.col * 2, // 4
                0xD80,
                0xDDE + sk.col,
                0xFBC + sk.col * 2, // 5
                0xC8B,
                0x645 + sk.col,
                0x197 + sk.col * 2, // 6
                0xEE8,
                0x2C3 + sk.col,
                0x021 + sk.col * 2, // 7
                0xED5,
                0x249 + sk.col,
                0x6DE + sk.col * 2, // 8
                0x6CC,
                0xC55 + sk.col,
                0xEC0 + sk.col * 2, // 9
            ],
        );
        for i in 0..10 {
            sk.insert(&SketchInput::I32(i));
        }
        let data = sk.as_storage().as_slice();
        // some counter is 2 now
        assert_eq!(
            data[0xE3D], 2,
            "incorrect value {} for row 0 of insertion i32 0",
            data[0xE3D]
        );
        assert_eq!(
            data[0x1B9 + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 0",
            data[0x1B9 + sk.col]
        );
        assert_eq!(
            data[0x0F6 + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 0",
            data[0x0F6 + sk.col * 2]
        );
        assert_eq!(
            data[0x547], 2,
            "incorrect value {} for row 0 of insertion i32 1",
            data[0x547]
        );
        assert_eq!(
            data[0xD89 + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 1",
            data[0xD89 + sk.col]
        );
        assert_eq!(
            data[0x18C + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 1",
            data[0x18C + sk.col * 2]
        );
        assert_eq!(
            data[0x585], 2,
            "incorrect value {} for row 0 of insertion i32 2",
            data[0x585]
        );
        assert_eq!(
            data[0x30D + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 2",
            data[0x30D + sk.col]
        );
        assert_eq!(
            data[0x154 + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 2",
            data[0x154 + sk.col * 2]
        );
        assert_eq!(
            data[0xBED], 2,
            "incorrect value {} for row 0 of insertion i32 3",
            data[0xBED]
        );
        assert_eq!(
            data[0x5CB + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 3",
            data[0x5CB + sk.col]
        );
        assert_eq!(
            data[0x1B8 + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 3",
            data[0x1B8 + sk.col * 2]
        );
        assert_eq!(
            data[0x83F], 2,
            "incorrect value {} for row 0 of insertion i32 4",
            data[0x83F]
        );
        assert_eq!(
            data[0x76A + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 4",
            data[0x76A + sk.col]
        );
        assert_eq!(
            data[0xFE6 + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 4",
            data[0xFE6 + sk.col * 2]
        );
        assert_eq!(
            data[0xD80], 2,
            "incorrect value {} for row 0 of insertion i32 5",
            data[0xD80]
        );
        assert_eq!(
            data[0xDDE + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 5",
            data[0xDDE + sk.col]
        );
        assert_eq!(
            data[0xFBC + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 5",
            data[0xFBC + sk.col * 2]
        );
        assert_eq!(
            data[0xC8B], 2,
            "incorrect value {} for row 0 of insertion i32 6",
            data[0xC8B]
        );
        assert_eq!(
            data[0x645 + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 6",
            data[0x645 + sk.col]
        );
        assert_eq!(
            data[0x197 + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 6",
            data[0x197 + sk.col * 2]
        );
        assert_eq!(
            data[0xEE8], 2,
            "incorrect value {} for row 0 of insertion i32 7",
            data[0xEE8]
        );
        assert_eq!(
            data[0x2C3 + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 7",
            data[0x2C3 + sk.col]
        );
        assert_eq!(
            data[0x021 + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 7",
            data[0x021 + sk.col * 2]
        );
        assert_eq!(
            data[0xED5], 2,
            "incorrect value {} for row 0 of insertion i32 8",
            data[0xED5]
        );
        assert_eq!(
            data[0x249 + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 8",
            data[0x249 + sk.col]
        );
        assert_eq!(
            data[0x6DE + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 8",
            data[0x6DE + sk.col * 2]
        );
        assert_eq!(
            data[0x6CC], 2,
            "incorrect value {} for row 0 of insertion i32 9",
            data[0x6CC]
        );
        assert_eq!(
            data[0xC55 + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 9",
            data[0xC55 + sk.col]
        );
        assert_eq!(
            data[0xEC0 + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 9",
            data[0xEC0 + sk.col * 2]
        );
        // other remains zero
        all_zero_except_u64(
            sk.as_storage(),
            vec![
                0xE3D,
                0x1B9 + sk.col,
                0x0F6 + sk.col * 2, // 0
                0x547,
                0xD89 + sk.col,
                0x18C + sk.col * 2, // 1
                0x585,
                0x30D + sk.col,
                0x154 + sk.col * 2, // 2
                0xBED,
                0x5CB + sk.col,
                0x1B8 + sk.col * 2, // 3
                0x83F,
                0x76A + sk.col,
                0xFE6 + sk.col * 2, // 4
                0xD80,
                0xDDE + sk.col,
                0xFBC + sk.col * 2, // 5
                0xC8B,
                0x645 + sk.col,
                0x197 + sk.col * 2, // 6
                0xEE8,
                0x2C3 + sk.col,
                0x021 + sk.col * 2, // 7
                0xED5,
                0x249 + sk.col,
                0x6DE + sk.col * 2, // 8
                0x6CC,
                0xC55 + sk.col,
                0xEC0 + sk.col * 2, // 9
            ],
        );
        // check estimate for 0~9 is 2
        for i in 0..10 {
            assert_eq!(
                sk.estimate(&SketchInput::I32(i)),
                2,
                "estimate for {i} should be 2, but get {}",
                sk.estimate(&SketchInput::I32(i))
            )
        }
    }

    #[test]
    fn cm_fast_path_correctness() {
        let mut sk = CountMin::default();
        // insert 0~9
        for i in 0..10 {
            sk.fast_insert(&SketchInput::I32(i));
        }
        let data = sk.as_storage().as_slice();
        // some counters are 1
        assert_eq!(
            data[0xE3D], 1,
            "incorrect value {} for row 0 of insertion i32 0",
            data[0xE3D]
        );
        assert_eq!(
            data[0xFF7 + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 0",
            data[0xFF7 + sk.col]
        );
        assert_eq!(
            data[0x8E6 + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 0",
            data[0x8E6 + sk.col * 2]
        );
        assert_eq!(
            data[0x547], 1,
            "incorrect value {} for row 0 of insertion i32 1",
            data[0x547]
        );
        assert_eq!(
            data[0xB1F + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 1",
            data[0xB1F + sk.col]
        );
        assert_eq!(
            data[0x2AE + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 1",
            data[0x2AE + sk.col * 2]
        );
        assert_eq!(
            data[0x585], 1,
            "incorrect value {} for row 0 of insertion i32 2",
            data[0x585]
        );
        assert_eq!(
            data[0xEBC + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 2",
            data[0xEBC + sk.col]
        );
        assert_eq!(
            data[0xF9C + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 2",
            data[0xF9C + sk.col * 2]
        );
        assert_eq!(
            data[0xBED], 1,
            "incorrect value {} for row 0 of insertion i32 3",
            data[0xBED]
        );
        assert_eq!(
            data[0xD24 + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 3",
            data[0xD24 + sk.col]
        );
        assert_eq!(
            data[0x2AA + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 3",
            data[0x2AA + sk.col * 2]
        );
        assert_eq!(
            data[0x83F], 1,
            "incorrect value {} for row 0 of insertion i32 4",
            data[0x83F]
        );
        assert_eq!(
            data[0x570 + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 4",
            data[0x570 + sk.col]
        );
        assert_eq!(
            data[0x37A + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 4",
            data[0x37A + sk.col * 2]
        );
        assert_eq!(
            data[0xD80], 1,
            "incorrect value {} for row 0 of insertion i32 5",
            data[0xD80]
        );
        assert_eq!(
            data[0x5CE + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 5",
            data[0x5CE + sk.col]
        );
        assert_eq!(
            data[0x20D + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 5",
            data[0x20D + sk.col * 2]
        );
        assert_eq!(
            data[0xC8B], 1,
            "incorrect value {} for row 0 of insertion i32 6",
            data[0xC8B]
        );
        assert_eq!(
            data[0x99A + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 6",
            data[0x99A + sk.col]
        );
        assert_eq!(
            data[0xF56 + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 6",
            data[0xF56 + sk.col * 2]
        );
        assert_eq!(
            data[0xEE8], 1,
            "incorrect value {} for row 0 of insertion i32 7",
            data[0xEE8]
        );
        assert_eq!(
            data[0x96C + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 7",
            data[0x96C + sk.col]
        );
        assert_eq!(
            data[0xAF0 + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 7",
            data[0xAF0 + sk.col * 2]
        );
        assert_eq!(
            data[0xED5], 1,
            "incorrect value {} for row 0 of insertion i32 8",
            data[0xED5]
        );
        assert_eq!(
            data[0x405 + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 8",
            data[0x405 + sk.col]
        );
        assert_eq!(
            data[0xDF0 + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 8",
            data[0xDF0 + sk.col * 2]
        );
        assert_eq!(
            data[0x6CC], 1,
            "incorrect value {} for row 0 of insertion i32 9",
            data[0x6CC]
        );
        assert_eq!(
            data[0x39F + sk.col],
            1,
            "incorrect value {} for row 1 of insertion i32 9",
            data[0x39F + sk.col]
        );
        assert_eq!(
            data[0x687 + sk.col * 2],
            1,
            "incorrect value {} for row 2 of insertion i32 9",
            data[0x687 + sk.col * 2]
        );
        // others are 0
        all_zero_except_u64(
            sk.as_storage(),
            vec![
                0xE3D,
                0xFF7 + sk.col,
                0x8E6 + sk.col * 2,
                0x547,
                0xB1F + sk.col,
                0x2AE + sk.col * 2,
                0x585,
                0xEBC + sk.col,
                0xF9C + sk.col * 2,
                0xBED,
                0xD24 + sk.col,
                0x2AA + sk.col * 2,
                0x83F,
                0x570 + sk.col,
                0x37A + sk.col * 2,
                0xD80,
                0x5CE + sk.col,
                0x20D + sk.col * 2,
                0xC8B,
                0x99A + sk.col,
                0xF56 + sk.col * 2,
                0xEE8,
                0x96C + sk.col,
                0xAF0 + sk.col * 2,
                0xED5,
                0x405 + sk.col,
                0xDF0 + sk.col * 2,
                0x6CC,
                0x39F + sk.col,
                0x687 + sk.col * 2,
            ],
        );
        // insert 0~9 again
        for i in 0..10 {
            sk.fast_insert(&SketchInput::I32(i));
        }
        let data = sk.as_storage().as_slice();
        // some counters are 2
        assert_eq!(
            data[0xE3D], 2,
            "incorrect value {} for row 0 of insertion i32 0",
            data[0xE3D]
        );
        assert_eq!(
            data[0xFF7 + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 0",
            data[0xFF7 + sk.col]
        );
        assert_eq!(
            data[0x8E6 + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 0",
            data[0x8E6 + sk.col * 2]
        );
        assert_eq!(
            data[0x547], 2,
            "incorrect value {} for row 0 of insertion i32 1",
            data[0x547]
        );
        assert_eq!(
            data[0xB1F + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 1",
            data[0xB1F + sk.col]
        );
        assert_eq!(
            data[0x2AE + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 1",
            data[0x2AE + sk.col * 2]
        );
        assert_eq!(
            data[0x585], 2,
            "incorrect value {} for row 0 of insertion i32 2",
            data[0x585]
        );
        assert_eq!(
            data[0xEBC + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 2",
            data[0xEBC + sk.col]
        );
        assert_eq!(
            data[0xF9C + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 2",
            data[0xF9C + sk.col * 2]
        );
        assert_eq!(
            data[0xBED], 2,
            "incorrect value {} for row 0 of insertion i32 3",
            data[0xBED]
        );
        assert_eq!(
            data[0xD24 + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 3",
            data[0xD24 + sk.col]
        );
        assert_eq!(
            data[0x2AA + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 3",
            data[0x2AA + sk.col * 2]
        );
        assert_eq!(
            data[0x83F], 2,
            "incorrect value {} for row 0 of insertion i32 4",
            data[0x83F]
        );
        assert_eq!(
            data[0x570 + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 4",
            data[0x570 + sk.col]
        );
        assert_eq!(
            data[0x37A + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 4",
            data[0x37A + sk.col * 2]
        );
        assert_eq!(
            data[0xD80], 2,
            "incorrect value {} for row 0 of insertion i32 5",
            data[0xD80]
        );
        assert_eq!(
            data[0x5CE + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 5",
            data[0x5CE + sk.col]
        );
        assert_eq!(
            data[0x20D + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 5",
            data[0x20D + sk.col * 2]
        );
        assert_eq!(
            data[0xC8B], 2,
            "incorrect value {} for row 0 of insertion i32 6",
            data[0xC8B]
        );
        assert_eq!(
            data[0x99A + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 6",
            data[0x99A + sk.col]
        );
        assert_eq!(
            data[0xF56 + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 6",
            data[0xF56 + sk.col * 2]
        );
        assert_eq!(
            data[0xEE8], 2,
            "incorrect value {} for row 0 of insertion i32 7",
            data[0xEE8]
        );
        assert_eq!(
            data[0x96C + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 7",
            data[0x96C + sk.col]
        );
        assert_eq!(
            data[0xAF0 + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 7",
            data[0xAF0 + sk.col * 2]
        );
        assert_eq!(
            data[0xED5], 2,
            "incorrect value {} for row 0 of insertion i32 8",
            data[0xED5]
        );
        assert_eq!(
            data[0x405 + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 8",
            data[0x405 + sk.col]
        );
        assert_eq!(
            data[0xDF0 + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 8",
            data[0xDF0 + sk.col * 2]
        );
        assert_eq!(
            data[0x6CC], 2,
            "incorrect value {} for row 0 of insertion i32 9",
            data[0x6CC]
        );
        assert_eq!(
            data[0x39F + sk.col],
            2,
            "incorrect value {} for row 1 of insertion i32 9",
            data[0x39F + sk.col]
        );
        assert_eq!(
            data[0x687 + sk.col * 2],
            2,
            "incorrect value {} for row 2 of insertion i32 9",
            data[0x687 + sk.col * 2]
        );
        // others are still 0
        all_zero_except_u64(
            sk.as_storage(),
            vec![
                0xE3D,
                0xFF7 + sk.col,
                0x8E6 + sk.col * 2,
                0x547,
                0xB1F + sk.col,
                0x2AE + sk.col * 2,
                0x585,
                0xEBC + sk.col,
                0xF9C + sk.col * 2,
                0xBED,
                0xD24 + sk.col,
                0x2AA + sk.col * 2,
                0x83F,
                0x570 + sk.col,
                0x37A + sk.col * 2,
                0xD80,
                0x5CE + sk.col,
                0x20D + sk.col * 2,
                0xC8B,
                0x99A + sk.col,
                0xF56 + sk.col * 2,
                0xEE8,
                0x96C + sk.col,
                0xAF0 + sk.col * 2,
                0xED5,
                0x405 + sk.col,
                0xDF0 + sk.col * 2,
                0x6CC,
                0x39F + sk.col,
                0x687 + sk.col * 2,
            ],
        );
        // check estimate for 0~9 is 2
        for i in 0..10 {
            assert_eq!(
                sk.fast_estimate(&SketchInput::I32(i)),
                2,
                "estimate for {i} should be 2, but get {}",
                sk.fast_estimate(&SketchInput::I32(i))
            )
        }
    }

    // test for zipf distribution for domain 8192 and exponent 1.1 with 200_000 items
    // verify: (1-delta)*(query_size) is within bound (epsilon*input_size)
    #[test]
    fn cm_error_bound_zipf() {
        // regular path
        let (sk, truth) = run_zipf_stream(
            DEFAULT_ROW_NUM,
            DEFAULT_COL_NUM,
            8192,
            1.1,
            200_000,
            0x5eed_c0de,
        );
        let epsilon = std::f64::consts::E / DEFAULT_COL_NUM as f64;
        let delta = 1.0 / std::f64::consts::E.powi(DEFAULT_ROW_NUM as i32);
        let error_bound = epsilon * 200_000 as f64;
        let keys = truth.keys();
        let correct_lower_bound = keys.len() as f64 * (1.0 - delta);
        let mut within_count = 0;
        for key in keys {
            let est = sk.estimate(&SketchInput::U64(*key));
            if (est.abs_diff(*truth.get(key).unwrap()) as f64) < error_bound {
                within_count += 1;
            }
        }
        assert!(
            within_count as f64 > correct_lower_bound,
            "in-bound items number {within_count} not greater than expected amount {correct_lower_bound}"
        );
        // fast path
        let (sk, truth) = run_zipf_stream_fast(
            DEFAULT_ROW_NUM,
            DEFAULT_COL_NUM,
            8192,
            1.1,
            200_000,
            0x5eed_c0de,
        );
        let epsilon = std::f64::consts::E / DEFAULT_COL_NUM as f64;
        let delta = 1.0 / std::f64::consts::E.powi(DEFAULT_ROW_NUM as i32);
        let error_bound = epsilon * 200_000 as f64;
        let keys = truth.keys();
        let correct_lower_bound = keys.len() as f64 * (1.0 - delta);
        let mut within_count = 0;
        for key in keys {
            let est = sk.fast_estimate(&SketchInput::U64(*key));
            if (est.abs_diff(*truth.get(key).unwrap()) as f64) < error_bound {
                within_count += 1;
            }
        }
        assert!(
            within_count as f64 > correct_lower_bound,
            "in-bound items number {within_count} not greater than expected amount {correct_lower_bound}"
        );
    }

    // test for uniform distribution from 100.0 to 1000.0 with 200_000 items
    // verify: (1-delta)*(query_size) is within bound (epsilon*input_size)
    #[test]
    fn cm_error_bound_uniform() {
        // regular path
        let (sk, truth) = run_uniform_stream(
            DEFAULT_ROW_NUM,
            DEFAULT_COL_NUM,
            100.0,
            1000.0,
            200_000,
            0x5eed_c0de,
        );
        let epsilon = std::f64::consts::E / DEFAULT_COL_NUM as f64;
        let delta = 1.0 / std::f64::consts::E.powi(DEFAULT_ROW_NUM as i32);
        let error_bound = epsilon * 200_000 as f64;
        let keys = truth.keys();
        let correct_lower_bound = keys.len() as f64 * (1.0 - delta);
        let mut within_count = 0;
        for key in keys {
            let est = sk.estimate(&SketchInput::U64(*key));
            if (est.abs_diff(*truth.get(key).unwrap()) as f64) < error_bound {
                within_count += 1;
            }
        }
        assert!(
            within_count as f64 > correct_lower_bound,
            "in-bound items number {within_count} not greater than expected amount {correct_lower_bound}"
        );
        // fast path
        let (sk, truth) = run_uniform_stream_fast(
            DEFAULT_ROW_NUM,
            DEFAULT_COL_NUM,
            100.0,
            1000.0,
            200_000,
            0x5eed_c0de,
        );
        let epsilon = std::f64::consts::E / DEFAULT_COL_NUM as f64;
        let delta = 1.0 / std::f64::consts::E.powi(DEFAULT_ROW_NUM as i32);
        let error_bound = epsilon * 200_000 as f64;
        let keys = truth.keys();
        let correct_lower_bound = keys.len() as f64 * (1.0 - delta);
        let mut within_count = 0;
        for key in keys {
            let est = sk.fast_estimate(&SketchInput::U64(*key));
            if (est.abs_diff(*truth.get(key).unwrap()) as f64) < error_bound {
                within_count += 1;
            }
        }
        assert!(
            within_count as f64 > correct_lower_bound,
            "in-bound items number {within_count} not greater than expected amount {correct_lower_bound}"
        );
    }

    #[test]
    fn count_min_round_trip_serialization() {
        let mut sketch = CountMin::with_dimensions(3, 8);
        sketch.insert(&SketchInput::U64(42));
        sketch.insert(&SketchInput::U64(7));

        let encoded = sketch.serialize_to_bytes().expect("serialize CountMin");
        assert!(!encoded.is_empty());
        let data_copied = encoded.clone();

        let decoded = CountMin::deserialize_from_bytes(&data_copied).expect("deserialize CountMin");

        assert_eq!(sketch.rows(), decoded.rows());
        assert_eq!(sketch.cols(), decoded.cols());
        assert_eq!(
            sketch.as_storage().as_slice(),
            decoded.as_storage().as_slice()
        );
    }
}
