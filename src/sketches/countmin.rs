use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};
use std::hash::Hash;

use super::MicroScope;
// use super::utils::{SketchInput, hash_it};
use super::super::input::{SketchInput, hash_it};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CountMin {
    pub row: usize,
    pub col: usize,
    pub matrix: Vec<Vec<u64>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CountMinMS {
    pub row: usize,
    pub col: usize,
    pub matrix: Vec<Vec<MicroScope>>,
}

pub struct MicroScopeCM {
    pub window_size: usize,
    pub row: usize,
    pub col: usize,
    pub matrix: Vec<Vec<Vec<u64>>>,
    pub zooming_counter: Vec<Vec<u64>>,
    pub shutter_counter: Vec<Vec<u64>>,
    pub sub_window_count: usize,
    pub pixel_counter_size: u8, // I think the size is smaller than 32, right?
    pub log_base: u8,           // use u8 for now, may change later
    pub lst: usize,             // tracks the previous sub window count
}

impl Default for CountMin {
    fn default() -> Self {
        Self::init_count_min()
    }
}

impl CountMin {
    pub fn debug(&self) -> () {
        println!("Counters: ");
        for i in 0..self.row {
            println!("row {}: {:?}", i, self.matrix[i]);
        }
    }

    pub fn init_count_min() -> Self {
        CountMin::init_cm_with_row_col(4, 32)
    }

    pub fn init_cm_with_row_col(r: usize, c: usize) -> Self {
        assert!(r <= 5, "Too many rows, not supported now");
        let mat = vec![vec![0; c]; r];
        CountMin {
            row: r,
            col: c,
            matrix: mat,
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>, RmpEncodeError> {
        rmp_serde::to_vec(self)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        rmp_serde::from_slice(bytes)
    }

    pub fn merge(&mut self, other: &CountMin) {
        assert!(self.row == other.row, "Row number different, cannot merge");
        assert!(self.col == other.col, "Col number different, cannot merge");
        for i in 0..self.row {
            for j in 0..self.col {
                self.matrix[i][j] += other.matrix[i][j];
            }
        }
    }

    // pub fn insert_cm<T: Hash+?Sized>(&mut self, val: &T) {
    //     // for i in 0..self.row {
    //     //     let h = utils::hash_with_seed(&val, self.hash_seed_lst[i]);
    //     //     // just use lower 32 bit, whatever
    //     //     let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //     //     self.matrix[i][idx] += 1;
    //     // }
    //     let mut min_row = Vec::new();
    //     let mut min_count = u64::MAX;
    //     for i in 0..self.row {
    //         let h = hash_it(i, &val);
    //         // just use lower 32 bit, whatever
    //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //         if self.matrix[i][idx] < min_count {
    //             min_row.clear();
    //             min_row.push(i);
    //             min_count = self.matrix[i][idx];
    //         } else if self.matrix[i][idx] == min_count {
    //             min_row.push(i);
    //         }
    //         // self.matrix[i][idx] += 1;
    //     }
    //     for i in min_row {
    //         let h = hash_it(i, &val);
    //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //         self.matrix[i][idx] += 1;
    //     }
    //     // self.matrix[min_row][idx] += 1;
    // }
    pub fn insert_cm(&mut self, val: &SketchInput) {
        let mut min_row = Vec::new();
        let mut min_count = u64::MAX;
        for i in 0..self.row {
            let h = hash_it(i, &val);
            // just use lower 32 bit, whatever
            let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
            if self.matrix[i][idx] < min_count {
                min_row.clear();
                min_row.push(i);
                min_count = self.matrix[i][idx];
            } else if self.matrix[i][idx] == min_count {
                min_row.push(i);
            }
        }
        for i in min_row {
            let h = hash_it(i, &val);
            let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
            self.matrix[i][idx] += 1;
        }
    }

    // pub fn get_est<T: Hash+?Sized>(&self, val: &T) -> u64 {
    //     let mut res = u64::MAX;
    //     for i in 0..self.row {
    //         let h = hash_it(i, &val);
    //         // just use lower 32 bit, whatever
    //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //         res = res.min(self.matrix[i][idx]);
    //     }
    //     res
    // }
    pub fn get_est(&self, val: &SketchInput) -> u64 {
        let mut res = u64::MAX;
        for i in 0..self.row {
            let h = hash_it(i, &val);
            // just use lower 32 bit, whatever
            let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
            res = res.min(self.matrix[i][idx]);
        }
        res
    }
}

impl CountMinMS {
    pub fn debug(&self) -> () {
        println!("Counters: ");
        for i in 0..self.row {
            for j in 0..self.col {
                println!("row {} col {}:", i, j);
                self.matrix[i][j].debug();
                println!("============")
            }
        }
    }

    pub fn init_cmms(r: usize, c: usize, w: usize, t: usize) -> Self {
        let mut mat = Vec::with_capacity(r);
        for _ in 0..r {
            let mut cur_row = Vec::with_capacity(c);
            for _ in 0..c {
                let ms = MicroScope::init_microscope(w, t);
                cur_row.push(ms);
            }
            mat.push(cur_row);
        }
        CountMinMS {
            row: r,
            col: c,
            matrix: mat,
        }
    }

    pub fn merge(&mut self, other: &CountMinMS, timestamp: u64) {
        assert!(self.row == other.row, "Row number different, cannot merge");
        assert!(self.col == other.col, "Col number different, cannot merge");
        for i in 0..self.row {
            for j in 0..self.col {
                self.matrix[i][j].merge(&other.matrix[i][j], timestamp);
            }
        }
    }

    // pub fn insert<T: Hash>(&mut self, val: &T, timestamp: u64) {
    //     for i in 0..self.row {
    //         let h = hash_it(i, &val);
    //         // just use lower 32 bit, whatever
    //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //         self.matrix[i][idx].insert(timestamp);
    //     }
    // }
    pub fn insert(&mut self, val: &SketchInput, timestamp: u64) {
        for i in 0..self.row {
            let h = hash_it(i, &val);
            // just use lower 32 bit, whatever
            let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
            self.matrix[i][idx].insert(timestamp);
        }
    }

    // pub fn delete<T: Hash>(&mut self, val: &T, timestamp: u64) {
    //     for i in 0..self.row {
    //         let h = hash_it(i, &val);
    //         // just use lower 32 bit, whatever
    //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //         self.matrix[i][idx].delete(timestamp);
    //     }
    // }
    pub fn delete(&mut self, val: &SketchInput, timestamp: u64) {
        for i in 0..self.row {
            let h = hash_it(i, &val);
            // just use lower 32 bit, whatever
            let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
            self.matrix[i][idx].delete(timestamp);
        }
    }

    // pub fn get_est<T: Hash>(&self, val: &T, timestamp: u64) -> f64 {
    //     let mut res = f64::MAX;
    //     for i in 0..self.row {
    //         let h = hash_it(i, &val);
    //         // just use lower 32 bit, whatever
    //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //         res = res.min(self.matrix[i][idx].query(timestamp));
    //     }
    //     res
    // }
    pub fn get_est(&self, val: &SketchInput, timestamp: u64) -> f64 {
        let mut res = f64::MAX;
        for i in 0..self.row {
            let h = hash_it(i, &val);
            // just use lower 32 bit, whatever
            let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
            res = res.min(self.matrix[i][idx].query(timestamp));
        }
        res
    }
}

impl MicroScopeCM {
    pub fn debug(&self) -> () {
        println!("Counters: ");
        for i in 0..self.row {
            println!("row {}: {:?}", i, self.matrix[i]);
        }
        println!("Zooming Counters: ");
        for i in 0..self.row {
            println!("row {}: {:?}", i, self.zooming_counter[i]);
        }
        println!("Shutter Counters: ");
        for i in 0..self.row {
            println!("row {}: {:?}", i, self.shutter_counter[i]);
        }
    }

    pub fn init_mscm(s1: &Vec<u64>) -> Self {
        assert!(
            s1.len() == 4,
            "Hash seeds number different from 4, consider using init_mscm_with_rc() instead"
        );
        MicroScopeCM::init_mscm_with_rc(10000, 4, 32, 3, 4, 1, s1)
    }

    pub fn init_mscm_with_rc(
        w: usize,
        r: usize,
        c: usize,
        t: usize,
        k: u8,
        l: u8,
        s1: &Vec<u64>,
    ) -> Self {
        assert!(
            s1.len() == r,
            "Hash seeds number different from row count, cannot create sketch"
        );
        let mat = vec![vec![vec![0; t + 2]; c]; r];
        MicroScopeCM {
            window_size: w,
            row: r,
            col: c,
            matrix: mat,
            zooming_counter: vec![vec![0; c]; r],
            shutter_counter: vec![vec![0; c]; r],
            sub_window_count: t + 2, // why not just pass a value that is already +2
            pixel_counter_size: k,
            log_base: l,
            lst: 2 * (t + 2), // I think the point is to make sure it will not be the same with sub window count initially
                              // lst: 0, // weird, set it to 0 for now
        }
    }

    pub fn counter_add(&mut self, h: usize, idx: usize, cur: usize) {
        self.shutter_counter[h][idx] += 1;
        while self.shutter_counter[h][idx] >> (self.log_base as u64 * self.zooming_counter[h][idx])
            != 0
        {
            self.shutter_counter[h][idx] -=
                1 << (self.log_base as u64 * self.zooming_counter[h][idx]);
            if self.matrix[h][idx][cur] == (1 << self.pixel_counter_size) - 1 {
                self.zooming_counter[h][idx] += 1;
                self.matrix[h][idx][cur] = 1 << (self.pixel_counter_size - self.log_base);
                for i in 0..self.sub_window_count {
                    if i != cur {
                        self.matrix[h][idx][i] += (1 << self.log_base) - 1;
                        self.matrix[h][idx][i] >>= self.log_base;
                    }
                }
            } else {
                self.matrix[h][idx][cur] += 1;
            }
        }
    }

    pub fn counter_convert(&mut self, h: usize, idx: usize, cur: usize) {
        self.shutter_counter[h][idx] = 0;
        if self.matrix[h][idx][cur] == ((0b1 << self.pixel_counter_size) - 1) {
            self.zooming_counter[h][idx] += 1;
            self.matrix[h][idx][cur] = 0b1 << (self.pixel_counter_size - self.log_base);
            for i in 0..self.sub_window_count {
                if i != cur {
                    self.matrix[h][idx][i] += (0b1 << self.log_base) - 1;
                    self.matrix[h][idx][i] >>= self.log_base;
                }
            }
        } else {
            self.matrix[h][idx][cur] += 1;
        }
    }

    pub fn counter_maintain(&mut self, h: usize, idx: usize) {
        while self.zooming_counter[h][idx] > 0 {
            let mut mle = false;
            for i in 0..self.sub_window_count {
                if self.matrix[h][idx][i] >> (self.pixel_counter_size - self.log_base) != 0 {
                    mle = true;
                }
            }
            if mle {
                return;
            }
            self.zooming_counter[h][idx] -= 1;
            for i in 0..self.sub_window_count {
                self.matrix[h][idx][i] <<= self.log_base;
            }
        }
    }

    pub fn counter_clear(&mut self, h: usize, idx: usize, cur: usize) {
        let mut local_cur = cur + 1;
        if local_cur >= self.sub_window_count {
            local_cur -= self.sub_window_count;
        }
        if self.matrix[h][idx][local_cur] == 0 {
            return;
        }
        self.matrix[h][idx][local_cur] = 0;
        self.counter_maintain(h, idx);
    }

    pub fn counter_query(&mut self, hash_ids: &Vec<usize>, cur: usize, rate: f64) -> f64 {
        let mut res = 0.0;
        let mut tmp = 2000000000;
        for k in 0..self.row {
            tmp = tmp.min(
                self.shutter_counter[k][hash_ids[k]]
                    + (self.matrix[k][hash_ids[k]][cur]
                        << (self.log_base as u64 * self.zooming_counter[k][hash_ids[k]])),
            );
        }
        let mut local_cur;
        if cur == 0 {
            local_cur = self.sub_window_count - 1;
        } else {
            local_cur = cur - 1;
        }
        res += tmp as f64;
        for _ in 0..(self.sub_window_count - 3) {
            tmp = 2000000000;
            for k in 0..self.row {
                tmp = tmp.min(
                    self.matrix[k][hash_ids[k]][local_cur]
                        << (self.log_base as u64 * self.zooming_counter[k][hash_ids[k]]),
                );
            }
            res += tmp as f64;
            if local_cur == 0 {
                local_cur = self.sub_window_count - 1;
            } else {
                local_cur -= 1;
            }
        }
        tmp = 2000000000;
        for k in 0..self.row {
            tmp = tmp.min(
                self.matrix[k][hash_ids[k]][local_cur]
                    << (self.log_base as u64 * self.zooming_counter[k][hash_ids[k]]),
            );
        }
        res += tmp as f64 * rate;
        return res;
    }

    // pub fn insert_mscm<T: Hash>(&mut self, val: &T, time_stamp: u64) {
    //     let sub_window_size = self.window_size as f64 / (self.sub_window_count as f64 - 2.0);
    //     let cur_sub_window = (time_stamp as f64 / sub_window_size) as usize % self.sub_window_count;
    //     // let cur_sub_window =
    //     // ((time_stamp as f64 / (self.window_size as f64 / (self.sub_window_count as f64-2.0))) % self.sub_window_count as f64) as usize;
    //     let mut idxes = Vec::with_capacity(self.row);
    //     for i in 0..self.row {
    //         let h = hash_it(i, &val);
    //         // just use lower 32 bit, whatever
    //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //         idxes.push(idx);
    //     }
    //     // the rate is... the porportion of subwindow that is still valid?
    //     // let rate =
    //     // 1.0 - 1.0 * (time_stamp % (self.window_size as u64 / (self.sub_window_count as u64 - 2))) as f64 / (self.window_size / (self.sub_window_count - 2)) as f64;
    //     // self.matrix[i][idx] += 1;
    //     // println!("cur_sub_window: {}, self.lst: {}", cur_sub_window, self.lst);
    //     if cur_sub_window != self.lst && self.lst < self.sub_window_count {
    //         for k in 0..self.row {
    //             for idx in 0..self.col {
    //                 self.counter_convert(k, idx, self.lst);
    //                 self.counter_clear(k, idx, cur_sub_window);
    //             }
    //         }
    //     }
    //     self.lst = cur_sub_window;
    //     for k in 0..self.row {
    //         self.counter_add(k, idxes[k], cur_sub_window);
    //     }
    // }
    pub fn insert_mscm(&mut self, val: &SketchInput, time_stamp: u64) {
        let sub_window_size = self.window_size as f64 / (self.sub_window_count as f64 - 2.0);
        let cur_sub_window = (time_stamp as f64 / sub_window_size) as usize % self.sub_window_count;
        // let cur_sub_window =
        // ((time_stamp as f64 / (self.window_size as f64 / (self.sub_window_count as f64-2.0))) % self.sub_window_count as f64) as usize;
        let mut idxes = Vec::with_capacity(self.row);
        for i in 0..self.row {
            let h = hash_it(i, &val);
            // just use lower 32 bit, whatever
            let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
            idxes.push(idx);
        }
        // the rate is... the porportion of subwindow that is still valid?
        // let rate =
        // 1.0 - 1.0 * (time_stamp % (self.window_size as u64 / (self.sub_window_count as u64 - 2))) as f64 / (self.window_size / (self.sub_window_count - 2)) as f64;
        // self.matrix[i][idx] += 1;
        // println!("cur_sub_window: {}, self.lst: {}", cur_sub_window, self.lst);
        if cur_sub_window != self.lst && self.lst < self.sub_window_count {
            for k in 0..self.row {
                for idx in 0..self.col {
                    self.counter_convert(k, idx, self.lst);
                    self.counter_clear(k, idx, cur_sub_window);
                }
            }
        }
        self.lst = cur_sub_window;
        for k in 0..self.row {
            self.counter_add(k, idxes[k], cur_sub_window);
        }
    }

    // pub fn query<T: Hash>(&mut self, val: &T, time_stamp: u64) -> f64 {
    //     let cur_sub_window =
    //     ((time_stamp as f64 / (self.window_size as f64 / (self.sub_window_count as f64-2.0))) % self.sub_window_count as f64) as usize;
    //     let mut idxes = Vec::with_capacity(self.row);
    //     for i in 0..self.row {
    //         let h = hash_it(i, &val);
    //         // just use lower 32 bit, whatever
    //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //         idxes.push(idx);
    //     }
    //     // the rate is... the porportion of subwindow that is still valid?
    //     let rate =
    //     1.0 - 1.0 * (time_stamp as f64 % (self.window_size as f64 / (self.sub_window_count as f64 - 2.0))) as f64 / (self.window_size as f64 / (self.sub_window_count as f64 - 2.0)) as f64;
    //     return self.counter_query(&idxes, cur_sub_window, rate);
    // }
    pub fn query<T: Hash>(&mut self, val: &SketchInput, time_stamp: u64) -> f64 {
        let cur_sub_window = ((time_stamp as f64
            / (self.window_size as f64 / (self.sub_window_count as f64 - 2.0)))
            % self.sub_window_count as f64) as usize;
        let mut idxes = Vec::with_capacity(self.row);
        for i in 0..self.row {
            let h = hash_it(i, &val);
            // just use lower 32 bit, whatever
            let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
            idxes.push(idx);
        }
        // the rate is... the porportion of subwindow that is still valid?
        let rate = 1.0
            - 1.0
                * (time_stamp as f64
                    % (self.window_size as f64 / (self.sub_window_count as f64 - 2.0)))
                    as f64
                / (self.window_size as f64 / (self.sub_window_count as f64 - 2.0)) as f64;
        return self.counter_query(&idxes, cur_sub_window, rate);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sketches::test_utils::sample_zipf_u64;
    use crate::{SketchInput, hash_it};
    use std::collections::HashMap;

    fn counter_index(row: usize, key: &SketchInput, columns: usize) -> usize {
        let hash = hash_it(row, key);
        ((hash & ((0x1 << 32) - 1)) as usize) % columns
    }

    fn run_zipf_stream(
        rows: usize,
        cols: usize,
        domain: usize,
        exponent: f64,
        samples: usize,
        seed: u64,
    ) -> (CountMin, HashMap<u64, u64>) {
        let mut truth = HashMap::<u64, u64>::new();
        let mut sketch = CountMin::init_cm_with_row_col(rows, cols);

        for value in sample_zipf_u64(domain, exponent, samples, seed) {
            let key = SketchInput::U64(value);
            sketch.insert_cm(&key);
            *truth.entry(value).or_insert(0) += 1;
        }

        (sketch, truth)
    }
    #[test]
    fn default_initializes_expected_dimensions() {
        let cm = CountMin::default();
        assert_eq!(cm.row, 4);
        assert_eq!(cm.col, 32);
        assert!(
            cm.matrix
                .iter()
                .all(|row| row.len() == cm.col && row.iter().all(|&value| value == 0))
        );
    }

    #[test]
    fn init_cm_with_row_col_uses_custom_sizes() {
        let cm = CountMin::init_cm_with_row_col(3, 17);
        assert_eq!(cm.row, 3);
        assert_eq!(cm.col, 17);
        assert!(
            cm.matrix
                .iter()
                .all(|row| row.len() == cm.col && row.iter().all(|&value| value == 0))
        );
    }

    #[test]
    fn insert_cm_updates_all_minimal_rows() {
        let mut cm = CountMin::init_cm_with_row_col(4, 64);
        let key = SketchInput::Str("alpha");

        cm.insert_cm(&key);

        for row in 0..cm.row {
            let idx = counter_index(row, &key, cm.col);
            assert_eq!(cm.matrix[row][idx], 1, "row {} counter should be 1", row);
        }
    }

    #[test]
    fn insert_cm_prefers_rows_with_lowest_count() {
        let mut cm = CountMin::init_cm_with_row_col(2, 32);
        let key = SketchInput::Str("alpha");

        let row0_idx = counter_index(0, &key, cm.col);
        let row1_idx = counter_index(1, &key, cm.col);

        cm.matrix[0][row0_idx] = 5;
        cm.matrix[1][row1_idx] = 2;

        cm.insert_cm(&key);

        assert_eq!(cm.matrix[0][row0_idx], 5);
        assert_eq!(cm.matrix[1][row1_idx], 3);
    }

    #[test]
    fn get_est_returns_smallest_counter_for_key() {
        let mut cm = CountMin::init_cm_with_row_col(3, 32);
        let key = SketchInput::Str("gamma");

        for row in 0..cm.row {
            let idx = counter_index(row, &key, cm.col);
            cm.matrix[row][idx] = (row as u64 + 4) * 2;
        }

        assert_eq!(cm.get_est(&key), 8);
    }

    #[test]
    fn merge_adds_counters_element_wise() {
        let mut left = CountMin::init_cm_with_row_col(2, 32);
        let mut right = CountMin::init_cm_with_row_col(2, 32);
        let key = SketchInput::Str("delta");

        left.insert_cm(&key);
        right.insert_cm(&key);
        right.insert_cm(&key);

        let left_indices: Vec<_> = (0..left.row)
            .map(|row| counter_index(row, &key, left.col))
            .collect();

        left.merge(&right);

        for (row, idx) in left_indices.into_iter().enumerate() {
            assert_eq!(left.matrix[row][idx], 3);
        }
    }

    #[test]
    #[should_panic(expected = "Row number different")]
    fn merge_requires_matching_dimensions() {
        let mut left = CountMin::init_cm_with_row_col(2, 32);
        let right = CountMin::init_cm_with_row_col(3, 32);
        left.merge(&right);
    }

    #[test]
    fn zipf_stream_stays_within_five_percent_for_most_keys() {
        let (sketch, truth) = run_zipf_stream(5, 8192, 8192, 1.1, 200_000, 0x5eed_c0de);
        let mut within_tolerance = 0usize;
        for (&value, &count) in &truth {
            let estimate = sketch.get_est(&SketchInput::U64(value));
            let rel_error = (estimate.abs_diff(count) as f64) / (count as f64);
            if rel_error < 0.05 {
                within_tolerance += 1;
            }
        }

        let total = truth.len();
        let accuracy = within_tolerance as f64 / total as f64;
        assert!(
            accuracy >= 0.95,
            "Only {:.2}% of keys within tolerance ({} of {}); expected at least 95%",
            accuracy * 100.0,
            within_tolerance,
            total
        );
    }

    #[test]
    fn zipf_stream_estimates_heavy_hitters_within_three_percent() {
        let (sketch, truth) = run_zipf_stream(3, 512, 8192, 1.1, 200_000, 0x5eed_c0de);
        let mut counts: Vec<(u64, u64)> = truth.iter().map(|(&k, &v)| (k, v)).collect();
        counts.sort_unstable_by(|a, b| b.1.cmp(&a.1));

        let top_k = counts.len().min(25);
        assert!(top_k > 0, "expected at least one heavy hitter");

        for (key, count) in counts.into_iter().take(top_k) {
            let estimate = sketch.get_est(&SketchInput::U64(key));
            let rel_error = (estimate.abs_diff(count) as f64) / (count as f64);
            assert!(
                rel_error < 0.03,
                "Heavy hitter key {key} truth {count} estimate {estimate} rel error {rel_error:.4}"
            );
        }
    }

    #[test]
    fn count_min_round_trip_serialization() {
        let mut sketch = CountMin::init_cm_with_row_col(3, 8);
        sketch.insert_cm(&SketchInput::U64(42));
        sketch.insert_cm(&SketchInput::U64(7));

        let encoded = sketch.serialize().expect("serialize CountMin");
        assert!(!encoded.is_empty());

        let decoded = CountMin::deserialize(&encoded).expect("deserialize CountMin");

        assert_eq!(sketch.row, decoded.row);
        assert_eq!(sketch.col, decoded.col);
        assert_eq!(sketch.matrix, decoded.matrix);
    }
}
