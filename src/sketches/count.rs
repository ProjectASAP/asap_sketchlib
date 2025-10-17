use super::utils::{LASTSTATE, SketchInput, hash_it};
use serde::{Deserialize, Serialize};

pub struct Count {
    pub row: usize,
    pub col: usize,
    pub matrix: Vec<Vec<i64>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CountUniv {
    pub row: usize,
    pub col: usize,
    pub matrix: Vec<Vec<i64>>,
    pub l2: Vec<i64>,
}

impl Default for Count {
    fn default() -> Self {
        Self::init_count()
    }
}

impl Count {
    pub fn debug(&self) -> () {
        println!("Counters: ");
        for i in 0..self.row {
            println!("row {}: {:?}", i, self.matrix[i]);
        }
    }

    pub fn init_count() -> Self {
        Count::init_count_with_rc(4, 32)
    }

    pub fn init_count_with_rc(r: usize, c: usize) -> Self {
        assert!(r <= 5, "Too many rows, not supported now");
        let mat = vec![vec![0; c]; r];
        Count {
            row: r,
            col: c,
            matrix: mat,
        }
    }

    pub fn merge(&mut self, other: &Count) {
        assert!(self.row == other.row, "Row number different, cannot merge");
        assert!(self.col == other.col, "Col number different, cannot merge");
        for i in 0..self.row {
            for j in 0..self.col {
                self.matrix[i][j] += other.matrix[i][j];
            }
        }
    }

    // pub fn insert_count<T: Hash>(&mut self, val: &T) {
    //     for i in 0..self.row {
    //         let h = hash_it(i, &val);
    //         let s = hash_it(LASTSTATE, &val);
    //         // just use lower 32 bit, whatever
    //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //         let sign = s % 2;
    //         if sign == 1 { self.matrix[i][idx] += 1; } else { self.matrix[i][idx] -= 1; }
    //     }
    // }
    pub fn insert_count(&mut self, val: &SketchInput) {
        for i in 0..self.row {
            let h = hash_it(i, &val);
            let s = hash_it(LASTSTATE, &val);
            // just use lower 32 bit, whatever
            let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
            let sign = s % 2;
            if sign == 1 {
                self.matrix[i][idx] += 1;
            } else {
                self.matrix[i][idx] -= 1;
            }
        }
    }

    // pub fn get_est<T: Hash>(&self, val: &T) -> f64 {
    //     let mut lst = Vec::new();
    //     for i in 0..self.row {
    //         let h = hash_it(i, &val);
    //         let s = hash_it(LASTSTATE, &val);
    //         // just use lower 32 bit, whatever
    //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //         let sign = s % 2;
    //         if sign == 1 { lst.push(self.matrix[i][idx]); } else { lst.push(self.matrix[i][idx] * (-1)); }
    //     }
    //     lst.sort();
    //     // get median
    //     if self.row == 1 {
    //         return lst[0] as f64;
    //     } else if self.row == 2 {
    //         return (lst[0] + lst[1]) as f64 / 2.0;
    //     } else if self.row == 3 {
    //         return lst[1] as f64;
    //     } else if self.row % 2 == 0 {
    //         return (lst[self.row/2] + lst[(self.row/2) - 1]) as f64 / 2.0;
    //     } else {
    //         return lst[self.row / 2] as f64;
    //     }
    // }
    pub fn get_est(&self, val: &SketchInput) -> f64 {
        let mut lst = Vec::new();
        for i in 0..self.row {
            let h = hash_it(i, &val);
            let s = hash_it(LASTSTATE, &val);
            // just use lower 32 bit, whatever
            let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
            let sign = s % 2;
            if sign == 1 {
                lst.push(self.matrix[i][idx]);
            } else {
                lst.push(self.matrix[i][idx] * (-1));
            }
        }
        lst.sort();
        // get median
        if self.row == 1 {
            return lst[0] as f64;
        } else if self.row == 2 {
            return (lst[0] + lst[1]) as f64 / 2.0;
        } else if self.row == 3 {
            return lst[1] as f64;
        } else if self.row % 2 == 0 {
            return (lst[self.row / 2] + lst[(self.row / 2) - 1]) as f64 / 2.0;
        } else {
            return lst[self.row / 2] as f64;
        }
    }
}

impl Default for CountUniv {
    fn default() -> Self {
        Self::init_count()
    }
}

impl CountUniv {
    pub fn debug(&self) -> () {
        println!("Counters: ");
        for i in 0..self.row {
            println!("row {}: {:?}", i, self.matrix[i]);
        }
        println!("L2: {:?}", self.l2);
    }

    pub fn init_count() -> Self {
        CountUniv::init_countuniv_with_rc(4, 32)
    }

    pub fn init_countuniv_with_rc(r: usize, c: usize) -> Self {
        assert!(r <= 5, "Too many rows, not supported now");
        let mat = vec![vec![0; c]; r];
        CountUniv {
            row: r,
            col: c,
            matrix: mat,
            l2: vec![0; r],
        }
    }

    pub fn merge(&mut self, other: &CountUniv) {
        assert!(self.row == other.row, "Row number different, cannot merge");
        assert!(self.col == other.col, "Col number different, cannot merge");
        for i in 0..self.row {
            for j in 0..self.col {
                self.matrix[i][j] += other.matrix[i][j];
            }
        }
    }

    // pub fn insert_once<T: Hash+?Sized>(&mut self, val: &T) {
    //     self.insert_with_count(val, 1);
    // }
    pub fn insert_once(&mut self, val: &SketchInput) {
        self.insert_with_count(val, 1);
    }

    // pub fn insert_with_count<T: Hash+?Sized>(&mut self, val: &T, c: i64) {
    //     for i in 0..self.row {
    //         let h = hash_it(i, &val);
    //         let s = hash_it(LASTSTATE, &val);
    //         // just use lower 32 bit, whatever
    //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //         let sign = s % 2;
    //         let old_value = self.matrix[i][idx];
    //         if sign == 1 {
    //             self.matrix[i][idx] += c;
    //         } else {
    //             self.matrix[i][idx] -= c;
    //         }
    //         self.l2[i] = self.l2[i] + self.matrix[i][idx]*self.matrix[i][idx] - old_value*old_value;
    //     }
    // }
    pub fn insert_with_count(&mut self, val: &SketchInput, c: i64) {
        for i in 0..self.row {
            let h = hash_it(i, &val);
            let s = hash_it(LASTSTATE, &val);
            // just use lower 32 bit, whatever
            let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
            let sign = s % 2;
            let old_value = self.matrix[i][idx];
            if sign == 1 {
                self.matrix[i][idx] += c;
            } else {
                self.matrix[i][idx] -= c;
            }
            self.l2[i] =
                self.l2[i] + self.matrix[i][idx] * self.matrix[i][idx] - old_value * old_value;
        }
    }

    // pub fn insert_with_count_without_l2<T: Hash+?Sized>(&mut self, val: &T, c: i64) {
    //     for i in 0..self.row {
    //         let h = hash_it(i, &val);
    //         let s = hash_it(LASTSTATE, &val);
    //         // just use lower 32 bit, whatever
    //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //         let sign = s % 2;
    //         if sign == 1 {
    //             self.matrix[i][idx] += c;
    //         } else {
    //             self.matrix[i][idx] -= c;
    //         }
    //     }
    // }
    pub fn insert_with_count_without_l2(&mut self, val: &SketchInput, c: i64) {
        for i in 0..self.row {
            let h = hash_it(i, &val);
            let s = hash_it(LASTSTATE, &val);
            // just use lower 32 bit, whatever
            let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
            let sign = s % 2;
            if sign == 1 {
                self.matrix[i][idx] += c;
            } else {
                self.matrix[i][idx] -= c;
            }
        }
    }

    // pub fn update_and_est<T: Hash+?Sized>(&mut self, val: &T, c: i64) -> f64 {
    //     self.insert_with_count(val, c);
    //     self.get_est(val)
    // }
    pub fn update_and_est(&mut self, val: &SketchInput, c: i64) -> f64 {
        self.insert_with_count(val, c);
        self.get_est(val)
    }

    // pub fn update_and_est_without_l2<T: Hash+?Sized>(&mut self, val: &T, c: i64) -> f64 {
    //     self.insert_with_count_without_l2(val, c);
    //     self.get_est(val)
    // }
    pub fn update_and_est_without_l2(&mut self, val: &SketchInput, c: i64) -> f64 {
        self.insert_with_count_without_l2(val, c);
        self.get_est(val)
    }

    pub fn get_l2_sqr(&self) -> f64 {
        let mut lst = Vec::new();
        for i in 0..self.row {
            lst.push(self.l2[i]);
        }
        lst.sort();
        // get median
        if self.row == 1 {
            return lst[0] as f64;
        } else if self.row == 2 {
            return (lst[0] + lst[1]) as f64 / 2.0;
        } else if self.row == 3 {
            return lst[1] as f64;
        } else if self.row % 2 == 0 {
            return (lst[self.row / 2] + lst[(self.row / 2) - 1]) as f64 / 2.0;
        } else {
            return lst[self.row / 2] as f64;
        }
    }

    pub fn get_l2(&self) -> f64 {
        // let mut lst = Vec::new();
        // for i in 0..self.row {
        //     lst.push(self.l2[i]);
        // }
        // lst.sort();
        // // get median
        // let l2;
        // if self.row == 1 {
        //     l2 = lst[0] as f64;
        // } else if self.row == 2 {
        //     l2 = (lst[0] + lst[1]) as f64 / 2.0;
        // } else if self.row == 3 {
        //     l2 = lst[1] as f64;
        // } else if self.row % 2 == 0 {
        //     l2 =  (lst[self.row/2] + lst[(self.row/2) - 1]) as f64 / 2.0;
        // } else {
        //     l2 = lst[self.row / 2] as f64;
        // }
        let l2 = self.get_l2_sqr();
        return l2.sqrt();
    }

    // pub fn get_est<T: Hash+?Sized>(&self, val: &T) -> f64 {
    //     let mut lst = Vec::new();
    //     for i in 0..self.row {
    //         let h = hash_it(i, &val);
    //         let s = hash_it(LASTSTATE, &val);
    //         // just use lower 32 bit, whatever
    //         let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
    //         let sign = s % 2;
    //         if sign == 1 { lst.push(self.matrix[i][idx]); } else { lst.push(self.matrix[i][idx] * (-1)); }
    //     }
    //     lst.sort();
    //     // get median
    //     if self.row == 1 {
    //         return lst[0] as f64;
    //     } else if self.row == 2 {
    //         return (lst[0] + lst[1]) as f64 / 2.0;
    //     } else if self.row == 3 {
    //         return lst[1] as f64;
    //     } else if self.row % 2 == 0 {
    //         return (lst[self.row/2] + lst[(self.row/2) - 1]) as f64 / 2.0;
    //     } else {
    //         return lst[self.row / 2] as f64;
    //     }
    // }
    pub fn get_est(&self, val: &SketchInput) -> f64 {
        let mut lst = Vec::new();
        for i in 0..self.row {
            let h = hash_it(i, &val);
            let s = hash_it(LASTSTATE, &val);
            // just use lower 32 bit, whatever
            let idx = ((h & ((0x1 << 32) - 1)) as usize) % self.col;
            let sign = s % 2;
            if sign == 1 {
                lst.push(self.matrix[i][idx]);
            } else {
                lst.push(self.matrix[i][idx] * (-1));
            }
        }
        lst.sort();
        // get median
        if self.row == 1 {
            return lst[0] as f64;
        } else if self.row == 2 {
            return (lst[0] + lst[1]) as f64 / 2.0;
        } else if self.row == 3 {
            return lst[1] as f64;
        } else if self.row % 2 == 0 {
            return (lst[self.row / 2] + lst[(self.row / 2) - 1]) as f64 / 2.0;
        } else {
            return lst[self.row / 2] as f64;
        }
    }
}
