use crate::{SketchInput, hash_it};
use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bucket<'long_enough_sketch> {
    // pub full_key: Option<String>,
    #[serde(borrow)]
    pub full_key: Option<SketchInput<'long_enough_sketch>>,
    pub val: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Coco<'long_enough_sketch> {
    pub w: usize,
    pub d: usize,
    #[serde(borrow)]
    pub table: Vec<Vec<Bucket<'long_enough_sketch>>>,
}

impl<'long_enough_sketch> Default for Bucket<'long_enough_sketch> {
    fn default() -> Self {
        Self::new()
    }
}

// I believe this means 'long_enough_sketch will outlive 'just_for_est
impl<'long_enough_sketch, 'just_for_est> Bucket<'long_enough_sketch> {
    pub fn new() -> Self {
        Bucket {
            full_key: None,
            val: 0,
        }
    }

    pub fn update_key(&mut self, key: &SketchInput<'long_enough_sketch>) {
        self.full_key = Some(key.clone());
    }

    // apparently, this is far less than finish
    pub fn is_partial_key(&mut self, partial_key: &SketchInput) -> bool {
        match (&self.full_key, partial_key) {
            (Some(SketchInput::String(full)), SketchInput::String(partial)) => {
                (*full).contains(partial)
            }
            (Some(SketchInput::Str(full)), &SketchInput::Str(partial)) => (*full).contains(partial),
            (Some(SketchInput::String(full)), &SketchInput::Str(partial)) => {
                (*full).contains(partial)
            }
            (Some(SketchInput::Str(full)), SketchInput::String(partial)) => {
                (*full).contains(partial)
            }
            _ => false,
        }
    }
    /// the function should take in full key first, then partial key
    pub fn is_partial_key_with_udf<F>(
        &mut self,
        partial_key: &SketchInput<'just_for_est>,
        udf: F,
    ) -> bool
    where
        F: Fn(&SketchInput<'long_enough_sketch>, &SketchInput<'just_for_est>) -> bool,
    {
        match &self.full_key {
            Some(k) => udf(k, partial_key),
            None => false,
        }
    }

    pub fn debug(&mut self) {
        match &self.full_key {
            Some(k) => match k {
                SketchInput::I32(i) => print!(" <i32::{}, {}> ", i, self.val),
                SketchInput::I64(i) => print!(" <i64::{}, {}> ", i, self.val),
                SketchInput::U32(u) => print!(" <u32::{}, {}> ", u, self.val),
                SketchInput::U64(u) => print!(" <u64::{}, {}> ", u, self.val),
                SketchInput::F32(f) => print!(" <f32::{}, {}> ", f, self.val),
                SketchInput::F64(f) => print!(" <f64::{}, {}> ", f, self.val),
                SketchInput::Str(s) => print!(" <str::{}, {}> ", s, self.val),
                SketchInput::String(s) => print!(" <String::{}, {}> ", s, self.val),
                SketchInput::Bytes(_items) => todo!(),
                SketchInput::I8(_) => todo!(),
                SketchInput::I16(_) => todo!(),
                SketchInput::I128(_) => todo!(),
                SketchInput::ISIZE(_) => todo!(),
                SketchInput::U8(_) => todo!(),
                SketchInput::U16(_) => todo!(),
                SketchInput::U128(_) => todo!(),
                SketchInput::USIZE(_) => todo!(),
            },
            None => print!(" <None, {}> ", self.val),
        }
    }

    pub fn add_v(&mut self, v: u64) {
        self.val += v;
    }
}

impl<'long_enough_sketch> Default for Coco<'long_enough_sketch> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'long_enough_sketch, 'just_for_est> Coco<'long_enough_sketch> {
    pub fn new() -> Self {
        Coco::init_with_size(64, 5)
    }

    pub fn debug(&mut self) {
        println!("w: {}", self.w);
        println!("d: {}", self.d);
        for i in 0..self.d {
            print!("[ ");
            for j in 0..self.w {
                self.table[i][j].debug();
            }
            println!(" ]");
        }
    }

    pub fn init_with_size(w: usize, d: usize) -> Self {
        Coco {
            w,
            d,
            table: vec![vec![Bucket::default(); w]; d],
        }
    }

    // oh, unfinished again!
    pub fn insert(&mut self, key: &SketchInput<'long_enough_sketch>, v: u64) {
        let mut min_val_row = usize::MAX;
        let mut min_val = u64::MAX;
        for i in 0..self.d {
            // let idx = STATELIST[i].hash_one(&key) as usize % self.w;
            // let idx = hash_it(i, &key) as usize % self.w;
            let idx = hash_it(i, key) as usize % self.w;
            match (&self.table[i][idx].full_key, key) {
                (Some(SketchInput::String(k)), SketchInput::String(key)) => {
                    if *k == *key {
                        self.table[i][idx].val += v;
                        return;
                    } else if self.table[i][idx].val < min_val {
                        min_val_row = i;
                        min_val = self.table[i][idx].val;
                    }
                }
                (Some(SketchInput::Str(k)), SketchInput::Str(key)) => {
                    if *k == *key {
                        self.table[i][idx].val += v;
                        return;
                    } else {
                        // println!("i: {}, min_val: {}, cur_val: {}", i, min_val, self.table[i][idx].val);
                        if self.table[i][idx].val < min_val {
                            min_val_row = i;
                            min_val = self.table[i][idx].val;
                        }
                    }
                }
                (Some(_), _) => {
                    // as long as the type is incorrect, it is bad
                    if self.table[i][idx].val < min_val {
                        min_val_row = i;
                        min_val = self.table[i][idx].val;
                    }
                }
                (None, _) => {
                    // seems like if nothing there, I should just update, and return
                    self.table[i][idx].val += v;
                    self.table[i][idx].update_key(key);
                    return;
                }
            }
            // println!("i: {}", i);
        }
        // all empty
        // println!("min val row: {}", min_val_row);
        if min_val_row > self.d {
            min_val_row = 0;
        }
        // let idx = STATELIST[min_val_row].hash_one(&key) as usize % self.w;
        // let idx = hash_it(min_val_row, &key) as usize % self.w;
        let idx = hash_it(min_val_row, key) as usize % self.w;
        self.table[min_val_row][idx].val += v;
        match self.table[min_val_row][idx].full_key {
            Some(_) => {
                let mut name_decider = rand::rng();
                let random_float = name_decider.random_range(0.0..=1.0_f64);
                if (v as f64 / self.table[min_val_row][idx].val as f64) < random_float {
                    // self.table[min_val_row][idx].full_key = Some(key.clone());
                    // to make lifetime happy
                    self.table[min_val_row][idx].update_key(key);
                }
            }
            // None => self.table[min_val_row][idx].full_key = Some(key.clone()),
            // to make lifetime happy
            None => self.table[min_val_row][idx].update_key(key),
        }
    }

    /// the udf parameter takes in full key first, and then partial key
    pub fn estimate_with_udf<F>(&mut self, partial_key: SketchInput<'just_for_est>, udf: F) -> u64
    where
        F: Fn(&SketchInput<'long_enough_sketch>, &SketchInput<'just_for_est>) -> bool,
    {
        let mut total = 0;
        for i in 0..self.d {
            for j in 0..self.w {
                if self.table[i][j].is_partial_key_with_udf(&partial_key, &udf) {
                    total += self.table[i][j].val;
                    // println!("partial: {:?}, full: {:?}, val: {}, total: {}", partial_key, self.table[i][j].full_key, self.table[i][j].val, total);
                }
            }
        }
        total
    }

    pub fn estimate(&mut self, partial_key: SketchInput<'_>) -> u64 {
        let mut total = 0;
        for i in 0..self.d {
            for j in 0..self.w {
                if self.table[i][j].is_partial_key(&partial_key) {
                    total += self.table[i][j].val;
                }
            }
        }
        total
    }

    pub fn merge(&mut self, other: &Coco<'long_enough_sketch>) {
        assert_eq!(self.d, other.d, "Different depth, do nothing");
        assert_eq!(self.w, other.w, "Different width, do nothing");
        for i in 0..self.d {
            for j in 0..self.w {
                if let Some(k) = &other.table[i][j].full_key {
                    self.insert(k, other.table[i][j].val);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_then_estimate_matches_full_value_for_partial_key() {
        // cover end-to-end flow of inserting a key and querying with a substring
        let mut coco = Coco::init_with_size(32, 4);
        let key = SketchInput::String("user:1234".to_string());

        coco.insert(&key, 3);
        coco.insert(&key, 2);

        let estimate = coco.estimate(SketchInput::Str("user"));
        assert_eq!(estimate, 5);
    }

    #[test]
    fn estimate_with_udf_allows_custom_partial_matching() {
        // ensure custom UDF matching logic aggregates only intended buckets
        let mut coco = Coco::init_with_size(32, 4);
        coco.insert(&SketchInput::String("region=us|id=1".into()), 4);
        coco.insert(&SketchInput::String("region=eu|id=2".into()), 6);

        fn matcher(full: &SketchInput, partial: &SketchInput) -> bool {
            match (full, partial) {
                (SketchInput::String(f), SketchInput::Str(p)) => f.contains(p),
                (SketchInput::Str(f), SketchInput::Str(p)) => f.contains(p),
                (SketchInput::String(f), SketchInput::String(p)) => f.contains(p),
                (SketchInput::Str(f), SketchInput::String(p)) => f.contains(p),
                _ => false,
            }
        }

        let total_us = coco.estimate_with_udf(SketchInput::Str("us"), matcher);
        assert_eq!(total_us, 4);

        let total_all = coco.estimate_with_udf(SketchInput::Str("region"), matcher);
        assert_eq!(total_all, 10);
    }

    #[test]
    fn merge_combines_tables_without_losing_counts() {
        // verify merging replays entries so both sketches contribute to totals
        let mut left = Coco::init_with_size(32, 4);
        let mut right = Coco::init_with_size(32, 4);

        left.insert(&SketchInput::String("alpha:key".into()), 7);
        right.insert(&SketchInput::String("beta:key".into()), 11);

        left.merge(&right);

        assert_eq!(left.estimate(SketchInput::Str("alpha")), 7);
        assert_eq!(left.estimate(SketchInput::Str("beta")), 11);
    }
}
