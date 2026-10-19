//! Test-only copy of the KLL insert, compaction and merge paths as they were
//! before the item buffer grew lazily: the whole buffer is allocated at
//! construction. `lazy_allocation_matches_eager` compares the live sketch
//! against it, state for state.

use super::{
    CAPACITY_CACHE_LEN, CAPACITY_DECAY, Coin, MAX_CACHEABLE_K, MAX_LEVELS, compute_max_capacity,
    merge_sorted_runs, randomly_halve_up,
};
use crate::common::numerical::NumericalValue;

pub(super) struct EagerKll<T: NumericalValue = f64> {
    items: Box<[T]>,
    levels: Box<[usize]>,
    k: usize,
    m: usize,
    num_levels: usize,
    max_capacity: usize,
    pub(super) co: Coin,
    capacity_cache: [u32; CAPACITY_CACHE_LEN],
    top_height: usize,
    level0_capacity: usize,
    merge_buf: Vec<T>,
}

impl<T: NumericalValue> EagerKll<T> {
    pub(super) fn init_with_seed(k: usize, m: usize, seed: u64) -> Self {
        let mut norm_m = m.min(MAX_CACHEABLE_K);
        norm_m = norm_m.max(2);
        let mut norm_k = k.max(norm_m);
        if norm_k > MAX_CACHEABLE_K {
            norm_k = MAX_CACHEABLE_K;
        }
        let max_cap = compute_max_capacity(norm_k, norm_m);
        let mut s = Self {
            items: vec![T::default(); max_cap].into_boxed_slice(),
            levels: {
                let mut v = vec![0usize; MAX_LEVELS + 1];
                v[0] = max_cap;
                v[1] = max_cap;
                v.into_boxed_slice()
            },
            k: norm_k,
            m: norm_m,
            num_levels: 1,
            max_capacity: max_cap,
            co: Coin::from_seed(seed),
            capacity_cache: [0; CAPACITY_CACHE_LEN],
            top_height: 0,
            level0_capacity: 0,
            merge_buf: Vec::with_capacity(norm_k),
        };
        s.rebuild_capacity_cache();
        s
    }

    pub(super) fn update(&mut self, value: T) {
        if self.levels[0] == 0 {
            self.compress_while_updating();
        }
        self.levels[0] -= 1;
        self.items[self.levels[0]] = value;
        if self.levels[1] - self.levels[0] > self.level0_capacity {
            self.compress_while_updating();
        }
    }

    fn compress_while_updating(&mut self) {
        let mut h = 0;
        loop {
            let pop = self.level_size(h);
            let cap = self.capacity_for_level(h);
            if pop <= cap {
                break;
            }
            if h + 1 == self.num_levels {
                self.add_new_top_level();
            }
            self.compact(h);
            h += 1;
        }
    }

    fn compact(&mut self, h: usize) {
        let beg = self.levels[h];
        let end = self.levels[h + 1];
        let pop = end - beg;
        if h == 0 {
            self.items[beg..end].sort_unstable_by(T::total_cmp);
        }
        let offset = usize::from(self.co.toss());
        let num_survivors = randomly_halve_up(&mut self.items, beg, pop, offset);
        let surv_start = beg + pop - num_survivors;
        let pop_above = self.levels[h + 2] - end;
        if pop_above > 0 {
            merge_sorted_runs(
                &mut self.items[surv_start..end + pop_above],
                num_survivors,
                &mut self.merge_buf,
            );
        }
        let delta = surv_start - beg;
        if delta > 0 && h > 0 {
            let lo = self.levels[0];
            let hi = beg;
            if hi > lo {
                self.items.copy_within(lo..hi, lo + delta);
            }
            for lvl in self.levels[..h].iter_mut() {
                *lvl += delta;
            }
        }
        self.levels[h] = surv_start;
        self.levels[h + 1] = surv_start;
    }

    fn add_new_top_level(&mut self) {
        let sentinel = self.levels[self.num_levels];
        self.num_levels += 1;
        self.levels[self.num_levels] = sentinel;
        self.top_height = self.num_levels - 1;
        self.level0_capacity = self.capacity_for_level(0);
    }

    fn capacity_for_level(&self, level: usize) -> usize {
        if self.num_levels == 0 {
            return self.m;
        }
        let height_from_top = self.top_height.saturating_sub(level);
        let idx = height_from_top.min(CAPACITY_CACHE_LEN - 1);
        self.capacity_cache[idx] as usize
    }

    fn rebuild_capacity_cache(&mut self) {
        self.top_height = self.num_levels.saturating_sub(1);
        let mut scale = 1.0_f64;
        for idx in 0..CAPACITY_CACHE_LEN {
            let scaled = ((self.k as f64) * scale).ceil() as usize;
            let cap = scaled.max(self.m);
            self.capacity_cache[idx] = cap as u32;
            scale *= CAPACITY_DECAY;
        }
        self.level0_capacity = self.capacity_for_level(0);
    }

    fn level_size(&self, h: usize) -> usize {
        self.levels[h + 1] - self.levels[h]
    }

    pub(super) fn merge(&mut self, other: &EagerKll<T>) {
        let other_start = other.levels[0];
        let other_end = other.levels[other.num_levels];
        if other_end == other_start {
            return;
        }
        let target_num_levels = self.num_levels.max(other.num_levels);
        let mut work: Vec<Vec<T>> = vec![Vec::new(); target_num_levels + 1];
        #[allow(clippy::needless_range_loop)]
        for h in 0..self.num_levels {
            let s = self.levels[h];
            let e = self.levels[h + 1];
            work[h].extend_from_slice(&self.items[s..e]);
        }
        let mut merge_buf: Vec<T> = Vec::new();
        #[allow(clippy::needless_range_loop)]
        for h in 0..other.num_levels {
            let s = other.levels[h];
            let e = other.levels[h + 1];
            let self_len = work[h].len();
            work[h].extend_from_slice(&other.items[s..e]);
            if h > 0 {
                merge_sorted_runs(work[h].as_mut_slice(), self_len, &mut merge_buf);
            }
        }
        self.num_levels = target_num_levels;
        self.rebuild_capacity_cache();
        let mut h = 0;
        while h < self.num_levels {
            if h == 0 {
                work[0].sort_unstable_by(T::total_cmp);
            }
            while work[h].len() > self.capacity_for_level(h) {
                if h + 1 == self.num_levels {
                    self.num_levels += 1;
                    self.rebuild_capacity_cache();
                    work.resize(self.num_levels + 1, Vec::new());
                }
                let pop = work[h].len();
                let offset = usize::from(self.co.toss());
                let num_survivors = randomly_halve_up(work[h].as_mut_slice(), 0, pop, offset);
                let discard = pop - num_survivors;
                work[h].drain(0..discard);
                let mut promoted = std::mem::take(&mut work[h]);
                let left_len = promoted.len();
                promoted.append(&mut work[h + 1]);
                merge_sorted_runs(promoted.as_mut_slice(), left_len, &mut merge_buf);
                work[h + 1] = promoted;
            }
            h += 1;
        }
        let mc = self.max_capacity;
        let mut cursor = mc;
        for h in (0..self.num_levels).rev() {
            let len = work[h].len();
            cursor -= len;
            self.items[cursor..cursor + len].copy_from_slice(&work[h]);
            self.levels[h] = cursor;
        }
        self.levels[self.num_levels] = mc;
        for lvl in self.levels[self.num_levels + 1..].iter_mut() {
            *lvl = mc;
        }
    }

    /// Each level's retained items, bottom level first, in buffer order.
    pub(super) fn levels_items(&self) -> Vec<Vec<T>> {
        (0..self.num_levels)
            .map(|h| self.items[self.levels[h]..self.levels[h + 1]].to_vec())
            .collect()
    }
}
