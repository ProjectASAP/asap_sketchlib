//! EHUnivOptimized: Hybrid two-tier Exponential Histogram for UnivMon.
//!
//! Recent/small EH buckets store exact frequency maps, while older/larger
//! buckets use full UnivMon sketches. When a map bucket grows too large,
//! it is promoted to a UnivMon sketch.

use std::collections::HashMap;

use crate::common::input::{heap_item_to_sketch_input, input_to_owned};
use crate::{HeapItem, SketchInput, UnivMon};

const MASS_EPSILON: f64 = 1e-9;
const DEFAULT_HEAP_SIZE: usize = 32;
const DEFAULT_SKETCH_ROW: usize = 5;
const DEFAULT_SKETCH_COL: usize = 2048;
const DEFAULT_LAYER_SIZE: usize = 8;

fn calc_map_l22(freq_map: &HashMap<HeapItem, i64>) -> f64 {
    freq_map.values().map(|&v| (v as f64) * (v as f64)).sum()
}

/// Map-tier bucket: exact frequency counts.
#[derive(Clone, Debug)]
pub struct EHMapBucket {
    pub freq_map: HashMap<HeapItem, i64>,
    pub l22: f64,
    pub bucket_size: usize,
    pub min_time: u64,
    pub max_time: u64,
}

/// Sketch-tier bucket: UnivMon sketch.
#[derive(Clone, Debug)]
pub struct EHSketchBucket {
    pub sketch: UnivMon,
    pub l22: f64,
    pub bucket_size: usize,
    pub min_time: u64,
    pub max_time: u64,
}

/// The hybrid EH structure.
pub struct EHUnivOptimized {
    pub sketch_buckets: Vec<EHSketchBucket>,
    pub map_buckets: Vec<EHMapBucket>,
    pub k: usize,
    pub window: u64,
    pub max_map_size: usize,
    heap_size: usize,
    sketch_row: usize,
    sketch_col: usize,
    layer_size: usize,
}

/// Query result: either an exact map or a UnivMon sketch.
pub enum EHUnivQueryResult {
    Sketch(UnivMon),
    Map {
        freq_map: HashMap<HeapItem, i64>,
        total_count: usize,
    },
}

impl EHUnivQueryResult {
    pub fn calc_l1(&self) -> f64 {
        match self {
            Self::Sketch(um) => um.calc_l1(),
            Self::Map { freq_map, .. } => {
                freq_map.values().map(|&v| (v as f64).abs()).sum()
            }
        }
    }

    pub fn calc_l2(&self) -> f64 {
        match self {
            Self::Sketch(um) => um.calc_l2(),
            Self::Map { freq_map, .. } => {
                freq_map
                    .values()
                    .map(|&v| {
                        let f = v as f64;
                        f * f
                    })
                    .sum::<f64>()
                    .sqrt()
            }
        }
    }

    pub fn calc_entropy(&self) -> f64 {
        match self {
            Self::Sketch(um) => um.calc_entropy(),
            Self::Map {
                freq_map,
                total_count,
            } => {
                let n = *total_count as f64;
                if n <= 0.0 {
                    return 0.0;
                }
                let sum_f_log_f: f64 = freq_map
                    .values()
                    .map(|&v| {
                        let f = v as f64;
                        if f > 0.0 {
                            f * f.log2()
                        } else {
                            0.0
                        }
                    })
                    .sum();
                n.log2() - sum_f_log_f / n
            }
        }
    }

    pub fn calc_card(&self) -> f64 {
        match self {
            Self::Sketch(um) => um.calc_card(),
            Self::Map { freq_map, .. } => freq_map.len() as f64,
        }
    }
}

impl EHUnivOptimized {
    pub fn new(
        k: usize,
        window: u64,
        heap_size: usize,
        sketch_row: usize,
        sketch_col: usize,
        layer_size: usize,
    ) -> Self {
        let k_eff = k.max(1);
        EHUnivOptimized {
            sketch_buckets: Vec::new(),
            map_buckets: Vec::new(),
            k: k_eff,
            window,
            max_map_size: layer_size * sketch_row * sketch_col,
            heap_size,
            sketch_row,
            sketch_col,
            layer_size,
        }
    }

    pub fn with_defaults(k: usize, window: u64) -> Self {
        Self::new(
            k,
            window,
            DEFAULT_HEAP_SIZE,
            DEFAULT_SKETCH_ROW,
            DEFAULT_SKETCH_COL,
            DEFAULT_LAYER_SIZE,
        )
    }

    pub fn update(&mut self, time: u64, key: &SketchInput, value: i64) {
        // 1. Expire old sketch buckets
        let cutoff = time.saturating_sub(self.window);
        let expired = self
            .sketch_buckets
            .iter()
            .take_while(|b| b.max_time < cutoff)
            .count();
        if expired > 0 {
            self.sketch_buckets.drain(0..expired);
        }

        // 2. Expire old map buckets
        let expired = self
            .map_buckets
            .iter()
            .take_while(|b| b.max_time < cutoff)
            .count();
        if expired > 0 {
            self.map_buckets.drain(0..expired);
        }

        // 3. Create new map bucket
        let owned_key = input_to_owned(key);
        let mut freq_map = HashMap::new();
        freq_map.insert(owned_key, value);
        self.map_buckets.push(EHMapBucket {
            freq_map,
            l22: (value as f64) * (value as f64),
            bucket_size: value as usize,
            min_time: time,
            max_time: time,
        });

        // 4. L2-merge map buckets (backward scan)
        let mut sum_l22: f64 = 0.0;
        if self.map_buckets.len() >= 2 {
            let mut i = self.map_buckets.len() - 2;
            loop {
                let pair_l22 = self.map_buckets[i].l22 + self.map_buckets[i + 1].l22;
                let threshold = sum_l22 / (self.k as f64);
                if pair_l22 <= threshold + MASS_EPSILON {
                    // Merge i+1 into i
                    let other = self.map_buckets.remove(i + 1);
                    let bucket = &mut self.map_buckets[i];
                    bucket.bucket_size += other.bucket_size;
                    bucket.max_time = bucket.max_time.max(other.max_time);
                    bucket.min_time = bucket.min_time.min(other.min_time);
                    for (k, v) in other.freq_map {
                        *bucket.freq_map.entry(k).or_insert(0) += v;
                    }
                    bucket.l22 = calc_map_l22(&bucket.freq_map);
                } else {
                    sum_l22 += self.map_buckets[i + 1].l22;
                }
                if i == 0 {
                    break;
                }
                i -= 1;
            }
        }

        // 5. Promotion: if oldest map bucket is too large, promote to sketch
        if !self.map_buckets.is_empty()
            && 2 * self.map_buckets[0].freq_map.len() >= self.max_map_size
        {
            self.promote_oldest_map(sum_l22);
        }
    }

    fn promote_oldest_map(&mut self, sum_l22: f64) {
        let oldest = self.map_buckets.remove(0);

        let mut sketch = UnivMon::init_univmon(
            self.heap_size,
            self.sketch_row,
            self.sketch_col,
            self.layer_size,
        );
        for (key, value) in &oldest.freq_map {
            let input = heap_item_to_sketch_input(key);
            sketch.insert(&input, *value);
        }

        let l22 = sketch.l2_sketch_layers[0].get_l2().powi(2);
        self.sketch_buckets.push(EHSketchBucket {
            sketch,
            l22,
            bucket_size: oldest.bucket_size,
            min_time: oldest.min_time,
            max_time: oldest.max_time,
        });

        self.merge_sketch_buckets(sum_l22);
    }

    fn merge_sketch_buckets(&mut self, mut sum_l22: f64) {
        if self.sketch_buckets.len() < 2 {
            return;
        }
        let mut i = self.sketch_buckets.len() - 2;
        loop {
            let l22_i = self.sketch_buckets[i]
                .sketch
                .l2_sketch_layers[0]
                .get_l2()
                .powi(2);
            let l22_next = self.sketch_buckets[i + 1]
                .sketch
                .l2_sketch_layers[0]
                .get_l2()
                .powi(2);
            let pair_l22 = l22_i + l22_next;
            let threshold = sum_l22 / (self.k as f64);
            if pair_l22 <= threshold + MASS_EPSILON {
                let other = self.sketch_buckets.remove(i + 1);
                let bucket = &mut self.sketch_buckets[i];
                bucket.sketch.merge(&other.sketch);
                bucket.sketch.bucket_size += other.sketch.bucket_size;
                bucket.bucket_size += other.bucket_size;
                bucket.max_time = bucket.max_time.max(other.max_time);
                bucket.min_time = bucket.min_time.min(other.min_time);
                bucket.l22 = bucket.sketch.l2_sketch_layers[0].get_l2().powi(2);
            } else {
                sum_l22 += l22_next;
            }
            if i == 0 {
                break;
            }
            i -= 1;
        }
    }

    pub fn query_interval(&self, t1: u64, t2: u64) -> Option<EHUnivQueryResult> {
        let s_count = self.sketch_buckets.len();
        let m_count = self.map_buckets.len();
        let total = s_count + m_count;
        if total == 0 {
            return None;
        }

        let mut from_bucket: usize = 0;
        let mut to_bucket: usize = 0;

        // Search sketch buckets
        for i in 0..s_count {
            if t1 >= self.sketch_buckets[i].min_time && t1 <= self.sketch_buckets[i].max_time {
                from_bucket = i;
                break;
            }
        }
        for i in 0..s_count {
            if t2 >= self.sketch_buckets[i].min_time && t2 <= self.sketch_buckets[i].max_time {
                to_bucket = i;
                break;
            }
        }

        // Search map buckets (may override sketch results)
        for i in 0..m_count {
            if t1 >= self.map_buckets[i].min_time && t1 <= self.map_buckets[i].max_time {
                from_bucket = i + s_count;
                break;
            }
        }
        for i in 0..m_count {
            if t2 >= self.map_buckets[i].min_time && t2 <= self.map_buckets[i].max_time {
                to_bucket = i + s_count;
                break;
            }
        }

        // Edge cases
        if m_count > 0 && t2 > self.map_buckets[m_count - 1].max_time {
            to_bucket = m_count - 1 + s_count;
        }
        if s_count > 0 && t1 < self.sketch_buckets[0].min_time {
            from_bucket = 0;
        } else if s_count == 0 && m_count > 0 && t1 < self.map_buckets[0].min_time {
            from_bucket = 0;
        }

        // Snap from_bucket forward if t1 is closer to max_time of the bucket
        if from_bucket < s_count {
            let b = &self.sketch_buckets[from_bucket];
            if t1.abs_diff(b.min_time) > t1.abs_diff(b.max_time) && from_bucket + 1 < total {
                from_bucket += 1;
            }
        } else if from_bucket >= s_count && from_bucket - s_count < m_count {
            let mi = from_bucket - s_count;
            let b = &self.map_buckets[mi];
            if t1.abs_diff(b.min_time) > t1.abs_diff(b.max_time) && from_bucket + 1 < total {
                from_bucket += 1;
            }
        }

        // Clamp indices
        if from_bucket >= total {
            from_bucket = total - 1;
        }
        if to_bucket >= total {
            to_bucket = total - 1;
        }
        if from_bucket > to_bucket {
            to_bucket = from_bucket;
        }

        // Three cases
        if to_bucket < s_count {
            // Case 1: Both in sketch tier
            let mut merged = self.sketch_buckets[from_bucket].sketch.clone();
            for i in (from_bucket + 1)..=to_bucket {
                merged.merge(&self.sketch_buckets[i].sketch);
                merged.bucket_size += self.sketch_buckets[i].sketch.bucket_size;
            }
            Some(EHUnivQueryResult::Sketch(merged))
        } else if from_bucket >= s_count {
            // Case 2: Both in map tier
            let from_map = from_bucket - s_count;
            let to_map = to_bucket - s_count;
            let mut merged_map: HashMap<HeapItem, i64> = HashMap::new();
            for i in from_map..=to_map {
                for (k, &v) in &self.map_buckets[i].freq_map {
                    *merged_map.entry(k.clone()).or_insert(0) += v;
                }
            }
            let total_count = merged_map.values().sum::<i64>() as usize;
            Some(EHUnivQueryResult::Map {
                freq_map: merged_map,
                total_count,
            })
        } else {
            // Case 3: Hybrid — from in sketch, to in map
            let mut merged = UnivMon::init_univmon(
                self.heap_size,
                self.sketch_row,
                self.sketch_col,
                self.layer_size,
            );
            for i in from_bucket..s_count {
                merged.merge(&self.sketch_buckets[i].sketch);
                merged.bucket_size += self.sketch_buckets[i].sketch.bucket_size;
            }

            // Merge qualifying map buckets into a temporary map
            let to_map = to_bucket - s_count;
            let mut map_merged: HashMap<HeapItem, i64> = HashMap::new();
            for i in 0..=to_map {
                for (k, &v) in &self.map_buckets[i].freq_map {
                    *map_merged.entry(k.clone()).or_insert(0) += v;
                }
            }

            // Insert map entries into merged sketch
            for (key, value) in &map_merged {
                let input = heap_item_to_sketch_input(key);
                merged.insert(&input, *value);
            }

            Some(EHUnivQueryResult::Sketch(merged))
        }
    }

    pub fn cover(&self, mint: u64, maxt: u64) -> bool {
        match (self.get_min_time(), self.get_max_time()) {
            (Some(gmin), Some(gmax)) => gmin <= mint && gmax >= maxt,
            _ => false,
        }
    }

    pub fn get_min_time(&self) -> Option<u64> {
        let sketch_min = self.sketch_buckets.first().map(|b| b.min_time);
        let map_min = self.map_buckets.first().map(|b| b.min_time);
        match (sketch_min, map_min) {
            (Some(s), Some(m)) => Some(s.min(m)),
            (s @ Some(_), None) => s,
            (None, m @ Some(_)) => m,
            (None, None) => None,
        }
    }

    pub fn get_max_time(&self) -> Option<u64> {
        let sketch_max = self.sketch_buckets.last().map(|b| b.max_time);
        let map_max = self.map_buckets.last().map(|b| b.max_time);
        match (sketch_max, map_max) {
            (Some(s), Some(m)) => Some(s.max(m)),
            (s @ Some(_), None) => s,
            (None, m @ Some(_)) => m,
            (None, None) => None,
        }
    }

    pub fn update_window(&mut self, window: u64) {
        self.window = window;
    }

    pub fn volume_count(&self) -> usize {
        self.sketch_buckets.len() + self.map_buckets.len()
    }

    pub fn print_buckets(&self) {
        println!("=== EHUnivOptimized Buckets ===");
        println!(
            "k: {}, window: {}, max_map_size: {}",
            self.k, self.window, self.max_map_size
        );
        println!("Sketch buckets ({}):", self.sketch_buckets.len());
        for (i, b) in self.sketch_buckets.iter().enumerate() {
            println!(
                "  [S{}] min_time={}, max_time={}, bucket_size={}, l22={:.2}",
                i, b.min_time, b.max_time, b.bucket_size, b.l22
            );
        }
        println!("Map buckets ({}):", self.map_buckets.len());
        for (i, b) in self.map_buckets.iter().enumerate() {
            println!(
                "  [M{}] min_time={}, max_time={}, bucket_size={}, l22={:.2}, keys={}",
                i,
                b.min_time,
                b.max_time,
                b.bucket_size,
                b.l22,
                b.freq_map.len()
            );
        }
    }

    pub fn get_memory_info(&self) -> (usize, usize, Vec<usize>, Vec<usize>) {
        let sketch_sizes: Vec<usize> = self.sketch_buckets.iter().map(|b| b.bucket_size).collect();
        let map_sizes: Vec<usize> = self.map_buckets.iter().map(|b| b.bucket_size).collect();
        (
            self.sketch_buckets.len(),
            self.map_buckets.len(),
            sketch_sizes,
            map_sizes,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_insertion_and_query() {
        let mut eh = EHUnivOptimized::with_defaults(4, 1000);

        eh.update(100, &SketchInput::I64(1), 5);
        eh.update(101, &SketchInput::I64(2), 3);
        eh.update(102, &SketchInput::I64(1), 2);

        assert!(eh.sketch_buckets.is_empty());
        assert!(!eh.map_buckets.is_empty());

        let result = eh.query_interval(100, 102).unwrap();
        match &result {
            EHUnivQueryResult::Map {
                freq_map,
                total_count,
            } => {
                // key=1 has total count 5+2=7, key=2 has 3
                let k1 = freq_map.get(&HeapItem::I64(1)).copied().unwrap_or(0);
                let k2 = freq_map.get(&HeapItem::I64(2)).copied().unwrap_or(0);
                assert_eq!(k1, 7);
                assert_eq!(k2, 3);
                assert_eq!(*total_count, 10);
            }
            EHUnivQueryResult::Sketch(_) => panic!("Expected Map result"),
        }

        assert!((result.calc_l1() - 10.0).abs() < 1e-9);
        assert_eq!(result.calc_card(), 2.0);
    }

    #[test]
    fn map_merge_bounds_volume() {
        let mut eh = EHUnivOptimized::with_defaults(1, 10000);

        for i in 0..50u64 {
            eh.update(i, &SketchInput::I64(i as i64), 1);
        }

        // With k=1 and L2 merging, volume count should stay bounded
        assert!(
            eh.volume_count() < 50,
            "volume_count {} should be bounded below 50",
            eh.volume_count()
        );
    }

    #[test]
    fn promotion_creates_sketch_buckets() {
        // Use small parameters so promotion triggers quickly
        // max_map_size = layer_size * sketch_row * sketch_col = 2 * 2 * 5 = 20
        // promotion at 2 * map.len() >= 20, i.e. map.len() >= 10
        let mut eh = EHUnivOptimized::new(8, 100000, 16, 2, 5, 2);

        assert!(eh.sketch_buckets.is_empty());

        // Insert many distinct keys to grow the oldest map bucket
        for i in 0..200u64 {
            eh.update(i, &SketchInput::I64(i as i64), 1);
        }

        assert!(
            !eh.sketch_buckets.is_empty(),
            "Should have promoted at least one map bucket to sketch"
        );
    }

    #[test]
    fn window_expiration() {
        let mut eh = EHUnivOptimized::with_defaults(4, 100);

        eh.update(10, &SketchInput::I64(1), 1);
        eh.update(20, &SketchInput::I64(2), 1);
        eh.update(30, &SketchInput::I64(3), 1);

        assert_eq!(eh.get_min_time(), Some(10));

        // This update at time=200 should expire buckets with max_time < 200-100=100
        eh.update(200, &SketchInput::I64(4), 1);

        // All buckets with max_time < 100 should be gone
        assert!(
            eh.get_min_time().unwrap() >= 100 || eh.get_min_time() == Some(200),
            "Old buckets should be expired, got min_time={:?}",
            eh.get_min_time()
        );
    }

    #[test]
    fn hybrid_query_returns_sketch() {
        // Use small parameters for fast promotion
        // max_map_size = 2 * 2 * 5 = 20, promotion at map.len() >= 10
        let mut eh = EHUnivOptimized::new(8, 100000, 16, 2, 5, 2);

        // Insert enough distinct keys to force promotion
        for i in 0..200u64 {
            eh.update(i, &SketchInput::I64(i as i64), 1);
        }

        assert!(!eh.sketch_buckets.is_empty(), "Need sketch buckets");
        assert!(!eh.map_buckets.is_empty(), "Need map buckets");

        // Query spanning both tiers
        let result = eh.query_interval(0, 199).unwrap();
        match result {
            EHUnivQueryResult::Sketch(_) => {} // expected
            EHUnivQueryResult::Map { .. } => panic!("Expected Sketch result for hybrid query"),
        }
    }

    #[test]
    fn cover_check() {
        let mut eh = EHUnivOptimized::with_defaults(4, 1000);

        assert!(!eh.cover(0, 100));

        eh.update(50, &SketchInput::I64(1), 1);
        eh.update(100, &SketchInput::I64(2), 1);

        assert!(eh.cover(50, 100));
        assert!(eh.cover(60, 90));
        assert!(!eh.cover(40, 100));
        assert!(!eh.cover(50, 110));
    }

    #[test]
    fn accuracy_known_distribution() {
        let mut eh = EHUnivOptimized::with_defaults(4, 100000);

        // Insert a known distribution
        let data: Vec<(i64, i64)> = vec![
            (1, 100),
            (2, 200),
            (3, 50),
            (4, 150),
            (5, 80),
        ];

        let mut time = 0u64;
        for &(key, count) in &data {
            for _ in 0..count {
                eh.update(time, &SketchInput::I64(key), 1);
                time += 1;
            }
        }

        let result = eh.query_interval(0, time - 1).unwrap();

        // Ground truth
        let true_l1: f64 = data.iter().map(|&(_, c)| c as f64).sum();
        let true_l2: f64 = data
            .iter()
            .map(|&(_, c)| (c as f64) * (c as f64))
            .sum::<f64>()
            .sqrt();
        let true_card = data.len() as f64;
        let entropy_term: f64 = data
            .iter()
            .map(|&(_, c)| {
                let f = c as f64;
                f * f.log2()
            })
            .sum();
        let true_entropy = true_l1.log2() - entropy_term / true_l1;

        let est_l1 = result.calc_l1();
        let est_l2 = result.calc_l2();
        let est_card = result.calc_card();
        let est_entropy = result.calc_entropy();

        // Map results should be exact (or very close due to merge)
        let l1_err = (est_l1 - true_l1).abs() / true_l1;
        let l2_err = (est_l2 - true_l2).abs() / true_l2;
        let card_err = (est_card - true_card).abs() / true_card;
        let ent_err = (est_entropy - true_entropy).abs() / true_entropy;

        assert!(
            l1_err < 0.10,
            "L1 error {:.2}%: est={}, true={}",
            l1_err * 100.0,
            est_l1,
            true_l1
        );
        assert!(
            l2_err < 0.10,
            "L2 error {:.2}%: est={}, true={}",
            l2_err * 100.0,
            est_l2,
            true_l2
        );
        assert!(
            card_err < 0.10,
            "Card error {:.2}%: est={}, true={}",
            card_err * 100.0,
            est_card,
            true_card
        );
        assert!(
            ent_err < 0.10,
            "Entropy error {:.2}%: est={}, true={}",
            ent_err * 100.0,
            est_entropy,
            true_entropy
        );
    }
}
