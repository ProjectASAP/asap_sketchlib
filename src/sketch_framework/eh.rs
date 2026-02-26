
use super::EHSketchList;
use super::eh_sketch_list::SketchNorm;
use crate::SketchInput;

const MASS_EPSILON: f64 = 1e-9;

#[derive(Clone, Debug)]
pub struct EHBucket {
    pub bucket: EHSketchList,
    pub size: usize,
    pub l2_mass: f64,
    pub min_time: u64,
    pub max_time: u64,
}

#[derive(Clone, Debug)]
pub struct ExponentialHistogram {
    pub payload: Vec<EHBucket>,
    pub window: u64,
    pub k: usize,
    pub merge_norm: SketchNorm,
    pub type_to_clone: EHSketchList,
}

fn infer_merge_norm(eh_sketch: &EHSketchList) -> SketchNorm {
    if eh_sketch.supports_norm(SketchNorm::L2) && !eh_sketch.supports_norm(SketchNorm::L1) {
        SketchNorm::L2
    } else {
        SketchNorm::L1
    }
}

fn compute_l2_mass(eh_sketch: &EHSketchList) -> f64 {
    eh_sketch.eh_l2_mass().unwrap_or(0.0)
}

impl EHBucket {
    pub fn to_merge(&mut self, other: EHBucket) {
        let _ = self.bucket.merge(&other.bucket);
        self.size += other.size;
        self.max_time = self.max_time.max(other.max_time);
        self.min_time = self.min_time.min(other.min_time);
        self.l2_mass = compute_l2_mass(&self.bucket);
    }
}

impl ExponentialHistogram {
    pub fn new(k: usize, window: u64, eh_type: EHSketchList) -> Self {
        let k_eff = k.max(1);
        ExponentialHistogram {
            payload: Vec::new(),
            window,
            k: k_eff,
            merge_norm: infer_merge_norm(&eh_type),
            type_to_clone: eh_type,
        }
    }

    pub fn update_window(&mut self, window: u64) {
        self.window = window;
    }

    pub fn update(&mut self, time: u64, val: &SketchInput) {
        self.update_with(time, |sketch| {
            sketch.insert(val);
        });
    }

    pub fn update_with<F>(&mut self, time: u64, update_fn: F)
    where
        F: FnOnce(&mut EHSketchList),
    {
        let expired_count = self
            .payload
            .iter()
            .take_while(|b| b.max_time < time.saturating_sub(self.window))
            .count();

        if expired_count > 0 {
            self.payload.drain(0..expired_count);
        }

        let mut sketch = self.type_to_clone.clone();
        update_fn(&mut sketch);
        let new_eh_vol = EHBucket {
            l2_mass: compute_l2_mass(&sketch),
            bucket: sketch,
            size: 1,
            min_time: time,
            max_time: time,
        };
        self.payload.push(new_eh_vol);

        self.merge_volumes();
    }

    fn merge_volumes(&mut self) {
        match self.merge_norm {
            SketchNorm::L1 => self.merge_volumes_l1(),
            SketchNorm::L2 => self.merge_volumes_l2(),
        }
    }

    fn merge_volumes_l1(&mut self) {
        let s_count = self.payload.len();
        if s_count < 2 {
            return;
        }

        let mut same_size_vol = 1;
        let mut i = s_count - 2;

        loop {
            if self.payload[i].size == self.payload[i + 1].size {
                same_size_vol += 1;
            } else {
                if (same_size_vol as f64) >= (self.k as f64) / 2.0 + 2.0 {
                    self.merge_at_index(i + 1);
                }
                same_size_vol = 1;
                if i + 1 < self.payload.len()
                    && i > 0
                    && self.payload[i + 1].size == self.payload[i].size
                {
                    same_size_vol += 1;
                }
            }

            if i == 0 {
                break;
            }
            i -= 1;
        }
        if self.payload.len() >= 2 && (same_size_vol as f64) >= (self.k as f64) / 2.0 + 2.0 {
            self.merge_at_index(0);
        }
    }

    fn merge_volumes_l2(&mut self) {
        while let Some(index) = self.find_l2_merge_candidate() {
            self.merge_at_index(index);
        }
    }

    fn find_l2_merge_candidate(&self) -> Option<usize> {
        if self.payload.len() < 2 {
            return None;
        }

        let mut sum_l22_newer = 0.0;
        for i in (0..(self.payload.len() - 1)).rev() {
            let pair_l22 = self.payload[i].l2_mass + self.payload[i + 1].l2_mass;
            let threshold = sum_l22_newer / (self.k as f64);
            if pair_l22 <= threshold + MASS_EPSILON {
                return Some(i);
            }
            sum_l22_newer += self.payload[i + 1].l2_mass;
        }

        None
    }

    fn merge_at_index(&mut self, index: usize) {
        if index + 1 >= self.payload.len() {
            return;
        }
        let vol_to_merge = self.payload[index + 1].clone();
        self.payload[index].to_merge(vol_to_merge);
        self.payload.remove(index + 1);
    }

    pub fn cover(&self, mint: u64, maxt: u64) -> bool {
        if self.payload.is_empty() {
            return false;
        }

        let first = &self.payload[0];
        let last = &self.payload[self.payload.len() - 1];

        last.max_time >= maxt && first.min_time <= mint
    }

    pub fn get_max_time(&self) -> Option<u64> {
        self.payload.last().map(|b| b.max_time)
    }

    pub fn get_min_time(&self) -> Option<u64> {
        self.payload.first().map(|b| b.min_time)
    }

    pub fn bucket_count(&self) -> usize {
        self.payload.len()
    }

    pub fn query_interval_merge(&self, t1: u64, t2: u64) -> Option<EHSketchList> {
        if self.payload.is_empty() {
            return None;
        }

        let mut from_volume = 0;
        let mut to_volume = 0;

        for (i, vol) in self.payload.iter().enumerate() {
            if t1 >= vol.min_time && t1 <= vol.max_time {
                from_volume = i;
            }
            if t2 >= vol.min_time && t2 <= vol.max_time {
                to_volume = i;
            }
        }

        if t2 > self.payload[self.payload.len() - 1].max_time {
            to_volume = self.payload.len() - 1;
        }
        if t1 < self.payload[0].min_time {
            from_volume = 0;
        }
        let from_min_diff = t1.abs_diff(self.payload[from_volume].min_time);
        let from_max_diff = t1.abs_diff(self.payload[from_volume].max_time);
        if from_min_diff > from_max_diff && from_volume + 1 < self.payload.len() {
            from_volume += 1;
        }
        if to_volume >= self.payload.len() {
            to_volume = self.payload.len() - 1;
        }
        if from_volume < to_volume {
            let mut merged = self.payload[from_volume].bucket.clone();
            for i in (from_volume + 1)..=to_volume {
                let _ = merged.merge(&self.payload[i].bucket);
            }
            Some(merged)
        } else {
            Some(self.payload[from_volume].bucket.clone())
        }
    }

    /// Debug print bucket information
    pub fn print_buckets(&self) {
        println!("Bucket count: {}", self.payload.len());
        println!("k: {}", self.k);
        println!("merge_norm: {:?}", self.merge_norm);
        for (i, bucket) in self.payload.iter().enumerate() {
            println!(
                "{}: min_time={}, max_time={}, size={}, l2_mass={}",
                i, bucket.min_time, bucket.max_time, bucket.size, bucket.l2_mass
            );
        }
    }

    /// Get memory statistics
    pub fn get_memory_info(&self) -> (usize, Vec<usize>) {
        let count = self.payload.len();
        let sizes = self.payload.iter().map(|b| b.size).collect();
        (count, sizes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructor_infers_merge_norm() {
        let eh_l1 = ExponentialHistogram::new(
            2,
            1000,
            EHSketchList::CM(crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default()),
        );
        assert_eq!(eh_l1.merge_norm, SketchNorm::L1);

        let eh_l2 = ExponentialHistogram::new(
            2,
            1000,
            EHSketchList::COUNTL2HH(crate::CountL2HH::with_dimensions(5, 2048)),
        );
        assert_eq!(eh_l2.merge_norm, SketchNorm::L2);
    }

    #[test]
    fn l1_merge_invariant_same_size() {
        let mut eh = ExponentialHistogram::new(
            2,
            100,
            EHSketchList::CM(crate::CountMin::<crate::Vector2D<i32>, crate::FastPath>::default()),
        );

        for i in 0..10 {
            eh.update(i * 10, &SketchInput::I64(1));
        }

        assert!(eh.bucket_count() < 10);
    }

    #[test]
    fn l2_merge_invariant_sum_l22() {
        let mut eh = ExponentialHistogram::new(
            1,
            100,
            EHSketchList::COUNTL2HH(crate::CountL2HH::with_dimensions(5, 2048)),
        );

        eh.update_with(1, |chapter| {
            if let EHSketchList::COUNTL2HH(sketch) = chapter {
                sketch.fast_insert_with_count(&SketchInput::I64(1), 1);
            }
        });
        eh.update_with(2, |chapter| {
            if let EHSketchList::COUNTL2HH(sketch) = chapter {
                sketch.fast_insert_with_count(&SketchInput::I64(2), 1);
            }
        });
        eh.update_with(3, |chapter| {
            if let EHSketchList::COUNTL2HH(sketch) = chapter {
                sketch.fast_insert_with_count(&SketchInput::I64(3), 20);
            }
        });

        // First two low-mass buckets should satisfy pair <= (1/k)*sum_newer with k=1.
        assert!(eh.bucket_count() <= 2);
    }

    #[test]
    fn merge_recomputes_l2_mass() {
        let mut eh = ExponentialHistogram::new(
            1,
            100,
            EHSketchList::COUNTL2HH(crate::CountL2HH::with_dimensions(5, 2048)),
        );

        eh.update_with(1, |chapter| {
            if let EHSketchList::COUNTL2HH(sketch) = chapter {
                sketch.fast_insert_with_count(&SketchInput::I64(7), 2);
            }
        });
        eh.update_with(2, |chapter| {
            if let EHSketchList::COUNTL2HH(sketch) = chapter {
                sketch.fast_insert_with_count(&SketchInput::I64(8), 2);
            }
        });
        eh.update_with(3, |chapter| {
            if let EHSketchList::COUNTL2HH(sketch) = chapter {
                sketch.fast_insert_with_count(&SketchInput::I64(9), 16);
            }
        });

        assert!(eh.bucket_count() <= 2);
        assert!(eh.payload.iter().all(|v| v.l2_mass >= 0.0));
    }

    #[test]
    fn test_basic_insertion_and_query() {
        let mut eh = ExponentialHistogram::new(
            2,
            1000,
            EHSketchList::HLL(crate::HyperLogLog::<crate::DataFusion>::default()),
        );

        eh.update(100, &SketchInput::I64(1));

        assert_eq!(eh.bucket_count(), 1);
        assert_eq!(eh.get_min_time(), Some(100));
        assert_eq!(eh.get_max_time(), Some(100));
        assert!(eh.query_interval_merge(100, 100).is_some());
    }
}
