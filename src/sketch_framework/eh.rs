use super::Chapter;
use crate::SketchInput;

#[derive(Clone, Debug)]
pub struct EHVolume<'bucket> {
    pub volume: Chapter<'bucket>,
    pub size: usize,
    pub min_time: u64,
    pub max_time: u64,
}

#[derive(Clone, Debug)]
pub struct ExponentialHistogram<'bucket> {
    pub payload: Vec<EHVolume<'bucket>>,
    pub window: u64,
    pub k: usize,
    pub type_to_clone: Chapter<'bucket>,
}

impl<'bucket> EHVolume<'bucket> {
    pub fn to_merge(&mut self, other: EHVolume<'bucket>) {
        let _ = self.volume.merge(&other.volume);
        self.size += other.size;
        self.max_time = self.max_time.max(other.max_time);
        self.min_time = self.min_time.min(other.min_time);
    }
}

impl<'bucket> ExponentialHistogram<'bucket> {
    pub fn new(k: usize, window: u64, eh_type: Chapter<'bucket>) -> Self {
        ExponentialHistogram {
            payload: Vec::new(),
            window,
            k,
            type_to_clone: eh_type,
        }
    }

    pub fn update_window(&mut self, window: u64) {
        self.window = window;
    }

    pub fn update(&mut self, time: u64, val: &SketchInput<'bucket>) {
        let expired_count = self
            .payload
            .iter()
            .take_while(|b| b.max_time < time.saturating_sub(self.window))
            .count();

        if expired_count > 0 {
            self.payload.drain(0..expired_count);
        }

        let new_eh_vol = EHVolume {
            volume: {
                let mut sk = self.type_to_clone.clone();
                sk.insert(val);
                sk
            },
            size: 1,
            min_time: time,
            max_time: time,
        };
        self.payload.push(new_eh_vol);

        self.merge_volumes();
    }

    fn merge_volumes(&mut self) {
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
                if i + 1 < self.payload.len() && i > 0 {
                    if self.payload[i + 1].size == self.payload[i].size {
                        same_size_vol += 1;
                    }
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

    fn merge_at_index(&mut self, index: usize) {
        if index + 1 >= self.payload.len() {
            return;
        }
        // there should be a better way to avoid clone
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

    pub fn volume_count(&self) -> usize {
        self.payload.len()
    }

    pub fn query_interval_merge(&self, t1: u64, t2: u64) -> Option<Chapter<'bucket>> {
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
            let mut merged = self.payload[from_volume].volume.clone();
            for i in (from_volume + 1)..=to_volume {
                let _ = merged.merge(&self.payload[i].volume);
            }
            Some(merged)
        } else {
            Some(self.payload[from_volume].volume.clone())
        }
    }

    /// Debug print bucket information
    pub fn print_buckets(&self) {
        println!("Bucket count: {}", self.payload.len());
        println!("k: {}", self.k);
        for (i, bucket) in self.payload.iter().enumerate() {
            println!(
                "{}: min_time={}, max_time={}, size={}",
                i, bucket.min_time, bucket.max_time, bucket.size
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
    fn test_new_exponential_histogram() {
        let eh: ExponentialHistogram =
            ExponentialHistogram::new(2, 1000, Chapter::HLL(crate::HllDf::default()));
        assert_eq!(eh.k, 2);
        assert_eq!(eh.window, 1000);
        assert_eq!(eh.volume_count(), 0);
    }

    #[test]
    fn test_basic_insertion() {
        let mut eh: ExponentialHistogram =
            ExponentialHistogram::new(2, 1000, Chapter::HLL(crate::HllDf::default()));

        eh.update(100, &SketchInput::I64(1));

        assert_eq!(eh.volume_count(), 1);
        assert_eq!(eh.get_min_time(), Some(100));
        assert_eq!(eh.get_max_time(), Some(100));
    }

    #[test]
    fn test_window_expiration() {
        let mut eh: ExponentialHistogram =
            ExponentialHistogram::new(2, 100, Chapter::HLL(crate::HllDf::default()));

        for i in 0..5 {
            eh.update(i * 30, &SketchInput::U64(i));
        }

        let count_before = eh.volume_count();
        assert!(count_before <= 5);

        // Insert a new value far in the future (time = 200)
        // This should expire values with max_time < 200 - 100 = 100

        eh.update(200, &SketchInput::U64(200));

        // Values at time 0, 30, 60 should be expired (max_time < 100)
        // Values at time 90, 120, 200 should remain
        assert!(eh.volume_count() < 6);
        assert!(eh.get_min_time().unwrap() >= 90);
    }

    #[test]
    fn test_volume_merging() {
        let mut eh: ExponentialHistogram =
            ExponentialHistogram::new(2, 100, Chapter::HLL(crate::HllDf::default()));

        for i in 0..10 {
            eh.update(i * 10, &SketchInput::U64(i));
        }
        assert!(eh.volume_count() < 10);
        let (count, sizes) = eh.get_memory_info();
        assert!(count > 0);
        let has_merged = sizes.iter().any(|&s| s > 1);
        assert!(has_merged, "Expected some volumes to be merged");
    }

    #[test]
    fn test_cover_functionality() {
        let mut eh: ExponentialHistogram =
            ExponentialHistogram::new(2, 1000, Chapter::HLL(crate::HllDf::default()));

        // Insert value at times 100, 200, 300
        for i in 1..=3 {
            eh.update(i * 100, &SketchInput::U64(i));
        }

        // Should cover range [100, 300]
        assert!(eh.cover(100, 300));
        assert!(eh.cover(150, 250));

        // Should not cover ranges outside the bounds
        assert!(!eh.cover(50, 250));
        assert!(!eh.cover(150, 400));
    }

    #[test]
    fn test_update_window() {
        let mut eh: ExponentialHistogram =
            ExponentialHistogram::new(2, 1000, Chapter::HLL(crate::HllDf::default()));
        assert_eq!(eh.window, 1000);

        eh.update_window(2000);
        assert_eq!(eh.window, 2000);
    }

    #[test]
    fn test_query_interval_merge() {
        let mut eh: ExponentialHistogram =
            ExponentialHistogram::new(2, 10000, Chapter::HLL(crate::HllDf::default()));

        // Insert with some data
        for i in 0..5 {
            eh.update(i * 100, &SketchInput::U64(i));
        }

        // Query an interval
        let result = eh.query_interval_merge(100, 300);
        assert!(result.is_some());

        let result2 = eh.query_interval_merge(0, 50);
        assert!(result2.is_some());
    }

    #[test]
    fn test_empty_histogram() {
        let eh: ExponentialHistogram =
            ExponentialHistogram::new(2, 1000, Chapter::HLL(crate::HllDf::default()));

        assert_eq!(eh.volume_count(), 0);
        assert_eq!(eh.get_min_time(), None);
        assert_eq!(eh.get_max_time(), None);
        assert!(!eh.cover(0, 100));
        assert!(eh.query_interval_merge(0, 100).is_none());
    }
}
