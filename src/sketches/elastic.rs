use crate::{CANONICAL_HASH_SEED, DefaultXxHasher, SketchHasher, SketchInput};

use super::{CountMin, RegularPath};
use crate::Vector2D;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

#[derive(Serialize, Deserialize, Clone)]
pub struct HeavyCounter {
    pub key: String, // flow id?
    pub vote_pos: i32,
    pub vote_neg: i32,
    pub flag: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HeavyBucket {
    pub flow_id: String,
    pub vote_pos: i32,
    pub vote_neg: i32,
    pub eviction: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(bound = "")]
pub struct Elastic<H: SketchHasher = DefaultXxHasher> {
    pub heavy: Vec<HeavyBucket>,
    pub light: CountMin<Vector2D<i32>, RegularPath, H>,
    pub bktlen: i32,
    #[serde(skip)]
    _hasher: PhantomData<H>,
}

impl Default for HeavyBucket {
    fn default() -> Self {
        Self::new()
    }
}

impl HeavyBucket {
    pub fn new() -> Self {
        HeavyBucket {
            flow_id: "".to_string(),
            vote_pos: 0,
            vote_neg: 0,
            eviction: false,
        }
    }

    pub fn evict(&mut self, id: String) {
        self.flow_id = id;
        self.vote_pos = 1;
        self.vote_neg = 1;
        self.eviction = true;
    }
}

impl Default for Elastic {
    fn default() -> Self {
        Self::new()
    }
}

impl<H: SketchHasher> Elastic<H> {
    pub fn new() -> Self {
        Elastic::init_with_length(8)
    }

    pub fn init_with_length(l: i32) -> Self {
        let mut heavy = Vec::with_capacity(l as usize);
        for _ in 0..l {
            heavy.push(HeavyBucket::new());
        }
        let light = CountMin::<Vector2D<i32>, RegularPath, H>::with_dimensions(3, 4096);
        Elastic {
            heavy,
            light,
            bktlen: l,
            _hasher: PhantomData,
        }
    }

    pub fn insert(&mut self, id: String) {
        let hash = H::hash64_seeded(CANONICAL_HASH_SEED, &SketchInput::String(id.clone()));
        let idx = hash as usize % self.bktlen as usize;
        let heavy_bkt = &mut self.heavy[idx];
        if heavy_bkt.flow_id.is_empty() && heavy_bkt.vote_neg == 0 && heavy_bkt.vote_pos == 0 {
            // empty
            heavy_bkt.flow_id = id;
            heavy_bkt.vote_pos += 1;
        } else if id == heavy_bkt.flow_id {
            // matched
            heavy_bkt.vote_pos += 1;
        } else if id != heavy_bkt.flow_id {
            heavy_bkt.vote_neg += 1;
            if heavy_bkt.vote_neg / heavy_bkt.vote_pos < 8 {
                // self.light.insert_cm(&id);
                self.light.insert(&SketchInput::String(id));
            } else {
                let vote = heavy_bkt.vote_pos;
                heavy_bkt.evict(id);
                for _ in 0..vote {
                    // self.light. insert_cm(&to_evict);
                    self.light
                        .insert(&SketchInput::String(heavy_bkt.flow_id.clone()));
                }
            }
        }
    }

    pub fn query(&mut self, id: String) -> i32 {
        let hash = H::hash64_seeded(CANONICAL_HASH_SEED, &SketchInput::String(id.clone()));
        let idx = hash as usize % self.bktlen as usize;
        let heavy_bkt = &self.heavy[idx];
        if id == heavy_bkt.flow_id {
            if heavy_bkt.eviction {
                // let light_result = self.light.get_est(&id) as i32;
                let light_result = self.light.estimate(&SketchInput::String(id)) as i32;
                let heavy_result = heavy_bkt.vote_pos;
                light_result + heavy_result
            } else {
                heavy_bkt.vote_pos
            }
        } else {
            // return self.light.get_est(&id) as i32;
            self.light.estimate(&SketchInput::String(id)) as i32
        }
    }

    pub fn merge(&mut self, other: &Elastic<H>) {
        assert_eq!(
            self.bktlen, other.bktlen,
            "bucket length mismatch while merging Elastic sketches"
        );

        self.flush_heavy_to_light();

        let mut other_clone = other.clone();
        other_clone.flush_heavy_to_light();

        self.light.merge(&other_clone.light);
        self.reset_heavy();
    }

    fn spill_heavy_to_light(&mut self, bucket: &HeavyBucket) {
        if bucket.flow_id.is_empty() || bucket.vote_pos <= 0 {
            return;
        }
        let flow_id = bucket.flow_id.clone();
        for _ in 0..bucket.vote_pos {
            self.light.insert(&SketchInput::String(flow_id.clone()));
        }
    }

    fn flush_heavy_to_light(&mut self) {
        let buckets = self.heavy.clone();
        for bucket in &buckets {
            self.spill_heavy_to_light(bucket);
        }
    }

    fn reset_heavy(&mut self) {
        for bucket in &mut self.heavy {
            *bucket = HeavyBucket::new();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CANONICAL_HASH_SEED, SketchInput, hash64_seeded};

    fn bucket_for(id: &str, sketch: &Elastic) -> usize {
        let hash = hash64_seeded(CANONICAL_HASH_SEED, &SketchInput::String(id.to_string()));
        hash as usize % sketch.bktlen as usize
    }

    #[test]
    fn heavy_bucket_tracks_repeated_flow_exactly() {
        // repeated inserts of the same flow should accumulate in the heavy bucket
        let mut sketch: Elastic = Elastic::init_with_length(8);
        let flow = "flow::primary".to_string();

        for _ in 0..12 {
            sketch.insert(flow.clone());
        }

        assert_eq!(sketch.query(flow.clone()), 12);
        assert_eq!(sketch.query("other".to_string()), 0);
    }

    #[test]
    fn light_sketch_counts_colliding_flows() {
        // simulate two flows mapped to the same bucket so the light CountMin tracks the second one
        let mut sketch: Elastic = Elastic::init_with_length(8);
        let primary = "flow::primary";
        let primary_bucket = bucket_for(primary, &sketch);

        let mut secondary = None;
        for idx in 0..10_000 {
            let candidate = format!("flow::secondary::{idx}");
            if bucket_for(&candidate, &sketch) == primary_bucket && candidate != primary {
                secondary = Some(candidate);
                break;
            }
        }
        let secondary = secondary.expect("unable to find colliding key for test");

        for _ in 0..10 {
            sketch.insert(primary.to_string());
        }
        for _ in 0..6 {
            sketch.insert(secondary.clone());
        }

        let heavy_est = sketch.query(primary.to_string());
        let light_est = sketch.query(secondary.clone());

        assert!(
            heavy_est >= 10,
            "expected heavy bucket >= 10 after repeated inserts, got {heavy_est}"
        );
        assert!(
            light_est >= 6,
            "colliding flow should accumulate in CountMin, expected >= 6, got {light_est}"
        );
    }

    #[test]
    fn merge_flushes_heavy_and_sum_merges_light() {
        let mut left: Elastic = Elastic::init_with_length(16);
        let mut right: Elastic = Elastic::init_with_length(16);

        for _ in 0..30 {
            left.insert("flow::left".to_string());
        }
        for _ in 0..18 {
            right.insert("flow::right".to_string());
        }

        left.merge(&right);

        assert!(left.query("flow::left".to_string()) >= 30);
        assert!(left.query("flow::right".to_string()) >= 18);
        assert!(left.heavy.iter().all(|bucket| {
            bucket.flow_id.is_empty()
                && bucket.vote_pos == 0
                && bucket.vote_neg == 0
                && !bucket.eviction
        }));
    }

    #[test]
    fn merge_preserves_colliding_flow_mass() {
        let mut left: Elastic = Elastic::init_with_length(8);
        let primary = "flow::primary";
        let primary_bucket = bucket_for(primary, &left);

        let mut secondary = None;
        for idx in 0..10_000 {
            let candidate = format!("flow::secondary::{idx}");
            if bucket_for(&candidate, &left) == primary_bucket && candidate != primary {
                secondary = Some(candidate);
                break;
            }
        }
        let secondary = secondary.expect("unable to find colliding key for merge test");

        for _ in 0..20 {
            left.insert(primary.to_string());
        }

        let mut right: Elastic = Elastic::init_with_length(8);
        for _ in 0..9 {
            right.insert(secondary.clone());
        }

        left.merge(&right);

        assert!(left.query(primary.to_string()) >= 20);
        assert!(left.query(secondary.clone()) >= 9);
    }
}
