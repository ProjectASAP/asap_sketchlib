//! Elastic Sketch.
//!
//! Reference:
//! - Chen et al., "Elastic Sketch: Adaptive and Fast Network-wide Measurements,"
//!   SIGCOMM 2018.
//!   <https://dl.acm.org/doi/10.1145/3230543.3230544>

use crate::{CANONICAL_HASH_SEED, DataInput, DefaultXxHasher, SketchHasher};

use super::{CountMin, RegularPath};
use crate::Vector2D;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

/// Eviction threshold `lambda` from the paper. A resident flow is replaced once
/// its negative votes reach `LAMBDA` times its positive votes.
pub const LAMBDA: i32 = 8;

/// Rows in the light layer built by [`Elastic::new`] and
/// [`Elastic::init_with_length`].
pub const DEFAULT_LIGHT_ROWS: usize = 3;

/// Columns per light-layer row built by [`Elastic::new`] and
/// [`Elastic::init_with_length`].
pub const DEFAULT_LIGHT_COLS: usize = 4096;

/// One slot of the heavy part: the resident flow, its vote pair, and the flag
/// marking that part of its size lives in the light layer.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HeavyBucket {
    pub flow_id: String,
    pub vote_pos: i32,
    pub vote_neg: i32,
    pub eviction: bool,
}

/// Heavy/light frequency estimator: a heavy hash table over flow ids backed by
/// a Count-Min light layer that absorbs evicted and unelected flows.
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
            flow_id: String::new(),
            vote_pos: 0,
            vote_neg: 0,
            eviction: false,
        }
    }

    /// A bucket holds no flow exactly while it has no positive vote.
    #[inline]
    pub fn is_vacant(&self) -> bool {
        self.vote_pos == 0
    }

    /// Seats `id` in a vacant bucket. The `eviction` flag is carried over so a
    /// bucket vacated by [`Elastic::merge`] still reports its light-layer mass.
    pub fn occupy(&mut self, id: String) {
        self.flow_id = id;
        self.vote_pos = 1;
        self.vote_neg = 0;
    }

    /// Replaces the resident flow with `id`, per the paper's takeover rule.
    pub fn evict(&mut self, id: String) -> String {
        let evicted = std::mem::replace(&mut self.flow_id, id);
        self.vote_pos = 1;
        self.vote_neg = 1;
        self.eviction = true;
        evicted
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

    /// Heavy table of `l` buckets over the default light layer.
    pub fn init_with_length(l: i32) -> Self {
        Elastic::init_with_dimensions(l, DEFAULT_LIGHT_ROWS, DEFAULT_LIGHT_COLS)
    }

    /// Heavy table of `bucket_count` buckets over a `light_rows` by
    /// `light_cols` Count-Min. Bucket count sets the elephant collision rate;
    /// the light dimensions set the error carried by every non-resident flow.
    pub fn init_with_dimensions(bucket_count: i32, light_rows: usize, light_cols: usize) -> Self {
        assert!(
            bucket_count > 0,
            "Elastic needs at least one heavy bucket, got {bucket_count}"
        );
        assert!(
            light_rows > 0 && light_cols > 0,
            "Elastic needs a non-empty light layer, got {light_rows}x{light_cols}"
        );

        let heavy = (0..bucket_count).map(|_| HeavyBucket::new()).collect();
        let light =
            CountMin::<Vector2D<i32>, RegularPath, H>::with_dimensions(light_rows, light_cols);
        Elastic {
            heavy,
            light,
            bktlen: bucket_count,
            _hasher: PhantomData,
        }
    }

    /// Records one occurrence of `id`.
    ///
    /// A vacant bucket seats the flow; a matching bucket takes a positive vote.
    /// Otherwise the bucket takes a negative vote and either the arriving flow
    /// goes to the light layer, or, once `vote_neg >= LAMBDA * vote_pos`, the
    /// resident flow is evicted into the light layer with its full positive
    /// vote and `id` takes the bucket.
    pub fn insert(&mut self, id: String) {
        let idx = self.bucket_index(&id);
        let bucket = &mut self.heavy[idx];

        if bucket.is_vacant() {
            bucket.occupy(id);
            return;
        }
        if bucket.flow_id == id {
            bucket.vote_pos += 1;
            return;
        }

        bucket.vote_neg += 1;
        if bucket.vote_neg < LAMBDA * bucket.vote_pos {
            self.light.insert(&DataInput::String(id));
            return;
        }

        let evicted_votes = bucket.vote_pos;
        let evicted_id = bucket.evict(id);
        self.light
            .insert_many(&DataInput::String(evicted_id), evicted_votes);
    }

    /// Frequency estimate for `id`: the resident vote count, plus the light
    /// layer whenever the bucket carries the eviction flag.
    pub fn query(&self, id: String) -> i32 {
        let idx = self.bucket_index(&id);
        let bucket = &self.heavy[idx];
        if !bucket.is_vacant() && bucket.flow_id == id {
            if bucket.eviction {
                bucket.vote_pos + self.light.estimate(&DataInput::String(id))
            } else {
                bucket.vote_pos
            }
        } else {
            self.light.estimate(&DataInput::String(id))
        }
    }

    /// The paper's Sum merging: folds both heavy parts into their light layers
    /// and adds the layers counter by counter. Correct whatever the two
    /// sketches saw, including flows that appear on both sides.
    ///
    /// The merged sketch answers every flow from the light layer, and its
    /// vacated buckets stay flagged so later residents keep reading it.
    pub fn merge(&mut self, other: &Elastic<H>) {
        assert_eq!(
            self.bktlen, other.bktlen,
            "bucket length mismatch while merging Elastic sketches"
        );

        self.flush_heavy_to_light();
        self.light.merge(&other.light);
        for bucket in &other.heavy {
            if bucket.is_vacant() {
                continue;
            }
            self.light
                .insert_many(&DataInput::Str(&bucket.flow_id), bucket.vote_pos);
        }
    }

    /// The paper's Maximum merging: folds both heavy parts into their light
    /// layers and keeps the larger of each counter pair, which is tighter than
    /// [`Self::merge`] and still never underestimates.
    ///
    /// Requires the two sketches to have observed **disjoint flow sets**. A
    /// flow both sides saw reads back as the larger side rather than the sum,
    /// which underestimates it; use [`Self::merge`] whenever flows can repeat
    /// across sketches.
    pub fn merge_max(&mut self, other: &Elastic<H>) {
        assert_eq!(
            self.bktlen, other.bktlen,
            "bucket length mismatch while merging Elastic sketches"
        );

        self.flush_heavy_to_light();

        // max does not commute with the peer's pending heavy mass the way sum
        // does, so the peer's light layer is completed before the comparison
        let mut other_light = other.light.clone();
        for bucket in &other.heavy {
            if bucket.is_vacant() {
                continue;
            }
            other_light.insert_many(&DataInput::Str(&bucket.flow_id), bucket.vote_pos);
        }
        self.light.merge_max(&other_light);
    }

    #[inline]
    fn bucket_index(&self, id: &str) -> usize {
        let hash = H::hash64_seeded(CANONICAL_HASH_SEED, &DataInput::Str(id));
        hash as usize % self.bktlen as usize
    }

    fn flush_heavy_to_light(&mut self) {
        for idx in 0..self.heavy.len() {
            let bucket = &mut self.heavy[idx];
            if bucket.is_vacant() {
                bucket.eviction = true;
                continue;
            }
            let votes = bucket.vote_pos;
            let flow_id = std::mem::take(&mut bucket.flow_id);
            bucket.vote_pos = 0;
            bucket.vote_neg = 0;
            bucket.eviction = true;
            self.light.insert_many(&DataInput::String(flow_id), votes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CANONICAL_HASH_SEED, DataInput, hash64_seeded};

    fn bucket_for(id: &str, sketch: &Elastic) -> usize {
        let hash = hash64_seeded(CANONICAL_HASH_SEED, &DataInput::Str(id));
        hash as usize % sketch.bktlen as usize
    }

    fn colliding_key(primary: &str, sketch: &Elastic) -> String {
        let target = bucket_for(primary, sketch);
        (0..10_000)
            .map(|idx| format!("flow::secondary::{idx}"))
            .find(|candidate| bucket_for(candidate, sketch) == target && candidate != primary)
            .expect("unable to find colliding key for test")
    }

    #[test]
    fn init_with_dimensions_sizes_both_parts() {
        let sketch: Elastic = Elastic::init_with_dimensions(12, 2, 256);

        assert_eq!(sketch.heavy.len(), 12);
        assert_eq!(sketch.bktlen, 12);
        assert_eq!(sketch.light.rows(), 2);
        assert_eq!(sketch.light.cols(), 256);
    }

    #[test]
    fn init_with_length_keeps_the_default_light_layer() {
        let sketch: Elastic = Elastic::init_with_length(8);

        assert_eq!(sketch.heavy.len(), 8);
        assert_eq!(sketch.light.rows(), DEFAULT_LIGHT_ROWS);
        assert_eq!(sketch.light.cols(), DEFAULT_LIGHT_COLS);
    }

    #[test]
    #[should_panic(expected = "at least one heavy bucket")]
    fn an_empty_heavy_table_is_rejected() {
        let _: Elastic = Elastic::init_with_length(0);
    }

    #[test]
    #[should_panic(expected = "at least one heavy bucket")]
    fn a_negative_heavy_table_is_rejected() {
        let _: Elastic = Elastic::init_with_length(-1);
    }

    #[test]
    #[should_panic(expected = "non-empty light layer")]
    fn an_empty_light_layer_is_rejected() {
        let _: Elastic = Elastic::init_with_dimensions(8, 0, 4096);
    }

    #[test]
    #[should_panic(expected = "non-empty light layer")]
    fn a_zero_width_light_layer_is_rejected() {
        let _: Elastic = Elastic::init_with_dimensions(8, 3, 0);
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
        let secondary = colliding_key(primary, &sketch);

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
    fn eviction_moves_the_resident_flow_into_the_light_layer() {
        // the paper evicts the flow sitting in the bucket, not the arriving one
        let mut sketch: Elastic = Elastic::init_with_length(8);
        let primary = "flow::primary";
        let secondary = colliding_key(primary, &sketch);

        for _ in 0..10 {
            sketch.insert(primary.to_string());
        }
        // vote_neg reaches LAMBDA * 10 on the 80th arrival, which triggers takeover
        for _ in 0..(LAMBDA * 10) {
            sketch.insert(secondary.clone());
        }

        let idx = bucket_for(primary, &sketch);
        assert_eq!(sketch.heavy[idx].flow_id, secondary, "takeover must happen");
        assert!(sketch.heavy[idx].eviction);
        assert_eq!(sketch.heavy[idx].vote_pos, 1);
        assert_eq!(sketch.heavy[idx].vote_neg, 1);

        assert_eq!(
            sketch.query(primary.to_string()),
            10,
            "evicted flow keeps its full size in the light layer"
        );
        assert_eq!(
            sketch.query(secondary.clone()),
            LAMBDA * 10,
            "arriving flow must not absorb the evicted flow's votes"
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

        assert_eq!(left.query("flow::left".to_string()), 30);
        assert_eq!(left.query("flow::right".to_string()), 18);
        assert!(left.heavy.iter().all(|bucket| {
            bucket.flow_id.is_empty()
                && bucket.is_vacant()
                && bucket.vote_neg == 0
                && bucket.eviction
        }));
    }

    #[test]
    fn merge_preserves_colliding_flow_mass() {
        let mut left: Elastic = Elastic::init_with_length(8);
        let primary = "flow::primary";
        let secondary = colliding_key(primary, &left);

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

    #[test]
    fn a_bucket_reoccupied_after_merge_still_reads_the_light_layer() {
        let mut left: Elastic = Elastic::init_with_length(8);
        for _ in 0..30 {
            left.insert("flow::left".to_string());
        }
        let right: Elastic = Elastic::init_with_length(8);

        left.merge(&right);
        left.insert("flow::left".to_string());

        assert_eq!(
            left.query("flow::left".to_string()),
            31,
            "a post-merge resident must keep the mass flushed into the light layer"
        );
    }

    /// Builds two sketches over disjoint flow sets, plus the truth table.
    fn disjoint_pair() -> (Elastic, Elastic, Vec<(String, i32)>) {
        let mut left: Elastic = Elastic::init_with_dimensions(8, 2, 64);
        let mut right: Elastic = Elastic::init_with_dimensions(8, 2, 64);
        let mut truth = Vec::new();

        for i in 0..40i32 {
            let key = format!("left::{i}");
            let count = (i % 7) + 1;
            for _ in 0..count {
                left.insert(key.clone());
            }
            truth.push((key, count));
        }
        for i in 0..40i32 {
            let key = format!("right::{i}");
            let count = (i % 5) + 1;
            for _ in 0..count {
                right.insert(key.clone());
            }
            truth.push((key, count));
        }
        (left, right, truth)
    }

    #[test]
    fn maximum_merging_never_underestimates_disjoint_flows() {
        let (mut left, right, truth) = disjoint_pair();
        left.merge_max(&right);

        for (key, count) in &truth {
            let est = left.query(key.clone());
            assert!(
                est >= *count,
                "maximum merging underestimated {key}: {est} < {count}"
            );
        }
    }

    #[test]
    fn maximum_merging_is_tighter_than_sum_merging() {
        let (mut summed, right, truth) = disjoint_pair();
        summed.merge(&right);
        let (mut maxed, right, _) = disjoint_pair();
        maxed.merge_max(&right);

        let mut strictly_tighter = 0;
        for (key, _) in &truth {
            let sum_est = summed.query(key.clone());
            let max_est = maxed.query(key.clone());
            assert!(
                max_est <= sum_est,
                "maximum merging must not be looser on {key}: {max_est} > {sum_est}"
            );
            if max_est < sum_est {
                strictly_tighter += 1;
            }
        }
        assert!(
            strictly_tighter > 0,
            "no flow got a tighter estimate, so this input cannot tell the merges apart"
        );
    }

    #[test]
    fn maximum_merging_underestimates_a_flow_both_sides_saw() {
        // the paper's precondition, pinned: MM is for disjoint flow sets, and a
        // shared flow reads back as the larger side instead of the sum
        let mut left: Elastic = Elastic::init_with_length(8);
        let mut right: Elastic = Elastic::init_with_length(8);
        for _ in 0..30 {
            left.insert("flow::shared".to_string());
        }
        for _ in 0..20 {
            right.insert("flow::shared".to_string());
        }

        left.merge_max(&right);

        assert_eq!(left.query("flow::shared".to_string()), 30);

        let mut summed: Elastic = Elastic::init_with_length(8);
        let mut right: Elastic = Elastic::init_with_length(8);
        for _ in 0..30 {
            summed.insert("flow::shared".to_string());
        }
        for _ in 0..20 {
            right.insert("flow::shared".to_string());
        }
        summed.merge(&right);
        assert_eq!(summed.query("flow::shared".to_string()), 50);
    }
}
