// use crate::common::{LASTSTATE, SketchInput, Vector1D, hash_it};
// use crate::sketches::HeavyBucket;

// use super::countmin::CountMin as StructuredCountMin;

// /// Elastic sketch backed by the shared vector abstractions.
// #[derive(Clone, Debug)]
// pub struct Elastic {
//     heavy: Vector1D<HeavyBucket>,
//     light: StructuredCountMin,
//     bucket_len: usize,
// }

// impl Default for Elastic {
//     fn default() -> Self {
//         Self::new()
//     }
// }

// impl Elastic {
//     /// Builds a sketch with the default bucket length (8).
//     pub fn new() -> Self {
//         Self::with_length(8)
//     }

//     /// Builds a sketch with a user-specified number of buckets.
//     pub fn with_length(length: usize) -> Self {
//         let heavy = Vector1D::filled(length, HeavyBucket::new());
//         let light = StructuredCountMin::default();
//         Self {
//             heavy,
//             light,
//             bucket_len: length,
//         }
//     }

//     /// Inserts a flow identifier into the sketch.
//     pub fn insert(&mut self, id: &str) {
//         let idx = self.bucket_index(id);
//         let bucket = self
//             .heavy
//             .as_mut_slice()
//             .get_mut(idx)
//             .expect("bucket index validated");

//         if bucket.flow_id.is_empty() && bucket.vote_neg == 0 && bucket.vote_pos == 0 {
//             bucket.flow_id = id.to_string();
//             bucket.vote_pos += 1;
//             bucket.eviction = false;
//             return;
//         }

//         if bucket.flow_id == id {
//             bucket.vote_pos += 1;
//             return;
//         }

//         bucket.vote_neg += 1;
//         if bucket.vote_pos == 0 || bucket.vote_neg / bucket.vote_pos < 8 {
//             let input = SketchInput::Str(id);
//             self.light.insert(&input);
//         } else {
//             let vote = bucket.vote_pos;
//             bucket.evict(id.to_string());
//             for _ in 0..vote {
//                 let input = SketchInput::Str(bucket.flow_id.as_str());
//                 self.light.insert(&input);
//             }
//         }
//     }

//     /// Returns the estimated frequency for `id`.
//     pub fn query(&self, id: &str) -> i32 {
//         let idx = self.bucket_index(id);
//         let bucket = self
//             .heavy
//             .as_slice()
//             .get(idx)
//             .expect("bucket index validated");

//         if bucket.flow_id == id {
//             if bucket.eviction {
//                 let input = SketchInput::Str(id);
//                 let light_est = self.light.estimate(&input) as i32;
//                 light_est + bucket.vote_pos
//             } else {
//                 bucket.vote_pos
//             }
//         } else {
//             let input = SketchInput::Str(id);
//             self.light.estimate(&input) as i32
//         }
//     }

//     fn bucket_index(&self, id: &str) -> usize {
//         let hash = hash_it(LASTSTATE, &SketchInput::Str(id));
//         (hash as usize) % self.bucket_len
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     fn bucket_for(sketch: &Elastic, id: &str) -> usize {
//         let hash = hash_it(LASTSTATE, &SketchInput::Str(id));
//         (hash as usize) % sketch.bucket_len
//     }

//     #[test]
//     fn heavy_bucket_tracks_repeated_flow_exactly() {
//         let mut sketch = Elastic::with_length(8);
//         let flow = "flow::primary";

//         for _ in 0..12 {
//             sketch.insert(flow);
//         }

//         assert_eq!(sketch.query(flow), 12);
//         assert_eq!(sketch.query("other"), 0);
//     }

//     #[test]
//     fn light_sketch_counts_colliding_flows() {
//         let mut sketch = Elastic::with_length(8);
//         let primary = "flow::primary";
//         let target_bucket = bucket_for(&sketch, primary);

//         let mut secondary = None;
//         for idx in 0..10_000 {
//             let candidate = format!("flow::secondary::{idx}");
//             if bucket_for(&sketch, &candidate) == target_bucket && candidate != primary {
//                 secondary = Some(candidate);
//                 break;
//             }
//         }
//         let secondary = secondary.expect("unable to find colliding key for test");

//         for _ in 0..10 {
//             sketch.insert(primary);
//         }
//         for _ in 0..6 {
//             sketch.insert(&secondary);
//         }

//         let heavy_est = sketch.query(primary);
//         let light_est = sketch.query(&secondary);

//         assert!(
//             heavy_est >= 10,
//             "expected heavy bucket >= 10 after repeated inserts, got {}",
//             heavy_est
//         );
//         assert!(
//             light_est >= 6,
//             "colliding flow should accumulate in CountMin, expected >= 6, got {}",
//             light_est
//         );
//     }
// }
