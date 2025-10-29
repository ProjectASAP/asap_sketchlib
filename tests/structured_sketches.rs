// use sketchlib_rust::{
//     CountMin, SketchInput, SketchList, SketchMatrix,
//     sketches::{HyperLogLog, StructuredCount, StructuredCountMin, VectorCount, VectorCountMin},
// };

// #[test]
// fn sketch_matrix_filled_initializes_and_updates() {
//     // ensures SketchMatrix clones seed values and lets callers mutate individual cells
//     let mut matrix = SketchMatrix::filled(2, 3, 0u8);
//     assert_eq!(matrix.rows(), 2);
//     assert_eq!(matrix.cols(), 3);
//     assert_eq!(matrix[0][1], 0);

//     *matrix.get_mut(0, 1).expect("matrix cell exists") = 7;
//     assert_eq!(matrix.get(0, 1), Some(&7));

//     matrix.for_each_mut(|row, col, value| {
//         if row == col {
//             *value = 1;
//         }
//     });
//     assert_eq!(matrix[0][0], 1);
//     assert_eq!(matrix[1][1], 1);
// }

// #[test]
// fn sketch_list_filled_supports_iteration() {
//     // ensures SketchList::filled creates a list and iteration exposes the same values
//     let list = SketchList::filled(4, 5u32);
//     assert_eq!(list.len(), 4);
//     assert!(list.iter().all(|item| *item == 5));
// }

// #[test]
// fn structured_countmin_tracks_frequency_estimates() {
//     // verifies the new CountMin implementation reports at least the inserted frequency
//     let mut sketch = StructuredCountMin::with_dimensions(4, 64);
//     let heavy = SketchInput::String("hot-key".into());
//     let cold = SketchInput::String("cold-key".into());

//     for _ in 0..25 {
//         sketch.insert(&heavy);
//     }
//     for _ in 0..3 {
//         sketch.insert(&cold);
//     }

//     let heavy_est = sketch.estimate(&heavy);
//     let cold_est = sketch.estimate(&cold);

//     assert!(
//         heavy_est >= 25,
//         "heavy key should have estimate >= 25, got {heavy_est}"
//     );
//     assert!(
//         cold_est >= 3,
//         "cold key should at least match inserted count, got {cold_est}"
//     );
//     assert!(heavy_est >= cold_est, "heavy key should dominate cold key");
// }

// #[test]
// fn structured_count_tracks_frequency_estimates() {
//     let mut sketch = StructuredCount::with_dimensions(4, 64);
//     let heavy = SketchInput::String("count-hot-key".into());
//     let cold = SketchInput::String("count-cold-key".into());

//     for _ in 0..20 {
//         sketch.insert(&heavy);
//     }
//     for _ in 0..5 {
//         sketch.insert(&cold);
//     }

//     let heavy_est = sketch.estimate(&heavy);
//     let cold_est = sketch.estimate(&cold);
//     let heavy_mag = heavy_est.abs();
//     let cold_mag = cold_est.abs();

//     assert!(
//         heavy_mag >= cold_mag,
//         "heavy key magnitude should dominate cold key, heavy {heavy_est}, cold {cold_est}"
//     );
//     assert!(
//         heavy_mag >= 20.0,
//         "expected heavy estimate magnitude near 20, got {heavy_est}"
//     );
//     assert!(
//         cold_mag >= 5.0,
//         "expected cold estimate magnitude near 5, got {cold_est}"
//     );
// }

// #[test]
// fn vector_countmin_tracks_frequency_estimates() {
//     let mut sketch = VectorCountMin::with_dimensions(4, 64);
//     let heavy = SketchInput::String("vector-hot-key".into());
//     let cold = SketchInput::String("vector-cold-key".into());

//     for _ in 0..25 {
//         sketch.insert(&heavy);
//     }
//     for _ in 0..3 {
//         sketch.insert(&cold);
//     }

//     let heavy_est = sketch.estimate(&heavy);
//     let cold_est = sketch.estimate(&cold);

//     assert!(
//         heavy_est >= 25,
//         "heavy key should have estimate >= 25, got {heavy_est}"
//     );
//     assert!(
//         cold_est >= 3,
//         "cold key should at least match inserted count, got {cold_est}"
//     );
//     assert!(heavy_est >= cold_est, "heavy key should dominate cold key");
// }

// #[test]
// fn vector_countmin_matches_matrix_estimates_on_shared_stream() {
//     let mut matrix = StructuredCountMin::with_dimensions(3, 256);
//     let mut vector = VectorCountMin::with_dimensions(3, 256);
//     let mut classic = CountMin::init_cm_with_row_col(3, 256);

//     let updates: Vec<SketchInput<'static>> = (0..1_000)
//         .map(|i| SketchInput::U64((i % 97) as u64))
//         .collect();

//     for value in &updates {
//         matrix.insert(value);
//         vector.insert(value);
//         classic.insert_cm(value);
//     }

//     for query in [
//         SketchInput::U64(1),
//         SketchInput::U64(32),
//         SketchInput::U64(96),
//     ] {
//         let matrix_est = matrix.estimate(&query);
//         let vector_est = vector.estimate(&query);
//         assert_eq!(
//             matrix_est, vector_est,
//             "expected vector-backed sketch to mirror matrix-backed results"
//         );

//         let target = match &query {
//             SketchInput::U64(v) => *v,
//             _ => unreachable!(),
//         };

//         let actual = updates
//             .iter()
//             .filter(|value| match value {
//                 SketchInput::U64(v) => *v == target,
//                 _ => false,
//             })
//             .count() as u64;

//         assert!(
//             vector_est >= actual,
//             "count-min estimate should not be below true count"
//         );

//         let classic_est = classic.get_est(&query);
//         assert!(
//             classic_est > 0,
//             "classic sketch estimate should remain finite"
//         );
//     }
// }

// #[test]
// fn vector_count_matches_structured_count_on_shared_stream() {
//     let mut matrix = StructuredCount::with_dimensions(3, 128);
//     let mut vector = VectorCount::with_dimensions(3, 128);

//     let updates: Vec<SketchInput<'static>> = (0..512)
//         .map(|i| SketchInput::U64((i % 97) as u64))
//         .collect();

//     for value in &updates {
//         matrix.insert(value);
//         vector.insert(value);
//     }

//     for query in [
//         SketchInput::U64(1),
//         SketchInput::U64(32),
//         SketchInput::U64(96),
//     ] {
//         let matrix_est = matrix.estimate(&query);
//         let vector_est = vector.estimate(&query);
//         assert!(
//             (matrix_est - vector_est).abs() < f64::EPSILON,
//             "expected vector-backed sketch to mirror matrix-backed results"
//         );
//     }
// }

// #[test]
// fn structured_and_vector_counts_stay_in_sync_after_merges() {
//     let mut primary_matrix = StructuredCount::with_dimensions(3, 64);
//     let mut primary_vector = VectorCount::with_dimensions(3, 64);
//     let mut shard_matrix = StructuredCount::with_dimensions(3, 64);
//     let mut shard_vector = VectorCount::with_dimensions(3, 64);

//     let heavy = SketchInput::String("merged-hot-key".into());
//     let cold = SketchInput::String("merged-cold-key".into());

//     for _ in 0..6 {
//         primary_matrix.insert(&heavy);
//         primary_vector.insert(&heavy);
//     }
//     for _ in 0..2 {
//         primary_matrix.insert(&cold);
//         primary_vector.insert(&cold);
//     }

//     for _ in 0..5 {
//         shard_matrix.insert(&heavy);
//         shard_vector.insert(&heavy);
//     }
//     shard_vector.insert(&cold);
//     shard_matrix.insert(&cold);

//     primary_matrix.merge(&shard_matrix);
//     primary_vector.merge(&shard_vector);

//     let heavy_matrix = primary_matrix.estimate(&heavy);
//     let heavy_vector = primary_vector.estimate(&heavy);
//     let cold_matrix = primary_matrix.estimate(&cold);
//     let cold_vector = primary_vector.estimate(&cold);

//     assert!(
//         (heavy_matrix - heavy_vector).abs() < f64::EPSILON,
//         "heavy key estimates should align between matrix and vector sketches: matrix={heavy_matrix}, vector={heavy_vector}"
//     );
//     assert!(
//         (cold_matrix - cold_vector).abs() < f64::EPSILON,
//         "cold key estimates should align between matrix and vector sketches: matrix={cold_matrix}, vector={cold_vector}"
//     );
//     assert!(
//         heavy_matrix.abs() >= cold_matrix.abs(),
//         "heavy key magnitude should dominate cold key after merge"
//     );
// }

// #[test]
// fn hyperloglog_estimate_is_within_reasonable_error() {
//     // ensures HyperLogLog leverages SketchList to provide a cardinality estimate near truth
//     let mut sketch = HyperLogLog::new();
//     let population = 10_000u64;

//     for value in 0..population {
//         sketch.insert(&SketchInput::U64(value));
//     }

//     let estimate = sketch.estimate() as f64;
//     let relative_error = (estimate - population as f64).abs() / population as f64;

//     assert!(
//         relative_error < 0.06,
//         "expected relative error < 6%, got {relative_error}"
//     );
// }
