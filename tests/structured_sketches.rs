use sketchlib_rust::{
    SketchInput, SketchList, SketchMatrix,
    sketches::{HyperLogLog, StructuredCountMin},
};

#[test]
fn sketch_matrix_filled_initializes_and_updates() {
    // ensures SketchMatrix clones seed values and lets callers mutate individual cells
    let mut matrix = SketchMatrix::filled(2, 3, 0u8);
    assert_eq!(matrix.rows(), 2);
    assert_eq!(matrix.cols(), 3);
    assert_eq!(matrix[0][1], 0);

    *matrix.get_mut(0, 1).expect("matrix cell exists") = 7;
    assert_eq!(matrix.get(0, 1), Some(&7));

    matrix.for_each_mut(|row, col, value| {
        if row == col {
            *value = 1;
        }
    });
    assert_eq!(matrix[0][0], 1);
    assert_eq!(matrix[1][1], 1);
}

#[test]
fn sketch_list_filled_supports_iteration() {
    // ensures SketchList::filled creates a list and iteration exposes the same values
    let list = SketchList::filled(4, 5u32);
    assert_eq!(list.len(), 4);
    assert!(list.iter().all(|item| *item == 5));
}

#[test]
fn structured_countmin_tracks_frequency_estimates() {
    // verifies the new CountMin implementation reports at least the inserted frequency
    let mut sketch = StructuredCountMin::with_dimensions(4, 64);
    let heavy = SketchInput::String("hot-key".into());
    let cold = SketchInput::String("cold-key".into());

    for _ in 0..25 {
        sketch.insert(&heavy);
    }
    for _ in 0..3 {
        sketch.insert(&cold);
    }

    let heavy_est = sketch.estimate(&heavy);
    let cold_est = sketch.estimate(&cold);

    assert!(
        heavy_est >= 25,
        "heavy key should have estimate >= 25, got {heavy_est}"
    );
    assert!(
        cold_est >= 3,
        "cold key should at least match inserted count, got {cold_est}"
    );
    assert!(heavy_est >= cold_est, "heavy key should dominate cold key");
}

#[test]
fn hyperloglog_estimate_is_within_reasonable_error() {
    // ensures HyperLogLog leverages SketchList to provide a cardinality estimate near truth
    let mut sketch = HyperLogLog::new();
    let population = 10_000u64;

    for value in 0..population {
        sketch.insert(&SketchInput::U64(value));
    }

    let estimate = sketch.estimate() as f64;
    let relative_error = (estimate - population as f64).abs() / population as f64;

    assert!(
        relative_error < 0.06,
        "expected relative error < 6%, got {relative_error}"
    );
}
