use sketchlib_rust::sketches::countmin::CountMin;
use sketchlib_rust::sketches::utils::SketchInput;
// use std::collections::HashMap;

fn main() {
    println!("=== CountMin Test Suite ===\n");
    test_basic_operations();
    test_random_seed_operations();
    // test_accuracy();
    // test_merge_operation();
    // test_large_cardinality();
    // test_duplicate_handling();
}

fn test_basic_operations() {
    println!("Test 1: Basic Operations");
    println!("------------------------");

    let mut cm = CountMin::init_count_min();

    // Insert some values
    for i in 1..11 {
        for _ in 0..i {
            cm.insert_cm(&SketchInput::I32(i));
            cm.get_est(&SketchInput::I32(i));
        }
    }

    for i in 1..11 {
        let estimate = cm.get_est(&SketchInput::I32(i));
        println!("Estimate for {} is: {} while should be: {}", i, estimate, i);
    }
}

fn test_random_seed_operations() {
    println!("Test 2: Operations With Large CountMin");
    println!("--------------------------------------");

    let mut c = CountMin::init_cm_with_row_col(4, 1024);
    // println!("Initial Sketch: ");
    // c.debug();
    // println!("********************");
    for i in 1..101 {
        for _ in 0..i {
            c.insert_cm(&SketchInput::I32(i));
        }
    }
    // println!("Sketch after Insertion: ");
    // c.debug();
    // println!("********************");
    for i in 1..101 {
        let estimate = c.get_est(&SketchInput::I32(i));
        println!("Estimate for {} is: {} while should be: {}", i, estimate, i);
    }
    // println!("Sketch after query: ");
    // c.debug();
    // println!("********************");
}
