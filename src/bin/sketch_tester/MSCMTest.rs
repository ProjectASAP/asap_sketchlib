use sketchlib_rust::sketches::{countmin::CountMinMS, utils::SketchInput};
// use std::collections::HashMap;

fn main() {
    println!("=== CountMin Test Suite ===\n");
    test_basic_operations();
    // test_random_seed_operations(&hash_seeds);
    // test_accuracy();
    // test_merge_operation();
    // test_large_cardinality();
    // test_duplicate_handling();
}

fn test_basic_operations() {
    println!("Test 1: Basic Operations");
    println!("------------------------");

    let mut cm = CountMinMS::init_cmms(3, 32, 10, 5);

    // Insert some values
    for i in 1..21 {
        for _ in 0..i {
            cm.insert(&SketchInput::I32(i), i as u64);
        }
        if i > 10 {
            for _ in 0..(i - 10) {
                cm.delete(&SketchInput::I32(i - 10), i as u64 - 10);
            }
        }
        // mscm.debug();
        println!(
            "<----- at timestamp: {}, just insert {} for {} times ----->",
            i, i, i
        );
        for j in 1..21 {
            let estimate = cm.get_est(&SketchInput::I32(j), i as u64);
            println!("Estimate at ts {} for {} is: {}", i, j, estimate);
        }
        println!("<----- Check Complete ----->")
    }
}

// fn test_random_seed_operations(s1: &Vec<u64>) {
//     println!("Test 2: Operations With Large CountMin");
//     println!("--------------------------------------");

//     let mut c = CountMin::init_cm_with_row_col(4, 1024, s1);
//     // println!("Initial Sketch: ");
//     // c.debug();
//     // println!("********************");
//     for i in 1..101 {
//         for _ in 0..i {
//             c.insert_cm(&i);
//         }
//     }
//     // println!("Sketch after Insertion: ");
//     // c.debug();
//     // println!("********************");
//     for i in 1..101 {
//         let estimate = c.get_est(&i);
//         println!("Estimate for {} is: {} while should be: {}", i, estimate, i);
//     }
//     // println!("Sketch after query: ");
//     // c.debug();
//     // println!("********************");
// }
