use sketchlib_rust::sketches::microscope::MicroScope;
// use std::collections::HashMap;

fn main() {
    println!("=== Microscope Test Suite ===\n");
    test_basic_operations();
    test_different_window_sizes();
    test_time_jump();
    // test_random_seed_operations(&hash_seeds);
    // test_accuracy();
    // test_merge_operation();
    // test_large_cardinality();
    // test_duplicate_handling();
}

fn test_basic_operations() {
    println!("Test 1: Basic Operations");
    println!("------------------------");

    let mut ms = MicroScope::init_microscope(10, 5);
    let mut count = 0;

    // Insert some values
    for i in 1..21 {
        for _ in 0..i {
            ms.insert(i + 1000);
            count += 1;
        }
        // hard code window size to 10
        if i > 10 {
            for _ in 0..i - 10 {
                // ms.delete(i+1000-10);
                count -= 1;
            }
        }
        // mscm.debug();
        println!(
            "<----- at timestamp: {}, just insert for {} times ----->",
            i + 1000,
            i
        );
        let estimate = ms.query(i + 1000);
        println!(
            "Estimate at ts {} is: {}, where it should be: {}",
            i, estimate, count
        );
        // ms.debug();
        println!("<----- Check Complete ----->\n")
    }
}

fn test_different_window_sizes() {
    println!("Test 2: Different Window Size");
    println!("-----------------------------");
    let w = 8;

    let mut ms = MicroScope::init_microscope(w, 4);
    let mut count = 0;

    // Insert some values
    for i in 1..21 {
        for _ in 0..i {
            ms.insert(i);
            count += 1;
        }
        // hard code window size to 10
        if i > w as u64 {
            for _ in 0..i - w as u64 {
                // ms.delete(i+1000-10);
                count -= 1;
            }
        }
        // ms.debug();
        println!(
            "<----- at timestamp: {}, just insert for {} times ----->",
            i, i
        );
        let estimate = ms.query(i);
        println!(
            "Estimate at ts {} is: ***{}***, where it should be: ***{}***",
            i, estimate, count
        );
        // ms.debug();
        println!("<----- Check Complete ----->\n")
    }
}

fn test_time_jump() {
    println!("Test 3: Time             Jump");
    println!("-----------------------------");
    let w = 10;

    let mut ms = MicroScope::init_microscope(w, 5);
    let mut count = 0;

    // Insert some values
    for i in 1..21 {
        for _ in 0..i {
            ms.insert(i + 1000);
            count += 1;
        }
        // hard code window size to 10
        if i > w as u64 {
            for _ in 0..i - w as u64 {
                // ms.delete(i+1000-10);
                count -= 1;
            }
        }
        // ms.debug();
        println!(
            "<----- at timestamp: {}, just insert for {} times ----->",
            i, i
        );
        let estimate = ms.query(i);
        println!(
            "Estimate at ts {} is: ***{}***, where it should be: ***{}***",
            i, estimate, count
        );
        // ms.debug();
        println!("<----- Check Complete ----->\n")
    }

    for i in 3000..3005 {
        ms.insert(i);
        println!("After time jump, estimate at {} is: {}", i, ms.query(i));
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
