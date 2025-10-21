// use sketchlib_rust::sketches::hll::HLL;
// use std::collections::HashSet;

fn main() {
    println!("=== HLLHIP Test Suite ===\n");

    // test_basic_operations();
    // test_accuracy();
    // test_merge_operation();
    // test_large_cardinality();
    // test_duplicate_handling();
}

// fn test_basic_operations() {
//     println!("Test 1: Basic Operations");
//     println!("------------------------");

//     let mut hll = HLL::default();
//     println!("Initial estimate (empty): {}", hll.calculate_est());

//     // Insert some values
//     for i in 0..10 {
//         hll.insert_hll(&i);
//     }

//     let estimate = hll.calculate_est();

//     println!("After inserting 0-9:");
//     println!("  Actual: 10");
//     println!("  Estimate: {}", estimate);
//     println!(
//         "  Error: {:.2}%\n",
//         ((estimate as f64 - 10.0) / 10.0 * 100.0).abs()
//     );
// }

// fn test_accuracy() {
//     println!("Test 2: Accuracy at Different Cardinalities");
//     println!("-------------------------------------------");

//     let test_sizes = vec![100, 1000, 10000, 100000];

//     for size in test_sizes {
//         let mut hll = HLL::default();

//         for i in 0..size {
//             hll.insert_hll(&i);
//         }

//         let estimate = hll.calculate_est();
//         let error_rate = ((estimate as f64 - size as f64) / size as f64 * 100.0).abs();

//         println!("Cardinality: {}", size);
//         println!("  Estimate: {}", estimate);
//         println!("  Error: {:.2}%", error_rate);
//     }
//     println!();
// }

// fn test_merge_operation() {
//     println!("Test 3: Merge Operation");
//     println!("-----------------------");

//     let mut hll1 = HLL::default();
//     let mut hll2 = HLL::default();

//     // Insert different ranges into each HLL
//     for i in 0..5000 {
//         hll1.insert_hll(&i);
//     }

//     for i in 5000..10000 {
//         hll2.insert_hll(&i);
//     }

//     let estimate1 = hll1.calculate_est();
//     let estimate2 = hll2.calculate_est();

//     println!("HLL1 (0-4999): {}", estimate1);
//     println!("HLL2 (5000-9999): {}", estimate2);

//     // Merge HLL2 into HLL1
//     hll1.merge_hll(&hll2);
//     let merged_estimate = hll1.calculate_est();

//     println!("Merged HLL (0-9999):");
//     println!("  Actual: 10000");
//     println!("  Estimate: {}", merged_estimate);
//     println!(
//         "  Error: {:.2}%\n",
//         ((merged_estimate as f64 - 10000.0) / 10000.0 * 100.0).abs()
//     );
// }

// fn test_large_cardinality() {
//     println!("Test 4: Large Cardinality");
//     println!("-------------------------");

//     let mut hll = HLL::default();
//     let size = 5_000_000;

//     // Insert a million unique values
//     for i in 0..size {
//         hll.insert_hll(&format!("item_{}", i));
//     }

//     let estimate = hll.calculate_est();
//     let error_rate = ((estimate as f64 - size as f64) / size as f64 * 100.0).abs();

//     println!("Cardinality: {}", size);
//     println!("Estimate: {}", estimate);
//     println!("Error: {:.2}%", error_rate);
// }

// fn test_duplicate_handling() {
//     println!("Test 5: Duplicate Handling");
//     println!("--------------------------");

//     let mut hll = HLL::default();
//     let mut exact_set = HashSet::new();

//     // Insert values with duplicates
//     let values = vec![1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

//     for val in &values {
//         hll.insert_hll(val);
//         exact_set.insert(val);
//     }

//     println!("Inserted values: {:?}", values);
//     println!("Unique count: {}", exact_set.len());
//     println!("HLL estimate: {}", hll.calculate_est());

//     // Test with string duplicates
//     let mut hll_str = HLL::default();
//     let strings = vec!["apple", "banana", "apple", "cherry", "banana", "date"];

//     for s in &strings {
//         hll_str.insert_hll(s);
//     }

//     println!("\nString test:");
//     println!("Inserted: {:?}", strings);
//     println!("Unique count: 4");
//     println!("HLL estimate: {}", hll_str.calculate_est());
// }
