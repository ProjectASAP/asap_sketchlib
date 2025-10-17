// use querysimulation::sketches::countmin::CountMin;
use querysimulation::sketches::kll::KLL;
// use rand::rngs::StdRng;
// use rand::{thread_rng, Rng, SeedableRng};
// use std::collections::HashMap;

// Claude generates some tests
// well, they don't seem to be ideal for my intention

fn main() {
    println!("=== KLL Test Suite ===\n");

    test_basic_functionality();

    // test_quantiles();

    // test_basic_operations();
    // test_accuracy();
    // test_merge_operation();
    // test_large_cardinality();
    // test_duplicate_handling();
}

fn test_basic_functionality() {
    let mut kll = KLL::init_kll(20);

    // Insert some values
    for i in 1..=1000000 {
        kll.update(i as f64);
        // println!("update {}", i)
    }
    kll.print_compactors();

    // assert_eq!(kll.count(), 100);

    // Test rank
    // assert_eq!(kll.rank(50.0), 50);
    // assert_eq!(kll.rank(0.0), 0);
    // assert_eq!(kll.rank(100.0), 100);
    println!("rank 5000.0: {}", kll.rank(5000.0));
    println!("rank 0.0: {}", kll.rank(0.0));
    println!("rank 10000.0: {}", kll.rank(10000.0));
    println!("rank 2500.0: {}", kll.rank(2500.0));
    println!("rank 7500.0: {}", kll.rank(7500.0));

    println!("quantile 5000.0: {}", kll.quantile(5000.0));
    println!("quantile 0.0: {}", kll.quantile(0.0));
    println!("quantile 10000.0: {}", kll.quantile(10000.0));
    println!("quantile 2500.0: {}", kll.quantile(2500.0));
    println!("quantile 7500.0: {}", kll.quantile(7500.0));

    println!("possible min: {}", kll.cdf().query(0.0));
    println!("possible max: {}", kll.cdf().query(1.0));
    println!("possible lower quantile: {}", kll.cdf().query(0.25));
    println!("possible upper quantile: {}", kll.cdf().query(0.75));
}

// fn test_quantiles() {
//     let mut kll = KLL::init_kll(2);

//     // Insert values 1 to 1000
//     for i in 1..=1000 {
//         kll.update(i as f64);
//     }

//     println!("quantile of 500.0: {}", kll.quantile(500.0));
//     println!("quantile of 0.0: {}", kll.quantile(0.0));
//     println!("quantile of 1000.0: {}", kll.quantile(1000.0));
//     println!("quantile of 250.0: {}", kll.quantile(250.0));
//     println!("quantile of 750.0: {}", kll.quantile(750.0));

//     // Test quantiles
//     // let q50 = kll.quantile(0.5);
//     // assert!((q50 - 500.0).abs() < 50.0, "Median should be ~500, got {}", q50);

//     // let q25 = kll.quantile(0.25);
//     // assert!((q25 - 250.0).abs() < 50.0, "25th percentile should be ~250, got {}", q25);

//     // let q75 = kll.quantile(0.75);
//     // assert!((q75 - 750.0).abs() < 50.0, "75th percentile should be ~750, got {}", q75);

//     // let possible_min = kll.cdf().query(0.0);
//     // let possible_max = kll.cdf().query(1.0);

// }

// fn test_cdf() {
//     let mut kll = KLL::init_kll(200);

//     for i in 1..=100 {
//         kll.update(i as f64);
//     }

//     let cdf = kll.cdf();

//     // Test CDF properties
//     assert!(cdf.quantile(0.0) == 0.0);
//     assert!(cdf.quantile(101.0) == 1.0);

//     // Test that CDF is monotonic
//     for i in 1..100 {
//         assert!(cdf.quantile(i as f64) <= cdf.quantile((i + 1) as f64));
//     }
// }

// fn test_merge() {
//     let mut kll1 = KLL::init_kll(200);
//     let mut kll2 = KLL::init_kll(200);

//     // Fill first sketch with 1-50
//     for i in 1..=50 {
//         kll1.update(i as f64);
//     }

//     // Fill second sketch with 51-100
//     for i in 51..=100 {
//         kll2.update(i as f64);
//     }

//     // Merge
//     kll1.merge(&kll2);

//     assert_eq!(kll1.count(), 100);

//     // Check that median is approximately 50
//     let median = kll1.quantile(0.5);
//     assert!((median - 50.0).abs() < 10.0, "Median should be ~50, got {}", median);
// }

// fn test_accuracy_uniform() {
//     let mut rng = StdRng::seed_from_u64(42);
//     let mut kll = KLL::init_kll(200);
//     let n = 10000;

//     let mut values = Vec::new();
//     for _ in 0..n {
//         let v = rng.gen_range(0.0..1000.0);
//         values.push(v);
//         kll.update(v);
//     }

//     // Sort values for exact quantiles
//     values.sort_by(|a, b| a.partial_cmp(b).unwrap());

//     // Test various quantiles
//     for &q in &[0.1, 0.25, 0.5, 0.75, 0.9] {
//         let exact_idx = ((n as f64 * q) as usize).min(n - 1);
//         let exact_value = values[exact_idx];
//         let estimate = kll.quantile(q);

//         let error = (estimate - exact_value).abs() / exact_value;
//         assert!(error < 0.1, "Quantile {} error too large: {} vs {}", q, estimate, exact_value);
//     }
// }

// // fn test_accuracy_normal() {
// //     let mut rng = StdRng::seed_from_u64(42);
// //     let mut kll = KLL::init_kll(200);
// //     let n = 10000;

// //     let mut values = Vec::new();

// //     // Generate normal distribution (Box-Muller transform)
// //     for _ in 0..n/2 {
// //         let u1 = rng.gen_range(0.0001..1.0);
// //         let u2 = rng.gen_range(0.0..1.0);
// //         let z0 = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
// //         let z1 = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).sin();

// //         // Mean 100, std dev 15
// //         let v0 = z0 * 15.0 + 100.0;
// //         let v1 = z1 * 15.0 + 100.0;

// //         values.push(v0);
// //         values.push(v1);
// //         kll.update(v0);
// //         kll.update(v1);
// //     }

// //     values.sort_by(|a, b| a.partial_cmp(b).unwrap());

// //     // Test median
// //     let median = kll.quantile(0.5);
// //     assert!((median - 100.0).abs() < 5.0, "Median should be ~100, got {}", median);
// // }

// fn test_linear_interpolation() {
//     let mut kll = KLL::init_kll(200);

//     // Insert values with gaps
//     for i in 0..10 {
//         kll.update((i * 10) as f64);
//     }

//     let cdf = kll.cdf();

//     // Test interpolation between values
//     let q = cdf.quantile_li(25.0); // Between 20 and 30
//     assert!(q > cdf.quantile(20.0) && q < cdf.quantile(30.0));

//     // Test value interpolation
//     let v = cdf.query_li(0.5); // Should be around 45
//     assert!(v >= 40.0 && v <= 50.0);
// }

// fn test_edge_cases() {
//     let mut kll = KLL::init_kll(200);

//     // Empty sketch
//     assert_eq!(kll.count(), 0);
//     assert_eq!(kll.rank(0.0), 0);

//     // Single element
//     kll.update(42.0);
//     assert_eq!(kll.count(), 1);
//     assert_eq!(kll.rank(41.0), 0);
//     assert_eq!(kll.rank(42.0), 1);
//     assert_eq!(kll.rank(43.0), 1);

//     // Duplicate values
//     let mut kll2 = KLL::init_kll(200);
//     for _ in 0..100 {
//         kll2.update(5.0);
//     }
//     assert_eq!(kll2.count(), 100);
//     assert_eq!(kll2.rank(4.0), 0);
//     assert_eq!(kll2.rank(5.0), 100);
//     assert_eq!(kll2.rank(6.0), 100);
// }

// // fn test_compaction() {
// //     let mut kll = KLL::init_kll(100);

// //     // Force compaction by adding many items
// //     for i in 0..10000 {
// //         kll.update(i as f64);
// //     }

// //     // Should have multiple levels
// //     assert!(kll.compactors.len() > 1, "Should have multiple compactor levels");

// //     // Total count should still be accurate
// //     assert_eq!(kll.count(), 10000);

// //     // Size should be much less than total items
// //     let actual_items: usize = kll.compactors.iter().map(|c| c.items.len()).sum();
// //     assert!(actual_items < 2000, "Should have compacted to fewer items");
// // }

// // fn test_deterministic_compaction() {
// //     // Two sketches with same seed should compact identically
// //     let mut kll1 = KLL::new(100);
// //     let mut kll2 = KLL::new(100);

// //     // Reset their coins to same state
// //     kll1.co = Coin { st: 12345, mask: 0 };
// //     kll2.co = Coin { st: 12345, mask: 0 };

// //     for i in 0..1000 {
// //         kll1.update(i as f64);
// //         kll2.update(i as f64);
// //     }

// //     // Should have same structure
// //     assert_eq!(kll1.compactors.len(), kll2.compactors.len());
// //     for i in 0..kll1.compactors.len() {
// //         assert_eq!(kll1.compactors[i].items.len(), kll2.compactors[i].items.len());
// //     }
// // }

// // fn test_growing() {
// //     let mut kll = KLL::init_kll(50);
// //     let initial_levels = kll.compactors.len();

// //     // Add enough items to force growing
// //     for i in 0..5000 {
// //         kll.update(i as f64);
// //     }

// //     assert!(kll.compactors.len() > initial_levels, "Should have grown");
// //     assert_eq!(kll.compactors.len(), kll.h as usize);
// // }

// fn test_basic_operations() {
//     println!("Test 1: Basic Operations");
//     println!("------------------------");

//     let mut cm = CountMin::init_count_min();

//     // Insert some values
//     for i in 1..11 {
//         for _ in 0..i {
//             cm.insert_cm(&i);
//             cm.get_est(&i);
//         }
//     }

//     for i in 1..11 {
//         let estimate = cm.get_est(&i);
//         println!("Estimate for {} is: {}", i, estimate);
//     }
//     // cm.debug();

//     // println!("After inserting 0-9:");
//     // println!("  Actual: 10");
//     // println!("  Estimate: {}", estimate);
//     // println!("  Error: {:.2}%\n", ((estimate as f64 - 10.0) / 10.0 * 100.0).abs());
// }

// // fn test_accuracy() {
// //     println!("Test 2: Accuracy at Different Cardinalities");
// //     println!("-------------------------------------------");

// //     let test_sizes = vec![100, 1000, 10000, 100000];

// //     for size in test_sizes {
// //         let mut hll = HLL::init_hll();

// //         for i in 0..size {
// //             hll.insert_hll(&i);
// //         }

// //         let estimate = hll.calculate_est();
// //         let error_rate = ((estimate as f64 - size as f64) / size as f64 * 100.0).abs();

// //         println!("Cardinality: {}", size);
// //         println!("  Estimate: {}", estimate);
// //         println!("  Error: {:.2}%", error_rate);
// //     }
// //     println!();
// // }

// // fn test_merge_operation() {
// //     println!("Test 3: Merge Operation");
// //     println!("-----------------------");

// //     let mut hll1 = HLL::init_hll();
// //     let mut hll2 = HLL::init_hll();

// //     // Insert different ranges into each HLL
// //     for i in 0..5000 {
// //         hll1.insert_hll(&i);
// //     }

// //     for i in 5000..10000 {
// //         hll2.insert_hll(&i);
// //     }

// //     let estimate1 = hll1.calculate_est();
// //     let estimate2 = hll2.calculate_est();

// //     println!("HLL1 (0-4999): {}", estimate1);
// //     println!("HLL2 (5000-9999): {}", estimate2);

// //     // Merge HLL2 into HLL1
// //     hll1.merge_hll(&hll2);
// //     let merged_estimate = hll1.calculate_est();

// //     println!("Merged HLL (0-9999):");
// //     println!("  Actual: 10000");
// //     println!("  Estimate: {}", merged_estimate);
// //     println!("  Error: {:.2}%\n", ((merged_estimate as f64 - 10000.0) / 10000.0 * 100.0).abs());
// // }

// // fn test_large_cardinality() {
// //     println!("Test 4: Large Cardinality");
// //     println!("-------------------------");

// //     let mut hll = HLL::init_hll();
// //     let size = 1_000_000;

// //     // Insert a million unique values
// //     for i in 0..size {
// //         hll.insert_hll(&format!("item_{}", i));
// //     }

// //     let estimate = hll.calculate_est();
// //     let error_rate = ((estimate as f64 - size as f64) / size as f64 * 100.0).abs();

// //     println!("Cardinality: {}", size);
// //     println!("Estimate: {}", estimate);
// //     println!("Error: {:.2}%", error_rate);
// //     println!("(Note: HLL with m=32 has high standard error ~20%)\n");
// // }

// // fn test_duplicate_handling() {
// //     println!("Test 5: Duplicate Handling");
// //     println!("--------------------------");

// //     let mut hll = HLL::init_hll();
// //     let mut exact_set = HashSet::new();

// //     // Insert values with duplicates
// //     let values = vec![1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

// //     for val in &values {
// //         hll.insert_hll(val);
// //         exact_set.insert(val);
// //     }

// //     println!("Inserted values: {:?}", values);
// //     println!("Unique count: {}", exact_set.len());
// //     println!("HLL estimate: {}", hll.calculate_est());

// //     // Test with string duplicates
// //     let mut hll_str = HLL::init_hll();
// //     let strings = vec!["apple", "banana", "apple", "cherry", "banana", "date"];

// //     for s in &strings {
// //         hll_str.insert_hll(s);
// //     }

// //     println!("\nString test:");
// //     println!("Inserted: {:?}", strings);
// //     println!("Unique count: 4");
// //     println!("HLL estimate: {}", hll_str.calculate_est());
// // }
