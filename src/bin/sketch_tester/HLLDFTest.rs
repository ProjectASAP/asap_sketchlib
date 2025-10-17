use querysimulation::sketches::hll::HLLDataFusion;
use std::collections::HashSet;

fn main() {
    println!("=== HLLHIP Test Suite ===\n");

    test_basic_operations();
    test_accuracy();
    test_merge_operation();
    test_large_cardinality();
    test_duplicate_handling();
}

fn test_basic_operations() {
    println!("Test 1: Basic Operations");
    println!("------------------------");

    let mut hll = HLLDataFusion::default();
    println!("Initial estimate (empty): {}", hll.count());

    // Insert some values
    for i in 0..10 {
        hll.add(&i);
    }

    let estimate = hll.count();

    println!("After inserting 0-9:");
    println!("  Actual: 10");
    println!("  Estimate: {}", estimate);
    println!(
        "  Error: {:.2}%\n",
        ((estimate as f64 - 10.0) / 10.0 * 100.0).abs()
    );
}

fn test_accuracy() {
    println!("Test 2: Accuracy at Different Cardinalities");
    println!("-------------------------------------------");

    let test_sizes = vec![100, 1000, 10000, 100000];

    for size in test_sizes {
        let mut hll = HLLDataFusion::default();

        for i in 0..size {
            hll.add(&i);
        }

        let estimate = hll.count();
        let error_rate = ((estimate as f64 - size as f64) / size as f64 * 100.0).abs();

        println!("Cardinality: {}", size);
        println!("  Estimate: {}", estimate);
        println!("  Error: {:.2}%", error_rate);
    }
    println!();
}

fn test_merge_operation() {
    println!("Test 3: Merge Operation");
    println!("-----------------------");

    let mut hll1 = HLLDataFusion::default();
    let mut hll2 = HLLDataFusion::default();

    // Insert different ranges into each HLL
    for i in 0..5000 {
        hll1.add(&i);
    }

    for i in 5000..10000 {
        hll2.add(&i);
    }

    let estimate1 = hll1.count();
    let estimate2 = hll2.count();

    println!("HLL1 (0-4999): {}", estimate1);
    println!("HLL2 (5000-9999): {}", estimate2);

    // Merge HLL2 into HLL1
    hll1.merge(&hll2);
    let merged_estimate = hll1.count();

    println!("Merged HLL (0-9999):");
    println!("  Actual: 10000");
    println!("  Estimate: {}", merged_estimate);
    println!(
        "  Error: {:.2}%\n",
        ((merged_estimate as f64 - 10000.0) / 10000.0 * 100.0).abs()
    );
}

fn test_large_cardinality() {
    println!("Test 4: Large Cardinality");
    println!("-------------------------");

    let mut hll = HLLDataFusion::default();
    let size = 5_000_000;

    // Insert a million unique values
    for i in 0..size {
        hll.add(&format!("item_{}", i));
    }

    let estimate = hll.count();
    let error_rate = ((estimate as f64 - size as f64) / size as f64 * 100.0).abs();

    println!("Cardinality: {}", size);
    println!("Estimate: {}", estimate);
    println!("Error: {:.2}%", error_rate);
}

fn test_duplicate_handling() {
    println!("Test 5: Duplicate Handling");
    println!("--------------------------");

    let mut hll = HLLDataFusion::default();
    let mut exact_set = HashSet::new();

    // Insert values with duplicates
    let values = vec![1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

    for val in &values {
        hll.add(val);
        exact_set.insert(val);
    }

    println!("Inserted values: {:?}", values);
    println!("Unique count: {}", exact_set.len());
    println!("HLL estimate: {}", hll.count());

    // Test with string duplicates
    let mut hll_str = HLLDataFusion::default();
    let strings = vec!["apple", "banana", "apple", "cherry", "banana", "date"];

    for s in &strings {
        hll_str.add(s);
    }

    println!("\nString test:");
    println!("Inserted: {:?}", strings);
    println!("Unique count: 4");
    println!("HLL estimate: {}", hll_str.count());
}
