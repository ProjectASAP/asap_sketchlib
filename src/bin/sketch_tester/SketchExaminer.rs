use querysimulation::sketches::{
    CountMin, CountUniv, HllDfModified, KLL, UnivMon, utils::LASTSTATE, utils::SketchInput,
    utils::hash_it,
};
use std::collections::HashSet;

fn main() {
    println!("=== Test Suites ===\n\n");

    println!("--- Hll (Trivially Modified From DataFusion) ---\n");
    test_basic_operations_hll();
    test_accuracy_hll();
    test_merge_operation_hll();
    test_large_cardinality_hll();
    test_duplicate_handling_hll();

    println!("--- CountMin ---\n");
    test_basic_operations_cm();
    test_accuracy_cm();
    test_merge_operation_cm();
    test_large_cardinality_cm();
    test_duplicate_handling_cm();

    println!("--- CountUniv ---\n");
    test_basic_operations_cu();
    test_accuracy_cu();
    test_merge_operation_cu();
    test_large_cardinality_cu();
    test_duplicate_handling_cu();

    println!("--- KLL ---\n");
    test_basic_operations_kll();
    test_accuracy_kll();
    test_merge_operation_kll();
    test_large_cardinality_kll();
    test_duplicate_handling_kll();

    println!("--- UnivMon ---\n");
    test_basic_operations_um();
    test_accuracy_um();
    test_merge_operation_um();
    test_large_cardinality_um();
    test_duplicate_handling_um();
}

fn test_basic_operations_hll() {
    println!("Test 1: Basic Operations");
    println!("------------------------");

    let mut hll = HllDfModified::default();
    println!("Initial estimate (empty): {}", hll.get_est());

    // Insert some values
    for i in 0..10 {
        // hll.insert(&i);
        hll.insert(&SketchInput::I32(i));
    }

    let estimate = hll.get_est();

    println!("After inserting 0-9:");
    println!("  Actual: 10");
    println!("  Estimate: {}", estimate);
    println!(
        "  Error: {:.2}%\n",
        ((estimate as f64 - 10.0) / 10.0 * 100.0).abs()
    );
}

fn test_accuracy_hll() {
    println!("Test 2: Accuracy at Different Cardinalities");
    println!("-------------------------------------------");

    let test_sizes = vec![100, 1000, 10000, 100000];

    for size in test_sizes {
        let mut hll = HllDfModified::default();

        for i in 0..size {
            // hll.insert(&i);
            hll.insert(&SketchInput::I32(i));
        }

        let estimate = hll.get_est();
        let error_rate = ((estimate as f64 - size as f64) / size as f64 * 100.0).abs();

        println!("Cardinality: {}", size);
        println!("  Estimate: {}", estimate);
        println!("  Error: {:.2}%", error_rate);
    }
    println!();
}

fn test_merge_operation_hll() {
    println!("Test 3: Merge Operation");
    println!("-----------------------");

    let mut hll1 = HllDfModified::default();
    let mut hll2 = HllDfModified::default();

    // Insert different ranges into each HLL
    for i in 0..5000 {
        // hll1.insert(&i);
        hll1.insert(&SketchInput::I32(i));
    }

    for i in 5000..10000 {
        // hll2.insert(&i);
        hll2.insert(&SketchInput::I32(i));
    }

    let estimate1 = hll1.get_est();
    let estimate2 = hll2.get_est();

    println!("HLL1 (0-4999): {}", estimate1);
    println!("HLL2 (5000-9999): {}", estimate2);

    // Merge HLL2 into HLL1
    hll1.merge(&hll2);
    let merged_estimate = hll1.get_est();

    println!("Merged HLL (0-9999):");
    println!("  Actual: 10000");
    println!("  Estimate: {}", merged_estimate);
    println!(
        "  Error: {:.2}%\n",
        ((merged_estimate as f64 - 10000.0) / 10000.0 * 100.0).abs()
    );
}

fn test_large_cardinality_hll() {
    println!("Test 4: Large Cardinality");
    println!("-------------------------");

    let mut hll = HllDfModified::default();
    let size = 5_000_000;

    // Insert a million unique values
    for i in 0..size {
        // hll.insert(&format!("item_{}", i));
        hll.insert(&SketchInput::String(format!("item_{}", i)));
    }

    let estimate = hll.get_est();
    let error_rate = ((estimate as f64 - size as f64) / size as f64 * 100.0).abs();

    println!("Cardinality: {}", size);
    println!("Estimate: {}", estimate);
    println!("Error: {:.2}%", error_rate);
}

fn test_duplicate_handling_hll() {
    println!("Test 5: Duplicate Handling");
    println!("--------------------------");

    let mut hll = HllDfModified::default();
    let mut exact_set = HashSet::new();

    // Insert values with duplicates
    let values = vec![1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

    for val in &values {
        // hll.insert(val);
        hll.insert(&SketchInput::I32(*val));
        exact_set.insert(val);
    }

    println!("Inserted values: {:?}", values);
    println!("Unique count: {}", exact_set.len());
    println!("HLL estimate: {}", hll.get_est());

    // Test with string duplicates
    let mut hll_str = HllDfModified::default();
    let strings = vec!["apple", "banana", "apple", "cherry", "banana", "date"];

    for s in &strings {
        // hll_str.insert(s);
        hll_str.insert(&SketchInput::Str(*s));
    }

    println!("\nString test:");
    println!("Inserted: {:?}", strings);
    println!("Unique count: 4");
    println!("HLL estimate: {}", hll_str.get_est());
}

fn test_basic_operations_cm() {
    println!("Test 1: Basic Operations");
    println!("------------------------");
    let mut cm = CountMin::init_count_min();

    // Insert some values
    for i in 1..11 {
        for _ in 0..i {
            // cm.insert_cm(&i);
            // cm.get_est(&i);
            cm.insert_cm(&SketchInput::I32(i));
            cm.get_est(&SketchInput::I32(i));
        }
    }
    let mut correct_count = 0;
    for i in 1..11 {
        // let estimate = cm.get_est(&i);
        let estimate = cm.get_est(&SketchInput::I32(i)) as i32;
        // println!("Estimate for {} is: {} while should be: {}", i, estimate, i);
        if i == estimate {
            correct_count += 1;
        }
    }
    println!("Correct count: {}", correct_count);
    println!(
        "Correct rate: {:.2}%\n",
        (correct_count as f64 / 10.0) * 100.0
    );
}

fn test_accuracy_cm() {
    println!("Test 2: Accuracy at Different Input Level");
    println!("-------------------------------------------");

    // let test_sizes = vec![100, 1000, 10000, 100000];
    let test_sizes = vec![50];

    for size in test_sizes {
        let mut cm = CountMin::init_count_min();
        // println!("before: ");
        // cm.debug();
        // Insert some values
        for i in 1..=size {
            for _ in 0..i {
                // cm.insert_cm(&i);
                // cm.get_est(&i);
                cm.insert_cm(&SketchInput::I32(i));
                cm.get_est(&SketchInput::I32(i));
            }
        }
        let mut correct_count = 0;
        for i in 1..=size {
            // let estimate = cm.get_est(&i);
            let estimate = cm.get_est(&SketchInput::I32(i)) as i32;
            // println!("Estimate for {} is: {} while should be: {}", i, estimate, i);
            if i == estimate {
                correct_count += 1;
            }
        }
        println!("Correct count: {}", correct_count);
        println!(
            "Correct rate: {:.2}%\n",
            (correct_count as f64 / size as f64) * 100.0
        );
        // println!("after: ");
        // cm.debug();
    }
    println!();
}
fn test_merge_operation_cm() {
    println!("Test 3: Merge Operation");
    println!("-----------------------");

    let mut cm1 = CountMin::init_count_min();
    let mut cm2 = CountMin::init_count_min();

    // Insert different values into each CountMin
    for i in 1..11 {
        for _ in 0..i {
            // cm1.insert_cm(&i);
            cm1.insert_cm(&SketchInput::I32(i));
        }
    }

    for i in 11..21 {
        for _ in 0..i {
            // cm2.insert_cm(&i);
            cm2.insert_cm(&SketchInput::I32(i));
        }
    }

    cm1.merge(&cm2);

    let mut correct_count = 0;
    for i in 1..21 {
        // let estimate = cm1.get_est(&i);
        let estimate = cm1.get_est(&SketchInput::I32(i)) as i32;
        if i == estimate {
            correct_count += 1;
        }
    }
    println!(
        "Merged CountMin correct rate: {:.2}%\n",
        (correct_count as f64 / 20.0) * 100.0
    );
}

fn test_large_cardinality_cm() {
    println!("Test 4: Large Cardinality");
    println!("--------------------------");

    let mut cm = CountMin::init_count_min();

    // Insert large number of unique values
    for i in 1..=1000 {
        for _ in 0..i {
            // cm.insert_cm(&format!("item_{}", i));
            cm.insert_cm(&SketchInput::Str(&format!("item_{}", i)));
        }
    }

    let mut correct_count = 0;
    for i in 1..=100 {
        // Test subset for performance
        // let estimate = cm.get_est(&format!("item_{}", i));
        let estimate = cm.get_est(&SketchInput::Str(&format!("item_{}", i)));
        if i as u64 == estimate {
            correct_count += 1;
        }
    }
    println!(
        "Large cardinality test correct rate: {:.2}%\n",
        (correct_count as f64 / 100.0) * 100.0
    );
}

fn test_duplicate_handling_cm() {
    println!("Test 5: Duplicate Handling");
    println!("--------------------------");

    let mut cm = CountMin::init_count_min();

    // Insert values with duplicates
    let values = vec!["apple", "banana", "apple", "cherry", "banana", "apple"];
    let expected_counts = [("apple", 3), ("banana", 2), ("cherry", 1)];

    for val in &values {
        // cm.insert_cm(val);
        cm.insert_cm(&SketchInput::Str(*val));
    }

    for (key, expected) in expected_counts {
        // let estimate = cm.get_est(&key);
        let estimate = cm.get_est(&SketchInput::Str(key));
        println!(
            "Key: {}, Expected: {}, Estimate: {}",
            key, expected, estimate
        );
    }
    println!();
}

// CountUniv tests
fn test_basic_operations_cu() {
    println!("Test 1: Basic Operations");
    println!("------------------------");

    let mut cu = CountUniv::default();

    // Insert some values with counts
    for i in 1..11 {
        // cu.insert_with_count(&i, i as i64);
        cu.insert_with_count(&SketchInput::I32(i), i as i64);
    }

    let mut correct_count = 0;
    for i in 1..11 {
        // let estimate = cu.get_est(&i);
        let estimate = cu.get_est(&SketchInput::I32(i));
        // println!("Estimate for {} is: {:.1} while should be: {}", i, estimate, i);
        if (estimate - i as f64).abs() < 1.0 {
            correct_count += 1;
        }
    }
    println!("Correct count: {}", correct_count);
    println!(
        "Correct rate: {:.2}%",
        (correct_count as f64 / 10.0) * 100.0
    );
    println!("L2 norm: {:.2}\n", cu.get_l2());
}

fn test_accuracy_cu() {
    println!("Test 2: Accuracy at Different Input Level");
    println!("-------------------------------------------");

    let test_sizes = vec![10, 50, 100];

    for size in test_sizes {
        let mut cu = CountUniv::default();

        for i in 1..=size {
            // cu.insert_with_count(&i, i as i64);
            cu.insert_with_count(&SketchInput::I32(i), i as i64);
        }

        let mut correct_count = 0;
        for i in 1..=size {
            // let estimate = cu.get_est(&i);
            let estimate = cu.get_est(&SketchInput::I32(i));
            if (estimate - i as f64).abs() < 2.0 {
                correct_count += 1;
            }
        }

        println!(
            "Size: {}, Correct rate: {:.2}%, L2: {:.2}",
            size,
            (correct_count as f64 / size as f64) * 100.0,
            cu.get_l2()
        );
    }
    println!();
}

fn test_merge_operation_cu() {
    println!("Test 3: Merge Operation");
    println!("-----------------------");

    let mut cu1 = CountUniv::init_count();
    let mut cu2 = CountUniv::init_count();

    for i in 1..11 {
        // cu1.insert_with_count(&i, i as i64);
        cu1.insert_with_count(&SketchInput::I64(i), i);
    }

    for i in 1..11 {
        // cu2.insert_with_count(&i, i as i64);
        cu2.insert_with_count(&SketchInput::I64(i), i);
    }

    cu1.merge(&cu2);

    let mut correct_count = 0;
    for i in 1..11 {
        // let estimate = cu1.get_est(&i);
        let estimate = cu1.get_est(&SketchInput::I64(i));
        let expected = 2 * i; // Each value was inserted in both sketches
        if (estimate - expected as f64).abs() < 2.0 {
            correct_count += 1;
        }
    }
    println!(
        "Merged CountUniv correct rate: {:.2}%\n",
        (correct_count as f64 / 10.0) * 100.0
    );
}

fn test_large_cardinality_cu() {
    println!("Test 4: Large Cardinality");
    println!("--------------------------");

    let mut cu = CountUniv::default();

    for i in 1..=500 {
        // cu.insert_with_count(&format!("item_{}", i), i as i64);
        cu.insert_with_count(&SketchInput::String(format!("item_{}", i)), i as i64);
    }

    let mut correct_count = 0;
    for i in 1..=50 {
        // Test subset for performance
        // let estimate = cu.get_est(&format!("item_{}", i));
        let estimate = cu.get_est(&SketchInput::String(format!("item_{}", i)));
        if (estimate - i as f64).abs() < 3.0 {
            correct_count += 1;
        }
    }
    println!(
        "Large cardinality test correct rate: {:.2}%\n",
        (correct_count as f64 / 50.0) * 100.0
    );
}

fn test_duplicate_handling_cu() {
    println!("Test 5: Duplicate Handling");
    println!("--------------------------");

    let mut cu = CountUniv::default();

    let values = vec![
        ("apple", 3),
        ("banana", 2),
        ("cherry", 5),
        ("apple", 1),
        ("banana", 3),
    ];
    let expected_counts = [("apple", 4), ("banana", 5), ("cherry", 5)];

    for (key, count) in &values {
        // cu.insert_with_count(key, *count);
        cu.insert_with_count(&SketchInput::Str(*key), *count);
    }

    for (key, expected) in expected_counts {
        // let estimate = cu.get_est(&key);
        let estimate = cu.get_est(&SketchInput::Str(key));
        println!(
            "Key: {}, Expected: {}, Estimate: {:.1}",
            key, expected, estimate
        );
    }
    println!();
}

// KLL tests
fn test_basic_operations_kll() {
    println!("Test 1: Basic Operations");
    println!("------------------------");

    let mut kll = KLL::init_kll(128);

    // Insert values
    for i in 0..100 {
        kll.update(i as f64);
    }

    let count = kll.count();
    let median = kll.cdf().query(0.5);
    let p90 = kll.cdf().query(0.9);

    println!("Inserted 0-99 (100 values)");
    println!("Count: {}", count);
    println!("Median (50th percentile): {:.1}", median);
    println!("90th percentile: {:.1}", p90);
    println!("Expected median: ~49.5, Expected p90: ~89.1\n");
}

fn test_accuracy_kll() {
    println!("Test 2: Accuracy at Different Input Level");
    println!("-------------------------------------------");

    let test_sizes = vec![100, 500, 1000];

    for size in test_sizes {
        let mut kll = KLL::init_kll(128);

        for i in 0..size {
            kll.update(i as f64);
        }

        let count = kll.count();
        let median = kll.cdf().query(0.5);
        let expected_median = (size - 1) as f64 / 2.0;
        let error = ((median - expected_median) / expected_median * 100.0).abs();

        println!(
            "Size: {}, Count: {}, Median: {:.1}, Expected: {:.1}, Error: {:.2}%",
            size, count, median, expected_median, error
        );
    }
    println!();
}

fn test_merge_operation_kll() {
    println!("Test 3: Merge Operation");
    println!("-----------------------");

    let mut kll1 = KLL::init_kll(128);
    let mut kll2 = KLL::init_kll(128);

    // Insert different ranges
    for i in 0..50 {
        kll1.update(i as f64);
    }

    for i in 50..100 {
        kll2.update(i as f64);
    }

    kll1.merge(&kll2);

    let count = kll1.count();
    let median = kll1.cdf().query(0.5);

    println!("Merged KLL (0-99)");
    println!("Count: {}, Median: {:.1}", count, median);
    println!("Expected count: 100, Expected median: ~49.5\n");
}

fn test_large_cardinality_kll() {
    println!("Test 4: Large Cardinality");
    println!("--------------------------");

    let mut kll = KLL::init_kll(256);

    for i in 0..10000 {
        kll.update(i as f64);
    }

    let count = kll.count();
    let median = kll.cdf().query(0.5);
    let p95 = kll.cdf().query(0.95);

    println!("Large dataset (0-9999)");
    println!("Count: {}, Median: {:.1}, P95: {:.1}", count, median, p95);
    println!("Expected median: ~4999.5, Expected P95: ~9499.05\n");
}

fn test_duplicate_handling_kll() {
    println!("Test 5: Duplicate Handling");
    println!("--------------------------");

    let mut kll = KLL::init_kll(128);

    let values = vec![1.0, 2.0, 3.0, 1.0, 2.0, 3.0, 4.0, 5.0];

    for val in &values {
        kll.update(*val);
    }

    let count = kll.count();
    let median = kll.cdf().query(0.5);

    println!("Inserted values: {:?}", values);
    println!("Count: {}, Median: {:.1}", count, median);
    println!();
}

// UnivMon tests
fn test_basic_operations_um() {
    println!("Test 1: Basic Operations");
    println!("------------------------");

    let mut um = UnivMon::init_univmon(100, 3, 2048, 8, -1);

    let test_data = vec![
        ("hello".to_string(), 5),
        ("world".to_string(), 10),
        ("count".to_string(), 3),
        ("sketch".to_string(), 8),
    ];

    for (key, value) in &test_data {
        // let h = hash_it(LASTSTATE, key);
        let h = hash_it(LASTSTATE, &SketchInput::Str(key));
        let bln = um.find_bottom_layer_num(h, 8);
        um.univmon_processing(key, *value, bln);
    }

    let card = um.calc_card();
    let l1 = um.calc_l1();
    let l2 = um.calc_l2();
    let entropy = um.calc_entropy();

    println!("Cardinality: {:.2}", card);
    println!("L1 norm: {:.2}", l1);
    println!("L2 norm: {:.2}", l2);
    println!("Entropy: {:.2}", entropy);
    println!("Bucket size: {}\n", um.get_bucket_size());
}

fn test_accuracy_um() {
    println!("Test 2: Accuracy at Different Input Level");
    println!("-------------------------------------------");

    let test_sizes = vec![10, 50, 100];

    for size in test_sizes {
        let mut um = UnivMon::init_univmon(100, 3, 2048, 8, -1);

        for i in 0..size {
            let key = format!("item_{}", i);
            let value = i + 1;
            // let h = hash_it(LASTSTATE, &key);
            let h = hash_it(LASTSTATE, &SketchInput::I64(i));
            let bln = um.find_bottom_layer_num(h, 8);
            um.univmon_processing(&key, value, bln);
        }

        let card = um.calc_card();
        let l1 = um.calc_l1();
        let expected_l1: i64 = (1..=size).sum();
        let error = ((l1 - expected_l1 as f64) / expected_l1 as f64 * 100.0).abs();

        println!(
            "Size: {}, Card: {:.1}, L1: {:.1}, Expected L1: {}, Error: {:.2}%",
            size, card, l1, expected_l1, error
        );
    }
    println!();
}

fn test_merge_operation_um() {
    println!("Test 3: Merge Operation");
    println!("-----------------------");

    let mut um1 = UnivMon::init_univmon(100, 3, 2048, 8, -1);
    let mut um2 = UnivMon::init_univmon(100, 3, 2048, 8, -1);

    // Insert different data into each UnivMon
    for i in 0..25 {
        let key = format!("item_{}", i);
        let value = i + 1;
        // let h = hash_it(LASTSTATE, &key);
        let h = hash_it(LASTSTATE, &SketchInput::Str(&key));
        let bln = um1.find_bottom_layer_num(h, 8);
        um1.univmon_processing(&key, value, bln);
    }

    for i in 25..50 {
        let key = format!("item_{}", i);
        let value = i + 1;
        // let h = hash_it(LASTSTATE, &key);
        let h = hash_it(LASTSTATE, &SketchInput::Str(&key));
        let bln = um2.find_bottom_layer_num(h, 8);
        um2.univmon_processing(&key, value, bln);
    }

    let card1 = um1.calc_card();
    let card2 = um2.calc_card();

    um1.merge_with(&um2);

    let merged_card = um1.calc_card();
    let merged_l1 = um1.calc_l1();

    println!("UM1 card: {:.1}, UM2 card: {:.1}", card1, card2);
    println!(
        "Merged card: {:.1}, Merged L1: {:.1}",
        merged_card, merged_l1
    );
    println!(
        "Expected merged card: ~50, Expected L1: {}\n",
        (1..=50).sum::<i64>()
    );
}

fn test_large_cardinality_um() {
    println!("Test 4: Large Cardinality");
    println!("--------------------------");

    let mut um = UnivMon::init_univmon(200, 3, 2048, 8, -1);

    for i in 0..1000 {
        let key = format!("large_item_{}", i);
        let value = (i % 10) + 1; // Values 1-10
        // let h = hash_it(LASTSTATE, &key);
        let h = hash_it(LASTSTATE, &SketchInput::Str(&key));
        let bln = um.find_bottom_layer_num(h, 8);
        um.univmon_processing(&key, value, bln);
    }

    let card = um.calc_card();
    let l1 = um.calc_l1();
    let l2 = um.calc_l2();
    let entropy = um.calc_entropy();

    println!("Large dataset (1000 items)");
    println!("Cardinality: {:.1}", card);
    println!("L1 norm: {:.1}", l1);
    println!("L2 norm: {:.1}", l2);
    println!("Entropy: {:.2}\n", entropy);
}

fn test_duplicate_handling_um() {
    println!("Test 5: Duplicate Handling");
    println!("--------------------------");

    let mut um = UnivMon::init_univmon(100, 3, 2048, 8, -1);

    let test_cases = vec![
        ("apple".to_string(), 3),
        ("banana".to_string(), 2),
        ("apple".to_string(), 1), // Duplicate
        ("cherry".to_string(), 5),
        ("banana".to_string(), 3), // Duplicate
    ];

    for (key, value) in &test_cases {
        // let h = hash_it(LASTSTATE, key);
        let h = hash_it(LASTSTATE, &SketchInput::Str(key));
        let bln = um.find_bottom_layer_num(h, 8);
        um.univmon_processing(key, *value, bln);
    }

    let card = um.calc_card();
    let l1 = um.calc_l1();

    println!("Test cases: {:?}", test_cases);
    println!("Cardinality: {:.1} (expected ~3)", card);
    println!(
        "L1 norm: {:.1} (expected 14: apple=4, banana=5, cherry=5)",
        l1
    );
    println!();
}
