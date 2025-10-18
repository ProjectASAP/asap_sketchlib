use sketchlib_rust::Chapter;
use sketchlib_rust::sketchbook::ExponentialHistogram;
use sketchlib_rust::sketches::hll::HllDfModified;
use sketchlib_rust::sketches::utils::SketchInput;
use std::fs;
use std::path::PathBuf;

fn main() {
    println!("=== Exponential Histogram Test Suite ===\n");

    test_basic_functionality();
    test_window_sliding();
    test_merging_behavior();
    test_query_operations();
    test_with_real_data();

    println!("\n=== All tests completed ===");
}

fn test_basic_functionality() {
    println!("Test 1: Basic Functionality");
    println!("---------------------------");

    let mut eh: ExponentialHistogram =
        ExponentialHistogram::new(2, 1000, Chapter::HLL(HllDfModified::default()));
    println!("Created ExponentialHistogram with k=4, window=1000");

    // Insert some buckets
    for i in 0..5 {
        eh.update(i * 100, &SketchInput::U64(i));
    }

    println!("Inserted 5 buckets at times: 0, 100, 200, 300, 400");
    println!("Bucket count: {}", eh.volume_count());
    println!("Min time: {:?}", eh.get_min_time());
    println!("Max time: {:?}", eh.get_max_time());

    eh.print_buckets();
    println!();
}

fn test_window_sliding() {
    println!("Test 2: Window Sliding (Expiration)");
    println!("------------------------------------");

    let mut eh: ExponentialHistogram =
        ExponentialHistogram::new(2, 200, Chapter::HLL(HllDfModified::default()));
    println!("Created ExponentialHistogram with k=4, window=200");

    // Insert buckets at times 0, 50, 100, 150, 200
    for i in 0..5 {
        eh.update(i * 50, &SketchInput::U64(i));
        println!(
            "After inserting at time {}: bucket_count={}",
            i * 50,
            eh.volume_count()
        );
    }

    println!("\nBefore expiration:");
    eh.print_buckets();

    // Insert at time 300 - this should expire buckets with max_time < 300 - 200 = 100
    eh.update(300, &SketchInput::U64(300));

    println!("\nAfter inserting at time 300 (window=200, expires < 100):");
    eh.print_buckets();
    println!("Bucket count: {}", eh.volume_count());
    println!("Min time: {:?}", eh.get_min_time());
    println!("Max time: {:?}", eh.get_max_time());
    println!();
}

fn test_merging_behavior() {
    println!("Test 3: Bucket Merging Behavior");
    println!("--------------------------------");

    // Use small k to trigger merging
    let mut eh: ExponentialHistogram =
        ExponentialHistogram::new(2, 1000, Chapter::HLL(HllDfModified::default()));
    println!("Created ExponentialHistogram with k=2, window=10000");
    println!("With k=2, buckets merge when there are >= k/2 + 2 = 3 buckets of same size\n");

    // Insert buckets one by one and observe merging
    for i in 0..12 {
        eh.update(i * 10, &SketchInput::U64(i));

        println!(
            "After insert #{}: bucket_count={}",
            i + 1,
            eh.volume_count()
        );
        let (_, sizes) = eh.get_memory_info();
        println!("  Bucket sizes: {:?}", sizes);
    }

    println!("\nFinal state:");
    eh.print_buckets();
    println!();
}

fn test_query_operations() {
    println!("Test 4: Query Operations");
    println!("------------------------");

    let mut eh: ExponentialHistogram =
        ExponentialHistogram::new(4, 10000, Chapter::HLL(HllDfModified::default()));

    // Insert buckets with known data
    for i in 0..10 {
        eh.update(i * 100, &SketchInput::U64(i));
    }

    println!("Inserted 10 buckets with time series data");
    println!("Bucket count: {}", eh.volume_count());
    eh.print_buckets();

    // Test cover functionality
    println!("\nTesting cover functionality:");
    println!("cover(0, 900): {}", eh.cover(0, 900));
    println!("cover(100, 800): {}", eh.cover(100, 800));
    println!("cover(0, 1000): {}", eh.cover(0, 1000));
    println!("cover(1000, 2000): {}", eh.cover(1000, 2000));

    // Test query_interval_merge
    println!("\nTesting query_interval_merge:");
    let result = eh.query_interval_merge(200, 500);
    match result {
        Some(chapter) => {
            println!("Query [200, 500]: Success");
            if let Chapter::HLL(hll) = chapter {
                println!("  Estimated cardinality: {}", hll.get_est());
            }
        }
        None => println!("Query [200, 500]: No data"),
    }

    let result2 = eh.query_interval_merge(0, 900);
    match result2 {
        Some(chapter) => {
            println!("Query [0, 900]: Success");
            if let Chapter::HLL(h) = chapter {
                println!("  Merged Cardinality: {}", h.get_est());
            }
        }
        None => println!("Query [0, 900]: No data"),
    }

    println!();
}

fn test_with_real_data() {
    println!("Test 5: With Real Prometheus Metrics Data");
    println!("------------------------------------------");

    // Find all .timestamp files in testdata directory
    let testdata_path = PathBuf::from("testdata");

    if !testdata_path.exists() {
        println!("Testdata directory not found, skipping this test");
        return;
    }

    let mut timestamp_files: Vec<_> = fs::read_dir(&testdata_path)
        .expect("Failed to read testdata directory")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("timestamp"))
        .collect();

    // Sort by filename (timestamp)
    timestamp_files.sort_by_key(|entry| {
        entry
            .path()
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
    });

    if timestamp_files.is_empty() {
        println!("No .timestamp files found in testdata directory");
        return;
    }

    println!("Found {} timestamp files", timestamp_files.len());

    // Create EH with appropriate window
    let window_size = 5000; // Adjust based on your data
    let mut eh: ExponentialHistogram =
        ExponentialHistogram::new(4, window_size, Chapter::HLL(HllDfModified::default()));

    let mut processed = 0;
    let max_files = 20.min(timestamp_files.len()); // Process first 20 files

    for entry in timestamp_files.iter().take(max_files) {
        let path = entry.path();
        let timestamp = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        // Read the file content
        if let Ok(content) = fs::read_to_string(&path) {
            // Parse prometheus metrics and extract values
            for line in content.lines() {
                if line.starts_with('#') || line.trim().is_empty() {
                    continue;
                }

                // Parse the metric value (last token)
                if let Some(value_str) = line.split_whitespace().last() {
                    if let Ok(value) = value_str.parse::<f64>() {
                        eh.update(timestamp, &SketchInput::F64(value));
                    }
                }
            }
            processed += 1;

            if processed % 5 == 0 {
                println!(
                    "Processed {} files, current bucket count: {}",
                    processed,
                    eh.volume_count()
                );
            }
        }
    }

    println!("\nFinal state after processing {} files:", processed);
    println!("Bucket count: {}", eh.volume_count());
    println!(
        "Time range: {:?} to {:?}",
        eh.get_min_time(),
        eh.get_max_time()
    );

    let (count, sizes) = eh.get_memory_info();
    println!("Bucket details - count: {}, sizes: {:?}", count, sizes);

    eh.print_buckets();

    // Test querying
    if let (Some(min_t), Some(max_t)) = (eh.get_min_time(), eh.get_max_time()) {
        let mid_t = (min_t + max_t) / 2;
        println!("\nQuerying middle range [{}, {}]:", min_t, mid_t);

        if let Some(chapter) = eh.query_interval_merge(min_t, mid_t) {
            if let Chapter::HLL(h) = chapter {
                println!("Approximate cardinality: {}", h.get_est());
            }
        }
    }

    println!();
}
