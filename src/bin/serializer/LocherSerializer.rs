use querysimulation::sketches::LocherSketch;
use rmp_serde::to_vec_named;
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating Locher sketch serialization demo...");

    // Create a new Locher sketch
    // Parameters: r (rows), l (columns), k (top-k per cell)
    let r = 5; // 5 rows
    let l = 8; // 8 columns
    let k = 10; // top-10 per cell

    let mut locher = LocherSketch::new(r, l, k);

    println!("Inserting test data into Locher sketch...");

    // Test data - simulate flow tracking with different patterns
    let test_flows = vec![
        // Heavy hitters
        ("flow_heavy_1", 500),
        ("flow_heavy_2", 400),
        ("flow_heavy_3", 350),
        // Medium flows
        ("flow_medium_1", 100),
        ("flow_medium_2", 80),
        ("flow_medium_3", 60),
        ("flow_medium_4", 50),
        // Light flows
        ("flow_light_1", 20),
        ("flow_light_2", 15),
        ("flow_light_3", 10),
        ("flow_light_4", 8),
        ("flow_light_5", 5),
    ];

    // Insert flows into Locher
    for (flow_key, count) in &test_flows {
        let key_string = flow_key.to_string();
        for _ in 0..*count {
            locher.insert(&key_string, 1);
        }
    }

    // Test queries
    println!("\nLocher sketch estimates:");
    for (flow_key, expected) in &test_flows {
        let key_string = flow_key.to_string();
        let estimate = locher.estimate(&key_string);
        println!("  {}: {:.2} (expected: {})", flow_key, estimate, expected);
    }

    // Display sketch statistics
    println!("\nLocher sketch statistics:");
    println!("  Rows (r): {}", locher.r);
    println!("  Columns (l): {}", locher.l);
    println!("  Total cells: {}", locher.r * locher.l);

    println!("\nRow sums:");
    for (i, sum) in locher.row_sum.iter().enumerate() {
        println!("  Row {}: {:.2}", i, sum);
    }

    // Count non-empty cells
    let mut non_empty_cells = 0;
    let mut total_items = 0;
    for i in 0..locher.r {
        for j in 0..locher.l {
            if !locher.rows[i][j].heap.is_empty() {
                non_empty_cells += 1;
                total_items += locher.rows[i][j].heap.len();
            }
        }
    }
    println!(
        "  Non-empty cells: {}/{}",
        non_empty_cells,
        locher.r * locher.l
    );
    println!("  Total items in heaps: {}", total_items);

    // Create localsketch directory if it doesn't exist
    let sketch_dir = "localsketch";
    if !Path::new(sketch_dir).exists() {
        fs::create_dir_all(sketch_dir)?;
        println!("\nCreated directory: {}", sketch_dir);
    }

    // Serialize using MessagePack
    let buf = to_vec_named(&locher)?;
    let file_path = format!("{}/locher_sketch.dat", sketch_dir);
    fs::write(&file_path, &buf)?;

    println!("\nSerialization completed successfully!");
    println!("Serialized Locher sketch to: {}", file_path);
    println!("File size: {} bytes", buf.len());

    Ok(())
}
