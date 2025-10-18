use sketchlib_rust::sketches::Elastic;
use rmp_serde::to_vec_named;
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating Elastic sketch serialization demo...");

    // Create a new Elastic sketch with 8 heavy buckets (default)
    let mut elastic = Elastic::new();

    println!("Inserting test data into Elastic sketch...");

    // Simulate some flow data with different patterns
    // Heavy flows (appear many times)
    let heavy_flows = vec![
        ("elephant_flow_1", 1000),
        ("elephant_flow_2", 800),
        ("elephant_flow_3", 600),
    ];

    // Medium flows
    let medium_flows = vec![
        ("medium_flow_1", 100),
        ("medium_flow_2", 80),
        ("medium_flow_3", 60),
    ];

    // Light flows (appear few times)
    let light_flows = vec![
        ("mouse_flow_1", 10),
        ("mouse_flow_2", 8),
        ("mouse_flow_3", 5),
        ("mouse_flow_4", 3),
        ("mouse_flow_5", 2),
        ("mouse_flow_6", 1),
    ];

    // Insert heavy flows
    for (flow_key, count) in &heavy_flows {
        for _ in 0..*count {
            elastic.insert(flow_key.to_string());
        }
    }

    // Insert medium flows
    for (flow_key, count) in &medium_flows {
        for _ in 0..*count {
            elastic.insert(flow_key.to_string());
        }
    }

    // Insert light flows
    for (flow_key, count) in &light_flows {
        for _ in 0..*count {
            elastic.insert(flow_key.to_string());
        }
    }

    // Query and display results
    println!("\nElastic sketch queries:");
    println!("Heavy flows:");
    for (flow_key, expected) in &heavy_flows {
        let estimate = elastic.query(flow_key.to_string());
        println!("  {}: {} (expected: {})", flow_key, estimate, expected);
    }

    println!("\nMedium flows:");
    for (flow_key, expected) in &medium_flows {
        let estimate = elastic.query(flow_key.to_string());
        println!("  {}: {} (expected: {})", flow_key, estimate, expected);
    }

    println!("\nLight flows:");
    for (flow_key, expected) in &light_flows {
        let estimate = elastic.query(flow_key.to_string());
        println!("  {}: {} (expected: {})", flow_key, estimate, expected);
    }

    // Display heavy bucket status
    println!("\nHeavy buckets status:");
    for (i, bucket) in elastic.heavy.iter().enumerate() {
        if !bucket.flow_id.is_empty() {
            println!(
                "  Bucket {}: flow='{}', pos={}, neg={}, eviction={}",
                i, bucket.flow_id, bucket.vote_pos, bucket.vote_neg, bucket.eviction
            );
        }
    }

    // Create localsketch directory if it doesn't exist
    let sketch_dir = "localsketch";
    if !Path::new(sketch_dir).exists() {
        fs::create_dir_all(sketch_dir)?;
        println!("\nCreated directory: {}", sketch_dir);
    }

    // Serialize using MessagePack
    let buf = to_vec_named(&elastic)?;
    let file_path = format!("{}/elastic_sketch.dat", sketch_dir);
    fs::write(&file_path, &buf)?;

    println!("\nSerialization completed successfully!");
    println!("Serialized Elastic sketch to: {}", file_path);
    println!("File size: {} bytes", buf.len());

    Ok(())
}
