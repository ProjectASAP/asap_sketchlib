use rmp_serde::to_vec_named;
use sketchlib_rust::sketches::Coco;
use sketchlib_rust::sketches::utils::SketchInput;
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating Coco sketch serialization demo...");

    // Test data - URLs with different patterns
    let test_flows = vec![
        ("google.com/search?q=rust", 100),
        ("google.com/search?q=programming", 80),
        ("google.com/maps", 60),
        ("facebook.com/profile/user1", 50),
        ("facebook.com/profile/user2", 40),
        ("facebook.com/feed", 30),
        ("twitter.com/home", 90),
        ("twitter.com/notifications", 70),
        ("github.com/rust-lang/rust", 120),
        ("github.com/microsoft/vscode", 110),
    ];

    // Keep owned strings alive for the lifetime of coco
    let owned_flows: Vec<String> = test_flows.iter().map(|(k, _)| k.to_string()).collect();

    // Create a new Coco sketch (64 buckets width, 5 rows depth)
    let mut coco: Coco = Coco::new();

    println!("Inserting test data into Coco sketch...");

    // Insert flows into Coco using owned strings
    for (flow_string, (_, count)) in owned_flows.iter().zip(test_flows.iter()) {
        coco.insert(&SketchInput::Str(flow_string), *count);
    }

    // Test exact match queries
    println!("\nCoco exact match queries:");
    for (flow_string, (flow_key, expected)) in owned_flows.iter().zip(test_flows.iter()) {
        let estimate = coco.estimate(SketchInput::Str(flow_string));
        println!("  {}: {} (expected: {})", flow_key, estimate, expected);
    }

    // Test prefix queries (partial match)
    println!("\nCoco prefix match queries:");
    let prefixes = vec![
        ("google.com", 240),   // sum of all google.com flows
        ("facebook.com", 120), // sum of all facebook.com flows
        ("twitter.com", 160),  // sum of all twitter.com flows
        ("github.com", 230),   // sum of all github.com flows
    ];

    // coco.debug();
    for (prefix, expected) in &prefixes {
        let estimate = coco.estimate(SketchInput::Str(prefix));
        println!("  {}: {} (expected: ~{})", prefix, estimate, expected);
    }

    // Display sketch statistics
    println!("\nCoco sketch statistics:");
    println!("  Width (w): {}", coco.w);
    println!("  Depth (d): {}", coco.d);

    let mut non_empty_count = 0;
    for row in &coco.table {
        for bucket in row {
            if bucket.full_key.is_some() && bucket.val > 0 {
                non_empty_count += 1;
            }
        }
    }
    println!(
        "  Non-empty buckets: {}/{}",
        non_empty_count,
        coco.w * coco.d
    );

    // Create localsketch directory if it doesn't exist
    let sketch_dir = "localsketch";
    if !Path::new(sketch_dir).exists() {
        fs::create_dir_all(sketch_dir)?;
        println!("\nCreated directory: {}", sketch_dir);
    }

    // Serialize using MessagePack - serde can handle borrowed strings directly!
    let buf = to_vec_named(&coco)?;
    let file_path = format!("{}/coco_sketch.dat", sketch_dir);
    fs::write(&file_path, &buf)?;

    println!("\nSerialization completed successfully!");
    println!("Serialized Coco sketch to: {}", file_path);
    println!("File size: {} bytes", buf.len());

    Ok(())
}
