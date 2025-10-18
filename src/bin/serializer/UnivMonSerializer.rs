use sketchlib_rust::sketches::UnivMon;
use sketchlib_rust::sketches::utils::SketchInput;
use rmp_serde::to_vec_named;
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating UnivMon sketch serialization demo...");

    // Create a new UnivMon sketch
    // Parameters: k (top-k), row, col, layer, pool_idx
    let k = 100;
    let row = 4;
    let col = 32;
    let layer = 5;
    let pool_idx = 0;

    let mut univmon = UnivMon::init_univmon(k, row, col, layer, pool_idx);

    // Insert some test data
    println!("Inserting test data into UnivMon sketch...");

    // Simulate some flow data
    let test_flows = vec![
        ("flow_1", 100),
        ("flow_2", 50),
        ("flow_3", 200),
        ("flow_4", 75),
        ("flow_5", 150),
        ("flow_6", 25),
        ("flow_7", 300),
        ("flow_8", 10),
        ("flow_9", 80),
        ("flow_10", 120),
    ];

    for (flow_key, count) in &test_flows {
        let key = flow_key.to_string();
        // Find bottom layer using hash
        let hash = sketchlib_rust::utils::hash_it(5, &SketchInput::Str(&key));
        let bottom_layer = univmon.find_bottom_layer_num(hash, layer);

        // Update the UnivMon with the flow
        univmon.univmon_processing(&key, *count, bottom_layer);
    }

    // Calculate and display statistics
    println!("\nUnivMon statistics:");
    println!("  Bucket size: {}", univmon.get_bucket_size());
    println!("  L1 norm: {:.2}", univmon.calc_l1());
    println!("  L2 norm: {:.2}", univmon.calc_l2());
    println!("  Cardinality: {:.2}", univmon.calc_card());
    println!("  Entropy: {:.4}", univmon.calc_entropy());

    // Display heavy hitters
    println!("\nHeavy hitters at layer 0:");
    for (i, item) in univmon.hh_layers[0].heap.iter().enumerate().take(5) {
        println!("  {}: key='{}', count={}", i + 1, item.key, item.count);
    }

    // Create localsketch directory if it doesn't exist
    let sketch_dir = "localsketch";
    if !Path::new(sketch_dir).exists() {
        fs::create_dir_all(sketch_dir)?;
        println!("\nCreated directory: {}", sketch_dir);
    }

    // Serialize using MessagePack
    let buf = to_vec_named(&univmon)?;
    let file_path = format!("{}/univmon_sketch.dat", sketch_dir);
    fs::write(&file_path, &buf)?;

    println!("\nSerialization completed successfully!");
    println!("Serialized UnivMon sketch to: {}", file_path);
    println!("File size: {} bytes", buf.len());

    Ok(())
}
