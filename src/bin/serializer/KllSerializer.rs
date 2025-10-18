use sketchlib_rust::sketches::KLL;
use rmp_serde::to_vec_named;
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating KLL sketch serialization demo...");

    // Create a new KLL sketch with k=100
    let mut kll = KLL::init_kll(100);

    // Insert 100 values
    println!("Inserting 100 values into KLL sketch...");
    for i in 1..=100 {
        kll.update(i as f64);
    }

    // Get some statistics before serialization
    let count = kll.count();
    println!("KLL count: {} (expected: 100)", count);

    // Get some quantile estimates
    println!("Quantile estimates:");
    println!("  p=0.25: {}", kll.cdf().query(0.25));
    println!("  p=0.50: {}", kll.cdf().query(0.50));
    println!("  p=0.75: {}", kll.cdf().query(0.75));

    // Create localsketch directory if it doesn't exist
    let sketch_dir = "localsketch";
    if !Path::new(sketch_dir).exists() {
        fs::create_dir_all(sketch_dir)?;
        println!("Created directory: {}", sketch_dir);
    }

    // Serialize using MessagePack
    let buf = to_vec_named(&kll)?;
    let file_path = format!("{}/kll_sketch.dat", sketch_dir);
    fs::write(&file_path, &buf)?;

    println!("Serialization completed successfully!");
    println!("Serialized KLL sketch to: {}", file_path);

    Ok(())
}
