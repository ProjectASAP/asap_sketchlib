use rmp_serde::to_vec_named;
use sketchlib_rust::sketches::HllDfModified;
use sketchlib_rust::sketches::utils::SketchInput;
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating HLL sketch serialization demo...");

    // Create a new HLL sketch
    let mut hll = HllDfModified::new();

    // Insert 100 values (simple integers)
    println!("Inserting 100 values into HLL sketch...");
    for i in 0..10000 {
        hll.insert(&SketchInput::I32(i));
    }

    // Get the estimate before serialization
    let estimate = hll.get_est();
    println!("HLL estimate: {} (expected: ~10000)", estimate);

    // Create localsketch directory if it doesn't exist
    let sketch_dir = "localsketch";
    if !Path::new(sketch_dir).exists() {
        fs::create_dir_all(sketch_dir)?;
        println!("Created directory: {}", sketch_dir);
    }

    // Serialize to JSON
    // let json_data = serde_json::to_string_pretty(&hll)?;
    // let json_data = serde_json::to_string(&hll)?;
    // let json_path = format!("{}/hll_sketch.json", sketch_dir);
    // fs::write(&json_path, json_data)?;
    // println!("Serialized HLL sketch to: {}", json_path);

    // let mut buf = Vec::with_capacity(256 * 2);
    // hll.serialize(&mut Serializer::new(&mut buf))?;
    // let file_path = format!("{}/hll_sketch.dat", sketch_dir);
    // fs::write(&file_path, &buf)?;

    let buf = to_vec_named(&hll)?;
    // println!("buf: {:?}", buf);
    let file_path = format!("{}/hll_sketch.dat", sketch_dir);
    fs::write(&file_path, &buf)?;

    // // Also create a metadata file with sketch information
    // let metadata = serde_json::json!({
    //     "sketch_type": "HLL",
    //     "num_registers": hll.registers.len(),
    //     "inserted_values": 100,
    //     "estimated_cardinality": estimate,
    //     "file_format": "JSON",
    //     "timestamp": chrono::Utc::now().to_rfc3339()
    // });

    // let metadata_path = format!("{}/metadata.json", sketch_dir);
    // fs::write(&metadata_path, serde_json::to_string_pretty(&metadata)?)?;
    // println!("Created metadata file: {}", metadata_path);

    println!("Serialization completed successfully!");

    Ok(())
}
