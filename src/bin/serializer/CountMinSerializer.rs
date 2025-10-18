use sketchlib_rust::sketches::CountMin;
use sketchlib_rust::sketches::utils::SketchInput;
use sketchlib_rust::utils::hash_it;
use rmp_serde::to_vec_named;
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating CountMin sketch serialization demo...");

    // Create a new CountMin sketch
    let mut cm = CountMin::init_count_min();

    // Insert values with their frequencies (similar to SketchExaminer.rs)
    println!("Inserting values into CountMin sketch...");

    // Insert numbers 1-10, where number i appears i times
    for i in 1..=10 {
        for _ in 0..i {
            // cm.insert_cm(&i);
            cm.insert_cm(&SketchInput::I32(i));
        }
    }

    // Also insert some string values for testing
    let string_values = vec![
        ("apple", 5),
        ("banana", 3),
        ("cherry", 7),
        ("date", 2),
        ("elderberry", 4),
    ];

    for (key, count) in &string_values {
        for _ in 0..*count {
            // cm.insert_cm(key);
            cm.insert_cm(&SketchInput::Str(*key));
        }
    }

    // Get some estimates before serialization
    println!("CountMin estimates before serialization:");
    for i in 1..=10 {
        // let estimate = cm.get_est(&i);
        let estimate = cm.get_est(&SketchInput::I32(i));
        println!("  {}: {} (expected: {})", i, estimate, i);
    }
    for _ in 0..100000 {
        cm.insert_cm(&SketchInput::I32(100));
    }

    for (key, expected) in &string_values {
        // let estimate = cm.get_est(key);
        let estimate = cm.get_est(&SketchInput::Str(*key));
        println!("  {}: {} (expected: {})", key, estimate, expected);
    }

    // Create localsketch directory if it doesn't exist
    let sketch_dir = "localsketch";
    if !Path::new(sketch_dir).exists() {
        fs::create_dir_all(sketch_dir)?;
        println!("Created directory: {}", sketch_dir);
    }
    cm.debug();

    for i in 0..4 {
        for j in 1..=10 {
            let idx = hash_it(i, &SketchInput::I32(j)) % 32;
            println!("at row {} => insert {}, idx {}", i, j, idx);
        }
    }

    // Serialize to JSON
    // let json_data = serde_json::to_string_pretty(&cm)?;
    // let json_path = format!("{}/countmin_sketch.json", sketch_dir);
    // fs::write(&json_path, json_data)?;
    // println!("Serialized CountMin sketch to: {}", json_path);

    // let mut buf = Vec::with_capacity(256 * 2);
    // cm.serialize(&mut Serializer::new(&mut buf))?;
    // let file_path = format!("{}/cm_sketch.dat", sketch_dir);
    // fs::write(&file_path, &buf)?;
    // let mut buf = Vec::with_capacity(256 * 2);
    // let opts = DefaultOptions::new().with_fixint_encoding().with_little_endian();
    // let encoded = opts.serialize(&cm)?;

    // let cfg = config::standard().with_fixed_int_encoding().with_little_endian();
    // let buf= encode_to_vec(&cm, cfg)?;
    // let file_path = format!("{}/cm_sketch.dat", sketch_dir);
    // fs::write(&file_path, &buf)?;

    let buf = to_vec_named(&cm)?;
    let file_path = format!("{}/cm_sketch.dat", sketch_dir);
    fs::write(&file_path, &buf)?;

    println!("CountMin serialization completed successfully!");

    Ok(())
}
