use rmp_serde::to_vec_named;
use sketchlib_rust::sketches::CountUniv;
use sketchlib_rust::sketches::utils::SketchInput;
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating CountUniv sketch serialization demo...");

    // Create a new CountUniv sketch with default size (4 rows, 32 columns)
    let mut cu = CountUniv::init_count();

    // Insert values with their frequencies
    println!("Inserting values into CountUniv sketch...");

    // Insert numbers 1-10, where number i appears i times
    for i in 1..=10 {
        for _ in 0..i {
            cu.insert_once(&SketchInput::I32(i));
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
            cu.insert_once(&SketchInput::Str(*key));
        }
    }

    // Get some estimates before serialization
    println!("CountUniv estimates before serialization:");
    for i in 1..=10 {
        let estimate = cu.get_est(&SketchInput::I32(i));
        println!("  {}: {:.1} (expected: {})", i, estimate, i);
    }

    for (key, expected) in &string_values {
        let estimate = cu.get_est(&SketchInput::Str(*key));
        println!("  {}: {:.1} (expected: {})", key, estimate, expected);
    }

    // Get L2 norm
    let l2 = cu.get_l2();
    println!("L2 norm: {:.2}", l2);

    // Create localsketch directory if it doesn't exist
    let sketch_dir = "localsketch";
    if !Path::new(sketch_dir).exists() {
        fs::create_dir_all(sketch_dir)?;
        println!("Created directory: {}", sketch_dir);
    }

    cu.debug();

    // Serialize using MessagePack
    let buf = to_vec_named(&cu)?;
    let file_path = format!("{}/countuniv_sketch.dat", sketch_dir);
    fs::write(&file_path, &buf)?;

    println!("Serialization completed successfully!");
    println!("Serialized CountUniv sketch to: {}", file_path);

    Ok(())
}
