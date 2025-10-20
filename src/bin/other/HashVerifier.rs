use sketchlib_rust::{SketchInput, hash_it};

pub fn main() {
    println!("===Check i32===");
    for i in 0..4 {
        for j in 1..=10 {
            let idx = hash_it(i, &SketchInput::I32(j)) % 32;
            println!("at row {} => insert {}, idx {}", i, j, idx);
        }
    }
    println!("===Check i64===");
    for i in 0..4 {
        for j in 1..=10 {
            let idx = hash_it(i, &SketchInput::I64(j)) % 32;
            println!("at row {} => insert {}, idx {}", i, j, idx);
        }
    }
    println!("===Check u32===");
    for i in 0..4 {
        for j in 1..=10 {
            let idx = hash_it(i, &SketchInput::U32(j)) % 32;
            println!("at row {} => insert {}, idx {}", i, j, idx);
        }
    }
    println!("===Check u64===");
    for i in 0..4 {
        for j in 1..=10 {
            let idx = hash_it(i, &SketchInput::U64(j)) % 32;
            println!("at row {} => insert {}, idx {}", i, j, idx);
        }
    }
    println!("===Check f32===");
    for i in 0..4 {
        for j in 1..=10 {
            let idx = hash_it(i, &SketchInput::F32(j as f32)) % 32;
            println!("at row {} => insert {}, idx {}", i, j, idx);
        }
    }
    println!("===Check f64===");
    for i in 0..4 {
        for j in 1..=10 {
        let idx = hash_it(i, &SketchInput::F64(j as f64)) % 32;
            println!("at row {} => insert {}, idx {}", i, j, idx);
        }
    }
    println!("===Check 100000===");
    for i in 0..4 {
        let idx = hash_it(i, &SketchInput::U64(100000)) % 32;
        println!("U64: at row {} => insert {}, idx {}", i, 100000, idx);
        println!("U64: bytes of 100000 is: {:?}", 100000_u64.to_ne_bytes());

        let idx = hash_it(i, &SketchInput::I32(100000)) % 32;
        println!("I32: at row {} => insert {}, idx {}", i, 100000, idx);
        println!("I32: bytes of 100000 is: {:?}", 100000_i32.to_ne_bytes());
    }
}
