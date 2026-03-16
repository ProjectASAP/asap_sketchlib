// ================================================================
// THIS FILE MUST BE IDENTICAL IN sketchlib-go AND sketchlib-rust
// ================================================================

fn main() {
    prost_build::compile_protos(
        &["proto/sketchlib.proto"],
        &["proto"],
    )
    .expect("prost_build failed to compile sketchlib.proto");
}

//use sketchlib-go to build:

// fn main() {
//     prost_build::compile_protos(
//         &["../sketchlib-go/proto/sketchlib.proto"],
//         &["../sketchlib-go/proto"],
//     ).unwrap();
// }

