//! Build script that compiles the crate's protobuf definitions.
//!
//! The large precomputed sampling and hash tables that used to live next to
//! this file as multi-megabyte literal arrays are now built lazily at runtime
//! via [`std::sync::LazyLock`] (see `src/common/precompute_*.rs`), so this
//! script no longer needs to do any code generation beyond `prost`.

fn main() {
    let protoc =
        protoc_bin_vendored::protoc_bin_path().expect("failed to locate vendored protoc binary");
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    prost_build::compile_protos(
        &[
            "proto/common/common.proto",
            "proto/countminsketch/countminsketch.proto",
            "proto/countsketch/countsketch.proto",
            "proto/hll/hll.proto",
            "proto/kll/kll.proto",
            "proto/ddsketch/ddsketch.proto",
            "proto/univmon/univmon.proto",
            "proto/hydra/hydra.proto",
            "proto/cocosketch/cocosketch.proto",
            "proto/elasticsketch/elasticsketch.proto",
            "proto/sketchlib.proto",
        ],
        &["proto"],
    )
    .expect("prost_build failed to compile proto files");
}
