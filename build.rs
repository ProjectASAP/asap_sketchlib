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
