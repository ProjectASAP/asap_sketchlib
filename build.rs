fn main() {
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
