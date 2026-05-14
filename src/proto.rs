// Proto-generated types for cross-language sketch serialization.
//
// The contents of `sketchlib` are vendored: the file under
// `src/proto/generated/sketchlib.v1.rs` is produced by `tools/gen-proto`
// and committed to the repository. End-users of this crate therefore do
// not need `protoc` or any build-script machinery to build it.
//
// To regenerate after editing any `proto/**/*.proto`:
//
//     cargo run --manifest-path tools/gen-proto/Cargo.toml
//
// CI enforces that the committed file matches the result of running the
// tool against the current `.proto` sources.
#[allow(rustdoc::broken_intra_doc_links)]
#[allow(missing_docs)]
#[allow(clippy::all)]
#[rustfmt::skip]
pub mod sketchlib {
    include!("proto/generated/sketchlib.v1.rs");
}
