# sketchlib-rs

`sketchlib-rs` is an experimental Rust library that bundles probabilistic sketches and orchestration utilities for streaming telemetry experiments. The ultimate goal is an easy-to-import rust library to use in either streaming engines or local programs.

## Highlights

- Rich sketch collection covering Count-Min, Count/CountUniv, Coco, Elastic, HyperLogLog variants, KLL, Locher, Microscope, and UnivMon implementations.
- Hydra coordinator builds multi-dimensional label combinations so the same data supports queries over arbitrary label subsets.
- Chapter enum normalizes insert, merge, and query flows across sketches for composable pipelines.
- Exponential Histogram wrapper maintains time-bounded aggregates without duplicating sketch state.
- MessagePack + hex serialization bridges the Rust sketches with PromSketch, Go clients, and optional Arroyo UDFs.

## Quick Start
<!-- - Install a Rust toolchain that supports edition 2024 (currently nightly via `rustup toolchain install nightly`).
- Build everything: `cargo build --all-targets`.
- Run the library tests: `cargo test --all-features`.
- Explore the sketch demos: `cargo run --bin test_all_sketch` or any tester in `src/bin/sketch_tester`. -->
At this moment, ```cargo test``` is a good starting point.

### Example: Count-Min frequency

```rust
use querysimulation::{CountMin, sketches::utils::SketchInput};

fn main() {
    let mut cm = CountMin::default();
    cm.insert_cm(&SketchInput::String("error".into()));
    cm.insert_cm(&SketchInput::String("error".into()));

    let estimate = cm.get_est(&SketchInput::String("error".into()));
    println!("approximate error count = {}", estimate);
}
```

### Example: Windowed aggregates with Exponential Histogram

```rust
use querysimulation::{
    sketchbook::{Chapter, ExponentialHistogram},
    sketches::{countmin::CountMin, utils::SketchInput},
};

fn main() {
    let template = Chapter::CM(CountMin::default());
    let mut eh = ExponentialHistogram::new(3, 120, template);

    eh.update(10, &SketchInput::String("flow".into()));
    eh.update(70, &SketchInput::String("flow".into()));

    if let Some(volume) = eh.query_interval_merge(0, 120) {
        let estimate = volume.query(&SketchInput::String("flow".into())).unwrap();
        println!("approximate count inside window = {}", estimate);
    }
}
```

## Library Map

- `src/sketches`: core sketch implementations plus helpers such as `SketchInput`, hashing utilities, and serialization hooks.
- `src/sketchbook`: orchestration layers (Hydra, Chapter, ExponentialHistogram) for combining sketches into label-aware and time-aware structures.
- `src/deserializers`: serde-ready records that decode hex-encoded MessagePack payloads emitted by Arroyo and PromSketch experiments.
- `src/bin/sketch_tester`: per-sketch binaries that exercise insertion/query paths and print diagnostics.
- `src/bin/serializer`: tools that build serialized artifacts saved in `localsketch/` for cross-language testing.
- `localsketch/` and `testdata/`: canned sketches and timestamp fixtures useful for reproducible experiments.

## Serialization & Interop

- `SketchInput` unifies numeric keys, floats, strings, and byte blobs so sketches share the same entry points.
- MessagePack via `rmp-serde` keeps payloads compact while `serde_bytes` ensures buffers stay binary-friendly.
- `deserializers::Record` and friends handle the hex framing that Arroyo UDFs produce before shipping to downstream consumers.
- Enable the `arroyo` feature (`cargo build --features arroyo`) to compile the UDF plugin glue when embedding in Arroyo jobs.

## Development

- Format sources with `cargo fmt` before committing changes.
- Lint with `cargo clippy --all-targets --all-features` to catch obvious mistakes across sketches and orchestration layers.
- Run targeted binaries such as `cargo run --bin cm_test` when iterating on a specific sketch.
- Regenerate serialized fixtures via the serializer binaries whenever sketch layouts change.

## Status & Next Steps

- Early-stage code: APIs may change, and several sketches are still being tuned for accuracy.
- Some components (for example Elastic merges or Hydra's public update surface) remain works in progress.
<!-- - Cross-language support currently targets PromSketch and Go; extend the deserializers if new consumers appear. -->
- Contributions and experiment results are welcome—open an issue describing the workload or sketch you plan to add.
- Missing many testing
- Missing many serialization and deserialization support
