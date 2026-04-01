# sketchlib-rust

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

`sketchlib-rust` is a Rust sketch library with reusable sketch building blocks, sketch implementations, and orchestration frameworks.

## Supported Sketches

| Goal | Use This | When to pick it |
| --- | --- | --- |
| Frequency estimation | `CountMin`, `Count Sketch` | You need fast approximate counts for high-volume keys. |
| Cardinality estimation | `HyperLogLog` (`Regular`, `DataFusion`, `HIP`) | You need approximate distinct counts with bounded memory. |
| Quantiles/distribution | `KLL`, `DDSketch` | You need percentile/latency summaries over streams. |
| Multi-sketch orchestration/windowing | `Hydra`, `UnivMon`, `HashLayer`, `ExponentialHistogram`, `EHUnivOptimized`, `NitroBatch`, `Orchestrator` | You need hierarchical queries, sketch coordination, or sliding-window aggregation. |

Full sketch status and API details: [APIs Index](./docs/apis.md).

## Quick Start

Simple demo use case: estimate unique users with HyperLogLog.
Example usage:

```rust
use sketchlib_rust::common::input::{HydraCounter, HydraQuery};
use sketchlib_rust::{CountMin, FastPath, SketchInput, Vector2D};

// Create a CountMin-based counter
let mut counter = HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default());

// Insert values
let key = SketchInput::String("event".into());
counter.insert(&key, None);
counter.insert(&key, None);

// Query frequency (compatible)
let freq = counter.query(&HydraQuery::Frequency(key)).unwrap();
println!("frequency: {}", freq);

// Query cardinality (incompatible - returns error)
match counter.query(&HydraQuery::Cardinality) {
    Ok(_) => println!("success"),
    Err(e) => println!("error: {}", e),
}
```

To validate the repo quickly:

```bash
cargo test
```

Common dev commands:

```bash
cargo build --all-targets
cargo test --all-features
cargo bench
```

## Why sketchlib-rust (vs Apache DataSketches)

- Native Rust library: no JNI/FFI bridge needed for Rust services.
- Rust-first API surface: typed inputs (`SketchInput`) and consistent `insert`/`estimate`/`merge` patterns across sketches.
- Built-in framework layer: `Hydra`, `HashLayer`, `ExponentialHistogram`, and `EHUnivOptimized` are included in the same crate.
- Optimization hooks for Rust workloads: shared-hash fast paths and pluggable hashing via `SketchHasher`.

When DataSketches may be a better fit:

- You need its broader algorithm catalog and long-running production maturity.
- You need direct compatibility with existing DataSketches deployments across Java/C++/Python ecosystems.

## Documentation

For more details, see [Docs Index](./docs/index.md).

## Contributors

- [Yancheng Yuan](https://github.com/GordonYuanyc)
- [Zeying Zhu](https://github.com/zzylol)

## License

Copyright 2025 ProjectASAP

Licensed under the MIT License. See [LICENSE](LICENSE).
