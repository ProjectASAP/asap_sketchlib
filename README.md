# ASAPSketchLib

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

`ASAPSketchLib` is a Rust sketch library with reusable sketch building blocks, sketch implementations, and orchestration frameworks.

## Supported Sketches

| Goal | Use This | When to pick it | Pandas/Polars equivalent (exact, unbounded memory) |
| --- | --- | --- | --- |
| Frequency estimation | `CountMin`, `Count Sketch` | You need fast approximate counts for high-volume keys. | `df.groupby("key").size()` / `df.group_by("key").agg(pl.len())` — exact but O(distinct keys) memory |
| Cardinality estimation | `HyperLogLog` (`Regular`, `DataFusion`, `HIP`) | You need approximate distinct counts with bounded memory. | `df["col"].nunique()` / `df["col"].n_unique()` — exact but O(n) memory |
| Quantiles/distribution | `KLL`, `DDSketch` | You need percentile/latency summaries over streams. | `df["col"].quantile(0.99)` — exact but requires storing all values |
| Advanced use cases (frameworks) | see [Advanced Use Cases](./docs/advanced_use_cases.md) | Hierarchical subpopulation queries, multi-sketch coordination, or sliding-window aggregation over streams. | No direct equivalent — sketches are the only practical solution at stream scale |

Full sketch status and API details: [APIs Index](./docs/apis.md).

## Quick Start

Simple demo use case: estimate unique users with HyperLogLog.
Example usage:

```rust
use asap_sketch_lib::{DataFusion, HyperLogLog, SketchInput};

let mut hll = HyperLogLog::<DataFusion>::default();

// Simulate a stream of user IDs (with duplicates)
for user_id in [101, 202, 303, 101, 404, 202, 505, 101] {
    hll.insert(&SketchInput::U64(user_id));
}

let unique_users = hll.estimate();
println!("estimated unique users: {unique_users}"); // ≈ 5
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

## Why ASAPSketchLib (vs Apache DataSketches)

Performance is the primary motivation for this library:

- Performance-focused implementations with cache-friendly flat counter arrays, row-major layouts, and direct slice access in core sketch paths.
- `FastPath` mode computes a single hash and derives row indices via bit masking, reducing hashing overhead relative to independent-hash modes.
- Native Rust: no JNI/FFI bridge. Memory layout, allocation, and hashing stay within the Rust implementation.
- Rust-first API: typed inputs (`SketchInput`) and largely consistent `insert`/`estimate`/`merge` patterns across the main sketches, with pluggable hashing via `SketchHasher`.
- Built-in framework layer (`Hydra`, `HashSketchEnsemble`, `ExponentialHistogram`, `UnivMon`) included in the same crate, including hash-reuse support for coordinated sketch collections.

When DataSketches may be a better fit:

- You need its broader algorithm catalog: CPC sketch, Theta/Tuple sketches with set operators (Union, Intersection, Difference), REQ quantiles sketch, VarOpt/Reservoir sampling, or FM85.
- You need cross-language binary compatibility with existing DataSketches deployments in Java, C++, or Python.
- You need long-running production maturity and an Apache-governed release cycle.

Algorithms this library provides that DataSketches does not: `UnivMon` (universal monitoring), `Hydra` (hierarchical subpopulation sketching), `FoldCMS`/`FoldCS` (memory-efficient windowed sketching), and `NitroBatch`.

## Choosing Between Sketches for the Same Goal

Several sketches address the same analytical goal with different trade-offs. For example, `CountMin` and `Count Sketch` both estimate frequencies; `HyperLogLog` (`Regular`, `DataFusion`, `HIP`) all estimate cardinality; `KLL` and `DDSketch` both answer quantile queries.

The best current approach is to **profile the sketch against a representative sample of your actual data** and compare error rates, memory usage, and insert throughput for your specific key distribution and stream volume. The [APIs Index](./docs/apis.md) lists the status and caveats for each sketch.

A detailed comparison guide with benchmark data across sketch types and workloads is planned.

## Documentation

For more details, see [Docs Index](./docs/index.md).

## Contributors

### Major Contributors

- [Yancheng Yuan](https://github.com/GordonYuanyc)
- [Zeying Zhu](https://github.com/zzylol)

### Other Contributors

- [Sie Deta Dirganjaya](https://github.com/SieDeta)
- [Gnanesh Gnani](https://github.com/GnaneshGnani)

## License

Copyright 2025 ProjectASAP

Licensed under the MIT License. See [LICENSE](LICENSE).
