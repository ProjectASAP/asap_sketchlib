# sketchlib-rust

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

`sketchlib-rust` is a Rust sketch library with reusable sketch building blocks, sketch implementations, and orchestration frameworks.

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
use sketchlib_rust::{DataFusion, HyperLogLog, SketchInput};

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

## Why sketchlib-rust (vs Apache DataSketches)

Performance is the primary motivation for this library:

- Sub-microsecond insert/query with zero heap allocation in the common path: cache-friendly flat counter arrays, row-major layout, and direct slice access.
- `FastPath` mode: a single hash across all rows via bit masking, giving 2-3x throughput over independent-hash modes. See [Key Abstractions](#key-abstractions).
- Native Rust: no JNI/FFI bridge. Memory layout, allocation, and hashing are fully under the caller's control.
- Rust-first API: typed inputs (`SketchInput`) and consistent `insert`/`estimate`/`merge` patterns across all sketches, with pluggable hashing via `SketchHasher`.
- Built-in framework layer (`Hydra`, `HashSketchEnsemble`, `ExponentialHistogram`, `UnivMon`) included in the same crate with hash reuse across sketch collections.

When DataSketches may be a better fit:

- You need its broader algorithm catalog: CPC sketch, Theta/Tuple sketches with set operators (Union, Intersection, Difference), REQ quantiles sketch, VarOpt/Reservoir sampling, or FM85.
- You need cross-language binary compatibility with existing DataSketches deployments in Java, C++, or Python.
- You need long-running production maturity and an Apache-governed release cycle.

Algorithms this library provides that DataSketches does not: `UnivMon` (universal monitoring), `Hydra` (hierarchical subpopulation sketching), `FoldCMS`/`FoldCS` (memory-efficient windowed sketching), and `NitroBatch`.

## Key Abstractions

**`RegularPath` / `FastPath`** — Type-level mode parameters for `CountMin` and `Count Sketch`. `RegularPath` computes R independent hash calls per insert. `FastPath` computes one hash and derives all row indices via bit masking, giving 2-3x higher insert throughput at the cost of slight row correlation (safe for most workloads). Choose `FastPath` when insert rate is the bottleneck.

**`HydraCounter`** — An enum selecting the inner sketch type for each Hydra node (a CMS or Count Sketch variant). Passed at construction via `Hydra::with_dimensions`. Determines what query types are supported.

**`HydraQuery`** — An enum for querying Hydra: `HydraQuery::Frequency(SketchInput)` for frequency estimation or `HydraQuery::Quantile(threshold)` for quantile queries. Defined in [`docs/api/api_common_input.md`](./docs/api/api_common_input.md).

**`SketchInput`** — A unified enum covering all scalar and string key types (`U64`, `Str`, `F64`, etc.), providing a single insert/estimate interface across all sketches.

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
