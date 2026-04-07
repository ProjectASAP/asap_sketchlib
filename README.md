# asap_sketchlib

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A Rust library for **streaming data sketches** — fixed-memory data structures that give approximate answers (counts, distinct counts, percentiles) over data streams too large to store exactly.

## Why asap_sketchlib

- **Native Rust, no JNI/FFI bridge.** Memory layout, allocation, and hashing stay within Rust — no overhead from crossing language boundaries.
- **Consistent API across sketches.** Typed inputs (`SketchInput`) and uniform `insert`/`estimate`/`merge` patterns, with pluggable hashing via `SketchHasher`.
- **Algorithms not found elsewhere.** Includes `UnivMon` (universal monitoring), `Hydra` (hierarchical subpopulation sketching), `FoldCMS`/`FoldCS` (memory-efficient windowed sketching), and `NitroBatch`.
- **Built-in orchestration frameworks** — coordinate multiple sketches with shared hashing (`HashLayer`), manage sliding windows (`TumblingWindow`), or run hierarchical queries (`Hydra`).

When Apache DataSketches may be a better fit:

- You need its broader algorithm catalog (CPC, Theta/Tuple with set operators, REQ, VarOpt/Reservoir, FM85).
- You need cross-language binary compatibility with existing DataSketches deployments in Java, C++, or Python.
- You need long-running production maturity and an Apache-governed release cycle.

## Supported Sketches

| Goal | Sketch | When to pick it | Pandas/Polars equivalent (exact, unbounded memory) |
| --- | --- | --- | --- |
| Frequency estimation | `CountMin`, `Count Sketch` | Fast approximate counts for high-volume keys | `df.groupby("key").size()` / `df.group_by("key").agg(pl.len())` |
| Cardinality estimation | `HyperLogLog` (`Regular`, `ErtlMLE`, `HIP`) | Approximate distinct counts with bounded memory | `df["col"].nunique()` / `df["col"].n_unique()` |
| Quantiles / distribution | `KLL`, `DDSketch` | Percentile / latency summaries over streams | `df["col"].quantile(0.99)` |
| Windowed frequency | `FoldCMS`, `FoldCS` | Sliding-window frequency estimation with reduced per-window memory | No direct equivalent |
| Advanced frameworks | `Hydra`, `UnivMon`, `NitroBatch` | Hierarchical queries, universal monitoring, batch sampling | No direct equivalent |

Full sketch status and API details: [APIs Index](./docs/apis.md).

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
asap_sketchlib = { git = "https://github.com/ProjectASAP/asap_sketchlib" }
```

To enable the multi-threaded OctoSketch runtime (`OctoRuntime`, `run_octo`):

```toml
[dependencies]
asap_sketchlib = { git = "https://github.com/ProjectASAP/asap_sketchlib", features = ["octo-runtime"] }
```

The delta types and traits (`OctoWorker`, `OctoAggregator`, `insert_emit_delta`, `apply_delta`) are always available without this feature.

### Count distinct users with HyperLogLog

```rust
use asap_sketchlib::{ErtlMLE, HyperLogLog, SketchInput};

// ErtlMLE: Ertl's maximum-likelihood HLL estimator (arXiv:1702.01284)
// — better accuracy than classic HLL at low and high cardinalities
let mut hll = HyperLogLog::<ErtlMLE>::default();

for user_id in [101, 202, 303, 101, 404, 202, 505, 101] {
    hll.insert(&SketchInput::U64(user_id));
}

let unique_users = hll.estimate();
println!("estimated unique users: {unique_users}"); // ≈ 5
```

### Estimate frequency of items with Count-Min Sketch

```rust
use asap_sketchlib::{CountMin, FastPath, Vector2D, SketchInput};

// 3 rows × 2048 columns, using FastPath (single hash, bit-masked row indices)
let mut cms = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 2048);

// Simulate an event stream
for _ in 0..1000 { cms.insert(&SketchInput::Str("page_view")); }
for _ in 0..50  { cms.insert(&SketchInput::Str("purchase")); }

let page_views = cms.estimate(&SketchInput::Str("page_view"));
let purchases  = cms.estimate(&SketchInput::Str("purchase"));
println!("page_view ≈ {page_views}, purchase ≈ {purchases}");
```

### Track p99 latency with KLL

```rust
use asap_sketchlib::{KLL, SketchInput};

let mut sketch = KLL::new();

// Simulate latencies in milliseconds
for &ms in &[12.0, 15.0, 14.5, 200.0, 13.0, 16.0, 11.0, 210.0, 14.0, 13.5] {
    sketch.update(&SketchInput::F64(ms)).unwrap();
}

let p99 = sketch.quantile(0.99);
println!("p99 latency ≈ {p99:.1} ms");
```

### Merge sketches from distributed nodes

```rust
use asap_sketchlib::{ErtlMLE, HyperLogLog, SketchInput};

let mut node_a = HyperLogLog::<ErtlMLE>::default();
let mut node_b = HyperLogLog::<ErtlMLE>::default();

// Each node sees different (and some overlapping) users
for id in [1, 2, 3, 4, 5]  { node_a.insert(&SketchInput::U64(id)); }
for id in [4, 5, 6, 7, 8]  { node_b.insert(&SketchInput::U64(id)); }

node_a.merge(&node_b);
println!("total unique users ≈ {}", node_a.estimate()); // ≈ 8
```

## Choosing Between Sketches

Several sketches address the same goal with different trade-offs — for example, `CountMin` vs `Count Sketch` for frequency, or `KLL` vs `DDSketch` for quantiles.

The best approach is to **profile against a representative sample of your data** and compare error, memory, and throughput. The [APIs Index](./docs/apis.md) lists guarantees and caveats for each sketch.

## Performance

Performance details including cache-friendly layouts, `FastPath` single-hash mode, and benchmark methodology are documented in [Performance Notes](./docs/features.md).

## Documentation

| Doc | Contents |
| --- | --- |
| [APIs Index](./docs/apis.md) | Per-sketch API reference with status and error guarantees |
| [Advanced Use Cases](./docs/advanced_use_cases.md) | Hierarchical queries, windowed sketching, multi-sketch coordination |
| [Fold Sketch Design](./docs/fold_sketch_design.md) | Design and analysis of FoldCMS / FoldCS |
| [Docs Index](./docs/index.md) | Full documentation index |

## Dev Commands

```bash
cargo build --all-targets
cargo test --all-features
cargo bench
```

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
