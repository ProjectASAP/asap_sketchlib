# asap_sketchlib

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/ProjectASAP/asap_sketchlib/blob/main/LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://blog.rust-lang.org/2025/02/20/Rust-1.85.0.html)

A Rust library for **streaming data sketches** — fixed-memory data structures that give approximate answers (counts, distinct counts, percentiles) over data streams too large to store exactly.

## Why asap_sketchlib

- **Fast.** Up to 8–14× higher insertion throughput than comparable libraries on frequency sketches, and 2–3× on cardinality sketches. See [benchmarks](#performance).
- **Native Rust, no JNI/FFI bridge.** Memory layout, allocation, and hashing stay within Rust — no overhead from crossing language boundaries.
- **Consistent API across sketches.** Typed inputs (`SketchInput`) and uniform `insert`/`estimate`/`merge` patterns, with pluggable hashing via `SketchHasher`.
- **Algorithms not found elsewhere.** Includes `UnivMon` (universal monitoring), `Hydra` (hierarchical subpopulation sketching), and `NitroBatch`.
- **Built-in orchestration frameworks** — coordinate multiple sketches with shared hashing (`HashLayer`), manage sliding windows (`ExponentialHistogram`), or run hierarchical queries (`Hydra`).

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
| Advanced frameworks | `Hydra`, `UnivMon`, `NitroBatch` | Hierarchical queries, universal monitoring, batch sampling | No direct equivalent |

Full sketch status and API details: [APIs Index](./docs/apis.md).

## Quick Start

**Minimum Supported Rust Version (MSRV): 1.85** (Rust 2024 edition)

Add to your `Cargo.toml`:

```toml
[dependencies]
asap_sketchlib = { git = "https://github.com/ProjectASAP/asap_sketchlib" }
```

### Count distinct users with HyperLogLog

```rust
use asap_sketchlib::{ErtlMLE, HyperLogLog, SketchInput};

// HyperLogLog estimates the number of distinct items in a stream using fixed memory.
// ErtlMLE is one of the HLL variants we offer — it tends to be more accurate than
// the classic ("Regular") version, especially at very low or very high cardinalities.
let mut hll = HyperLogLog::<ErtlMLE>::default();

// Insert some user IDs — duplicates are fine, HLL handles them.
for user_id in [101, 202, 303, 101, 404, 202, 505, 101] {
    hll.insert(&SketchInput::U64(user_id));
}

let unique_users = hll.estimate();
println!("estimated unique users: {unique_users}"); // ≈ 5
```

### Estimate frequency of items with Count-Min Sketch

```rust
use asap_sketchlib::{CountMin, FastPath, Vector2D, SketchInput};

// Count-Min Sketch estimates how often each item appears in a stream.
// It may over-count but never under-counts.
//
// Vector2D<i32> is the backing storage (a 2D array of 32-bit counters).
// FastPath uses a single hash with bit-masking to pick row indices — faster
// than the default RegularPath which hashes once per row.
let mut cms = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 2048);

// Simulate an event stream
for _ in 0..1000 { cms.insert(&SketchInput::Str("page_view")); }
for _ in 0..50  { cms.insert(&SketchInput::Str("purchase")); }

let page_views = cms.estimate(&SketchInput::Str("page_view"));
let purchases  = cms.estimate(&SketchInput::Str("purchase"));
println!("page_view ≈ {page_views}, purchase ≈ {purchases}");
```

### Track latency percentiles with KLL

```rust
use asap_sketchlib::{KLL, SketchInput};

// KLL is a quantile sketch — it tracks the distribution of values so you can
// ask questions like "what is the median?" without storing every data point.
let mut sketch = KLL::default();

// Simulate 1000 latency samples in milliseconds
for i in 0..1000 {
    let ms = (i as f64) * 0.5 + 1.0;
    sketch.update(&SketchInput::F64(ms)).unwrap();
}

let p50 = sketch.quantile(0.50);
let p99 = sketch.quantile(0.99);
println!("median ≈ {p50:.1} ms, p99 ≈ {p99:.1} ms");
```

### Merge sketches from distributed nodes

```rust
use asap_sketchlib::{ErtlMLE, HyperLogLog, SketchInput};

// Sketches are mergeable — you can build one per node and combine them later
// to get a global answer without shipping raw data.
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

Insertion throughput on 10M Zipf-distributed values, averaged over 10 runs:

![CMS Insertion Throughput](./docs/benchmark_plots/plots/cms/cms_throughput_insertion.png)

![HLL Insertion Throughput](./docs/benchmark_plots/plots/hll/hll_throughput_insertion.png)

More benchmark results and performance details (cache-friendly layouts, `FastPath` single-hash mode) are in [Performance Notes](./docs/features.md).

## Documentation

| Doc | Contents |
| --- | --- |
| [APIs Index](./docs/apis.md) | Per-sketch API reference with status and error guarantees |
| [Advanced Use Cases](./docs/advanced_use_cases.md) | Hierarchical queries, windowed sketching, multi-sketch coordination |
| [Docs Index](./docs/index.md) | Full documentation index |

## Dev Commands

```bash
cargo build --all-targets
cargo test --all-features
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

Licensed under the MIT License. See [LICENSE](https://github.com/ProjectASAP/asap_sketchlib/blob/main/LICENSE).
