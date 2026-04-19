# asap_sketchlib

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/ProjectASAP/asap_sketchlib/blob/main/LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://blog.rust-lang.org/2025/02/20/Rust-1.85.0.html)

A Rust library for **streaming data sketches** — compact data structures that give approximate answers (counts, distinct counts, percentiles) over data streams too large to store exactly.

## Why asap_sketchlib

- **Fast.** Up to 8–14× higher insertion throughput than comparable libraries on frequency sketches, 2–3× on cardinality sketches, and 2–4× on quantile sketches. Rust-native with no language-boundary overhead. See [benchmarks](#performance).
- **High coverage.** Supports frequency, cardinality, quantile, and distribution sketches (`CountMin`, `Count Sketch`, `HyperLogLog`, `KLL`, `DDSketch`). Also includes algorithms not found in other libraries: `UnivMon` for estimating a broad class of streaming statistics (L1/L2 norms, entropy) in a single pass, `Hydra` for answering sketch queries over arbitrary subpopulations without per-group sketches, and `NitroBatch` for accelerating sketch updates through batching. Unique sketch frameworks for sliding windows (`ExponentialHistogram`) and subpopulation queries (`Hydra`).
- **Easy to use.** Uniform `insert`/`estimate`/`merge` API across all sketches, input data type (`DataInput`) for typed inputs, and pluggable hashing input to sketches via `SketchHasher`. Composite multiple sketches with shared hashing (`HashLayer`).

## Supported Sketches

| Goal | Sketch | When to pick it | What it does | Polars equivalent |
| --- | --- | --- | --- | --- |
| Frequency estimation | `CountMin`, `Count Sketch` | Fast approximate counts for high-volume keys | Estimates how often each key appears in a stream | `df.group_by("key").agg(pl.len())` |
| Cardinality estimation | `HyperLogLog` (`Classic`, `ErtlMLE`, `HIP`) | Approximate distinct counts with bounded memory | Estimates the number of unique elements | `df["col"].n_unique()` |
| Quantiles / distribution | `KLL`, `DDSketch` | Percentile / latency summaries over streams | Approximates arbitrary quantiles (e.g. p50, p99) of a value distribution | `df["col"].quantile(0.99)` |
| Subpopulation queries | `Hydra` | Hierarchical / filtered sketch queries | Answers sketch queries over arbitrary subpopulations without maintaining per-group sketches | No direct equivalent — requires per-group aggregation |
| Universal monitoring | `UnivMon` | G-sum queries (L1/L2 norms, cardinality, entropy) | Estimates a broad class of streaming statistics in a single pass | No direct equivalent — requires custom multi-pass pipelines |
| Update acceleration | `NitroBatch` | Batch-accelerated sketch updates | Speeds up sketch insertions by batching updates | No direct equivalent |

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
use asap_sketchlib::{ErtlMLE, HyperLogLog, DataInput};

// HyperLogLog estimates the number of distinct items in a stream using fixed memory.
// ErtlMLE is one of the HLL variants we offer — it tends to be more accurate than
// the `Classic` variant, especially at very low or very high cardinalities.
let mut hll = HyperLogLog::<ErtlMLE>::default();

// Insert some user IDs — HLL handles distinct counting and deduplicates items.
for user_id in [101, 202, 303, 101, 404, 202, 505, 101] {
    hll.insert(&DataInput::U64(user_id));
}

let unique_users = hll.estimate();
println!("estimated unique users: {unique_users}"); // ≈ 5
```

### Estimate frequency of items with Count-Min Sketch

```rust
use asap_sketchlib::{CountMin, FastPath, Vector2D, DataInput};

// Count-Min Sketch estimates how often each item appears in a stream.
// It may over-count but never under-counts.
//
// Vector2D<i32> is the backing storage (a 2D array of 32-bit counters).
// FastPath uses a single hash with bit-masking to pick row indices — faster
// than the default RegularPath which hashes once per row.
let mut cms = CountMin::<Vector2D<i32>, FastPath>::with_dimensions(3, 2048);

// Simulate an event stream with known frequencies.
let events = [
    ("page_view", 1000),
    ("click",      500),
    ("signup",     100),
    ("purchase",    50),
];
for &(event, count) in &events {
    for _ in 0..count {
        cms.insert(&DataInput::Str(event));
    }
}

// Estimates are close to the true counts (CMS may over-count, but never under-counts).
for &(event, true_count) in &events {
    let est = cms.estimate(&DataInput::Str(event));
    println!("{event:>10}: estimate = {est}, true = {true_count}");
}
```

### Track latency percentiles with KLL

```rust
use asap_sketchlib::KLL;

// KLL is a quantile sketch — it tracks the distribution of values so you can
// ask questions like "what is the median?" without storing every data point.
let mut sketch = KLL::<f64>::default();

// Simulate 1000 latency samples in milliseconds
for i in 0..1000 {
    let ms = (i as f64) * 0.5 + 1.0;
    sketch.update(&ms);
}

let p50 = sketch.quantile(0.50);
let p99 = sketch.quantile(0.99);
println!("median ≈ {p50:.1} ms, p99 ≈ {p99:.1} ms");
```

### Merge multiple sketch instances

```rust
use asap_sketchlib::{ErtlMLE, HyperLogLog, DataInput};

// Sketches are mergeable — you can build one per node and combine them later
// to get a global answer without shipping raw data.
let mut node_a = HyperLogLog::<ErtlMLE>::default();
let mut node_b = HyperLogLog::<ErtlMLE>::default();

// Each node sees different (and some overlapping) users
for id in [1, 2, 3, 4, 5]  { node_a.insert(&DataInput::U64(id)); }
for id in [4, 5, 6, 7, 8]  { node_b.insert(&DataInput::U64(id)); }

node_a.merge(&node_b);
println!("total unique users ≈ {}", node_a.estimate()); // ≈ 8
```

## Choosing Between Sketches

Several sketches address the same goal with different trade-offs — for example, `CountMin` vs `Count Sketch` for frequency, or `KLL` vs `DDSketch` for quantiles.

We are building **SketchPlan**, a profiler that analyzes a representative sample of your data and recommends the best sketch configuration (algorithm, memory budget, error tolerance) for your workload. Until SketchPlan is ready, the [APIs Index](./docs/apis.md) lists guarantees, error bounds, and caveats for each sketch to help you decide.

## Performance

Insertion throughput on 10M Zipf-distributed values, averaged over 10 runs:

![CMS Insertion Throughput](./docs/benchmark_plots/plots/cms/cms_throughput_insertion.png)

![HLL Insertion Throughput](./docs/benchmark_plots/plots/hll/hll_throughput_insertion.png)

![KLL Insertion Throughput](./docs/benchmark_plots/plots/kll/kll_throughput_insertion.png)

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

- `--all-targets` builds everything: the library, binaries, and tests.
- `--all-features` enables every Cargo feature, so all feature-gated code is compiled and tested. The features include:
  - `experimental` — enables sketches and APIs that are still under development and may change without notice.
  - `octo-runtime` — enables the Octo multi-threaded runtime (pulls in `core_affinity` and `crossbeam-channel`).

To build or test with a specific feature:

```bash
cargo build --features experimental
cargo test --features "experimental octo-runtime"
```

## Protobuf Requirements

This project currently compiles `.proto` files at build time via `prost-build` in `build.rs`.
That means the Protocol Buffers compiler (`protoc`) must be installed on your system before running `cargo build` or `cargo test`.

Install `protoc`:

```bash
# macOS (Homebrew)
brew install protobuf

# Ubuntu / Debian
sudo apt-get update && sudo apt-get install -y protobuf-compiler

# Windows (Chocolatey)
choco install protoc
```

Verify installation:

```bash
protoc --version
```

If `protoc` is missing, the build will fail with a `prost_build` compile error.

## FAQ

### When is Apache DataSketches a better fit?

- You need its broader algorithm catalog (CPC, Theta/Tuple with set operators, REQ, VarOpt/Reservoir, FM85).
- You need cross-language binary compatibility with existing DataSketches deployments in Java, C++, or Python.
- You need long-running production maturity and an Apache-governed release cycle.

## Contributors

### Major Contributors

- [Yancheng Yuan](https://github.com/GordonYuanyc)
- [Zeying Zhu](https://github.com/zzylol)

### Other Contributors

- [Sie Deta Dirganjaya](https://github.com/SieDeta)
- [Gnanesh Gnani](https://github.com/GnaneshGnani)

## License

Copyright 2025 - present ProjectASAP

Licensed under the MIT License. See [LICENSE](https://github.com/ProjectASAP/asap_sketchlib/blob/main/LICENSE).
