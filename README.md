# sketchlib-rust

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

`sketchlib-rust` is a Rust sketch library with reusable sketch building blocks, sketch implementations, and orchestration frameworks.

## Supported Sketches

| Goal | Use This | When to pick it |
|---|---|---|
| Frequency estimation | `CountMin`, `Count Sketch` | You need fast approximate counts for high-volume keys. |
| Cardinality estimation | `HyperLogLog` (`Regular`, `DataFusion`, `HIP`) | You need approximate distinct counts with bounded memory. |
| Quantiles/distribution | `KLL`, `DDSketch` | You need percentile/latency summaries over streams. |
| Multi-sketch orchestration/windowing | `Hydra`, `UnivMon`, `HashLayer`, `ExponentialHistogram`, `EHUnivOptimized`, `NitroBatch`, `Orchestrator` | You need hierarchical queries, sketch coordination, or sliding-window aggregation. |

Full sketch status and API details: [APIs Index](./docs/apis.md).

## Quick Start

Simple demo use case: estimate unique users with HyperLogLog.
`sketchlib-rust` is a sketch library for native rust sketch, with potential optimization. This repo contains mainly these parts:

- **Building blocks**: located in `/src/common`, contains common structure to build sketches and other common utilities
  - More detail about building block can be found in: [common api](./docs/common_api.md)
- **Native Sketch**: located in `/src/sketches`, contains Rust sketch implementations built on common structures where applicable
  - Core structured sketches include: CountMin, Count, HyperLogLog
- **Sketch Framework**: located in `/src/sketch_framework`, contains sketch serving/orchestration strategies
  - Includes: Hydra, UnivMon, HashLayer, ExponentialHistogram, Nitro, Orchestrator
- **Optimization**: integrated into sketches implementation
  - More detail about optimization techniques/features can be found in: [features](./docs/features.md)

## Current State

- ✅ Core structured sketches are available and actively used: `CountMin`, `Count`, `HyperLogLog`, `KLL`
- ✅ Framework coverage includes `Hydra`, `UnivMon`, `HashLayer`, `ExponentialHistogram`, and `NitroBatch`
- ✅ Folded window sketches are implemented: `FoldCMS` and `FoldCS` ([design doc](./docs/fold_sketch_design.md))
- ✅ Optimized EH path is implemented via `EHUnivOptimized` (hybrid map + sketch tiers with sketch pooling)
- ✅ All built-in sketches and frameworks use the shared `xxh3` helpers in `src/common/hash.rs`
- 🚧 Ongoing work focuses on feature expansion, broader test coverage, benchmark depth, serialization coverage, and API stabilization

## API Overview

There are three sections in the API overview section:

- Built-in `enum` for various purpose is introduced first
- Core sketches and sketch frameworks are introduced with their example usage
- Legacy sketches that is not migrated to [common api](./docs/common_api.md) yet.

Only introductory usage is provided here. For full API list, please check [sketch api](./docs/sketch_api.md).

### Provided Enum

There are some built-in enum to make it easier to use the sketch.

#### SketchInput

`SketchInput` is a enum that wraps around various input type. It supports multiple primitive types and formats, eliminating the need for per-sketch type conversions / type-specific insertion function.

**Signed Integers:**

- `I8(i8)`, `I16(i16)`, `I32(i32)`, `I64(i64)`, `I128(i128)`, `ISIZE(isize)`

**Unsigned Integers:**

- `U8(u8)`, `U16(u16)`, `U32(u32)`, `U64(u64)`, `U128(u128)`, `USIZE(usize)`

**Floating Point:**

- `F32(f32)`, `F64(f64)`

**Text/Binary:**

- `Str(&'a str)` - borrowed string slice
- `String(String)` - owned string
- `Bytes(&'a [u8])` - borrowed byte slice

Example usage:

```rust
use sketchlib_rust::SketchInput;

let int_key = SketchInput::U64(12345);
let str_key = SketchInput::Str("user_id");
let string_key = SketchInput::String("event_name".to_string());
let float_key = SketchInput::F64(3.14159);
```

#### L2HH

`L2HH` is an enum wrapper for Count Sketch variants that track both frequency estimates and L2 norm (second frequency moment). It is primarily used internally by UnivMon for multi-moment estimation.

**Variants:**

- `COUNT(CountL2HH)` - Count Sketch with L2 heavy-hitter tracking

**Methods:**

- `update_and_est(&mut self, key: &SketchInput, value: i64) -> f64` - Updates the sketch and returns the frequency estimate (includes L2 update)
- `update_and_est_without_l2(&mut self, key: &SketchInput, value: i64) -> f64` - Updates without maintaining L2 state (faster for upper layers)
- `get_l2(&self) -> f64` - Returns the current L2 norm estimate
- `merge(&mut self, other: &L2HH)` - Merges another L2HH sketch

Example usage in UnivMon context:

```rust
use sketchlib_rust::common::input::L2HH;
use sketchlib_rust::CountL2HH;
use sketchlib_rust::SketchInput;

let mut l2hh = L2HH::COUNT(CountL2HH::with_dimensions(3, 2048));
let key = SketchInput::Str("flow_id");

// Update and get frequency estimate
let freq = l2hh.update_and_est(&key, 1);
println!("frequency: {}", freq);

// Get L2 norm
let l2_norm = l2hh.get_l2();
println!("L2 norm: {}", l2_norm);
```

#### HydraQuery

`HydraQuery` is an enum that specifies the type of query to perform on a Hydra sketch. Different sketch types support different query semantics.

**Variants:**

- `Frequency(SketchInput)` - Query the frequency/count of a specific item (for CountMin, Count, etc.)
- `Quantile(f64)` - Query the quantile at a threshold value (for KLL, DDSketch, etc.)
- `Cdf(f64)` - Query cumulative distribution up to a threshold value
- `Cardinality` - Query the number of distinct elements (for HyperLogLog, etc.)
- `L1Norm` - Query L1 norm (for UnivMon)
- `L2Norm` - Query L2 norm (for UnivMon)
- `Entropy` - Query Shannon entropy (for UnivMon)

Example usage:

```rust
use sketchlib_rust::common::input::{HydraQuery, HydraCounter};
use sketchlib_rust::{Hydra, DataFusion, HyperLogLog, SketchInput};

// Create Hydra with HyperLogLog for cardinality queries
let hll_template = HydraCounter::HLL(HyperLogLog::<DataFusion>::new());
let mut hydra = Hydra::with_dimensions(3, 128, hll_template);

// Insert data
for id in 0..1000 {
    hydra.update("region=us-west", &SketchInput::U64(id), None);
}

// Query cardinality
let card = hydra.query_key(vec!["region=us-west"], &HydraQuery::Cardinality);
println!("distinct count: {}", card);
```

#### HydraCounter

`HydraCounter` is an enum that wraps different sketch types for use within Hydra's multi-dimensional framework. Each variant supports specific query types.

**Variants:**

- `CM(CountMin<Vector2D<i32>, FastPath>)` - Count-Min Sketch for frequency queries
- `HLL(HyperLogLog<DataFusion>)` - HyperLogLog for cardinality queries
- `CS(Count<Vector2D<i32>, FastPath>)` - Count Sketch for frequency queries
- `KLL(KLL)` - KLL for quantile/CDF queries
- `UNIVERSAL(UnivMon)` - UnivMon for L1, L2, entropy, cardinality queries

**Methods:**

- `insert(&mut self, value: &SketchInput, count: Option<i32>)` - Inserts a value into the underlying sketch
- `query(&self, query: &HydraQuery) -> Result<f64, String>` - Queries the sketch; returns error if query type is incompatible
- `merge(&mut self, other: &HydraCounter) -> Result<(), String>` - Merges another counter; returns error if types differ

**Query Compatibility Matrix:**

| Sketch Type | Frequency | Quantile | Cdf | Cardinality | L1/L2/Entropy |
|-------------|-----------|----------|-----|-------------|---------------|
| CM          | yes       |          |     |             |               |
| CS          | yes       |          |     |             |               |
| HLL         |           |          |     | yes         |               |
| KLL         |           | yes      | yes |             |               |
| UNIVERSAL   |           |          |     | yes         | yes           |

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

### Core Sketches

This section documents the primary sketch implementations with their initialization, insertion, query, and merge APIs.

#### Count-Min Sketch (CMS)

Count-Min Sketch tracks approximate frequencies for keys using a 2D array of counters. It provides probabilistic guarantees on overestimation.

Initialize with default dimensions (3 rows x 4096 columns):

```rust
use sketchlib_rust::CountMin;

let mut cms = CountMin::default();
```

Or specify custom dimensions:

```rust
let mut cms = CountMin::with_dimensions(4, 2048);
```

Insert keys to track their frequency:

```rust
use sketchlib_rust::SketchInput;

let key = SketchInput::String("user_123".into());
cms.insert(&key);
cms.insert(&key);
```

Query the approximate frequency:

```rust
let estimate = cms.estimate(&key);
println!("estimated frequency: {}", estimate);
```

Merge two Count-Min sketches (must have identical dimensions):

```rust
let mut cms1 = CountMin::with_dimensions(3, 64);
let mut cms2 = CountMin::with_dimensions(3, 64);
let key = SketchInput::Str("event");

cms1.insert(&key);
cms2.insert(&key);
cms2.insert(&key);

cms1.merge(&cms2);
assert_eq!(cms1.estimate(&key), 3);
```

#### Count Sketch (CS)

Count Sketch uses signed counters with hash-based sign determination to provide unbiased frequency estimates via median aggregation.

Initialize with default dimensions (3 rows x 4096 columns):

```rust
use sketchlib_rust::Count;

let mut cs = Count::default();
```

Or specify custom dimensions:

```rust
let mut cs = Count::with_dimensions(5, 8192);
```

Insert keys to track their frequency:

```rust
use sketchlib_rust::SketchInput;

let key = SketchInput::String("metric_name".into());
cs.insert(&key);
```

Query the approximate frequency (returns median estimate as f64):

```rust
let estimate = cs.estimate(&key);
println!("estimated frequency: {}", estimate);
```

Merge two Count sketches (must have identical dimensions):

```rust
let mut cs1 = Count::with_dimensions(3, 64);
let mut cs2 = Count::with_dimensions(3, 64);
let key = SketchInput::Str("counter");

cs1.insert(&key);
cs2.insert(&key);

cs1.merge(&cs2);
let merged_est = cs1.estimate(&key);
println!("merged estimate: {}", merged_est);
```

#### HyperLogLog (HLL)

HyperLogLog estimates the cardinality (number of distinct elements) in a stream with high accuracy and low memory footprint. Three variants are available:

- `HyperLogLog<Regular>` - Classic HyperLogLog algorithm, mergeable
- `HyperLogLog<DataFusion>` - Improved Ertl estimator (as used in DataFusion/Redis), mergeable
- `HyperLogLogHIP` - HIP estimator from Apache DataSketches, **not mergeable** but O(1) query

Initialize with default configuration (14-bit precision, 16384 registers):

```rust
use sketchlib_rust::{DataFusion, HyperLogLog};

let mut hll = HyperLogLog::<DataFusion>::new();
```

Insert elements to track distinct count:

```rust
use sketchlib_rust::SketchInput;

for user_id in 0..10_000u64 {
    hll.insert(&SketchInput::U64(user_id));
}
```

Query the estimated cardinality:

```rust
let cardinality = hll.estimate();
println!("approximate distinct count: {}", cardinality);
```

Merge two HyperLogLog sketches:

```rust
use sketchlib_rust::{DataFusion, HyperLogLog, SketchInput};

fn main() {
    let mut hll = HyperLogLog::<DataFusion>::new();

    for user_id in 0..10_000u64 {
        hll.insert(&SketchInput::U64(user_id));
    }

    let approx_unique_users = hll.estimate();
    println!("approx unique users = {}", approx_unique_users);
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
