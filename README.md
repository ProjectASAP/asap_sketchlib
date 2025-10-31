# sketchlib-rust

`sketchlib-rust` is a sketch library for native rust sketch, with potential optimization. This repo contains mainly these parts:

- **Building blocks**: located in `/src/common`, contains common structure to build sketches and other common utilities
  - More detail about building block can be found in: [common api](./docs/common_api.md)
- **Native Sketch**: located in `/src/sketches`, contains rust sketches, migration from hard coded sketch into common structure based sketches
  - Completed Migration: CountMin, HyperLogLog, Count
- **Sketch Framework**: located in `/src/sketch_framework`, contains sketch serving strategies
  - Complete Migration: Hydra, UnivMon
- **Optimization**: integrated into sketches implementation
  - More detail about optimization techniques/features can be found in: [features](./docs/features.md)
  - Benchmark related information can be found in: [benchmark](./docs/benchmark.md)

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
- `Quantile(f64)` - Query the CDF at a threshold value (for KLL, DDSketch, etc.)
- `Cardinality` - Query the number of distinct elements (for HyperLogLog, etc.)

Example usage:

```rust
use sketchlib_rust::common::input::{HydraQuery, HydraCounter};
use sketchlib_rust::{Hydra, HllDf, SketchInput};

// Create Hydra with HyperLogLog for cardinality queries
let hll_template = HydraCounter::HLL(HllDf::new());
let mut hydra = Hydra::with_dimensions(3, 128, hll_template);

// Insert data
for id in 0..1000 {
    hydra.update("region=us-west", &SketchInput::U64(id));
}

// Query cardinality
let card = hydra.query_key(vec!["region=us-west"], &HydraQuery::Cardinality);
println!("distinct count: {}", card);
```

#### HydraCounter

`HydraCounter` is an enum that wraps different sketch types for use within Hydra's multi-dimensional framework. Each variant supports specific query types.

**Variants:**

- `CM(CountMin)` - Count-Min Sketch for frequency queries
- `HLL(HllDf)` - HyperLogLog for cardinality queries

**Methods:**

- `insert(&mut self, value: &SketchInput)` - Inserts a value into the underlying sketch
- `query(&self, query: &HydraQuery) -> Result<f64, String>` - Queries the sketch; returns error if query type is incompatible
- `merge(&mut self, other: &HydraCounter) -> Result<(), String>` - Merges another counter; returns error if types differ

**Query Compatibility Matrix:**

| Sketch Type | Frequency | Quantile | Cardinality |
|-------------|-----------|----------|-------------|
| CM          | ✓         | ✗        | ✗           |
| HLL         | ✗         | ✗        | ✓           |

Example usage:

```rust
use sketchlib_rust::common::input::{HydraCounter, HydraQuery};
use sketchlib_rust::{CountMin, SketchInput};

// Create a CountMin-based counter
let mut counter = HydraCounter::CM(CountMin::with_dimensions(3, 64));

// Insert values
let key = SketchInput::String("event".into());
counter.insert(&key);
counter.insert(&key);

// Query frequency (compatible)
let freq = counter.query(&HydraQuery::Frequency(key)).unwrap();
println!("frequency: {}", freq);

// Query cardinality (incompatible - returns error)
match counter.query(&HydraQuery::Cardinality) {
    Ok(_) => println!("success"),
    Err(e) => println!("error: {}", e), // "CountMin does not support cardinality queries"
}
```

### Core Sketches

This section documents the primary sketch implementations with their initialization, insertion, query, and merge APIs.

#### Count-Min Sketch (CMS)

Count-Min Sketch tracks approximate frequencies for keys using a 2D array of counters. It provides probabilistic guarantees on overestimation.

Initialize with default dimensions (3 rows × 4096 columns):

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

Initialize with default dimensions (3 rows × 4096 columns):

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

HyperLogLog estimates the cardinality (number of distinct elements) in a stream with high accuracy and low memory footprint. Three variants are available: `HyperLogLog` (classic), `HllDf` (improved estimator), and `HllDs` (streaming estimator, non-mergeable).

Initialize with default configuration (14-bit precision, 16384 registers):

```rust
use sketchlib_rust::sketches::hll::HllDf;

let mut hll = HllDf::new();
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
let cardinality = hll.get_est();
println!("approximate distinct count: {}", cardinality);
```

Merge two HyperLogLog sketches:

```rust
let mut hll1 = HllDf::new();
let mut hll2 = HllDf::new();

for i in 0..5_000u64 {
    hll1.insert(&SketchInput::U64(i));
}
for i in 2_500..7_500u64 {
    hll2.insert(&SketchInput::U64(i));
}

hll1.merge(&hll2);
let total_distinct = hll1.get_est();
println!("merged cardinality: {}", total_distinct);
```

### Sketch Frameworks

#### UnivMon

UnivMon provides a multi-layer pyramid structure for computing frequency moments (L1, L2, cardinality, entropy) over streams using Count Sketch layers and heavy-hitter tracking.

Initialize with custom parameters (k=heap size, rows, columns, layers, pool_idx):

```rust
use sketchlib_rust::sketch_framework::univmon::UnivMon;

let mut univmon = UnivMon::init_univmon(32, 3, 1024, 4, 0);
```

Insert items with their bottom layer (determined by hash):

```rust
use sketchlib_rust::{BOTTOM_LAYER_FINDER, SketchInput, hash_it};

let key = "flow::123";
let input = SketchInput::Str(key);
let hash = hash_it(BOTTOM_LAYER_FINDER, &input);
let bottom_layer = univmon.find_bottom_layer_num(hash, univmon.layer);

univmon.update(key, 1, bottom_layer);
```

Query various statistics:

```rust
let cardinality = univmon.calc_card();
let l1_norm = univmon.calc_l1();
let l2_norm = univmon.calc_l2();
let entropy = univmon.calc_entropy();

println!("cardinality: {}", cardinality);
println!("L1: {}, L2: {}", l1_norm, l2_norm);
println!("entropy: {}", entropy);
```

Merge two UnivMon sketches (must have identical structure):

```rust
let mut um1 = UnivMon::init_univmon(32, 3, 1024, 4, 0);
let mut um2 = UnivMon::init_univmon(32, 3, 1024, 4, 0);

// Insert data into both
let key1 = "flow_a";
let input1 = SketchInput::Str(key1);
let hash1 = hash_it(BOTTOM_LAYER_FINDER, &input1);
let bottom1 = um1.find_bottom_layer_num(hash1, um1.layer);
um1.update(key1, 10, bottom1);

let key2 = "flow_b";
let input2 = SketchInput::Str(key2);
let hash2 = hash_it(BOTTOM_LAYER_FINDER, &input2);
let bottom2 = um2.find_bottom_layer_num(hash2, um2.layer);
um2.update(key2, 15, bottom2);

um1.merge_with(&um2);
println!("merged L1: {}", um1.calc_l1());
```

#### Hydra

Hydra coordinates multi-dimensional queries by maintaining sketches for all label combinations. It accepts semicolon-delimited keys and automatically fans updates across subpopulations.

Initialize with sketch template (uses CountMin by default):

```rust
use sketchlib_rust::Hydra;
use sketchlib_rust::CountMin;
use sketchlib_rust::common::input::HydraCounter;

let template = HydraCounter::CM(CountMin::with_dimensions(3, 64));
let mut hydra = Hydra::with_dimensions(3, 128, template);
```

Insert with multi-dimensional keys (**semicolon-separated**):

```rust
use sketchlib_rust::SketchInput;

let value = SketchInput::String("error".into());
hydra.update("service=api;route=/users;status=500", &value);
```

Query specific label combinations:

```rust
// Query 2D combination
let estimate = hydra.query_frequency(vec!["service=api", "status=500"], &value);
println!("api + 500 errors: {}", estimate);

// Query single dimension
let service_total = hydra.query_frequency(vec!["service=api"], &value);
println!("all api errors: {}", service_total);
```

Hydra with HyperLogLog for cardinality queries:

```rust
use sketchlib_rust::HllDf;
use sketchlib_rust::common::input::{HydraCounter, HydraQuery};

let hll_template = HydraCounter::HLL(HllDf::new());
let mut hydra = Hydra::with_dimensions(5, 128, hll_template);

// Insert user IDs with labels
for user_id in 0..1000 {
    let value = SketchInput::U64(user_id);
    hydra.update("region=us-west;device=mobile", &value);
}

// Query distinct users by region
let cardinality = hydra.query_key(vec!["region=us-west"], &HydraQuery::Cardinality);
println!("distinct users in us-west: {}", cardinality);
```

### Additional Sketches

The library includes additional specialized sketches. Each follows a consistent lifecycle: construct, insert, query, and optionally merge.

#### KLL (quantile CDF)

Prepare two KLL sketches.

```rust
use sketchlib_rust::sketches::kll::KLL;

let mut sketch = KLL::init_kll(200);
let mut peer = KLL::init_kll(200);
```

Stream values into each sketch.

```rust
for sample in [12.0, 18.0, 21.0, 35.0, 42.0] {
    sketch.update(sample);
}
for sample in [30.0, 33.0, 38.0] {
    peer.update(sample);
}
```

Merge the peer CDF into the primary.

```rust
sketch.merge(&peer);
```

Query the cumulative distribution for a threshold.

```rust
let cdf = sketch.quantile(32.0);
println!("fraction of samples <= 32 ≈ {cdf:.3}");
```

#### Elastic (heavy + light split)

Create an Elastic sketch with a heavy bucket array.

```rust
use sketchlib_rust::Elastic;

let mut flows = Elastic::init_with_length(16);
```

Insert flow identifiers into the structure.

```rust
for id in ["api/login", "api/login", "api/search"] {
    flows.insert(id.to_string());
}
```

Query both the heavy bucket and the backing Count-Min.

```rust
let heavy = flows.query("api/login".to_string());
let light = flows.query("api/search".to_string());
println!("heavy flow estimate = {heavy}, light flow estimate = {light}");
```

#### Coco (substring aggregation)

Allocate primary and secondary Coco tables.

```rust
use sketchlib_rust::sketches::{coco::Coco, utils::SketchInput};

let mut coco = Coco::init_with_size(64, 4);
let mut shard = Coco::init_with_size(64, 4);
let key = SketchInput::String("region=us-west|id=42".into());
```

Insert weighted updates for composite keys.

```rust
coco.insert(&key, 5);
coco.insert(&key, 1);
shard.insert(&SketchInput::String("region=us-west|id=13".into()), 3);
```

Estimate using substring matches.

```rust
let regional = coco.estimate(SketchInput::Str("us-west"));
println!("regional count ≈ {}", regional);
```

Merge the shard back into the primary sketch.

```rust
coco.merge(&shard);
```

#### Locher (heavy hitter sampling)

Construct a Locher sketch with three rows of Top-K heaps.

```rust
use sketchlib_rust::sketches::locher::LocherSketch;

let mut sketch = LocherSketch::new(3, 64, 5);
let key = "endpoint=/checkout".to_string();
```

Insert repeated events for a heavy hitter.

```rust
for _ in 0..50 {
    sketch.insert(&key, 1);
}
```

Estimate the adjusted heavy-hitter count.

```rust
println!("heavy estimate ≈ {}", sketch.estimate(&key));
```

### `Chapter`: one enum to drive them all

`Chapter` wraps each sketch in a single enum so callers can build pipelines without matching on individual types. The enum normalizes `insert`, `merge`, and `query` across the different sketches and returns helpful errors when an operation is not supported.

Construct two `Chapter` wrappers over Count-Min sketches.

```rust
use sketchlib_rust::{
    sketchbook::Chapter,
    sketches::{countmin::CountMin, utils::SketchInput},
};

let mut counts = Chapter::CM(CountMin::default());
let mut canary = Chapter::CM(CountMin::default());
let key = SketchInput::String("endpoint=/search".into());
```

Insert values through the unified enum interface.

```rust
counts.insert(&key);
canary.insert(&key);
```

Merge compatible `Chapter` variants.

```rust
counts.merge(&canary)?;
```

Query estimates without matching on the underlying sketch.

```rust
let estimate = counts.query(&key)?;
println!("merged chapter estimate = {estimate}");
```

When the underlying sketch does not implement an operation (for example, Locher lacks merge support today), `Chapter::merge` returns an error explaining the mismatch.

### Exponential Histogram: Time-bounded aggregates

Initialize the windowed coordinator with a sketch template.

```rust
use sketchlib_rust::{
    sketchbook::{Chapter, ExponentialHistogram},
    sketches::countmin::CountMin,
    SketchInput,
};

let template = Chapter::CM(CountMin::default());
let mut eh = ExponentialHistogram::new(3, 120, template);
```

Insert timestamped events.

```rust
eh.update(10, &SketchInput::String("flow".into()));
eh.update(70, &SketchInput::String("flow".into()));
```

Query the merged sketch for a given interval.

```rust
if let Some(volume) = eh.query_interval_merge(0, 120) {
    let estimate = volume.query(&SketchInput::String("flow".into())).unwrap();
    println!("approximate count inside window = {}", estimate);
}
```

## Quick Start
<!-- - Install a Rust toolchain that supports edition 2024 (currently nightly via `rustup toolchain install nightly`).
- Build everything: `cargo build --all-targets`.
- Run the library tests: `cargo test --all-features`.
- Explore the sketch demos: `cargo run --bin test_all_sketch` or any tester in `src/bin/sketch_tester`. -->
At this moment, ```cargo test``` is a good starting point.

## Library Map

### Core Modules

- **`src/common/`** - Foundation for all sketches ([common_api.md](./docs/common_api.md))
  - `input.rs` - `SketchInput` enum, `HHItem`, framework enums (`HydraCounter`, `L2HH`, `HydraQuery`)
  - `structures.rs` - High-performance data structures (`Vector1D`, `Vector2D`, `Vector3D`, `CommonHeap`)
  - `heap.rs` - `HHHeap` convenience wrapper for heavy hitter tracking
  - Hashing utilities (`hash_it`, `SEEDLIST`) for deterministic sketch operations

- **`src/sketches/`** - Core sketch implementations ([sketch_api.md](./docs/sketch_api.md))
  - ✅ **Recommended** (built on common structures): `countmin.rs`, `count.rs`, `hll.rs`
  - ⚠️ **Legacy**: `coco.rs`, `elastic.rs`, `kll.rs`, `uniform.rs`, `microscope.rs`, `locher.rs`
  - `structured/` - Migration zone for sketches adopting common API

- **`src/sketch_framework/`** - Orchestration and serving layers
  - `chapter.rs` - Unified interface (`Chapter` enum) wrapping all sketch types
  - `hydra.rs` - Multi-dimensional hierarchical heavy hitters
  - `univmon.rs` - Universal monitoring (L1, L2, entropy, cardinality)
  - `eh.rs` - Exponential histogram for sliding window queries

### Testing & Benchmarking

- **`benches/`** - Criterion-based performance benchmarks
  - Run with: `cargo bench --bench structured_countmin`

### Documentation

- **`docs/`** - Comprehensive API and feature documentation
  - [sketch_api.md](./docs/sketch_api.md) - Complete sketch API reference with usage examples
  - [common_api.md](./docs/common_api.md) - Data structures and shared utilities
  - [features.md](./docs/features.md) - Feature status and roadmap
  - [benchmark.md](./docs/benchmark.md) - Performance testing methodology
  - [timeline.md](./docs/timeline.md) - Development history

### Legacy/Experimental

- **`src/deserializers/`** - Hex-encoded MessagePack deserialization (for Arroyo/PromSketch interop)
- **`src/bin/serializer/`** - Tools for generating serialized test fixtures
- **`localsketch/`** and **`testdata/`** - Canned sketches and fixtures for reproducible testing

## Common Structure

To build new sketch with the Common API, check [this](./docs/common_api.md)

## Development

- Format sources with `cargo fmt` before committing changes.
- Lint with `cargo clippy --all-targets --all-features` to catch obvious mistakes across sketches and orchestration layers.
<!-- - Run targeted binaries such as `cargo run --bin cm_test` when iterating on a specific sketch. -->
<!-- - Regenerate serialized fixtures via the serializer binaries whenever sketch layouts change. -->

<!-- ## Status & Next Steps

- Early-stage code: APIs may change, and several sketches are still being tuned for accuracy.
- Some components (for example Elastic merges or Hydra's public update surface) remain works in progress.
- Cross-language support currently targets PromSketch and Go; extend the deserializers if new consumers appear.
- Contributions and experiment results are welcome—open an issue describing the workload or sketch you plan to add.
- Missing many testing
- Missing many serialization and deserialization support -->

## Future Update TimeLine

For planned future update, please check [timeline.md](./docs/timeline.md) for more detail.
