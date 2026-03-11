# sketchlib-rust

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

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
- ✅ Hashing is customizable through `SketchHasher` (default: `DefaultXxHasher`)
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

let mut hll1 = HyperLogLog::<DataFusion>::new();
let mut hll2 = HyperLogLog::<DataFusion>::new();

for i in 0..5_000u64 {
    hll1.insert(&SketchInput::U64(i));
}
for i in 2_500..7_500u64 {
    hll2.insert(&SketchInput::U64(i));
}

hll1.merge(&hll2);
let total_distinct = hll1.estimate();
println!("merged cardinality: {}", total_distinct);
```

### Sketch Frameworks

#### UnivMon

UnivMon provides a multi-layer pyramid structure for computing frequency moments (L1, L2, cardinality, entropy) over streams using Count Sketch layers and heavy-hitter tracking.

Initialize with custom parameters (heap_size, sketch_rows, sketch_cols, layer_size):

```rust
use sketchlib_rust::UnivMon;

let mut univmon = UnivMon::init_univmon(32, 3, 1024, 4);
```

Insert items (hashing and layer assignment are handled internally):

```rust
use sketchlib_rust::SketchInput;

let key = SketchInput::Str("flow::123");
univmon.insert(&key, 1);
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
use sketchlib_rust::{UnivMon, SketchInput};

let mut um1 = UnivMon::init_univmon(32, 3, 1024, 4);
let mut um2 = UnivMon::init_univmon(32, 3, 1024, 4);

um1.insert(&SketchInput::Str("flow_a"), 10);
um2.insert(&SketchInput::Str("flow_b"), 15);

um1.merge(&um2);
println!("merged L1: {}", um1.calc_l1());
```

#### HashLayer

HashLayer provides a performance optimization for managing multiple sketches by computing hash values once and reusing them across all sketches. It uses `OrchestratedSketch` from the orchestrator module.

Initialize with default sketches (CountMin, Count, HyperLogLog):

```rust
use sketchlib_rust::HashLayer;

let mut layer = HashLayer::default();
```

Or create with custom sketch configuration:

```rust
use sketchlib_rust::{
    CountMin, Count, DataFusion, FastPath, FreqSketch, HashLayer,
    HyperLogLog, OrchestratedSketch, CardinalitySketch, Vector2D,
};

let sketches = vec![
    OrchestratedSketch::Freq(FreqSketch::CountMin(
        CountMin::<Vector2D<i32>, FastPath>::default(),
    )),
    OrchestratedSketch::Freq(FreqSketch::Count(
        Count::<Vector2D<i32>, FastPath>::default(),
    )),
    OrchestratedSketch::Cardinality(CardinalitySketch::HllDf(
        HyperLogLog::<DataFusion>::default(),
    )),
];
let mut layer = HashLayer::new(sketches).unwrap();
```

Insert to all sketches with hash computed once:

```rust
use sketchlib_rust::SketchInput;

for value in 0..10_000 {
    let input = SketchInput::U64(value);
    layer.insert_all(&input);  // Hash computed once, reused for all sketches
}
```

Insert to specific sketch indices:

```rust
let input = SketchInput::U64(42);
layer.insert_at(&[0, 1], &input);  // Only insert to sketches at index 0 and 1
```

Query specific sketches by index:

```rust
let input = SketchInput::U64(42);
let estimate = layer.query_at(0, &input).unwrap();  // Query sketch at index 0
println!("estimate from sketch 0: {}", estimate);
```

#### Hydra

Hydra coordinates multi-dimensional queries by maintaining sketches for all label combinations. It accepts semicolon-delimited keys and automatically fans updates across subpopulations.

Initialize with sketch template (uses CountMin by default):

```rust
use sketchlib_rust::{Hydra, CountMin, FastPath, Vector2D};
use sketchlib_rust::common::input::HydraCounter;

let template = HydraCounter::CM(CountMin::<Vector2D<i32>, FastPath>::default());
let mut hydra = Hydra::with_dimensions(3, 128, template);
```

Insert with multi-dimensional keys (**semicolon-separated**):

```rust
use sketchlib_rust::SketchInput;

let value = SketchInput::String("error".into());
hydra.update("service=api;route=/users;status=500", &value, None);
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
use sketchlib_rust::{DataFusion, HyperLogLog, SketchInput};
use sketchlib_rust::common::input::{HydraCounter, HydraQuery};

let hll_template = HydraCounter::HLL(HyperLogLog::<DataFusion>::new());
let mut hydra = Hydra::with_dimensions(5, 128, hll_template);

// Insert user IDs with labels
for user_id in 0..1000 {
    let value = SketchInput::U64(user_id);
    hydra.update("region=us-west;device=mobile", &value, None);
}

// Query distinct users by region
let cardinality = hydra.query_key(vec!["region=us-west"], &HydraQuery::Cardinality);
println!("distinct users in us-west: {}", cardinality);
```

### Additional Sketches

The library includes additional specialized sketches. Each follows a consistent lifecycle: construct, insert, query, and optionally merge.

#### KLL (quantile estimation)

```rust
use sketchlib_rust::{KLL, SketchInput};

let mut sketch = KLL::init_kll(200);
let mut peer = KLL::init_kll(200);
```

Stream values into each sketch:

```rust
for sample in [12.0, 18.0, 21.0, 35.0, 42.0] {
    sketch.update(&SketchInput::F64(sample)).unwrap();
}
for sample in [30.0, 33.0, 38.0] {
    peer.update(&SketchInput::F64(sample)).unwrap();
}
```

Merge the peer into the primary:

```rust
sketch.merge(&peer);
```

Query quantiles (argument is a rank in [0, 1]):

```rust
let median = sketch.quantile(0.5);
println!("median value ≈ {median:.3}");
```

Or use the CDF interface for value-based queries:

```rust
let cdf = sketch.cdf();
let fraction = cdf.quantile(32.0);  // fraction of values <= 32
println!("fraction of samples <= 32 ≈ {fraction:.3}");
```

#### DDSketch (relative-error quantiles, mergeable)

DDSketch provides approximate quantile estimation with configurable relative error guarantees.

```rust
use sketchlib_rust::DDSketch;

// alpha is the relative error bound
let mut dds = DDSketch::new(0.01);
```

Insertion:

```rust
dds.add(1.0);
dds.add(5.2);
dds.add(42.0);
```

Quantile queries:

```rust
let p50 = dds.get_value_at_quantile(0.50).unwrap();
let p90 = dds.get_value_at_quantile(0.90).unwrap();
let p99 = dds.get_value_at_quantile(0.99).unwrap();
```

Merge (two DDSketch instances must share the same `alpha`):

```rust
let mut a = DDSketch::new(0.01);
let mut b = DDSketch::new(0.01);

a.add(1.0);
a.add(2.0);
b.add(10.0);
b.add(20.0);

a.merge(&b);
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
use sketchlib_rust::{Coco, SketchInput};

let mut coco = Coco::init_with_size(64, 4);
let mut shard = Coco::init_with_size(64, 4);
```

Insert weighted updates for composite keys.

```rust
coco.insert("region=us-west|id=42", 5);
coco.insert("region=us-west|id=42", 1);
shard.insert("region=us-west|id=13", 3);
```

Estimate using substring matches.

```rust
let regional = coco.estimate("us-west");
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

### `EHSketchList`: one enum to drive them all

`EHSketchList` wraps each sketch in a single enum so callers can build pipelines without matching on individual types. The enum normalizes `insert`, `merge`, and `query` across the different sketches and returns helpful errors when an operation is not supported.

**Variants:**

- `CM(CountMin<Vector2D<i32>, FastPath>)` - Count-Min Sketch
- `CS(Count<Vector2D<i32>, FastPath>)` - Count Sketch
- `COUNTL2HH(CountL2HH)` - Count Sketch with L2 heavy hitters
- `HLL(HyperLogLog<DataFusion>)` - HyperLogLog
- `KLL(KLL)` - KLL quantile sketch
- `DDS(DDSketch)` - DDSketch quantile sketch
- `COCO(Coco)` - Coco substring aggregation
- `ELASTIC(Elastic)` - Elastic heavy/light split
- `UNIFORM(UniformSampling)` - Uniform reservoir sampling
- `UNIVMON(UnivMon)` - UnivMon universal monitoring

Construct two `EHSketchList` wrappers over Count-Min sketches.

```rust
use sketchlib_rust::{EHSketchList, CountMin, FastPath, Vector2D, SketchInput};

let mut counts = EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::default());
let mut canary = EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::default());
let key = SketchInput::String("endpoint=/search".into());
```

Insert values through the unified enum interface.

```rust
counts.insert(&key);
canary.insert(&key);
```

Merge compatible `EHSketchList` variants.

```rust
counts.merge(&canary)?;
```

Query estimates without matching on the underlying sketch.

```rust
let estimate = counts.query(&key)?;
println!("merged sketch estimate = {estimate}");
```

When the underlying sketch does not implement an operation, `EHSketchList::merge` returns an error explaining the mismatch.

### Exponential Histogram: Time-bounded aggregates

Initialize the windowed coordinator with a sketch template.

```rust
use sketchlib_rust::{EHSketchList, ExponentialHistogram, CountMin, FastPath, Vector2D, SketchInput};

let template = EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::default());
let mut eh = ExponentialHistogram::new(3, 120, template);
```

Insert timestamped events.

```rust
eh.update(10, &SketchInput::String("flow".into()));
eh.update(70, &SketchInput::String("flow".into()));
```

Query the merged sketch for a given interval.

```rust
if let Some(bucket) = eh.query_interval_merge(0, 120) {
    let estimate = bucket.query(&SketchInput::String("flow".into())).unwrap();
    println!("approximate count inside window = {}", estimate);
}
```

## Quick Start
<!-- - Install a Rust toolchain that supports edition 2024 (currently nightly via `rustup toolchain install nightly`).
- Build everything: `cargo build --all-targets`.
- Run the library tests: `cargo test --all-features`.
- Explore helper binaries in `src/bin/` (e.g., `cargo run --bin generate_precomputed_hash`). -->
At this moment, ```cargo test``` is a good starting point.

## Library Map

### Core Modules

- **`src/common/`** - Foundation for all sketches ([common_api.md](./docs/common_api.md))
  - `input.rs` - `SketchInput` enum, `HeapItem`, `HHItem`, framework enums (`HydraCounter`, `L2HH`, `HydraQuery`)
  - `structures/` - High-performance data structures (`Vector1D`, `Vector2D`, `Vector3D`, `CommonHeap`, `MatrixStorage`, `FixedMatrix`)
  - `heap.rs` - `HHHeap` convenience wrapper for heavy hitter tracking
  - `hash.rs` - Hashing utilities (`hash_for_matrix`, `hash64_seeded`, `SEEDLIST`, `BOTTOM_LAYER_FINDER`) plus `SketchHasher` for custom hasher injection
  - `mode.rs` is under `src/sketches/` and provides `RegularPath` / `FastPath` type-level insert/estimate path selection

- **`src/sketches/`** - Core sketch implementations ([sketch_api.md](./docs/sketch_api.md))
  - **Recommended** (built on common structures): `countmin.rs`, `count.rs`, `hll.rs`
  - **Additional**: `ddsketch.rs`, `kll.rs`, `kmv.rs`
  - **Legacy**: `coco.rs`, `elastic.rs`, `uniform.rs`, `microscope.rs`, `locher.rs`

- **`src/sketch_framework/`** - Orchestration and serving layers
  - `eh_sketch_list.rs` - Unified interface (`EHSketchList` enum) wrapping all sketch types
  - `hashlayer.rs` - Hash-once-use-many optimization for multiple sketches
  - `hydra.rs` - Multi-dimensional hierarchical heavy hitters (includes `MultiHeadHydra`)
  - `univmon.rs` - Universal monitoring (L1, L2, entropy, cardinality)
  - `univmon_optimized.rs` - `UnivMonPyramid` and `UnivSketchPool` for two-tier sketch dimensions
  - `eh.rs` - Exponential histogram for sliding window queries
  - `eh_univ_optimized.rs` - `EHUnivOptimized` for optimized EH+UnivMon combination
  - `nitro.rs` - `NitroBatch` batch-mode sampling wrapper
  - `orchestrator/` - Node-level manager for sketches and frameworks (EH/HashLayer/Nitro nodes)

### Testing & Benchmarking

- **`benches/`** - Criterion-based performance benchmarks
  - Run with: `cargo bench`
- **CMS guarantee plots** - theorem-focused error/probability visualization
  - Quick profile (3 trials, `N={1e2..1e6}`):
    - `cargo run --release --bin cms_guarantee_plot -- --mode quick`
  - Full profile (10 trials, `N={1e2..1e9}`):
    - `cargo run --release --bin cms_guarantee_plot -- --mode full`
  - Optional runtime cap:
    - `cargo run --release --bin cms_guarantee_plot -- --mode full --max-n 1000000`
  - Artifacts are written to `data/results/cms_guarantee/<mode>_<timestamp>/`:
    - `metrics.csv` (per trial)
    - `summary.csv` (worst/mean aggregates)
  - Plot from CSV using Python/matplotlib:
    - `python3 scripts/plot_cms_guarantee.py --summary data/results/cms_guarantee/<mode>_<timestamp>/summary.csv --out-dir data/results/cms_guarantee/<mode>_<timestamp>/`
  - Interpretation:
    - Panel A bars (`max_abs_error_worst`) should remain below the `epsilon * N` line.
    - Panel B bars (`violation_rate_worst`) should remain below the `delta` line.

### Documentation

- **`docs/`** - API and feature documentation
  - [sketch_api.md](./docs/sketch_api.md) - Complete sketch API reference with usage examples
  - [common_api.md](./docs/common_api.md) - Data structures and shared utilities
  - [features.md](./docs/features.md) - Feature status and roadmap

### Utilities

- **`src/bin/`** - Helper binaries for generating precomputed fixtures and experiments (`generate_precomputed_hash`, `generate_precomputed_sample`, `generate_precomputed_sample2`, `cms_guarantee_plot`)

## Common Structure

To build new sketch with the Common API, check [this](./docs/common_api.md)

## Development

- Format sources with `cargo fmt` before committing changes.
- Lint with `cargo clippy --all-targets --all-features` to catch obvious mistakes across sketches and orchestration layers.

## Contributors

<table>
  <tr>
    <td align="center" style="padding: 15px;">
      <a href="https://github.com/GordonYuanyc">
        <img src="https://github.com/GordonYuanyc.png" width="100px;" alt="GordonYuanyc"/><br />
      </a>
      <strong style="font-size: 16px;">Yancheng Yuan</strong><br />
      <sub><a href="https://github.com/GordonYuanyc">GitHub</a></sub>
    </td>
    <td align="center" style="padding: 15px;">
      <a href="https://github.com/zzylol">
        <img src="https://github.com/zzylol.png" width="100px;" alt="zzylol"/><br />
      </a>
      <strong style="font-size: 16px;">Zeying Zhu</strong><br />
      <sub><a href="https://github.com/zzylol">GitHub</a></sub>
    </td>
  </tr>
</table>

## License

Copyright 2025 ProjectASAP

Licensed under the MIT License. See [LICENSE](LICENSE) for details.
