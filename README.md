# sketchlib-rust

`sketchlib-rust` is a sketch library for native rust sketch. This repo contains mainly three parts:

- Basic building blocks: located in `/src/common`, contains common structure to build sketches and other common utilities
- Native Sketch: located in `/src/sketches`, contains rust sketches, migration from hard coded sketch into common structure based sketches
  - Completed Migration: CountMin, HyperLogLog
- Sketch Framework: located in `/src/sketch_framework`, contains sketch serving strategies

## Highlights

- Rich sketch collection covering Count-Min, Count/CountUniv, Coco, Elastic, HyperLogLog variants, KLL, Locher, Microscope, and UnivMon implementations.
- Hydra coordinator builds multi-dimensional label combinations so the same data supports queries over arbitrary label subsets.
- Chapter enum normalizes insert, merge, and query flows across sketches for composable pipelines.
- Exponential Histogram wrapper maintains time-bounded aggregates without duplicating sketch state.
- MessagePack + hex serialization bridges the Rust sketches with PromSketch, Go clients, and optional Arroyo UDFs.

## API Overview

### `SketchInput`

`SketchInput` is the common entry point for data flowing into every sketch implementation. It supports four families of values so call sites do not need per-sketch conversions:
- Integer counters: `I32`, `I64`, `U32`, `U64`
- Floating point samples: `F32`, `F64`
- Text keys: borrowed `Str` and owned `String`
- Binary payloads: `Bytes`

### Standalone sketches

Every sketch exposes a consistent lifecycle: construct the sketch, insert data, derive an estimate, and optionally merge state from a peer sketch. The example below uses Count-Min to illustrate the pattern.

Create two sketches with matching dimensions.

```rust
use sketchlib_rust::{CountMin, SketchInput};

let mut primary = CountMin::init_cm_with_row_col(3, 64);
let mut peer = CountMin::init_cm_with_row_col(3, 64);
let key = SketchInput::String("error".into());
```

Insert updates into both instances.

```rust
primary.insert_cm(&key);
primary.insert_cm(&key);
peer.insert_cm(&key);
```

Merge the peer into the primary sketch.

```rust
primary.merge(&peer);
```

Estimate the frequency for the tracked key.

```rust
let estimate = primary.get_est(&key);
println!("approximate error count = {}", estimate);
```

#### Structured Count-Min (hash reuse)

The structured variant keeps row slices inside a shared 64-bit digest so `fast_insert` and `fast_estimate` perform the hash only once per value.

Create the optimized sketch and key.

```rust
use sketchlib_rust::{StructuredCountMin, common::SketchInput};

let mut sketch = StructuredCountMin::with_dimensions(4, 2048);
let key = SketchInput::String("warning".into());
```

Apply high-throughput updates while reusing the precomputed hash internally.

```rust
for _ in 0..10_000 {
    sketch.fast_insert(&key);
}
```

Estimate the frequency with the same single-hash shortcut.

```rust
let approx = sketch.fast_estimate(&key);
println!("fast estimate ≈ {}", approx);
```

Other sketches follow the same shape with their own method names (for example `KLL::update`, `HllDfModified::insert`, `Elastic::insert`). Depending on the sketch, inserts may take a `SketchInput` or a domain-specific value (such as `f64` for quantiles); most provide a `merge` method that rejects mismatched shapes at runtime.

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

#### HyperLogLog (cardinality)

Stand up two HLL sketches.

```rust
use sketchlib_rust::sketches::{hll::HllDfModified, utils::SketchInput};

let mut uniques = HllDfModified::new();
let mut shard = HllDfModified::new();
```

Insert hashed values into each sketch.

```rust
for user in 0..10_000u64 {
    uniques.insert(&SketchInput::U64(user));
}
for user in 5_000..7_500u64 {
    shard.insert(&SketchInput::U64(user));
}
```

Merge register state from the shard.

```rust
uniques.merge(&shard);
```

Read back the approximate cardinality.

```rust
println!("approximate distinct users = {}", uniques.get_est());
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

#### CountUniv (signed frequency moments)

Set up two CountUniv sketches to accumulate signed counts.

```rust
use sketchlib_rust::sketches::{count::CountUniv, utils::SketchInput};

let mut traffic = CountUniv::init_countuniv_with_rc(3, 64);
let mut replica = CountUniv::init_countuniv_with_rc(3, 64);
let key = SketchInput::Str("db-query");
```

Apply positive deltas to both sketches.

```rust
traffic.insert_once(&key);
traffic.insert_with_count(&key, 4);
replica.insert_with_count(&key, 7);
```

Merge row-wise counters from the replica.

```rust
traffic.merge(&replica);
```

Recover the median estimate and l2 norm.

```rust
let est = traffic.get_est(&key);
let l2 = traffic.get_l2();
println!("signed estimate ≈ {est}, l2 norm ≈ {l2}");
```

#### UnivMon (multi-moment pyramid)

Create the UnivMon pyramid and derive placement metadata.

```rust
use sketchlib_rust::sketches::{
    univmon::UnivMon,
    utils::{SketchInput, hash_it, LASTSTATE},
};

let mut sketch = UnivMon::init_univmon(32, 3, 1024, 4, 0);
let key = "flow::123";
let hash = hash_it(LASTSTATE, &SketchInput::Str(key));
let bottom = sketch.find_bottom_layer_num(hash, sketch.layer);
```

Update every layer through the computed bottom.

```rust
sketch.update(key, 1, bottom);
sketch.update(key, 1, bottom);
```

Read back aggregate signals such as cardinality.

```rust
println!("approximate cardinality = {}", sketch.calc_card());
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

### Coordinating multi-label sketches with Hydra

`Hydra` fans a single transition across many `Chapter` instances, automatically building label combinations so queries can drill into any subset of tags. Its public `update` method accepts semicolon-delimited keys (for example `service=api;route=/search`) and replays the same `SketchInput` into every label combination before answering multidimensional queries via `Hydra::query_key`.

Initialize the coordinator with a `Chapter` template.

```rust
use sketchlib_rust::{
    sketchbook::{Chapter, Hydra},
    sketches::{countmin::CountMin, utils::SketchInput},
};

let template = Chapter::CM(CountMin::init_cm_with_row_col(3, 64));
let mut hydra = Hydra::new(3, 128, template);
```

Replay updates across every label combination.

```rust
let value = SketchInput::String("latency>250ms".into());
hydra.update("service=api;route=/search", &value);
hydra.update("service=api;route=/search", &value);
```

Query a subset of labels for the aggregated estimate.

```rust
let estimate = hydra.query_key(vec!["service=api", "route=/search"], &value);
println!("2-D combination count ≈ {}", estimate);
```

### Time-bounded aggregates with Exponential Histogram

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

- `src/common`: shared structures (`SketchMatrix`, `SketchList`), the `SketchInput` enum, and hashing helpers used by sketches and sketchbook.
- `src/sketches`: core sketch implementations plus serialization hooks.
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

## Optimization

### Single Hash Reuse

For a `CountMinSketch` with 3 rows and 4096 columns, the minimun size requirement of hash value is: `3*log(4096)=36` bits. One large hash value (i.e., 64 bits) is sufficient to insert the whole sketch, making hashing for each row unnecessary. This suggests an optimization that if the hash value is large enough, hash each key once is sufficient to insert the whole sketch.

## Benchmark

Benchmark located in `/benches`, with the help of rust benchmark support.

### To Run

To run the benchmark for `CountMinSketch` to check the optimization techniques applied to `insert()` and `estimate()`:

```bash
cargo bench --bench countmin
```

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
