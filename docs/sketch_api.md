# Sketch API

In this document is a list of the public APIs of the sketches.

## Sketch Status

Core sketches and frameworks are **✅ RECOMMENDED** (optimized, maintained, and used by the
frameworks). Additional sketches are **⚠️ legacy/experimental** and may have rough edges.

### Recommended (Structured) Sketches

- **CountMin (CMS)** - Frequency estimation
- **Count Sketch (CS)** - Frequency estimation with both standard and L2HH variants
- **HyperLogLog** - Cardinality estimation (Regular, DataFusion, HIP)
- **Hydra** - Framework for hierarchical heavy hitters
- **UnivMon** - Framework for universal monitoring
- **HashLayer** - Hash-reuse orchestration for multiple sketches
- **ExponentialHistogram** - Sliding-window coordinator for mergeable sketches
- **EHUnivOptimized** - Hybrid EH + UnivMon with map/sketch tiers and sketch pooling
- **NitroBatch** - Batch-mode sampling wrapper for sketches
- **Orchestrator** - Node-level manager for sketches and frameworks

### Additional Sketches

- **KLL** - Quantile estimation with mergeable compactors
- **DDSketch** - Quantile estimation with relative error guarantees

### Legacy / Experimental Sketches

- **Elastic** - Heavy/light split CMS
- **Coco** - Substring aggregation
- **Locher** - Heavy hitter sampling
- **MicroScope** - Compact CMS variant
- **UniformSampling** - Reservoir sampling
- **KMV** - K-minimum values

---

## Table of Contents

1. [Frequency Sketches](#frequency-sketches)
2. [Cardinality Sketches](#cardinality-sketches)
3. [Quantile Sketches](#quantile-sketches)
4. [Frameworks](#frameworks)
5. [Usage Examples](#usage-examples)

---

## Frequency Sketches

### CountMin (CMS)

**File:** [src/sketches/countmin.rs](../src/sketches/countmin.rs)

Count-Min sketch provides approximate frequency counting with sub-linear space using multiple hash functions and returns the minimum counter value.

**Type:** `CountMin<S = Vector2D<i32>, Mode = RegularPath, H = DefaultXxHasher>`

**Storage options:**

- `Vector2D<i32>`: dynamic dimensions, `HashValueType = MatrixHashType`
- `DefaultMatrixI32`: fixed 3 × 4096, `HashValueType = u64`
- `QuickMatrixI32`: fixed 5 × 2048, `HashValueType = u64`
- `FixedMatrix`: alias of `QuickMatrixI32` (fixed 5 × 2048), `HashValueType = u64`

**Mode options:**

- `RegularPath`: per-row hashing
- `FastPath`: single hash reused across rows

Mode and storage types mirror CountMin and enforce matching insert/estimate paths.

Mode and storage are part of the type signature, so insert/estimate pairs and
hash widths are enforced by the type system.

`H` selects the hash implementation via the `SketchHasher` trait.

**Constructor Methods:**

```rust
fn default() -> Self                           // 3 rows × 4096 columns
fn with_dimensions(rows: usize, cols: usize) -> Self  // Vector2D only
fn from_storage(counts: S) -> Self
```

**Insert & Query (RegularPath):**

```rust
fn insert(&mut self, value: &SketchInput)
fn estimate(&self, value: &SketchInput) -> S::Counter
```

**Insert & Query (FastPath):**

```rust
fn insert(&mut self, value: &SketchInput)
fn estimate(&self, value: &SketchInput) -> S::Counter
fn fast_insert_with_hash_value(&mut self, hashed_val: &S::HashValueType)
fn fast_estimate_with_hash(&self, hashed_val: &S::HashValueType) -> S::Counter
```

**Merge Methods:**

```rust
fn merge(&mut self, other: &Self)  // Element-wise counter addition
```

**Serialization:**

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

**Other Methods:**

```rust
fn rows(&self) -> usize
fn cols(&self) -> usize
fn as_storage(&self) -> &S
fn as_storage_mut(&mut self) -> &mut S
```

---

### Count Sketch (CS)

**File:** [src/sketches/count.rs](../src/sketches/count.rs)

There are two versions of the Count Sketch: standard Count Sketch, and Count Sketch optimized for L2-Heavy-Hitter (which is used by UnivMon).

#### Count (Standard Count Sketch)

Count Sketch uses signed counters and random hash functions to provide unbiased frequency estimates. Returns the median of row estimates.

**Type:** `Count<S = Vector2D<i32>, Mode = RegularPath, H = DefaultXxHasher>`

**Storage options:**

- `Vector2D<i32>`: dynamic dimensions, `HashValueType = MatrixHashType`
- `DefaultMatrixI32`: fixed 3 × 4096, `HashValueType = u64`
- `QuickMatrixI32`: fixed 5 × 2048, `HashValueType = u64`
- `FixedMatrix`: alias of `QuickMatrixI32` (fixed 5 × 2048), `HashValueType = u64`

**Mode options:**

- `RegularPath`: per-row hashing
- `FastPath`: single hash reused across rows

`H` selects the hash implementation via the `SketchHasher` trait.

**Constructor Methods:**

```rust
fn default() -> Self                           // 3 rows × 4096 columns
fn with_dimensions(rows: usize, cols: usize) -> Self  // Vector2D only
fn from_storage(counts: S) -> Self
```

**Insert & Query (RegularPath):**

```rust
fn insert(&mut self, value: &SketchInput)
fn estimate(&self, value: &SketchInput) -> f64
```

**Insert & Query (FastPath):**

```rust
fn insert(&mut self, value: &SketchInput)
fn estimate(&self, value: &SketchInput) -> f64
fn fast_insert_with_hash_value(&mut self, hashed_val: &S::HashValueType)
fn fast_estimate_with_hash(&self, hashed_val: &S::HashValueType) -> f64
```

**Merge & Serialization:** Same pattern as CountMin

---

#### CountL2HH (Count Sketch with L2 Heavy Hitters)

Extended Count Sketch that maintains L2 norm for heavy hitter detection and supports weighted updates.

**Constructor Methods:**

```rust
fn default() -> Self
fn with_dimensions(rows: usize, cols: usize) -> Self
fn with_dimensions_and_seed(rows: usize, cols: usize, seed_idx: usize) -> Self
```

**Insert Methods:**

```rust
fn fast_insert_with_count(&mut self, val: &SketchInput, c: i64)                     // With L2 update
fn fast_insert_with_count_and_hash(&mut self, hashed_val: u128, c: i64)             // With L2 (pre-hash)
fn fast_insert_with_count_without_l2_and_hash(&mut self, hashed_val: u128, c: i64)  // Without L2 (pre-hash)
```

**Query Methods:**

```rust
fn fast_get_est(&self, val: &SketchInput) -> f64               // Frequency estimate
fn fast_get_est_with_hash(&self, hashed_val: u128) -> f64
fn fast_update_and_est(&mut self, val: &SketchInput, c: i64) -> f64  // Atomic update+query
fn fast_update_and_est_without_l2(&mut self, val: &SketchInput, c: i64) -> f64
fn get_l2(&self) -> f64        // L2 norm
fn get_l2_sqr(&self) -> f64    // Squared L2 norm
```

---

## Cardinality Sketches

### HyperLogLog

**File:** [src/sketches/hll.rs](../src/sketches/hll.rs)

There are three HyperLogLog variants with different characteristics:

**Generic type:** `HyperLogLog<Variant, H = DefaultXxHasher>`

#### HyperLogLog<Regular> (Classic)

Implements the algorithm from the original paper.

- **Accuracy:** Solid baseline; uses classic bias correction
- **Mergeable:** Yes
- **Speed:** Standard

```rust
fn new() -> Self                    // 16384 registers (14-bit precision)
fn insert(&mut self, obj: &SketchInput)
fn estimate(&self) -> usize         // Cardinality estimate
fn merge(&mut self, other: &Self)
```

**Serialization:**

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

---

#### HyperLogLog<DataFusion> (Ertl Estimator)

Implements Otmar Ertl's improved estimator (as used in DataFusion/Redis-style HLL).

- **Accuracy:** Improved relative to classic
- **Mergeable:** Yes
- **Speed:** Similar to classic

```rust
fn new() -> Self
fn insert(&mut self, obj: &SketchInput)
fn estimate(&self) -> usize         // Ertl estimator
fn merge(&mut self, other: &Self)
```

**Serialization:**

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

---

#### HyperLogLogHIP (HIP Estimator)

HIP (Historical Inverse Probability) estimator, ported from Apache DataSketches.

- **Accuracy:** Good, but **NOT mergeable**
- **Insertion speed:** Slightly slower than classic (extra counters)
- **Query speed:** O(1) (maintains streaming estimate)

```rust
fn new() -> Self
fn insert(&mut self, obj: &SketchInput)
fn estimate(&self) -> usize         // Streaming estimate (O(1))
```

**Serialization:**

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

**Note:** Use `HyperLogLog<DataFusion>` when mergeability matters; use `HyperLogLogHIP` for fastest query in non-merge scenarios.

---

## Quantile Sketches

### KLL

**File:** [src/sketches/kll.rs](../src/sketches/kll.rs)

KLL (Karnin-Lang-Liberty) sketch provides approximate quantiles with mergeable compactors and rank-error guarantees.

**Constructor Methods:**

```rust
fn default() -> Self                    // k=200 (default accuracy)
fn init_kll(k: i32) -> Self             // k controls accuracy (higher = more accurate)
```

**Insert Methods:**

```rust
fn update(&mut self, val: &SketchInput) -> Result<(), &'static str>  // Insert numeric value
```

**Query Methods:**

```rust
fn quantile(&self, q: f64) -> f64       // Value at quantile q in [0,1]
fn rank(&self, x: f64) -> usize         // Rank of value x
fn count(&self) -> usize                // Total items processed
fn cdf(&self) -> Cdf                    // Full cumulative distribution
```

**CDF Methods:**

The `Cdf` structure provides additional query capabilities:

```rust
fn quantile(&self, x: f64) -> f64       // Fraction of values ≤ x
fn query(&self, p: f64) -> f64          // Value at quantile p
fn quantile_li(&self, x: f64) -> f64    // Quantile with linear interpolation
fn query_li(&self, p: f64) -> f64       // Value with linear interpolation
```

**Merge Methods:**

```rust
fn merge(&mut self, other: &KLL)        // Merge another KLL sketch
```

**Serialization:**

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

---

### DDSketch

**File:** [src/sketches/ddsketch.rs](../src/sketches/ddsketch.rs)

DDSketch provides approximate quantiles with **relative error guarantees** (as opposed to KLL's rank error). It uses logarithmically-spaced buckets for efficient storage.

**Key Characteristics:**

- **Error Type:** Relative error on quantile values
- **Accuracy:** Configurable via `alpha` parameter (e.g., 0.01 = 1% relative error)
- **Mergeable:** Yes
- **Input Type:** Positive `f64` values only

**Constructor Methods:**

```rust
fn new(alpha: f64) -> Self              // alpha controls relative error (0.0 < alpha < 1.0)
```

**Insert Methods:**

```rust
fn add(&mut self, v: f64)               // Insert positive value (non-positive values ignored)
```

**Query Methods:**

```rust
fn get_value_at_quantile(&self, q: f64) -> Option<f64>  // Value at quantile q in [0,1]
fn get_count(&self) -> u64              // Total samples added
fn min(&self) -> Option<f64>            // Minimum value seen
fn max(&self) -> Option<f64>            // Maximum value seen
```

**Merge Methods:**

```rust
fn merge(&mut self, other: &DDSketch)   // Merge another DDSketch (must have same alpha)
```

**Serialization:**

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

---

## Frameworks

### Hydra (Hierarchical Heavy Hitters)

**File:** [src/sketch_framework/hydra.rs](../src/sketch_framework/hydra.rs)

Hydra is a sketch framework for multi-dimensional hierarchical queries over subpopulations.
`MultiHeadHydra` extends this by allowing multiple counter types across named dimensions.

**Constructor Methods:**

```rust
fn default() -> Self                                               // 3 × 32 with CountMin
fn with_dimensions(r: usize, c: usize, sketch_type: HydraCounter) -> Self  // Custom
```

**Methods:**

```rust
fn update(&mut self, key: &str, value: &SketchInput, count: Option<i32>)  // key: semicolon-separated (e.g., "US;CA;male")
fn query_key(&self, key: Vec<&str>, query: &HydraQuery) -> f64  // Query subpopulation
fn query_frequency(&self, key: Vec<&str>, value: &SketchInput) -> f64  // Convenience
fn query_quantile(&self, key: Vec<&str>, threshold: f64) -> f64  // Convenience
fn merge(&mut self, other: &Hydra) -> Result<(), String>
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

**HydraCounter Enum:**

```rust
enum HydraCounter {
    CM(CountMin<Vector2D<i32>, FastPath>),
    HLL(HyperLogLog<DataFusion>),
    CS(Count<Vector2D<i32>, FastPath>),
    KLL(KLL),
    UNIVERSAL(UnivMon),
}
```

**HydraQuery Enum:**

```rust
enum HydraQuery {
    Frequency(SketchInput),
    Quantile(f64),
    Cdf(f64),
    Cardinality,
    L1Norm,
    L2Norm,
    Entropy,
}
```

---

### UnivMon (Universal Monitoring)

**File:** [src/sketch_framework/univmon.rs](../src/sketch_framework/univmon.rs)

UnivMon is a sketch framework for computing multiple stream statistics (L1, L2, entropy, cardinality) from a single data structure.

**Constructor Methods:**

```rust
fn init_univmon(heap_size: usize, sketch_row: usize, sketch_col: usize, layer_size: usize) -> Self
```

**Insert Methods:**

```rust
fn insert(&mut self, key: &SketchInput, value: i64)
fn fast_insert(&mut self, key: &SketchInput, value: i64)
```

**Query Methods:**

```rust
fn calc_l1(&self) -> f64          // L1 norm
fn calc_l2(&self) -> f64          // L2 norm
fn calc_entropy(&self) -> f64     // Shannon entropy
fn calc_card(&self) -> f64        // Cardinality
fn calc_g_sum<F>(&self, g: F, is_card: bool) -> f64  // Generic aggregation
```

**Merge Methods:**

```rust
fn merge(&mut self, other: &UnivMon)
```

---

### HashLayer (Hash Reuse)

**File:** [src/sketch_framework/hashlayer.rs](../src/sketch_framework/hashlayer.rs)

HashLayer coordinates multiple sketches and reuses hashes across compatible ones.
It accepts `OrchestratedSketch` values and automatically caches per-domain hashes.

**Constructor Methods:**

```rust
fn default() -> Self
fn new(sketches: Vec<OrchestratedSketch>) -> Result<Self, &'static str>
```

**Insert & Query Methods:**

```rust
fn insert_all(&mut self, val: &SketchInput)
fn insert_at(&mut self, indices: &[usize], val: &SketchInput)
fn query_at(&self, index: usize, val: &SketchInput) -> Result<f64, &'static str>
fn query_all(&self, val: &SketchInput) -> Vec<Result<f64, &'static str>>
```

---

### Orchestrator (Node Manager)

**File:** [src/sketch_framework/orchestrator/node_orchestrator.rs](../src/sketch_framework/orchestrator/node_orchestrator.rs)

The node orchestrator provides a unified interface across sketches and higher-level
frameworks (EH/Nitro/HashLayer). It routes inserts and queries via selectors.
Current status: **in progress / experimental**. Expect API adjustments while the node surface stabilizes.
Note: `HashLayerNode` query routing is currently placeholder (`TODO`) and may return an error.

**Core Types:**

- `Orchestrator` - node registry + selection
- `OrchestratorNode` - trait implemented by `SketchNode`, `HashLayerNode`, `EhNode`, `NitroNode`
- `NodeSelector` - select nodes by index/name/tag/kind
- `NodeInsert` / `NodeQuery` - typed insert/query payloads

---

### NitroBatch (Batch Sampling)

**File:** [src/sketch_framework/nitro.rs](../src/sketch_framework/nitro.rs)

NitroBatch provides batch-mode geometric sampling and updates a sketch through
the `NitroTarget` trait (single hash per sampled item).

**NitroTarget Trait:**

```rust
pub trait NitroTarget {
    fn rows(&self) -> usize;
    fn update_row(&mut self, row: usize, hashed: u128, delta: u64);
}
```

**Compatibility:**

- `Vector2D<u32>`
- `CountMin<Vector2D<i32>, FastPath>` (CMS)
- `Count<Vector2D<i32>, FastPath>`

**Constructor / Methods:**

```rust
fn init_nitro(rate: f64) -> Self
fn with_target(rate: f64, sk: S) -> Self
fn insert(&mut self, data: &[i64])
fn insert_cached_step(&mut self, data: &[i64])
fn target(&self) -> &S
fn target_mut(&mut self) -> &mut S
fn into_target(self) -> S
```

---

### ExponentialHistogram (Sliding Window)

**File:** [src/sketch_framework/eh.rs](../src/sketch_framework/eh.rs)

ExponentialHistogram maintains mergeable sketch buckets over a time window and
supports interval queries by merging covered buckets.

**Constructor Methods:**

```rust
fn new(k: usize, window: u64, eh_type: EHSketchList) -> Self
```

**Update / Query Methods:**

```rust
fn update(&mut self, time: u64, val: &SketchInput)
fn update_with<F>(&mut self, time: u64, update_fn: F) where F: FnOnce(&mut EHSketchList)
fn query_interval_merge(&self, t1: u64, t2: u64) -> Option<EHSketchList>
```

**Window / Introspection Methods:**

```rust
fn update_window(&mut self, window: u64)
fn cover(&self, mint: u64, maxt: u64) -> bool
fn get_min_time(&self) -> Option<u64>
fn get_max_time(&self) -> Option<u64>
fn bucket_count(&self) -> usize
fn get_memory_info(&self) -> (usize, Vec<usize>)
fn print_buckets(&self)
```

---

### EHUnivOptimized (Hybrid EH + UnivMon)

**File:** [src/sketch_framework/eh_univ_optimized.rs](../src/sketch_framework/eh_univ_optimized.rs)

EHUnivOptimized keeps recent buckets as exact maps and older/larger buckets as
`UnivMon` sketches. It reuses sketches via `UnivSketchPool` to reduce allocation
overhead during promotion/merge/expiration.

**Constructor Methods:**

```rust
fn new(
    k: usize,
    window: u64,
    heap_size: usize,
    sketch_row: usize,
    sketch_col: usize,
    layer_size: usize,
) -> Self

fn with_pool_cap(
    k: usize,
    window: u64,
    heap_size: usize,
    sketch_row: usize,
    sketch_col: usize,
    layer_size: usize,
    pool_cap: usize,
) -> Self

fn with_defaults(k: usize, window: u64) -> Self
```

**Update / Query Methods:**

```rust
fn update(&mut self, time: u64, key: &SketchInput, value: i64)
fn query_interval(&self, t1: u64, t2: u64) -> Option<EHUnivQueryResult>
```

**Window / Introspection Methods:**

```rust
fn cover(&self, mint: u64, maxt: u64) -> bool
fn get_min_time(&self) -> Option<u64>
fn get_max_time(&self) -> Option<u64>
fn update_window(&mut self, window: u64)
fn bucket_count(&self) -> usize
fn pool(&self) -> &UnivSketchPool
fn get_memory_info(&self) -> (usize, usize, Vec<usize>, Vec<usize>)
fn print_buckets(&self)
```

**Result Type:**

```rust
enum EHUnivQueryResult {
    Sketch(UnivMon),
    Map { freq_map: HashMap<HeapItem, i64>, total_count: usize },
}

impl EHUnivQueryResult {
    fn calc_l1(&self) -> f64
    fn calc_l2(&self) -> f64
    fn calc_entropy(&self) -> f64
    fn calc_card(&self) -> f64
}
```

---

## Usage Examples

### Basic Frequency Counting (CMS, RegularPath)

```rust
use sketchlib_rust::{CountMin, SketchInput};

let mut cm = CountMin::default(); // Vector2D<i32> + RegularPath
cm.insert(&SketchInput::U64(12345));
let freq = cm.estimate(&SketchInput::U64(12345));
```

### FastPath CMS with Shared Hash

```rust
use sketchlib_rust::{hash_for_matrix, CountMin, FastPath, SketchInput, Vector2D};

let mut cm = CountMin::<Vector2D<i32>, FastPath>::default();
let key = SketchInput::U64(12345);
let hash = hash_for_matrix(cm.rows(), cm.cols(), &key);
cm.fast_insert_with_hash_value(&hash);
let freq = cm.fast_estimate_with_hash(&hash);
```

### NitroBatch with CMS (FastPath)

```rust
use sketchlib_rust::{CountMin, FastPath, NitroBatch, Vector2D};

let sk = CountMin::<Vector2D<i32>, FastPath>::default();
let mut nitro = NitroBatch::with_target(0.1, sk);
let data = vec![10_i64, 20, 30, 40, 50];
nitro.insert(&data);
let cm = nitro.into_target();
```

### Cardinality Estimation

```rust
use sketchlib_rust::{DataFusion, HyperLogLog, SketchInput};

let mut hll = HyperLogLog::<DataFusion>::new();
hll.insert(&SketchInput::String("item1".to_string()));
hll.insert(&SketchInput::String("item2".to_string()));
let cardinality = hll.estimate();
```

### Quantile Queries (KLL)

```rust
use sketchlib_rust::{KLL, SketchInput};

let mut kll = KLL::init_kll(200);
kll.update(&SketchInput::F64(1.5)).unwrap();
kll.update(&SketchInput::F64(2.7)).unwrap();
kll.update(&SketchInput::F64(3.2)).unwrap();
let median = kll.cdf().query(0.5);
```

### Quantile Queries (DDSketch)

```rust
use sketchlib_rust::DDSketch;

let mut sketch = DDSketch::new(0.01); // 1% relative error
sketch.add(1.5);
sketch.add(2.7);
sketch.add(3.2);
let median = sketch.get_value_at_quantile(0.5).unwrap();
let p99 = sketch.get_value_at_quantile(0.99).unwrap();
```

### Hierarchical Queries

```rust
use sketchlib_rust::{Hydra, SketchInput};

let mut hydra = Hydra::default();
hydra.update("US;CA;male", &SketchInput::U64(1), None);
let freq = hydra.query_frequency(vec!["US", "CA"], &SketchInput::U64(1));
```

### Orchestrator + HashLayer (in-progress API)

```rust
use sketchlib_rust::{
    CountMin, FastPath, FreqSketch, HashLayer, HashLayerNode, NodeMeta, NodeQuery, NodeSelector,
    OrchestratedSketch, Orchestrator, SketchInput, Vector2D,
};

let layer = HashLayer::new(vec![
    OrchestratedSketch::Freq(FreqSketch::CountMin(
        CountMin::<Vector2D<i32>, FastPath>::default(),
    )),
])
.expect("hash-reuse compatible sketches");

let node = HashLayerNode::new(layer);
let mut orchestrator = Orchestrator::new(vec![(Box::new(node), NodeMeta::new("hashlayer"))]);

let key = SketchInput::U64(42);
orchestrator.insert(NodeSelector::Names(&["hashlayer"]), &key);
let results = orchestrator.query(NodeSelector::Names(&["hashlayer"]), &NodeQuery::Sketch(&key));
println!("{results:?}");
```

---

## Common Patterns

### Serialization

Most sketches support MessagePack serialization:

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

### Fast Paths

Many sketches provide optimized paths:

- `FastPath` mode (CountMin/Count) selects single-hash algorithms at the type level
- Methods with `_with_hash_value` suffix accept pre-computed hashes for multi-sketch coordination

### Custom Hashers

`CountMin`, `Count`, and `HyperLogLog` accept a hasher generic `H` that
implements `SketchHasher` (default: `DefaultXxHasher`).

```rust
use sketchlib_rust::{
    CountMin, DefaultXxHasher, HeapItem, RegularPath, SketchHasher, SketchInput, Vector2D,
};

#[derive(Clone, Debug)]
struct MyHasher;

impl SketchHasher for MyHasher {
    fn hash64_seeded(d: usize, key: &SketchInput) -> u64 {
        <DefaultXxHasher as SketchHasher>::hash64_seeded(d, key)
    }
    fn hash128_seeded(d: usize, key: &SketchInput) -> u128 {
        <DefaultXxHasher as SketchHasher>::hash128_seeded(d, key)
    }
    fn hash_item64_seeded(d: usize, key: &HeapItem) -> u64 {
        <DefaultXxHasher as SketchHasher>::hash_item64_seeded(d, key)
    }
    fn hash_item128_seeded(d: usize, key: &HeapItem) -> u128 {
        <DefaultXxHasher as SketchHasher>::hash_item128_seeded(d, key)
    }
}

let mut cm = CountMin::<Vector2D<i32>, RegularPath, MyHasher>::with_dimensions(3, 1024);
cm.insert(&SketchInput::U64(7));
```

### SketchInput

All sketches accept the `SketchInput` enum for type-agnostic insertion:

```rust
pub enum SketchInput<'a> {
    I8(i8), I16(i16), I32(i32), I64(i64), I128(i128), ISIZE(isize),
    U8(u8), U16(u16), U32(u32), U64(u64), U128(u128), USIZE(usize),
    F32(f32), F64(f64), Str(&'a str),
    String(String), Bytes(&'a [u8]),
}
```

---

## Summary

**Available Sketches:**

- **Frequency:** CountMin, Count, CountL2HH
- **Cardinality:** HyperLogLog (Regular, DataFusion, HIP)
- **Quantile:** KLL, DDSketch
- **Frameworks:** Hydra, UnivMon, HashLayer, ExponentialHistogram, EHUnivOptimized, NitroBatch, Orchestrator

**File Locations:**

- Core sketches: [src/sketches/](../src/sketches/)
- Frameworks: [src/sketch_framework/](../src/sketch_framework/)
- Common primitives: [src/common/](../src/common/) (see [common_api.md](common_api.md))

For implementation details and internal data structures, see [common_api.md](common_api.md).
