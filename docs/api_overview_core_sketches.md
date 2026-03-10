# API Overview: Core Sketches

Migrated from `docs/readme_details.md` -> `API Overview`.

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

