# API Overview: Enums

Migrated from `docs/readme_details.md` -> `API Overview`.

## API Overview

There are three sections in the API overview section:

- Built-in `enum` for various purpose is introduced first
- Core sketches and sketch frameworks are introduced with their example usage
- Legacy sketches that is not migrated to [common api](./common_api.md) yet.

Only introductory usage is provided here. For full API list, please check [sketch api](./sketch_api.md).

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

