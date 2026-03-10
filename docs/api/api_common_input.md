# API: Common Input Types

Status: `Shared`

## Purpose

Canonical input and ownership model shared across sketches/frameworks.

**File:** [src/common/input.rs](../../src/common/input.rs)

## Type/Struct

- `SketchInput<'a>`
- `HeapItem`
- `HHItem`
- `HydraQuery<'a>`
- `HydraCounter`
- `L2HH`

`HydraQuery` and `HydraCounter` are defined in `sketchlib_rust::input`.

## Constructors / Conversions

```rust
fn heap_item_to_sketch_input(item: &HeapItem) -> SketchInput<'_>
fn input_to_owned(input: &SketchInput<'_>) -> HeapItem

// HHItem
fn new(k: SketchInput, count: i64) -> Self
fn create_item(k: HeapItem, count: i64) -> Self
fn init_item(k: SketchInput, count: i64) -> Self
```

## Insert/Update

```rust
// L2HH
fn update_and_est(&mut self, key: &SketchInput, value: i64) -> f64
fn update_and_est_without_l2(&mut self, key: &SketchInput, value: i64) -> f64
fn clear(&mut self)

// HydraCounter
fn insert(&mut self, value: &SketchInput, count: Option<i32>)
fn insert_with_hash(
    &mut self,
    value: &SketchInput,
    hashed_val: &MatrixHashType,
    count: Option<i32>,
)
```

## Query

```rust
// L2HH
fn get_l2(&self) -> f64

// HydraCounter
fn query(&self, query: &HydraQuery) -> Result<f64, String>
```

## Merge

```rust
// L2HH
fn merge(&mut self, other: &L2HH)

// HydraCounter
fn merge(&mut self, other: &HydraCounter) -> Result<(), String>
```

## Serialization

- `SketchInput`, `HeapItem`, `HHItem`, `HydraCounter`, and `L2HH` derive serde traits.
- `HydraQuery` is query-only and does not derive serde in current code.

## Variants (Core)

```rust
enum SketchInput<'a> {
    I8(i8), I16(i16), I32(i32), I64(i64), I128(i128), ISIZE(isize),
    U8(u8), U16(u16), U32(u32), U64(u64), U128(u128), USIZE(usize),
    F32(f32), F64(f64),
    Str(&'a str), String(String), Bytes(&'a [u8]),
}

enum HeapItem {
    I8(i8), I16(i16), I32(i32), I64(i64), I128(i128), ISIZE(isize),
    U8(u8), U16(u16), U32(u32), U64(u64), U128(u128), USIZE(usize),
    F32(f32), F64(f64),
    String(String),
}
```

## Examples

```rust
use sketchlib_rust::{SketchInput, input_to_owned};
use sketchlib_rust::input::HydraQuery;

let q = HydraQuery::Frequency(SketchInput::Str("key"));
let _ = q;

let key = SketchInput::String("flow".to_string());
let _owned = input_to_owned(&key);
```

## See Also

- [Common Module (Canonical)](./api_common.md)
- [Common Heap Utilities](./api_common_heap.md)
- [API: Hydra](./api_hydra.md)
