# API: Common Input Types

Status: `Shared`

## Purpose

Canonical input and ownership model shared across sketches/frameworks.

**File:** [src/common/input.rs](../../src/common/input.rs)

## Type/Struct

- `DataInput<'a>`
- `OwnedDataInput`
- `HeapEntry<C: HeapCount = i64>`
- `HydraQuery<'a>`
- `HydraCounter`

`HydraQuery` and `HydraCounter` are defined in `asap_sketchlib::common::input`.

## Constructors / Conversions

```rust
fn heap_item_to_sketch_input(item: &OwnedDataInput) -> DataInput<'_>
fn input_to_owned(input: &DataInput<'_>) -> OwnedDataInput

// HeapEntry<C>
fn from_input(k: DataInput, count: C) -> Self
fn new(key: OwnedDataInput, count: C) -> Self
fn init_item(k: DataInput, count: C) -> Self
```

## Insert/Update

```rust
// HydraCounter
fn update(&mut self, value: &DataInput, weight: Option<i32>)
fn update_with_hash(
    &mut self,
    value: &DataInput,
    hashed_val: &MatrixHash,
    count: Option<i32>,
)
```

## Query

```rust
// HydraCounter
fn query(&self, query: &HydraQuery) -> Result<f64, String>
```

## Merge

```rust
// HydraCounter
fn merge(&mut self, other: &HydraCounter) -> Result<(), String>
```

## Serialization

- `DataInput`, `OwnedDataInput`, `HeapEntry`, and `HydraCounter` derive serde traits.
- `HydraQuery` is query-only and does not derive serde in current code.

## Variants (Core)

```rust
enum DataInput<'a> {
    I8(i8), I16(i16), I32(i32), I64(i64), I128(i128), ISIZE(isize),
    U8(u8), U16(u16), U32(u32), U64(u64), U128(u128), USIZE(usize),
    F32(f32), F64(f64),
    Str(&'a str), String(String), Bytes(&'a [u8]),
}

enum OwnedDataInput {
    I8(i8), I16(i16), I32(i32), I64(i64), I128(i128), ISIZE(isize),
    U8(u8), U16(u16), U32(u32), U64(u64), U128(u128), USIZE(usize),
    F32(f32), F64(f64),
    String(String),
}
```

## Tuple keys

**File:** [src/common/tuple_key.rs](../../src/common/tuple_key.rs)

```rust
enum KeyColumn { Null, Bool(bool), I64(i64), F64(f64), Str(String) }

// TupleKey
fn new(columns: Vec<KeyColumn>) -> Self
fn columns(&self) -> &[KeyColumn]
fn to_bytes(&self) -> Result<Vec<u8>, String>
fn from_bytes(bytes: &[u8]) -> Result<Self, String>
```

A `TupleKey` is a key of several typed columns. `to_bytes` writes the ASAPv1
tuple key encoding ([spec](../asapv1_wire_format.md#tuple-keys)): one byte
string per tuple, `-0.0` written as `0.0` and every `NaN` as one `NaN`, so a
heap-carrying sketch keeps one candidate per tuple under a `bytes` key.
`from_bytes` rejects bytes `to_bytes` does not write. Equality and hashing
follow the bytes: `-0.0 == 0.0` and `NaN == NaN`.

## Examples

```rust
use asap_sketchlib::{DataInput, input_to_owned};
use asap_sketchlib::common::input::HydraQuery;

let q = HydraQuery::Frequency(DataInput::Str("key"));
let _ = q;

let key = DataInput::String("flow".to_string());
let _owned = input_to_owned(&key);
```

## See Also

- [Common Module (Canonical)](./api_common.md)
- [Common Heap Utilities](./api_common_heap.md)
- [API: Hydra](./api_hydra.md)
