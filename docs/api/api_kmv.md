# API: KMV

Status: `Unstable`

> Warning: This API is available and tested, but not yet integrated into the primary structured framework surfaces.

## Purpose

K-minimum values cardinality estimator.

## Type/Struct

- `KMV<H = DefaultXxHasher>`

## Constructors

```rust
fn default() -> Self
fn new(k: usize) -> Self
```

## Insert/Update

```rust
fn insert(&mut self, item: &DataInput)
fn insert_by_hash(&mut self, hash_value: u64)
```

## Query

```rust
fn estimate(&mut self) -> f64
```

## Merge

```rust
fn merge(&mut self, other: &mut KMV<H>)
```

## Serialization

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, RmpEncodeError>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

These produce/consume the **ASAPv1** wire envelope (kind `0x0e 0x00`) — see the
[ASAPv1 wire format spec](../asapv1_wire_format.md). The impl is bounded on
`H: HashProfile`, so a sketch built with an unprofiled hasher cannot serialize
at all. The retention bound `k` is metadata; the payload is the retained 64-bit
hashes alone, emitted in strictly ascending order, so two sketches holding the
same set emit the same bytes and a decoded sketch re-serializes byte-identically.
A `k` of zero, a `k` past the metadata's `u32` field, and a retained set larger
than `k` all fail to serialize.

## Examples

```rust
use asap_sketchlib::{KMV, DataInput};

let mut kmv = KMV::new(1024);
kmv.insert(&DataInput::U64(1));
let _ = kmv.estimate();
```

## Caveats

- Not currently part of primary framework wrappers.

## Status

Unstable; retained for compatibility visibility.
