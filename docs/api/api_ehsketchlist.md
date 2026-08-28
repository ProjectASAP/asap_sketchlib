# API: EHSketchList

Status: `Ready`

## Purpose

Unified enum wrapper for sketch payloads used by EH-style frameworks.

## Type/Struct

```rust
enum EHSketchList {
    CM(CountMin<Vector2D<i32>, FastPath>),
    CS(Count<Vector2D<i32>, FastPath>),
    COUNTL2HH(CountL2HH),
    HLL(HyperLogLog<ErtlMLE>),
    KLL(KLL),
    DDS(DDSketch),
    COCO(Coco),
    ELASTIC(Elastic),
    UNIFORM(UniformSampling),
    UNIVMON(UnivMon),
}
```

## Constructors

Enum-based; construct by variant.

## Insert/Update

```rust
fn insert(&mut self, val: &DataInput)
```

## Query

```rust
fn query(&self, key: &DataInput) -> Result<f64, &'static str>
fn supports_norm(&self, norm: SketchNorm) -> bool
fn sketch_type(&self) -> &'static str
```

## Merge

```rust
fn merge(&mut self, other: &EHSketchList) -> Result<(), &'static str>
```

## Serialization

```rust
fn serialize_to_bytes(&self) -> Result<Vec<u8>, rmp_serde::encode::Error>
fn deserialize_from_bytes(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error>
```

ASAPv1 MessagePack, kind_id `0x14 0x00`. The metadata carries only
`metadata_version`; the payload is the triple `[variant, descriptor, state]`,
where `variant` is the wire name of the enum arm and `descriptor` / `state` are
that sketch's own ASAPv1 metadata and payload blocks carried verbatim as
msgpack `bin`. An `ExponentialHistogram` bucket inlines the same triple, so
there is one EHSketchList encoding.

The ten wire names and the kind_id whose blocks they carry:

| Variant | Wire name | Blocks belong to | Feature |
| ------- | --------- | ---------------- | ------- |
| `CM` | `"CountMin"` | `0x02 0x00` | default |
| `COCO` | `"Coco"` | `0x0c 0x00` | `experimental` |
| `COUNTL2HH` | `"CountL2HH"` | `0x19 0x00` | default |
| `CS` | `"CountSketch"` | `0x04 0x00` | default |
| `DDS` | `"DDSketch"` | `0x05 0x00` | default |
| `ELASTIC` | `"Elastic"` | `0x0b 0x00` | `experimental` |
| `HLL` | `"HLL"` | `0x01 0x02` | default |
| `KLL` | `"KLL"` | `0x06 0x00` | default |
| `UNIFORM` | `"UniformSampling"` | `0x0d 0x00` | `experimental` |
| `UNIVMON` | `"UnivMon"` | `0x10 0x00` | default |

The name table is the same in every build. A decoder built without
`experimental` rejects `"Coco"`, `"Elastic"` and `"UniformSampling"` with an
error naming the variant, and its encoder can never emit them. An unrecognized
name is rejected. `EHSketchList` also derives serde.

## Examples

```rust
use asap_sketchlib::{CountMin, EHSketchList, FastPath, DataInput, Vector2D};

let mut sk = EHSketchList::CM(CountMin::<Vector2D<i32>, FastPath>::default());
sk.insert(&DataInput::U64(1));
let _ = sk.query(&DataInput::U64(1));
```

## Caveats

- Some variant paths still contain `todo!()` branches in input conversion.
- Some merge/query variant combinations are intentionally unsupported.

## Status

Core wrapper in EH and optimized window frameworks.
