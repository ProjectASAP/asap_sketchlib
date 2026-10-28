# API: HyperLogLog

Status: `Ready`

## Purpose

Approximate cardinality (distinct-count) estimation.

## Types

The library provides three HLL algorithm variants, each available at
multiple precision levels.

### Algorithm Variants

- `HyperLogLog<Classic, H>` — classic HyperLogLog (Flajolet et al.).
- `HyperLogLog<Ertl, H>` — improved estimator (Ertl, arXiv:1702.01284).
- `HyperLogLogHip` — Historic Inverse Probability estimator (Lang, arXiv:1708.06839). Not mergeable.

### Precision Aliases

Each variant is backed by a register storage with a configurable precision
parameter `p` (number of address bits). Higher precision uses more memory
but improves accuracy.

| Alias | Precision | Registers | Underlying Type |
| --- | --- | --- | --- |
| `HyperLogLogP12<Variant, H>` | p=12 | 4,096 | `HyperLogLogImpl<Variant, HllRegistersP12, H>` |
| `HyperLogLogP14<Variant, H>` | p=14 | 16,384 | `HyperLogLogImpl<Variant, HllRegistersP14, H>` |
| `HyperLogLogP16<Variant, H>` | p=16 | 65,536 | `HyperLogLogImpl<Variant, HllRegistersP16, H>` |
| `HyperLogLog<Variant, H>` | p=14 (default) | 16,384 | = `HyperLogLogP14<Variant, H>` |
| `HyperLogLogHipP12` | p=12 | 4,096 | `HyperLogLogHipImpl<HllRegistersP12>` |
| `HyperLogLogHipP14` | p=14 | 16,384 | `HyperLogLogHipImpl<HllRegistersP14>` |
| `HyperLogLogHip` | p=14 (default) | 16,384 | = `HyperLogLogHipP14` |

`H` defaults to `DefaultXxHasher` and can be omitted in most usage.

## Constructors

```rust
fn new() -> Self
fn default() -> Self
```

## Insert / Update

```rust
fn update(&mut self, obj: &DataInput)
fn update_batch(&mut self, items: &[DataInput])
fn update_with_hash(&mut self, hashed: u64)
fn update_batch_with_hashes(&mut self, hashes: &[u64])
```

## Query

```rust
fn estimate(&self) -> usize
```

`Classic` also exposes:

```rust
fn indicator(&self) -> f64
```

## Merge

```rust
fn merge(&mut self, other: &Self)
```

Available on `Classic` and `Ertl` variants. **Not available on HIP.**

## Serialization

```rust
fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

These produce/consume the **ASAPv1** wire envelope — see the
[ASAPv1 wire format spec](../asapv1_wire_format.md). On the generic
`HyperLogLogImpl<Variant, Registers, H>`, the methods are bounded on
`Variant: HllWireVariant` and `H: HashProfile`, so every
(variant × precision × hasher) combination serializes — **HLL is fully
wire-covered**. Each variant maps to its own `kind_id` (Classic → `0x01 0x01`,
Ertl-MLE → `0x01 0x02`, HIP → `0x01 0x04`); `precision` and the
`HashProfile`-derived hash spec live in the envelope metadata, and the payload is
the register bytes (plus the running estimate `hip_est` for the HIP variant).
HIP's `kxq0` / `kxq1` sums are recomputed from the registers on decode, bit for
bit, so a decoded HIP sketch keeps counting exactly as the encoded one would.
`HyperLogLogHip::from_asapv1` also reads the retired `0x01 0x03` payload, which
carried those two sums, and drops them; `to_asapv1` never writes it.
`HyperLogLogHip` is a non-generic struct that hashes through the default
functions, so it is wire-eligible under the **standard** hash profile only.

## Deltas

```rust
HyperLogLogDelta<Variant, Registers, H = DefaultXxHasher>

fn new(indices: Vec<u64>, registers: Vec<u8>) -> Result<Self, String>
fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>

// on HyperLogLogImpl<Variant, Registers, H>
fn merge_delta(&mut self, delta: &HyperLogLogDelta<Variant, Registers, H>) -> Result<(), String>
```

A `HyperLogLogDelta` is the ASAPv1 delta kind `0x01 0x81` (Classic) or
`0x01 0x82` (Ertl-MLE), for the same configs as the state serialization; HIP
has no delta. It carries the changed registers as strictly ascending indices
below `2^p` and a new value for each, non-zero and at most `65 - p`. `new` and
the decoder refuse anything else. `merge_delta` sets each named register to the
larger of its value and the delta's; the type parameters pin the estimator,
precision and hasher, so it fails, changing nothing, only when the admission
rates differ.

## Admission rate

```rust
fn with_admission_rate(self, p: f64) -> Self
fn admission_rate(&self) -> f64

// on HyperLogLogDelta
fn with_admission_rate(self, p: f64) -> Self
```

`p` is the probability with which each update was admitted, `1` unless set. The
estimate counts admitted keys alone; a consumer divides it by `p`. A key's
updates are admitted all or none. The ASAPv1 metadata carries `p` as its last
key, `admission_rate`, when `p < 1`; a `p` outside `(0, 1]` fails to serialize.
`merge` panics, and `merge_delta` fails, when the two rates differ. A sketch
nested in another kind (`EhBucketSketch`, `Hydra`, CountMinSketchWithHeap, CountSketchWithHeap, Elastic) must
hold `p = 1` to serialize. Both methods are on `HyperLogLogImpl` and
`HyperLogLogHipImpl`.

## Examples

```rust
use asap_sketchlib::{Ertl, HyperLogLog, DataInput};

let mut hll = HyperLogLog::<Ertl>::default();
for i in 0..1000u64 {
    hll.update(&DataInput::U64(i));
}
let card = hll.estimate();
assert!(card > 900);
```

## Caveats

- `HyperLogLogHip` is not mergeable.
- P12 variants trade accuracy for lower memory; expect slightly higher
  error than the default P14.

## Status

Canonical cardinality implementation in this library.
