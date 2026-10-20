# API: DDSketch

Status: `Ready`

## Purpose

Approximate quantile estimation with relative error guarantees.

## Type/Struct

- `DdSketch`

## Constructors

```rust
fn new(alpha: f64) -> Self
```

## Insert/Update

```rust
fn add<T: NumericalValue>(&mut self, val: &T)
fn add_input(&mut self, v: &DataInput) -> Result<(), &'static str>
```

`add` accepts any type implementing `NumericalValue` (all standard integer and
float primitives). Values are internally converted to `f64`; non-positive or
non-finite values are ignored. `add_input` is the `DataInput` adapter used by
the type-erased dispatch path.

## Query

```rust
fn get_value_at_quantile(&self, q: f64) -> Option<f64>
fn get_count(&self) -> u64
fn min(&self) -> Option<f64>
fn max(&self) -> Option<f64>
```

## Merge

```rust
fn merge(&mut self, other: &DdSketch) -> Result<(), String>
```

## Serialization

```rust
fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>
```

These produce/consume the **ASAPv1** wire envelope (kind `0x05 0x00`) — see the
[ASAPv1 wire format spec](../asapv1_wire_format.md). DDSketch never hashes its
inputs, so the metadata carries **no hash-spec group**: it is `metadata_version`
plus `alpha`, the sketch's one construction parameter. `gamma` and its logs are
derived from `alpha` and never reach the wire.

Every payload is `[counts, offset, sum, min, max, negative_counts,
negative_offset, zero_count]`: the positive store and the negative-magnitude
store carried verbatim (the bucket index of `counts[i]` is `offset + i`), the
three scalars the buckets do not determine, and the zero count. `count` is
**not** carried — it is the sum of both stores and `zero_count`, recovered
exactly on decode. `sum`, `min` and `max` come back exactly rather than as
α-bounded bucket estimates, so a decoded sketch re-serializes byte-identically.

Bucket indices are signed, so either offset may be negative. Decode rejects a
`metadata_version` other than `1`, an `alpha` outside `(0, 1)`, a store span
past `i32`, a non-zero offset on an empty store, counts that overflow the total,
and scalars inconsistent with that total; `to_asapv1` enforces the same
rules, so the format never emits bytes it would refuse to read back.

Independently of ASAPv1, `DdSketch` is plain `Serialize`/`Deserialize` for use
where it is nested in another type (`EhBucketSketch`); that form skips the four
running scalars.

## Deltas

```rust
DdSketchDelta

fn new(
    alpha: f64,
    indices: Vec<i32>,
    deltas: Vec<u64>,
    negative_indices: Vec<i32>,
    negative_deltas: Vec<u64>,
    zero_count: u64,
) -> Result<Self, String>
fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError>
fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError>

// on DdSketch
fn merge_delta(&mut self, delta: &DdSketchDelta) -> Result<(), String>
```

A `DdSketchDelta` is the ASAPv1 delta kind `0x05 0x80`: the changed buckets of
each store as strictly ascending bucket indices inside the range `alpha`'s
mapping produces, the non-zero count added to each, and a count added to the
zero count. `new` and the decoder refuse anything else. `merge_delta` adds each
count to its bucket, growing the store as insertion does, and folds the
bucket's representative into `sum`, `min` and `max`; a delta of another
`alpha` or admission rate, one that overflows the total count, or one that
would grow a store past `i32`, fails and leaves the sketch unchanged.

## Admission rate

```rust
fn with_admission_rate(self, p: f64) -> Self
fn admission_rate(&self) -> f64

// on DdSketchDelta
fn with_admission_rate(self, p: f64) -> Self
```

`p` is the probability with which each update was admitted, `1` unless set.
`count` and `sum` cover admitted samples alone; a consumer divides them by `p`.
Quantiles, `min` and `max` need no rescaling. The ASAPv1 metadata carries `p` as
its last key, `admission_rate`, when `p < 1`; a `p` outside `(0, 1]` fails to
serialize. `merge` and `merge_delta` fail when the two rates differ. A sketch
nested in another kind (`EhBucketSketch`, `Hydra`, CountMinSketchWithHeap, CountSketchWithHeap, Elastic) must
hold `p = 1` to serialize.

## Examples

```rust
use asap_sketchlib::DdSketch;

let mut dds = DdSketch::new(0.01);
dds.add(&1.0_f64);
dds.add(&2_i32);
let p50 = dds.get_value_at_quantile(0.5).unwrap();
assert!(p50 >= 1.0);
```

## Quantile convention

`get_value_at_quantile(q)` uses the **nearest-rank** convention
`rank = ceil(q * n)` (1-based), so it answers the order statistic
`sorted[ceil(q*n) - 1]`. `q <= 0` and `q >= 1` short-circuit to the exactly
retained minimum and maximum rather than to a bucket representative, so the
endpoints carry zero error. The convention is pinned by
`ddsketch_answers_the_ceil_nearest_rank_order_statistic`.

## Caveats

- Inputs must be positive values.
- Merge returns `Err` unless both sketches share the same `alpha`.

## Status

Supported and tested for multiple distributions.
