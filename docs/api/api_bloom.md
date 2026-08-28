# API: Bloom

Status: `Ready`

## Purpose

Approximate set membership. `contains` never says no about a key that was
inserted, and says yes about a bounded fraction of keys that were not.

## Type/Struct

- `Bloom<Mode = RegularPath, H = DefaultXxHasher>`
- `BitMatrix` — the packed bit grid, a `MatrixStorage` in its own right

This is the *partitioned* variant: the filter is `rows` slices of `cols` bits,
one slice per hash function, which is the `rows x cols` shape `CountMin` probes.
A membership query is the minimum across rows, which over single bits is their
AND.

## Constructors

```rust
fn with_capacity(expected_items: usize, target_fpp: f64) -> Self
fn with_dimensions(rows: usize, cols: usize) -> Self
fn dimensions_for(expected_items: usize, target_fpp: f64) -> (usize, usize)
```

`with_capacity` sizes from the standard formula — `m = -n ln p / (ln 2)^2` bits
over `k = (m/n) ln 2` hash functions — then splits `m` into `k` slices and
rounds each slice up to a power of two. The rounding keeps the column fold free
of modulo bias, so the delivered rate lands under the target rather than over
it. `dimensions_for` reports the choice without allocating.

`Default` is 7 x 65536 — 56 KiB of packed bits — which holds 20k distinct keys
at about 1 false positive in 11,500.

`with_dimensions` is the escape hatch when memory is fixed. A non-power-of-two
`cols` folds with a modulo and carries the bias the sized path avoids.

## Insert/Update

```rust
fn insert(&mut self, value: &DataInput)
fn bulk_insert(&mut self, values: &[DataInput])
```

One bit per slice. Re-inserting a key sets nothing new, so the fill is a
function of the distinct keys alone while `inserted()` counts every call.

## Query

```rust
fn contains(&self, value: &DataInput) -> bool
```

False is exact: the key was never inserted. True is probabilistic.

```rust
fn predicted_fpp(&self, distinct_items: usize) -> f64
fn estimated_fpp(&self) -> f64
fn fill_ratio(&self) -> f64
fn inserted(&self) -> u64
fn rows(&self) -> usize
fn cols(&self) -> usize
fn bit_capacity(&self) -> usize
fn size_in_bytes(&self) -> usize
fn is_empty(&self) -> bool
```

`predicted_fpp` is the model, `(1 - e^(-n/cols))^rows`. `estimated_fpp` reads
the bits actually set, so it is the one to trust on a filter whose distinct
count is unknown.

## Merge

```rust
fn merge_from(&mut self, other: &Self)
```

Bitwise union. Both filters must have the same dimensions and hasher; the
result is exactly the filter the concatenated streams would have built, which
makes the filter shardable without loss. Mismatched dimensions assert.

## Serialization

Derived serde. The packed words are the state; there is no derived index to
rebuild.

## Examples

```rust
use asap_sketchlib::{Bloom, DataInput, RegularPath};

let mut seen = Bloom::<RegularPath>::with_capacity(1_000_000, 0.001);
seen.insert(&DataInput::Str("10.0.0.1:443"));

if !seen.contains(&DataInput::Str("10.0.0.2:443")) {
    // definitely new
}
```

## Caveats

- No deletion. Clearing a bit would erase it for every key that hashes there.
  A counting variant would need a counter matrix rather than a bit one.
- No count and no cardinality. The filter answers membership only.
- The two hash paths set different bits for the same key, so a `RegularPath`
  filter and a `FastPath` filter are not interchangeable and cannot be merged.
- `with_dimensions(1, 1)` is legal and answers yes to everything after the
  first insert.

## Relation to the paper

Bloom's 1970 construction with the per-slice partitioning of Kirsch and
Mitzenmacher: `k` disjoint slices rather than one shared array. The false-positive
constant is marginally worse than the shared-array form and the row loop is the
same one every matrix-backed sketch in this crate runs.

## Status

Ready.
