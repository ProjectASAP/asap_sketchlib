# API: Coco

Status: `Unstable`

> Warning: This API is kept for backward compatibility. It uses specialized substring semantics and does not match structured sketch interfaces.

## Purpose

Flow-size estimator over arbitrary key spaces, extended with substring-based
subset aggregation.

## Type/Struct

- `Coco<H = DefaultXxHasher>`
- `CocoBucket`

## Constructors

```rust
fn new() -> Self
fn init_with_size(w: usize, d: usize) -> Self
```

`new()` builds a `1024 x 4` table: 4096 buckets, 128 KiB, plus a heap allocation
per occupied bucket for its stored key.

Sizing: `d` is the number of arrays an insert scans, and the paper recommends
2 to 4 — larger `d` spreads small flows better at the cost of a longer scan per
packet. `w` is the buckets per array, and it is the parameter to raise: bucket
count drives the collision rate, and the sketch can attribute mass to at most
`w * d` distinct keys at once, so size it against the number of heavy flows you
expect to resolve.

## Insert/Update

```rust
fn insert(&mut self, key: &str, v: u64)
```

The SIGCOMM '21 update: the `d` mapped buckets are scanned for `key` first and a
match absorbs `v` directly; otherwise the whole increment lands in the smallest
of them and that bucket's key is replaced with `key` with probability `v / val`.
When several buckets share that smallest value the paper draws one of them
uniformly at random, which matters most on a fresh table where every mapped
bucket still holds `0`.

## Query

```rust
fn estimate_key(&self, key: &str) -> u64
fn estimate(&self, partial_key: &str) -> u64
fn estimate_with_udf<F>(&self, partial_key: &str, udf: F) -> u64
```

`estimate_key` is the paper's point query: the sum of the `d` mapped buckets
that currently hold `key`, in `O(d)`. Because every increment is attributed to
exactly one bucket, summing it over the observed keys returns the total inserted
mass.

`estimate` and `estimate_with_udf` are the subset-query extension. They scan the
whole table in `O(w * d)` and sum every bucket whose stored key matches by
substring or by the supplied predicate, so `estimate("k1")` also collects `k10`,
`k11`, and so on.

## Merge

```rust
fn merge(&mut self, other: &Coco<H>)
```

## Serialization

Derives serde; no dedicated byte API helpers.

## Examples

```rust
use asap_sketchlib::Coco;

let mut sk = Coco::init_with_size(64, 4);
sk.insert("region=us|id=1", 3);
let _ = sk.estimate_key("region=us|id=1");
let _ = sk.estimate("region=us");
```

## Caveats

- `estimate`/`estimate_with_udf` are substring/UDF based, not point queries.
- Replacement behavior is probabilistic.

## Status

Unstable.
