# API: Hydra

Status: `Ready`

## Purpose

Subpopulation sketching over a fixed schema of labelled key columns; an update
fans its row out into all `2^D - 1` non-empty subpopulations.

## Type/Struct

- `Hydra`

## Constructors

```rust
fn with_schema<S, I>(rows: usize, cols: usize, schema: I, prototype: HydraCounter) -> Result<Self, String>
```

## Insert/Update

```rust
fn update(&mut self, key: &[&str], value: &DataInput, count: Option<i32>) -> Result<(), String>
```

## Query

```rust
fn query_key(&self, key: &[Option<&str>], query: &HydraQuery) -> Result<f64, String>
fn query_frequency(&self, key: &[Option<&str>], value: &DataInput) -> Result<f64, String>
fn query_cdf(&self, key: &[Option<&str>], threshold: f64) -> Result<f64, String>
fn query_group_total(&self, key: &[Option<&str>]) -> Result<f64, String>
fn schema(&self) -> &[String]
```

A key is positional and full width: one value per column on `update`, and on a
query `Some(v)` to constrain a column or `None` to leave it free, with at least
one constrained. `query_cdf` dispatches `HydraQuery::Cdf`.

`query_group_total` returns a subpopulation's total mass (`count by (g)`): the
minimum over rows of the subkey's cell `HydraCounter::total_mass()`. A Count-Min
cell's mass is its inner row sum, exact while each `i32` counter stays below
`2^31`; a KLL cell's is its retained weight, which is approximate once the KLL
compacts. Other counters return an error. The total is read from existing cell
state, so the wire format carries it unchanged.

Error bound (Count-Min cells). The statement uses an ideal uniform hash model in
which each row's column is independent of the other rows'. Let `n_Q` be the
subpopulation's true total, `N` the total inserted mass, `D` the number of key
columns, `w = cols` and `r = rows`. With non-negative counts a row's cell holds
`n_Q` plus the mass of every other subkey hashed to it. A column is a hash
slice of `ceil(log2 w)` bits taken modulo `w`, so two subkeys share a column
with probability `q = 1/w` when `w` is a power of two and `q <= 9/(8w)`
otherwise, and the expected excess
is at most `q M` for the post-fan-out mass `M = N (2^D - 1)`. By Markov a row
exceeds `n_Q + 4M/w` with probability at most `qw/4`, and the minimum exceeds it
only if every row does:

`n_Q <= estimate <= n_Q + 4 N (2^D - 1) / w` with probability at least
`1 - (9/32)^r`, and at least `1 - (1/4)^r` when `w` is a power of two.

Negative counts void the bound.

## Merge

```rust
fn merge(&mut self, other: &Hydra) -> Result<(), String>
```

## Serialization

```rust
fn to_asapv1(&self) -> Result<Vec<u8>, rmp_serde::encode::Error>
fn from_asapv1(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error>
```

ASAPv1 MessagePack, one kind_id per counter variant: `0x07 0x00` KLL,
`0x07 0x01` Count-Min, `0x07 0x02` Count Sketch, `0x07 0x03` HyperLogLog,
`0x07 0x04` UnivMon. `from_asapv1` routes on the kind_id and rejects
any other one.

The metadata carries the hash spec (Hydra hashes its subkeys, so it is present
even for the KLL counter), the grid `rows` / `cols`, the key-column `schema`,
and the counter's own structural params — `cell_rows`, `cell_cols`,
`cell_counter_type` (`"i32"`) and `cell_mode` (`"fast"`) for the two matrix
counters; `cell_precision` for HyperLogLog; `cell_k`, `cell_m` and
`cell_item_type` (`"f64"`) for KLL; `cell_layers`, `cell_rows`, `cell_cols`,
`cell_heap_size` and `cell_key_type` for UnivMon.

The payload is the counters' raw state, row-major over the grid: `[counts]` for
the matrix counters, `[registers]` for HyperLogLog, `[cells]` for KLL and
UnivMon. Counters are inlined, not nested in their own envelopes, and every
cell shares the prototype's geometry, so it is carried once.

The grid and the matrix counters are both hashed per row, so each carries the
same bound: `1 <= rows <= 20` and `1 <= cell_rows <= 20` (`MATRIX_MAX_ROWS`,
the seed list length).

A grid mixing counter variants, UnivMon cells mixing key variants, a cell whose
geometry differs from the prototype's, a `prototype` holding data, a
declared grid the storage does not match, a row count past `MATRIX_MAX_ROWS`,
and a payload length that disagrees with the declared geometry are all rejected
on both sides.

## Examples

```rust
use asap_sketchlib::{DataInput, Hydra, input::HydraCounter};

let counter = HydraCounter::CountMinSketch(Default::default());
let mut hydra = Hydra::with_schema(4, 4096, ["region", "service"], counter).unwrap();
hydra.update(&["us", "api"], &DataInput::Str("err"), None).unwrap();
let est = hydra.query_frequency(&[Some("us"), None], &DataInput::Str("err")).unwrap();
assert!(est >= 1.0);
```

## Caveats

- Canonical enum/query/input definitions are in [Common Input Types](./api_common_input.md).
- Query compatibility depends on `HydraCounter` variant.

## Status

Primary subpopulation framework with broad test coverage.
