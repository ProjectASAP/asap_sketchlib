# Common Module API Design

The `src/common` package gathers primitives that every sketch relies on: a common
input shape, deterministic hashing helpers, and lightweight vector wrappers that
encode layout assumptions. This note documents the current surface so new
sketches can slot in without rediscovering invariants.

## Module Layout

- `mod.rs` exposes the `input` and `structures` submodules and re-exports
  `SketchInput`, `hash_it`, `LASTSTATE`, `SEEDLIST`, and the vector wrappers.
- Prefer importing from `sketchlib_rust::common::{SketchInput, Vector2D}` rather
  than pathing directly to submodules; the re-export keeps public API decisions
  centralized.

## Input Primitives (`common::input`)

### `SketchInput<'a>`

- Enum that wraps the shapes we accept at ingestion time:
  `I32`, `I64`, `U32`, `U64`, `F32`, `F64`, `Str(&'a str)`, `String`, and
  `Bytes(&'a [u8])`.
- Borrowed variants carry a lifetime, so prefer `String` / `Vec<u8>` when the
  caller's buffer is short-lived.
- Sketches can accept a single enum rather than maintaining per-type overloads.
  Downstream code typically pattern-matches once before hashing or storing.
- **TODO**: Add support for generic type / custom type T
  - challenge 1: the trait that requires the T to have
  - challenge 2: lifetime

### Hashing Helpers

- `LASTSTATE: usize = 5` matches the final valid index into `SEEDLIST`.
- `SEEDLIST: [u64; 6]` provides deterministic per-row seeds. Expand both together if you need more domains.
  - predefined seed so that sketches can be merged
  - **TODO**: maybe need to be longer
- `hash_it(d, key)` hashes the provided key with the `d`th seed. The helper will panic if `d` exceeds the seed list, so runtime callers must either validate indices or wrap the function with their own bounds check.
  - easy to test around for different hash functions
- `hash_for_all_rows(r, key)` reuses a single hash to derive `r` column
  candidates in 13-bit windows. It returns a `Vec<u64>`; allocate once and reuse
  the buffer at call sites that are sensitive to heap churn.
  - **Special**: return a `Vec` is slow, so the use case needs reconsideration or just remove it
- **TODO**: custom hash functions to generate only required bits (to save time)

## Shared Structures (`common::structures`)

### `Vector1D<T: Clone>`

- Thin wrapper around `Vec<T>` that records the intended logical length.
- Constructors: `init(len)` (reserve capacity) and `filled(len, value)`.
- Mutation helpers: `fill`, `update_if_greater`, `update_if_smaller`,
  `update_one_counter`. All expect the index to be in range—no extra bounds
  checks beyond standard panics.
- Accessors: `len`, `is_empty`, `as_slice`, `as_mut_slice`, `get`, `get_mut`,
  iterator helpers, and `into_vec`.
- **TODO**: think about other missing functions

### `Vector2D<T>`

- Row-major matrix backed by a flat `Vec<T>` plus `(rows, cols)` metadata.
- Constructors: `init(rows, cols)` and `from_fn(rows, cols, f)` for eager
  population.
- Utility methods mirror `Vector1D` plus 2D-specific helpers:
  - `fill` replaces the full matrix.
  - `rows`, `cols`, `len`, `row_slice`, `row_slice_mut`.
  - Index operators (`Index`, `IndexMut`) expose row slices directly.
- Sketch-oriented paths:
  - `update_one_counter(row, col, op, value)` applies a closure to a single cell.
  - `fast_insert(op, value, hashed_val)` computes per-row column indices using the same 13-bit window convention as `hash_for_all_rows`.
    - **TODO**: not scalable, needs to redesign
    - Current Pro: takes in hash value, not key, so that an extra hash layer elsewhere possible
    - Current Con: no place to give requirements to hash values
  - `fast_query(hashed_val)` computes the minimum across rows after deriving the matching column in each row. Assumes `T: Clone + Ord`.
    - **TODO**: same as `hash_for_all_rows`
- Keep callers responsible for bounds safety; any invalid `(row, col)` will panic.

### `Vector3D<T>`

- Placeholder wrapper that currently only offers `init(layer, row, col)`.
- Before relying on it, round out parity with `Vector2D` (slice access, `fill`,
  fast update/query helpers). Otherwise, prefer composing multiple `Vector2D`
  instances for clarity.

### Heap (planned add)

- **Goal:** provide a lightweight binary heap optimized for sketches that track
  top-k or bottom-k elements without pulling in `std::collections::BinaryHeap`
  everywhere.
- **Shape:** `struct SketchHeap<T, Ord = std::cmp::Reverse<T>>` wrapping a
  pre-allocated `Vec<T>` plus a comparator. Default to a min-heap so `Ord`
  matches the usual top-k flow, but allow callers to swap comparator types for
  max-heap semantics.
- **Constructors:**
  - `with_capacity(cap, comparator)` to pre-size storage (avoids repeated allocations during streaming inserts).
  - `from_vec(vec, comparator)` for bulk-load scenarios.
- **Core operations:**
  - `push(value)` inserts a new element and preserves the heap property.
  - `pop()` removes the extremum according to the comparator.
  - `peek()` returns a reference to the extremum without removal.
  - `len()` / `is_empty()` mirror `Vec` conventions.
  - `shrink_to_fit()` optional helper when callers want to trim slack.
- **Sketch-specific helpers:**
  - `push_bounded(value, bound)` inserts only if the heap has capacity; when
    full, compare against the extremum and replace when the new value wins. This
    avoids the "push then pop" dance in high-volume streams.
  - `iter_sorted()` materializes a sorted iterator for reporting, using a
    scratch buffer so the live heap stays intact.
- **Safety:** the type should be `#[derive(Serialize, Deserialize)]` so heaps
  can be persisted alongside other structures. Ensure comparator implements
  `Clone` or require `'static + Send + Sync` depending on use cases.
- **Integration:** host the implementation in `common::structures::heap` and
  re-export `SketchHeap` from `common::mod.rs` once stable. Downstream sketches
  (e.g., KLL variants, heavy-hitter tracking) can opt in when they need bounded
  order statistics.

## Conventions and Extension Points

- **Bounds & invariants:** Helpers rely on caller discipline rather than adding runtime checks. Wrap them if you need defensive validations in higher-level APIs.
- Benchmark to test the optimization required
- **Logic separation** between *data plane* and *control plane* is not clear in the current API design
- **Hashing windows:** The 13-bit window (`(1u64 << 13) - 1`) is shared by `hash_for_all_rows`, `Vector2D::fast_insert`, and `Vector2D::fast_query`. Keep the constant in sync if you tune the width.
- **SIMD** Optimization needs consideration
- **Extra Hash Layer** location needs consideration
<!-- - **API ergonomics:** Consider adding `From` impls or constructors for
  `SketchInput` to reduce manual enum construction. Avoid adding traits until
  we have more than one alternative backend. -->
