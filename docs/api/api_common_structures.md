# API: Common Structures

Status: `Shared`

## Purpose

Shared matrix/vector storage and utility structures used by sketch implementations.

## Type/Struct

- `Vector2D<T>`
- `MatrixStorage` / `FastPathHasher`
- `MatrixHashType`
- `Nitro`

## Constructors

```rust
// Vector2D
fn init(rows: usize, cols: usize) -> Self
fn from_fn<F>(rows: usize, cols: usize, f: F) -> Self

// Nitro
fn init_nitro(rate: f64) -> Self
```

## Insert/Update

```rust
// Vector2D
fn update_one_counter<F, V>(&mut self, row: usize, col: usize, op: F, value: V)
fn fast_insert<F, V>(&mut self, op: F, value: V, hashed_val: &MatrixHashType)
fn update_by_row<F, V>(&mut self, row: usize, hashed: u128, op: F, value: V)

// Nitro utility
fn draw_geometric(&mut self)
fn reduce_to_skip(&mut self)
fn reduce_to_skip_by_count(&mut self, c: usize)
```

## Query

```rust
// Vector2D
fn rows(&self) -> usize
fn cols(&self) -> usize
fn get(&self, row: usize, col: usize) -> Option<&T>
fn row_slice(&self, row: usize) -> &[T]
fn fast_query_min<F, R>(&self, hashed_val: &MatrixHashType, op: F) -> R
fn fast_query_median<F>(&self, hashed_val: &MatrixHashType, op: F) -> f64
fn fast_query_max<F, R>(&self, hashed_val: &MatrixHashType, op: F) -> R

// Utility
fn compute_median_inline_f64(values: &mut [f64]) -> f64
```

## Merge

Not applicable at this utility-layer boundary.

## Serialization

Not applicable at this utility-layer boundary.

## Examples

```rust
use sketchlib_rust::Vector2D;

let matrix = Vector2D::<i32>::init(3, 16);
assert_eq!(matrix.rows(), 3);
assert_eq!(matrix.cols(), 16);
```

## Caveats

- This page summarizes commonly used entry points; full module context remains in [Common Module (Canonical)](./api_common.md).

## See Also

- [Common Module (Canonical)](./api_common.md)
- [Common Input Types](./api_common_input.md)

## Status

Canonical shared structures layer.
