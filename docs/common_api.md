# Common Module API Reference

The `src/common` module provides foundational building blocks for all sketch implementations in sketchlib-rust. It defines shared data structures, input types, and hashing utilities for efficient and consistent operations.

---

## Table of Contents

1. [Module Organization](#module-organization)
2. [Input Types](#input-types)
3. [Data Structures](#data-structures)
4. [Hashing Functions](#hashing-functions)
5. [Hash Customization](#hash-customization)
6. [Heavy Hitter Heap](#heavy-hitter-heap)

---

## Module Organization

**File:** [src/common/mod.rs](../src/common/mod.rs)

The common module is organized into several submodules:

```text
src/common/
├── mod.rs                  # Public API exports
├── input.rs                # SketchInput, HeapItem, HHItem, framework enums
├── hash.rs                 # Hash functions and seed constants
├── heap.rs                 # HHHeap wrapper for heavy hitter tracking
├── structure_utils.rs      # Utility functions (Nitro, median)
├── precompute_hash.rs      # Pre-computed hash values
├── precompute_sample.rs    # Pre-computed samples
├── precompute_sample2.rs   # Pre-computed samples (1% rate)
└── structures/
    ├── mod.rs              # Re-exports
    ├── vector1d.rs         # Vector1D implementation
    ├── vector2d.rs         # Vector2D implementation
    ├── vector3d.rs         # Vector3D implementation
    ├── matrix_storage.rs   # MatrixStorage trait
    ├── fixed_structure.rs  # FixedMatrix implementation
    └── heap.rs             # CommonHeap, CommonHeapOrder, KeepSmallest, KeepLargest
```

**Public exports:**

```rust
use sketchlib_rust::common::{
    // Input types
    SketchInput, HeapItem, HHItem,
    L2HH, input_to_owned, heap_item_to_sketch_input,

    // Data structures
    Vector1D, Vector2D, Vector3D,
    MatrixStorage, FastPathHasher, MatrixHashType,
    FixedMatrix, HllBucketList,
    DefaultMatrixI32, DefaultMatrixI64, DefaultMatrixI128,
    QuickMatrixI32, QuickMatrixI64, QuickMatrixI128,
    CommonHeap, CommonHeapOrder, KeepSmallest, KeepLargest,
    HHHeap,

    // Hash functions
    SketchHasher, DefaultXxHasher,
    hash64_seeded, hash128_seeded,
    hash_item64_seeded, hash_item128_seeded,
    hash_for_matrix, hash_for_matrix_seeded, hash_for_matrix_seeded_with_mode,
    hash_for_matrix_generic, hash_for_matrix_seeded_generic,
    hash_mode_for_matrix, MatrixHashMode,

    // Constants
    SEEDLIST, CANONICAL_HASH_SEED, BOTTOM_LAYER_FINDER, HYDRA_SEED,
    PRECOMPUTED_HASH, PRECOMPUTED_SAMPLE, PRECOMPUTED_SAMPLE_RATE_1PERCENT,

    // Utilities
    Nitro, compute_median_inline_f64,
};
```

---

## Input Types

**File:** [src/common/input.rs](../src/common/input.rs)

### SketchInput

Unified type-agnostic input for all sketches with zero-copy support.

```rust
pub enum SketchInput<'a> {
    // Signed integers
    I8(i8), I16(i16), I32(i32), I64(i64), I128(i128), ISIZE(isize),

    // Unsigned integers
    U8(u8), U16(u16), U32(u32), U64(u64), U128(u128), USIZE(usize),

    // Floating point
    F32(f32), F64(f64),

    // Strings and bytes
    Str(&'a str),       // Borrowed (zero-copy)
    String(String),      // Owned
    Bytes(&'a [u8]),    // Borrowed byte slice
}
```

**Usage:**

```rust
sketch.insert(&SketchInput::U64(12345));
sketch.insert(&SketchInput::Str("key"));  // Zero-copy
sketch.insert(&SketchInput::String("owned".into()));
```

### HeapItem

Owned variant of SketchInput for heap storage (no borrowed types).

```rust
pub enum HeapItem {
    I8(i8), I16(i16), I32(i32), I64(i64), I128(i128), ISIZE(isize),
    U8(u8), U16(u16), U32(u32), U64(u64), U128(u128), USIZE(usize),
    F32(f32), F64(f64),
    String(String),  // Note: no &str or &[u8] variants
}
```

**Conversion:**

```rust
pub fn input_to_owned(input: &SketchInput) -> HeapItem
```

### HHItem

Key-count pair for heavy hitter tracking.

```rust
pub struct HHItem {
    pub key: HeapItem,    // Owned key (not borrowed)
    pub count: i64,
}

impl HHItem {
    pub fn new(k: SketchInput, count: i64) -> Self
    pub fn create_item(k: HeapItem, count: i64) -> Self
    pub fn init_item(k: SketchInput, count: i64) -> Self
    pub fn print_item(&self)
}
```

**Ordering:** Implements `Ord` by comparing `count` field.

### Framework Enums

#### HydraCounter

Counter types for Hydra hierarchical sketches.

```rust
pub enum HydraCounter {
    CM(CountMin<Vector2D<i32>, FastPath>),
    HLL(HyperLogLog<DataFusion>),
    CS(Count<Vector2D<i32>, FastPath>),
    KLL(KLL),
    UNIVERSAL(UnivMon),
}

impl HydraCounter {
    pub fn insert(&mut self, value: &SketchInput, count: Option<i32>)
    pub fn query(&self, query: &HydraQuery) -> Result<f64, String>
    pub fn merge(&mut self, other: &HydraCounter) -> Result<(), String>
}
```

#### HydraQuery

Query types for Hydra framework.

```rust
pub enum HydraQuery<'a> {
    Frequency(SketchInput<'a>),  // For CountMin, Count
    Quantile(f64),                // For KLL
    Cdf(f64),                     // For KLL cumulative distribution
    Cardinality,                  // For HyperLogLog
    L1Norm,                       // For UnivMon
    L2Norm,                       // For UnivMon
    Entropy,                      // For UnivMon
}
```

#### L2HH

L2-Heavy-Hitter wrapper for UnivMon.

```rust
pub enum L2HH {
    COUNT(CountL2HH),
}

impl L2HH {
    pub fn update_and_est(&mut self, key: &SketchInput, value: i64) -> f64
    pub fn update_and_est_without_l2(&mut self, key: &SketchInput, value: i64) -> f64
    pub fn get_l2(&self) -> f64
    pub fn merge(&mut self, other: &L2HH)
}
```

---

## Data Structures

**File:** [src/common/structures/](../src/common/structures/)

### Vector1D

Lightweight 1D vector wrapper for sketch counters.

```rust
pub struct Vector1D<T> {
    data: Vec<T>,
}
```

**Constructors:**

```rust
fn init(capacity: usize) -> Self
fn filled(len: usize, value: T) -> Self where T: Clone
fn from_vec(vec: Vec<T>) -> Self
```

**Core methods:**

```rust
fn len(&self) -> usize
fn is_empty(&self) -> bool
fn as_slice(&self) -> &[T]
fn as_mut_slice(&mut self) -> &mut [T]
fn as_mut_ptr(&mut self) -> *mut T
fn get(&self, index: usize) -> Option<&T>
fn get_mut(&mut self, index: usize) -> Option<&mut T>
fn last_mut(&mut self) -> Option<&mut T>
fn iter(&self) -> impl Iterator<Item = &T>
fn iter_mut(&mut self) -> impl Iterator<Item = &mut T>
fn fill(&mut self, value: T) where T: Clone
fn insert(&mut self, pos: usize, val: T)
fn update_if_greater(&mut self, pos: usize, val: T) where T: Copy + Ord
fn update_if_smaller(&mut self, pos: usize, val: T) where T: Copy + Ord
fn update_one_counter<F, V>(&mut self, pos: usize, op: F, value: V)
    where F: Fn(&mut T, V), T: Clone
fn push(&mut self, value: T)
fn truncate(&mut self, len: usize)
fn append(&mut self, other: &mut Vec<T>)
fn extend_from_slice(&mut self, other: &[T]) where T: Clone
fn swap(&mut self, a: usize, b: usize)
fn sort_by<F>(&mut self, compare: F) where F: FnMut(&T, &T) -> std::cmp::Ordering
fn sort_unstable_by<F>(&mut self, compare: F) where F: FnMut(&T, &T) -> std::cmp::Ordering
fn clear(&mut self)
fn into_vec(self) -> Vec<T>
```

**Indexing:** Supports `vector[i]` for read/write.

### MatrixStorage

Trait bound for matrix-backed sketches (CountMin/Count). It enforces the
hash width and fast-path operations at the type level.

```rust
pub trait MatrixStorage {
    type Counter: Clone;
    type HashValueType;
    fn rows(&self) -> usize;
    fn cols(&self) -> usize;

    fn update_one_counter<F, V>(&mut self, row: usize, col: usize, op: F, value: V)
        where F: Fn(&mut Self::Counter, V);
    fn increment_by_row(&mut self, row: usize, col: usize, value: Self::Counter);

    fn fast_insert<F, V>(&mut self, op: F, value: V, hashed_val: &Self::HashValueType)
        where F: Fn(&mut Self::Counter, &V, usize), V: Clone;
    fn fast_query_min<F, R>(&self, hashed_val: &Self::HashValueType, op: F) -> R
        where F: Fn(&Self::Counter, usize, &Self::HashValueType) -> R, R: Ord;
    fn fast_query_median<F>(&self, hashed_val: &Self::HashValueType, op: F) -> f64
        where F: Fn(&Self::Counter, usize, &Self::HashValueType) -> f64;
    fn query_one_counter(&self, row: usize, col: usize) -> Self::Counter;
}
```

**FastPathHasher** extends `MatrixStorage` with hash computation:

```rust
pub trait FastPathHasher: MatrixStorage {
    fn hash_for_matrix(&self, value: &SketchInput) -> Self::HashValueType;
}
```

**Implementations:**

- `Vector2D<i32>`: flexible dimensions, `Counter = i32`, `HashValueType = MatrixHashType`
- `FixedMatrix` (`QuickMatrixI32`): fixed 5 × 2048, `Counter = i32`, `HashValueType = u64`
- `DefaultMatrixI32`: fixed 3 × 4096, `Counter = i32`, `HashValueType = u64`
- Also available: `DefaultMatrixI64`, `DefaultMatrixI128`, `QuickMatrixI64`, `QuickMatrixI128`

### Vector2D

High-performance 2D matrix with fast-path hash-based operations.

```rust
pub struct Vector2D<T> {
    data: Vec<T>,
    rows: usize,
    cols: usize,
    mask_bits: u32,     // Pre-computed for fast column selection
    mask: u128,
    hash_mode: MatrixHashMode,
    nitro: Nitro,       // Optional sampling support
}
```

**Constructors:**

```rust
fn init(rows: usize, cols: usize) -> Self
fn from_fn<F>(rows: usize, cols: usize, f: F) -> Self
    where F: FnMut(usize, usize) -> T
```

**Core methods:**

```rust
fn rows(&self) -> usize
fn cols(&self) -> usize
fn len(&self) -> usize  // Total elements (rows × cols)
fn as_slice(&self) -> &[T]
fn as_mut_slice(&mut self) -> &mut [T]
fn get(&self, row: usize, col: usize) -> Option<&T>
fn get_mut(&mut self, row: usize, col: usize) -> Option<&mut T>
fn fill(&mut self, value: T) where T: Clone
fn allocate_extra_row(&mut self, value: T) where T: Clone
fn is_empty(&self) -> bool
fn update_one_counter<F, V>(&mut self, row: usize, col: usize, op: F, value: V)
    where F: Fn(&mut T, V), T: Clone
fn update_by_row<F, V>(&mut self, row: usize, hashed: u128, op: F, value: V)
    where F: Fn(&mut T, V), T: Clone
fn hash_for_matrix(&self, value: &SketchInput) -> MatrixHashType
fn get_mask_bits(&self) -> u32
fn get_required_bits(&self) -> usize
fn query_one_counter(&self, row: usize, col: usize) -> T where T: Clone
fn row_slice(&self, row: usize) -> &[T]
fn row_slice_mut(&mut self, row: usize) -> &mut [T]
fn get_row(&self) -> usize
```

**Fast-path methods** (hash-optimized):

```rust
// Insert using pre-computed hash
fn fast_insert<F, V>(&mut self, op: F, value: V, hashed_val: &MatrixHashType)
    where F: Fn(&mut T, &V, usize), V: Clone

// Query methods
fn fast_query_min<F, R>(&self, hashed_val: &MatrixHashType, op: F) -> R
    where F: Fn(&T, usize, &MatrixHashType) -> R, R: Ord
fn fast_query_max<F, R>(&self, hashed_val: &MatrixHashType, op: F) -> R
    where F: Fn(&T, usize, &MatrixHashType) -> R, R: Ord
fn fast_query_median<F>(&self, hashed_val: &MatrixHashType, op: F) -> f64
    where F: Fn(&T, usize, &MatrixHashType) -> f64

// Query with key parameter
fn fast_query_min_with_key<F, Q, R>(&self, hashed_val: &MatrixHashType, query_key: &Q, op: F) -> R
    where F: Fn(&T, &Q, usize, &MatrixHashType) -> R, R: Ord
fn fast_query_max_with_key<F, Q, R>(&self, hashed_val: &MatrixHashType, query_key: &Q, op: F) -> R
fn fast_query_median_with_key<F, Q>(&self, hashed_val: &MatrixHashType, query_key: &Q, op: F) -> f64

// Generic aggregation
fn fast_query_aggregate<F, Q, R>(&self, hashed_val: &MatrixHashType, query_key: &Q, init: R, fold_fn: F) -> R
    where F: Fn(R, &T, &Q, usize, &MatrixHashType) -> R
```

**Nitro sampling support:**

```rust
fn enable_nitro(&mut self, sampling_rate: f64)
fn disable_nitro(&mut self)
fn reduce_to_skip(&mut self)
fn get_delta(&self) -> u64
fn nitro(&self) -> &Nitro
fn nitro_mut(&mut self) -> &mut Nitro
fn reduce_nitro_skip(&mut self, c: usize)
fn update_nitro_skip(&mut self, c: usize)
fn get_nitro_skip(&mut self) -> usize
```

**Indexing:** `matrix[row]` returns row slice; `matrix[row][col]` accesses element.

### FixedMatrix

Fixed-size `i32` matrix optimized for quickstart CountMin/Count with fixed
dimensions. Implements `MatrixStorage` with `Counter = i32` and `HashValueType = u64`.

```rust
pub struct FixedMatrix {
    pub data: Box<[i32; QUICKSTART_SIZE]>,
}

pub const QUICKSTART_ROW_NUM: usize = 5;
pub const QUICKSTART_COL_NUM: usize = 2048;
pub const QUICKSTART_SIZE: usize = QUICKSTART_ROW_NUM * QUICKSTART_COL_NUM;
```

**Notes:**

- Fixed layout avoids dynamic allocation resizing.
- Hash width is `u64` (while `Vector2D` uses `MatrixHashType` and can use packed 64/128 or per-row hashes).

### Vector3D

3D tensor for multi-layer sketches.

```rust
pub struct Vector3D<T> {
    data: Vec<T>,
    layer: usize,
    row: usize,
    col: usize,
}
```

**Constructor:**

```rust
fn init(layer: usize, row: usize, col: usize) -> Self
```

**Note:** Minimal implementation - consider using multiple `Vector2D` instances instead.

### CommonHeap

Generic bounded heap with pluggable ordering.

```rust
pub struct CommonHeap<T, O: CommonHeapOrder<T>> {
    data: Vec<T>,
    size: usize,      // Max capacity
    order: O,         // Min or Max heap (zero-sized)
}
```

**Constructors:**

```rust
fn with_capacity(capacity: usize, order: O) -> Self
fn new_min(capacity: usize) -> CommonHeap<T, KeepSmallest>
fn new_max(capacity: usize) -> CommonHeap<T, KeepLargest>
```

**Core methods:**

```rust
fn len(&self) -> usize
fn is_empty(&self) -> bool
fn capacity(&self) -> usize
fn is_full(&self) -> bool
fn peek(&self) -> Option<&T>
fn peek_mut(&mut self) -> Option<&mut T>
fn push(&mut self, value: T)              // Auto-evicts when full
fn pop(&mut self) -> Option<T>
fn clear(&mut self)
fn update_at(&mut self, index: usize) -> bool
fn iter(&self) -> impl Iterator<Item = &T>
fn as_slice(&self) -> &[T]
```

**Heap ordering trait:**

```rust
pub trait CommonHeapOrder<T> {
    fn should_swap(&self, parent: &T, child: &T) -> bool;
    fn should_replace_root(&self, root: &T, new_value: &T) -> bool;
}
```

**Built-in orderings:**

- `KeepSmallest` - Smallest values at root
- `KeepLargest` - Largest values at root

---

## Hashing Functions

**File:** [src/common/hash.rs](../src/common/hash.rs)

### Constants

```rust
pub const CANONICAL_HASH_SEED: usize = 5;
pub const BOTTOM_LAYER_FINDER: usize = 19;
pub const HYDRA_SEED: usize = 6;
pub const SEEDLIST: [u64; 20] = [...];  // Pre-defined seeds
```

### Hash Functions

Hashing uses XxHash3 for 64/128-bit paths.
```rust
// Main hash function (returns u64)
pub fn hash64_seeded(d: usize, key: &SketchInput) -> u64

// 128-bit hash
pub fn hash128_seeded(d: usize, key: &SketchInput) -> u128

// Hash HeapItem directly (optimization)
pub fn hash_item64_seeded(d: usize, key: &HeapItem) -> u64
pub fn hash_item128_seeded(d: usize, key: &HeapItem) -> u128

```

**Usage:**

```rust
let key = SketchInput::U64(12345);
let hash = hash128_seeded(0, &key);  // Use seed 0
```

**Note:** Index `d` must be `< SEEDLIST.len()` (panics otherwise).

---

## Hash Customization

The library exposes a pluggable hasher trait so sketches can be instantiated
with a custom hash implementation when needed.

### SketchHasher Trait

```rust
pub trait SketchHasher: Clone + Debug {
    fn hash64_seeded(d: usize, key: &SketchInput) -> u64;
    fn hash128_seeded(d: usize, key: &SketchInput) -> u128;
    fn hash_item64_seeded(d: usize, key: &HeapItem) -> u64;
    fn hash_item128_seeded(d: usize, key: &HeapItem) -> u128;
}
```

`DefaultXxHasher` is the built-in implementation (XxHash3).

### Generic Hash Helpers

`src/common/hash.rs` also provides generic helpers for matrix hash generation:

```rust
fn hash_for_matrix_generic<H: SketchHasher>(
    rows: usize,
    cols: usize,
    key: &SketchInput,
) -> MatrixHashType

fn hash_for_matrix_seeded_generic<H: SketchHasher>(
    seed_idx: usize,
    rows: usize,
    cols: usize,
    key: &SketchInput,
) -> MatrixHashType
```

### Custom Hasher Example

```rust
use sketchlib_rust::{
    CountMin, DefaultXxHasher, HeapItem, RegularPath, SketchHasher, SketchInput, Vector2D,
};

#[derive(Clone, Debug)]
struct MyHasher;

impl SketchHasher for MyHasher {
    fn hash64_seeded(d: usize, key: &SketchInput) -> u64 {
        <DefaultXxHasher as SketchHasher>::hash64_seeded(d, key)
    }
    fn hash128_seeded(d: usize, key: &SketchInput) -> u128 {
        <DefaultXxHasher as SketchHasher>::hash128_seeded(d, key)
    }
    fn hash_item64_seeded(d: usize, key: &HeapItem) -> u64 {
        <DefaultXxHasher as SketchHasher>::hash_item64_seeded(d, key)
    }
    fn hash_item128_seeded(d: usize, key: &HeapItem) -> u128 {
        <DefaultXxHasher as SketchHasher>::hash_item128_seeded(d, key)
    }
}

let mut cm = CountMin::<Vector2D<i32>, RegularPath, MyHasher>::with_dimensions(3, 1024);
cm.insert(&SketchInput::U64(42));
```

---

## Heavy Hitter Heap

**File:** [src/common/heap.rs](../src/common/heap.rs)

### HHHeap

Specialized wrapper around `CommonHeap` for top-K heavy hitter tracking with fast key lookups.

```rust
pub struct HHHeap {
    heap: CommonHeap<HHItem, KeepSmallest>,
    positions: HashMap<u64, Vec<(HeapItem, usize)>>,  // Fast lookup by hash
    k: usize,
}
```

**Constructor:**

```rust
pub fn new(k: usize) -> Self
```

**Core methods:**

```rust
pub fn update(&mut self, key: &SketchInput, count: i64) -> bool
pub fn update_heap_item(&mut self, key: &HeapItem, count: i64) -> bool
pub fn find(&self, key: &SketchInput) -> Option<usize>
pub fn find_heap_item(&self, key: &HeapItem) -> Option<usize>
pub fn heap(&self) -> &[HHItem]
pub fn len(&self) -> usize
pub fn capacity(&self) -> usize
pub fn is_empty(&self) -> bool
pub fn clear(&mut self)
pub fn print_heap(&self)
pub fn from_heap(other: &HHHeap) -> Self
```

**Usage:**

```rust
let mut topk = HHHeap::new(10);
topk.update(&SketchInput::String("user_123".into()), 100);
topk.update(&SketchInput::String("user_456".into()), 250);

if let Some(idx) = topk.find(&SketchInput::String("user_456".into())) {
    println!("Found at index {}", idx);
}

for item in topk.heap() {
    println!("{:?}: {}", item.key, item.count);
}
```

**Implementation notes:**

- Uses min-heap to track top-K (smallest at root for easy eviction)
- Maintains hash-based position map for O(1) average-case lookups
- Automatically evicts smallest when at capacity
- Hash collisions handled via chaining in position map

---

## Utility Functions

**File:** [src/common/structure_utils.rs](../src/common/structure_utils.rs)

```rust
// Compute median from inline slice
pub fn compute_median_inline_f64(values: &mut [f64]) -> f64

// Nitro sampling support
pub struct Nitro {
    pub delta: u64,
    pub to_skip: usize,
    // ... internal fields
}
```

---

## Design Principles

1. **Zero-cost abstractions** - Generic types compile to specialized code with no overhead
2. **Cache-friendly layouts** - Row-major storage for sequential access
3. **Pre-computed values** - Mask bits cached to avoid redundant computation
4. **Fast-path methods** - Hash reuse across multiple operations
5. **Type safety** - Owned vs borrowed types enforced at compile time
