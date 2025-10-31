# Common Module API Reference

The `src/common` module provides the foundational building blocks for all sketch implementations in sketchlib-rust. It defines a common API of shared data structures, input types, and hashing utilities to ensure efficient and consistent operations. This modular foundation is designed to be extensible, simplifying the process of building new sketches.

**Key Components:**

- **Data Structures** ([structures.rs](#data-structures-structuresrs)) - High-performance containers optimized for sketch workloads
- **Input Types** ([input.rs](#input-types-inputrs)) - Unified type system for sketch ingestion
- **Hashing Utilities** ([hash.rs](#hashing-helpers)) - Deterministic hash functions and seed management

---

## Table of Contents

1. [Module Layout](#module-layout)
2. [Data Structures (structures.rs)](#data-structures-structuresrs)
   - [Vector1D](#vector1d)
   - [Vector2D](#vector2d)
   - [Vector3D](#vector3d)
   - [CommonHeap](#commonheap)
3. [Heavy Hitter Heap](#heavy-hitter-heap-heaprs)
4. [Input Types](#input-types-inputrs)
5. [Hashing Helpers](#hashing-helpers)

---

## Module Layout

**File:** [src/common/mod.rs](../src/common/mod.rs)

The common module re-exports all public APIs for centralized access:

```rust
// Preferred import style
use sketchlib_rust::common::{SketchInput, Vector2D, Vector1D, CommonHeap};

// Avoid directly importing from submodules
// use sketchlib_rust::common::structures::Vector2D;  // Don't do this
```

**Module Structure:**

- `mod.rs` - Re-exports `SketchInput`, `hash_it`, `LASTSTATE`, `SEEDLIST`, and all data structures
- `input.rs` - Input types, hashing functions, framework enums (HydraCounter, L2HH)
- `structures.rs` - Core data structures (Vector1D, Vector2D, Vector3D, CommonHeap)
- `heap.rs` - Convenience wrapper (HHHeap) for heavy hitter tracking

---

## Data Structures (structures.rs)

**File:** [src/common/structures.rs](../src/common/structures.rs)

The structures module provides high-performance, sketch-optimized data structures that form the backbone of all recommended sketch implementations. These structures are designed for:

- **Memory efficiency** - Flat layouts minimize allocation overhead
- **Cache locality** - Row-major storage maximizes sequential access patterns
- **Fast paths** - Specialized methods leverage pre-computed hash values
- **Zero-copy operations** - Direct slice access avoids unnecessary copies

### Vector1D

**Purpose:** Lightweight 1D vector wrapper for sketch counters and registers.

**Type Signature:**

```rust
pub struct Vector1D<T> {
    data: Vec<T>,
    length: usize,
}
```

**Design:** Thin wrapper around `Vec<T>` that maintains logical length separately from capacity. This allows for pre-allocation strategies that avoid repeated reallocations during sketch construction.

#### Vector1D Constructors

```rust
// Reserve capacity without initialization
fn init(len: usize) -> Self

// Create with filled values
fn filled(len: usize, value: T) -> Self
where T: Clone

// Build from existing vector
fn from_vec(vec: Vec<T>) -> Self
```

**Usage Example:**

```rust
use sketchlib_rust::common::Vector1D;

// Pre-allocate for HyperLogLog registers
let mut registers = Vector1D::<u8>::filled(16384, 0);

// Direct indexing
registers[100] = 5;

// Efficient updates
registers.update_if_greater(200, 7);
```

#### Vector1D Core Methods

```rust
// Size and access
fn len(&self) -> usize
fn is_empty(&self) -> bool
fn as_slice(&self) -> &[T]
fn as_mut_slice(&mut self) -> &mut [T]
fn get(&self, index: usize) -> Option<&T>
fn get_mut(&mut self, index: usize) -> Option<&mut T>

// Modification
fn fill(&mut self, value: T) where T: Clone
fn update_if_greater(&mut self, pos: usize, val: T) where T: Copy + Ord
fn update_if_smaller(&mut self, pos: usize, val: T) where T: Copy + Ord
fn update_one_counter<F, V>(&mut self, pos: usize, op: F, value: V)
where
    F: Fn(&mut T, V),
    T: Clone

// Iteration
fn iter(&self) -> impl Iterator<Item = &T>
fn iter_mut(&mut self) -> impl Iterator<Item = &mut T>
fn into_vec(self) -> Vec<T>
```

#### Vector1D Indexing

Implements `Index` and `IndexMut` for natural array syntax:

```rust
let val = vector[10];      // Read
vector[10] = 42;            // Write
```

**Bounds checking:** Uses `debug_assert!` for performance. Invalid indices will panic in debug builds.

#### Vector1D Traits

- `Clone`, `Debug`, `PartialEq`, `Eq`
- `Serialize`, `Deserialize` (via serde)

---

### Vector2D

**Purpose:** High-performance 2D matrix for sketch counter arrays (Count-Min, Count Sketch, etc.).

**Type Signature:**

```rust
pub struct Vector2D<T> {
    data: Vec<T>,
    rows: usize,
    cols: usize,
}
```

**Design:** Row-major flat storage (`data[row * cols + col]`) provides cache-friendly sequential access. Specialized fast-path methods leverage bit-masking and hash reuse for optimal sketch operations.

#### Vector2D Constructors

```rust
// Reserve capacity without initialization
fn init(rows: usize, cols: usize) -> Self

// Build with generator function
fn from_fn<F>(rows: usize, cols: usize, f: F) -> Self
where
    F: FnMut(usize, usize) -> T
```

**Usage Example:**

```rust
use sketchlib_rust::common::Vector2D;

// CountMin sketch: 3 rows × 4096 columns
let mut cm = Vector2D::<u64>::from_fn(3, 4096, |_r, _c| 0);

// Direct 2D indexing (returns row slice)
cm[0][100] += 1;

// Efficient cell access
let count = cm.query_one_counter(1, 50);
```

#### Vector2D Core Methods

```rust
// Dimensions
fn rows(&self) -> usize
fn cols(&self) -> usize
fn len(&self) -> usize                     // Total elements (rows × cols)

// Access
fn as_slice(&self) -> &[T]                 // Flat storage
fn as_mut_slice(&mut self) -> &mut [T]
fn get(&self, row: usize, col: usize) -> Option<&T>
fn get_mut(&mut self, row: usize, col: usize) -> Option<&mut T>

// Row operations
fn row_slice(&self, row: usize) -> &[T]
fn row_slice_mut(&mut self, row: usize) -> &mut [T]

// Modification
fn fill(&mut self, value: T) where T: Clone
fn update_one_counter<F, V>(&mut self, row: usize, col: usize, op: F, value: V)
where
    F: Fn(&mut T, V),
    T: Clone
```

#### Fast-Path Methods (Hash-Optimized)

These methods are the **core innovation** of the common structure. They use pre-computed hash values with bit-masking to efficiently select columns across multiple rows **without re-hashing**.

##### Hash Configuration

```rust
// Compute mask bits for column selection
fn get_mask_bits(&self) -> u32

// Determine required hash width (32, 64, or 128 bits)
fn get_required_bits(&self) -> usize
```

**How it works:**

For a sketch with `cols` columns, we need `log2(cols)` bits per row to select a column. For 3 rows with 4096 columns (12 bits each), we need 36 total bits, so `u64` or `u128` hash values are used.

##### Fast Insert

```rust
fn fast_insert<F, V>(&mut self, op: F, value: V, hashed_val: u128)
where
    F: Fn(&mut T, V),
    V: Clone
```

**Purpose:** Insert a value into all rows using a single pre-computed hash.

**Algorithm:**

1. Extract `mask_bits` from hash for each row using bit shifting
2. Apply modulo to get column index: `col = (hash_chunk % cols)`
3. Apply operation `op` to `data[row * cols + col]`

**Example:**

```rust
let hash = hash_it(0, &SketchInput::U64(12345));  // Pre-compute once
cm.fast_insert(|counter, val| *counter += val, 1, hash);
```

##### Fast Query (Min)

```rust
fn fast_query_min(&self, hashed_val: u128) -> T
where
    T: Clone + Ord
```

**Purpose:** Query all rows and return the minimum counter value (for Count-Min sketch).

**Algorithm:**

1. Extract column index from hash for each row
2. Read `data[row * cols + col]`
3. Return minimum across all rows

**Example:**

```rust
let hash = hash_it(0, &SketchInput::U64(12345));
let frequency = cm.fast_query_min(hash);
```

##### Fast Query (Median)

```rust
fn fast_query_median(&self, hashed_val: u128) -> f64
where
    T: Clone + Ord + Copy + ToF64

fn fast_query_median_with_key<F, Q>(&self, hashed_val: u128, op: F, q: &Q) -> f64
where
    F: Fn(&T, &Q) -> f64
```

**Purpose:** Query all rows and return the median estimate (for Count Sketch).

**Algorithm:**

1. Extract column index from hash for each row
2. Convert counters to `f64` using `ToF64` trait or query the counter to get `f64` with user-provided function and query-key
3. Sort and compute median (odd length: middle value; even length: average of two middle values)

**Variant with key:** Allows custom query operations (e.g., Hydra partial key matching).

##### Fast Query (Max)

```rust
fn fast_query_max(&self, hashed_val: u128) -> T
where
    T: Clone + Ord
```

**Purpose:** Query all rows and return the maximum counter value.

#### Direct Cell Access

```rust
// Faster than 2D indexing for single reads
fn query_one_counter(&self, row: usize, col: usize) -> T
where
    T: Clone
```

**Performance note:** Benchmarks show this is slightly faster than `vector[row][col]` due to avoiding slice creation.

#### Vector2D Indexing

Implements `Index` and `IndexMut` to return **row slices**:

```rust
let row: &[u64] = &matrix[2];     // Get entire row 2
matrix[1][50] = 10;                // Update cell (1, 50)
```

**Note:** This returns a slice, not a single element. For single cells, use `get()` or `query_one_counter()`.

#### Legacy Methods

```rust
fn get_row(&self) -> usize  // Alias for rows()
fn get_col(&self) -> usize  // Alias for cols()
```

#### Vector2D Traits

- `Clone`, `Debug`, `PartialEq`, `Eq`
- `Serialize`, `Deserialize` (via serde)

---

### Vector3D

**Purpose:** 3D tensor for multi-layer sketches (e.g., UnivMon).

**Type Signature:**

```rust
pub struct Vector3D<T> {
    data: Vec<T>,
    layer: usize,
    row: usize,
    col: usize,
}
```

**Status:** ⚠️ **Minimal implementation** - Currently only provides `init()` constructor.

**Design:** Flat storage with layout `data[layer * (row * col) + row * col + col]`.

#### Vector3D Constructor

```rust
fn init(layer: usize, row: usize, col: usize) -> Self
```

**TODO:** This structure needs parity with Vector2D:

- Slice access methods
- `fill()` operation
- Fast insert/query helpers
- Indexing traits

**Recommendation:** For now, prefer composing multiple `Vector2D` instances instead of using `Vector3D` until the API is fully developed.

---

### CommonHeap

**Purpose:** Generic bounded heap for tracking top-K or bottom-K heavy hitters.

**Type Signature:**

```rust
pub struct CommonHeap<T, O: CommonHeapOrder<T>> {
    data: Vec<T>,
    size: usize,      // Maximum capacity
    order: O,         // Min or Max heap ordering
}
```

**Design:** Zero-cost abstraction over standard binary heap with:

- **Bounded capacity** - Automatically evicts when full
- **Pluggable ordering** - Min-heap or max-heap via zero-sized types
- **Zero overhead** - Order type is zero-sized (`size_of::<CommonHeap<T, MinHeap>>() == size_of::<Vec<T>>() + 8`)

#### Heap Ordering

Heaps use a trait-based ordering system:

```rust
pub trait CommonHeapOrder<T> {
    fn should_swap(&self, parent: &T, child: &T) -> bool;
    fn should_replace_root(&self, root: &T, new_value: &T) -> bool;
}
```

**Built-in orderings:**

- `CommonMinHeap` - Smallest values have highest priority (root is minimum)
- `CommonMaxHeap` - Largest values have highest priority (root is maximum)

#### CommonHeap Constructors

```rust
// Generic constructor
fn with_capacity(capacity: usize, order: O) -> Self

// Convenience constructors
fn new_min(capacity: usize) -> CommonHeap<T, CommonMinHeap>
fn new_max(capacity: usize) -> CommonHeap<T, CommonMaxHeap>
```

**Usage Example:**

```rust
use sketchlib_rust::common::{CommonHeap, HHItem};

// Track top-3 heavy hitters (use min-heap to evict smallest)
let mut top3 = CommonHeap::new_min(3);

top3.push(HHItem::new("key1".into(), 10));
top3.push(HHItem::new("key2".into(), 20));
top3.push(HHItem::new("key3".into(), 5));
top3.push(HHItem::new("key4".into(), 15));  // Evicts key3 (count=5)

assert_eq!(top3.len(), 3);
assert_eq!(top3.peek().unwrap().count, 10);  // Min is now 10
```

#### CommonHeap Core Methods

```rust
// Size queries
fn len(&self) -> usize
fn is_empty(&self) -> bool
fn capacity(&self) -> usize
fn is_full(&self) -> bool

// Access
fn peek(&self) -> Option<&T>
fn peek_mut(&mut self) -> Option<&mut T>

// Modification
fn push(&mut self, value: T)              // Auto-evicts when full
fn pop(&mut self) -> Option<T>            // Remove root
fn clear(&mut self)

// Advanced
fn update_at(&mut self, index: usize) -> bool  // Re-heapify after manual update

// Iteration (not sorted)
fn iter(&self) -> impl Iterator<Item = &T>
fn iter_mut(&mut self) -> impl Iterator<Item = &mut T>
fn as_slice(&self) -> &[T]
```

#### Bounded Heap Behavior

**Key feature:** When heap is at capacity, `push()` automatically evicts if needed:

- **Min-heap:** Evicts smallest when new value is larger than root
- **Max-heap:** Evicts largest when new value is smaller than root

This is critical for heavy hitter tracking where you want to keep the top-K largest items.

**Example (Top-K tracking):**

```rust
let mut top_k = CommonHeap::new_min(5);  // Keep top 5

for i in 1..=10 {
    top_k.push(HHItem::new(format!("key{}", i), i));
}

// Heap contains: 6, 7, 8, 9, 10 (smallest values 1-5 were evicted)
assert_eq!(top_k.len(), 5);
assert_eq!(top_k.peek().unwrap().count, 6);  // Root is minimum
```

#### Custom Orderings

You can define custom heap orderings for specialized use cases:

```rust
#[derive(Clone)]
struct CompareByCount;

impl CommonHeapOrder<HHItem> for CompareByCount {
    fn should_swap(&self, parent: &HHItem, child: &HHItem) -> bool {
        child.count < parent.count  // Min-heap by count
    }

    fn should_replace_root(&self, root: &HHItem, new_value: &HHItem) -> bool {
        new_value.count > root.count
    }
}

let mut heap = CommonHeap::with_capacity(10, CompareByCount);
```

#### CommonHeap Indexing

Direct indexing for manual updates (requires calling `update_at()` afterward):

```rust
heap[0].count += 5;      // Modify root
heap.update_at(0);        // Re-heapify
```

#### CommonHeap Traits

- `Clone`, `Debug`, `PartialEq`, `Eq`
- `Serialize`, `Deserialize` (via serde)

#### CommonHeap Performance

**Memory overhead:** Zero-sized ordering types mean:

```rust
size_of::<CommonHeap<T, CommonMinHeap>>() == size_of::<Vec<T>>() + 8
```

**Time complexity:**

- `push()` / `pop()`: O(log n)
- `peek()`: O(1)
- `update_at()`: O(log n)

---

## Heavy Hitter Heap (heap.rs)

**File:** [src/common/heap.rs](../src/common/heap.rs)

### HHHeap

**Purpose:** Ready-to-use Top-K heavy hitter tracking for sketch implementations.

For user convenience, we provide **HHHeap** - a specialized wrapper around `CommonHeap<HHItem, CommonMinHeap>` that simplifies heavy hitter tracking. This eliminates the need to understand the generic heap API for the common case of tracking top-K items.

**Type Signature:**

```rust
pub struct HHHeap {
    heap: CommonHeap<HHItem, CommonMinHeap>,
    k: usize,
}
```

**Key Features:**

- **Simplified API** - No need to specify generic types or ordering
- **Built-in key lookup** - Find items by key string
- **Memory tracking** - Calculate total memory usage
- **Legacy compatibility** - Drop-in replacement for older `TopKHeap`

**Constructor:**

```rust
fn new(k: usize) -> Self  // Create top-K heap
```

**Common Methods:**

```rust
fn update(&mut self, key: &str, count: i64) -> bool  // Insert or update item
fn find(&self, key: &str) -> Option<usize>            // Find item by key
fn heap(&self) -> &[HHItem]                           // Access all items
fn len(&self) -> usize                                // Number of items
fn capacity(&self) -> usize                           // Maximum capacity (k)
fn get_memory_bytes(&self) -> f64                     // Memory usage
fn clear(&mut self)                                   // Remove all items
fn print_heap(&self)                                  // Debug printing
```

**Usage Example:**

```rust
use sketchlib_rust::common::HHHeap;

// Track top-10 heavy hitters
let mut topk = HHHeap::new(10);

// Insert items (automatically maintains top-K)
topk.update("user_123", 100);
topk.update("user_456", 250);
topk.update("user_789", 50);

// Find by key
if let Some(idx) = topk.find("user_456") {
    println!("Found at index {}", idx);
}

// Access all top-K items
for item in topk.heap() {
    println!("{}: {}", item.key, item.count);
}

// Memory usage
println!("Memory: {} bytes", topk.get_memory_bytes());
```

**When to Use:**

- ✅ **Use HHHeap** when you need simple top-K tracking with string keys
- ✅ **Use HHHeap** for sketch implementations that track heavy hitters (Locher, Elastic, etc.)
- ⚙️ **Use CommonHeap** when you need custom types, custom ordering, or more control

**Recommendation:** Start with `HHHeap` for heavy hitter tracking. Only use the generic `CommonHeap` if you need specialized behavior beyond `HHItem` tracking.

---

## Input Types (input.rs)

**File:** [src/common/input.rs](../src/common/input.rs)

### SketchInput

**Purpose:** Unified type-agnostic input for all sketches.

**Type Signature:**

```rust
pub enum SketchInput<'a> {
    // Signed integers
    I8(i8), I16(i16), I32(i32), I64(i64), I128(i128), ISIZE(isize),

    // Unsigned integers
    U8(u8), U16(u16), U32(u32), U64(u64), U128(u128), USIZE(usize),

    // Floating point
    F32(f32), F64(f64),

    // Strings and bytes
    Str(&'a str),      // Borrowed with lifetime
    String(String),     // Owned
    Bytes(&'a [u8]),   // Borrowed byte slice
}
```

**Design:** Borrowed variants (`Str`, `Bytes`) carry a lifetime for zero-copy operations. Use owned variants (`String`) when input buffer is short-lived.

**Usage Example:**

```rust
use sketchlib_rust::common::SketchInput;

// Insert various types
sketch.insert(&SketchInput::U64(12345));
sketch.insert(&SketchInput::String("user_id".into()));
sketch.insert(&SketchInput::F64(3.14159));

// Zero-copy for &str
let s = "temporary";
sketch.insert(&SketchInput::Str(s));  // No allocation
```

**Pattern matching:**

```rust
match input {
    SketchInput::U64(val) => { /* ... */ }
    SketchInput::String(ref s) => { /* ... */ }
    _ => { /* ... */ }
}
```

**TODO:**

- Generic type support: Allow custom types `T` to implement sketch input
- Challenges: Trait requirements, lifetime management

---

### HHItem

**Purpose:** Key-count pair for heavy hitter tracking in heap-based sketches.

**Type Signature:**

```rust
pub struct HHItem {
    pub key: String,
    pub count: i64,
}
```

**Design:** Implements `Ord` by count for natural heap ordering.

**Methods:**

```rust
fn new(key: String, count: i64) -> Self
fn print_item(&self)
```

**Ordering:** Compares by `count` field, enabling direct use in `CommonHeap`:

```rust
impl Ord for HHItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.count.cmp(&other.count)
    }
}
```

**Usage with CommonHeap:**

```rust
let mut heap = CommonHeap::<HHItem, CommonMinHeap>::new_min(100);
heap.push(HHItem::new("key".into(), 42));
```

---

### Framework Enums

#### HydraCounter

**Purpose:** Enum wrapper for sketches used in Hydra framework.

```rust
pub enum HydraCounter {
    CM(CountMin),
    HLL(HllDf),
}
```

**Methods:**

```rust
fn insert(&mut self, value: &SketchInput)
fn query(&self, query: &HydraQuery) -> Result<f64, String>
fn merge(&mut self, other: &HydraCounter) -> Result<(), String>
```

#### HydraQuery

**Purpose:** Query types for Hydra hierarchical queries.

```rust
pub enum HydraQuery<'a> {
    Frequency(SketchInput<'a>),   // For CountMin
    Quantile(f64),                 // For KLL
    Cardinality,                   // For HyperLogLog
}
```

#### L2HH

**Purpose:** Enum wrapper for L2-Heavy-Hitter sketches used in UnivMon.

```rust
pub enum L2HH {
    COUNT(CountL2HH),
}
```

**Methods:**

```rust
fn update_and_est(&mut self, key: &SketchInput, value: i64) -> f64
fn update_and_est_without_l2(&mut self, key: &SketchInput, value: i64) -> f64
fn get_l2(&self) -> f64
fn merge(&mut self, other: &L2HH)
```

---

## Hashing Helpers

**File:** [src/common/input.rs](../src/common/input.rs)

### Constants

```rust
pub const LASTSTATE: usize = 5;
pub const BOTTOM_LAYER_FINDER: usize = 19;
pub const HYDRA_SEED: usize = 6;
const MASK_32BITS: u64 = (1 << 32) - 1;
pub const SEEDLIST: [u64; 20]
```

**Design:** Predefined seeds enable deterministic hashing and sketch merging.

- `LASTSTATE`: can be used for calculating the sign bit
- `BOTTOM_LAYER_FINDER`: marks the last valid index into `SEEDLIST`, used to find bottom layer in `UnivMon`
- `HYDRA_SEED`: used for `Hydra` to find corresponding counter
- `SEEDLIST`: A pre-defined list of seeds to ensure that sketches created independently can be successfully merged

**TODO:** May need to expand for sketches requiring more hash functions.

### Hash Functions

```rust
pub fn hash_it_to_128(d: usize, key: &SketchInput) -> u128
```

**Purpose:** Hash `key` with the `d`-th seed from `SEEDLIST`.

**Behavior:** Panics if `d > LASTSTATE`. Callers must validate indices or wrap with bounds checking.

**Usage:**

```rust
use sketchlib_rust::common::{hash_it, SketchInput};

let key = SketchInput::U64(12345);
let hash = hash_it_to_128(0, &key);  // Use seed 0
```

**Design note:** Easy to swap hash function implementations for testing different algorithms.

---

```rust
pub fn hash_for_all_rows(r: usize, key: &SketchInput) -> Vec<u64>
```

**Purpose:** Generate `r` column indices from a single hash using 13-bit windows.

**Status:** ⚠️ **Deprecated** - Returns `Vec<u64>` which is slow due to heap allocation.

**Recommendation:** Use `Vector2D::fast_insert()` and `Vector2D::fast_query_*()` instead, which use inline bit-masking without allocation.

**TODO:** Remove or redesign if use cases still exist.
