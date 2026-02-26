# FoldSketch: Design and Algorithm

A memory-efficient technique for reducing sub-window sketch storage in long-range time-windowed query.

**Implementation:**
- FoldCMS: [`src/sketches/fold_cms.rs`](../src/sketches/fold_cms.rs)
- FoldCS: [`src/sketches/fold_cs.rs`](../src/sketches/fold_cs.rs)

---

## Table of Contents

1. [Motivation](#motivation)
2. [Naming Convention](#naming-convention)
3. [Core Insight](#core-insight)
4. [Data Structure Design](#data-structure-design)
5. [Operations](#operations)
6. [Memory Analysis](#memory-analysis)
7. [Error Bound Analysis](#error-bound-analysis)
8. [FoldCS: Folding Count Sketch](#foldcs-folding-count-sketch)
9. [Top-K Heavy Hitter Integration](#top-k-heavy-hitter-integration)
10. [Windowing Integration](#windowing-integration)
11. [API Reference](#api-reference)
12. [Examples](#examples)
13. [Summary](#summary)

---

## 1. Motivation

In time-windowed queries (e.g., tumbling windows), a query window of duration T is divided into n sub-windows of duration T/n. Each sub-window maintains a Count-Min Sketch (CMS) summarizing the items that arrive during that interval. At query time, sub-window sketches are merged element-wise to answer frequency or heavy-hitter queries over the full window.

The fundamental problem: **every sub-window must allocate a full R x W CMS**, where W is the width required for the final merged query's accuracy guarantee. This width is dictated by the full window's stream volume and error tolerance, not by the sub-window's cardinality.

### Example Scenarios

**Scenario 1: API Rate Limiting**

A rate limiter monitors per-user request counts over a 10-minute sliding window. The full window needs W = 4096 columns for epsilon = 0.00066 accuracy across millions of requests. But each 1-minute sub-window only sees ~200 active users.

```
Standard approach:
  10 sub-windows x (3 rows x 4096 cols x 8 bytes) = 980 KB

Actual distinct items per sub-window: ~200
  200 items across 4096 columns -> 95% of cells are zeros
```

**Scenario 2: Error Frequency Monitoring**

An observability system tracks per-endpoint error counts. The merged 10-minute window needs W = 4096, but each 1-minute sub-window sees errors from only ~50 endpoints.

```
Standard approach:  980 KB for 10 sub-windows
Active cells per sub-window: ~50 out of 4096 per row -> 98.8% waste
```

**Scenario 3: DDoS Detection**

A network monitor counts per-source-IP packets over a 5-minute window. Each 10-second sub-window needs W = 8192 for the merged query, but only ~500 IPs are active in any 10-second interval.

```
Standard approach:
  30 sub-windows x (5 rows x 8192 cols x 8 bytes) = 9.8 MB

Active cells per sub-window: ~500 out of 8192 per row -> 93.9% waste
```

**The common pattern**: Sub-windows allocate memory for the *final merged width*, but their actual cardinality D is far smaller than W. The ratio D/W determines how much memory is wasted — often 90-99%.

---

## 2. Naming Convention

| Name | Description |
|------|-------------|
| **FoldSketch** | The general technique of column-folding for any linear sketch |
| **FoldCMS** | Column-folded Count-Min Sketch |
| **FoldCS** | Column-folded Count Sketch |

Both FoldCMS and FoldCS are implemented. They share the same `FoldCell`/`FoldEntry` cell types and the same folding/unfolding mechanics. The key difference is how counters are aggregated: FoldCMS uses **minimum** across rows (one-sided error), while FoldCS uses **median** with signed counters (two-sided, unbiased error).

The "fold" metaphor: imagine the full W-column sketch printed on paper. Folding the paper in half k times produces 2^k layers, each W/2^k columns wide. Multiple full-width columns are stacked (folded) onto the same physical column, distinguished by tags.

---

## 3. Core Insight

### The Key Observation

In a CMS with R rows and W columns, inserting a key computes R hash values, each selecting one of W columns. The column index for row r is:

```
full_col_r(key) = hash_r(key) mod W
```

This `full_col` is the key's **permanent address** in the full-width CMS — it depends only on the hash function and W, not on the physical storage. As long as we record *which* full_col each counter belongs to, we can use fewer physical columns without losing information.

### The Folding Operation

Define a **fold level** k >= 0. A FoldCMS at level k uses:

```
fold_cols = W / 2^k = W >> k
```

physical columns. Each key's physical column is:

```
fold_col = full_col mod fold_cols
         = full_col & (fold_cols - 1)    // because fold_cols is a power of 2
```

Multiple `full_col` values map to the same `fold_col`. Each physical cell stores *tagged entries* `(full_col, count)` to distinguish them. At level k, up to 2^k distinct `full_col` values share each physical column.

### Visual Representation

```
Full CMS (level 0), W = 8:
  +---+---+---+---+---+---+---+---+
  | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |    8 cells, one counter each
  +---+---+---+---+---+---+---+---+

Folded (level 1), W/2 = 4 physical columns:
  +----------+----------+----------+----------+
  |  {0, 4}  |  {1, 5}  |  {2, 6}  |  {3, 7}  |  4 cells, each holds
  +----------+----------+----------+----------+  entries from 2 full_cols

Folded (level 2), W/4 = 2 physical columns:
  +----------------------+----------------------+
  |   {0, 2, 4, 6}       |   {1, 3, 5, 7}       |  2 cells, each holds
  +----------------------+----------------------+  entries from 4 full_cols
```

### Why Folding Preserves Correctness

The critical property: **folding is purely a storage optimization**. Every entry's `full_col` tag preserves its logical position in the full W-column CMS. When querying, we look up the specific `full_col` entry within the physical cell — entries with different `full_col` values in the same cell do not interfere. This means:

- No additional hash collisions are introduced
- No counter sharing occurs between different `full_col` addresses
- Query accuracy is identical to a standard W-column CMS

---

## 4. Data Structure Design

### Tagged Cell Architecture

The core data structure is a **tagged cell** that lazily expands based on actual collision pressure. These types are shared between FoldCMS and FoldCS.

```
FoldEntry
+-- full_col: u16     permanent column address in the full W-column sketch
+-- count: i64        accumulated counter value

FoldCell (enum)
+-- Empty                           no entry yet (zero cost)
+-- Single { full_col, count }      exactly one full_col present (inline, no heap alloc)
+-- Collided(Vec<FoldEntry>)        2+ distinct full_cols (heap-allocated Vec)
```

**Lazy Expansion Principle**: A cell allocates a `Vec` *only* when a genuine collision occurs — i.e., when a second *distinct* `full_col` arrives at the same physical cell. For sparse sub-windows where D << fold_cols, most cells see at most one `full_col` and remain in the `Single` state.

State transitions:

```
   Empty --insert(f1, d)--> Single(f1, d)
                                 |
                    insert(f1, d')  |  insert(f2, d2)  where f2 != f1
                         |          |
                         v          v
                  Single(f1, d+d')  Collided([(f1, d), (f2, d2)])
                                         |
                            insert(f1, d')  |  insert(f3, d3)
                                  |          |
                                  v          v
                        Collided(...)  Collided([..., (f3, d3)])
```

A cell **never** transitions backward (Collided -> Single -> Empty). This is safe because deletions are not supported; counters only accumulate.

### FoldCMS Structure

```
FoldCMS
+-- rows: usize           R independent hash functions
+-- fold_cols: usize      W / 2^k physical columns
+-- full_cols: usize      W target columns (invariant across all operations)
+-- fold_level: u32       k (folding depth)
+-- cells: Vec<FoldCell>  flat R x fold_cols grid (row-major)
+-- heap: HHHeap          top-K heavy hitter heap
```

### FoldCS Structure

```
FoldCS
+-- rows: usize           R independent hash functions
+-- fold_cols: usize      W / 2^k physical columns
+-- full_cols: usize      W target columns (invariant across all operations)
+-- fold_level: u32       k (folding depth)
+-- cells: Vec<FoldCell>  flat R x fold_cols grid (row-major) [shared FoldCell type]
+-- heap: HHHeap          top-K heavy hitter heap
```

FoldCS has an identical struct layout to FoldCMS. The difference is purely behavioral: how `insert` and `query` use the hash values (see [Section 8](#foldcs-folding-count-sketch)).

The cell grid is stored in row-major order: `cells[r * fold_cols + c]` gives the cell at row r, physical column c. This layout provides good cache locality for per-row operations during insert and query.

### Invariants

1. **full_cols is constant**: Never changes across fold/unfold/merge operations.
2. **fold_cols = full_cols >> fold_level**: Always a power of two.
3. **fold_level <= log2(full_cols)**: Cannot fold more times than there are column bits.
4. **Every entry's full_col is valid**: `full_col < full_cols` for all entries.
5. **Consistent full_col -> fold_col mapping**: `full_col & (fold_cols - 1) == fold_col` for every entry in the cell at position fold_col.

---

## 5. Operations

The operations below are described for FoldCMS. FoldCS uses the same merge/unfold logic with different insert and query semantics (see [Section 8](#foldcs-folding-count-sketch)).

### 5.1 Insert

```
INSERT(sketch, key, delta):
    for each row r in 0..R:
        full_col  <- hash_r(key) mod W           // permanent address
        fold_col  <- full_col & (fold_cols - 1)   // physical column

        cell <- sketch.cells[r * fold_cols + fold_col]

        match cell:
            Empty:
                cell <- Single(full_col, delta)

            Single(existing_col, count):
                if existing_col == full_col:
                    count += delta               // same entry, accumulate
                else:
                    cell <- Collided([            // real collision: promote
                        (existing_col, count),
                        (full_col, delta)
                    ])

            Collided(entries):
                if entries contains full_col:
                    entries[full_col].count += delta
                else:
                    entries.append((full_col, delta))

    // Update heavy hitter heap
    est <- QUERY(sketch, key)
    sketch.heap.update(key, est)
```

**Time complexity**: O(R x E) per insert, where E is the average entries per cell.
For sparse sub-windows (D << fold_cols), E ~ 1 and insert is O(R).

### 5.2 Point Query

```
QUERY(sketch, key) -> i64:
    min_count <- +inf
    for each row r in 0..R:
        full_col  <- hash_r(key) mod W
        fold_col  <- full_col & (fold_cols - 1)

        cell <- sketch.cells[r * fold_cols + fold_col]
        row_count <- cell.lookup(full_col)    // returns 0 if absent

        min_count <- min(min_count, row_count)

    return min_count
```

**Time complexity**: O(R x E). Returns exactly the same value as a standard W-column CMS.

### 5.3 Same-Level Merge

Combines two FoldCMS sketches at the same fold level without changing the physical column count. Used to aggregate sub-windows that share the same granularity.

```
MERGE_SAME_LEVEL(self, other):
    assert self.fold_level == other.fold_level
    assert self.full_cols == other.full_cols

    for each cell index i in 0..(R x fold_cols):
        for each (full_col, count) in other.cells[i]:
            self.cells[i].insert(full_col, count)

    reconcile_heaps(self, other)
```

The cell-level `insert` respects lazy expansion: if both cells contain entries for the same `full_col`, they accumulate without allocating. Only genuinely new `full_col` values trigger `Collided` promotion.

**Time complexity**: O(R x fold_cols x E)

### 5.4 Unfold Merge

The central merge operation: combines two **same-level** sketches at level k into a new sketch at level k-1, doubling the physical column count. Entries are scattered to their correct positions in the wider grid.

```
UNFOLD_MERGE(a, b) -> FoldCMS:        // both at level k, result at level k-1
    assert a.fold_level == b.fold_level > 0
    new_level <- a.fold_level - 1
    new_fold_cols <- 2 x a.fold_cols   // = W / 2^(k-1)

    result <- new FoldCMS(R, W, new_level)

    for each source in [a, b]:
        for each row r in 0..R:
            for each fold_col c in 0..source.fold_cols:
                for each (full_col, count) in source.cells[r][c]:
                    new_fc <- full_col & (new_fold_cols - 1)
                    result.cells[r x new_fold_cols + new_fc].insert(full_col, count)

    reconcile_heaps(result, a, b)
    return result
```

**Geometric interpretation**: At the old level k, entries with `full_col = f` are in physical column `f mod old_fold_cols`. At level k-1, they move to `f mod (2 x old_fold_cols)`. Since `new_fold_cols = 2 x old_fold_cols`:

```
new_fc = f mod (2 x old_fold_cols)

This is either:
  old_fc                          if bit (log2(old_fold_cols)) of f is 0
  old_fc + old_fold_cols          if bit (log2(old_fold_cols)) of f is 1
```

Each old cell's entries split into at most 2 destination cells. Entries that shared a physical column due to folding may now land in different cells, reducing collision density.

**Time complexity**: O(R x fold_cols x E) for each source sketch.

### 5.5 Unfold to Target Level

Repeatedly unfold-merges with an empty sketch to reduce fold level step by step:

```
UNFOLD_TO(sketch, target_level) -> FoldCMS:
    assert target_level <= sketch.fold_level
    current <- clone(sketch)
    while current.fold_level > target_level:
        empty <- new FoldCMS(R, W, current.fold_level)
        current <- UNFOLD_MERGE(current, empty)
    return current
```

### 5.6 Hierarchical Merge

Merges an arbitrary-length sequence of sketches via pairwise unfold-merging:

```
HIERARCHICAL_MERGE(sketches[0..n]) -> FoldCMS:
    current <- sketches
    while |current| > 1:
        next <- []
        for each adjacent pair (left, right):
            if left.level == right.level and level > 0:
                next.append(UNFOLD_MERGE(left, right))
            elif left.level == right.level and level == 0:
                next.append(SAME_LEVEL_MERGE(left, right))
            else:
                // mixed levels: unfold higher-level to match lower
                target <- min(left.level, right.level)
                a <- UNFOLD_TO(left, target)
                b <- UNFOLD_TO(right, target)
                next.append(merge(a, b))    // unfold or same-level

        if |current| is odd:
            next.append(current.last)       // carry forward

        current <- next
    return current[0]
```

For n sketches at fold level k:
- **Round 1**: n/2 pairs -> n/2 sketches at level k-1
- **Round 2**: n/4 pairs -> n/4 sketches at level k-2
- ...
- **Round k**: 1 sketch at level 0

Non-power-of-two counts are handled by carrying the odd sketch forward, then merging it in the next round after unfolding to match the partner's level.

### 5.7 Conversion to Flat Counters

Extract a standard R x W counter array from any fold level:

```
TO_FLAT_COUNTERS(sketch) -> i64[R][W]:
    out <- zeros(R, W)
    for each row r:
        for each fold_col c:
            for each (full_col, count) in sketch.cells[r][c]:
                out[r][full_col] += count
    return out
```

This produces exactly the same array as a standard CMS that processed the same stream, regardless of fold level.

---

## 6. Memory Analysis

### 6.1 Standard CMS Memory

```
Memory_CMS = R x W x sizeof(counter)
           = R x W x 8 bytes    (for i64 counters)
```

### 6.2 FoldCMS/FoldCS Memory Model

FoldCMS and FoldCS share the same memory model since they use identical cell types. Memory has two components:

**Cell grid**: Fixed overhead for the physical column grid.
```
Cell grid = R x fold_cols x sizeof(FoldCell)
```

The `FoldCell` enum uses Rust's tagged union representation:
- `Empty`: discriminant only (part of grid allocation)
- `Single { full_col: u16, count: i64 }`: discriminant + inline data, no heap allocation
- `Collided(Vec<FoldEntry>)`: discriminant + Vec header (pointer + length + capacity = 24 bytes) + heap-allocated entries

**Active entries**: Per-entry cost for non-empty cells.
```
Active entries <= R x D x sizeof(FoldEntry)
               = R x D x 10 bytes    (u16 + i64)
```

where D is the number of distinct keys in the sub-window.

**Total memory**:
```
Memory_Fold ~ R x (fold_cols x C_cell + D x C_entry + collisions x C_vec)
```

where:
- C_cell ~ 32 bytes (enum with inline Single variant)
- C_entry = 10 bytes (u16 + i64 FoldEntry)
- C_vec = 24 bytes (Vec header, only for Collided cells)
- collisions = number of physical cells with 2+ distinct full_cols

### 6.3 Collision Analysis

For D distinct keys, R rows, and fold_cols physical columns, the expected number of fold-collisions per row follows the birthday paradox:

```
E[collisions per row] ~ D - fold_cols x (1 - (1 - 1/fold_cols)^D)
                       ~ D^2/(2 x fold_cols)    for D << fold_cols
```

When fold_cols >= 2D, collisions are rare and most cells remain in the `Single` state.

### 6.4 Comparison Table

Parameters: R = 3, W = 4096, sizeof(counter) = 8 bytes.

| Fold Level k | fold_cols | D | FoldCMS/FoldCS Memory | Standard Sketch | Savings |
|:---:|:---:|:---:|:---:|:---:|:---:|
| 0 | 4096 | -- | >= 384 KB | 96 KB | worse (level 0 has per-cell tag overhead) |
| 2 | 1024 | 200 | ~104 KB | 96 KB | ~1x |
| 4 | 256 | 200 | ~30 KB | 96 KB | **3.2x** |
| 4 | 256 | 50 | ~26 KB | 96 KB | **3.7x** |
| 6 | 64 | 200 | ~12 KB | 96 KB | **8x** |
| 6 | 64 | 50 | ~7.5 KB | 96 KB | **12.8x** |
| 8 | 16 | 200 | ~7.5 KB | 96 KB | **12.8x** |
| 8 | 16 | 50 | ~3 KB | 96 KB | **32x** |

### 6.5 When to Use FoldSketch

**FoldCMS/FoldCS is beneficial when**: D << W and k >= 3.

**Guideline for choosing k**: Set fold_cols ~ 2D to 4D for a good balance between memory savings and collision rate.

```
D = 200, W = 4096  ->  fold_cols ~ 400-800  ->  k = 2 or 3
D = 50,  W = 4096  ->  fold_cols ~ 100-200  ->  k = 4 or 5
D = 500, W = 8192  ->  fold_cols ~ 1000-2000 ->  k = 2
```

**FoldCMS/FoldCS is not beneficial when**: D is close to W (dense sub-windows), or the fold level is 0 (the per-cell tag overhead exceeds a standard flat counter array).

---

## 7. Error Bound Analysis

### 7.1 Standard CMS Error Bounds

For a standard Count-Min Sketch with R rows and W columns processing a stream of total volume N = ||f||_1:

```
Pr[ estimate(key) - true_count(key) > eps x N ] < delta

where:
    eps = e / W          (per-row error probability via Markov)
    delta = e^(-R)       (amplification across R independent rows)
```

The estimate is always >= true_count (one-sided error), and the min across R rows gives the tightest bound.

### 7.2 FoldCMS Error Bound

**Theorem**: For any fold level k, FoldCMS produces exactly the same query results as a standard CMS with W = full_cols columns.

**Proof**:

**(a) Hash identity**: Both FoldCMS and standard CMS compute the same column index for each (row, key) pair:
```
full_col_r(key) = hash_r(key) mod W
```
This value depends only on the hash function and W, not on fold_cols or fold_level.

**(b) Counter isolation**: In a standard CMS, cell `[r][full_col]` accumulates counts from all keys whose row-r hash maps to `full_col`. In FoldCMS, the entry `(full_col, count)` in physical cell `fold_col = full_col mod fold_cols` tracks exactly the same set of keys and accumulates the same count. Entries with different `full_col` values in the same physical cell are stored separately and do not interfere.

**(c) Query equivalence**: A FoldCMS query looks up the specific `full_col` entry per row and returns the minimum. This is identical to looking up cell `[r][full_col]` in the full-width CMS.

**Corollary**: Folding introduces **zero additional approximation error**. The error bound is unchanged:

```
Pr[ estimate(key) - true_count(key) <= eps x ||f||_1 ] >= 1 - delta

where  eps = e / W       (depends on full_cols, not fold_cols)
       delta = e^(-R)
```

### 7.3 Merge Correctness

**Same-level merge**: For two FoldCMS sketches A and B at the same fold level, `merge_same_level(A, B)` produces a sketch whose `to_flat_counters()` output equals the element-wise sum of A's and B's flat counters. This is identical to merging two standard CMS sketches.

**Unfold merge**: For two FoldCMS sketches A and B at level k, `unfold_merge(A, B)` at level k-1 produces the same flat counters as `merge_same_level` would (if both were at level k-1). The entries are simply redistributed to wider physical columns.

**Hierarchical merge**: By induction on the merge tree, the final result at level 0 has the same flat counters as a standard CMS that processed the concatenation of all input streams.

### 7.4 Verified Empirically

The implementation includes tests that:
1. Compare FoldCMS query results against standard `CountMin<Vector2D<i64>>` for identical input streams -- **exact match** for every key
2. Compare flat counter arrays extracted via `to_flat_counters()` against standard CMS storage -- **byte-for-byte identical**
3. Validate error bounds under Zipf(1.1) workloads with 200K samples

---

## 8. FoldCS: Folding Count Sketch

**File:** [`src/sketches/fold_cs.rs`](../src/sketches/fold_cs.rs)

### 8.1 Count Sketch Background

Count Sketch (CS) is a linear sketch that supports both positive and negative frequency updates. Each row uses two hash functions: one for column selection and one for a +/-1 sign. The estimate for a key is the **median** across rows rather than the minimum.

```
CS insert:   cell[r][h_r(key)] += sign_r(key) x delta
CS query:    median over r of (cell[r][h_r(key)] x sign_r(key))
```

The Count Sketch error bound uses the L2 norm rather than the L1 norm:

```
Pr[ |estimate(key) - true_count(key)| > eps x ||f||_2 ] < delta

where  eps = 1/sqrt(W)
       delta = 2^(-Omega(R))
```

### 8.2 Why Folding Applies to Count Sketch

The FoldSketch technique applies identically to Count Sketch because:

1. **Column selection is the same**: `full_col_r(key) = h_r(key) mod W`. This is the value we tag in the cell.
2. **Sign is per-entry**: The sign `sign_r(key)` is applied to the count when inserting and querying. It does not affect which physical cell stores the entry.
3. **Counter isolation still holds**: Tagged entries with different `full_col` values never interact.

### 8.3 FoldCS Implementation

FoldCS **reuses** `FoldCell`, `FoldEntry`, and `FoldCellIter` from `fold_cms.rs`. The cell storage is identical — only the insert and query behavior differs.

#### Hashing

FoldCS uses a single `hash64_seeded` call per (row, key) to derive both the column and the sign:

```
hash_for(row, key) -> (full_col, sign):
    hashed = hash64_seeded(row, key)
    full_col = (hashed & LOWER_32_MASK) % W     // lower 32 bits -> column
    sign = if (hashed >> 63) & 1 == 1 { +1 } else { -1 }   // bit 63 -> sign
```

This matches the sign convention in `count.rs` (standard Count Sketch).

#### Insert

```
FOLDCS_INSERT(sketch, key, delta):
    for each row r:
        (full_col, sign) <- hash_for(r, key)
        fold_col <- full_col & (fold_cols - 1)

        cell <- sketch.cells[r * fold_cols + fold_col]
        cell.insert(full_col, sign x delta)    // sign applied to delta

    est <- FOLDCS_QUERY(sketch, key)
    sketch.heap.update(key, est)
```

The stored cell values are signed: a key inserted with `delta = 1` will produce `+1` in rows where `sign = +1` and `-1` in rows where `sign = -1`.

#### Query

```
FOLDCS_QUERY(sketch, key) -> i64:
    values <- []
    for each row r:
        (full_col, sign) <- hash_for(r, key)
        fold_col <- full_col & (fold_cols - 1)

        raw <- cell.lookup(full_col)       // 0 if absent
        values.append(sign x raw)          // undo sign to recover estimate

    return median(values)                  // median, NOT min
```

The median is computed using `compute_median_inline_f64` (optimized for small arrays of 3-5 elements).

#### Merge Operations

All merge operations (same-level, unfold, hierarchical, unfold_to, unfold_full) use **identical logic** to FoldCMS — they operate purely on the cell grid, scattering `(full_col, count)` entries to their correct positions. The sign is already baked into the stored count values, so no sign adjustment is needed during merge.

Heap reconciliation re-queries heap items using FoldCS's median-based query, ensuring correct estimates.

### 8.4 FoldCS Error Bound

Since folding preserves per-cell counter isolation, the Count Sketch error bound is unchanged:

```
Pr[ |estimate(key) - true_count(key)| > eps x ||f||_2 ] < delta

where  eps = 1/sqrt(W)        (depends on full_cols)
       delta = 2^(-Omega(R))
```

The sign flipping ensures unbiased estimation, and the median amplification across R rows provides the concentration bound. Folding does not affect either property.

### 8.5 Verified Empirically

The FoldCS implementation includes tests that:
1. Compare FoldCS query results against standard `Count<Vector2D<i64>, RegularPath>` for identical input streams -- **exact match** for every key
2. Compare flat counter arrays extracted via `to_flat_counters()` against standard CS storage -- **byte-for-byte identical**
3. Verify sign application: raw cell values contain both positive and negative entries
4. Validate error bounds under Zipf(1.1) workloads with 200K samples (CS bound: `|est - truth| < eps x ||f||_2`)
5. Large-window merge benchmark: 16 sub-windows, 500K Zipf samples, hierarchical merge with printed memory/error stats

### 8.6 FoldCMS vs FoldCS: When to Use Which

| Criterion | FoldCMS | FoldCS |
|-----------|---------|--------|
| **Error type** | One-sided (overestimates only) | Two-sided (unbiased) |
| **Aggregation** | min across rows | median across rows |
| **Error bound** | `eps x \|\|f\|\|_1` (L1 norm) | `eps x \|\|f\|\|_2` (L2 norm) |
| **Best for** | Frequency estimation, rate limiting | Heavy hitter detection, L2 queries |
| **Supports deletions** | No (counters only grow) | Yes (signed counters allow negative deltas) |
| **Memory** | Same as FoldCS | Same as FoldCMS |

---

## 9. Top-K Heavy Hitter Integration

### 9.1 Design

Each FoldCMS and FoldCS maintains an `HHHeap` -- a bounded min-heap of `(key, estimated_count)` items for heavy hitter tracking. The heap has a fixed capacity K.

**On insert**: After updating the tagged cells, the current query estimate for the key is computed and the heap is updated. If the key's count exceeds the heap's minimum, it enters (or updates in) the heap.

```
INSERT(sketch, key, delta):
    ... update cells ...
    est <- QUERY(sketch, key)
    sketch.heap.update(key, est)
```

**On merge**: After merging cells, all heap items from the other sketch are re-queried against the merged sketch and reconciled.

```
MERGE(self, other):
    ... merge cells ...
    for item in other.heap:
        est <- QUERY(self, item.key)
        self.heap.update(item.key, est)
```

### 9.2 Heap Reconciliation During Unfold Merge

During unfold merge, a fresh heap is created and populated by re-querying all heap items from both source sketches against the merged result:

```
UNFOLD_MERGE(a, b):
    result <- ... merge cells into wider grid ...
    for source in [a, b]:
        for item in source.heap:
            est <- QUERY(result, item.key)
            result.heap.update(item.key, est)
    return result
```

This ensures the heap reflects the combined counts from both sources, correctly handling cases where a key appears in one source's heap but not the other's.

### 9.3 Correctness

The heap's counts are always derived from the underlying sketch query (which is exact w.r.t. the standard sketch). Therefore, the top-K heavy hitters reported by a merged FoldCMS/FoldCS are identical to those a standard CMS/CS with the same heap would report.

---

## 10. Windowing Integration

### 10.1 Exponential Histogram (EH) Integration

FoldCMS and FoldCS are designed to work with window frameworks such as tumbling window, and Exponential Histogram framework. 

In Exponential Histogram framework:
- Each arriving sub-window creates a new FoldCMS/FoldCS at a high fold level k
- The EH maintains a sequence of "buckets" of geometrically increasing size
- When two buckets of equal size are merged, their sketches are **unfold-merged**, reducing the fold level by 1
- Older (larger) buckets have lower fold levels and more physical columns
- The oldest/largest bucket at level 0 is a standard sketch

```
Time ->  [newest]                                                  [oldest]
         Level k    Level k    Level k-1   Level k-2   ...   Level 0
         W/2^k cols W/2^k cols W/2^(k-1)   W/2^(k-2)         W cols
         ---------  ---------  ----------  ----------         --------
         tiny       tiny       small       medium             full
```

### 10.2 EH Merge Schedule Example

Consider W = 4096, k = 4 (fold_cols = 256), with sub-windows arriving over time:

```
Step 1: Sub-window 1 arrives -> level-4 sketch (256 cols)
Step 2: Sub-window 2 arrives -> level-4 sketch (256 cols)
  EH merge: unfold_merge(sub1, sub2) -> level-3 sketch (512 cols)

Step 3: Sub-window 3 arrives -> level-4 sketch (256 cols)
Step 4: Sub-window 4 arrives -> level-4 sketch (256 cols)
  EH merge: unfold_merge(sub3, sub4) -> level-3 sketch (512 cols)
  EH merge: unfold_merge(level3_a, level3_b) -> level-2 sketch (1024 cols)

...and so on, following the EH merge policy
```

### 10.3 Memory Savings in Windowed Setting

Consider a 10-minute window with 1-minute sub-windows, W = 4096, R = 3:

**Standard EH + CMS**:
```
Each bucket stores a full 3 x 4096 x 8 = 96 KB CMS
With ~2 x log2(10) ~ 7 buckets: 672 KB total sketch memory
```

**EH + FoldCMS** (k = 4, D ~ 200 per sub-window):
```
Newest buckets (level 4): 256 cols x ~30 KB each
Mid-age buckets (level 2-3): 512-1024 cols x 40-60 KB each
Oldest bucket (level 0): 4096 cols x 96 KB

Estimated total: ~300 KB (2.2x savings)
```

The savings are greater when:
- Sub-window cardinality D is much smaller than W
- More sub-windows are active (more small buckets benefit from folding)
- Higher initial fold level k is used

### 10.4 Query Over Merged Window

To answer a frequency query over the full sliding window:

1. Identify the EH buckets that cover the query window
2. Use `hierarchical_merge` to combine them (handles mixed fold levels)
3. Query the result (or query individual buckets and sum, depending on the sketch type)

Alternatively, for a simple sum query, each bucket can be queried independently:

```
WINDOW_QUERY(eh_buckets, key) -> i64:
    total <- 0
    for bucket in eh_buckets:
        total += bucket.fold_sketch.query(key)
    return total
```

This avoids materializing the merged sketch, but sacrifices the min-across-rows tightening (CMS) or median-based denoising (CS) that a single merged sketch provides.

---

## 11. API Reference

### 11.1 FoldCMS API

**File:** [`src/sketches/fold_cms.rs`](../src/sketches/fold_cms.rs)

#### Constructor

```rust
// Create a folded sketch
// fold_level 0 = standard CMS, k = folded by 2^k
fn new(rows: usize, full_cols: usize, fold_level: u32, top_k: usize) -> FoldCMS

// Shorthand for fold_level = 0
fn new_full(rows: usize, full_cols: usize, top_k: usize) -> FoldCMS
```

#### Insert & Query

```rust
fn insert(&mut self, key: &SketchInput, delta: i64)
fn insert_one(&mut self, key: &SketchInput)           // delta = 1
fn query(&self, key: &SketchInput) -> i64              // min across rows
```

#### Merge

```rust
// Combine two same-level sketches (no unfolding)
fn merge_same_level(&mut self, other: &FoldCMS)

// Combine two level-k sketches into one level-(k-1) sketch
fn unfold_merge(a: &FoldCMS, b: &FoldCMS) -> FoldCMS

// Unfold to a specific level
fn unfold_to(&self, target_level: u32) -> FoldCMS

// Unfold all the way to level 0
fn unfold_full(&self) -> FoldCMS

// Pairwise merge of a slice of sketches
fn hierarchical_merge(sketches: &[FoldCMS]) -> FoldCMS
```

#### Conversion & Inspection

```rust
fn to_flat_counters(&self) -> Vec<i64>     // R x W row-major counter array
fn rows(&self) -> usize
fn fold_cols(&self) -> usize
fn full_cols(&self) -> usize
fn fold_level(&self) -> u32
fn total_entries(&self) -> usize           // sum of entries across all cells
fn collided_cells(&self) -> usize          // cells with 2+ entries
fn heap(&self) -> &HHHeap
```

### 11.2 FoldCS API

**File:** [`src/sketches/fold_cs.rs`](../src/sketches/fold_cs.rs)

#### Constructor

```rust
// Create a folded Count Sketch
// fold_level 0 = standard CS, k = folded by 2^k
fn new(rows: usize, full_cols: usize, fold_level: u32, top_k: usize) -> FoldCS

// Shorthand for fold_level = 0
fn new_full(rows: usize, full_cols: usize, top_k: usize) -> FoldCS
```

#### Insert & Query

```rust
fn insert(&mut self, key: &SketchInput, delta: i64)    // stores sign * delta
fn insert_one(&mut self, key: &SketchInput)            // delta = 1
fn query(&self, key: &SketchInput) -> i64              // median across rows (sign-corrected)
```

#### Merge

```rust
// Combine two same-level sketches (no unfolding)
fn merge_same_level(&mut self, other: &FoldCS)

// Combine two level-k sketches into one level-(k-1) sketch
fn unfold_merge(a: &FoldCS, b: &FoldCS) -> FoldCS

// Unfold to a specific level
fn unfold_to(&self, target_level: u32) -> FoldCS

// Unfold all the way to level 0
fn unfold_full(&self) -> FoldCS

// Pairwise merge of a slice of sketches
fn hierarchical_merge(sketches: &[FoldCS]) -> FoldCS
```

#### Conversion & Inspection

```rust
fn to_flat_counters(&self) -> Vec<i64>     // R x W row-major signed counter array
fn rows(&self) -> usize
fn fold_cols(&self) -> usize
fn full_cols(&self) -> usize
fn fold_level(&self) -> u32
fn total_entries(&self) -> usize           // sum of entries across all cells
fn collided_cells(&self) -> usize          // cells with 2+ entries
fn heap(&self) -> &HHHeap
```

---

## 12. Examples

### Rate Limiting (Per-User Request Counting) -- FoldCMS

```rust
use sketchlib_rust::{FoldCMS, SketchInput};

let rows = 3;
let full_cols = 4096;
let fold_level = 4;  // 256 physical columns per sub-window

// Epoch 1: 10:00-10:01
let mut epoch1 = FoldCMS::new(rows, full_cols, fold_level, 5);
epoch1.insert(&SketchInput::Str("user_001"), 350);
epoch1.insert(&SketchInput::Str("user_002"), 10);
epoch1.insert(&SketchInput::Str("user_003"), 600);

// Epoch 2: 10:01-10:02
let mut epoch2 = FoldCMS::new(rows, full_cols, fold_level, 5);
epoch2.insert(&SketchInput::Str("user_001"), 350);
epoch2.insert(&SketchInput::Str("user_002"), 5);
epoch2.insert(&SketchInput::Str("user_003"), 700);

// Merge (same level -- no unfolding needed)
epoch1.merge_same_level(&epoch2);

assert_eq!(epoch1.query(&SketchInput::Str("user_001")), 700);
assert_eq!(epoch1.query(&SketchInput::Str("user_003")), 1300);
```

### DDoS Detection with Hierarchical Merge -- FoldCMS

```rust
use sketchlib_rust::{FoldCMS, SketchInput};

let rows = 3;
let full_cols = 4096;
let fold_level = 4;

let mut epochs: Vec<FoldCMS> = Vec::new();
for _ in 0..3 {
    epochs.push(FoldCMS::new(rows, full_cols, fold_level, 5));
}

epochs[0].insert(&SketchInput::Str("10.0.0.42"), 10_000);
epochs[1].insert(&SketchInput::Str("10.0.0.42"), 15_000);
epochs[2].insert(&SketchInput::Str("10.0.0.42"), 12_000);

let merged = FoldCMS::hierarchical_merge(&epochs);

let total = merged.query(&SketchInput::Str("10.0.0.42"));
assert_eq!(total, 37_000);
// total > 15_000 threshold -> ALERT
```

### Heavy Hitter Detection -- FoldCS

```rust
use sketchlib_rust::{FoldCS, SketchInput};

let rows = 3;
let full_cols = 4096;
let fold_level = 4;

// Sub-window 1
let mut sw1 = FoldCS::new(rows, full_cols, fold_level, 10);
sw1.insert(&SketchInput::Str("popular_endpoint"), 5000);
sw1.insert(&SketchInput::Str("rare_endpoint"), 3);

// Sub-window 2
let mut sw2 = FoldCS::new(rows, full_cols, fold_level, 10);
sw2.insert(&SketchInput::Str("popular_endpoint"), 8000);
sw2.insert(&SketchInput::Str("rare_endpoint"), 2);

// Unfold merge
let merged = FoldCS::unfold_merge(&sw1, &sw2);

assert_eq!(merged.query(&SketchInput::Str("popular_endpoint")), 13000);
assert_eq!(merged.query(&SketchInput::Str("rare_endpoint")), 5);
```

### Choosing Fold Level

```
Given:
  W = 4096     (target accuracy)
  R = 3        (failure probability)
  D ~ 200      (expected distinct keys per sub-window)

Fold level selection:
  k = 4 -> fold_cols = 256 -> ~30 KB per sub-window  (3.2x savings)
  k = 6 -> fold_cols = 64  -> ~12 KB per sub-window  (8x savings)
  k = 8 -> fold_cols = 16  -> ~7.5 KB per sub-window (12.8x savings)

Higher k = more memory savings but more entries per cell on collision.
Choose k such that fold_cols is at least a few times larger than D
to keep collisions rare.

Recommended: fold_cols ~ 2xD to 4xD for a good balance.
  D = 200 -> fold_cols ~ 400-800 -> k = 2 or 3
  D = 50  -> fold_cols ~ 100-200 -> k = 4 or 5
```

---

## 13. Summary

### Properties at a Glance

| Property | FoldCMS | FoldCS |
|----------|---------|--------|
| **Error bound** | eps = e/W, delta = e^(-R) | eps = 1/sqrt(W), delta = 2^(-Omega(R)) |
| **Error norm** | L1 (one-sided) | L2 (two-sided, unbiased) |
| **Additional error from folding** | Zero | Zero |
| **Memory (sparse sub-window)** | O(R x (W/2^k + D)) | O(R x (W/2^k + D)) |
| **Insert time** | O(R x E), E ~ 1 for sparse | O(R x E), E ~ 1 for sparse |
| **Query time** | O(R x E) | O(R x E) |
| **Query aggregation** | min across rows | median across rows |
| **Same-level merge** | O(R x fold_cols x E) | O(R x fold_cols x E) |
| **Unfold merge** | O(R x fold_cols x E) | O(R x fold_cols x E) |
| **Hierarchical merge** | O(R x W x k) total | O(R x W x k) total |
| **Serializable** | Yes (serde) | Yes (serde) |
| **Linearity preserved** | Yes | Yes |

### Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Cell representation | Enum (Empty/Single/Collided) | Zero heap allocation for the common case (sparse cells) |
| Column index type | u16 | Supports W up to 65536; saves 6 bytes per entry vs u64 |
| Counter type | i64 | Signed to support Count Sketch |
| Grid layout | Flat row-major Vec | Cache-friendly, simple indexing |
| Merge direction | Unfold (k -> k-1) | Natural fit with EH pairwise merge schedule |
| Top-K tracking | Per-sketch HHHeap | Enables heavy hitter queries without full scan |
| Cell type sharing | FoldCS reuses FoldCell from FoldCMS | No code duplication for cell logic |

### When to Use

| Condition | Recommendation |
|-----------|---------------|
| D << W, sub-windows are sparse | Use FoldCMS/FoldCS with k >= 3 |
| D ~ W, sub-windows are dense | Use standard CMS/CS (no benefit from folding) |
| Need exact CMS/CS accuracy | FoldCMS/FoldCS provides identical accuracy at any fold level |
| Frequency counting, no deletions | FoldCMS (one-sided error, min aggregation) |
| Heavy hitter detection, L2 queries | FoldCS (unbiased, median aggregation) |
| Need deletion support | FoldCS (signed counters handle negative deltas) |
| Memory is the bottleneck | Use higher k (fold_cols ~ 2-4x D) |
| Latency is the bottleneck | Use lower k (fewer entries per cell -> faster operations) |
| Windowed monitoring (EH/sliding window) | FoldCMS/FoldCS + EH with unfold merge on bucket combine |
