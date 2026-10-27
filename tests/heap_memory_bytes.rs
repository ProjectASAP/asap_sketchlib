//! `TopKHeap`, `CountMinSketch`, `CountSketch`, their WithHeap forms, `Kll`,
//! `DdSketch`, the HyperLogLogs, `Kmv`, `CountSketchWithL2Norm`, `UnivMon` and
//! `UnivMonQ` memory methods checked against `size_of` and against the bytes
//! an instance holds, counted by a global allocator. One `#[test]` so no other test
//! allocates while a count is taken.

use asap_sketchlib::{
    Classic, CountMinF64, CountMinSketchWithHeap, CountMinSketchWithHeapDelta, CountSketch,
    CountSketchWithHeap, CountSketchWithHeapDelta, CountSketchWithL2Norm, DataInput, DdSketch,
    Ertl, FastPath, HeapEntry, HyperLogLog, HyperLogLogHip, HyperLogLogHipP12, HyperLogLogHipP16,
    HyperLogLogP12, HyperLogLogP14, HyperLogLogP16, Kll, Kmv, RegularPath, TopKHeap, UnivMon,
    UnivMonQ, UnivMonQConfig, Vector2D,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};

/// Counts the bytes currently allocated through it.
struct CountingAllocator;

static LIVE_BYTES: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE_BYTES.fetch_add(layout.size() as isize, Ordering::SeqCst);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as isize, Ordering::SeqCst);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LIVE_BYTES.fetch_add(new_size as isize - layout.size() as isize, Ordering::SeqCst);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn live_bytes() -> isize {
    LIVE_BYTES.load(Ordering::SeqCst)
}

/// Heap bytes `build` leaves allocated, plus the size of the value it returns.
fn measured_bytes<T>(build: impl FnOnce() -> T) -> (T, usize) {
    let before = live_bytes();
    let value = build();
    let heap = (live_bytes() - before) as usize;
    (value, heap + std::mem::size_of::<T>())
}

/// `measured` is at most `estimated`, and equal to it once keys have churned
/// (for these shapes; a half-full index may take longer to grow).
fn check(measured: usize, estimated: usize, churned: bool, context: &str) {
    if churned {
        assert_eq!(measured, estimated, "{context}");
    } else {
        assert!(measured <= estimated, "{context}: {measured} > {estimated}");
    }
}

#[test]
fn estimated_memory_bytes_match_size_of_and_the_allocation() {
    // A heap of capacity 0 allocates nothing; a resident is at least one entry
    // and one digest.
    assert_eq!(
        TopKHeap::<i64>::estimated_memory_bytes(0, &DataInput::U64(0)),
        std::mem::size_of::<TopKHeap<i64>>()
    );
    assert!(
        TopKHeap::<i64>::estimated_memory_bytes(1024, &DataInput::U64(0))
            > std::mem::size_of::<TopKHeap<i64>>()
                + 1024 * (std::mem::size_of::<HeapEntry<i64>>() + 8)
    );

    // A shape past the address space prices at `usize::MAX`.
    assert_eq!(
        TopKHeap::<i64>::estimated_memory_bytes(usize::MAX, &DataInput::Str("key")),
        usize::MAX
    );
    assert_eq!(
        CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::estimated_memory_bytes(
            5,
            512,
            1 << 61,
            &DataInput::U64(0),
        ),
        usize::MAX
    );

    // After `heap_size` distinct keys a heap holds at most the estimate; after
    // 50 times as many, each evicting the root, exactly it. `memory_bytes` is
    // the allocation throughout. Capacities sit on both sides of the
    // 1,024-slot reservation.
    for heap_size in [1usize, 3, 7, 8, 20, 100, 1024, 2000, 5000] {
        for distinct in [heap_size, 50 * heap_size] {
            let churned = distinct > heap_size;
            let context = format!("heap_size = {heap_size}, distinct = {distinct}");
            let (heap, bytes) = measured_bytes(|| {
                let mut heap = TopKHeap::<i64>::new(heap_size);
                for i in 0..distinct as u64 {
                    heap.update(&DataInput::U64(i), i as i64 + 1);
                }
                heap
            });
            let estimated = TopKHeap::<i64>::estimated_memory_bytes(heap_size, &DataInput::U64(0));
            check(bytes, estimated, churned, &format!("u64 keys, {context}"));
            assert_eq!(heap.memory_bytes(), bytes, "u64 keys, {context}");

            let (heap, bytes) = measured_bytes(|| {
                let mut heap = TopKHeap::<f64>::new(heap_size);
                for i in 0..distinct as u64 {
                    let key = [&[3u8][..], &i.to_le_bytes()].concat();
                    heap.update(&DataInput::Bytes(&key), i as f64 + 1.0);
                }
                heap
            });
            let estimated =
                TopKHeap::<f64>::estimated_memory_bytes(heap_size, &DataInput::Bytes(&[0; 9]));
            check(
                bytes,
                estimated,
                churned,
                &format!("9-byte keys, {context}"),
            );
            assert_eq!(heap.memory_bytes(), bytes, "9-byte keys, {context}");
        }
    }

    // The top-k sketches on both paths, filled and then churned by keys whose
    // rising weights make each new one evict the root.
    for (rows, cols, heap_size) in [
        (1usize, 1usize, 1usize),
        (3, 64, 7),
        (5, 2048, 100),
        (20, 100, 2000),
    ] {
        for distinct in [heap_size, 50 * heap_size] {
            let churned = distinct > heap_size;
            let context = format!("{rows}x{cols}, heap {heap_size}, distinct {distinct}");
            let (sketch, bytes) = measured_bytes(|| {
                let mut sketch = CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::new(
                    rows, cols, heap_size,
                );
                for i in 0..distinct as u64 {
                    sketch.update_weighted(&DataInput::U64(i), i as i64 + 1);
                }
                sketch
            });
            let estimated =
                CountMinSketchWithHeap::<Vector2D<i64>, RegularPath>::estimated_memory_bytes(
                    rows,
                    cols,
                    heap_size,
                    &DataInput::U64(0),
                );
            check(
                bytes,
                estimated,
                churned,
                &format!("CountMinSketchWithHeap {context}"),
            );
            assert_eq!(
                sketch.memory_bytes(),
                bytes,
                "CountMinSketchWithHeap {context}"
            );

            let (sketch, bytes) = measured_bytes(|| {
                let mut sketch =
                    CountSketchWithHeap::<Vector2D<f64>, FastPath>::new(rows, cols, heap_size);
                for i in 0..distinct as u64 {
                    let name = format!("key-{i:08}");
                    sketch.update_weighted(&DataInput::Str(&name), (i + 1) as f64 * 1e6);
                }
                sketch
            });
            let estimated = CountSketchWithHeap::<Vector2D<f64>, FastPath>::estimated_memory_bytes(
                rows,
                cols,
                heap_size,
                &DataInput::Str("key-00000000"),
            );
            check(
                bytes,
                estimated,
                churned,
                &format!("CountSketchWithHeap {context}"),
            );
            assert_eq!(
                sketch.memory_bytes(),
                bytes,
                "CountSketchWithHeap {context}"
            );
        }
    }

    // A decoded top-k sketch grows as a built one does: at most the estimate
    // after its first keys, exactly it once churned.
    type CountMinI64Heap = CountMinSketchWithHeap<Vector2D<i64>, RegularPath>;
    for (rows, cols, heap_size) in [(3usize, 64usize, 7usize), (5, 2048, 100), (5, 100, 2000)] {
        let estimated =
            CountMinI64Heap::estimated_memory_bytes(rows, cols, heap_size, &DataInput::U64(0));
        for (encoded_keys, distinct) in [
            (3usize, heap_size),
            (3, 50 * heap_size),
            (heap_size, 50 * heap_size),
        ] {
            let context = format!(
                "decoded CountMinSketchWithHeap {rows}x{cols}, heap {heap_size}, \
                 {encoded_keys} keys encoded, distinct {distinct}"
            );
            let mut source = CountMinI64Heap::new(rows, cols, heap_size);
            for i in 0..encoded_keys as u64 {
                source.update_weighted(&DataInput::U64(i), i as i64 + 1);
            }
            let encoded = source.to_asapv1().unwrap();
            let (sketch, bytes) = measured_bytes(|| {
                let mut sketch = CountMinI64Heap::from_asapv1(&encoded).unwrap();
                for i in encoded_keys as u64..distinct as u64 {
                    sketch.update_weighted(&DataInput::U64(i), i as i64 + 1);
                }
                sketch
            });
            check(bytes, estimated, distinct > heap_size, &context);
            assert_eq!(sketch.memory_bytes(), bytes, "{context}");
        }
    }

    // A clone keeps its source's allocation and a merged delta refills the
    // sketch's own heap, so both grow as a built heap does: exactly to the
    // estimate once churned.
    for heap_size in [7usize, 1024, 2000] {
        let size = heap_size as u64;
        let estimated = TopKHeap::<i64>::estimated_memory_bytes(heap_size, &DataInput::U64(0));
        let mut source = TopKHeap::<i64>::new(heap_size);
        for i in 0..size - 1 {
            source.update(&DataInput::U64(i), i as i64 + 1);
        }
        let (heap, bytes) = measured_bytes(|| {
            let mut heap = source.clone();
            for i in size..50 * size {
                heap.update(&DataInput::U64(i), i as i64 + 1);
            }
            heap
        });
        let context = format!("cloned TopKHeap, heap_size = {heap_size}");
        check(bytes, estimated, true, &context);
        assert_eq!(heap.memory_bytes(), bytes, "{context}");
    }
    type CountSketchF64Heap = CountSketchWithHeap<Vector2D<f64>, FastPath>;
    for (rows, cols, heap_size) in [(3usize, 64usize, 1024usize), (5, 512, 7)] {
        let size = heap_size as u64;
        let mut count_min = CountMinI64Heap::new(rows, cols, heap_size);
        let mut count_sketch = CountSketchF64Heap::new(rows, cols, heap_size);
        for i in 0..size - 1 {
            count_min.update_weighted(&DataInput::U64(i), i as i64 + 1);
            count_sketch.update_weighted(&DataInput::U64(i), (i + 1) as f64 * 1e6);
        }
        let count_min_delta = CountMinSketchWithHeapDelta::<i64>::from_asapv1(
            &CountMinSketchWithHeapDelta::<i64>::new(
                rows,
                cols,
                Vec::new(),
                Vec::new(),
                count_min.heap().clone(),
            )
            .unwrap()
            .to_asapv1()
            .unwrap(),
        )
        .unwrap();
        let count_sketch_delta = CountSketchWithHeapDelta::<f64, FastPath>::from_asapv1(
            &CountSketchWithHeapDelta::<f64, FastPath>::new(
                rows,
                cols,
                Vec::new(),
                Vec::new(),
                count_sketch.heap().clone(),
            )
            .unwrap()
            .to_asapv1()
            .unwrap(),
        )
        .unwrap();
        let estimated =
            CountMinI64Heap::estimated_memory_bytes(rows, cols, heap_size, &DataInput::U64(0));
        for delta in [false, true] {
            let context =
                format!("CountMinSketchWithHeap {rows}x{cols}, heap {heap_size}, delta = {delta}");
            let (sketch, bytes) = measured_bytes(|| {
                let mut sketch = if delta {
                    let mut sketch = CountMinI64Heap::new(rows, cols, heap_size);
                    sketch.merge_delta(&count_min_delta).unwrap();
                    sketch
                } else {
                    count_min.clone()
                };
                for i in size..50 * size {
                    sketch.update_weighted(&DataInput::U64(i), i as i64 + 1);
                }
                sketch
            });
            check(bytes, estimated, true, &context);
            assert_eq!(sketch.memory_bytes(), bytes, "{context}");
        }
        let estimated =
            CountSketchF64Heap::estimated_memory_bytes(rows, cols, heap_size, &DataInput::U64(0));
        for delta in [false, true] {
            let context =
                format!("CountSketchWithHeap {rows}x{cols}, heap {heap_size}, delta = {delta}");
            let (sketch, bytes) = measured_bytes(|| {
                let mut sketch = if delta {
                    let mut sketch = CountSketchF64Heap::new(rows, cols, heap_size);
                    sketch.merge_delta(&count_sketch_delta).unwrap();
                    sketch
                } else {
                    count_sketch.clone()
                };
                for i in size..50 * size {
                    sketch.update_weighted(&DataInput::U64(i), (i + 1) as f64 * 1e6);
                }
                sketch
            });
            check(bytes, estimated, true, &context);
            assert_eq!(sketch.memory_bytes(), bytes, "{context}");
        }
    }

    // Plain Count-Min and Count Sketch hold the struct and their counters, as
    // their estimates price them.
    for (rows, cols) in [(1usize, 1usize), (3, 64), (5, 2048), (64, 1024)] {
        let (sketch, bytes) = measured_bytes(|| {
            let mut sketch: CountMinF64 = CountMinF64::with_dimensions(rows, cols);
            for i in 0..1_000u64 {
                sketch.update(&DataInput::U64(i));
            }
            sketch
        });
        assert_eq!(sketch.memory_bytes(), bytes, "CountMinF64 {rows}x{cols}");
        assert_eq!(
            CountMinF64::estimated_memory_bytes(rows, cols),
            bytes,
            "CountMinF64 {rows}x{cols}"
        );
        let (sketch, bytes) = measured_bytes(|| {
            let mut sketch = CountSketch::<Vector2D<i32>, FastPath>::with_dimensions(rows, cols);
            for i in 0..1_000u64 {
                sketch.update(&DataInput::U64(i));
            }
            sketch
        });
        assert_eq!(sketch.memory_bytes(), bytes, "CountSketch {rows}x{cols}");
        assert_eq!(
            CountSketch::<Vector2D<i32>, FastPath>::estimated_memory_bytes(rows, cols),
            bytes,
            "CountSketch {rows}x{cols}"
        );
    }

    // KLL holds at most its estimate once `n` values have arrived directly,
    // through merges, a self-merge or after a decode, with a quantile cached:
    // exactly it for no value or one, and under three times the largest case.
    let kll_fill = |sketch: &mut Kll<f64>, values: std::ops::Range<usize>| {
        for i in values {
            sketch.update(&((i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) as f64));
        }
    };
    for (k, m) in [(200usize, 8usize), (64, 4)] {
        for n in [0usize, 1, 150, 1_000, 100_000] {
            let estimated = Kll::<f64>::estimated_memory_bytes(k, m, n);
            let mut half = Kll::<f64>::with_seed(k, m, 9);
            kll_fill(&mut half, 0..n / 2);
            let encoded = half.to_asapv1().unwrap();
            let mut largest = 0;
            for scenario in 0..6 {
                let context = format!("Kll k = {k}, m = {m}, n = {n}, scenario {scenario}");
                let (sketch, bytes) = measured_bytes(|| {
                    let mut sketch = Kll::<f64>::with_seed(k, m, 1);
                    match scenario {
                        0 => kll_fill(&mut sketch, 0..n),
                        1 => {
                            kll_fill(&mut sketch, 0..n / 2);
                            let mut other = Kll::<f64>::with_seed(k, m, 2);
                            kll_fill(&mut other, n / 2..n);
                            sketch.merge(&other);
                        }
                        2 | 3 => {
                            let parts = if scenario == 2 { 1 } else { 4 };
                            for part in 0..parts {
                                let mut other = Kll::<f64>::with_seed(k, m, part as u64 + 2);
                                kll_fill(&mut other, part * n / parts..(part + 1) * n / parts);
                                sketch.merge(&other);
                            }
                        }
                        4 if n % 2 == 0 => {
                            kll_fill(&mut sketch, 0..n / 2);
                            let copy = sketch.clone();
                            sketch.merge(&copy);
                        }
                        _ => {
                            sketch = Kll::<f64>::from_asapv1(&encoded).unwrap();
                            kll_fill(&mut sketch, n / 2..n);
                        }
                    }
                    sketch.quantile_cached(0.5);
                    sketch
                });
                check(bytes, estimated, n <= 1, &context);
                assert_eq!(sketch.memory_bytes(), bytes, "{context}");
                largest = largest.max(bytes);
            }
            assert!(
                estimated < 3 * largest,
                "Kll k = {k}, m = {m}, n = {n}: {estimated} against {largest}"
            );
        }
    }

    // DDSketch holds at most its estimate for values across a magnitude range
    // in ascending, descending and scattered order, with and without
    // negatives, built directly, from merged halves or from a decoded half:
    // exactly it for no value or one, under twice the largest case past three.
    for alpha in [0.01, 0.05] {
        for (low, high) in [(1.0f64, 1.5f64), (1e-3, 1e3), (1.0, 1e6), (1e-9, 1e9)] {
            for negatives in [false, true] {
                for n in [0usize, 1, 2, 3, 10_000] {
                    let estimated =
                        DdSketch::estimated_memory_bytes(alpha, n, low, high, negatives);
                    let mut largest = 0;
                    for order in 0..3 {
                        for source in ["direct", "merged", "decoded"] {
                            let value = |i: usize| {
                                let t = match order {
                                    0 => i as f64 / (n.max(2) - 1) as f64,
                                    1 => 1.0 - i as f64 / (n.max(2) - 1) as f64,
                                    _ => {
                                        ((i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 11)
                                            as f64
                                            / (1u64 << 53) as f64
                                    }
                                };
                                let magnitude = (low * (high / low).powf(t)).clamp(low, high);
                                if negatives && i % 2 == 1 {
                                    -magnitude
                                } else {
                                    magnitude
                                }
                            };
                            let context = format!(
                                "DdSketch alpha {alpha}, [{low}, {high}], negatives = {negatives}, \
                                 order {order}, {source}, n = {n}"
                            );
                            let (sketch, bytes) = measured_bytes(|| {
                                let mut sketch = DdSketch::new(alpha);
                                let mut other = DdSketch::new(alpha);
                                for i in 0..n / 2 {
                                    sketch.add(&value(i));
                                }
                                if source == "decoded" {
                                    let encoded = sketch.to_asapv1().unwrap();
                                    sketch = DdSketch::from_asapv1(&encoded).unwrap();
                                }
                                for i in n / 2..n {
                                    if source == "merged" {
                                        other.add(&value(i));
                                    } else {
                                        sketch.add(&value(i));
                                    }
                                }
                                sketch.merge(&other).unwrap();
                                sketch
                            });
                            check(bytes, estimated, n <= 1, &context);
                            assert_eq!(sketch.memory_bytes(), bytes, "{context}");
                            largest = largest.max(bytes);
                        }
                    }
                    assert!(
                        n == 2 || n == 3 || estimated < 2 * largest,
                        "DdSketch alpha {alpha}, [{low}, {high}], negatives = {negatives}, \
                         n = {n}: {estimated} against {largest}"
                    );
                }
            }
        }
    }

    // HyperLogLog holds the struct and one byte per register, whatever its
    // variant; the estimate takes the precision at run time.
    fn hll_bytes<T>(build: impl FnOnce() -> T, memory_bytes: impl Fn(&T) -> usize) -> usize {
        let (sketch, bytes) = measured_bytes(build);
        assert_eq!(memory_bytes(&sketch), bytes);
        bytes
    }
    let fill = |update: &mut dyn FnMut(&DataInput)| {
        for i in 0..10_000u64 {
            update(&DataInput::U64(i));
        }
    };
    for (precision, bytes) in [
        (
            12,
            hll_bytes(
                || {
                    let mut sketch = HyperLogLogP12::<Classic>::new();
                    fill(&mut |key| sketch.update(key));
                    sketch
                },
                HyperLogLogP12::<Classic>::memory_bytes,
            ),
        ),
        (
            14,
            hll_bytes(
                || {
                    let mut sketch = HyperLogLogP14::<Ertl>::new();
                    fill(&mut |key| sketch.update(key));
                    sketch
                },
                HyperLogLogP14::<Ertl>::memory_bytes,
            ),
        ),
        (
            16,
            hll_bytes(
                || {
                    let mut sketch = HyperLogLogP16::<Classic>::new();
                    fill(&mut |key| sketch.update(key));
                    sketch
                },
                HyperLogLogP16::<Classic>::memory_bytes,
            ),
        ),
    ] {
        assert_eq!(
            HyperLogLog::<Classic>::estimated_memory_bytes(precision),
            bytes,
            "HyperLogLog p = {precision}"
        );
    }
    for (precision, bytes) in [
        (
            12,
            hll_bytes(
                || {
                    let mut sketch = HyperLogLogHipP12::new();
                    fill(&mut |key| sketch.update(key));
                    sketch
                },
                HyperLogLogHipP12::memory_bytes,
            ),
        ),
        (
            16,
            hll_bytes(
                || {
                    let mut sketch = HyperLogLogHipP16::new();
                    fill(&mut |key| sketch.update(key));
                    sketch
                },
                HyperLogLogHipP16::memory_bytes,
            ),
        ),
    ] {
        assert_eq!(
            HyperLogLogHip::estimated_memory_bytes(precision),
            bytes,
            "HyperLogLogHip p = {precision}"
        );
    }
    assert_eq!(
        HyperLogLog::<Classic>::estimated_memory_bytes(200),
        usize::MAX
    );

    // KMV at k = 4323 holds exactly its estimate after `distinct` distinct
    // inputs, below the 1,024-slot reservation, between it and k, and past k,
    // and at most it when the inputs repeat.
    for distinct in [0usize, 1, 500, 1_024, 1_025, 3_000, 4_323, 20_000] {
        for (merged, repeats) in [(false, 1u64), (true, 1), (false, 2)] {
            let context =
                format!("Kmv distinct = {distinct}, merged = {merged}, repeats = {repeats}");
            let (sketch, bytes) = measured_bytes(|| {
                let mut sketch: Kmv = Kmv::new(4_323);
                let mut other: Kmv = Kmv::new(4_323);
                for i in 0..distinct as u64 {
                    let target = if merged && i % 2 == 1 {
                        &mut other
                    } else {
                        &mut sketch
                    };
                    target.update(&DataInput::U64(i / repeats));
                }
                sketch.merge(&other);
                sketch
            });
            let estimated = Kmv::estimated_memory_bytes(4_323, distinct);
            check(bytes, estimated, repeats == 1, &context);
            assert_eq!(sketch.memory_bytes(), bytes, "{context}");
        }
    }

    // A cloned KMV keeps its source's slots and grows as it would.
    for (k, cloned, distinct) in [(4_096usize, 1_500usize, 10_000usize), (4_323, 1_100, 3_000)] {
        let mut source: Kmv = Kmv::new(k);
        for i in 0..cloned as u64 {
            source.update(&DataInput::U64(i));
        }
        let (sketch, bytes) = measured_bytes(|| {
            let mut sketch = source.clone();
            for i in cloned as u64..distinct as u64 {
                sketch.update(&DataInput::U64(i));
            }
            sketch
        });
        let context = format!("cloned Kmv k = {k}, {cloned} cloned, distinct = {distinct}");
        check(
            bytes,
            Kmv::estimated_memory_bytes(k, distinct),
            true,
            &context,
        );
        assert_eq!(sketch.memory_bytes(), bytes, "{context}");
    }

    // A decoded KMV grows as a built one does, and a shape past the address
    // space prices at `usize::MAX`.
    for (k, encoded, distinct) in [
        (4_000usize, 3_000usize, 13_000usize),
        (4_096, 10, 10_010),
        (4_323, 2_000, 2_500),
    ] {
        let context = format!("decoded Kmv k = {k}, {encoded} encoded, distinct = {distinct}");
        let mut source: Kmv = Kmv::new(k);
        for i in 0..encoded as u64 {
            source.update(&DataInput::U64(i));
        }
        let bytes_on_wire = source.to_asapv1().unwrap();
        let (sketch, bytes) = measured_bytes(|| {
            let mut sketch: Kmv = Kmv::from_asapv1(&bytes_on_wire).unwrap();
            for i in encoded as u64..distinct as u64 {
                sketch.update(&DataInput::U64(i));
            }
            sketch
        });
        check(
            bytes,
            Kmv::estimated_memory_bytes(k, distinct),
            true,
            &context,
        );
        assert_eq!(sketch.memory_bytes(), bytes, "{context}");
    }
    assert_eq!(
        Kmv::estimated_memory_bytes(usize::MAX, usize::MAX),
        usize::MAX
    );

    // UnivMon holds at most its estimate, and exactly it once string keys with
    // rising weights have churned every layer's heap; merged halves stay within it.
    for (heap_size, rows, cols, layers) in [
        (7usize, 3usize, 64usize, 4usize),
        (20, 5, 512, 6),
        (100, 5, 2048, 3),
    ] {
        let estimated = UnivMon::estimated_memory_bytes(
            heap_size,
            rows,
            cols,
            layers,
            &DataInput::Str("flow-00000000"),
        );
        for distinct in [heap_size, (50 * heap_size) << (layers - 1)] {
            for merged in [false, true] {
                let churned = distinct > heap_size && !merged;
                let context = format!(
                    "UnivMon heap {heap_size}, {rows}x{cols}, {layers} layers, \
                     distinct {distinct}, merged = {merged}"
                );
                let (sketch, bytes) = measured_bytes(|| {
                    let mut sketch = UnivMon::new(heap_size, rows, cols, layers);
                    let mut other = UnivMon::new(heap_size, rows, cols, layers);
                    for i in 0..distinct {
                        let name = format!("flow-{i:08}");
                        let target = if merged && i % 2 == 1 {
                            &mut other
                        } else {
                            &mut sketch
                        };
                        target.update_weighted(&DataInput::Str(&name), i as i64 + 1);
                    }
                    if merged {
                        sketch.merge(&other);
                    }
                    sketch
                });
                check(bytes, estimated, churned, &context);
                assert_eq!(sketch.memory_bytes(), bytes, "{context}");
            }
        }
        let (sketch, bytes) =
            measured_bytes(|| <CountSketchWithL2Norm>::with_dimensions(rows, cols));
        assert_eq!(
            sketch.memory_bytes(),
            bytes,
            "CountSketchWithL2Norm {rows}x{cols}"
        );
        assert_eq!(
            CountSketchWithL2Norm::estimated_memory_bytes(rows, cols),
            bytes,
            "CountSketchWithL2Norm {rows}x{cols}"
        );
    }

    // A cloned pyramid keeps its heaps' allocations and grows as its source
    // would, exactly to the estimate once churned.
    for (heap_size, rows, cols, layers) in [(100usize, 5usize, 256usize, 4usize), (20, 5, 512, 6)] {
        let mut source = UnivMon::new(heap_size, rows, cols, layers);
        for i in 0..heap_size - 1 {
            let name = format!("flow-{i:08}");
            source.update_weighted(&DataInput::Str(&name), i as i64 + 1);
        }
        let distinct = (50 * heap_size) << (layers - 1);
        let (sketch, bytes) = measured_bytes(|| {
            let mut sketch = source.clone();
            for i in heap_size..distinct {
                let name = format!("flow-{i:08}");
                sketch.update_weighted(&DataInput::Str(&name), i as i64 + 1);
            }
            sketch
        });
        let estimated = UnivMon::estimated_memory_bytes(
            heap_size,
            rows,
            cols,
            layers,
            &DataInput::Str("flow-00000000"),
        );
        let context = format!("cloned UnivMon heap {heap_size}, {rows}x{cols}, {layers} layers");
        check(bytes, estimated, true, &context);
        assert_eq!(sketch.memory_bytes(), bytes, "{context}");
    }

    // A decoded pyramid grows as a built one does, exactly to the estimate once
    // churned, and a shape past the address space prices at `usize::MAX`.
    for (encoded_keys, distinct) in [(100usize, 50_000usize), (5, 50_000), (100, 300)] {
        let (heap_size, rows, cols, layers) = (20, 5, 512, 6);
        let context = format!("decoded UnivMon, {encoded_keys} keys encoded, distinct {distinct}");
        let mut source = UnivMon::new(heap_size, rows, cols, layers);
        for i in 0..encoded_keys {
            let name = format!("flow-{i:08}");
            source.update_weighted(&DataInput::Str(&name), i as i64 + 1);
        }
        let encoded = source.to_asapv1().unwrap();
        let (sketch, bytes) = measured_bytes(|| {
            let mut sketch = UnivMon::from_asapv1(&encoded).unwrap();
            for i in encoded_keys..distinct {
                let name = format!("flow-{i:08}");
                sketch.update_weighted(&DataInput::Str(&name), i as i64 + 1);
            }
            sketch
        });
        let estimated = UnivMon::estimated_memory_bytes(
            heap_size,
            rows,
            cols,
            layers,
            &DataInput::Str("flow-00000000"),
        );
        check(bytes, estimated, distinct > 300, &context);
        assert_eq!(sketch.memory_bytes(), bytes, "{context}");
    }
    assert_eq!(
        UnivMon::estimated_memory_bytes(1 << 61, 5, 512, 6, &DataInput::Str("flow")),
        usize::MAX
    );

    // UnivMon-Q reports exactly what it allocated, fresh and after enough
    // values to churn its candidate tables and fill its ordered samples.
    for n in [0usize, 1_000, 200_000] {
        let (sketch, bytes) = measured_bytes(|| {
            let mut sketch = UnivMonQ::new(UnivMonQConfig::default()).unwrap();
            for i in 0..n {
                sketch.update(&((i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) % 50_000));
            }
            sketch
        });
        assert_eq!(sketch.memory_bytes(), bytes, "UnivMon-Q after {n} values");
    }
}
