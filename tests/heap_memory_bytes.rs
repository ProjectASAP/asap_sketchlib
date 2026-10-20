//! `TopKHeap`, `CountMinSketchWithHeap`, `CountSketchWithHeap` and `UnivMonQ`
//! memory methods checked against `size_of` and against the bytes an instance
//! holds, counted by a global allocator. One `#[test]` so no other test
//! allocates while a count is taken.

use asap_sketchlib::{
    CountMinSketchWithHeap, CountSketchWithHeap, DataInput, FastPath, HeapEntry, RegularPath,
    TopKHeap, UnivMonQ, UnivMonQConfig, Vector2D,
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
