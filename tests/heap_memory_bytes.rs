//! `TopKHeap::estimated_memory_bytes` checked against `size_of` and against the
//! bytes a filled heap holds, counted by a global allocator. One `#[test]` so
//! no other test allocates while a count is taken.

use asap_sketchlib::{DataInput, HeapEntry, TopKHeap, UnivMonQ, UnivMonQConfig};
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

    // Filled with `heap_size` distinct keys the heap holds at most the
    // estimate; after 50 times as many, each evicting the root, exactly it.
    // Capacities sit on both sides of the 1,024-slot reservation.
    for heap_size in [1usize, 3, 7, 8, 20, 100, 1024, 2000, 5000] {
        for distinct in [heap_size, 50 * heap_size] {
            let (_heap, bytes) = measured_bytes(|| {
                let mut heap = TopKHeap::<i64>::new(heap_size);
                for i in 0..distinct as u64 {
                    heap.update(&DataInput::U64(i), i as i64 + 1);
                }
                heap
            });
            check(
                bytes,
                TopKHeap::<i64>::estimated_memory_bytes(heap_size, &DataInput::U64(0)),
                distinct > heap_size,
                &format!("u64 keys, heap_size = {heap_size}, distinct = {distinct}"),
            );

            let (_heap, bytes) = measured_bytes(|| {
                let mut heap = TopKHeap::<f64>::new(heap_size);
                for i in 0..distinct as u64 {
                    let key = [&[3u8][..], &i.to_le_bytes()].concat();
                    heap.update(&DataInput::Bytes(&key), i as f64 + 1.0);
                }
                heap
            });
            check(
                bytes,
                TopKHeap::<f64>::estimated_memory_bytes(heap_size, &DataInput::Bytes(&[0; 9])),
                distinct > heap_size,
                &format!("9-byte keys, heap_size = {heap_size}, distinct = {distinct}"),
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

/// `measured` is at most `estimated`, and equal to it once keys have churned
/// (for these shapes; a half-full index may take longer to grow).
fn check(measured: usize, estimated: usize, churned: bool, context: &str) {
    if churned {
        assert_eq!(measured, estimated, "{context}");
    } else {
        assert!(measured <= estimated, "{context}: {measured} > {estimated}");
    }
}
