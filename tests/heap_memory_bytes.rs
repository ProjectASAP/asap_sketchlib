//! `TopKHeap::estimated_memory_bytes` and the heap sketches'
//! `estimated_memory_bytes`, checked against `size_of` and against the bytes
//! actually allocated, counted by a global allocator. One `#[test]` so no other
//! test allocates while a count is taken.

use asap_sketchlib::{
    CountMinSketchWithHeap, CountSketchWithHeap, DataInput, HeapEntry, RegularPath, TopKHeap,
    Vector2D,
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

/// A 9-byte key, the length of a one-column `TupleKey` holding an `i64`.
fn tuple_like_key(i: u64) -> Vec<u8> {
    [&[3u8][..], &i.to_be_bytes()].concat()
}

#[test]
fn estimated_memory_bytes_match_size_of_and_the_allocation() {
    // size_of: a resident is one `HeapEntry` in the heap array, one `u64`
    // digest and one index entry; a heap of capacity 0 allocates nothing.
    assert_eq!(
        TopKHeap::<i64>::estimated_memory_bytes(0, &DataInput::U64(0)),
        std::mem::size_of::<TopKHeap<i64>>()
    );
    assert!(
        TopKHeap::<f64>::estimated_memory_bytes(1024, &DataInput::U64(0))
            > std::mem::size_of::<TopKHeap<f64>>()
                + 1024 * (std::mem::size_of::<HeapEntry<f64>>() + 8)
    );
    assert_eq!(
        CountMinSketchWithHeap::<Vector2D<f64>, RegularPath>::estimated_memory_bytes(
            3,
            64,
            0,
            &DataInput::U64(0)
        ),
        std::mem::size_of::<CountMinSketchWithHeap<Vector2D<f64>, RegularPath>>() + 3 * 64 * 8
    );

    // TopKHeap filled with `k` distinct keys (no eviction), for capacities on
    // both sides of the 1,024-slot up-front reservation.
    for k in [1usize, 3, 7, 8, 100, 1024, 2000, 5000] {
        let (_heap, bytes) = measured_bytes(|| {
            let mut heap = TopKHeap::<f64>::new(k);
            for i in 0..k as u64 {
                heap.update(&DataInput::U64(i), i as f64 + 0.5);
            }
            heap
        });
        assert_eq!(
            bytes,
            TopKHeap::<f64>::estimated_memory_bytes(k, &DataInput::U64(0)),
            "u64 keys, k = {k}"
        );

        let (_heap, bytes) = measured_bytes(|| {
            let mut heap = TopKHeap::<i64>::new(k);
            for i in 0..k as u64 {
                heap.update(&DataInput::Bytes(&tuple_like_key(i)), i as i64 + 1);
            }
            heap
        });
        assert_eq!(
            bytes,
            TopKHeap::<i64>::estimated_memory_bytes(k, &DataInput::Bytes(&[0; 9])),
            "9-byte keys, k = {k}"
        );
    }

    // The f64 heap sketches filled with `k` distinct 9-byte keys.
    for k in [1usize, 7, 64, 100, 1000] {
        let key_shape = DataInput::Bytes(&[0; 9]);
        let (_sketch, bytes) = measured_bytes(|| {
            let mut sketch = CountMinSketchWithHeap::<Vector2D<f64>, RegularPath>::new(3, 64, k);
            for i in 0..k as u64 {
                sketch.update_weighted(&DataInput::Bytes(&tuple_like_key(i)), 0.5);
            }
            sketch
        });
        assert_eq!(
            bytes,
            CountMinSketchWithHeap::<Vector2D<f64>, RegularPath>::estimated_memory_bytes(
                3, 64, k, &key_shape
            ),
            "CountMinSketchWithHeap, k = {k}"
        );

        let (_sketch, bytes) = measured_bytes(|| {
            let mut sketch = CountSketchWithHeap::<Vector2D<f64>, RegularPath>::new(5, 64, k);
            for i in 0..k as u64 {
                sketch.update_weighted(&DataInput::Bytes(&tuple_like_key(i)), 0.5);
            }
            sketch
        });
        assert_eq!(
            bytes,
            CountSketchWithHeap::<Vector2D<f64>, RegularPath>::estimated_memory_bytes(
                5, 64, k, &key_shape
            ),
            "CountSketchWithHeap, k = {k}"
        );
    }
}
