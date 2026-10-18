//! `HHHeap::estimated_memory_bytes` and `WeightedFrequency::estimated_memory_bytes`
//! checked against `size_of` and against the bytes the heap actually holds,
//! counted by a global allocator. One `#[test]` so no other test allocates
//! while a count is taken.

use asap_sketchlib::{
    DataInput, FrequencyAlgorithm, FrequencyIdentity, HHHeap, HHItem, WeightedFrequency,
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

fn int_identity(i: usize) -> Vec<FrequencyIdentity> {
    vec![FrequencyIdentity::Int64(i as i64)]
}

fn text_identity(i: usize) -> Vec<FrequencyIdentity> {
    vec![
        FrequencyIdentity::Utf8(format!("{i:016}")),
        FrequencyIdentity::Bool(i % 2 == 0),
    ]
}

#[test]
fn estimated_memory_bytes_match_size_of_and_the_allocation() {
    // size_of: a resident is one `HHItem` in the heap array, one `u64` digest
    // and one index entry; a heap of capacity 0 allocates nothing.
    assert_eq!(
        HHHeap::estimated_memory_bytes(0, &DataInput::U64(0)),
        std::mem::size_of::<HHHeap>()
    );
    assert!(
        HHHeap::estimated_memory_bytes(1024, &DataInput::U64(0))
            > std::mem::size_of::<HHHeap>() + 1024 * (std::mem::size_of::<HHItem>() + 8)
    );
    assert_eq!(
        WeightedFrequency::estimated_memory_bytes(64, 3, 0, &int_identity(0)),
        std::mem::size_of::<WeightedFrequency>() + 64 * 3 * 8
    );

    // HHHeap filled with `k` distinct keys (no eviction), for capacities on
    // both sides of the 1,024-slot up-front reservation.
    for k in [1usize, 3, 7, 8, 100, 1024, 2000, 5000] {
        let (_heap, bytes) = measured_bytes(|| {
            let mut heap = HHHeap::new(k);
            for i in 0..k as u64 {
                heap.update(&DataInput::U64(i), i as i64 + 1);
            }
            heap
        });
        assert_eq!(
            bytes,
            HHHeap::estimated_memory_bytes(k, &DataInput::U64(0)),
            "u64 keys, k = {k}"
        );

        let (_heap, bytes) = measured_bytes(|| {
            let mut heap = HHHeap::new(k);
            for i in 0..k as u64 {
                let key = [&[3u8][..], &i.to_le_bytes()].concat();
                heap.update(&DataInput::Bytes(&key), i as i64 + 1);
            }
            heap
        });
        assert_eq!(
            bytes,
            HHHeap::estimated_memory_bytes(k, &DataInput::Bytes(&[0; 9])),
            "9-byte keys, k = {k}"
        );
    }

    // WeightedFrequency after four times `capacity` distinct identities, so the
    // candidate heap is full and has turned keys away.
    for capacity in [1usize, 7, 64, 100, 1000] {
        for (name, identity) in [
            ("Int64", int_identity as fn(usize) -> Vec<FrequencyIdentity>),
            ("Utf8 + Bool", text_identity),
        ] {
            let (_state, bytes) = measured_bytes(|| {
                let mut state =
                    WeightedFrequency::new(FrequencyAlgorithm::Cms, 64, 3, capacity).unwrap();
                for i in 0..4 * capacity {
                    state.update(&identity(i), 1.0).unwrap();
                }
                state
            });
            assert_eq!(
                bytes,
                WeightedFrequency::estimated_memory_bytes(64, 3, capacity, &identity(0)),
                "{name} identities, capacity = {capacity}"
            );
        }
    }
}
