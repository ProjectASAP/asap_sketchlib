//! Property tests for the two bounded heaps.
//!
//! `CommonHeap` is checked against the obvious model - sort the offered values
//! and keep the end the ordering asks for - and against the heap property of
//! its own array. `HHHeap` is path dependent once it overflows, so the exact
//! model is asserted only in the regimes where the path cannot matter.

use asap_sketchlib::common::{CommonHeap, KeepLargest, KeepSmallest};
use asap_sketchlib::{DataInput, HHHeap};
use proptest::prelude::*;
use std::collections::HashMap;

fn values(max: usize) -> impl Strategy<Value = Vec<i64>> {
    prop::collection::vec(-1_000i64..1_000, 0..max)
}

fn keyed(max: usize) -> impl Strategy<Value = Vec<(u64, i64)>> {
    prop::collection::vec((0u64..64, -1_000i64..1_000), 0..max)
}

/// A capacity paired with updates over a key domain no wider than it, so the
/// heap has room for everything offered.
fn roomy(max: usize) -> impl Strategy<Value = (usize, Vec<(u64, i64)>)> {
    (1usize..24).prop_flat_map(move |k| {
        (
            Just(k),
            prop::collection::vec((0u64..k as u64, -1_000i64..1_000), 0..max),
        )
    })
}

fn min_heap_of(capacity: usize, pushed: &[i64]) -> CommonHeap<i64, KeepSmallest> {
    let mut h = CommonHeap::new_min(capacity);
    for v in pushed {
        h.push(*v);
    }
    h
}

fn max_heap_of(capacity: usize, pushed: &[i64]) -> CommonHeap<i64, KeepLargest> {
    let mut h = CommonHeap::new_max(capacity);
    for v in pushed {
        h.push(*v);
    }
    h
}

fn multiset(values: &[i64]) -> Vec<i64> {
    let mut v = values.to_vec();
    v.sort_unstable();
    v
}

proptest! {
    // ===== The capacity-bounded binary heap =====

    #[test]
    fn a_bounded_heap_never_outgrows_its_capacity(
        capacity in 1usize..32,
        pushed in values(200),
    ) {
        let heap = min_heap_of(capacity, &pushed);

        prop_assert_eq!(heap.capacity(), capacity);
        prop_assert!(heap.len() <= heap.capacity(), "{} over {}", heap.len(), heap.capacity());
        prop_assert_eq!(heap.len(), pushed.len().min(capacity));
        prop_assert_eq!(heap.is_full(), heap.len() == heap.capacity());
    }

    #[test]
    fn a_min_heap_keeps_the_largest_values(
        capacity in 1usize..32,
        pushed in values(200),
    ) {
        let heap = min_heap_of(capacity, &pushed);

        let mut expected = multiset(&pushed);
        expected.reverse();
        expected.truncate(capacity);

        prop_assert_eq!(multiset(heap.as_slice()), multiset(&expected));
    }

    #[test]
    fn a_max_heap_keeps_the_smallest_values(
        capacity in 1usize..32,
        pushed in values(200),
    ) {
        let heap = max_heap_of(capacity, &pushed);

        let mut expected = multiset(&pushed);
        expected.truncate(capacity);

        prop_assert_eq!(multiset(heap.as_slice()), multiset(&expected));
    }

    #[test]
    fn the_root_is_the_extreme_of_what_is_retained(
        capacity in 1usize..32,
        pushed in values(200),
    ) {
        let low = min_heap_of(capacity, &pushed);
        let high = max_heap_of(capacity, &pushed);

        prop_assert_eq!(low.peek(), low.as_slice().iter().min());
        prop_assert_eq!(high.peek(), high.as_slice().iter().max());
    }

    #[test]
    fn the_array_satisfies_the_heap_property(
        capacity in 1usize..32,
        pushed in values(200),
    ) {
        let low = min_heap_of(capacity, &pushed);
        let high = max_heap_of(capacity, &pushed);

        let cells = low.as_slice();
        for child in 1..cells.len() {
            prop_assert!(
                cells[(child - 1) / 2] <= cells[child],
                "min-heap: parent {} above child {}", cells[(child - 1) / 2], cells[child]
            );
        }
        let cells = high.as_slice();
        for child in 1..cells.len() {
            prop_assert!(
                cells[(child - 1) / 2] >= cells[child],
                "max-heap: parent {} below child {}", cells[(child - 1) / 2], cells[child]
            );
        }
    }

    #[test]
    fn popping_drains_the_heap_in_order(
        capacity in 1usize..32,
        pushed in values(200),
    ) {
        let mut heap = min_heap_of(capacity, &pushed);
        let mut drained = Vec::new();
        while let Some(v) = heap.pop() {
            drained.push(v);
        }

        prop_assert!(drained.windows(2).all(|p| p[0] <= p[1]), "{:?} is out of order", drained);
        prop_assert_eq!(drained.len(), pushed.len().min(capacity));
        prop_assert!(heap.is_empty());
    }

    #[test]
    fn clearing_empties_the_heap(capacity in 1usize..32, pushed in values(200)) {
        let mut heap = min_heap_of(capacity, &pushed);
        heap.clear();

        prop_assert_eq!(heap.len(), 0);
        prop_assert!(heap.is_empty());
        prop_assert_eq!(heap.capacity(), capacity);
        prop_assert_eq!(heap.peek(), None);
    }

    // ===== The keyed heavy-hitter heap =====

    #[test]
    fn the_hitter_heap_never_outgrows_k(
        k in 1usize..24,
        updates in keyed(300),
    ) {
        let mut heap = HHHeap::new(k);
        for (key, count) in &updates {
            heap.update(&DataInput::U64(*key), *count);
            prop_assert!(heap.len() <= heap.capacity(), "{} over {}", heap.len(), heap.capacity());
        }

        prop_assert_eq!(heap.capacity(), k);
    }

    #[test]
    fn every_resident_carries_the_last_count_offered_for_its_key(
        k in 1usize..24,
        updates in keyed(300),
    ) {
        let mut heap = HHHeap::new(k);
        let mut latest: HashMap<u64, i64> = HashMap::new();
        for (key, count) in &updates {
            heap.update(&DataInput::U64(*key), *count);
            latest.insert(*key, *count);
        }

        for (key, count) in &latest {
            if let Some(idx) = heap.find(&DataInput::U64(*key)) {
                prop_assert_eq!(heap.heap()[idx].count, *count, "key {}", key);
            }
        }
    }

    #[test]
    fn nothing_is_turned_away_while_the_heap_has_room(
        (k, updates) in roomy(300),
    ) {
        let mut heap = HHHeap::new(k);
        let mut latest: HashMap<u64, i64> = HashMap::new();
        for (key, count) in &updates {
            heap.update(&DataInput::U64(*key), *count);
            latest.insert(*key, *count);
        }

        prop_assert_eq!(heap.len(), latest.len());
        for (key, count) in &latest {
            let idx = heap.find(&DataInput::U64(*key)).expect("a key the heap had room for");
            prop_assert_eq!(heap.heap()[idx].count, *count, "key {}", key);
        }
    }

    #[test]
    fn one_offer_per_key_leaves_the_k_largest(
        k in 1usize..24,
        updates in keyed(300),
    ) {
        let mut once: Vec<(u64, i64)> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (key, count) in &updates {
            if seen.insert(*key) {
                once.push((*key, *count));
            }
        }

        let mut heap = HHHeap::new(k);
        for (key, count) in &once {
            heap.update(&DataInput::U64(*key), *count);
        }

        let mut expected: Vec<i64> = once.iter().map(|(_, c)| *c).collect();
        expected.sort_unstable();
        expected.reverse();
        expected.truncate(k);

        let held: Vec<i64> = heap.heap().iter().map(|item| item.count).collect();
        prop_assert_eq!(multiset(&held), multiset(&expected));
    }

    /// The documented meaning of the return value: the key took its place
    /// without displacing another.
    #[test]
    fn an_update_reports_whether_it_displaced_anything(
        k in 1usize..24,
        updates in keyed(300),
    ) {
        let mut heap = HHHeap::new(k);
        for (key, count) in &updates {
            let probe = DataInput::U64(*key);
            let resident = heap.find(&probe).is_some();
            let had_room = heap.len() < heap.capacity();

            prop_assert_eq!(
                heap.update(&probe, *count),
                resident || had_room,
                "key {}: resident {} room {}", key, resident, had_room
            );
        }
    }
}
