mod common;

use common::specs::{CountMinSpec, CountSketchSpec, Tally};
use common::{FreqTruth, zipf_u64};

use asap_sketchlib::{CMSHeap, CSHeap, DataInput, HeapItem, RegularPath, Vector2D};

/// The two heap-backed sketches share heap bookkeeping but not an error
/// metric: `CMSHeap` carries Count-Min's one-sided additive bound and
/// `CSHeap` carries Count Sketch's L2 bound. Both must keep every heap entry
/// equal to what the sketch itself estimates, and both must recover the true
/// heavy hitters that the top-k contract actually guarantees.
#[test]
fn heaps_satisfy_their_own_bounds_and_stay_heap_consistent() {
    const TOP_K: usize = 16;
    const DOMAIN: usize = 1024;
    const CM_ROWS: usize = 3;
    const CS_ROWS: usize = 5;
    const COLS: usize = 4096;
    const STREAM_SEED: u64 = 1004;

    let mut cms_heap = CMSHeap::<Vector2D<i64>, RegularPath>::new(CM_ROWS, COLS, TOP_K);
    let mut cs_heap = CSHeap::<Vector2D<i64>, RegularPath>::new(CS_ROWS, COLS, TOP_K);
    let stream = zipf_u64(20_000, DOMAIN, 1.1, STREAM_SEED);
    let mut truth = FreqTruth::default();
    for k in &stream {
        let d = DataInput::I64(*k as i64);
        truth.observe(*k as i64);
        cms_heap.insert(&d);
        cs_heap.insert(&d);
    }
    let context = format!("zipf(1.1) domain={DOMAIN} n=20000 stream_seed={STREAM_SEED}");

    // Each sketch against its own theorem.
    CountMinSpec::new(CM_ROWS, COLS).assert_contract(
        "CMSHeap point estimate",
        &truth,
        |k| cms_heap.estimate(&DataInput::I64(k)) as f64,
        &context,
    );
    CountSketchSpec::new(CS_ROWS, COLS).assert_contract(
        "CSHeap point estimate",
        &truth,
        |k| cs_heap.estimate(&DataInput::I64(k)),
        &context,
    );

    // Heap/sketch consistency is structural: a heap entry that disagrees with
    // the sketch is a bug regardless of any error bound.
    let kth_count = truth.top_k(TOP_K)[TOP_K - 1].1;
    for (label, items, cms) in [
        ("CMSHeap", cms_heap.heap().heap().to_vec(), true),
        ("CSHeap", cs_heap.heap().heap().to_vec(), false),
    ] {
        assert!(items.len() <= TOP_K, "{label} heap exceeded capacity");
        let mut consistency = Tally::default();
        let mut recall = 0usize;
        for it in &items {
            let key = match &it.key {
                HeapItem::I64(v) => *v,
                other => panic!("{label}: unexpected heap key {other:?}"),
            };
            let est = if cms {
                cms_heap.estimate(&DataInput::I64(key))
            } else {
                cs_heap.estimate(&DataInput::I64(key)) as i64
            };
            consistency.record(it.count == est, || {
                format!(
                    "key {key}: heap holds {} but the sketch estimates {est}",
                    it.count
                )
            });
            if truth.get(key) >= kth_count {
                recall += 1;
            }
        }
        consistency.assert_none(&format!("{label} heap/sketch consistency"), &context);

        // Recall target: a key whose true count is at least the true k-th
        // count must be admitted. Count-Min never underestimates, so every
        // such key's estimate dominates the heap minimum once seen; one
        // displacement slot of slack covers the eviction race at the
        // boundary, where several keys share the k-th count.
        assert!(
            recall >= TOP_K - 1,
            "{label} recovered only {recall}/{TOP_K} keys at or above the true k-th count \
             ({kth_count}); {context}"
        );
    }
}
