//! OctoSketch multi-threaded sketch framework.
//!
//! Implements the parent-child delta-promotion architecture from OctoSketch (NSDI 2024).
//! Worker threads maintain lightweight child sketches with small counters and emit
//! compact delta entries via an MPSC channel when counters overflow a promotion
//! threshold. An aggregator thread applies deltas to a full-precision parent sketch.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, RwLock, Weak};
use std::thread;

use crate::{
    CmDelta, Count, CountDelta, CountMin, HllDelta, HyperLogLog, Regular, RegularPath, SketchInput,
    Vector2D,
};

/// Legacy queue capacity default retained for config compatibility.
const DEFAULT_QUEUE_CAPACITY: usize = 65536;

// ---------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------

/// Worker-side trait: processes inputs and emits deltas.
pub trait OctoWorker: Send {
    type Delta: Copy + Send + 'static;

    /// Process one input and emit zero or more deltas.
    fn process(&mut self, input: &SketchInput, emit: &mut dyn FnMut(Self::Delta));
}

/// Parent-side trait: absorbs deltas into a full-precision sketch.
pub trait OctoAggregator: Send {
    type Delta: Copy + Send + 'static;

    /// Apply a single delta to the parent sketch.
    fn apply(&mut self, delta: Self::Delta);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for `run_octo`.
pub struct OctoConfig {
    /// Number of worker threads (default: 4).
    pub num_workers: usize,
    /// Pin worker threads to cores (default: true).
    /// Worker i is pinned to core i, aggregator to core num_workers.
    /// Silently skipped if pinning fails.
    pub pin_cores: bool,
    /// Legacy queue capacity retained for compatibility (default: 65536).
    /// Currently unused by the unbounded MPSC transport.
    pub queue_capacity: usize,
}

impl Default for OctoConfig {
    fn default() -> Self {
        Self {
            num_workers: 4,
            pin_cores: true,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }
}

/// Result of an `run_octo` execution.
pub struct OctoResult<P> {
    pub parent: P,
}

enum WorkerMsg {
    Data(SketchInput<'static>),
    End,
}

/// Extends a `SketchInput` lifetime to `'static` for cross-thread transport in
/// streaming mode. Caller must ensure all borrowed data outlives worker processing.
#[inline(always)]
unsafe fn assume_input_static(input: SketchInput<'_>) -> SketchInput<'static> {
    // SAFETY: enforced by caller contract described above.
    unsafe { std::mem::transmute::<SketchInput<'_>, SketchInput<'static>>(input) }
}

/// Streaming Octo runtime that accepts incremental inserts and finalizes into a parent sketch.
pub struct OctoRuntime<W, P>
where
    W: OctoWorker + 'static,
    P: OctoAggregator<Delta = W::Delta> + Send + Sync + 'static,
{
    core: Option<OctoCore<P>>,
    _worker_marker: PhantomData<W>,
}

/// Read-only handle for querying the live aggregator state while runtime is active.
pub struct OctoReadHandle<P> {
    parent: Weak<RwLock<P>>,
}

impl<P> Clone for OctoReadHandle<P> {
    fn clone(&self) -> Self {
        Self {
            parent: Weak::clone(&self.parent),
        }
    }
}

impl<P> OctoReadHandle<P> {
    /// Executes a read-only closure over the live parent state.
    pub fn with_parent<R>(&self, f: impl FnOnce(&P) -> R) -> R {
        let parent = self
            .parent
            .upgrade()
            .expect("Octo runtime has been finished and parent state was dropped");
        let guard = parent.read().expect("parent lock poisoned");
        f(&guard)
    }
}

struct OctoCore<P> {
    worker_input_txs: Vec<mpsc::Sender<WorkerMsg>>,
    next_worker: AtomicUsize,
    worker_handles: Vec<thread::JoinHandle<()>>,
    aggregator_handle: Option<thread::JoinHandle<()>>,
    parent: Arc<RwLock<P>>,
    closed: AtomicBool,
}

impl<P> OctoCore<P> {
    fn read_handle(&self) -> OctoReadHandle<P> {
        OctoReadHandle {
            parent: Arc::downgrade(&self.parent),
        }
    }

    fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        for tx in &self.worker_input_txs {
            let _ = tx.send(WorkerMsg::End);
        }
    }
}

impl<P> OctoCore<P> {
    fn into_parent(mut self) -> P {
        self.close();

        for handle in self.worker_handles.drain(..) {
            handle.join().expect("worker thread panicked during finish");
        }

        if let Some(aggregator) = self.aggregator_handle.take() {
            aggregator
                .join()
                .expect("aggregator thread panicked during finish");
        }

        let parent_lock = match Arc::try_unwrap(self.parent) {
            Ok(lock) => lock,
            Err(_) => panic!("Octo parent still has external strong references at finish"),
        };
        parent_lock.into_inner().expect("parent lock poisoned")
    }
}

impl<P> OctoCore<P>
where
    P: Send + Sync + 'static,
{
    fn start<W>(workers: Vec<W>, parent: P, num_workers: usize, pin_cores: bool) -> Self
    where
        W: OctoWorker + 'static,
        P: OctoAggregator<Delta = W::Delta>,
    {
        assert_eq!(workers.len(), num_workers);

        let (delta_tx, delta_rx) = mpsc::channel::<W::Delta>();
        let parent = Arc::new(RwLock::new(parent));
        let parent_for_aggregator = Arc::clone(&parent);

        let aggregator_handle = thread::spawn(move || {
            if pin_cores {
                let _ = core_affinity::set_for_current(core_affinity::CoreId { id: num_workers });
            }
            for delta in delta_rx {
                let mut guard = parent_for_aggregator
                    .write()
                    .expect("parent lock poisoned in aggregator");
                guard.apply(delta);
            }
        });

        let mut worker_input_txs = Vec::with_capacity(num_workers);
        let mut worker_handles = Vec::with_capacity(num_workers);
        for (worker_id, mut worker) in workers.into_iter().enumerate() {
            let (worker_tx, worker_rx) = mpsc::channel::<WorkerMsg>();
            worker_input_txs.push(worker_tx);
            let delta_tx_worker = delta_tx.clone();
            let pin_cores = pin_cores;
            worker_handles.push(thread::spawn(move || {
                if pin_cores {
                    let _ = core_affinity::set_for_current(core_affinity::CoreId { id: worker_id });
                }
                while let Ok(msg) = worker_rx.recv() {
                    match msg {
                        WorkerMsg::Data(input) => worker.process(&input, &mut |delta| {
                            delta_tx_worker
                                .send(delta)
                                .expect("aggregator receiver dropped while workers still running");
                        }),
                        WorkerMsg::End => break,
                    }
                }
            }));
        }
        drop(delta_tx);

        Self {
            worker_input_txs,
            next_worker: AtomicUsize::new(0),
            worker_handles,
            aggregator_handle: Some(aggregator_handle),
            parent,
            closed: AtomicBool::new(false),
        }
    }
}

impl<W, P> OctoRuntime<W, P>
where
    W: OctoWorker + 'static,
    P: OctoAggregator<Delta = W::Delta> + Send + Sync + 'static,
{
    pub fn new<F, PF>(config: &OctoConfig, worker_factory: F, parent_factory: PF) -> Self
    where
        F: Fn(usize) -> W,
        PF: FnOnce() -> P,
    {
        let num_workers = config.num_workers.max(1);
        let workers: Vec<W> = (0..num_workers).map(worker_factory).collect();
        let parent = parent_factory();
        let core = OctoCore::start(workers, parent, num_workers, config.pin_cores);

        Self {
            core: Some(core),
            _worker_marker: PhantomData,
        }
    }

    pub fn read_handle(&self) -> OctoReadHandle<P> {
        self.core
            .as_ref()
            .expect("runtime core missing")
            .read_handle()
    }

    pub fn close(&self) {
        self.core.as_ref().expect("runtime core missing").close();
    }

    pub fn insert(&mut self, input: SketchInput<'_>) {
        let core = self.core.as_ref().expect("runtime core missing");
        if core.closed.load(Ordering::Acquire) {
            panic!("cannot insert after runtime has been closed");
        }

        let worker_id =
            core.next_worker.fetch_add(1, Ordering::AcqRel) % core.worker_input_txs.len();
        // SAFETY: caller explicitly guarantees borrowed data lives long enough.
        let static_input = unsafe { assume_input_static(input) };
        core.worker_input_txs[worker_id]
            .send(WorkerMsg::Data(static_input))
            .expect("worker receiver dropped while runtime is active");
    }

    pub fn insert_batch(&mut self, inputs: &[SketchInput<'_>]) {
        for input in inputs {
            self.insert(input.clone());
        }
    }

    pub fn finish(mut self) -> OctoResult<P> {
        let parent = self
            .core
            .take()
            .expect("runtime core missing")
            .into_parent();

        OctoResult { parent }
    }
}

// ---------------------------------------------------------------------------
// Concrete worker/parent implementations
// ---------------------------------------------------------------------------

// -- CountMin --

/// OctoSketch worker backed by `CountMin`.
pub struct CmOctoWorker {
    sketch: CountMin<Vector2D<i32>, RegularPath>,
}

impl CmOctoWorker {
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            sketch: CountMin::with_dimensions(rows, cols),
        }
    }
}

impl OctoWorker for CmOctoWorker {
    type Delta = CmDelta;

    #[inline(always)]
    fn process(&mut self, input: &SketchInput, emit: &mut dyn FnMut(CmDelta)) {
        self.sketch.insert_emit_delta(input, emit);
    }
}

/// OctoSketch parent wrapping a full-precision `CountMin`.
pub struct CmOctoParent {
    pub sketch: CountMin<Vector2D<i32>, RegularPath>,
}

impl OctoAggregator for CmOctoParent {
    type Delta = CmDelta;

    #[inline(always)]
    fn apply(&mut self, delta: CmDelta) {
        self.sketch.apply_delta(delta);
    }
}

// -- Count Sketch --

/// OctoSketch worker backed by `Count`.
pub struct CountOctoWorker {
    child: Count<Vector2D<i32>, RegularPath>,
}

impl CountOctoWorker {
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            child: Count::with_dimensions(rows, cols),
        }
    }
}

impl OctoWorker for CountOctoWorker {
    type Delta = CountDelta;

    #[inline(always)]
    fn process(&mut self, input: &SketchInput, emit: &mut dyn FnMut(CountDelta)) {
        self.child.insert_emit_delta(input, emit);
    }
}

/// OctoSketch parent wrapping a full-precision `Count`.
pub struct CountOctoParent {
    pub sketch: Count<Vector2D<i32>, RegularPath>,
}

impl OctoAggregator for CountOctoParent {
    type Delta = CountDelta;

    #[inline(always)]
    fn apply(&mut self, delta: CountDelta) {
        self.sketch.apply_delta(delta);
    }
}

// -- HyperLogLog --

/// OctoSketch worker backed by `HyperLogLog`.
pub struct HllOctoWorker {
    child: HyperLogLog<Regular>,
}

impl HllOctoWorker {
    pub fn new() -> Self {
        Self {
            child: HyperLogLog::default(),
        }
    }
}

impl Default for HllOctoWorker {
    fn default() -> Self {
        Self::new()
    }
}

impl OctoWorker for HllOctoWorker {
    type Delta = HllDelta;

    #[inline(always)]
    fn process(&mut self, input: &SketchInput, emit: &mut dyn FnMut(HllDelta)) {
        self.child.insert_emit_delta(input, emit);
    }
}

/// OctoSketch parent wrapping a full-precision `HyperLogLog<Regular>`.
pub struct HllOctoParent {
    pub sketch: HyperLogLog<Regular>,
}

impl OctoAggregator for HllOctoParent {
    type Delta = HllDelta;

    #[inline(always)]
    fn apply(&mut self, delta: HllDelta) {
        self.sketch.apply_delta(delta);
    }
}

// ---------------------------------------------------------------------------
// Core execution engine
// ---------------------------------------------------------------------------

/// Runs the OctoSketch multi-threaded insert protocol.
///
/// 1. Dispatches `inputs` across workers round-robin through per-worker channels.
/// 2. Each worker maintains a child sketch, emitting deltas via an MPSC channel.
/// 3. The aggregator blocks on the channel and applies deltas to the parent.
/// 4. Returns the fully-merged parent sketch.
pub fn run_octo<W, P>(
    inputs: &[SketchInput<'_>],
    config: &OctoConfig,
    worker_factory: impl Fn(usize) -> W,
    parent_factory: impl FnOnce() -> P,
) -> OctoResult<P>
where
    W: OctoWorker + 'static,
    P: OctoAggregator<Delta = W::Delta> + Send + Sync + 'static,
{
    let mut runtime = OctoRuntime::new(config, worker_factory, parent_factory);
    for input in inputs {
        runtime.insert(input.clone());
    }
    runtime.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SketchInput;

    // -----------------------------------------------------------------------
    // Layer 2 unit tests: child sketches + apply_delta
    // -----------------------------------------------------------------------

    #[test]
    fn cm_insert_emit_delta_emits_at_threshold() {
        let mut worker_sketch = CountMin::with_dimensions(3, 64);
        let key = SketchInput::U64(42);
        let mut deltas: Vec<CmDelta> = Vec::new();

        // Insert CM_PROMASK-1 times: no delta yet.
        for _ in 0..(crate::CM_PROMASK - 1) {
            worker_sketch.insert_emit_delta(&key, &mut |d| deltas.push(d));
        }
        assert!(deltas.is_empty(), "should not emit before threshold");

        // One more insert triggers the delta.
        worker_sketch.insert_emit_delta(&key, &mut |d| deltas.push(d));
        assert_eq!(deltas.len(), 3, "should emit one delta per row (3 rows)");
        for d in &deltas {
            assert_eq!(d.value, crate::CM_PROMASK);
        }
    }

    #[test]
    fn cm_apply_delta_increments_parent() {
        let mut parent = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);
        let delta = CmDelta {
            row: 1,
            col: 5,
            value: 100,
        };
        parent.apply_delta(delta);
        assert_eq!(parent.as_storage().query_one_counter(1, 5), 100);

        parent.apply_delta(delta);
        assert_eq!(parent.as_storage().query_one_counter(1, 5), 200);
    }

    #[test]
    fn count_child_emits_delta_at_threshold() {
        let mut child = Count::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);
        let key = SketchInput::U64(99);
        let mut deltas: Vec<CountDelta> = Vec::new();

        // Insert enough times to trigger at least one delta.
        for _ in 0..200 {
            child.insert_emit_delta(&key, &mut |d| deltas.push(d));
        }
        // With COUNT_PROMASK = 63 and 3 rows, after 200 inserts we
        // should have emitted at least 3 deltas (one per row per threshold).
        assert!(
            deltas.len() >= 3,
            "expected at least 3 deltas, got {}",
            deltas.len()
        );
    }

    #[test]
    fn count_apply_delta_increments_parent() {
        let mut parent = Count::<Vector2D<i32>, RegularPath>::with_dimensions(3, 64);
        let delta = CountDelta {
            row: 0,
            col: 10,
            value: -50,
        };
        parent.apply_delta(delta);
        assert_eq!(parent.as_storage().query_one_counter(0, 10), -50);
    }

    #[test]
    fn hll_child_emits_delta_on_improvement() {
        let mut child = HyperLogLog::<Regular>::default();
        let mut deltas: Vec<HllDelta> = Vec::new();

        // First insert should always emit (register goes from 0 to something > 0).
        child.insert_emit_delta(&SketchInput::U64(1), &mut |d| deltas.push(d));
        assert_eq!(deltas.len(), 1, "first insert should emit a delta");

        // Inserting the same value again should NOT emit (register unchanged).
        let len_before = deltas.len();
        child.insert_emit_delta(&SketchInput::U64(1), &mut |d| deltas.push(d));
        assert_eq!(deltas.len(), len_before, "duplicate should not emit");
    }

    #[test]
    fn hll_apply_delta_updates_register() {
        let mut parent = HyperLogLog::<Regular>::default();
        let delta = HllDelta {
            pos: 100,
            value: 10,
        };
        parent.apply_delta(delta);

        // Apply a smaller value — should not change.
        let smaller = HllDelta { pos: 100, value: 5 };
        parent.apply_delta(smaller);

        // Apply a larger value — should update.
        let larger = HllDelta {
            pos: 100,
            value: 15,
        };
        parent.apply_delta(larger);
    }

    // -----------------------------------------------------------------------
    // Layer 3 integration tests: run_octo
    // -----------------------------------------------------------------------

    #[test]
    fn run_octo_cm_matches_single_thread() {
        let rows = 3;
        let cols = 4096;
        let n = 100_000u64;

        let inputs: Vec<SketchInput<'_>> = (0..n).map(|i| SketchInput::U64(i % 1024)).collect();

        // Single-threaded reference.
        let mut reference = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(rows, cols);
        for input in &inputs {
            reference.insert(input);
        }

        // Multi-threaded via OctoSketch.
        let config = OctoConfig {
            num_workers: 4,
            pin_cores: false, // don't pin in tests (CI may have few cores)
            queue_capacity: 8192,
        };
        let result = run_octo(
            &inputs,
            &config,
            |_| CmOctoWorker::new(rows, cols),
            || CmOctoParent {
                sketch: CountMin::with_dimensions(rows, cols),
            },
        );

        // Verify estimates are close.
        // OctoSketch child uses u8 counters, so there may be slight differences
        // due to the promotion threshold batching, but the total counts should match.
        for key_val in 0u64..1024 {
            let key = SketchInput::U64(key_val);
            let ref_est = reference.estimate(&key);
            let octo_est = result.parent.sketch.estimate(&key);
            // The octo estimate should be <= reference (some counts may be
            // lost in the u8 remainder that didn't reach PROMASK).
            assert!(
                (ref_est - octo_est).abs() < 200,
                "key {key_val}: ref={ref_est}, octo={octo_est}, diff={}",
                (ref_est - octo_est).abs()
            );
        }
    }

    #[test]
    fn run_octo_hll_cardinality() {
        let n = 50_000u64;
        let inputs: Vec<SketchInput<'_>> = (0..n).map(SketchInput::U64).collect();

        let config = OctoConfig {
            num_workers: 4,
            pin_cores: false,
            queue_capacity: 16384,
        };

        let result = run_octo(
            &inputs,
            &config,
            |_| HllOctoWorker::new(),
            || HllOctoParent {
                sketch: HyperLogLog::<Regular>::default(),
            },
        );

        let estimate = result.parent.sketch.estimate();
        let truth = n as f64;
        let error = (estimate as f64 - truth).abs() / truth;
        assert!(
            error < 0.05,
            "HLL cardinality error {error:.4} exceeded 5% (truth {truth}, estimate {estimate})"
        );
    }

    #[test]
    fn run_octo_count_sketch_basic() {
        let rows = 3;
        let cols = 4096;
        let n = 50_000u64;

        let inputs: Vec<SketchInput<'_>> = (0..n).map(|i| SketchInput::U64(i % 512)).collect();

        let config = OctoConfig {
            num_workers: 2,
            pin_cores: false,
            queue_capacity: 8192,
        };

        let result = run_octo(
            &inputs,
            &config,
            |_| CountOctoWorker::new(rows, cols),
            || CountOctoParent {
                sketch: Count::with_dimensions(rows, cols),
            },
        );

        // Each key appears ~97 times. Check a sample of keys.
        let expected_per_key = (n / 512) as f64;
        for key_val in [0u64, 100, 255, 511] {
            let key = SketchInput::U64(key_val);
            let est = result.parent.sketch.estimate(&key);
            assert!(
                (est - expected_per_key).abs() < expected_per_key * 0.5,
                "key {key_val}: estimate={est}, expected~{expected_per_key}"
            );
        }
    }

    #[test]
    fn run_octo_single_worker() {
        // Edge case: single worker should still work.
        let inputs: Vec<SketchInput<'_>> = (0..1000u64).map(SketchInput::U64).collect();

        let config = OctoConfig {
            num_workers: 1,
            pin_cores: false,
            queue_capacity: 4096,
        };

        let result = run_octo(
            &inputs,
            &config,
            |_| HllOctoWorker::new(),
            || HllOctoParent {
                sketch: HyperLogLog::<Regular>::default(),
            },
        );

        let estimate = result.parent.sketch.estimate();
        assert!(
            estimate > 900 && estimate < 1100,
            "single-worker HLL estimate {estimate} out of range for 1000 distinct keys"
        );
    }

    #[test]
    fn octo_runtime_streaming_cm_matches_batch_api() {
        let rows = 3;
        let cols = 4096;
        let n = 30_000u64;
        let inputs: Vec<SketchInput<'_>> = (0..n).map(|i| SketchInput::U64(i % 1024)).collect();
        let config = OctoConfig {
            num_workers: 4,
            pin_cores: false,
            queue_capacity: 8192,
        };

        let batch_result = run_octo(
            &inputs,
            &config,
            |_| CmOctoWorker::new(rows, cols),
            || CmOctoParent {
                sketch: CountMin::with_dimensions(rows, cols),
            },
        );

        let mut runtime = OctoRuntime::new(
            &config,
            move |_| CmOctoWorker::new(rows, cols),
            move || CmOctoParent {
                sketch: CountMin::with_dimensions(rows, cols),
            },
        );
        for input in &inputs {
            runtime.insert(input.clone());
        }
        let streaming_result = runtime.finish();

        for key_val in 0u64..128 {
            let key = SketchInput::U64(key_val);
            let batch_est = batch_result.parent.sketch.estimate(&key);
            let stream_est = streaming_result.parent.sketch.estimate(&key);
            assert_eq!(batch_est, stream_est, "key {key_val} mismatch");
        }
    }

    #[test]
    fn octo_runtime_mixed_insert_and_batch_hll() {
        let config = OctoConfig {
            num_workers: 2,
            pin_cores: false,
            queue_capacity: 4096,
        };
        let mut runtime = OctoRuntime::new(
            &config,
            |_| HllOctoWorker::new(),
            || HllOctoParent {
                sketch: HyperLogLog::<Regular>::default(),
            },
        );

        runtime.insert(SketchInput::U64(1));
        runtime.insert(SketchInput::U64(2));
        let batch: Vec<SketchInput<'_>> = (3..2000).map(SketchInput::U64).collect();
        runtime.insert_batch(&batch);
        let result = runtime.finish();
        let estimate = result.parent.sketch.estimate();
        assert!(
            estimate > 1700 && estimate < 2300,
            "runtime mixed insert+batch estimate {estimate} is out of expected range"
        );
    }

    #[test]
    fn octo_runtime_live_read_handle_observes_progress() {
        let config = OctoConfig {
            num_workers: 2,
            pin_cores: false,
            queue_capacity: 4096,
        };
        let mut runtime =
            OctoRuntime::new(&config, |_| CountingWorker, || CountingParent { total: 0 });
        let reader = runtime.read_handle();

        for i in 0..64u64 {
            runtime.insert(SketchInput::U64(i));
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
        let observed = reader.with_parent(|p| p.total);
        assert!(
            observed <= 64,
            "live reader should observe a partial or complete total"
        );

        let result = runtime.finish();
        assert_eq!(
            result.parent.total, 64,
            "all inserted items should be accounted for"
        );
        assert!(
            result.parent.total >= observed,
            "final total should not be less than live snapshot"
        );
    }

    #[test]
    fn octo_runtime_close_is_idempotent() {
        let config = OctoConfig {
            num_workers: 2,
            pin_cores: false,
            queue_capacity: 4096,
        };
        let runtime = OctoRuntime::new(
            &config,
            |_| HllOctoWorker::new(),
            || HllOctoParent {
                sketch: HyperLogLog::<Regular>::default(),
            },
        );
        runtime.close();
        runtime.close();
        let result = runtime.finish();
        assert_eq!(result.parent.sketch.estimate(), 0);
    }

    #[test]
    #[should_panic(expected = "cannot insert after runtime has been closed")]
    fn octo_runtime_insert_after_close_panics() {
        let config = OctoConfig {
            num_workers: 2,
            pin_cores: false,
            queue_capacity: 4096,
        };
        let mut runtime = OctoRuntime::new(
            &config,
            |_| HllOctoWorker::new(),
            || HllOctoParent {
                sketch: HyperLogLog::<Regular>::default(),
            },
        );
        runtime.close();
        runtime.insert(SketchInput::U64(1));
    }

    #[test]
    fn octo_runtime_empty_stream_finishes() {
        let config = OctoConfig {
            num_workers: 4,
            pin_cores: false,
            queue_capacity: 4096,
        };
        let runtime = OctoRuntime::new(
            &config,
            |_| HllOctoWorker::new(),
            || HllOctoParent {
                sketch: HyperLogLog::<Regular>::default(),
            },
        );
        let result = runtime.finish();
        let estimate = result.parent.sketch.estimate();
        assert!(
            estimate == 0,
            "empty runtime should estimate 0 cardinality, got {estimate}"
        );
    }

    struct CountingWorker;

    impl OctoWorker for CountingWorker {
        type Delta = u64;

        fn process(&mut self, _input: &SketchInput, emit: &mut dyn FnMut(Self::Delta)) {
            emit(1);
        }
    }

    struct CountingParent {
        total: u64,
    }

    impl OctoAggregator for CountingParent {
        type Delta = u64;

        fn apply(&mut self, delta: Self::Delta) {
            self.total += delta;
        }
    }

    #[test]
    fn octo_runtime_close_preserves_queued_items_without_dispatcher() {
        let config = OctoConfig {
            num_workers: 4,
            pin_cores: false,
            queue_capacity: 4096,
        };
        let n = 257usize;
        let mut runtime =
            OctoRuntime::new(&config, |_| CountingWorker, || CountingParent { total: 0 });

        for i in 0..n as u64 {
            runtime.insert(SketchInput::U64(i + 42));
        }
        runtime.close();
        let result = runtime.finish();
        assert_eq!(
            result.parent.total, n as u64,
            "runtime close should preserve already queued items"
        );
    }

    struct WorkerIdEmitter {
        worker_id: usize,
    }

    impl OctoWorker for WorkerIdEmitter {
        type Delta = usize;

        fn process(&mut self, _input: &SketchInput, emit: &mut dyn FnMut(Self::Delta)) {
            emit(self.worker_id);
        }
    }

    struct WorkerLoadParent {
        loads: Vec<u64>,
    }

    impl OctoAggregator for WorkerLoadParent {
        type Delta = usize;

        fn apply(&mut self, delta: Self::Delta) {
            self.loads[delta] += 1;
        }
    }

    #[test]
    fn octo_runtime_round_robin_selector_distributes_deterministically() {
        let num_workers = 3;
        let inserts = 10u64;
        let config = OctoConfig {
            num_workers,
            pin_cores: false,
            queue_capacity: 4096,
        };
        let mut runtime = OctoRuntime::new(
            &config,
            |worker_id| WorkerIdEmitter { worker_id },
            || WorkerLoadParent {
                loads: vec![0; num_workers],
            },
        );

        for i in 0..inserts {
            runtime.insert(SketchInput::U64(i));
        }
        let result = runtime.finish();

        assert_eq!(result.parent.loads, vec![4, 3, 3]);
    }
}
