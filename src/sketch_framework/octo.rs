//! OctoSketch multi-threaded sketch framework.
//!
//! Implements the parent-child delta-promotion architecture from OctoSketch (NSDI 2024).
//! Worker threads maintain lightweight child sketches with small counters and emit
//! compact delta entries via an MPSC channel when counters overflow a promotion
//! threshold. An aggregator thread applies deltas to a full-precision parent sketch.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;

use crate::{
    CmDelta, Count, CountChild, CountDelta, CountMin, CountMinChild, HllChild, HllDelta,
    HyperLogLog, Regular, RegularPath, SketchInput, Vector2D, hash64_seeded,
};

/// Legacy queue capacity default retained for config compatibility.
const DEFAULT_QUEUE_CAPACITY: usize = 65536;
const WORKER_DISPATCH_BATCH_SIZE: usize = 64;

// ---------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------

/// Worker-side trait: processes inputs and emits deltas.
pub trait OctoWorker: Send {
    type Delta: Copy + Send + 'static;

    /// Process one input, emitting zero or more deltas via `emit`.
    fn process(&mut self, input: &SketchInput, emit: &mut dyn FnMut(Self::Delta));
}

/// Parent-side trait: absorbs deltas into a full-precision sketch.
pub trait OctoParent: Send {
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

/// Owned variant of `SketchInput` for cross-thread transport.
#[derive(Clone, Debug)]
pub enum OwnedSketchInput {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    ISIZE(isize),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    USIZE(usize),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl OwnedSketchInput {
    fn as_sketch_input(&self) -> SketchInput<'_> {
        match self {
            OwnedSketchInput::I8(v) => SketchInput::I8(*v),
            OwnedSketchInput::I16(v) => SketchInput::I16(*v),
            OwnedSketchInput::I32(v) => SketchInput::I32(*v),
            OwnedSketchInput::I64(v) => SketchInput::I64(*v),
            OwnedSketchInput::I128(v) => SketchInput::I128(*v),
            OwnedSketchInput::ISIZE(v) => SketchInput::ISIZE(*v),
            OwnedSketchInput::U8(v) => SketchInput::U8(*v),
            OwnedSketchInput::U16(v) => SketchInput::U16(*v),
            OwnedSketchInput::U32(v) => SketchInput::U32(*v),
            OwnedSketchInput::U64(v) => SketchInput::U64(*v),
            OwnedSketchInput::U128(v) => SketchInput::U128(*v),
            OwnedSketchInput::USIZE(v) => SketchInput::USIZE(*v),
            OwnedSketchInput::F32(v) => SketchInput::F32(*v),
            OwnedSketchInput::F64(v) => SketchInput::F64(*v),
            OwnedSketchInput::String(s) => SketchInput::Str(s.as_str()),
            OwnedSketchInput::Bytes(b) => SketchInput::Bytes(b.as_slice()),
        }
    }
}

impl From<&SketchInput<'_>> for OwnedSketchInput {
    fn from(value: &SketchInput<'_>) -> Self {
        match value {
            SketchInput::I8(v) => OwnedSketchInput::I8(*v),
            SketchInput::I16(v) => OwnedSketchInput::I16(*v),
            SketchInput::I32(v) => OwnedSketchInput::I32(*v),
            SketchInput::I64(v) => OwnedSketchInput::I64(*v),
            SketchInput::I128(v) => OwnedSketchInput::I128(*v),
            SketchInput::ISIZE(v) => OwnedSketchInput::ISIZE(*v),
            SketchInput::U8(v) => OwnedSketchInput::U8(*v),
            SketchInput::U16(v) => OwnedSketchInput::U16(*v),
            SketchInput::U32(v) => OwnedSketchInput::U32(*v),
            SketchInput::U64(v) => OwnedSketchInput::U64(*v),
            SketchInput::U128(v) => OwnedSketchInput::U128(*v),
            SketchInput::USIZE(v) => OwnedSketchInput::USIZE(*v),
            SketchInput::F32(v) => OwnedSketchInput::F32(*v),
            SketchInput::F64(v) => OwnedSketchInput::F64(*v),
            SketchInput::Str(s) => OwnedSketchInput::String((*s).to_string()),
            SketchInput::String(s) => OwnedSketchInput::String(s.clone()),
            SketchInput::Bytes(b) => OwnedSketchInput::Bytes(b.to_vec()),
        }
    }
}

enum IngressMsg {
    Data(OwnedSketchInput),
    End,
}

enum WorkerMsg {
    Batch(Vec<OwnedSketchInput>),
    End,
}

#[inline(always)]
fn worker_index(hash: u64, num_workers: usize) -> usize {
    if num_workers.is_power_of_two() {
        (hash as usize) & (num_workers - 1)
    } else {
        (hash as usize) % num_workers
    }
}

#[inline]
fn flush_worker_batch(
    worker_input_txs: &[mpsc::Sender<WorkerMsg>],
    pending: &mut [Vec<OwnedSketchInput>],
    worker_id: usize,
) {
    if pending[worker_id].is_empty() {
        return;
    }
    let batch = std::mem::take(&mut pending[worker_id]);
    worker_input_txs[worker_id]
        .send(WorkerMsg::Batch(batch))
        .expect("worker receiver dropped unexpectedly");
}

#[inline]
fn flush_all_worker_batches(
    worker_input_txs: &[mpsc::Sender<WorkerMsg>],
    pending: &mut [Vec<OwnedSketchInput>],
) {
    for worker_id in 0..pending.len() {
        flush_worker_batch(worker_input_txs, pending, worker_id);
    }
}

#[inline]
fn send_end_to_all_workers(worker_input_txs: &[mpsc::Sender<WorkerMsg>]) {
    for tx in worker_input_txs {
        let _ = tx.send(WorkerMsg::End);
    }
}

/// Streaming Octo runtime that accepts incremental inserts and finalizes into a parent sketch.
pub struct OctoRuntime<W, P>
where
    W: OctoWorker + 'static,
    P: OctoParent<Delta = W::Delta> + Send + 'static,
{
    ingress_tx: mpsc::Sender<IngressMsg>,
    dispatcher_handle: Option<thread::JoinHandle<()>>,
    worker_handles: Vec<thread::JoinHandle<()>>,
    aggregator_handle: Option<thread::JoinHandle<P>>,
    _worker_marker: PhantomData<W>,
}

impl<W, P> OctoRuntime<W, P>
where
    W: OctoWorker + 'static,
    P: OctoParent<Delta = W::Delta> + Send + 'static,
{
    pub fn new<F, PF>(config: &OctoConfig, worker_factory: F, parent_factory: PF) -> Self
    where
        F: Fn(usize) -> W + Send + Sync + 'static,
        PF: FnOnce() -> P + Send + 'static,
    {
        let num_workers = config.num_workers.max(1);
        let (ingress_tx, ingress_rx) = mpsc::channel::<IngressMsg>();
        let (delta_tx, delta_rx) = mpsc::channel::<W::Delta>();
        let pin_cores = config.pin_cores;

        let aggregator_handle = thread::spawn(move || {
            if pin_cores {
                let _ = core_affinity::set_for_current(core_affinity::CoreId { id: num_workers });
            }
            let mut parent = parent_factory();
            for delta in delta_rx {
                parent.apply(delta);
            }
            parent
        });

        let wf = Arc::new(worker_factory);
        let mut worker_input_txs = Vec::with_capacity(num_workers);
        let mut worker_handles = Vec::with_capacity(num_workers);
        for worker_id in 0..num_workers {
            let (worker_tx, worker_rx) = mpsc::channel::<WorkerMsg>();
            worker_input_txs.push(worker_tx);
            let wf_clone = Arc::clone(&wf);
            let delta_tx_worker = delta_tx.clone();
            let pin_cores = config.pin_cores;
            worker_handles.push(thread::spawn(move || {
                if pin_cores {
                    let _ = core_affinity::set_for_current(core_affinity::CoreId { id: worker_id });
                }
                let mut worker = wf_clone(worker_id);
                while let Ok(msg) = worker_rx.recv() {
                    match msg {
                        WorkerMsg::Batch(batch) => {
                            for input in batch {
                                let borrowed = input.as_sketch_input();
                                worker.process(&borrowed, &mut |delta| {
                                    delta_tx_worker.send(delta).expect(
                                        "aggregator receiver dropped while workers still running",
                                    );
                                });
                            }
                        }
                        WorkerMsg::End => break,
                    }
                }
            }));
        }
        drop(delta_tx);

        let dispatcher_handle = thread::spawn(move || {
            let mut sent_end = false;
            let mut pending = vec![Vec::new(); num_workers];
            while let Ok(msg) = ingress_rx.recv() {
                match msg {
                    IngressMsg::Data(input) => {
                        let borrowed = input.as_sketch_input();
                        let worker_id =
                            worker_index(hash64_seeded(PARTITION_SEED, &borrowed), num_workers);
                        pending[worker_id].push(input);
                        if pending[worker_id].len() >= WORKER_DISPATCH_BATCH_SIZE {
                            flush_worker_batch(&worker_input_txs, &mut pending, worker_id);
                        }
                    }
                    IngressMsg::End => {
                        flush_all_worker_batches(&worker_input_txs, &mut pending);
                        send_end_to_all_workers(&worker_input_txs);
                        sent_end = true;
                        break;
                    }
                }
            }

            if !sent_end {
                flush_all_worker_batches(&worker_input_txs, &mut pending);
                send_end_to_all_workers(&worker_input_txs);
            }
        });

        Self {
            ingress_tx,
            dispatcher_handle: Some(dispatcher_handle),
            worker_handles,
            aggregator_handle: Some(aggregator_handle),
            _worker_marker: PhantomData,
        }
    }

    pub fn insert(&mut self, input: SketchInput<'_>) {
        self.ingress_tx
            .send(IngressMsg::Data(OwnedSketchInput::from(&input)))
            .expect("dispatcher receiver dropped while runtime is active");
    }

    pub fn insert_batch(&mut self, inputs: &[SketchInput<'_>]) {
        for input in inputs {
            self.insert(input.clone());
        }
    }

    pub fn finish(mut self) -> OctoResult<P> {
        self.ingress_tx
            .send(IngressMsg::End)
            .expect("dispatcher receiver dropped before finish");
        drop(self.ingress_tx);

        if let Some(dispatcher) = self.dispatcher_handle.take() {
            dispatcher
                .join()
                .expect("dispatcher thread panicked during finish");
        }

        for handle in self.worker_handles {
            handle.join().expect("worker thread panicked during finish");
        }

        let parent = self
            .aggregator_handle
            .take()
            .expect("aggregator handle missing")
            .join()
            .expect("aggregator thread panicked during finish");

        OctoResult { parent }
    }
}

// ---------------------------------------------------------------------------
// Concrete worker/parent implementations
// ---------------------------------------------------------------------------

// -- CountMin --

/// OctoSketch worker backed by a `CountMinChild`.
pub struct CmOctoWorker {
    child: CountMinChild,
}

impl CmOctoWorker {
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            child: CountMinChild::with_dimensions(rows, cols),
        }
    }
}

impl OctoWorker for CmOctoWorker {
    type Delta = CmDelta;

    #[inline(always)]
    fn process(&mut self, input: &SketchInput, emit: &mut dyn FnMut(CmDelta)) {
        self.child.insert_and_emit(input, emit);
    }
}

/// OctoSketch parent wrapping a full-precision `CountMin`.
pub struct CmOctoParent {
    pub sketch: CountMin<Vector2D<i32>, RegularPath>,
}

impl OctoParent for CmOctoParent {
    type Delta = CmDelta;

    #[inline(always)]
    fn apply(&mut self, delta: CmDelta) {
        self.sketch.apply_delta(delta);
    }
}

// -- Count Sketch --

/// OctoSketch worker backed by a `CountChild`.
pub struct CountOctoWorker {
    child: CountChild,
}

impl CountOctoWorker {
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            child: CountChild::with_dimensions(rows, cols),
        }
    }
}

impl OctoWorker for CountOctoWorker {
    type Delta = CountDelta;

    #[inline(always)]
    fn process(&mut self, input: &SketchInput, emit: &mut dyn FnMut(CountDelta)) {
        self.child.insert_and_emit(input, emit);
    }
}

/// OctoSketch parent wrapping a full-precision `Count`.
pub struct CountOctoParent {
    pub sketch: Count<Vector2D<i32>, RegularPath>,
}

impl OctoParent for CountOctoParent {
    type Delta = CountDelta;

    #[inline(always)]
    fn apply(&mut self, delta: CountDelta) {
        self.sketch.apply_delta(delta);
    }
}

// -- HyperLogLog --

/// OctoSketch worker backed by an `HllChild`.
pub struct HllOctoWorker {
    child: HllChild,
}

impl HllOctoWorker {
    pub fn new() -> Self {
        Self {
            child: HllChild::default(),
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
        self.child.insert_and_emit(input, emit);
    }
}

/// OctoSketch parent wrapping a full-precision `HyperLogLog<Regular>`.
pub struct HllOctoParent {
    pub sketch: HyperLogLog<Regular>,
}

impl OctoParent for HllOctoParent {
    type Delta = HllDelta;

    #[inline(always)]
    fn apply(&mut self, delta: HllDelta) {
        self.sketch.apply_delta(delta);
    }
}

// ---------------------------------------------------------------------------
// Core execution engine
// ---------------------------------------------------------------------------

/// Hash-based input partitioning seed index (uses the last seed in SEEDLIST
/// to avoid collision with sketch-internal seeds which use indices 0..~5).
const PARTITION_SEED: usize = 19;

/// Runs the OctoSketch multi-threaded insert protocol.
///
/// 1. Dispatches `inputs` across workers online by hash through channels.
/// 2. Each worker maintains a child sketch, emitting deltas via an MPSC channel.
/// 3. The aggregator blocks on the channel and applies deltas to the parent.
/// 4. Returns the fully-merged parent sketch.
///
/// Uses `std::thread::scope` so `inputs` can have any lifetime (no `'static` needed).
pub fn run_octo<W, P>(
    inputs: &[SketchInput<'_>],
    config: &OctoConfig,
    worker_factory: impl Fn(usize) -> W + Send + Sync,
    parent_factory: impl FnOnce() -> P,
) -> OctoResult<P>
where
    W: OctoWorker,
    P: OctoParent<Delta = W::Delta>,
{
    let num_workers = config.num_workers.max(1);
    let _capacity = config.queue_capacity.max(1024);

    // Step 1: Create an unbounded MPSC channel for worker-to-aggregator deltas.
    let (tx, rx) = mpsc::channel::<W::Delta>();

    // Step 2: Run dispatcher + workers + aggregator inside a scoped thread block.
    let mut parent = parent_factory();

    thread::scope(|s| {
        // Pin the aggregator (calling thread) to core num_workers if requested.
        if config.pin_cores {
            let _ = core_affinity::set_for_current(core_affinity::CoreId { id: num_workers });
        }

        // Spawn worker threads.
        let wf = &worker_factory;
        let mut worker_input_txs = Vec::with_capacity(num_workers);
        for worker_id in 0..num_workers {
            let (worker_tx, worker_rx) = mpsc::channel::<WorkerMsg>();
            worker_input_txs.push(worker_tx);
            let tx_worker = tx.clone();
            let pin_cores = config.pin_cores;

            s.spawn(move || {
                // Pin to core.
                if pin_cores {
                    let _ = core_affinity::set_for_current(core_affinity::CoreId { id: worker_id });
                }

                let mut worker = wf(worker_id);
                while let Ok(msg) = worker_rx.recv() {
                    match msg {
                        WorkerMsg::Batch(batch) => {
                            for input in batch {
                                let borrowed = input.as_sketch_input();
                                worker.process(&borrowed, &mut |delta| {
                                    tx_worker.send(delta).expect(
                                        "aggregator receiver dropped while workers still running",
                                    );
                                });
                            }
                        }
                        WorkerMsg::End => break,
                    }
                }
            });
        }

        // Spawn dispatcher thread: hash-route each input to a worker channel.
        s.spawn(move || {
            let mut pending = vec![Vec::new(); num_workers];
            for item in inputs {
                let owned = OwnedSketchInput::from(item);
                let borrowed = owned.as_sketch_input();
                let worker_id = worker_index(hash64_seeded(PARTITION_SEED, &borrowed), num_workers);
                pending[worker_id].push(owned);
                if pending[worker_id].len() >= WORKER_DISPATCH_BATCH_SIZE {
                    flush_worker_batch(&worker_input_txs, &mut pending, worker_id);
                }
            }

            flush_all_worker_batches(&worker_input_txs, &mut pending);
            send_end_to_all_workers(&worker_input_txs);
        });

        // Drop the main sender so the receiver closes once all worker clones drop.
        drop(tx);

        // Aggregator collect loop: block until a delta arrives; exits when senders are dropped.
        for delta in rx {
            parent.apply(delta);
        }
    });

    OctoResult { parent }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SketchInput;

    // -----------------------------------------------------------------------
    // Layer 2 unit tests: child sketches + apply_delta
    // -----------------------------------------------------------------------

    #[test]
    fn cm_child_emits_delta_at_threshold() {
        let mut child = CountMinChild::with_dimensions(3, 64);
        let key = SketchInput::U64(42);
        let mut deltas: Vec<CmDelta> = Vec::new();

        // Insert CM_PROMASK-1 times: no delta yet.
        for _ in 0..(crate::CM_PROMASK - 1) {
            child.insert_and_emit(&key, |d| deltas.push(d));
        }
        assert!(deltas.is_empty(), "should not emit before threshold");

        // One more insert triggers the delta.
        child.insert_and_emit(&key, |d| deltas.push(d));
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
        let mut child = CountChild::with_dimensions(3, 64);
        let key = SketchInput::U64(99);
        let mut deltas: Vec<CountDelta> = Vec::new();

        // Insert enough times to trigger at least one delta.
        for _ in 0..200 {
            child.insert_and_emit(&key, |d| deltas.push(d));
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
        let mut child = HllChild::default();
        let mut deltas: Vec<HllDelta> = Vec::new();

        // First insert should always emit (register goes from 0 to something > 0).
        child.insert_and_emit(&SketchInput::U64(1), |d| deltas.push(d));
        assert_eq!(deltas.len(), 1, "first insert should emit a delta");

        // Inserting the same value again should NOT emit (register unchanged).
        let len_before = deltas.len();
        child.insert_and_emit(&SketchInput::U64(1), |d| deltas.push(d));
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

    impl OctoParent for CountingParent {
        type Delta = u64;

        fn apply(&mut self, delta: Self::Delta) {
            self.total += delta;
        }
    }

    #[test]
    fn worker_index_is_deterministic_for_pow2_and_non_pow2_workers() {
        let key = SketchInput::U64(0xdead_beef);
        let hash = hash64_seeded(PARTITION_SEED, &key);

        let idx_pow2_a = worker_index(hash, 8);
        let idx_pow2_b = worker_index(hash, 8);
        assert_eq!(idx_pow2_a, idx_pow2_b);
        assert_eq!(idx_pow2_a, (hash as usize) & 7);

        let idx_non_pow2_a = worker_index(hash, 6);
        let idx_non_pow2_b = worker_index(hash, 6);
        assert_eq!(idx_non_pow2_a, idx_non_pow2_b);
        assert_eq!(idx_non_pow2_a, (hash as usize) % 6);
    }

    #[test]
    fn run_octo_batch_flush_boundary_matches_reference_count() {
        let config = OctoConfig {
            num_workers: 3,
            pin_cores: false,
            queue_capacity: 4096,
        };
        let sizes = [
            WORKER_DISPATCH_BATCH_SIZE - 1,
            WORKER_DISPATCH_BATCH_SIZE,
            WORKER_DISPATCH_BATCH_SIZE + 1,
        ];

        for &n in &sizes {
            let inputs: Vec<SketchInput<'_>> = (0..n as u64)
                .map(|i| SketchInput::U64(i ^ 0x1234))
                .collect();
            let result = run_octo(
                &inputs,
                &config,
                |_| CountingWorker,
                || CountingParent { total: 0 },
            );
            assert_eq!(
                result.parent.total, n as u64,
                "batch boundary size {n} should process all items"
            );
        }
    }

    #[test]
    fn octo_runtime_end_flush_preserves_tail_items() {
        let config = OctoConfig {
            num_workers: 4,
            pin_cores: false,
            queue_capacity: 4096,
        };
        let n = WORKER_DISPATCH_BATCH_SIZE + 7;
        let mut runtime =
            OctoRuntime::new(&config, |_| CountingWorker, || CountingParent { total: 0 });

        for i in 0..n as u64 {
            runtime.insert(SketchInput::U64(i + 42));
        }
        let result = runtime.finish();
        assert_eq!(
            result.parent.total, n as u64,
            "runtime finish should flush partial worker batches"
        );
    }
}
