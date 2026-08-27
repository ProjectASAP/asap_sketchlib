//! OctoSketch multi-threaded sketch framework.
//!
//! Implements the parent-child delta-promotion architecture from OctoSketch (NSDI 2024).
//! Worker threads maintain lightweight child sketches with one-byte counters and emit
//! compact delta entries via per-worker channels when a counter reaches the promotion
//! threshold τ. An aggregator thread applies deltas to a full-precision parent sketch
//! and adjusts τ to keep its receive rate matched to the workers' send rate.
//!
//! The three ideas of §3.2 map onto this module as follows:
//!
//! - Idea 1, change-based updates: [`OctoWorker`] emits one counter at a time
//!   instead of shipping whole sketches.
//! - Idea 2, adaptive resource allocation: [`OctoAdaptiveThreshold`] drives the
//!   shared [`OctoThreshold`] from the aggregator's queue occupancy.
//! - Idea 3, reconstructed data structures: [`CmWorkerSketch`] and
//!   [`CountWorkerSketch`] drop flow-key storage and use one-byte counters,
//!   while the `*TopK*` aggregators hold the only heavy-hitter heap.
//!
//! Reference:
//! - <https://www.usenix.org/conference/nsdi24/presentation/zhang-yinda>

#[cfg(feature = "octo-runtime")]
use std::marker::PhantomData;
#[cfg(feature = "octo-runtime")]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(feature = "octo-runtime")]
use std::sync::{Arc, RwLock, Weak};
#[cfg(feature = "octo-runtime")]
use std::thread;
#[cfg(feature = "octo-runtime")]
use std::time::{Duration, Instant};

#[cfg(feature = "octo-runtime")]
use crossbeam_channel::{Receiver, Sender, TryRecvError, bounded};

use std::collections::HashMap;

use crate::common::input::data_input_to_f64;
use crate::common::structures::matrix_storage::cols_mask_bits;
use crate::octo_delta::{
    DD_PROMASK, DdDelta, KeyedCmDelta, KeyedCountDelta, MAX_PROMASK, OctoThreshold, UNIVMON_PROMASK,
};
use crate::sketch_framework::univmon::{UnivMonDeltaFidelity, bottom_layer_for_hash};
use crate::sketches::countminsketch_topk::CMSHeap;
use crate::sketches::countsketch_topk::{CSHeap, l2hh_cell_for_row};
use crate::{
    BOTTOM_LAYER_FINDER, CM_PROMASK, COUNT_PROMASK, Classic, CmDelta, Count, CountDelta, CountMin,
    DDSketch, DataInput, HLL_PROMASK, HeapItem, HllDelta, HyperLogLog, LayeredCountDelta,
    RegularPath, UnivMon, Vector2D, hash64_seeded, hash128_seeded, heap_item_to_sketch_input,
    input_to_owned,
};

#[cfg(feature = "octo-runtime")]
/// Legacy queue capacity default retained for config compatibility.
const DEFAULT_QUEUE_CAPACITY: usize = 65536;

/// Default heavy-hitter heap capacity for the `*TopK*` aggregators.
pub const DEFAULT_OCTO_TOP_K: usize = 128;

const LOWER_32_MASK: u64 = (1u64 << 32) - 1;

// ---------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------

/// Worker-side trait: processes inputs and emits deltas.
pub trait OctoWorker: Send {
    /// Delta type emitted by the worker.
    ///
    /// Deltas that carry a flow key own it, so this is `Send + 'static` rather
    /// than `Copy`.
    type Delta: Send + 'static;

    /// Process one input and emit zero or more deltas.
    fn process<F>(&mut self, input: &DataInput, emit: &mut F)
    where
        F: FnMut(Self::Delta);
}

/// Aggregator-side trait: absorbs deltas into a full-precision sketch.
pub trait OctoAggregator: Send {
    /// Delta type consumed by the aggregator.
    type Delta: Send + 'static;

    /// Apply a single delta to the parent sketch.
    fn apply(&mut self, delta: Self::Delta);
}

// ---------------------------------------------------------------------------
// Compact worker sketches (§4.1, §3.2 Idea 3)
// ---------------------------------------------------------------------------

/// Count-Min worker sketch: one-byte counters, no flow-key storage.
///
/// A worker counter is cleared the moment it reaches τ, so it never exceeds
/// `MAX_PROMASK` and `⌈log τ⌉` bits suffice. Against the 32-bit counters a
/// full `CountMin` uses, this is the 4x memory saving of §4.1.
#[derive(Clone, Debug)]
pub struct CmWorkerSketch {
    counters: Vec<u8>,
    rows: usize,
    cols: usize,
}

impl CmWorkerSketch {
    /// Creates a `rows` x `cols` worker sketch with all counters cleared.
    pub fn new(rows: usize, cols: usize) -> Self {
        let rows = rows.max(1);
        let cols = cols.max(1);
        Self {
            counters: vec![0u8; rows * cols],
            rows,
            cols,
        }
    }

    /// Number of rows.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Number of columns per row.
    pub fn cols(&self) -> usize {
        self.cols
    }

    /// Bytes held by the counter array.
    pub fn counter_bytes(&self) -> usize {
        self.counters.len()
    }

    /// Counters still held back from the aggregator, one per cell.
    pub fn residual(&self) -> &[u8] {
        &self.counters
    }

    /// Inserts a key, emitting and clearing every counter that reaches
    /// `threshold` (Algorithm 1).
    #[inline(always)]
    pub fn insert_emit_delta(
        &mut self,
        value: &DataInput,
        threshold: u32,
        emit: &mut impl FnMut(CmDelta),
    ) {
        let threshold = threshold.clamp(1, MAX_PROMASK) as u8;
        for r in 0..self.rows {
            let hashed = hash64_seeded(r, value);
            let col = ((hashed & LOWER_32_MASK) as usize) % self.cols;
            let cell = &mut self.counters[r * self.cols + col];
            *cell += 1;
            if *cell >= threshold {
                emit(CmDelta {
                    row: r as u32,
                    col: col as u32,
                    value: *cell as u32,
                });
                *cell = 0;
            }
        }
    }

    /// As `insert_emit_delta`, but each delta carries the flow key so the
    /// aggregator can maintain the heavy-hitter heap.
    #[inline(always)]
    pub fn insert_emit_keyed_delta(
        &mut self,
        value: &DataInput,
        threshold: u32,
        emit: &mut impl FnMut(KeyedCmDelta),
    ) {
        let mut key: Option<HeapItem> = None;
        self.insert_emit_delta(value, threshold, &mut |delta| {
            let key = key.get_or_insert_with(|| input_to_owned(value));
            emit(KeyedCmDelta {
                key: key.clone(),
                delta,
            })
        });
    }
}

/// Count-sketch worker sketch: one-byte signed counters, no flow-key storage.
#[derive(Clone, Debug)]
pub struct CountWorkerSketch {
    counters: Vec<i8>,
    rows: usize,
    cols: usize,
}

impl CountWorkerSketch {
    /// Creates a `rows` x `cols` worker sketch with all counters cleared.
    pub fn new(rows: usize, cols: usize) -> Self {
        let rows = rows.max(1);
        let cols = cols.max(1);
        Self {
            counters: vec![0i8; rows * cols],
            rows,
            cols,
        }
    }

    /// Number of rows.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Number of columns per row.
    pub fn cols(&self) -> usize {
        self.cols
    }

    /// Bytes held by the counter array.
    pub fn counter_bytes(&self) -> usize {
        self.counters.len()
    }

    /// Counters still held back from the aggregator, one per cell.
    pub fn residual(&self) -> &[i8] {
        &self.counters
    }

    /// Inserts a key, emitting and clearing every counter whose magnitude
    /// reaches `threshold` (§4.4: signed counters compare on `|counter|`).
    #[inline(always)]
    pub fn insert_emit_delta(
        &mut self,
        value: &DataInput,
        threshold: u32,
        emit: &mut impl FnMut(CountDelta),
    ) {
        // A signed one-byte counter reaches -128 before +127, so the usable
        // magnitude is i8::MAX.
        let threshold = threshold.clamp(1, i8::MAX as u32) as i8;
        for r in 0..self.rows {
            let hashed = hash64_seeded(r, value);
            let col = ((hashed & LOWER_32_MASK) as usize) % self.cols;
            let sign: i8 = if ((hashed >> 63) & 1) == 1 { 1 } else { -1 };
            let cell = &mut self.counters[r * self.cols + col];
            *cell += sign;
            if cell.unsigned_abs() >= threshold as u8 {
                emit(CountDelta {
                    row: r as u32,
                    col: col as u32,
                    value: *cell as i32,
                });
                *cell = 0;
            }
        }
    }

    /// As `insert_emit_delta`, but each delta carries the flow key.
    #[inline(always)]
    pub fn insert_emit_keyed_delta(
        &mut self,
        value: &DataInput,
        threshold: u32,
        emit: &mut impl FnMut(KeyedCountDelta),
    ) {
        let mut key: Option<HeapItem> = None;
        self.insert_emit_delta(value, threshold, &mut |delta| {
            let key = key.get_or_insert_with(|| input_to_owned(value));
            emit(KeyedCountDelta {
                key: key.clone(),
                delta,
            })
        });
    }
}

/// UnivMon-layer worker sketch: one-byte signed counters over the cell mapping
/// `CountL2HH` uses.
///
/// A UnivMon layer is not a plain `Count` sketch: it slices one 128-bit hash
/// into per-row columns and takes each row's sign from a different high bit,
/// and every layer hashes under its own seed. This worker calls the same
/// `l2hh_cell_for_row` the parent does, so the two cannot drift apart.
#[derive(Clone, Debug)]
pub struct L2hhWorkerSketch {
    counters: Vec<i8>,
    rows: usize,
    cols: usize,
    seed_idx: usize,
    mask_bits: u32,
}

impl L2hhWorkerSketch {
    /// Creates a worker over the cell space of a `CountL2HH` with these
    /// dimensions and seed.
    pub fn new(rows: usize, cols: usize, seed_idx: usize) -> Self {
        let rows = rows.max(1);
        let cols = cols.max(1);
        Self {
            counters: vec![0i8; rows * cols],
            rows,
            cols,
            seed_idx,
            mask_bits: cols_mask_bits(cols),
        }
    }

    /// Bytes held by the counter array.
    pub fn counter_bytes(&self) -> usize {
        self.counters.len()
    }

    /// Counters still held back from the aggregator, one per cell.
    pub fn residual(&self) -> &[i8] {
        &self.counters
    }

    /// Inserts a key, emitting and clearing every counter whose magnitude
    /// reaches `threshold`.
    #[inline(always)]
    pub fn insert_emit_delta(
        &mut self,
        value: &DataInput,
        threshold: u32,
        emit: &mut impl FnMut(CountDelta),
    ) {
        let threshold = threshold.clamp(1, i8::MAX as u32) as u8;
        let hashed = hash128_seeded(self.seed_idx, value);
        for row in 0..self.rows {
            let (col, sign) = l2hh_cell_for_row(hashed, row, self.cols, self.mask_bits);
            let cell = &mut self.counters[row * self.cols + col];
            *cell += sign as i8;
            if cell.unsigned_abs() >= threshold {
                emit(CountDelta {
                    row: row as u32,
                    col: col as u32,
                    value: *cell as i32,
                });
                *cell = 0;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Configuration & Runtime (requires "octo-runtime" feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "octo-runtime")]
/// How inputs are handed to workers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum OctoPartition {
    /// Hash the key so one flow always lands on one worker.
    ///
    /// This is the paper's setting - each worker owns a slice of the traffic -
    /// and makes k', the number of workers a flow may pass by, equal to 1. The
    /// additive term k'τ in the error bounds is then as small as it can be.
    #[default]
    HashByKey,
    /// Hand inputs to workers in turn, regardless of key.
    ///
    /// Spreads load perfectly even under a skewed key distribution, at the cost
    /// of k' = k: every flow reaches every worker, so each one may hold back up
    /// to τ of that flow's count.
    RoundRobin,
}

#[cfg(feature = "octo-runtime")]
/// Aggregator-driven threshold control (§4.3).
///
/// The aggregator watches how much work is queued and nudges the shared τ so
/// its receive rate tracks the workers' send rate: a short queue means it can
/// afford more updates, a long one means it is falling behind.
#[derive(Clone, Debug)]
pub struct OctoAdaptiveThreshold {
    /// Target total queue length Q across all worker channels (paper: 10).
    pub target_queue_len: usize,
    /// Dead band α around the target (paper: 0.25).
    pub alpha: f64,
    /// How often the threshold is re-evaluated (paper: 100µs).
    pub interval: Duration,
    /// Floor for τ. Derive it from an accuracy target with `threshold_for_error`.
    pub min_threshold: u32,
    /// Ceiling for τ.
    pub max_threshold: u32,
}

#[cfg(feature = "octo-runtime")]
impl Default for OctoAdaptiveThreshold {
    fn default() -> Self {
        Self {
            target_queue_len: 10,
            alpha: 0.25,
            interval: Duration::from_micros(100),
            min_threshold: 1,
            max_threshold: MAX_PROMASK,
        }
    }
}

/// The threshold that meets an additive error target, from Equation 4: `τ = εL1/k'`.
///
/// `k_prime` is the number of workers a single flow may reach: 1 under
/// [`OctoPartition::HashByKey`], and the worker count under
/// [`OctoPartition::RoundRobin`].
pub fn threshold_for_error(epsilon: f64, l1: f64, k_prime: usize) -> u32 {
    if !(epsilon.is_finite() && l1.is_finite()) || k_prime == 0 {
        return 1;
    }
    let tau = epsilon * l1 / k_prime as f64;
    if !tau.is_finite() || tau < 1.0 {
        return 1;
    }
    (tau as u32).min(MAX_PROMASK)
}

#[cfg(feature = "octo-runtime")]
/// Configuration for `run_octo` and [`OctoRuntime`].
#[derive(Clone, Debug)]
pub struct OctoConfig {
    /// Number of worker threads (default: 4).
    pub num_workers: usize,
    /// Pin worker threads to cores (default: true).
    /// Worker i is pinned to core i, aggregator to core num_workers.
    /// Silently skipped if pinning fails.
    pub pin_cores: bool,
    /// Queue capacity for bounded worker-input and worker-delta channels (default: 65536).
    pub queue_capacity: usize,
    /// Promotion threshold τ shared by every worker.
    ///
    /// Clone it into the worker factory so the workers and the aggregator's
    /// controller refer to the same value.
    pub threshold: OctoThreshold,
    /// How inputs are routed to workers (default: [`OctoPartition::HashByKey`]).
    pub partition: OctoPartition,
    /// Threshold control, or `None` to hold τ fixed (default: `None`).
    pub adaptive: Option<OctoAdaptiveThreshold>,
}

#[cfg(feature = "octo-runtime")]
impl Default for OctoConfig {
    fn default() -> Self {
        Self {
            num_workers: 4,
            pin_cores: true,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            threshold: OctoThreshold::new(CM_PROMASK),
            partition: OctoPartition::default(),
            adaptive: None,
        }
    }
}

#[cfg(feature = "octo-runtime")]
/// Result of an `run_octo` execution.
pub struct OctoResult<P> {
    /// Final parent sketch after all deltas are applied.
    pub parent: P,
}

#[cfg(feature = "octo-runtime")]
enum WorkerMsg {
    Data(DataInput<'static>),
    End,
}

#[cfg(feature = "octo-runtime")]
/// Extends a `DataInput` lifetime to `'static` for cross-thread transport in
/// streaming mode. Caller must ensure all borrowed data outlives worker processing.
#[inline(always)]
unsafe fn assume_input_static(input: DataInput<'_>) -> DataInput<'static> {
    // SAFETY: enforced by caller contract described above.
    unsafe { std::mem::transmute::<DataInput<'_>, DataInput<'static>>(input) }
}

#[cfg(feature = "octo-runtime")]
/// Streaming Octo runtime that accepts incremental inserts and finalizes into a parent sketch.
pub struct OctoRuntime<W, P>
where
    W: OctoWorker + 'static,
    P: OctoAggregator<Delta = W::Delta> + Send + Sync + 'static,
{
    core: Option<OctoCore<P>>,
    _worker_marker: PhantomData<W>,
}

#[cfg(feature = "octo-runtime")]
/// Read-only handle for querying the live aggregator state while runtime is active.
pub struct OctoReadHandle<P> {
    parent: Weak<RwLock<P>>,
}

#[cfg(feature = "octo-runtime")]
impl<P> Clone for OctoReadHandle<P> {
    fn clone(&self) -> Self {
        Self {
            parent: Weak::clone(&self.parent),
        }
    }
}

#[cfg(feature = "octo-runtime")]
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

#[cfg(feature = "octo-runtime")]
struct OctoCore<P> {
    worker_input_txs: Vec<Sender<WorkerMsg>>,
    next_worker: AtomicUsize,
    partition: OctoPartition,
    worker_handles: Vec<thread::JoinHandle<()>>,
    aggregator_handle: Option<thread::JoinHandle<()>>,
    parent: Arc<RwLock<P>>,
    closed: AtomicBool,
}

#[cfg(feature = "octo-runtime")]
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

    fn worker_for(&self, input: &DataInput<'_>) -> usize {
        let workers = self.worker_input_txs.len();
        match self.partition {
            OctoPartition::RoundRobin => self.next_worker.fetch_add(1, Ordering::AcqRel) % workers,
            OctoPartition::HashByKey => (hash64_seeded(0, input) % workers as u64) as usize,
        }
    }
}

#[cfg(feature = "octo-runtime")]
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

#[cfg(feature = "octo-runtime")]
impl<P> OctoCore<P>
where
    P: Send + Sync + 'static,
{
    fn start<W>(workers: Vec<W>, parent: P, config: &OctoConfig) -> Self
    where
        W: OctoWorker + 'static,
        P: OctoAggregator<Delta = W::Delta>,
    {
        let num_workers = config.num_workers.max(1);
        assert_eq!(workers.len(), num_workers);
        let queue_capacity = config.queue_capacity.max(1);
        let pin_cores = config.pin_cores;

        let parent = Arc::new(RwLock::new(parent));
        let parent_for_aggregator = Arc::clone(&parent);
        let mut delta_txs: Vec<Sender<W::Delta>> = Vec::with_capacity(num_workers);
        let mut delta_rxs: Vec<Option<Receiver<W::Delta>>> = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let (tx, rx) = bounded::<W::Delta>(queue_capacity);
            delta_txs.push(tx);
            delta_rxs.push(Some(rx));
        }

        let adaptive = config.adaptive.clone();
        let threshold = config.threshold.clone();
        let aggregator_handle = thread::spawn(move || {
            if pin_cores {
                let _ = core_affinity::set_for_current(core_affinity::CoreId { id: num_workers });
            }

            let mut controller = adaptive.as_ref().map(ThresholdController::new);
            let mut disconnected_workers = 0usize;
            while disconnected_workers < num_workers {
                let mut made_progress = false;
                for rx_slot in &mut delta_rxs {
                    let Some(rx) = rx_slot else {
                        continue;
                    };
                    match rx.try_recv() {
                        Ok(delta) => {
                            let mut guard = parent_for_aggregator
                                .write()
                                .expect("parent lock poisoned in aggregator");
                            guard.apply(delta);
                            made_progress = true;
                        }
                        Err(TryRecvError::Empty) => {}
                        Err(TryRecvError::Disconnected) => {
                            *rx_slot = None;
                            disconnected_workers += 1;
                        }
                    }
                }
                if let Some(controller) = controller.as_mut() {
                    controller.maybe_adjust(&delta_rxs, &threshold);
                }
                if !made_progress {
                    std::hint::spin_loop();
                }
            }
        });

        let mut worker_input_txs = Vec::with_capacity(num_workers);
        let mut worker_handles = Vec::with_capacity(num_workers);
        for (worker_id, (mut worker, delta_tx_worker)) in
            workers.into_iter().zip(delta_txs).enumerate()
        {
            let (worker_tx, worker_rx) = bounded::<WorkerMsg>(queue_capacity);
            worker_input_txs.push(worker_tx);
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

        Self {
            worker_input_txs,
            next_worker: AtomicUsize::new(0),
            partition: config.partition,
            worker_handles,
            aggregator_handle: Some(aggregator_handle),
            parent,
            closed: AtomicBool::new(false),
        }
    }
}

#[cfg(feature = "octo-runtime")]
/// Applies Equations 1 and 2 of §4.3 on the aggregator thread.
struct ThresholdController {
    settings: OctoAdaptiveThreshold,
    previous_queue_len: f64,
    next_check: Instant,
    polls_since_check: u32,
}

#[cfg(feature = "octo-runtime")]
impl ThresholdController {
    /// Reading the clock on every poll would cost more than the control loop
    /// saves, so the elapsed-time check is itself sampled.
    const POLLS_BETWEEN_CLOCK_READS: u32 = 256;

    fn new(settings: &OctoAdaptiveThreshold) -> Self {
        Self {
            settings: settings.clone(),
            previous_queue_len: 0.0,
            next_check: Instant::now() + settings.interval,
            polls_since_check: 0,
        }
    }

    fn maybe_adjust<D>(&mut self, receivers: &[Option<Receiver<D>>], threshold: &OctoThreshold) {
        self.polls_since_check += 1;
        if self.polls_since_check < Self::POLLS_BETWEEN_CLOCK_READS {
            return;
        }
        self.polls_since_check = 0;

        let now = Instant::now();
        if now < self.next_check {
            return;
        }
        self.next_check = now + self.settings.interval;

        let queue_len: usize = receivers.iter().flatten().map(|rx| rx.len()).sum();
        let queue_len = queue_len as f64;
        // Equation 1: predict the next window from the last two observations.
        let predicted = queue_len + (queue_len - self.previous_queue_len);
        self.previous_queue_len = queue_len;

        // Equation 2: nudge τ by one in whichever direction the prediction misses.
        let target = self.settings.target_queue_len.max(1) as f64;
        let tau = threshold.get();
        let adjusted = if predicted < (1.0 - self.settings.alpha) * target {
            tau.saturating_sub(1)
        } else if predicted > (1.0 + self.settings.alpha) * target {
            tau.saturating_add(1)
        } else {
            tau
        };
        threshold.set(adjusted.clamp(self.settings.min_threshold, self.settings.max_threshold));
    }
}

#[cfg(feature = "octo-runtime")]
impl<W, P> OctoRuntime<W, P>
where
    W: OctoWorker + 'static,
    P: OctoAggregator<Delta = W::Delta> + Send + Sync + 'static,
{
    /// Starts the worker and aggregator threads described by `config`.
    pub fn new<F, PF>(config: &OctoConfig, worker_factory: F, parent_factory: PF) -> Self
    where
        F: Fn(usize) -> W,
        PF: FnOnce() -> P,
    {
        let num_workers = config.num_workers.max(1);
        let workers: Vec<W> = (0..num_workers).map(worker_factory).collect();
        let parent = parent_factory();
        let core = OctoCore::start(workers, parent, config);

        Self {
            core: Some(core),
            _worker_marker: PhantomData,
        }
    }

    /// Returns a handle for reading the parent while the runtime is live.
    pub fn read_handle(&self) -> OctoReadHandle<P> {
        self.core
            .as_ref()
            .expect("runtime core missing")
            .read_handle()
    }

    /// Signals every worker to stop after draining what is already queued.
    pub fn close(&self) {
        self.core.as_ref().expect("runtime core missing").close();
    }

    /// Routes one input to a worker according to the configured partition.
    pub fn insert(&mut self, input: DataInput<'_>) {
        let core = self.core.as_ref().expect("runtime core missing");
        if core.closed.load(Ordering::Acquire) {
            panic!("cannot insert after runtime has been closed");
        }

        let worker_id = core.worker_for(&input);
        // SAFETY: caller explicitly guarantees borrowed data lives long enough.
        let static_input = unsafe { assume_input_static(input) };
        core.worker_input_txs[worker_id]
            .send(WorkerMsg::Data(static_input))
            .expect("worker receiver dropped while runtime is active");
    }

    /// Inserts a batch, one element at a time.
    pub fn insert_batch(&mut self, inputs: &[DataInput<'_>]) {
        for input in inputs {
            self.insert(input.clone());
        }
    }

    /// Drains the pipeline and returns the finished parent sketch.
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

/// OctoSketch worker backed by a compact one-byte Count-Min sketch.
pub struct CmOctoWorker {
    sketch: CmWorkerSketch,
    threshold: OctoThreshold,
}

impl CmOctoWorker {
    /// Creates a worker with a private threshold fixed at `CM_PROMASK`.
    pub fn new(rows: usize, cols: usize) -> Self {
        Self::with_threshold(rows, cols, OctoThreshold::new(CM_PROMASK))
    }

    /// Creates a worker sharing `threshold` with its peers and the aggregator.
    pub fn with_threshold(rows: usize, cols: usize, threshold: OctoThreshold) -> Self {
        Self {
            sketch: CmWorkerSketch::new(rows, cols),
            threshold,
        }
    }

    /// Borrows the worker's counter array.
    pub fn sketch(&self) -> &CmWorkerSketch {
        &self.sketch
    }
}

impl OctoWorker for CmOctoWorker {
    type Delta = CmDelta;

    #[inline(always)]
    fn process<F>(&mut self, input: &DataInput, emit: &mut F)
    where
        F: FnMut(Self::Delta),
    {
        self.sketch
            .insert_emit_delta(input, self.threshold.get(), emit);
    }
}

/// OctoSketch parent wrapping a full-precision `CountMin`.
pub struct CmOctoAggregator {
    /// Parent Count-Min sketch updated by worker deltas.
    pub sketch: CountMin<Vector2D<i32>, RegularPath>,
}

impl CmOctoAggregator {
    /// Creates an aggregator with a `rows` x `cols` parent sketch.
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            sketch: CountMin::with_dimensions(rows, cols),
        }
    }
}

impl OctoAggregator for CmOctoAggregator {
    type Delta = CmDelta;

    #[inline(always)]
    fn apply(&mut self, delta: CmDelta) {
        self.sketch.apply_delta(delta);
    }
}

// -- CountMin with heavy-hitter tracking (§3.2, Idea 3) --

/// Count-Min worker that ships the flow key alongside each promoted counter.
pub struct CmTopKOctoWorker {
    sketch: CmWorkerSketch,
    threshold: OctoThreshold,
}

impl CmTopKOctoWorker {
    /// Creates a worker with a private threshold fixed at `CM_PROMASK`.
    pub fn new(rows: usize, cols: usize) -> Self {
        Self::with_threshold(rows, cols, OctoThreshold::new(CM_PROMASK))
    }

    /// Creates a worker sharing `threshold` with its peers and the aggregator.
    pub fn with_threshold(rows: usize, cols: usize, threshold: OctoThreshold) -> Self {
        Self {
            sketch: CmWorkerSketch::new(rows, cols),
            threshold,
        }
    }

    /// Borrows the worker's counter array.
    pub fn sketch(&self) -> &CmWorkerSketch {
        &self.sketch
    }
}

impl OctoWorker for CmTopKOctoWorker {
    type Delta = KeyedCmDelta;

    #[inline(always)]
    fn process<F>(&mut self, input: &DataInput, emit: &mut F)
    where
        F: FnMut(Self::Delta),
    {
        self.sketch
            .insert_emit_keyed_delta(input, self.threshold.get(), emit);
    }
}

/// Count-Min parent that also maintains the only heavy-hitter heap in the
/// pipeline, rebuilt from the keys the workers ship (Algorithm 2).
pub struct CmTopKOctoAggregator {
    /// Parent sketch plus heavy-hitter heap.
    pub sketch: CMSHeap<Vector2D<i32>, RegularPath>,
}

impl CmTopKOctoAggregator {
    /// Creates an aggregator tracking the `top_k` heaviest keys.
    pub fn new(rows: usize, cols: usize, top_k: usize) -> Self {
        Self {
            sketch: CMSHeap::new(rows, cols, top_k),
        }
    }
}

impl OctoAggregator for CmTopKOctoAggregator {
    type Delta = KeyedCmDelta;

    #[inline(always)]
    fn apply(&mut self, delta: KeyedCmDelta) {
        self.sketch.cms_mut().apply_delta(delta.delta);
        let key = heap_item_to_sketch_input(&delta.key);
        let estimate = self.sketch.cms().estimate(&key);
        self.sketch.heap_mut().update(&key, estimate as i64);
    }
}

// -- Count Sketch --

/// OctoSketch worker backed by a compact one-byte Count sketch.
pub struct CountOctoWorker {
    sketch: CountWorkerSketch,
    threshold: OctoThreshold,
}

impl CountOctoWorker {
    /// Creates a worker with a private threshold fixed at `COUNT_PROMASK`.
    pub fn new(rows: usize, cols: usize) -> Self {
        Self::with_threshold(rows, cols, OctoThreshold::new(COUNT_PROMASK))
    }

    /// Creates a worker sharing `threshold` with its peers and the aggregator.
    pub fn with_threshold(rows: usize, cols: usize, threshold: OctoThreshold) -> Self {
        Self {
            sketch: CountWorkerSketch::new(rows, cols),
            threshold,
        }
    }

    /// Borrows the worker's counter array.
    pub fn sketch(&self) -> &CountWorkerSketch {
        &self.sketch
    }
}

impl OctoWorker for CountOctoWorker {
    type Delta = CountDelta;

    #[inline(always)]
    fn process<F>(&mut self, input: &DataInput, emit: &mut F)
    where
        F: FnMut(Self::Delta),
    {
        self.sketch
            .insert_emit_delta(input, self.threshold.get(), emit);
    }
}

/// OctoSketch parent wrapping a full-precision `Count`.
pub struct CountOctoAggregator {
    /// Parent Count Sketch updated by worker deltas.
    pub sketch: Count<Vector2D<i32>, RegularPath>,
}

impl CountOctoAggregator {
    /// Creates an aggregator with a `rows` x `cols` parent sketch.
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            sketch: Count::with_dimensions(rows, cols),
        }
    }
}

impl OctoAggregator for CountOctoAggregator {
    type Delta = CountDelta;

    #[inline(always)]
    fn apply(&mut self, delta: CountDelta) {
        self.sketch.apply_delta(delta);
    }
}

// -- Count Sketch with heavy-hitter tracking --

/// Count-sketch worker that ships the flow key alongside each promoted counter.
pub struct CountTopKOctoWorker {
    sketch: CountWorkerSketch,
    threshold: OctoThreshold,
}

impl CountTopKOctoWorker {
    /// Creates a worker with a private threshold fixed at `COUNT_PROMASK`.
    pub fn new(rows: usize, cols: usize) -> Self {
        Self::with_threshold(rows, cols, OctoThreshold::new(COUNT_PROMASK))
    }

    /// Creates a worker sharing `threshold` with its peers and the aggregator.
    pub fn with_threshold(rows: usize, cols: usize, threshold: OctoThreshold) -> Self {
        Self {
            sketch: CountWorkerSketch::new(rows, cols),
            threshold,
        }
    }
}

impl OctoWorker for CountTopKOctoWorker {
    type Delta = KeyedCountDelta;

    #[inline(always)]
    fn process<F>(&mut self, input: &DataInput, emit: &mut F)
    where
        F: FnMut(Self::Delta),
    {
        self.sketch
            .insert_emit_keyed_delta(input, self.threshold.get(), emit);
    }
}

/// Count-sketch parent holding the pipeline's only heavy-hitter heap.
pub struct CountTopKOctoAggregator {
    /// Parent sketch plus heavy-hitter heap.
    pub sketch: CSHeap<Vector2D<i32>, RegularPath>,
}

impl CountTopKOctoAggregator {
    /// Creates an aggregator tracking the `top_k` heaviest keys.
    pub fn new(rows: usize, cols: usize, top_k: usize) -> Self {
        Self {
            sketch: CSHeap::new(rows, cols, top_k),
        }
    }
}

impl OctoAggregator for CountTopKOctoAggregator {
    type Delta = KeyedCountDelta;

    #[inline(always)]
    fn apply(&mut self, delta: KeyedCountDelta) {
        self.sketch.cs_mut().apply_delta(delta.delta);
        let key = heap_item_to_sketch_input(&delta.key);
        let estimate = self.sketch.cs().estimate(&key);
        self.sketch.heap_mut().update(&key, estimate as i64);
    }
}

// -- DDSketch --

/// DDSketch worker: one-byte counters over the parent's bucket space.
///
/// Appendix C applies OctoSketch to DDSketch the same way as to Count-Min - one
/// array of counters, promote and clear at τ. Bucket indices are sparse and
/// signed, so the counters are held in a map rather than a dense row.
#[derive(Clone, Debug)]
pub struct DdWorkerSketch {
    /// An empty sketch kept only for its logarithmic bucket mapping.
    mapping: DDSketch,
    counters: HashMap<i32, u8>,
}

impl DdWorkerSketch {
    /// Creates a worker over the bucket space of a DDSketch with this `alpha`.
    pub fn new(alpha: f64) -> Self {
        Self {
            mapping: DDSketch::new(alpha),
            counters: HashMap::new(),
        }
    }

    /// Relative accuracy of the bucket space this worker maps into.
    pub fn alpha(&self) -> f64 {
        self.mapping.alpha()
    }

    /// Counts still held back from the aggregator, by bucket index.
    pub fn residual(&self) -> &HashMap<i32, u8> {
        &self.counters
    }

    /// Total sample count still held back from the aggregator.
    ///
    /// A quantile read from the parent is off by at most this many ranks, so
    /// dividing it by the stream length bounds the rank error that promotion
    /// contributes on top of the sketch's own α.
    pub fn held_back(&self) -> u64 {
        self.counters.values().map(|&c| c as u64).sum()
    }

    /// Adds a sample, promoting and clearing the bucket that reaches `threshold`.
    ///
    /// Values the parent sketch would itself drop - non-positive, non-finite or
    /// outside the indexable range - are dropped here too.
    pub fn add_emit_delta(&mut self, value: f64, threshold: u32, emit: &mut impl FnMut(DdDelta)) {
        let Some(index) = self.mapping.bucket_index_for(value) else {
            return;
        };
        let threshold = threshold.clamp(1, MAX_PROMASK) as u8;
        let counter = self.counters.entry(index).or_insert(0);
        *counter += 1;
        if *counter >= threshold {
            emit(DdDelta {
                index,
                value: *counter as u64,
            });
            self.counters.remove(&index);
        }
    }
}

/// OctoSketch worker backed by a compact DDSketch bucket store.
pub struct DdOctoWorker {
    sketch: DdWorkerSketch,
    threshold: OctoThreshold,
}

impl DdOctoWorker {
    /// Creates a worker with a private threshold fixed at `DD_PROMASK`.
    pub fn new(alpha: f64) -> Self {
        Self::with_threshold(alpha, OctoThreshold::new(DD_PROMASK))
    }

    /// Creates a worker sharing `threshold` with its peers and the aggregator.
    pub fn with_threshold(alpha: f64, threshold: OctoThreshold) -> Self {
        Self {
            sketch: DdWorkerSketch::new(alpha),
            threshold,
        }
    }
}

impl OctoWorker for DdOctoWorker {
    type Delta = DdDelta;

    #[inline(always)]
    fn process<F>(&mut self, input: &DataInput, emit: &mut F)
    where
        F: FnMut(Self::Delta),
    {
        let Ok(value) = data_input_to_f64(input) else {
            return;
        };
        self.sketch
            .add_emit_delta(value, self.threshold.get(), emit);
    }
}

/// OctoSketch parent wrapping a full-precision `DDSketch`.
pub struct DdOctoAggregator {
    /// Parent quantile sketch updated by worker deltas.
    pub sketch: DDSketch,
}

impl DdOctoAggregator {
    /// Creates an aggregator with the given relative accuracy.
    pub fn new(alpha: f64) -> Self {
        Self {
            sketch: DDSketch::new(alpha),
        }
    }
}

impl OctoAggregator for DdOctoAggregator {
    type Delta = DdDelta;

    #[inline(always)]
    fn apply(&mut self, delta: DdDelta) {
        self.sketch.apply_delta(delta);
    }
}

// -- HyperLogLog --

/// OctoSketch worker backed by `HyperLogLog`.
///
/// Cardinality sketches merge by `max`, so this worker never clears a register;
/// its threshold is the exponent τ in the paper's `|2^C' - 2^C| >= 2^τ` rule and
/// is held fixed rather than driven by the aggregator's queue controller.
pub struct HllOctoWorker {
    child: HyperLogLog<Classic>,
    threshold: u8,
}

impl HllOctoWorker {
    /// Creates a HyperLogLog-backed Octo worker at the default threshold,
    /// which promotes every register improvement.
    pub fn new() -> Self {
        Self::with_threshold(HLL_PROMASK)
    }

    /// Creates a worker that promotes a register only once the improvement in
    /// `2^register` reaches `2^threshold`.
    pub fn with_threshold(threshold: u8) -> Self {
        Self {
            child: HyperLogLog::default(),
            threshold,
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
    fn process<F>(&mut self, input: &DataInput, emit: &mut F)
    where
        F: FnMut(Self::Delta),
    {
        self.child
            .insert_emit_delta_with_threshold(input, self.threshold, emit);
    }
}

/// OctoSketch parent wrapping a full-precision `HyperLogLog<Classic>`.
pub struct HllOctoAggregator {
    /// Parent HyperLogLog sketch updated by worker deltas.
    pub sketch: HyperLogLog<Classic>,
}

impl HllOctoAggregator {
    /// Creates an aggregator over a default-precision HyperLogLog.
    pub fn new() -> Self {
        Self {
            sketch: HyperLogLog::default(),
        }
    }
}

impl Default for HllOctoAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl OctoAggregator for HllOctoAggregator {
    type Delta = HllDelta;

    #[inline(always)]
    fn apply(&mut self, delta: HllDelta) {
        self.sketch.apply_delta(delta);
    }
}

// -- UnivMon --

/// Promotion threshold for UnivMon layer `layer`, given the pyramid's base τ.
///
/// A UnivMon layer only receives the keys that survive `layer` coin flips, so
/// layer L carries roughly `n / 2^L` of the stream. One threshold across the
/// whole pyramid would be sized for layer 0 and would starve the deep layers
/// outright - and those are exactly the layers the recursive estimator leans on
/// for cardinality. Halving τ per layer keeps each layer's promotion rate in
/// proportion to the traffic it actually sees, and the floor of 1 makes the
/// deepest layers exact, where they have too little traffic to threshold at all.
#[inline(always)]
pub fn univmon_layer_threshold(base: u32, layer: usize) -> u32 {
    if layer >= u32::BITS as usize {
        return 1;
    }
    (base >> layer).max(1)
}

/// OctoSketch worker backed by one compact sketch per UnivMon layer.
///
/// An insert reaches layers `0..=bottom_layer_for(key)`, exactly the set a
/// single-threaded `UnivMon::insert` touches, because the layer selector is a
/// pure function of the key's hash. The worker keeps no heavy-hitter heap;
/// that lives only in the aggregator.
pub struct UnivMonOctoWorker {
    layers: Vec<L2hhWorkerSketch>,
    threshold: OctoThreshold,
    worker_id: u32,
    weight_total: u64,
}

impl UnivMonOctoWorker {
    /// Creates a worker mirroring a `UnivMon` of these dimensions.
    pub fn new(worker_id: usize, sketch_row: usize, sketch_col: usize, layer_size: usize) -> Self {
        Self::with_threshold(
            worker_id,
            sketch_row,
            sketch_col,
            layer_size,
            OctoThreshold::new(UNIVMON_PROMASK),
        )
    }

    /// Creates a worker sharing `threshold` with its peers and the aggregator.
    pub fn with_threshold(
        worker_id: usize,
        sketch_row: usize,
        sketch_col: usize,
        layer_size: usize,
        threshold: OctoThreshold,
    ) -> Self {
        let layer_size = layer_size.max(1);
        Self {
            layers: (0..layer_size)
                .map(|layer| L2hhWorkerSketch::new(sketch_row, sketch_col, layer))
                .collect(),
            threshold,
            worker_id: worker_id as u32,
            weight_total: 0,
        }
    }

    /// Bytes held across every layer's counter array.
    pub fn counter_bytes(&self) -> usize {
        self.layers.iter().map(|l| l.counter_bytes()).sum()
    }
}

impl OctoWorker for UnivMonOctoWorker {
    type Delta = LayeredCountDelta;

    #[inline(always)]
    fn process<F>(&mut self, input: &DataInput, emit: &mut F)
    where
        F: FnMut(Self::Delta),
    {
        self.weight_total += 1;
        let bottom =
            bottom_layer_for_hash(hash64_seeded(BOTTOM_LAYER_FINDER, input), self.layers.len());
        let threshold = self.threshold.get();
        let (worker_id, weight_total) = (self.worker_id, self.weight_total);
        let mut key: Option<HeapItem> = None;
        for layer in 0..=bottom {
            let layer_threshold = univmon_layer_threshold(threshold, layer);
            self.layers[layer].insert_emit_delta(input, layer_threshold, &mut |delta| {
                let key = key.get_or_insert_with(|| input_to_owned(input));
                emit(LayeredCountDelta {
                    layer: layer as u32,
                    key: key.clone(),
                    delta,
                    worker_id,
                    weight_total,
                })
            });
        }
    }
}

/// OctoSketch parent wrapping a full-precision `UnivMon`.
///
/// It holds the pipeline's only heavy-hitter heaps and restores the total
/// weight UnivMon's g-sum queries divide by from the running totals its
/// workers report.
pub struct UnivMonOctoAggregator {
    /// Parent UnivMon updated by worker deltas.
    pub sketch: UnivMon,
    fidelity: Vec<UnivMonDeltaFidelity>,
    worker_weights: Vec<u64>,
    total_weight: u64,
}

impl UnivMonOctoAggregator {
    /// Creates an aggregator whose workers promote at `threshold`.
    ///
    /// The threshold decides whether candidate sets can ever be called
    /// complete: only a threshold of 1 delivers every insert to the heaps.
    pub fn new(
        heap_size: usize,
        sketch_row: usize,
        sketch_col: usize,
        layer_size: usize,
        threshold: u32,
    ) -> Self {
        let mut sketch = UnivMon::init_univmon(heap_size, sketch_row, sketch_col, layer_size);
        // Each layer runs its own scaled threshold, so completeness is decided
        // per layer: one that promotes every insert can still keep the heap's
        // own verdict, while one that thresholds has provably missed keys and
        // must give the flag up before any delta arrives.
        let fidelity: Vec<UnivMonDeltaFidelity> = (0..layer_size)
            .map(|layer| {
                if univmon_layer_threshold(threshold, layer) <= 1 {
                    UnivMonDeltaFidelity::EveryInsert
                } else {
                    UnivMonDeltaFidelity::PromotedOnly
                }
            })
            .collect();
        for (layer, mode) in fidelity.iter().enumerate() {
            if *mode == UnivMonDeltaFidelity::PromotedOnly {
                sketch.mark_layer_candidates_incomplete(layer);
            }
        }
        Self {
            sketch,
            fidelity,
            worker_weights: Vec::new(),
            total_weight: 0,
        }
    }
}

impl OctoAggregator for UnivMonOctoAggregator {
    type Delta = LayeredCountDelta;

    #[inline(always)]
    fn apply(&mut self, delta: LayeredCountDelta) {
        let worker = delta.worker_id as usize;
        if worker >= self.worker_weights.len() {
            self.worker_weights.resize(worker + 1, 0);
        }
        // A worker's running total only grows, so the newest report wins and
        // the fleet total is the sum of the newest report from each.
        if delta.weight_total > self.worker_weights[worker] {
            self.total_weight += delta.weight_total - self.worker_weights[worker];
            self.worker_weights[worker] = delta.weight_total;
        }
        let fidelity = self.fidelity[delta.layer as usize];
        self.sketch.apply_layered_delta(&delta, fidelity);
        self.sketch.set_total_weight(self.total_weight as usize);
    }
}

// ---------------------------------------------------------------------------
// Core execution engine
// ---------------------------------------------------------------------------

#[cfg(feature = "octo-runtime")]
/// Runs the OctoSketch multi-threaded insert protocol.
///
/// 1. Routes `inputs` to workers per `config.partition`.
/// 2. Each worker maintains a compact child sketch, emitting deltas on its own channel.
/// 3. The aggregator applies deltas to the parent and, if configured, adjusts τ.
/// 4. Returns the fully-merged parent sketch.
pub fn run_octo<W, P>(
    inputs: &[DataInput<'_>],
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
mod worker_tests {
    use super::*;

    #[test]
    fn compact_worker_clears_the_counter_it_promotes() {
        let mut worker = CmWorkerSketch::new(3, 64);
        let key = DataInput::U64(42);
        let mut deltas: Vec<CmDelta> = Vec::new();

        for _ in 0..(CM_PROMASK - 1) {
            worker.insert_emit_delta(&key, CM_PROMASK, &mut |d| deltas.push(d));
        }
        assert!(deltas.is_empty(), "should not promote before the threshold");

        worker.insert_emit_delta(&key, CM_PROMASK, &mut |d| deltas.push(d));
        assert_eq!(deltas.len(), 3, "one promotion per row");
        for d in &deltas {
            assert_eq!(d.value, CM_PROMASK);
        }
        assert!(
            worker.residual().iter().all(|&c| c == 0),
            "Algorithm 1 clears the counter it promotes"
        );
    }

    #[test]
    fn compact_worker_uses_a_quarter_of_the_counter_memory() {
        let (rows, cols) = (3usize, 4096usize);
        let compact = CmWorkerSketch::new(rows, cols).counter_bytes();
        let full = rows * cols * std::mem::size_of::<i32>();
        assert_eq!(compact * 4, full, "one byte per counter instead of four");
    }

    #[test]
    fn compact_worker_honours_a_custom_threshold() {
        let mut worker = CmWorkerSketch::new(1, 16);
        let key = DataInput::U64(7);
        let mut promotions = 0usize;
        for _ in 0..100 {
            worker.insert_emit_delta(&key, 10, &mut |_| promotions += 1);
        }
        assert_eq!(
            promotions, 10,
            "a threshold of 10 promotes every 10 inserts"
        );
    }

    #[test]
    fn compact_count_worker_promotes_on_magnitude_and_clears() {
        let mut worker = CountWorkerSketch::new(3, 64);
        let key = DataInput::U64(99);
        let mut deltas: Vec<CountDelta> = Vec::new();
        for _ in 0..200 {
            worker.insert_emit_delta(&key, COUNT_PROMASK, &mut |d| deltas.push(d));
        }
        assert!(!deltas.is_empty());
        for d in &deltas {
            assert_eq!(d.value.unsigned_abs(), COUNT_PROMASK);
        }
        assert!(worker.residual().iter().all(|&c| c.unsigned_abs() < 31));
    }

    #[test]
    fn keyed_deltas_carry_the_key_that_triggered_them() {
        let mut worker = CmWorkerSketch::new(2, 8);
        let key = DataInput::Str("flow-a");
        let mut keyed: Vec<KeyedCmDelta> = Vec::new();
        for _ in 0..CM_PROMASK {
            worker.insert_emit_keyed_delta(&key, CM_PROMASK, &mut |d| keyed.push(d));
        }
        assert!(!keyed.is_empty());
        for d in &keyed {
            assert_eq!(d.key, input_to_owned(&key));
        }
    }

    #[test]
    fn dd_worker_promotes_and_clears_a_bucket() {
        let mut worker = DdWorkerSketch::new(0.01);
        let mut deltas: Vec<DdDelta> = Vec::new();
        for _ in 0..CM_PROMASK {
            worker.add_emit_delta(42.0, CM_PROMASK, &mut |d| deltas.push(d));
        }
        assert_eq!(deltas.len(), 1, "one promotion per tau samples");
        assert_eq!(deltas[0].value as u32, CM_PROMASK);
        assert!(
            worker.residual().is_empty(),
            "the promoted bucket must be cleared"
        );
    }

    #[test]
    fn dd_worker_drops_what_the_parent_would_drop() {
        let mut worker = DdWorkerSketch::new(0.01);
        let mut promoted = 0usize;
        for value in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            for _ in 0..1_000 {
                worker.add_emit_delta(value, 1, &mut |_| promoted += 1);
            }
        }
        assert_eq!(
            promoted, 0,
            "non-positive and non-finite values are dropped"
        );
    }

    #[test]
    fn dd_promotion_bounds_the_quantile_rank_error_by_what_it_holds_back() {
        let alpha = 0.01;
        let mut worker = DdWorkerSketch::new(alpha);
        let mut parent = DDSketch::new(alpha);
        let mut exact: Vec<f64> = Vec::new();

        let n = 200_000u64;
        for i in 1..=n {
            let value = 1.0 + (i as f64 * 7.0) % 999.0;
            exact.push(value);
            worker.add_emit_delta(value, DD_PROMASK, &mut |d| parent.apply_delta(d));
        }
        exact.sort_by(f64::total_cmp);

        // Unlike a Count-Min row, an un-promoted bucket is missing from the
        // parent outright, so the promotion error shows up as a rank shift
        // bounded by the mass the worker still holds.
        assert_eq!(
            worker.held_back() + parent.get_count(),
            n,
            "every sample is either promoted or still held"
        );
        let rank_slack = worker.held_back() as f64 / n as f64;
        assert!(
            rank_slack < 0.05,
            "a threshold of {DD_PROMASK} held back {rank_slack:.3} of the stream"
        );

        for q in [0.1, 0.5, 0.9, 0.99] {
            let got = parent
                .get_value_at_quantile(q)
                .expect("parent received samples");
            let lo = exact[(((q - rank_slack).max(0.0)) * (exact.len() - 1) as f64) as usize]
                * (1.0 - alpha);
            let hi = exact[(((q + rank_slack).min(1.0)) * (exact.len() - 1) as f64) as usize]
                * (1.0 + alpha);
            assert!(
                (lo..=hi).contains(&got),
                "q={q}: {got} outside [{lo}, {hi}] at rank slack {rank_slack:.3}"
            );
        }
    }

    #[test]
    fn dd_a_higher_threshold_holds_back_more_of_a_sparse_stream() {
        let alpha = 0.01;
        let n = 20_000u64;
        let mut held = Vec::new();
        for threshold in [2u32, 8, 31] {
            let mut worker = DdWorkerSketch::new(alpha);
            for i in 1..=n {
                worker.add_emit_delta(1.0 + (i as f64 * 7.0) % 999.0, threshold, &mut |_| {});
            }
            held.push((threshold, worker.held_back()));
        }
        assert!(
            held[0].1 < held[1].1 && held[1].1 < held[2].1,
            "held-back mass must grow with the threshold: {held:?}"
        );
    }

    #[test]
    fn threshold_for_error_follows_equation_four() {
        // tau = eps * L1 / k'
        assert_eq!(threshold_for_error(0.001, 1_000_000.0, 4), 250);
        assert_eq!(threshold_for_error(0.001, 1_000_000.0, 1), MAX_PROMASK);
        assert_eq!(threshold_for_error(1e-9, 1_000.0, 1), 1, "never below one");
    }

    #[test]
    fn shared_threshold_is_visible_to_every_holder() {
        let shared = OctoThreshold::new(31);
        let clone = shared.clone();
        shared.increase(4);
        assert_eq!(clone.get(), 35);
        clone.decrease(34);
        assert_eq!(shared.get(), 1, "the floor is one");
        clone.set(u32::MAX);
        assert_eq!(shared.get(), MAX_PROMASK, "the ceiling is one byte");
    }

    #[test]
    fn hll_threshold_zero_promotes_every_improvement() {
        let mut child = HyperLogLog::<Classic>::default();
        let mut deltas = 0usize;
        child.insert_emit_delta_with_threshold(&DataInput::U64(1), 0, &mut |_| deltas += 1);
        assert_eq!(deltas, 1);
        child.insert_emit_delta_with_threshold(&DataInput::U64(1), 0, &mut |_| deltas += 1);
        assert_eq!(deltas, 1, "a duplicate cannot improve a register");
    }

    #[test]
    fn hll_threshold_holds_back_small_register_gains() {
        let n = 20_000u64;
        let mut promoted_at_zero = 0usize;
        let mut promoted_at_four = 0usize;
        let mut a = HyperLogLog::<Classic>::default();
        let mut b = HyperLogLog::<Classic>::default();
        for i in 0..n {
            a.insert_emit_delta_with_threshold(&DataInput::U64(i), 0, &mut |_| {
                promoted_at_zero += 1
            });
            b.insert_emit_delta_with_threshold(&DataInput::U64(i), 4, &mut |_| {
                promoted_at_four += 1
            });
        }
        assert!(
            promoted_at_four < promoted_at_zero,
            "a larger threshold must send fewer messages: {promoted_at_four} vs {promoted_at_zero}"
        );
    }
}

#[cfg(all(test, feature = "octo-runtime"))]
mod runtime_tests {
    use super::*;

    fn config(num_workers: usize) -> OctoConfig {
        OctoConfig {
            num_workers,
            // CI runners may have fewer cores than the widest configuration here.
            pin_cores: false,
            queue_capacity: 8192,
            ..OctoConfig::default()
        }
    }

    #[test]
    fn run_octo_cm_tracks_a_single_threaded_sketch() {
        let (rows, cols) = (3usize, 4096usize);
        let n = 100_000u64;
        let inputs: Vec<DataInput<'_>> = (0..n).map(|i| DataInput::U64(i % 1024)).collect();

        let mut reference = CountMin::<Vector2D<i32>, RegularPath>::with_dimensions(rows, cols);
        for input in &inputs {
            reference.insert(input);
        }

        let result = run_octo(
            &inputs,
            &config(4),
            |_| CmOctoWorker::new(rows, cols),
            || CmOctoAggregator::new(rows, cols),
        );

        // Hash partitioning puts each key on one worker, so at most one worker
        // holds back up to tau of any key: k' = 1.
        for key_val in 0u64..1024 {
            let key = DataInput::U64(key_val);
            let deficit = reference.estimate(&key) - result.parent.sketch.estimate(&key);
            assert!(
                (0..CM_PROMASK as i32).contains(&deficit),
                "key {key_val}: deficit {deficit} outside [0, tau)"
            );
        }
    }

    #[test]
    fn run_octo_hll_matches_a_single_threaded_sketch_exactly() {
        let n = 50_000u64;
        let inputs: Vec<DataInput<'_>> = (0..n).map(DataInput::U64).collect();
        let mut reference = HyperLogLog::<Classic>::default();
        for input in &inputs {
            reference.insert(input);
        }

        let result = run_octo(
            &inputs,
            &config(4),
            |_| HllOctoWorker::new(),
            HllOctoAggregator::new,
        );
        assert_eq!(
            result.parent.sketch.registers_as_slice(),
            reference.registers_as_slice()
        );
    }

    #[test]
    fn hash_partitioning_sends_a_key_to_exactly_one_worker() {
        let workers = 4usize;
        let cfg = OctoConfig {
            partition: OctoPartition::HashByKey,
            ..config(workers)
        };
        let mut runtime = OctoRuntime::new(
            &cfg,
            |worker_id| WorkerIdEmitter { worker_id },
            || WorkerLoadAggregator {
                loads: vec![0; workers],
            },
        );
        // One key, many inserts: a hash partition must pile them all on one worker.
        for _ in 0..1_000 {
            runtime.insert(DataInput::U64(4_242));
        }
        let loads = runtime.finish().parent.loads;
        assert_eq!(loads.iter().filter(|&&l| l > 0).count(), 1, "{loads:?}");
        assert_eq!(loads.iter().sum::<u64>(), 1_000);
    }

    #[test]
    fn round_robin_partitioning_still_spreads_inputs_evenly() {
        let workers = 3usize;
        let cfg = OctoConfig {
            partition: OctoPartition::RoundRobin,
            ..config(workers)
        };
        let mut runtime = OctoRuntime::new(
            &cfg,
            |worker_id| WorkerIdEmitter { worker_id },
            || WorkerLoadAggregator {
                loads: vec![0; workers],
            },
        );
        for i in 0..10u64 {
            runtime.insert(DataInput::U64(i));
        }
        assert_eq!(runtime.finish().parent.loads, vec![4, 3, 3]);
    }

    #[test]
    fn a_shared_threshold_reaches_every_worker() {
        let (rows, cols) = (2usize, 256usize);
        let threshold = OctoThreshold::new(4);
        let cfg = OctoConfig {
            threshold: threshold.clone(),
            ..config(2)
        };
        let inputs: Vec<DataInput<'_>> = (0..400u64).map(|i| DataInput::U64(i % 8)).collect();

        let low = run_octo(
            &inputs,
            &cfg,
            {
                let threshold = threshold.clone();
                move |_| CmOctoWorker::with_threshold(rows, cols, threshold.clone())
            },
            || CmOctoAggregator::new(rows, cols),
        );

        let high_threshold = OctoThreshold::new(64);
        let high_cfg = OctoConfig {
            threshold: high_threshold.clone(),
            ..config(2)
        };
        let high = run_octo(
            &inputs,
            &high_cfg,
            {
                let threshold = high_threshold.clone();
                move |_| CmOctoWorker::with_threshold(rows, cols, threshold.clone())
            },
            || CmOctoAggregator::new(rows, cols),
        );

        let probe = DataInput::U64(3);
        assert!(
            low.parent.sketch.estimate(&probe) > high.parent.sketch.estimate(&probe),
            "a lower threshold must promote more of the count"
        );
    }

    /// Drives `ThresholdController` over channels held at a fixed occupancy.
    fn adjust_against_queue_len(
        settings: &OctoAdaptiveThreshold,
        threshold: &OctoThreshold,
        queued: usize,
        rounds: usize,
    ) {
        let (tx, rx) = bounded::<u8>(queued.max(1));
        for _ in 0..queued {
            tx.send(0).expect("queue has room");
        }
        let receivers = vec![Some(rx)];
        let mut controller = ThresholdController::new(settings);
        for _ in 0..rounds {
            for _ in 0..ThresholdController::POLLS_BETWEEN_CLOCK_READS {
                controller.maybe_adjust(&receivers, threshold);
            }
        }
    }

    #[test]
    fn the_controller_raises_tau_when_the_queue_runs_long() {
        let settings = OctoAdaptiveThreshold {
            target_queue_len: 10,
            alpha: 0.25,
            interval: Duration::ZERO,
            min_threshold: 1,
            max_threshold: 200,
        };
        let threshold = OctoThreshold::new(31);
        adjust_against_queue_len(&settings, &threshold, 500, 8);
        assert!(
            threshold.get() > 31,
            "a queue far above target must push tau up, got {}",
            threshold.get()
        );
    }

    #[test]
    fn the_controller_lowers_tau_when_the_queue_runs_short() {
        let settings = OctoAdaptiveThreshold {
            target_queue_len: 10,
            alpha: 0.25,
            interval: Duration::ZERO,
            min_threshold: 4,
            max_threshold: 200,
        };
        let threshold = OctoThreshold::new(31);
        adjust_against_queue_len(&settings, &threshold, 0, 64);
        assert_eq!(
            threshold.get(),
            4,
            "an idle queue must walk tau down to its floor"
        );
    }

    #[test]
    fn the_controller_holds_tau_inside_the_dead_band() {
        let settings = OctoAdaptiveThreshold {
            target_queue_len: 10,
            alpha: 0.25,
            interval: Duration::ZERO,
            min_threshold: 1,
            max_threshold: 200,
        };
        let threshold = OctoThreshold::new(31);
        adjust_against_queue_len(&settings, &threshold, 10, 8);
        // Equation 1 has no previous sample on the first evaluation, so it
        // predicts 10 + (10 - 0) = 20 and nudges tau once. Every later
        // evaluation predicts 10, inside the [7.5, 12.5] dead band.
        assert_eq!(
            threshold.get(),
            32,
            "a queue held at target must settle after the initial transient"
        );
    }

    #[test]
    fn the_adaptive_controller_moves_tau_during_a_real_run() {
        let (rows, cols) = (3usize, 1024usize);
        let threshold = OctoThreshold::new(60);
        let settings = OctoAdaptiveThreshold {
            target_queue_len: 8,
            alpha: 0.25,
            interval: Duration::from_micros(1),
            min_threshold: 4,
            max_threshold: 120,
        };
        let cfg = OctoConfig {
            num_workers: 4,
            pin_cores: false,
            queue_capacity: 4096,
            threshold: threshold.clone(),
            partition: OctoPartition::HashByKey,
            adaptive: Some(settings.clone()),
        };

        let inputs: Vec<DataInput<'_>> =
            (0..200_000u64).map(|i| DataInput::U64(i % 4096)).collect();
        let result = run_octo(
            &inputs,
            &cfg,
            {
                let threshold = threshold.clone();
                move |_| CmOctoWorker::with_threshold(rows, cols, threshold.clone())
            },
            || CmOctoAggregator::new(rows, cols),
        );

        let settled = threshold.get();
        assert!(
            (settings.min_threshold..=settings.max_threshold).contains(&settled),
            "tau {settled} left the configured band"
        );
        assert_ne!(settled, 60, "the controller should have moved tau at all");
        // Whatever tau settled at, the parent is still a usable sketch.
        assert!(result.parent.sketch.estimate(&DataInput::U64(0)) > 0);
    }

    #[test]
    fn the_topk_aggregator_rebuilds_the_heavy_hitter_heap_from_worker_keys() {
        let (rows, cols, top_k) = (4usize, 2048usize, 8usize);
        let mut inputs: Vec<DataInput<'_>> = Vec::new();
        // Four unmistakable heavy hitters over a wide light tail.
        for i in 0..4u64 {
            for _ in 0..4_000 {
                inputs.push(DataInput::U64(i));
            }
        }
        for i in 1_000..6_000u64 {
            inputs.push(DataInput::U64(i));
        }

        let result = run_octo(
            &inputs,
            &config(4),
            |_| CmTopKOctoWorker::new(rows, cols),
            || CmTopKOctoAggregator::new(rows, cols, top_k),
        );

        let heap = result.parent.sketch.heap();
        assert!(!heap.is_empty(), "the aggregator must have built a heap");
        for hot in 0..4u64 {
            assert!(
                result
                    .parent
                    .sketch
                    .heap()
                    .find(&DataInput::U64(hot))
                    .is_some(),
                "heavy hitter {hot} missing from the aggregator heap"
            );
        }
    }

    #[test]
    fn the_count_topk_aggregator_also_tracks_heavy_hitters() {
        let (rows, cols, top_k) = (5usize, 2048usize, 8usize);
        let mut inputs: Vec<DataInput<'_>> = Vec::new();
        for i in 0..3u64 {
            for _ in 0..5_000 {
                inputs.push(DataInput::U64(i));
            }
        }
        for i in 500..3_000u64 {
            inputs.push(DataInput::U64(i));
        }

        let result = run_octo(
            &inputs,
            &config(4),
            |_| CountTopKOctoWorker::new(rows, cols),
            || CountTopKOctoAggregator::new(rows, cols, top_k),
        );
        for hot in 0..3u64 {
            assert!(
                result
                    .parent
                    .sketch
                    .heap()
                    .find(&DataInput::U64(hot))
                    .is_some(),
                "heavy hitter {hot} missing from the aggregator heap"
            );
        }
    }

    #[test]
    fn run_octo_ddsketch_tracks_quantiles_of_the_stream() {
        let alpha = 0.01;
        let inputs: Vec<DataInput<'_>> = (1..=200_000u64)
            .map(|i| DataInput::F64(1.0 + (i as f64 * 7.0) % 999.0))
            .collect();
        let mut exact: Vec<f64> = inputs
            .iter()
            .map(|i| match i {
                DataInput::F64(v) => *v,
                _ => unreachable!(),
            })
            .collect();
        exact.sort_by(f64::total_cmp);

        let result = run_octo(
            &inputs,
            &config(4),
            |_| DdOctoWorker::new(alpha),
            || DdOctoAggregator::new(alpha),
        );

        // Four workers each hold back their own partial buckets, so the rank
        // slack is what the whole fleet has yet to promote.
        let held_back = inputs.len() as u64 - result.parent.sketch.get_count();
        let rank_slack = held_back as f64 / inputs.len() as f64;
        assert!(rank_slack < 0.05, "held back {rank_slack:.3} of the stream");

        for q in [0.1, 0.5, 0.9] {
            let got = result
                .parent
                .sketch
                .get_value_at_quantile(q)
                .expect("parent received samples");
            let lo = exact[(((q - rank_slack).max(0.0)) * (exact.len() - 1) as f64) as usize]
                * (1.0 - alpha);
            let hi = exact[(((q + rank_slack).min(1.0)) * (exact.len() - 1) as f64) as usize]
                * (1.0 + alpha);
            assert!(
                (lo..=hi).contains(&got),
                "q={q}: {got} outside [{lo}, {hi}] at rank slack {rank_slack:.3}"
            );
        }
    }

    #[test]
    fn run_octo_univmon_tracks_heavy_hitters_and_total_weight() {
        let (heap, rows, cols, layers) = (32usize, 3usize, 256usize, 6usize);
        let tau = 8u32;
        let threshold = OctoThreshold::new(tau);

        let mut inputs: Vec<DataInput<'_>> = Vec::new();
        for hot in 0..5u64 {
            for _ in 0..4_000 {
                inputs.push(DataInput::U64(hot));
            }
        }
        for cold in 10_000..15_000u64 {
            inputs.push(DataInput::U64(cold));
        }

        let cfg = OctoConfig {
            threshold: threshold.clone(),
            ..config(4)
        };
        let result = run_octo(
            &inputs,
            &cfg,
            {
                let threshold = threshold.clone();
                move |worker_id| {
                    UnivMonOctoWorker::with_threshold(
                        worker_id,
                        rows,
                        cols,
                        layers,
                        threshold.clone(),
                    )
                }
            },
            || UnivMonOctoAggregator::new(heap, rows, cols, layers, tau),
        );
        let univmon = &result.parent.sketch;

        // The fleet reports its running weight on messages it was already
        // sending, so the aggregator's total trails by whatever arrived after
        // each worker's last promotion - never overshoots.
        assert!(
            univmon.bucket_size <= inputs.len(),
            "reported weight {} exceeds the stream",
            univmon.bucket_size
        );
        assert!(
            univmon.bucket_size as f64 > inputs.len() as f64 * 0.9,
            "reported weight {} trails the stream of {} by too much",
            univmon.bucket_size,
            inputs.len()
        );

        for hot in 0..5u64 {
            assert!(
                univmon.hh_layers[0].find(&DataInput::U64(hot)).is_some(),
                "heavy hitter {hot} missing from the aggregator's layer-0 heap"
            );
        }
        assert!(
            univmon.candidates_complete().iter().all(|c| !c),
            "a promoted-only aggregator must not claim complete candidate sets"
        );
    }

    #[test]
    fn octo_runtime_streaming_matches_the_batch_helper() {
        let (rows, cols) = (3usize, 4096usize);
        let inputs: Vec<DataInput<'_>> = (0..30_000u64).map(|i| DataInput::U64(i % 1024)).collect();
        let cfg = config(4);

        let batch = run_octo(
            &inputs,
            &cfg,
            |_| CmOctoWorker::new(rows, cols),
            || CmOctoAggregator::new(rows, cols),
        );

        let mut runtime = OctoRuntime::new(
            &cfg,
            move |_| CmOctoWorker::new(rows, cols),
            move || CmOctoAggregator::new(rows, cols),
        );
        for input in &inputs {
            runtime.insert(input.clone());
        }
        let streamed = runtime.finish();

        for key_val in 0u64..128 {
            let key = DataInput::U64(key_val);
            assert_eq!(
                batch.parent.sketch.estimate(&key),
                streamed.parent.sketch.estimate(&key),
                "key {key_val}"
            );
        }
    }

    #[test]
    fn octo_runtime_close_is_idempotent() {
        let runtime =
            OctoRuntime::new(&config(2), |_| HllOctoWorker::new(), HllOctoAggregator::new);
        runtime.close();
        runtime.close();
        assert_eq!(runtime.finish().parent.sketch.estimate(), 0);
    }

    #[test]
    #[should_panic(expected = "cannot insert after runtime has been closed")]
    fn octo_runtime_insert_after_close_panics() {
        let mut runtime =
            OctoRuntime::new(&config(2), |_| HllOctoWorker::new(), HllOctoAggregator::new);
        runtime.close();
        runtime.insert(DataInput::U64(1));
    }

    #[test]
    fn octo_runtime_empty_stream_finishes() {
        let runtime =
            OctoRuntime::new(&config(4), |_| HllOctoWorker::new(), HllOctoAggregator::new);
        assert_eq!(runtime.finish().parent.sketch.estimate(), 0);
    }

    #[test]
    fn octo_runtime_live_read_handle_observes_progress() {
        let mut runtime = OctoRuntime::new(
            &config(2),
            |_| CountingWorker,
            || CountingAggregator { total: 0 },
        );
        let reader = runtime.read_handle();

        for i in 0..64u64 {
            runtime.insert(DataInput::U64(i));
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
        let observed = reader.with_parent(|p| p.total);
        assert!(observed <= 64);

        let result = runtime.finish();
        assert_eq!(result.parent.total, 64);
        assert!(result.parent.total >= observed);
    }

    #[test]
    fn octo_runtime_close_preserves_queued_items() {
        let n = 257u64;
        let mut runtime = OctoRuntime::new(
            &config(4),
            |_| CountingWorker,
            || CountingAggregator { total: 0 },
        );
        for i in 0..n {
            runtime.insert(DataInput::U64(i + 42));
        }
        runtime.close();
        assert_eq!(runtime.finish().parent.total, n);
    }

    struct CountingWorker;

    impl OctoWorker for CountingWorker {
        type Delta = u64;

        fn process<F>(&mut self, _input: &DataInput, emit: &mut F)
        where
            F: FnMut(Self::Delta),
        {
            emit(1);
        }
    }

    struct CountingAggregator {
        total: u64,
    }

    impl OctoAggregator for CountingAggregator {
        type Delta = u64;

        fn apply(&mut self, delta: Self::Delta) {
            self.total += delta;
        }
    }

    struct WorkerIdEmitter {
        worker_id: usize,
    }

    impl OctoWorker for WorkerIdEmitter {
        type Delta = usize;

        fn process<F>(&mut self, _input: &DataInput, emit: &mut F)
        where
            F: FnMut(Self::Delta),
        {
            emit(self.worker_id);
        }
    }

    struct WorkerLoadAggregator {
        loads: Vec<u64>,
    }

    impl OctoAggregator for WorkerLoadAggregator {
        type Delta = usize;

        fn apply(&mut self, delta: Self::Delta) {
            self.loads[delta] += 1;
        }
    }
}

#[cfg(test)]
mod univmon_tests {
    use super::*;
    use crate::L2HH;

    const HEAP: usize = 32;
    const ROWS: usize = 3;
    const COLS: usize = 256;
    const LAYERS: usize = 6;

    fn layer_counters(sketch: &UnivMon, layer: usize) -> Vec<i64> {
        let L2HH::COUNT(inner) = &sketch.l2_sketch_layers[layer];
        (0..inner.rows())
            .flat_map(|r| (0..inner.cols()).map(move |c| (r, c)))
            .map(|(r, c)| inner.as_storage().query_one_counter(r, c))
            .collect()
    }

    fn heap_contents(sketch: &UnivMon, layer: usize) -> Vec<(HeapItem, i64)> {
        let mut items: Vec<(HeapItem, i64)> = sketch.hh_layers[layer]
            .heap()
            .iter()
            .map(|i| (i.key.clone(), i.count))
            .collect();
        items.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| format!("{:?}", a.0).cmp(&format!("{:?}", b.0)))
        });
        items
    }

    fn stream(n: u64) -> Vec<DataInput<'static>> {
        // A skewed stream: a few keys carry most of the weight.
        (0..n)
            .map(|i| {
                let key = match i % 10 {
                    0..=4 => i % 5,
                    5..=7 => 100 + (i % 40),
                    _ => 1_000 + (i % 900),
                };
                DataInput::U64(key)
            })
            .collect()
    }

    #[test]
    fn a_worker_picks_the_same_layers_as_a_single_threaded_insert() {
        let reference = UnivMon::init_univmon(HEAP, ROWS, COLS, LAYERS);
        for key in 0..5_000u64 {
            let input = DataInput::U64(key);
            let from_hash =
                bottom_layer_for_hash(hash64_seeded(BOTTOM_LAYER_FINDER, &input), LAYERS);
            assert_eq!(
                from_hash,
                reference.bottom_layer_for(&input),
                "key {key} routed to a different layer set"
            );
            assert!(from_hash < LAYERS);
        }
    }

    #[test]
    fn one_worker_at_threshold_one_reproduces_a_single_threaded_univmon() {
        // A threshold of 1 promotes every counter touch, and a single worker
        // applies them in stream order, so the aggregator must land in exactly
        // the state a single-threaded insert loop would.
        let inputs = stream(20_000);

        let mut reference = UnivMon::init_univmon(HEAP, ROWS, COLS, LAYERS);
        for input in &inputs {
            reference.insert(input, 1);
        }

        let mut worker =
            UnivMonOctoWorker::with_threshold(0, ROWS, COLS, LAYERS, OctoThreshold::new(1));
        let mut aggregator = UnivMonOctoAggregator::new(HEAP, ROWS, COLS, LAYERS, 1);
        for input in &inputs {
            let mut promoted = Vec::new();
            worker.process(input, &mut |d| promoted.push(d));
            for delta in promoted {
                aggregator.apply(delta);
            }
        }
        let octo = &aggregator.sketch;

        assert_eq!(octo.bucket_size, reference.bucket_size, "total weight");
        assert_eq!(
            octo.candidates_complete(),
            reference.candidates_complete(),
            "candidate completeness"
        );
        for layer in 0..LAYERS {
            assert_eq!(
                layer_counters(octo, layer),
                layer_counters(&reference, layer),
                "layer {layer} counters"
            );
            assert_eq!(
                octo.l2_sketch_layers[layer].get_l2(),
                reference.l2_sketch_layers[layer].get_l2(),
                "layer {layer} L2"
            );
            assert_eq!(
                heap_contents(octo, layer),
                heap_contents(&reference, layer),
                "layer {layer} heavy-hitter heap"
            );
        }
        assert_eq!(
            octo.calc_g_sum(|x| x, false),
            reference.calc_g_sum(|x| x, false),
            "g-sum query"
        );
    }

    #[test]
    fn promotion_conserves_every_layer_counter() {
        let inputs = stream(30_000);
        let threshold = 31u32;

        let mut reference = UnivMon::init_univmon(HEAP, ROWS, COLS, LAYERS);
        for input in &inputs {
            reference.insert(input, 1);
        }

        let mut worker =
            UnivMonOctoWorker::with_threshold(0, ROWS, COLS, LAYERS, OctoThreshold::new(threshold));
        let mut aggregator = UnivMonOctoAggregator::new(HEAP, ROWS, COLS, LAYERS, threshold);
        for input in &inputs {
            let mut promoted = Vec::new();
            worker.process(input, &mut |d| promoted.push(d));
            for delta in promoted {
                aggregator.apply(delta);
            }
        }

        for layer in 0..LAYERS {
            let promoted = layer_counters(&aggregator.sketch, layer);
            let exact = layer_counters(&reference, layer);
            let residual = worker.layers[layer].residual();
            for (cell, ((&p, &e), &r)) in
                promoted.iter().zip(exact.iter()).zip(residual).enumerate()
            {
                assert_eq!(
                    p + r as i64,
                    e,
                    "layer {layer} cell {cell}: promoted {p} plus residual {r} != {e}"
                );
                assert!(
                    r.unsigned_abs() < threshold as u8,
                    "layer {layer} cell {cell}: residual {r} should have been promoted"
                );
            }
        }
    }

    #[test]
    fn completeness_is_withdrawn_exactly_where_the_layer_thresholds() {
        let base = 8u32;
        let inputs = stream(5_000);
        let mut worker =
            UnivMonOctoWorker::with_threshold(0, ROWS, COLS, LAYERS, OctoThreshold::new(base));
        let mut aggregator = UnivMonOctoAggregator::new(HEAP, ROWS, COLS, LAYERS, base);
        for input in &inputs {
            let mut promoted = Vec::new();
            worker.process(input, &mut |d| promoted.push(d));
            for delta in promoted {
                aggregator.apply(delta);
            }
        }

        for (layer, complete) in aggregator.sketch.candidates_complete().iter().enumerate() {
            if univmon_layer_threshold(base, layer) > 1 {
                // The aggregator provably never saw the keys that stayed under
                // this layer's threshold.
                assert!(
                    !complete,
                    "layer {layer} thresholds at {} yet claims completeness",
                    univmon_layer_threshold(base, layer)
                );
            }
        }
        // The rule has to actually bite somewhere, or the test proves nothing.
        assert!(
            (0..LAYERS).any(|layer| univmon_layer_threshold(base, layer) > 1),
            "pick a base threshold that thresholds at least one layer"
        );
    }

    #[test]
    fn the_layer_threshold_tracks_how_little_traffic_a_deep_layer_sees() {
        // Layer L receives roughly n / 2^L of the stream, so its threshold
        // halves with it and bottoms out at 1.
        assert_eq!(univmon_layer_threshold(32, 0), 32);
        assert_eq!(univmon_layer_threshold(32, 1), 16);
        assert_eq!(univmon_layer_threshold(32, 5), 1);
        assert_eq!(univmon_layer_threshold(32, 40), 1, "no shift overflow");
        assert_eq!(univmon_layer_threshold(1, 0), 1);
    }

    #[test]
    fn the_worker_holds_one_byte_per_counter_and_no_keys() {
        let worker = UnivMonOctoWorker::new(0, ROWS, COLS, LAYERS);
        assert_eq!(worker.counter_bytes(), ROWS * COLS * LAYERS);
        // A UnivMon layer stores i64 counters plus a heap of owned keys.
        let parent_counter_bytes = ROWS * COLS * LAYERS * std::mem::size_of::<i64>();
        assert_eq!(worker.counter_bytes() * 8, parent_counter_bytes);
    }
}
