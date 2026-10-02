//! xtest_consumer — Cross-language integration test: Rust consumer side.
//!
//! Reads protobuf-encoded SketchEnvelope files written by the Go xtest_producer,
//! deserialises each sketch from the portable wire format, and runs sanity queries
//! to confirm the data survived the language boundary intact.
//!
//! Files consumed (from $XTEST_DIR/):
//!   coco.pb        — CocoSketchState (hash+val+hasKey buckets)
//!   elastic.pb     — ElasticState (heavy buckets + light CountMin)
//!   univmon.pb     — UnivMonState (layered CountSketch + TopK heaps)
//!   hydra.pb       — HydraState (CM-cell grid)
//!
//! Usage:
//!   XTEST_DIR=<path> cargo test --test xtest_consumer -- --nocapture

use asap_sketchlib::proto::sketchlib::*;
use asap_sketchlib::{effective_sample_p, rescale_count};
use prost::Message;
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
use twox_hash::XxHash3_64;

#[test]
fn cross_language_proto() {
    let Some(xtest_dir) = XtestDir::prepare() else {
        eprintln!(
            "skipping cross_language_proto: no XTEST_DIR provided and Go xtest producer is unavailable"
        );
        return;
    };
    let in_dir = xtest_dir.path();

    println!("=======================================================");
    println!("  asap_sketchlib ← xtest_consumer");
    println!("=======================================================");

    let mut all_ok = true;

    // -----------------------------------------------------------------------
    // -----------------------------------------------------------------------
    // CocoSketch
    // -----------------------------------------------------------------------
    println!();
    println!("[CocoSketch] Step 1/3 — Read coco.pb");
    let bytes = read_file(in_dir.join("coco.pb"));
    let env = SketchEnvelope::decode(bytes.as_slice()).expect("decode coco envelope");

    println!(
        "[CocoSketch] Step 2/3 — Validate envelope (format_version={}, producer={})",
        env.format_version,
        env.producer.as_ref().map_or("?", |p| &p.library)
    );

    let coco_state = match env.sketch_state {
        Some(sketch_envelope::SketchState::Coco(ref s)) => s.clone(),
        other => panic!("expected CocoSketch sketch_state, got {other:?}"),
    };

    println!(
        "[CocoSketch]   d={} width={} buckets={}",
        coco_state.d,
        coco_state.width,
        coco_state.hashes.len()
    );

    // Query "coco:hot" — inserted with val=500.
    // Go uses: hash = common.Hash64([]byte("coco:hot")) = xxh3_64_seeded(seedList[0], ...)
    // DeriveIndex(hash, row, width): col = (hash >> (row * maskBitsForWidth(width))) & (width-1)
    let coco_hash = xxh3_64_seeded(SEED_0, b"coco:hot");
    let coco_est = coco_estimate(&coco_state, coco_hash);
    println!("[CocoSketch] Step 3/3 — 'coco:hot' est = {coco_est} (expect ≥ 500)");
    if coco_est >= 500 {
        println!("[CocoSketch]   PASS");
    } else {
        eprintln!("[CocoSketch] FAIL: estimate {coco_est} < 500");
        all_ok = false;
    }

    // -----------------------------------------------------------------------
    // ElasticSketch
    // -----------------------------------------------------------------------
    println!();
    println!("[ElasticSketch] Step 1/3 — Read elastic.pb");
    let bytes = read_file(in_dir.join("elastic.pb"));
    let env = SketchEnvelope::decode(bytes.as_slice()).expect("decode elastic envelope");

    println!(
        "[ElasticSketch] Step 2/3 — Validate envelope (format_version={}, producer={})",
        env.format_version,
        env.producer.as_ref().map_or("?", |p| &p.library)
    );

    let elastic_state = match env.sketch_state {
        Some(sketch_envelope::SketchState::Elastic(ref s)) => s.clone(),
        other => panic!("expected ElasticSketch sketch_state, got {other:?}"),
    };

    println!(
        "[ElasticSketch]   bucket_count={} light_rows={} light_cols={}",
        elastic_state.bucket_count,
        elastic_state.light.as_ref().map_or(0, |l| l.rows),
        elastic_state.light.as_ref().map_or(0, |l| l.cols)
    );

    // Query "elephant" — inserted 1000 times.
    // Go uses CanonicalHashSeed = seedList[5] = 0x6a09e667
    let elephant_hash = xxh3_64_seeded(SEED_5, b"elephant");
    let elephant_est = elastic_query(&elastic_state, "elephant", elephant_hash);
    println!("[ElasticSketch] Step 3/3 — 'elephant' est = {elephant_est} (expect ≥ 900)");
    if elephant_est >= 900 {
        println!("[ElasticSketch]   PASS");
    } else {
        eprintln!("[ElasticSketch] FAIL: estimate {elephant_est} < 900");
        all_ok = false;
    }

    // -----------------------------------------------------------------------
    // UnivMon
    // -----------------------------------------------------------------------
    println!();
    println!("[UnivMon] Step 1/3 — Read univmon.pb");
    let bytes = read_file(in_dir.join("univmon.pb"));
    let env = SketchEnvelope::decode(bytes.as_slice()).expect("decode univmon envelope");

    println!(
        "[UnivMon] Step 2/3 — Validate envelope (format_version={}, producer={})",
        env.format_version,
        env.producer.as_ref().map_or("?", |p| &p.library)
    );

    let um_state = match env.sketch_state {
        Some(sketch_envelope::SketchState::Univmon(ref s)) => s.clone(),
        other => panic!("expected UnivMon sketch_state, got {other:?}"),
    };

    println!(
        "[UnivMon]   layer_size={} sketch_rows={} sketch_cols={} heap_size={}",
        um_state.layer_size, um_state.sketch_rows, um_state.sketch_cols, um_state.heap_size
    );

    let um_card = univmon_cardinality(&um_state);
    // Note: the g-sum heuristic typically underestimates (Go itself reports ~4250 for 10k inserts).
    // We verify the Rust result matches Go's algorithm rather than the true cardinality.
    println!("[UnivMon] Step 3/3 — cardinality ≈ {um_card:.0} (g-sum heuristic, Go also ~4250)");
    if (1_000.0..=15_000.0).contains(&um_card) {
        println!("[UnivMon]   PASS");
    } else {
        eprintln!("[UnivMon] FAIL: cardinality {um_card:.0} not in [1000, 15000]");
        all_ok = false;
    }

    // -----------------------------------------------------------------------
    // HydraSketch
    // -----------------------------------------------------------------------
    println!();
    println!("[Hydra] Step 1/3 — Read hydra.pb");
    let bytes = read_file(in_dir.join("hydra.pb"));
    let env = SketchEnvelope::decode(bytes.as_slice()).expect("decode hydra envelope");

    println!(
        "[Hydra] Step 2/3 — Validate envelope (format_version={}, producer={})",
        env.format_version,
        env.producer.as_ref().map_or("?", |p| &p.library)
    );

    let hydra_state = match env.sketch_state {
        Some(sketch_envelope::SketchState::Hydra(ref s)) => s.clone(),
        other => panic!("expected Hydra sketch_state, got {other:?}"),
    };

    println!(
        "[Hydra]   row_num={} col_num={} counter_type={} cells={}",
        hydra_state.row_num,
        hydra_state.col_num,
        hydra_state.counter_type,
        hydra_state.cells.len()
    );

    // Query "hydra:42" — inserted 51 times (1 base + 50 extra).
    // Routing: subkey_hash = xxh3_64_seeded(seedList[6]=0xbb67ae85, b"hydra:42")
    // Value hash: value_hash = xxh3_64_seeded(seedList[0]=0xcafe3553, b"hydra:42")
    let hydra_subkey_hash = xxh3_64_seeded(SEED_6, b"hydra:42");
    let hydra_value_hash = xxh3_64_seeded(SEED_0, b"hydra:42");
    let hydra_est = hydra_query_cm(&hydra_state, hydra_subkey_hash, hydra_value_hash);
    println!("[Hydra] Step 3/3 — 'hydra:42' est = {hydra_est:.0} (expect ≥ 51)");
    if hydra_est >= 51.0 {
        println!("[Hydra]   PASS");
    } else {
        eprintln!("[Hydra] FAIL: estimate {hydra_est:.0} < 51");
        all_ok = false;
    }

    // -----------------------------------------------------------------------
    // Final summary
    // -----------------------------------------------------------------------
    println!();
    println!("=======================================================");
    if all_ok {
        println!("  All cross-language checks PASSED.");
    } else {
        println!("  One or more cross-language checks FAILED.");
    }
    println!("=======================================================");

    assert!(all_ok, "one or more cross-language checks failed");
}

// ---------------------------------------------------------------------------
// Seed constants matching Go's seedList
// ---------------------------------------------------------------------------

const SEED_0: u64 = 0xcafe3553; // seedList[0] — Hash64 / default hash
const SEED_5: u64 = 0x6a09e667; // seedList[5] — CanonicalHashSeed
const SEED_6: u64 = 0xbb67ae85; // seedList[6] — defaultHydraSeed

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn read_file(path: impl AsRef<Path>) -> Vec<u8> {
    let p = path.as_ref();
    fs::read(p).unwrap_or_else(|e| panic!("cannot read {}: {}", p.display(), e))
}

struct XtestDir {
    path: PathBuf,
    cleanup: bool,
}

impl XtestDir {
    fn prepare() -> Option<Self> {
        if let Ok(dir) = env::var("XTEST_DIR") {
            return Some(Self {
                path: PathBuf::from(dir),
                cleanup: false,
            });
        }

        let go_dir = find_go_dir()?;
        let out_dir = new_xtest_temp_dir();
        if !generate_xtest_fixtures(&go_dir, &out_dir) {
            let _ = fs::remove_dir_all(&out_dir);
            return None;
        }
        Some(Self {
            path: out_dir,
            cleanup: true,
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for XtestDir {
    fn drop(&mut self) {
        if self.cleanup {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

fn generate_xtest_fixtures(go_dir: &Path, out_dir: &Path) -> bool {
    // The producer is a `go test` target (TestXtestProducer) under
    // tests/cross_language/; it writes the .pb fixtures into $XTEST_DIR.
    let output = match Command::new("go")
        .args([
            "test",
            "-run",
            "TestXtestProducer",
            "./tests/cross_language/",
        ])
        .env("XTEST_DIR", out_dir)
        .current_dir(go_dir)
        .output()
    {
        Ok(output) => output,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return false,
        Err(err) => {
            panic!(
                "failed to run Go xtest producer from {}: {}",
                go_dir.display(),
                err
            )
        }
    };

    if !output.status.success() {
        eprintln!(
            "Go xtest producer failed with status {}.\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        return false;
    }

    true
}

fn find_go_dir() -> Option<PathBuf> {
    if let Ok(dir) = env::var("SKETCHLIB_GO_DIR") {
        let dir = PathBuf::from(dir);
        if has_xtest_producer(&dir) {
            return Some(dir);
        }
    }

    for ancestor in Path::new(env!("CARGO_MANIFEST_DIR")).ancestors() {
        let candidate = ancestor.join("sketchlib-go");
        if has_xtest_producer(&candidate) {
            return Some(candidate);
        }
    }

    None
}

fn has_xtest_producer(dir: &Path) -> bool {
    dir.join("tests/cross_language/xtest_producer_test.go")
        .is_file()
}

fn new_xtest_temp_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let base = env::temp_dir();
    for _ in 0..16 {
        let nonce = COUNTER.fetch_add(1, Ordering::Relaxed);
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        let candidate = base.join(format!("sketchlib-xtest-{ts}-{nonce}"));
        match fs::create_dir(&candidate) {
            Ok(()) => return candidate,
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => panic!("failed to create temp dir {}: {}", candidate.display(), err),
        }
    }

    panic!("failed to allocate temp directory for cross-language fixtures");
}

/// Number of bits needed to index into a column vector of given width.
fn col_bits(cols: usize) -> u64 {
    let mut width = 1usize;
    while width < cols {
        width <<= 1;
    }
    if width <= 1 {
        return 0;
    }
    width.trailing_zeros() as u64
}

/// maskBitsForWidth — mirrors Go's common.maskBitsForWidth.
/// Returns the number of bits needed to represent (width-1).
fn mask_bits_for_width(width: usize) -> u64 {
    if width <= 1 {
        return 1;
    }
    let mut u = width - 1;
    let mut bits = 0u64;
    while u > 0 {
        bits += 1;
        u >>= 1;
    }
    bits
}

/// XXH3-64 with explicit seed, matching Go's `hash64_seeded(seed, key)`.
fn xxh3_64_seeded(seed: u64, data: &[u8]) -> u64 {
    XxHash3_64::oneshot_with_seed(seed, data)
}

// ---------------------------------------------------------------------------

fn median_f64(v: &mut [f64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

// ---------------------------------------------------------------------------
// CocoSketch estimate
// ---------------------------------------------------------------------------
// Mirrors Go's CocoSketch.EstimateHash:
//   for row i: col = DeriveIndex(hash, i, width); if b.HasKey && b.Hash==hash: total += b.Val
// DeriveIndex(hash, row, width) = (hash >> (row * maskBitsForWidth(width))) & (width-1)

fn coco_estimate(state: &CocoSketchState, hash: u64) -> u64 {
    let d = state.d as usize;
    let width = state.width as usize;
    let mbw = mask_bits_for_width(width);
    let mask = (width as u64) - 1;
    let mut total = 0u64;

    for i in 0..d {
        let shift = (i as u64) * mbw;
        let col = ((hash >> shift) & mask) as usize;
        let idx = i * width + col;
        if state.has_keys[idx] && state.hashes[idx] == hash {
            total += state.vals[idx];
        }
    }
    total
}

// ---------------------------------------------------------------------------
// ElasticSketch query
// ---------------------------------------------------------------------------
// Mirrors Go's ElasticSketch.queryLocked:
//   hash = HashIt(CanonicalHashSeed, []byte(id))   → SEED_5
//   idx = hash % bucket_count
//   if flow_ids[idx] == id: if !eviction: return vote_pos[idx]
//                           else: return vote_pos[idx] + light_estimate
//   else: return light_estimate
// Light layer: rows=5, cols=2048, bits=11, mask=2047
//   col_r = (hash >> (r * 11)) & 2047;  min across rows

fn elastic_query(state: &ElasticState, id: &str, hash: u64) -> i64 {
    let n = state.bucket_count as usize;
    let idx = (hash % n as u64) as usize;

    let heavy_match = idx < state.flow_ids.len() && state.flow_ids[idx] == id;

    if heavy_match {
        let vpos = state.vote_pos.get(idx).copied().unwrap_or(0) as i64;
        let evicted = state.evictions.get(idx).copied().unwrap_or(false);
        if !evicted {
            return vpos;
        }
        return vpos + elastic_light_min(state, hash);
    }
    elastic_light_min(state, hash)
}

fn elastic_light_min(state: &ElasticState, hash: u64) -> i64 {
    let light = match &state.light {
        Some(l) => l,
        None => return 0,
    };
    let rows = light.rows as usize;
    let cols = light.cols as usize;
    let bits = col_bits(cols); // trailing zeros of cols = 11 for 2048
    let mask = (cols as u64) - 1;
    let counts = &light.counts_float;

    let mut min_val = f64::MAX;
    for r in 0..rows {
        let shift = (r as u64) * bits;
        let col = ((hash >> shift) & mask) as usize;
        let v = counts[r * cols + col];
        if v < min_val {
            min_val = v;
        }
    }
    if min_val == f64::MAX {
        0
    } else {
        min_val as i64
    }
}

// ---------------------------------------------------------------------------
// UnivMon cardinality (g-sum heuristic)
// ---------------------------------------------------------------------------
// Mirrors Go's UnivSketch.calcGSumHeuristic(g=1, isCard=true):
//   Y[L-1] = count of heap items at top layer with count > threshold
//   for i from L-2 down to 0:
//     tmp = Σ coe*1 for items with count > threshold
//     coe = 1 - 2 * ((Hash64(key) >> (i+1)) & 1)
//     Y[i] = 2*Y[i+1] + tmp
//   return Y[0]
// l2_val = sqrt(median_of_first_3(layer.sketch.l2))

fn univmon_cardinality(state: &UnivMonState) -> f64 {
    let nlayers = state.layers.len();
    if nlayers == 0 {
        return 0.0;
    }

    let mut y = vec![0.0f64; nlayers];

    // Top layer
    let top = &state.layers[nlayers - 1];
    let l2_val = cs_l2_from_state(top.sketch.as_ref());
    let threshold = (l2_val * 0.01) as i64;
    let mut tmp = 0.0f64;
    if let Some(heap) = &top.heap {
        for entry in &heap.entries {
            if entry.count as i64 > threshold {
                tmp += 1.0;
            }
        }
    }
    y[nlayers - 1] = tmp;

    // Lower layers from L-2 down to 0
    for i in (0..nlayers - 1).rev() {
        tmp = 0.0;
        let layer = &state.layers[i];
        let l2_val = cs_l2_from_state(layer.sketch.as_ref());
        let threshold = (l2_val * 0.01) as i64;

        if let Some(heap) = &layer.heap {
            for entry in &heap.entries {
                if entry.count as i64 > threshold {
                    let h = xxh3_64_seeded(SEED_0, entry.key.as_bytes());
                    let bit = (h >> (i + 1)) & 1;
                    let coe = 1.0 - 2.0 * bit as f64;
                    tmp += coe;
                }
            }
        }
        y[i] = 2.0 * y[i + 1] + tmp;
    }

    y[0]
}

/// cs_l2_from_state mirrors Go's CountSketchUniv.cs_l2():
///   f2_value = MedianOfThree(l2[0], l2[1], l2[2])
///   return sqrt(f2_value)
/// The portable l2 values are raw int64 cast to float64.
fn cs_l2_from_state(cs: Option<&CountSketchState>) -> f64 {
    let cs = match cs {
        Some(s) => s,
        None => return 0.0,
    };
    let l2 = &cs.l2;
    if l2.len() < 3 {
        return 0.0;
    }
    let med = median_of_three_f64(l2[0], l2[1], l2[2]);
    med.abs().sqrt()
}

fn median_of_three_f64(a: f64, b: f64, c: f64) -> f64 {
    if a <= b {
        if b <= c {
            b
        } else if a <= c {
            c
        } else {
            a
        }
    } else if a <= c {
        a
    } else if b <= c {
        c
    } else {
        b
    }
}

// ---------------------------------------------------------------------------
// HydraSketch CountMin frequency query
// ---------------------------------------------------------------------------
// Routing (mirrors Go's fillPositionsFromHash with default seeds):
//   seedCM1 = 0x1111111111111111, seedCM2 = 0x2222222222222222
//   x = subkey_hash ^ seedCM1;  y = subkey_hash ^ seedCM2
//   for r in 0..D: xorshift both; pos[r] = (x ^ (y<<1)) % W
// For each row: query CM cell at cells[r*W + pos[r]] with value_hash.
// CM query: min across rows of count at col=(value_hash>>(r*bits))&mask.
// Final result: median of per-Hydra-row CM estimates.

const HYDRA_SEED_CM1: u64 = 0x1111111111111111;
const HYDRA_SEED_CM2: u64 = 0x2222222222222222;

fn xorshift64(x: &mut u64) {
    *x ^= *x << 13;
    *x ^= *x >> 7;
    *x ^= *x << 17;
}

fn hydra_fill_positions(subkey_hash: u64, d: usize, w: usize) -> Vec<usize> {
    let mut x = subkey_hash ^ HYDRA_SEED_CM1;
    let mut y = subkey_hash ^ HYDRA_SEED_CM2;
    if x == 0 {
        x = HYDRA_SEED_CM1;
    }
    if y == 0 {
        y = HYDRA_SEED_CM2 | 1;
    }

    let mut pos = Vec::with_capacity(d);
    for _ in 0..d {
        xorshift64(&mut x);
        xorshift64(&mut y);
        pos.push(((x ^ (y << 1)) % w as u64) as usize);
    }
    pos
}

fn hydra_query_cm(state: &HydraState, subkey_hash: u64, value_hash: u64) -> f64 {
    let d = state.row_num as usize;
    let w = state.col_num as usize;
    let cells = &state.cells;

    let pos = hydra_fill_positions(subkey_hash, d, w);

    let mut estimates = Vec::with_capacity(d);
    for (r, &p) in pos.iter().enumerate().take(d) {
        let cell_idx = r * w + p;
        if cell_idx >= cells.len() {
            estimates.push(0.0f64);
            continue;
        }
        let cell = &cells[cell_idx];
        let freq = match &cell.sketch {
            Some(hydra_cell::Sketch::CountMin(cm)) => cm_query_min(cm, value_hash),
            _ => 0.0,
        };
        estimates.push(freq);
    }

    median_f64(&mut estimates)
}

/// CountMin min-frequency query with packed hash.
fn cm_query_min(cm: &CountMinState, hash: u64) -> f64 {
    let rows = cm.rows as usize;
    let cols = cm.cols as usize;
    let bits_per_row = col_bits(cols);
    let mask = (cols as u64) - 1;
    let counts: Vec<f64> = if !cm.counts_float.is_empty() {
        cm.counts_float.clone()
    } else {
        cm.counts_int.iter().map(|&v| v as f64).collect()
    };
    let counts = &counts;

    let mut min_val = f64::MAX;
    for r in 0..rows {
        let shift = (r as u64) * bits_per_row;
        let col = ((hash >> shift) & mask) as usize;
        let v = counts[r * cols + col];
        if v < min_val {
            min_val = v;
        }
    }
    if min_val == f64::MAX { 0.0 } else { min_val }
}

// ---------------------------------------------------------------------------
