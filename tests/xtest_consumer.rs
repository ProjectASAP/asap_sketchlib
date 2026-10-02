//! xtest_consumer — Cross-language integration test: Rust consumer side.
//!
//! Reads protobuf-encoded SketchEnvelope files written by the Go xtest_producer,
//! deserialises each sketch from the portable wire format, and runs sanity queries
//! to confirm the data survived the language boundary intact.
//!
//! Files consumed (from $XTEST_DIR/):
//!   coco.pb        — CocoSketchState (hash+val+hasKey buckets)
//!   elastic.pb     — ElasticState (heavy buckets + light CountMin)
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
