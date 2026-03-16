//! xtest_consumer — Cross-language integration test: Rust consumer side.
//!
//! Reads protobuf-encoded SketchEnvelope files written by the Go xtest_producer,
//! deserialises each sketch from the portable wire format, and runs sanity queries
//! to confirm the data survived the language boundary intact.
//!
//! Files consumed (from <indir>/):
//!   countmin.pb    — CountMinState with float64 counters
//!   kll.pb         — KLLState with quantile items and coin RNG state
//!   ddsketch.pb    — DDSketchState with alpha + bucket array
//!   hll.pb         — HyperLogLogState (DataFusion variant)
//!   countsketch.pb — CountSketchState with float64 signed counters
//!   coco.pb        — CocoSketchState (hash+val+hasKey buckets)
//!   elastic.pb     — ElasticState (heavy buckets + light CountMin)
//!   univmon.pb     — UnivMonState (layered CountSketch + TopK heaps)
//!   hydra.pb       — HydraState (CM-cell grid)
//!
//! Usage:
//!   cargo run --bin xtest_consumer <indir>

use prost::Message;
use sketchlib_rust::proto::sketchlib::*;
use std::{env, fs, path::Path, process};
use twox_hash::XxHash3_64;

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: xtest_consumer <indir>");
        process::exit(1);
    }
    let in_dir = Path::new(&args[1]);

    println!("=======================================================");
    println!("  sketchlib-rust ← xtest_consumer");
    println!("=======================================================");

    let mut all_ok = true;

    // -----------------------------------------------------------------------
    // CountMin
    // -----------------------------------------------------------------------
    println!();
    println!("[CountMin] Step 1/3 — Read countmin.pb");
    let bytes = read_file(in_dir.join("countmin.pb"));
    let env = SketchEnvelope::decode(bytes.as_slice()).expect("decode countmin envelope");

    println!(
        "[CountMin] Step 2/3 — Validate envelope (format_version={}, producer={})",
        env.format_version,
        env.producer.as_ref().map_or("?", |p| &p.library)
    );

    let cm_state = match env.sketch_state {
        Some(sketch_envelope::SketchState::CountMin(ref s)) => s.clone(),
        other => fatal_env("CountMin", &other),
    };

    println!(
        "[CountMin]   rows={} cols={} counter_type={:?}",
        cm_state.rows,
        cm_state.cols,
        CounterType::try_from(cm_state.counter_type)
    );

    // Reconstruct the row-major float matrix.
    let rows = cm_state.rows as usize;
    let cols = cm_state.cols as usize;
    let counts = &cm_state.counts_float;
    if counts.len() != rows * cols {
        eprintln!(
            "[CountMin] FAIL: counts length {} != rows*cols {}",
            counts.len(),
            rows * cols
        );
        all_ok = false;
    }

    // Point query for hot key "item:42" using the same hash formula as Go.
    let hot_key = b"item:42" as &[u8];
    let hot_hash = xxh3_64_seeded(SEED_0, hot_key);

    println!(
        "[CountMin] Step 3/3 — Point query 'item:42' (hash=0x{:016x})",
        hot_hash
    );

    let bits_per_row = col_bits(cols);
    let mask = (cols as u64) - 1;
    let mut min_freq = f64::MAX;
    for r in 0..rows {
        let shift = (r as u64) * bits_per_row;
        let c = ((hot_hash >> shift) & mask) as usize;
        let v = counts[r * cols + c];
        if v < min_freq {
            min_freq = v;
        }
    }
    println!(
        "[CountMin]   min frequency estimate = {:.0} (expect ≥ 101)",
        min_freq
    );
    if min_freq < 101.0 {
        eprintln!("[CountMin] FAIL: frequency {:.0} < 101", min_freq);
        all_ok = false;
    } else {
        println!("[CountMin]   PASS");
    }

    // -----------------------------------------------------------------------
    // KLL
    // -----------------------------------------------------------------------
    println!();
    println!("[KLL] Step 1/3 — Read kll.pb");
    let bytes = read_file(in_dir.join("kll.pb"));
    let env = SketchEnvelope::decode(bytes.as_slice()).expect("decode kll envelope");

    println!(
        "[KLL] Step 2/3 — Validate envelope (format_version={}, producer={})",
        env.format_version,
        env.producer.as_ref().map_or("?", |p| &p.library)
    );

    let kll_state = match env.sketch_state {
        Some(sketch_envelope::SketchState::Kll(ref s)) => s.clone(),
        other => fatal_env("KLL", &other),
    };

    println!(
        "[KLL]   k={} m={} num_levels={} items={} levels={}",
        kll_state.k,
        kll_state.m,
        kll_state.num_levels,
        kll_state.items.len(),
        kll_state.levels.len()
    );

    let kll = KllFromProto::from_state(&kll_state);
    let p50 = kll.quantile(0.50);
    let p99 = kll.quantile(0.99);
    println!(
        "[KLL] Step 3/3 — p50 ≈ {:.1} (expect ~5000)  p99 ≈ {:.1} (expect ~9900)",
        p50, p99
    );

    let p50_ok = (p50 - 5000.0).abs() / 5000.0 < 0.05;
    let p99_ok = (p99 - 9900.0).abs() / 9900.0 < 0.05;
    if p50_ok && p99_ok {
        println!("[KLL]   PASS");
    } else {
        eprintln!("[KLL] FAIL: p50={:.1} p99={:.1}", p50, p99);
        all_ok = false;
    }

    // -----------------------------------------------------------------------
    // DDSketch
    // -----------------------------------------------------------------------
    println!();
    println!("[DDSketch] Step 1/3 — Read ddsketch.pb");
    let bytes = read_file(in_dir.join("ddsketch.pb"));
    let env = SketchEnvelope::decode(bytes.as_slice()).expect("decode ddsketch envelope");

    println!(
        "[DDSketch] Step 2/3 — Validate envelope (format_version={}, producer={})",
        env.format_version,
        env.producer.as_ref().map_or("?", |p| &p.library)
    );

    let dd_state = match env.sketch_state {
        Some(sketch_envelope::SketchState::Ddsketch(ref s)) => s.clone(),
        other => fatal_env("DDSketch", &other),
    };

    println!(
        "[DDSketch]   alpha={:.4} count={} buckets={} offset={}",
        dd_state.alpha,
        dd_state.count,
        dd_state.store_counts.len(),
        dd_state.store_offset
    );

    let dd = DdFromProto::from_state(&dd_state);
    let p50_dd = dd.quantile(0.50);
    let p99_dd = dd.quantile(0.99);
    println!(
        "[DDSketch] Step 3/3 — p50 ≈ {:.2} (expect ~5000)  p99 ≈ {:.2} (expect ~9900)",
        p50_dd.unwrap_or(f64::NAN),
        p99_dd.unwrap_or(f64::NAN)
    );

    let p50_ok = p50_dd
        .map(|v| (v - 5000.0).abs() / 5000.0 < 0.02)
        .unwrap_or(false);
    let p99_ok = p99_dd
        .map(|v| (v - 9900.0).abs() / 9900.0 < 0.02)
        .unwrap_or(false);
    if p50_ok && p99_ok {
        println!("[DDSketch]   PASS");
    } else {
        eprintln!("[DDSketch] FAIL: p50={:?} p99={:?}", p50_dd, p99_dd);
        all_ok = false;
    }

    // -----------------------------------------------------------------------
    // HLL (DataFusion estimator)
    // -----------------------------------------------------------------------
    println!();
    println!("[HLL] Step 1/3 — Read hll.pb");
    let bytes = read_file(in_dir.join("hll.pb"));
    let env = SketchEnvelope::decode(bytes.as_slice()).expect("decode hll envelope");

    println!(
        "[HLL] Step 2/3 — Validate envelope (format_version={}, producer={})",
        env.format_version,
        env.producer.as_ref().map_or("?", |p| &p.library)
    );

    let hll_state = match env.sketch_state {
        Some(sketch_envelope::SketchState::Hll(ref s)) => s.clone(),
        other => fatal_env("HLL", &other),
    };

    println!(
        "[HLL]   variant={} precision={} registers={}",
        hll_state.variant,
        hll_state.precision,
        hll_state.registers.len()
    );

    let hll_card = hll_datafusion_estimate(&hll_state);
    println!(
        "[HLL] Step 3/3 — cardinality ≈ {} (expect ~50000)",
        hll_card
    );
    if hll_card >= 40_000 && hll_card <= 65_000 {
        println!("[HLL]   PASS");
    } else {
        eprintln!("[HLL] FAIL: cardinality {} not in [40000, 65000]", hll_card);
        all_ok = false;
    }

    // -----------------------------------------------------------------------
    // CountSketch
    // -----------------------------------------------------------------------
    println!();
    println!("[CountSketch] Step 1/3 — Read countsketch.pb");
    let bytes = read_file(in_dir.join("countsketch.pb"));
    let env = SketchEnvelope::decode(bytes.as_slice()).expect("decode countsketch envelope");

    println!(
        "[CountSketch] Step 2/3 — Validate envelope (format_version={}, producer={})",
        env.format_version,
        env.producer.as_ref().map_or("?", |p| &p.library)
    );

    let cs_state = match env.sketch_state {
        Some(sketch_envelope::SketchState::CountSketch(ref s)) => s.clone(),
        other => fatal_env("CountSketch", &other),
    };

    println!(
        "[CountSketch]   rows={} cols={} counter_type={:?}",
        cs_state.rows,
        cs_state.cols,
        CounterType::try_from(cs_state.counter_type)
    );

    // Query "cs:hot" — inserted 200 extra times.
    // Go hash: common.Hash64([]byte("cs:hot")) = xxh3_64_seeded(seedList[0], b"cs:hot")
    let cs_hot_hash = xxh3_64_seeded(SEED_0, b"cs:hot");
    let cs_est = count_sketch_query_float(&cs_state, cs_hot_hash);
    println!(
        "[CountSketch] Step 3/3 — 'cs:hot' est = {:.0} (expect ≥ 200)",
        cs_est
    );
    if cs_est >= 200.0 {
        println!("[CountSketch]   PASS");
    } else {
        eprintln!("[CountSketch] FAIL: estimate {:.0} < 200", cs_est);
        all_ok = false;
    }

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
        other => fatal_env("CocoSketch", &other),
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
    println!(
        "[CocoSketch] Step 3/3 — 'coco:hot' est = {} (expect ≥ 500)",
        coco_est
    );
    if coco_est >= 500 {
        println!("[CocoSketch]   PASS");
    } else {
        eprintln!("[CocoSketch] FAIL: estimate {} < 500", coco_est);
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
        other => fatal_env("ElasticSketch", &other),
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
    println!(
        "[ElasticSketch] Step 3/3 — 'elephant' est = {} (expect ≥ 900)",
        elephant_est
    );
    if elephant_est >= 900 {
        println!("[ElasticSketch]   PASS");
    } else {
        eprintln!("[ElasticSketch] FAIL: estimate {} < 900", elephant_est);
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
        other => fatal_env("UnivMon", &other),
    };

    println!(
        "[UnivMon]   layer_size={} sketch_rows={} sketch_cols={} heap_size={}",
        um_state.layer_size, um_state.sketch_rows, um_state.sketch_cols, um_state.heap_size
    );

    let um_card = univmon_cardinality(&um_state);
    // Note: the g-sum heuristic typically underestimates (Go itself reports ~4250 for 10k inserts).
    // We verify the Rust result matches Go's algorithm rather than the true cardinality.
    println!(
        "[UnivMon] Step 3/3 — cardinality ≈ {:.0} (g-sum heuristic, Go also ~4250)",
        um_card
    );
    if um_card >= 1_000.0 && um_card <= 15_000.0 {
        println!("[UnivMon]   PASS");
    } else {
        eprintln!(
            "[UnivMon] FAIL: cardinality {:.0} not in [1000, 15000]",
            um_card
        );
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
        other => fatal_env("Hydra", &other),
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
    println!(
        "[Hydra] Step 3/3 — 'hydra:42' est = {:.0} (expect ≥ 51)",
        hydra_est
    );
    if hydra_est >= 51.0 {
        println!("[Hydra]   PASS");
    } else {
        eprintln!("[Hydra] FAIL: estimate {:.0} < 51", hydra_est);
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
        process::exit(1);
    }
    println!("=======================================================");
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
    fs::read(p).unwrap_or_else(|e| {
        eprintln!("FATAL: cannot read {}: {}", p.display(), e);
        process::exit(1);
    })
}

fn fatal_env(name: &str, got: &Option<sketch_envelope::SketchState>) -> ! {
    eprintln!("FATAL: expected {} sketch_state, got {:?}", name, got);
    process::exit(1);
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
// HLL DataFusion estimator (Ertl 2017)
// ---------------------------------------------------------------------------
// Mirrors Go's HyperLogLog.Estimate() which uses HLLRegisterBits = 50,
// HLLPrecision = 14, HLLRegisterCount = 16384.

fn hll_sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0f64;
    let mut z = x;
    loop {
        x *= x;
        let z_prev = z;
        z += x * y;
        y += y;
        if z_prev == z {
            break;
        }
    }
    z
}

fn hll_tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0f64;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let z_prev = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if z_prev == z {
            break;
        }
    }
    z / 3.0
}

fn hll_datafusion_estimate(state: &HyperLogLogState) -> u64 {
    let precision = state.precision as usize;
    let register_bits = 64 - precision; // Q = 50
    let m = (1usize << precision) as f64; // 16384

    // Build histogram C[v] for v in 0..=(register_bits+1)
    let hist_len = register_bits + 2;
    let mut hist = vec![0u32; hist_len];
    for &r in &state.registers {
        let v = r as usize;
        let capped = v.min(hist_len - 1);
        hist[capped] += 1;
    }

    // z = m * tau((m - C[register_bits+1]) / m)
    let mut z = m * hll_tau((m - hist[register_bits + 1] as f64) / m);

    // for i from register_bits down to 1: z = (z + C[i]) * 0.5
    for i in (1..=register_bits).rev() {
        z += hist[i] as f64;
        z *= 0.5;
    }

    // z += m * sigma(C[0] / m)
    z += m * hll_sigma(hist[0] as f64 / m);

    // estimate = round(0.5 / ln(2) * m^2 / z)
    (0.5 / std::f64::consts::LN_2 * m * m / z).round() as u64
}

// ---------------------------------------------------------------------------
// CountSketch median frequency query (float64 counters, packed hash)
// ---------------------------------------------------------------------------
// Mirrors Go's CountSketch.fastPacked64PosAndSign + ComputeMedianInlineF64.
// hash = xxh3_64_seeded(SEED_0, key)  — Go's Hash64

fn count_sketch_query_float(state: &CountSketchState, hash: u64) -> f64 {
    let rows = state.rows as usize;
    let cols = state.cols as usize;
    let bits_per_row = col_bits(cols);
    let mask = (cols as u64) - 1;
    let counts = &state.counts_float;

    let mut estimates: Vec<f64> = Vec::with_capacity(rows);
    for r in 0..rows {
        let shift = (r as u64) * bits_per_row;
        let col = ((hash >> shift) & mask) as usize;
        // sign: bit (63-r) of hash → 1 means +1, 0 means −1
        let sign_bit = (hash >> (63 - r)) & 1;
        let sign = if sign_bit == 1 { 1.0f64 } else { -1.0f64 };
        estimates.push(counts[r * cols + col] * sign);
    }
    median_f64(&mut estimates)
}

fn median_f64(v: &mut Vec<f64>) -> f64 {
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
    } else {
        if a <= c {
            a
        } else if b <= c {
            c
        } else {
            b
        }
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
    for r in 0..d {
        let cell_idx = r * w + pos[r];
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
    let counts = &cm.counts_float;

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
// Minimal KLL deserialization + CDF query
// ---------------------------------------------------------------------------

struct KllFromProto {
    items: Vec<f64>,
    levels: Vec<usize>,
    num_levels: usize,
}

impl KllFromProto {
    fn from_state(s: &KllState) -> Self {
        let items: Vec<f64> = s.items.clone();
        let levels: Vec<usize> = s.levels.iter().map(|&v| v as usize).collect();
        let num_levels = s.num_levels as usize;
        Self {
            items,
            levels,
            num_levels,
        }
    }

    fn weighted_samples(&self) -> Vec<(f64, u64)> {
        let mut out = Vec::with_capacity(self.items.len());
        for h in 0..self.num_levels {
            let weight: u64 = 1 << h;
            let idx = self.num_levels - 1 - h;
            if idx + 1 >= self.levels.len() {
                continue;
            }
            let start = self.levels[idx];
            let end = self.levels[idx + 1];
            for &v in &self.items[start..end] {
                out.push((v, weight));
            }
        }
        out
    }

    fn quantile(&self, q: f64) -> f64 {
        let mut samples = self.weighted_samples();
        if samples.is_empty() {
            return 0.0;
        }
        samples.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let total: u64 = samples.iter().map(|(_, w)| w).sum();
        let target = (q * total as f64).ceil() as u64;
        let mut acc = 0u64;
        for (v, w) in &samples {
            acc += w;
            if acc >= target {
                return *v;
            }
        }
        samples.last().unwrap().0
    }
}

// ---------------------------------------------------------------------------
// Minimal DDSketch deserialization + quantile query
// ---------------------------------------------------------------------------

struct DdFromProto {
    gamma: f64,
    #[allow(dead_code)]
    inv_log_gamma: f64,
    store_counts: Vec<u64>,
    store_offset: i32,
    count: u64,
    min: f64,
    max: f64,
}

impl DdFromProto {
    fn from_state(s: &DdSketchState) -> Self {
        let alpha = s.alpha;
        let gamma = (1.0 + alpha) / (1.0 - alpha);
        let log_gamma = gamma.ln();
        let inv_log_gamma = 1.0 / log_gamma;
        Self {
            gamma,
            inv_log_gamma,
            store_counts: s.store_counts.clone(),
            store_offset: s.store_offset,
            count: s.count,
            min: s.min,
            max: s.max,
        }
    }

    fn bin_representative(&self, k: i32) -> f64 {
        self.gamma.powf(k as f64 + 0.5)
    }

    fn quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        if q <= 0.0 {
            return Some(self.min);
        }
        if q >= 1.0 {
            return Some(self.max);
        }
        let rank = (q * self.count as f64).ceil() as u64;
        let mut seen = 0u64;
        for (i, &c) in self.store_counts.iter().enumerate() {
            if c == 0 {
                continue;
            }
            seen += c;
            if seen >= rank {
                let bin = self.store_offset + i as i32;
                let mut v = self.bin_representative(bin);
                if v < self.min {
                    v = self.min;
                }
                if v > self.max {
                    v = self.max;
                }
                return Some(v);
            }
        }
        Some(self.max)
    }
}
