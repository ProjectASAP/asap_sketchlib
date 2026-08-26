//! E2E quantile pipelines on synthetic numeric streams: KLL / KLLDynamic
//! (typed + shard merge), DDSketch core & portable across distributions,
//! UnivMon-Q's full metric surface, TumblingWindow<KLL> window queries, and
//! the portable HydraKll per-key medians.

mod common;

use common::{
    NumericTruth, assert_between, assert_in_rank_band, exponential_f64, log_uniform_f64,
    normal_f64, uniform_u64,
};

use asap_sketchlib::message_pack_format::portable::ddsketch::DdSketch as PortableDds;
use asap_sketchlib::message_pack_format::portable::hydra_kll::HydraKllSketch;
use asap_sketchlib::{
    DDSketch, DataInput, KLL, KLLConfig, TumblingWindow, UnivMonQ, UnivMonQPoint,
};
use std::collections::HashMap;

const QS: [f64; 5] = [0.1, 0.25, 0.5, 0.75, 0.9];

// --------------------------------------------------------------------- KLL

#[test]
fn kll_quantile_rank_bands_and_shard_merge() {
    let values: Vec<f64> = uniform_u64(80_000, 1_000_000, 3001)
        .into_iter()
        .map(|v| v as f64)
        .chain(normal_f64(20_000, 500_000.0, 120_000.0, 3002))
        .collect();
    let truth = NumericTruth::new(values.clone());

    let mut kll = KLL::init_kll(200);
    let mut left = KLL::init_kll_with_seed(200, 77);
    let mut right = KLL::init_kll_with_seed(200, 99);
    for (i, v) in values.iter().enumerate() {
        kll.update(v);
        if i % 2 == 0 {
            left.update(v);
        } else {
            right.update(v);
        }
    }
    for &q in &QS {
        assert_in_rank_band(kll.quantile(q), &truth, q, 0.03, "KLL");
    }

    // Merging shards must preserve the merged distribution.
    let sum_pre_merge = left.count() + right.count();
    assert!(
        (sum_pre_merge as f64 - values.len() as f64).abs() / values.len() as f64 <= 0.005,
        "shard counts {sum_pre_merge} must approximate stream length {}",
        values.len()
    );
    left.merge(&right);
    assert!(
        (left.count() as f64 - values.len() as f64).abs() / values.len() as f64 <= 0.005,
        "merged count {} must approximate stream length {}",
        left.count(),
        values.len()
    );
    for &q in &QS {
        assert_in_rank_band(left.quantile(q), &truth, q, 0.03, "KLL after shard merge");
    }
}

#[test]
fn kll_i64_generic_typed_path() {
    let mut kll: KLL<i64> = KLL::init_kll(200);
    let mut truth: Vec<i64> = Vec::new();
    for v in common::uniform_u64(50_000, 10_000_000, 3003) {
        let x = v as i64;
        kll.update(&x);
        truth.push(x);
    }
    truth.sort();
    for q in [0.25f64, 0.5, 0.9] {
        let lo = truth[((q - 0.02) * truth.len() as f64) as usize];
        let hi = truth[(((q + 0.02) * truth.len() as f64) as usize).min(truth.len() - 1)];
        let est = kll.quantile(q);
        assert!(
            est >= lo as f64 && est <= hi as f64,
            "KLL<i64> q={q}: {est} outside [{lo}, {hi}]"
        );
    }
}

#[test]
fn kll_dynamic_parity_with_kll() {
    let values: Vec<f64> = normal_f64(40_000, 100.0, 15.0, 3004)
        .into_iter()
        .filter(|v| *v > 0.0)
        .collect();
    let truth = NumericTruth::new(values.clone());

    let mut a = KLL::init_kll(200);
    let mut b = asap_sketchlib::KLLDynamic::<f64>::init_kll(200);
    for v in &values {
        a.update(v);
        b.update(v);
    }
    for &q in &QS {
        assert_in_rank_band(a.quantile(q), &truth, q, 0.03, "KLL normal");
        assert_in_rank_band(b.quantile(q), &truth, q, 0.03, "KLLDynamic normal");
    }
}

// ---------------------------------------------------------------- DDSketch

#[test]
fn ddsketch_alpha_across_distributions_core_and_portable() {
    let alpha = 0.01;

    // Adversarial log-uniform stream straddling bucket edges.
    let gamma = (1.0 + alpha) / (1.0 - alpha);
    let adversarial = log_uniform_f64(30_000, gamma, 5..40, 3005);
    // Smooth heavy-tail streams.
    let normal = normal_f64(20_000, 1000.0, 250.0, 3006)
        .into_iter()
        .filter(|v| *v > 0.0);
    let exponential = exponential_f64(20_000, 1e-3, 3007);

    for (label, values) in [
        ("adversarial", adversarial),
        ("normal", normal.collect()),
        ("exponential", exponential),
    ] {
        let truth = NumericTruth::new(values.clone());
        let mut core = DDSketch::new(alpha);
        let mut port = PortableDds::new(alpha);
        for v in &values {
            core.add(v);
            port.update(*v);
        }
        assert_eq!(
            core.get_count() as usize,
            truth.len(),
            "{label}: dropped samples"
        );

        for &q in &QS {
            // Contract: relative error <= alpha vs the TRUE order statistic.
            let t = truth.quantile(q);
            if t <= 0.0 {
                continue;
            }
            let qc = core.get_value_at_quantile(q).unwrap();
            let qp = port.quantile(q).unwrap();
            assert!(
                ((qc - t) / t).abs() <= alpha * 1.05,
                "core {label} q={q}: {qc} vs {t} exceeds alpha={alpha}"
            );
            assert!(
                ((qp - t) / t).abs() <= alpha * 1.05,
                "portable {label} q={q}: {qp} vs {t} exceeds alpha={alpha}"
            );
        }
    }
}

// ---------------------------------------------------------------- UnivMonQ

#[test]
fn univmonq_full_metric_suite() {
    let mut q = UnivMonQ::new(Default::default()).expect("default config valid");
    let mut freq: HashMap<u64, u64> = HashMap::new();

    // Well-separated heavy hitters plus uniform background.
    for i in 0..40u64 {
        let weight = (i + 1) * 60;
        for _ in 0..weight {
            q.update(&(i as f64));
            *freq.entry(i).or_insert(0) += 1;
        }
    }
    let bg = uniform_u64(20_000, 900, 3008);
    let mut values: Vec<f64> = Vec::with_capacity(20_000 + 49_200);
    for k in bg {
        let key = 100u64 + k % 800;
        q.update(&(key as f64));
        *freq.entry(key).or_insert(0) += 1;
        values.push(key as f64);
    }
    for (k, c) in &freq {
        if *k < 40 {
            values.extend(std::iter::repeat_n(*k as f64, *c as usize));
        }
    }
    let n = values.len() as f64;
    let mut sorted = values.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let truth = NumericTruth::new(values.clone());

    // Exact aggregates.
    assert_eq!(q.count() as usize, values.len(), "count");
    assert_eq!(q.min(), Some(sorted[0]), "min");
    assert_eq!(q.max(), Some(sorted[sorted.len() - 1]), "max");

    // Frequency of the heaviest separated key.
    let heaviest_key = 39u64;
    let hot_est = q.estimate_frequency(heaviest_key as f64) as f64;
    assert_between(
        hot_est,
        freq[&heaviest_key] as f64 * 0.95,
        freq[&heaviest_key] as f64 * 1.05,
        "frequency of heaviest key",
    );

    // Distinct / F2 / entropy.
    assert_between(
        q.estimate_distinct(),
        freq.len() as f64 * 0.90,
        freq.len() as f64 * 1.10,
        "distinct",
    );
    let f2_truth: f64 = freq.values().map(|c| (*c as f64).powi(2)).sum();
    assert_between(q.estimate_f2(), f2_truth * 0.85, f2_truth * 1.15, "F2");
    let p: Vec<f64> = freq.values().map(|c| *c as f64 / n).collect();
    let h_truth: f64 = -p.iter().map(|p| p * p.ln()).sum::<f64>();
    assert_between(
        q.estimate_entropy(),
        h_truth * 0.90,
        h_truth * 1.10,
        "entropy (nats)",
    );

    // Ordered queries: rank bands + monotone CDF + rank()/quantile() inverse pair.
    for &qq in &QS {
        match q.quantile(qq) {
            Some(est) => assert_in_rank_band(est, &truth, qq, 0.04, "UnivMonQ quantile"),
            None => panic!("quantile({qq}) returned None with ordered_samples enabled"),
        }
    }
    let cdf_pts: Vec<UnivMonQPoint> = q.cdf();
    assert!(!cdf_pts.is_empty(), "cdf must not be empty");
    for w in cdf_pts.windows(2) {
        assert!(w[0].rank <= w[1].rank + 1e-9, "cdf ranks not monotone");
    }
    let probe = sorted[sorted.len() / 3];
    let r = q.rank(probe).expect("rank") as f64;
    let true_rank = truth.cdf(probe) * n;
    assert_between(
        r,
        true_rank * 0.94,
        true_rank * 1.06,
        "rank() vs empirical count",
    );

    // Heavy hitters among the well-separated keys (weights 1800..2400).
    let hh = q.heavy_hitters(10);
    let top_true: std::collections::HashSet<u64> = freq
        .iter()
        .filter(|(k, c)| **k >= 30 && **c >= 30 * 60)
        .map(|(k, _)| *k)
        .collect();
    assert_eq!(
        top_true.len(),
        10,
        "test setup: expected 10 separated heavy keys"
    );
    let hits = hh
        .iter()
        .filter(|(v, _)| top_true.contains(&(*v as u64)))
        .count();
    assert!(
        hits >= 8,
        "heavy hitters recovered only {hits}/10 known keys"
    );
}

// -------------------------------------------------------- TumblingWindow<KLL>

#[test]
fn tumbling_kll_window_queries() {
    let cfg = KLLConfig { k: 200, m: 8 };
    let mut tw: TumblingWindow<KLL> = TumblingWindow::new(100, 16, cfg, 4);

    let all: Vec<f64> = uniform_u64(1000, 1_000_000, 3009)
        .iter()
        .map(|v| *v as f64)
        .collect();
    for (t, v) in all.iter().enumerate() {
        tw.insert(t as u64, &DataInput::F64(*v), 999); // value param ignored for KLL
    }
    assert_eq!(
        tw.closed_count(),
        9,
        "windows [0,900) should be closed at t=999"
    );

    // query_all covers every observation.
    let merged_all = tw.query_all();
    let full_truth = NumericTruth::new(all.clone());
    for &qq in &[0.5f64, 0.9] {
        assert_in_rank_band(
            merged_all.quantile(qq),
            &full_truth,
            qq,
            0.05,
            "tumbling query_all",
        );
    }

    // query_recent(1) = active window [900,1000) + last closed [800,900).
    let recent: Vec<f64> = all[800..].to_vec();
    let recent_truth = NumericTruth::new(recent);
    let merged_recent = tw.query_recent(1);
    for &qq in &[0.5f64, 0.9] {
        assert_in_rank_band(
            merged_recent.quantile(qq),
            &recent_truth,
            qq,
            0.05,
            "tumbling query_recent(1)",
        );
    }

    // The active window alone is the exact last-100 slice.
    let active_truth = NumericTruth::new(all[900..].to_vec());
    let active_median = tw.active_sketch().quantile(0.5);
    assert_in_rank_band(
        active_median,
        &active_truth,
        0.5,
        0.06,
        "active window median",
    );
}

// --------------------------------------------- Portable HydraKll per-key

#[test]
fn portable_hydra_kll_per_key_medians() {
    let mut hk = HydraKllSketch::new(3, 256, 200);
    let mut truths: HashMap<&str, NumericTruth> = HashMap::new();
    for (name, base) in [("svc-a", 100.0f64), ("svc-b", 900.0)] {
        let vals = normal_f64(
            4000,
            base,
            base * 0.05,
            if name == "svc-a" { 3010 } else { 3011 },
        );
        truths.insert(name, NumericTruth::new(vals.clone()));
        for v in vals {
            hk.update(name, v.abs()); // HydraKll cells are KLL: positive domain
        }
    }
    for (name, truth) in truths {
        for &qq in &[0.25f64, 0.5, 0.75] {
            let est = hk.quantile(name, qq);
            assert_in_rank_band(est, &truth, qq, 0.04, &format!("HydraKll {name}"));
        }
    }
}
