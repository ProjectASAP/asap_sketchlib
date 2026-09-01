//! scratch probe — not a real test
mod common;

use common::FreqTruth;
use common::specs::{CountMinSpec, CountSketchSpec, SIMULTANEOUS_LEVEL, Tally};
use common::streams::{uniform_u64, zipf_u64};
use common::variants::{
    VariantList, countminsketch_topk_variants, countminsketch_variants, countsketch_topk_variants,
    countsketch_variants,
};

fn truth_of(stream: &[u64]) -> FreqTruth {
    let mut t = FreqTruth::default();
    for k in stream {
        t.observe(*k as i64);
    }
    t
}

fn regimes() -> Vec<(&'static str, Vec<u64>)> {
    vec![
        ("zipf1.1 n=40k d=4096", zipf_u64(40_000, 4_096, 1.1, 0x10BE_C700)),
        ("zipf0.7 n=40k d=65536", zipf_u64(40_000, 65_536, 0.7, 0x21CF_D811)),
        ("zipf1.5 n=200k d=8192", zipf_u64(200_000, 8_192, 1.5, 0x32D0_E922)),
        ("zipf2.0 n=20k d=1024", zipf_u64(20_000, 1_024, 2.0, 0x43E1_FA33)),
        ("uniform n=40k d=4096", uniform_u64(40_000, 4_096, 0x54F2_0B44)),
        ("uniform n=150k d=65536", uniform_u64(150_000, 65_536, 0x6503_1C55)),
        ("uniform n=5k d=256", uniform_u64(5_000, 256, 0x7614_2D66)),
    ]
}

fn probe_cms(name: &str, variants: fn() -> VariantList) {
    for (rname, stream) in regimes() {
        let truth = truth_of(&stream);
        let mut worst: Option<(f64, f64, String)> = None;
        let mut bad_one_sided = 0usize;
        let mut bad_simul = 0usize;
        let mut bad_marginal: Vec<String> = vec![];
        for (label, mut sk) in variants() {
            for k in &stream {
                sk.insert(*k);
            }
            let (rows, cols) = sk.dims();
            let spec = CountMinSpec::new(rows, cols);
            let total = truth.total() as f64;
            let distinct = truth.distinct();
            let (mut o, mut s, mut m) = (Tally::default(), Tally::default(), Tally::default());
            for (key, count) in truth.pairs() {
                let est = sk.query(key as u64);
                let f = count as f64;
                o.record(est >= f, String::new);
                s.record(
                    est - f <= spec.simultaneous_bound(total, f, distinct, SIMULTANEOUS_LEVEL),
                    String::new,
                );
                m.record(est - f <= spec.marginal_bound(total, f), String::new);
            }
            if o.violations > 0 {
                bad_one_sided += 1;
            }
            if s.violations > 0 {
                bad_simul += 1;
            }
            let p = spec.marginal_failure();
            let r = m.rate();
            if r > p {
                bad_marginal.push(format!("{label} rate={r:.4} > p={p:.4}"));
            }
            let slack = r / p;
            if worst.as_ref().map(|w| slack > w.0).unwrap_or(true) {
                worst = Some((slack, r, format!("{label} rate={r:.4} p={p:.4}")));
            }
        }
        println!(
            "[{name}] {rname}: one_sided_bad={bad_one_sided} simul_bad={bad_simul} \
             marginal_bad={} worst={}",
            bad_marginal.len(),
            worst.map(|w| w.2).unwrap_or_default()
        );
        for b in bad_marginal.iter().take(5) {
            println!("      MARGINAL {b}");
        }
    }
}

fn probe_cs(name: &str, variants: fn() -> VariantList) {
    for (rname, stream) in regimes() {
        let truth = truth_of(&stream);
        let f2 = truth.f2();
        let mut bad_simul: Vec<String> = vec![];
        let mut bad_marginal: Vec<String> = vec![];
        let mut worst = 0.0f64;
        let mut worst_label = String::new();
        for (label, mut sk) in variants() {
            for k in &stream {
                sk.insert(*k);
            }
            let (rows, cols) = sk.dims();
            let spec = CountSketchSpec::new(rows, cols);
            let distinct = truth.distinct();
            let kappa = spec.simultaneous_kappa(distinct, SIMULTANEOUS_LEVEL);
            let (mut s, mut m) = (Tally::default(), Tally::default());
            for (key, count) in truth.pairs() {
                let est = sk.query(key as u64);
                let f = count as f64;
                let residual = (f2 - f * f).max(0.0).sqrt();
                s.record((est - f).abs() <= spec.scale_at(kappa, residual), String::new);
                m.record((est - f).abs() <= spec.marginal_scale(residual), String::new);
            }
            if s.violations > 0 {
                bad_simul.push(format!("{label} {}/{}", s.violations, s.checks));
            }
            let p = spec.marginal_failure();
            let r = m.rate();
            if r > p {
                bad_marginal.push(format!("{label} rate={r:.4} > p={p:.4}"));
            }
            if r / p > worst {
                worst = r / p;
                worst_label = format!("{label} rate={r:.4} p={p:.4}");
            }
        }
        println!(
            "[{name}] {rname}: simul_bad={} marginal_bad={} worst={worst_label}",
            bad_simul.len(),
            bad_marginal.len()
        );
        for b in bad_simul.iter().take(3) {
            println!("      SIMUL {b}");
        }
        for b in bad_marginal.iter().take(5) {
            println!("      MARGINAL {b}");
        }
    }
}

#[test]
fn probe() {
    std::thread::Builder::new()
        .stack_size(256 * 1024 * 1024)
        .spawn(|| {
            probe_cms("cms", countminsketch_variants);
            probe_cms("cms-topk", countminsketch_topk_variants);
            probe_cs("cs", countsketch_variants);
            probe_cs("cs-topk", countsketch_topk_variants);
        })
        .unwrap()
        .join()
        .unwrap();
}
