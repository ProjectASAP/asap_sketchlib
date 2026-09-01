#![allow(dead_code)]

use super::FreqTruth;
use super::streams::{uniform_u64, zipf_u64};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Scale {
    N10K,
    N100K,
    N1M,
    N10M,
}

impl Scale {
    pub fn n(self) -> usize {
        match self {
            Scale::N10K => 10_000,
            Scale::N100K => 100_000,
            Scale::N1M => 1_000_000,
            Scale::N10M => 10_000_000,
        }
    }

    pub fn domain(self) -> usize {
        match self {
            Scale::N10K => 1_024,
            Scale::N100K => 8_192,
            Scale::N1M => 65_536,
            Scale::N10M => 524_288,
        }
    }

    pub fn tag(self) -> &'static str {
        match self {
            Scale::N10K => "10k",
            Scale::N100K => "100k",
            Scale::N1M => "1m",
            Scale::N10M => "10m",
        }
    }

    pub const ALL: [Scale; 4] = [Scale::N10K, Scale::N100K, Scale::N1M, Scale::N10M];
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Shape {
    Zipf070,
    Zipf110,
    Zipf150,
    Uniform,
}

impl Shape {
    pub fn tag(self) -> &'static str {
        match self {
            Shape::Zipf070 => "zipf(0.7)",
            Shape::Zipf110 => "zipf(1.1)",
            Shape::Zipf150 => "zipf(1.5)",
            Shape::Uniform => "uniform",
        }
    }

    fn draw(self, n: usize, domain: usize, seed: u64) -> Vec<u64> {
        match self {
            Shape::Zipf070 => zipf_u64(n, domain, 0.7, seed),
            Shape::Zipf110 => zipf_u64(n, domain, 1.1, seed),
            Shape::Zipf150 => zipf_u64(n, domain, 1.5, seed),
            Shape::Uniform => uniform_u64(n, domain as u64, seed),
        }
    }

    pub const ALL: [Shape; 4] = [
        Shape::Zipf070,
        Shape::Zipf110,
        Shape::Zipf150,
        Shape::Uniform,
    ];
}

const SEED_BASE: u64 = 0x10BE_C700_0000_0000;

fn seed_for(scale: Scale, shape: Shape) -> u64 {
    let s = match scale {
        Scale::N10K => 1,
        Scale::N100K => 2,
        Scale::N1M => 3,
        Scale::N10M => 4,
    };
    let d = match shape {
        Shape::Zipf070 => 1,
        Shape::Zipf110 => 2,
        Shape::Zipf150 => 3,
        Shape::Uniform => 4,
    };
    SEED_BASE | (s << 16) | d
}

pub struct Regime {
    pub scale: Scale,
    pub shape: Shape,
    pub label: String,
}

impl Regime {
    pub fn new(scale: Scale, shape: Shape) -> Self {
        let label = format!(
            "{} n={} domain={} seed={:#018x}",
            shape.tag(),
            scale.tag(),
            scale.domain(),
            seed_for(scale, shape),
        );
        Self {
            scale,
            shape,
            label,
        }
    }

    pub fn build(&self) -> (Vec<u64>, FreqTruth) {
        let stream = self
            .shape
            .draw(self.scale.n(), self.scale.domain(), seed_for(self.scale, self.shape));
        let mut truth = FreqTruth::default();
        for k in &stream {
            truth.observe(*k as i64);
        }
        (stream, truth)
    }
}

pub const SCALES_ENV: &str = "ASAP_REGIME_SCALES";
pub const DEFAULT_SCALES: [Scale; 1] = [Scale::N10K];

pub fn selected_scales() -> Vec<Scale> {
    let raw = match std::env::var(SCALES_ENV) {
        Ok(v) => v,
        Err(_) => return DEFAULT_SCALES.to_vec(),
    };
    let raw = raw.trim();
    if raw.eq_ignore_ascii_case("all") {
        return Scale::ALL.to_vec();
    }
    let mut out: Vec<Scale> = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let scale = Scale::ALL
            .iter()
            .copied()
            .find(|s| s.tag().eq_ignore_ascii_case(part))
            .unwrap_or_else(|| {
                panic!(
                    "{SCALES_ENV}: unknown scale {part:?}; expected \"all\" or a comma list of {:?}",
                    Scale::ALL.map(|s| s.tag())
                )
            });
        if !out.contains(&scale) {
            out.push(scale);
        }
    }
    if out.is_empty() {
        return DEFAULT_SCALES.to_vec();
    }
    out.sort();
    out
}

pub fn frequency_regimes() -> Vec<Regime> {
    let scales = selected_scales();
    let mut out = Vec::with_capacity(scales.len() * Shape::ALL.len());
    for scale in scales {
        for shape in Shape::ALL {
            out.push(Regime::new(scale, shape));
        }
    }
    out
}

pub fn all_frequency_regimes() -> Vec<Regime> {
    let mut out = Vec::with_capacity(Scale::ALL.len() * Shape::ALL.len());
    for scale in Scale::ALL {
        for shape in Shape::ALL {
            out.push(Regime::new(scale, shape));
        }
    }
    out
}
