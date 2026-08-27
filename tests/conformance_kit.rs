//! Reference wiring for the conformance kit: established sketches run
//! through the same standard batteries every new sketch must pass. Copy an
//! adapter from here when onboarding a new sketch (see tests/README.md).

mod common;

use common::conformance::{
    self, CardinalityOps, CardinalitySpec, FrequencyOps, MergeOps, QuantileOps, QuantileSpec,
    SignedFrequencyOps,
};
use common::{FreqTruth, zipf_u64};

use asap_sketchlib::message_pack_format::portable::ddsketch::DdSketch as PortableDds;
use asap_sketchlib::message_pack_format::portable::hll::{HllSketch, HllVariant};
use asap_sketchlib::{
    CountMin, DataInput, FastPath, HyperLogLog, HyperLogLogHIP, KLL, KLLDynamic, RegularPath,
    UnivMonQ, Vector2D,
};
use std::cell::RefCell;

// ---------------------------------------------------------------------------
// Adapters: one small impl block per sketch is all it takes to onboard.
// ---------------------------------------------------------------------------

struct CountMinAdapter(CountMin<Vector2D<i64>, FastPath>);

impl FrequencyOps<i64> for CountMinAdapter {
    fn ingest(&mut self, key: &i64) {
        self.0.insert(&DataInput::I64(*key));
    }
    fn estimate(&self, key: &i64) -> f64 {
        self.0.estimate(&DataInput::I64(*key)) as f64
    }
}

impl MergeOps for CountMinAdapter {
    fn merge_from(&mut self, other: &Self) {
        self.0.merge(&other.0);
    }
}

struct CountSketchAdapter(asap_sketchlib::Count<Vector2D<i64>, RegularPath>);

impl FrequencyOps<i64> for CountSketchAdapter {
    fn ingest(&mut self, key: &i64) {
        self.0.insert(&DataInput::I64(*key));
    }
    fn estimate(&self, key: &i64) -> f64 {
        self.0.estimate(&DataInput::I64(*key))
    }
}

impl SignedFrequencyOps<i64> for CountSketchAdapter {
    fn ingest_weighted(&mut self, key: &i64, weight: i64) {
        self.0.insert_many(&DataInput::I64(*key), weight);
    }
}

impl MergeOps for CountSketchAdapter {
    fn merge_from(&mut self, other: &Self) {
        self.0.merge(&other.0);
    }
}

struct HllClassicAdapter(HyperLogLog<asap_sketchlib::Classic>);

impl CardinalityOps for HllClassicAdapter {
    fn ingest(&mut self, key: &[u8]) {
        self.0
            .insert(&DataInput::U64(u64::from_be_bytes(key.try_into().unwrap())));
    }
    fn estimate(&self) -> f64 {
        self.0.estimate() as f64
    }
}

struct HllErtlAdapter(HyperLogLog<asap_sketchlib::ErtlMLE>);

impl CardinalityOps for HllErtlAdapter {
    fn ingest(&mut self, key: &[u8]) {
        self.0
            .insert(&DataInput::U64(u64::from_be_bytes(key.try_into().unwrap())));
    }
    fn estimate(&self) -> f64 {
        self.0.estimate() as f64
    }
}

struct HllHipAdapter(HyperLogLogHIP);

impl CardinalityOps for HllHipAdapter {
    fn ingest(&mut self, key: &[u8]) {
        self.0
            .insert(&DataInput::U64(u64::from_be_bytes(key.try_into().unwrap())));
    }
    fn estimate(&self) -> f64 {
        self.0.estimate() as f64
    }
}

struct PortableHllAdapter(HllSketch);

impl CardinalityOps for PortableHllAdapter {
    fn ingest(&mut self, key: &[u8]) {
        self.0.update(key);
    }
    fn estimate(&self) -> f64 {
        self.0.estimate()
    }
}

struct KllAdapter(KLL);

impl QuantileOps for KllAdapter {
    fn update(&mut self, value: f64) {
        KLL::update(&mut self.0, &value);
    }
    fn quantile(&self, q: f64) -> f64 {
        self.0.quantile(q)
    }
}

struct KllCachedAdapter(RefCell<KLL>);

impl QuantileOps for KllCachedAdapter {
    fn update(&mut self, value: f64) {
        self.0.borrow_mut().update(&value);
    }
    fn quantile(&self, q: f64) -> f64 {
        self.0.borrow_mut().quantile_cached(q)
    }
}

struct KllDynamicAdapter(KLLDynamic<f64>);

impl QuantileOps for KllDynamicAdapter {
    fn update(&mut self, value: f64) {
        KLLDynamic::update(&mut self.0, &value);
    }
    fn quantile(&self, q: f64) -> f64 {
        self.0.quantile(q)
    }
}

struct PortableDdsAdapter(PortableDds);

impl QuantileOps for PortableDdsAdapter {
    fn update(&mut self, value: f64) {
        self.0.update(value);
    }
    fn quantile(&self, q: f64) -> f64 {
        self.0.quantile(q).expect("non-empty sketch")
    }
}

struct UnivMonQAdapter(UnivMonQ);

impl QuantileOps for UnivMonQAdapter {
    fn update(&mut self, value: f64) {
        UnivMonQ::update(&mut self.0, &value);
    }
    fn quantile(&self, q: f64) -> f64 {
        self.0.quantile(q).expect("ordered samples enabled")
    }
}

// ---------------------------------------------------------------------------
// Battery runs
// ---------------------------------------------------------------------------

fn zipf_stream() -> Vec<i64> {
    zipf_u64(60_000, 2048, 1.1, 9001)
        .iter()
        .map(|v| *v as i64)
        .collect()
}

fn stream_truth(stream: &[i64]) -> FreqTruth {
    let mut truth = FreqTruth::default();
    for k in stream {
        truth.observe(*k);
    }
    truth
}

#[test]
fn countmin_passes_frequency_and_merge_conformance() {
    let stream = zipf_stream();
    let truth = stream_truth(&stream);
    let spec = conformance::FrequencySpec {
        one_sided: true,
        rel_tol: 0.01,
        abs_tol: 4.0,
    };

    conformance::frequency_battery(
        "CountMin<FastPath>",
        || {
            CountMinAdapter(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                4, 4096,
            ))
        },
        &stream,
        &truth,
        spec,
    )
    .assert_ok();

    conformance::merge_equivalence_battery(
        "CountMin<FastPath>",
        || {
            CountMinAdapter(CountMin::<Vector2D<i64>, FastPath>::with_dimensions(
                4, 4096,
            ))
        },
        &stream,
        spec,
    )
    .assert_ok();
}

#[test]
fn countsketch_passes_signed_frequency_conformance() {
    let stream = zipf_stream();
    let truth = stream_truth(&stream);
    let spec = conformance::FrequencySpec {
        one_sided: false,
        rel_tol: 0.06,
        abs_tol: 25.0,
    };
    let make = || {
        CountSketchAdapter(
            asap_sketchlib::Count::<Vector2D<i64>, RegularPath>::with_dimensions(5, 4096),
        )
    };

    conformance::frequency_battery("Count<Regular>", make, &stream, &truth, spec).assert_ok();
    conformance::turnstile_battery("Count<Regular>", make, 42i64).assert_ok();
    conformance::merge_equivalence_battery("Count<Regular>", make, &stream, spec).assert_ok();
}

#[test]
fn hll_variants_pass_cardinality_conformance() {
    // Distinct u64 keys; encodings are byte-identical for every adapter.
    let unique: Vec<u64> = (0..100_000u64)
        .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .collect();
    let spec = CardinalitySpec { rel_tol: 0.03 };

    conformance::cardinality_battery(
        "HyperLogLog<Classic>",
        || HllClassicAdapter(HyperLogLog::<asap_sketchlib::Classic>::new()),
        &unique,
        100_000,
        spec,
    )
    .assert_ok();

    conformance::cardinality_battery(
        "HyperLogLog<ErtlMLE>",
        || HllErtlAdapter(HyperLogLog::<asap_sketchlib::ErtlMLE>::new()),
        &unique,
        100_000,
        spec,
    )
    .assert_ok();

    conformance::cardinality_battery(
        "HyperLogLogHIP",
        || HllHipAdapter(HyperLogLogHIP::new()),
        &unique,
        100_000,
        spec,
    )
    .assert_ok();

    conformance::cardinality_battery(
        "portable HllSketch<p14>",
        || PortableHllAdapter(HllSketch::new(HllVariant::Regular, 14)),
        &unique,
        100_000,
        spec,
    )
    .assert_ok();
}

#[test]
fn kll_family_passes_quantile_conformance() {
    let values: Vec<f64> = common::normal_f64(40_000, 500.0, 80.0, 7001)
        .into_iter()
        .filter(|v| *v > 0.0)
        .collect();

    conformance::quantile_battery(
        "KLL",
        || KllAdapter(KLL::init_kll(200)),
        &values,
        QuantileSpec::default(),
    )
    .assert_ok();

    conformance::quantile_battery(
        "KLLDynamic",
        || KllDynamicAdapter(KLLDynamic::<f64>::init_kll(200)),
        &values,
        QuantileSpec::default(),
    )
    .assert_ok();

    conformance::quantile_battery(
        "PortableDds",
        || PortableDdsAdapter(PortableDds::new(0.01)),
        &values,
        QuantileSpec {
            rank_tol: 0.02,
            ..QuantileSpec::default()
        },
    )
    .assert_ok();

    conformance::quantile_battery(
        "KLL-cached",
        || KllCachedAdapter(RefCell::new(KLL::init_kll(200))),
        &values,
        QuantileSpec::default(),
    )
    .assert_ok();
}

#[test]
fn univmonq_passes_quantile_conformance() {
    let values: Vec<f64> = common::uniform_u64(30_000, 50_000, 7002)
        .into_iter()
        .map(|v| v as f64)
        .collect();
    conformance::quantile_battery(
        "UnivMonQ",
        || UnivMonQAdapter(UnivMonQ::new(Default::default()).expect("config")),
        &values,
        QuantileSpec {
            rank_tol: 0.04,
            ..QuantileSpec::default()
        },
    )
    .assert_ok();
}
