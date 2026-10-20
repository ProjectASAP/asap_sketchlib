use crate::common;

use common::{uniform_u64, zipf_u64};

use asap_sketchlib::input::{HydraCounter, HydraQuery};
use asap_sketchlib::{
    Bloom, CocoSketch, CountMinSketch, CountMinSketchWithHeap, CountSketch, CountSketchWithHeap,
    DataInput, DdSketch, DefaultXxHasher, EhBucketSketch, ElasticSketch, Ertl,
    ExponentialHistogram, FastPath, Hydra, HyperLogLog, Kll, KllDynamic, RegularPath, SpaceSaving,
    UnivMon, UnivMonQ, UnivMonTwoTier, Vector2D,
};

fn assert_kind(bytes: &[u8], family: u8, variant: u8, label: &str) {
    assert!(
        bytes.len() > 9,
        "{label}: envelope is too short to carry a kind_id"
    );
    assert_eq!(bytes[7], 2, "{label}: kind_id_len");
    assert_eq!(
        (bytes[8], bytes[9]),
        (family, variant),
        "{label}: kind_id bytes"
    );
}

fn frequency_stream() -> Vec<u64> {
    zipf_u64(20_000, 1_024, 1.1, 0x_C1EA_0001)
}

#[test]
fn a_ddsketch_envelope_round_trips_and_names_its_kind() {
    let mut sketch = DdSketch::new(0.01);
    for v in uniform_u64(20_000, 1_000_000, 0x_1DE7_0001) {
        sketch.add(&(1.0 + v as f64));
    }
    let bytes = sketch.to_asapv1().expect("encode");
    assert_kind(&bytes, 0x05, 0x00, "DDSketch");
    let decoded = DdSketch::from_asapv1(&bytes).expect("decode");
    assert_eq!(decoded.get_count(), sketch.get_count(), "count");
    assert_eq!(decoded.alpha(), sketch.alpha(), "alpha");
    for q in [0.0f64, 0.1, 0.5, 0.9, 1.0] {
        assert_eq!(
            decoded.get_value_at_quantile(q),
            sketch.get_value_at_quantile(q),
            "q={q}"
        );
    }
}

#[test]
fn a_bloom_envelope_round_trips_on_both_paths_and_names_its_kind() {
    let mut regular = Bloom::<RegularPath>::with_capacity(2_000, 0.01);
    let mut fast = Bloom::<FastPath>::with_capacity(2_000, 0.01);
    for i in 0..2_000i64 {
        regular.update(&DataInput::I64(i));
        fast.update(&DataInput::I64(i));
    }

    let regular_bytes = regular.to_asapv1().expect("encode regular");
    assert_kind(&regular_bytes, 0x17, 0x00, "Bloom<RegularPath>");
    let decoded_regular =
        Bloom::<RegularPath>::from_asapv1(&regular_bytes).expect("decode regular");

    let fast_bytes = fast.to_asapv1().expect("encode fast");
    assert_kind(&fast_bytes, 0x17, 0x00, "Bloom<FastPath>");
    let decoded_fast = Bloom::<FastPath>::from_asapv1(&fast_bytes).expect("decode fast");

    for i in 0..2_000i64 {
        assert!(
            decoded_regular.contains(&DataInput::I64(i)),
            "regular path lost member {i} across the wire"
        );
        assert!(
            decoded_fast.contains(&DataInput::I64(i)),
            "fast path lost member {i} across the wire"
        );
    }
    for i in 10_000..12_000i64 {
        assert_eq!(
            decoded_regular.contains(&DataInput::I64(i)),
            regular.contains(&DataInput::I64(i)),
            "regular path changed its answer for a non-member {i}"
        );
    }
    assert_eq!(
        decoded_regular.inserted(),
        regular.inserted(),
        "insert count"
    );
}

#[test]
fn a_coco_envelope_round_trips_and_names_its_kind() {
    let mut coco = CocoSketch::<DefaultXxHasher>::init_with_size_and_seed(512, 4, 0xC0C0);
    for k in frequency_stream() {
        coco.update_weighted(&format!("flow-{k}"), 1);
    }
    let bytes = coco.to_asapv1().expect("encode");
    assert_kind(&bytes, 0x0c, 0x00, "Coco");
    let decoded = CocoSketch::<DefaultXxHasher>::from_asapv1(&bytes).expect("decode");
    for k in 0..1_024u64 {
        let key = format!("flow-{k}");
        assert_eq!(
            decoded.estimate(&key),
            coco.estimate(&key),
            "flow {k} changed across the wire"
        );
    }
}

#[test]
fn an_elastic_envelope_round_trips_and_names_its_kind() {
    let mut elastic = ElasticSketch::<DefaultXxHasher>::with_heavy_and_light(64, 3, 1_024);
    for k in frequency_stream() {
        elastic.update(format!("flow-{k}"));
    }
    let bytes = elastic.to_asapv1().expect("encode");
    assert_kind(&bytes, 0x0b, 0x00, "Elastic");
    let decoded = ElasticSketch::<DefaultXxHasher>::from_asapv1(&bytes).expect("decode");
    for k in 0..1_024u64 {
        let key = format!("flow-{k}");
        assert_eq!(
            decoded.estimate(key.clone()),
            elastic.estimate(key),
            "flow {k} changed across the wire"
        );
    }
}

#[test]
fn a_space_saving_envelope_round_trips_and_names_its_kind() {
    let mut summary = SpaceSaving::<DefaultXxHasher>::with_capacity(256);
    for k in frequency_stream() {
        summary.update(&DataInput::U64(k));
    }
    let bytes = summary.to_asapv1().expect("encode");
    assert_kind(&bytes, 0x18, 0x00, "SpaceSaving");
    let decoded = SpaceSaving::<DefaultXxHasher>::from_asapv1(&bytes).expect("decode");
    for k in 0..1_024u64 {
        assert_eq!(
            decoded.estimate(&DataInput::U64(k)),
            summary.estimate(&DataInput::U64(k)),
            "key {k} changed across the wire"
        );
    }
}

#[test]
fn a_kll_dynamic_envelope_round_trips_and_names_its_kind() {
    let mut sketch = KllDynamic::<f64>::with_k_and_seed(200, 0x5EED_9001);
    for v in uniform_u64(20_000, 1_000_000, 0x_C0DE_0001) {
        sketch.update(&(v as f64));
    }
    let bytes = sketch.to_asapv1().expect("encode");
    assert_kind(&bytes, 0x06, 0x01, "KllDynamic");
    let decoded = KllDynamic::<f64>::from_asapv1(&bytes).expect("decode");
    assert_eq!(decoded.count(), sketch.count(), "retained mass");
    for q in [0.0f64, 0.1, 0.5, 0.9, 1.0] {
        assert_eq!(
            decoded.quantile(q).to_bits(),
            sketch.quantile(q).to_bits(),
            "q={q}"
        );
    }
    assert_eq!(
        decoded.to_asapv1().expect("re-encode"),
        bytes,
        "a decoded sketch must re-encode byte-identically"
    );
}

#[test]
fn the_heap_backed_matrix_envelopes_round_trip_and_name_their_kinds() {
    let stream = frequency_stream();

    let mut cms = CountMinSketchWithHeap::<Vector2D<i64>, FastPath>::new(4, 2_048, 32);
    let mut cs = CountSketchWithHeap::<Vector2D<i64>, RegularPath>::new(5, 2_048, 32);
    for k in &stream {
        cms.update(&DataInput::U64(*k));
        cs.update(&DataInput::U64(*k));
    }

    let cms_bytes = cms.to_asapv1().expect("encode CountMinSketchWithHeap");
    assert_kind(&cms_bytes, 0x03, 0x00, "CountMinSketchWithHeap");
    let decoded_cms =
        CountMinSketchWithHeap::<Vector2D<i64>, FastPath>::from_asapv1(&cms_bytes).expect("decode");

    let cs_bytes = cs.to_asapv1().expect("encode CountSketchWithHeap");
    assert_kind(&cs_bytes, 0x0a, 0x00, "CountSketchWithHeap");
    let decoded_cs =
        CountSketchWithHeap::<Vector2D<i64>, RegularPath>::from_asapv1(&cs_bytes).expect("decode");

    for k in 0..1_024u64 {
        assert_eq!(
            decoded_cms.estimate(&DataInput::U64(k)),
            cms.estimate(&DataInput::U64(k)),
            "CountMinSketchWithHeap key {k} changed across the wire"
        );
        assert_eq!(
            decoded_cs.estimate(&DataInput::U64(k)),
            cs.estimate(&DataInput::U64(k)),
            "CountSketchWithHeap key {k} changed across the wire"
        );
    }
    assert_eq!(
        decoded_cms.heap().len(),
        cms.heap().len(),
        "CountMinSketchWithHeap heap size changed across the wire"
    );
    assert_eq!(
        decoded_cs.heap().len(),
        cs.heap().len(),
        "CountSketchWithHeap heap size changed across the wire"
    );
}

fn eh_prototype() -> EhBucketSketch {
    EhBucketSketch::CountMinSketch(CountMinSketch::<Vector2D<i32>, FastPath>::with_dimensions(
        3, 512,
    ))
}

#[test]
fn an_exponential_histogram_envelope_round_trips_and_names_its_kind() {
    let mut eh = ExponentialHistogram::new(8, 1_000_000, eh_prototype());
    for (t, k) in frequency_stream().iter().take(3_000).enumerate() {
        eh.update(t as u64, &DataInput::U64(*k));
    }
    let bytes = eh.to_asapv1().expect("encode");
    assert_kind(&bytes, 0x13, 0x00, "ExponentialHistogram");
    let decoded = ExponentialHistogram::from_asapv1(&bytes).expect("decode");
    assert_eq!(decoded.bucket_count(), eh.bucket_count(), "bucket count");
    assert_eq!(decoded.min_time(), eh.min_time(), "min time");
    assert_eq!(decoded.max_time(), eh.max_time(), "max time");

    let (lo, hi) = (
        eh.min_time().expect("populated"),
        eh.max_time().expect("populated"),
    );
    let before = eh.query_interval(lo, hi).expect("merge");
    let after = decoded.query_interval(lo, hi).expect("merge");
    for k in 0..256u64 {
        assert_eq!(
            after.query(&DataInput::U64(k)).expect("answer"),
            before.query(&DataInput::U64(k)).expect("answer"),
            "key {k} changed across the wire"
        );
    }
}

#[test]
fn an_eh_sketch_list_envelope_round_trips_and_names_its_kind() {
    let mut payload = eh_prototype();
    for k in frequency_stream().iter().take(5_000) {
        payload.update(&DataInput::U64(*k));
    }
    let bytes = payload.to_asapv1().expect("encode");
    assert_kind(&bytes, 0x14, 0x00, "EhBucketSketch");
    let decoded = EhBucketSketch::from_asapv1(&bytes).expect("decode");
    assert_eq!(
        decoded.sketch_type(),
        payload.sketch_type(),
        "the decoded payload must name the same sketch"
    );
    for k in 0..512u64 {
        assert_eq!(
            decoded.query(&DataInput::U64(k)).expect("answer"),
            payload.query(&DataInput::U64(k)).expect("answer"),
            "key {k} changed across the wire"
        );
    }
}

#[test]
fn a_hydra_envelope_round_trips_and_names_the_counter_it_carries() {
    let mut hydra = Hydra::with_schema(
        4,
        512,
        ["region", "user"],
        HydraCounter::CountMinSketch(CountMinSketch::<Vector2D<i32>, FastPath>::with_dimensions(
            3, 512,
        )),
    )
    .expect("schema");
    for r in ["eu", "us", "apac"] {
        for u in ["alice", "bob", "carol"] {
            for _ in 0..40 {
                hydra
                    .update(&[r, u], &DataInput::Str("event"), None)
                    .expect("update");
            }
        }
    }
    let bytes = hydra.to_asapv1().expect("encode");
    assert_kind(&bytes, 0x07, 0x01, "Hydra<CountMinSketch>");
    let decoded = Hydra::from_asapv1(&bytes).expect("decode");
    assert_eq!(decoded.schema(), hydra.schema(), "schema");
    for r in ["eu", "us", "apac"] {
        for u in ["alice", "bob", "carol"] {
            let key = [Some(r), Some(u)];
            assert_eq!(
                decoded
                    .query_key(&key, &HydraQuery::Frequency(DataInput::Str("event")))
                    .expect("query"),
                hydra
                    .query_key(&key, &HydraQuery::Frequency(DataInput::Str("event")))
                    .expect("query"),
                "cell {r}/{u} changed across the wire"
            );
        }
    }
}

#[test]
fn the_univmon_family_envelopes_round_trip_and_name_their_kinds() {
    let stream = frequency_stream();

    let mut um = UnivMon::new(32, 5, 512, 8);
    let mut pyramid = UnivMonTwoTier::with_defaults();
    let mut q = UnivMonQ::new(Default::default()).expect("default config valid");
    for k in &stream {
        um.update_weighted(&DataInput::U32(*k as u32), 1);
        pyramid.update_weighted(&DataInput::U32(*k as u32), 1);
        q.update(&(*k as f64));
    }

    let um_bytes = um.to_asapv1().expect("encode UnivMon");
    assert_kind(&um_bytes, 0x10, 0x00, "UnivMon");
    let decoded_um = UnivMon::from_asapv1(&um_bytes).expect("decode UnivMon");
    assert_eq!(decoded_um.estimate_l1(), um.estimate_l1(), "UnivMon L1");
    assert_eq!(decoded_um.estimate_l2(), um.estimate_l2(), "UnivMon L2");
    assert_eq!(
        decoded_um.estimate_cardinality(),
        um.estimate_cardinality(),
        "UnivMon cardinality"
    );

    let pyramid_bytes = pyramid.to_asapv1().expect("encode UnivMonTwoTier");
    assert_kind(&pyramid_bytes, 0x11, 0x00, "UnivMonTwoTier");
    let decoded_pyramid =
        UnivMonTwoTier::from_asapv1(&pyramid_bytes).expect("decode UnivMonTwoTier");
    assert_eq!(
        decoded_pyramid.estimate_l1(),
        pyramid.estimate_l1(),
        "UnivMonTwoTier L1"
    );
    assert_eq!(
        decoded_pyramid.estimate_l2(),
        pyramid.estimate_l2(),
        "UnivMonTwoTier L2"
    );

    let q_bytes = q.to_asapv1().expect("encode UnivMonQ");
    assert_kind(&q_bytes, 0x1a, 0x00, "UnivMonQ");
    let decoded_q = UnivMonQ::<DefaultXxHasher>::from_asapv1(&q_bytes).expect("decode UnivMonQ");
    assert_eq!(decoded_q.count(), q.count(), "UnivMonQ count");
    assert_eq!(decoded_q.estimate_f2(), q.estimate_f2(), "UnivMonQ F2");
    for probe in [0.1f64, 0.5, 0.9] {
        assert_eq!(
            decoded_q.quantile(probe),
            q.quantile(probe),
            "UnivMonQ quantile q={probe}"
        );
    }
}

#[test]
fn an_envelope_is_refused_by_every_decoder_but_its_own() {
    let mut dds = DdSketch::new(0.01);
    for v in 1..1_000u64 {
        dds.add(&(v as f64));
    }
    let dds_bytes = dds.to_asapv1().expect("encode");

    let mut bloom = Bloom::<RegularPath>::with_capacity(100, 0.01);
    bloom.update(&DataInput::I64(1));
    let bloom_bytes = bloom.to_asapv1().expect("encode");

    assert!(
        Bloom::<RegularPath>::from_asapv1(&dds_bytes).is_err(),
        "a DDSketch envelope must not decode as a Bloom filter"
    );
    assert!(
        DdSketch::from_asapv1(&bloom_bytes).is_err(),
        "a Bloom envelope must not decode as a DDSketch"
    );
    assert!(
        SpaceSaving::<DefaultXxHasher>::from_asapv1(&dds_bytes).is_err(),
        "a DDSketch envelope must not decode as a Space-Saving summary"
    );
    assert!(
        DdSketch::from_asapv1(&[]).is_err(),
        "an empty buffer is not an envelope"
    );
    assert!(
        DdSketch::from_asapv1(&dds_bytes[..dds_bytes.len() / 2]).is_err(),
        "a truncated envelope must be refused"
    );
}

#[test]
fn the_matrix_and_quantile_envelopes_carry_their_answers_unchanged() {
    let stream = frequency_stream();
    let mut cm = CountMinSketch::<Vector2D<i64>, FastPath>::with_dimensions(4, 2_048);
    let mut cs = CountSketch::<Vector2D<i64>, RegularPath>::with_dimensions(5, 2_048);
    let mut kll: Kll<f64> = Kll::with_k_and_seed(200, 0x5EED_9002);
    let mut hll = HyperLogLog::<Ertl>::new();
    for k in &stream {
        cm.update(&DataInput::U64(*k));
        cs.update(&DataInput::U64(*k));
        kll.update(&(*k as f64));
        hll.update(&DataInput::U64(*k));
    }

    let cm_bytes = cm.to_asapv1().expect("encode CountMinSketch");
    assert_kind(&cm_bytes, 0x02, 0x00, "CountMinSketch");
    let cs_bytes = cs.to_asapv1().expect("encode CountSketch");
    assert_kind(&cs_bytes, 0x04, 0x00, "CountSketch");
    let kll_bytes = kll.to_asapv1().expect("encode KLL");
    assert_kind(&kll_bytes, 0x06, 0x00, "KLL");
    let hll_bytes = hll.to_asapv1().expect("encode HyperLogLog");
    assert_kind(&hll_bytes, 0x01, 0x02, "HyperLogLog");

    let decoded_cm =
        CountMinSketch::<Vector2D<i64>, FastPath>::from_asapv1(&cm_bytes).expect("decode");
    let decoded_cs =
        CountSketch::<Vector2D<i64>, RegularPath>::from_asapv1(&cs_bytes).expect("decode");
    let decoded_kll = Kll::<f64>::from_asapv1(&kll_bytes).expect("decode");
    let decoded_hll = HyperLogLog::<Ertl>::from_asapv1(&hll_bytes).expect("decode");

    for k in 0..1_024u64 {
        assert_eq!(
            decoded_cm.estimate(&DataInput::U64(k)),
            cm.estimate(&DataInput::U64(k)),
            "CountMinSketch key {k}"
        );
        assert_eq!(
            decoded_cs.estimate(&DataInput::U64(k)),
            cs.estimate(&DataInput::U64(k)),
            "CountSketch key {k}"
        );
    }
    for q in [0.0f64, 0.5, 1.0] {
        assert_eq!(
            decoded_kll.quantile(q).to_bits(),
            kll.quantile(q).to_bits(),
            "KLL q={q}"
        );
    }
    assert_eq!(
        decoded_hll.estimate(),
        hll.estimate(),
        "HyperLogLog estimate"
    );
}

#[cfg(feature = "experimental")]
#[test]
fn the_experimental_envelopes_round_trip_and_name_their_kinds() {
    use asap_sketchlib::{Kmv, UniformSampling};

    let mut kmv = Kmv::<DefaultXxHasher>::new(1_024);
    for k in uniform_u64(50_000, 10_000_000, 0x_5EED_1001) {
        kmv.update(&DataInput::U64(k));
    }
    let kmv_bytes = kmv.to_asapv1().expect("encode KMV");
    assert_kind(&kmv_bytes, 0x0e, 0x00, "KMV");
    let mut decoded_kmv = Kmv::<DefaultXxHasher>::from_asapv1(&kmv_bytes).expect("decode KMV");
    assert_eq!(decoded_kmv.estimate(), kmv.estimate(), "KMV estimate");

    let mut sampler = UniformSampling::with_seed(0.2, 0x_5EED_1002);
    for v in uniform_u64(20_000, 1_000_000, 0x_5EED_1003) {
        sampler.update(v as f64);
    }
    let sampler_bytes = sampler.to_asapv1().expect("encode UniformSampling");
    assert_kind(&sampler_bytes, 0x0d, 0x00, "UniformSampling");
    let decoded_sampler =
        UniformSampling::from_asapv1(&sampler_bytes).expect("decode UniformSampling");
    assert_eq!(decoded_sampler.len(), sampler.len(), "retained count");
    assert_eq!(
        decoded_sampler.total_seen(),
        sampler.total_seen(),
        "total seen"
    );
    assert_eq!(
        decoded_sampler.to_asapv1().expect("re-encode"),
        sampler_bytes,
        "a decoded sampler must re-encode byte-identically"
    );
}
