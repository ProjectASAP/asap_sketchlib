use asap_sketchlib::message_pack_format::portable::hll::{HllSketch, HllVariant};
use asap_sketchlib::sketches::hll::{HyperLogLogHIPImpl, HyperLogLogImpl};
use asap_sketchlib::{
    Classic, DataInput, DefaultXxHasher, ErtlMLE, HllBucketListP12, HllBucketListP14,
    HllBucketListP16, HllRegisterStorage, SketchHasher,
};

use super::*;

impl<R, H> CardinalityVariant for HyperLogLogImpl<Classic, R, H>
where
    R: HllRegisterStorage + 'static,
    H: SketchHasher + 'static,
{
    fn insert(&mut self, key: &DataInput) {
        HyperLogLogImpl::<Classic, R, H>::insert(self, key);
    }
    fn estimate(&self) -> f64 {
        HyperLogLogImpl::<Classic, R, H>::estimate(self) as f64
    }
    fn registers(&self) -> usize {
        R::NUM_REGISTERS
    }
    fn sigma_rel(&self) -> f64 {
        1.04 / (R::NUM_REGISTERS as f64).sqrt()
    }
    fn merge(&mut self, other: &dyn CardinalityVariant) {
        let other = (other as &dyn Any)
            .downcast_ref::<Self>()
            .expect("merge requires both variants to be the same concrete sketch type");
        HyperLogLogImpl::<Classic, R, H>::merge(self, other);
    }
}

impl<R, H> CardinalityVariant for HyperLogLogImpl<ErtlMLE, R, H>
where
    R: HllRegisterStorage + 'static,
    H: SketchHasher + 'static,
{
    fn insert(&mut self, key: &DataInput) {
        HyperLogLogImpl::<ErtlMLE, R, H>::insert(self, key);
    }
    fn estimate(&self) -> f64 {
        HyperLogLogImpl::<ErtlMLE, R, H>::estimate(self) as f64
    }
    fn registers(&self) -> usize {
        R::NUM_REGISTERS
    }
    fn sigma_rel(&self) -> f64 {
        1.04 / (R::NUM_REGISTERS as f64).sqrt()
    }
    fn merge(&mut self, other: &dyn CardinalityVariant) {
        let other = (other as &dyn Any)
            .downcast_ref::<Self>()
            .expect("merge requires both variants to be the same concrete sketch type");
        HyperLogLogImpl::<ErtlMLE, R, H>::merge(self, other);
    }
}

impl<R> CardinalityVariant for HyperLogLogHIPImpl<R>
where
    R: HllRegisterStorage + 'static,
{
    fn insert(&mut self, key: &DataInput) {
        HyperLogLogHIPImpl::<R>::insert(self, key);
    }
    fn estimate(&self) -> f64 {
        HyperLogLogHIPImpl::<R>::estimate(self) as f64
    }
    fn registers(&self) -> usize {
        R::NUM_REGISTERS
    }
    fn sigma_rel(&self) -> f64 {
        (std::f64::consts::LN_2 / R::NUM_REGISTERS as f64).sqrt()
    }
    fn merge(&mut self, _other: &dyn CardinalityVariant) {}
}

fn portable_key_bytes(key: &DataInput) -> Vec<u8> {
    match key {
        DataInput::I64(v) => v.to_be_bytes().to_vec(),
        DataInput::U64(v) => v.to_be_bytes().to_vec(),
        DataInput::I32(v) => v.to_be_bytes().to_vec(),
        DataInput::U32(v) => v.to_be_bytes().to_vec(),
        DataInput::F64(v) => v.to_bits().to_be_bytes().to_vec(),
        DataInput::Str(s) => s.as_bytes().to_vec(),
        DataInput::Bytes(b) => b.to_vec(),
        other => panic!("portable HllSketch has no byte encoding for {other:?}"),
    }
}

impl CardinalityVariant for HllSketch {
    fn insert(&mut self, key: &DataInput) {
        self.update(&portable_key_bytes(key));
    }
    fn estimate(&self) -> f64 {
        HllSketch::estimate(self)
    }
    fn registers(&self) -> usize {
        self.registers.len()
    }
    fn sigma_rel(&self) -> f64 {
        1.04 / (self.registers.len() as f64).sqrt()
    }
    fn merge(&mut self, other: &dyn CardinalityVariant) {
        let other = (other as &dyn Any)
            .downcast_ref::<Self>()
            .expect("merge requires both variants to be the same concrete sketch type");
        HllSketch::merge(self, other).expect("portable HLL merge of matching variant/precision");
    }
}

pub fn hyperloglog_variants() -> CardinalityVariantList {
    vec![
        (
            "HyperLogLogImpl<Classic, p8>",
            Box::new(HyperLogLogImpl::<Classic, HllBucketListP8, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogImpl<ErtlMLE, p8>",
            Box::new(HyperLogLogImpl::<ErtlMLE, HllBucketListP8, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogHIPImpl<p8>",
            Box::new(HyperLogLogHIPImpl::<HllBucketListP8>::new()),
        ),
        (
            "HyperLogLogImpl<Classic, p9>",
            Box::new(HyperLogLogImpl::<Classic, HllBucketListP9, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogImpl<ErtlMLE, p9>",
            Box::new(HyperLogLogImpl::<ErtlMLE, HllBucketListP9, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogHIPImpl<p9>",
            Box::new(HyperLogLogHIPImpl::<HllBucketListP9>::new()),
        ),
        (
            "HyperLogLogImpl<Classic, p10>",
            Box::new(HyperLogLogImpl::<Classic, HllBucketListP10, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogImpl<ErtlMLE, p10>",
            Box::new(HyperLogLogImpl::<ErtlMLE, HllBucketListP10, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogHIPImpl<p10>",
            Box::new(HyperLogLogHIPImpl::<HllBucketListP10>::new()),
        ),
        (
            "HyperLogLogImpl<Classic, p11>",
            Box::new(HyperLogLogImpl::<Classic, HllBucketListP11, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogImpl<ErtlMLE, p11>",
            Box::new(HyperLogLogImpl::<ErtlMLE, HllBucketListP11, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogHIPImpl<p11>",
            Box::new(HyperLogLogHIPImpl::<HllBucketListP11>::new()),
        ),
        (
            "HyperLogLogImpl<Classic, p12>",
            Box::new(HyperLogLogImpl::<Classic, HllBucketListP12, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogImpl<ErtlMLE, p12>",
            Box::new(HyperLogLogImpl::<ErtlMLE, HllBucketListP12, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogHIPImpl<p12>",
            Box::new(HyperLogLogHIPImpl::<HllBucketListP12>::new()),
        ),
        (
            "HyperLogLogImpl<Classic, p13>",
            Box::new(HyperLogLogImpl::<Classic, HllBucketListP13, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogImpl<ErtlMLE, p13>",
            Box::new(HyperLogLogImpl::<ErtlMLE, HllBucketListP13, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogHIPImpl<p13>",
            Box::new(HyperLogLogHIPImpl::<HllBucketListP13>::new()),
        ),
        (
            "HyperLogLogImpl<Classic, p14>",
            Box::new(HyperLogLogImpl::<Classic, HllBucketListP14, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogImpl<ErtlMLE, p14>",
            Box::new(HyperLogLogImpl::<ErtlMLE, HllBucketListP14, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogHIPImpl<p14>",
            Box::new(HyperLogLogHIPImpl::<HllBucketListP14>::new()),
        ),
        (
            "HyperLogLogImpl<Classic, p15>",
            Box::new(HyperLogLogImpl::<Classic, HllBucketListP15, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogImpl<ErtlMLE, p15>",
            Box::new(HyperLogLogImpl::<ErtlMLE, HllBucketListP15, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogHIPImpl<p15>",
            Box::new(HyperLogLogHIPImpl::<HllBucketListP15>::new()),
        ),
        (
            "HyperLogLogImpl<Classic, p16>",
            Box::new(HyperLogLogImpl::<Classic, HllBucketListP16, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogImpl<ErtlMLE, p16>",
            Box::new(HyperLogLogImpl::<ErtlMLE, HllBucketListP16, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogHIPImpl<p16>",
            Box::new(HyperLogLogHIPImpl::<HllBucketListP16>::new()),
        ),
        (
            "HyperLogLogImpl<Classic, p17>",
            Box::new(HyperLogLogImpl::<Classic, HllBucketListP17, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogImpl<ErtlMLE, p17>",
            Box::new(HyperLogLogImpl::<ErtlMLE, HllBucketListP17, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogHIPImpl<p17>",
            Box::new(HyperLogLogHIPImpl::<HllBucketListP17>::new()),
        ),
        (
            "HyperLogLogImpl<Classic, p18>",
            Box::new(HyperLogLogImpl::<Classic, HllBucketListP18, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogImpl<ErtlMLE, p18>",
            Box::new(HyperLogLogImpl::<ErtlMLE, HllBucketListP18, DefaultXxHasher>::new()),
        ),
        (
            "HyperLogLogHIPImpl<p18>",
            Box::new(HyperLogLogHIPImpl::<HllBucketListP18>::new()),
        ),
    ]
}

pub fn portable_hll_variants() -> CardinalityVariantList {
    vec![
        (
            "HllSketch<Regular> p10",
            Box::new(HllSketch::new(HllVariant::Regular, 10)),
        ),
        (
            "HllSketch<Datafusion> p10",
            Box::new(HllSketch::new(HllVariant::Datafusion, 10)),
        ),
        (
            "HllSketch<Hip> p10",
            Box::new(HllSketch::new(HllVariant::Hip, 10)),
        ),
        (
            "HllSketch<Regular> p12",
            Box::new(HllSketch::new(HllVariant::Regular, 12)),
        ),
        (
            "HllSketch<Datafusion> p12",
            Box::new(HllSketch::new(HllVariant::Datafusion, 12)),
        ),
        (
            "HllSketch<Hip> p12",
            Box::new(HllSketch::new(HllVariant::Hip, 12)),
        ),
        (
            "HllSketch<Regular> p14",
            Box::new(HllSketch::new(HllVariant::Regular, 14)),
        ),
        (
            "HllSketch<Datafusion> p14",
            Box::new(HllSketch::new(HllVariant::Datafusion, 14)),
        ),
        (
            "HllSketch<Hip> p14",
            Box::new(HllSketch::new(HllVariant::Hip, 14)),
        ),
        (
            "HllSketch<Regular> p16",
            Box::new(HllSketch::new(HllVariant::Regular, 16)),
        ),
        (
            "HllSketch<Datafusion> p16",
            Box::new(HllSketch::new(HllVariant::Datafusion, 16)),
        ),
        (
            "HllSketch<Hip> p16",
            Box::new(HllSketch::new(HllVariant::Hip, 16)),
        ),
    ]
}
