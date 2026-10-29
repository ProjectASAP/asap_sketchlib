//! ASAPv1 `kind_id` constants, one per registered sketch kind (spec §2).

pub(crate) const HLL_FAMILY: u8 = 0x01;
pub(crate) const HLL_CLASSIC: &[u8] = &[HLL_FAMILY, 0x01];
pub(crate) const HLL_ERTL: &[u8] = &[HLL_FAMILY, 0x02];
pub(crate) const HLL_HIP: &[u8] = &[HLL_FAMILY, 0x03];
pub(crate) const HLL_CLASSIC_DELTA: &[u8] = &[HLL_FAMILY, 0x81];
pub(crate) const HLL_ERTL_DELTA: &[u8] = &[HLL_FAMILY, 0x82];
pub(crate) const COUNT_MIN: &[u8] = &[0x02, 0x00];
pub(crate) const COUNT_MIN_DELTA: &[u8] = &[0x02, 0x80];
pub(crate) const COUNT_MIN_WITH_HEAP: &[u8] = &[0x03, 0x00];
pub(crate) const COUNT_MIN_WITH_HEAP_DELTA: &[u8] = &[0x03, 0x80];
pub(crate) const COUNT_SKETCH: &[u8] = &[0x04, 0x00];
pub(crate) const COUNT_SKETCH_DELTA: &[u8] = &[0x04, 0x80];
/// DDSketch: the two bucket stores and the zero count (spec §3.8).
pub(crate) const DDSKETCH: &[u8] = &[0x05, 0x01];
/// The DDSketch delta (spec §3.23).
pub(crate) const DDSKETCH_DELTA: &[u8] = &[0x05, 0x81];
/// Retired DDSketch kind that also carried `sum` / `min` / `max`: decoded,
/// with those scalars dropped, and never encoded.
pub(crate) const DDSKETCH_WITH_SCALARS: &[u8] = &[0x05, 0x00];
/// Retired delta of [`DDSKETCH_WITH_SCALARS`]: decoded, never encoded. Its
/// payload is the current delta's.
pub(crate) const DDSKETCH_WITH_SCALARS_DELTA: &[u8] = &[0x05, 0x80];
pub(crate) const KLL_FAMILY: u8 = 0x06;
pub(crate) const KLL: &[u8] = &[KLL_FAMILY, 0x00];
pub(crate) const KLL_DYNAMIC: &[u8] = &[KLL_FAMILY, 0x01];
pub(crate) const HYDRA_KLL: &[u8] = &[0x07, 0x00];
pub(crate) const HYDRA_COUNT_MIN: &[u8] = &[0x07, 0x01];
pub(crate) const HYDRA_COUNT_SKETCH: &[u8] = &[0x07, 0x02];
pub(crate) const HYDRA_HLL: &[u8] = &[0x07, 0x03];
pub(crate) const HYDRA_UNIVMON: &[u8] = &[0x07, 0x04];
pub(crate) const SET_AGGREGATOR: &[u8] = &[0x08, 0x00];
pub(crate) const SET_AGGREGATOR_DELTA: &[u8] = &[0x08, 0x80];
pub(crate) const COUNT_SKETCH_WITH_HEAP: &[u8] = &[0x0a, 0x00];
pub(crate) const COUNT_SKETCH_WITH_HEAP_DELTA: &[u8] = &[0x0a, 0x80];
pub(crate) const ELASTIC: &[u8] = &[0x0b, 0x00];
pub(crate) const COCO: &[u8] = &[0x0c, 0x00];
pub(crate) const UNIFORM_SAMPLING: &[u8] = &[0x0d, 0x00];
pub(crate) const KMV: &[u8] = &[0x0e, 0x00];
pub(crate) const UNIVMON: &[u8] = &[0x10, 0x00];
pub(crate) const UNIVMON_TWO_TIER: &[u8] = &[0x11, 0x00];
pub(crate) const EXPONENTIAL_HISTOGRAM: &[u8] = &[0x13, 0x00];
pub(crate) const EH_BUCKET_SKETCH: &[u8] = &[0x14, 0x00];
/// Bloom: the packed bit grid (spec §3.4).
pub(crate) const BLOOM: &[u8] = &[0x17, 0x01];
/// Retired Bloom kind that also carried the `inserted` counter: decoded, with
/// the counter dropped, and never encoded.
pub(crate) const BLOOM_WITH_INSERTED: &[u8] = &[0x17, 0x00];
/// Space-Saving: the answer triples and the ceiling (spec §3.5).
pub(crate) const SPACE_SAVING: &[u8] = &[0x18, 0x01];
/// Retired Space-Saving kind that also carried `total`: decoded, with `total`
/// dropped, and never encoded.
pub(crate) const SPACE_SAVING_WITH_TOTAL: &[u8] = &[0x18, 0x00];
pub(crate) const COUNT_SKETCH_WITH_L2_NORM: &[u8] = &[0x19, 0x00];
pub(crate) const UNIVMON_Q: &[u8] = &[0x1a, 0x00];
/// Count-Min with a Space-Saving summary, without its `total` (spec §3.24).
pub(crate) const COUNT_MIN_WITH_SPACE_SAVING: &[u8] = &[0x1e, 0x01];
/// Retired kind whose summary also carried `total`: decoded, never encoded.
pub(crate) const COUNT_MIN_WITH_SPACE_SAVING_WITH_TOTAL: &[u8] = &[0x1e, 0x00];
/// Count Sketch with a Space-Saving summary, without its `total` (spec §3.25).
pub(crate) const COUNT_SKETCH_WITH_SPACE_SAVING: &[u8] = &[0x1f, 0x01];
/// Retired kind whose summary also carried `total`: decoded, never encoded.
pub(crate) const COUNT_SKETCH_WITH_SPACE_SAVING_WITH_TOTAL: &[u8] = &[0x1f, 0x00];
