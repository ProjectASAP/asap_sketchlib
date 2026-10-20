//! The DDSketch delta, ASAPv1 kind `0x05 0x80` (`docs/asapv1_wire_format.md`
//! §3.23). Its metadata is the state's (§3.8); its payload is
//! `[indices, deltas, negative_indices, negative_deltas, zero_count]`.

use std::ops::RangeInclusive;

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};

use crate::message_pack_format::kind::DDSKETCH_DELTA;
use crate::message_pack_format::kind;
use crate::message_pack_format::admission_rate;
use crate::message_pack_format::delta::check_cells;
use crate::message_pack_format::envelope;

use super::wire::{check_alpha, decode_metadata, encode_metadata};
use super::{Buckets, DdSketch, GROW_CHUNK, ddsketch_indexable_bounds};


/// A change to a [`DdSketch`] state of relative accuracy `alpha`: each count of
/// `deltas` is added to the positive-store bucket its position names in
/// `indices`, each of `negative_deltas` to the negative-store bucket named in
/// `negative_indices`, and `zero_count` to the zero count. Each index array is
/// strictly ascending and inside the mapping's index range, and no count in
/// `deltas` or `negative_deltas` is zero.
#[derive(Clone, Debug)]
pub struct DdSketchDelta {
    alpha: f64,
    indices: Vec<i32>,
    deltas: Vec<u64>,
    negative_indices: Vec<i32>,
    negative_deltas: Vec<u64>,
    zero_count: u64,
    admission_rate: f64,
}

/// The delta payload: `[indices, deltas, negative_indices, negative_deltas,
/// zero_count]`.
#[derive(Serialize, Deserialize)]
struct DdDeltaPayload {
    indices: Vec<i32>,
    deltas: Vec<u64>,
    negative_indices: Vec<i32>,
    negative_deltas: Vec<u64>,
    zero_count: u64,
}

/// The bucket indices `alpha`'s mapping produces: `key(min_v)..=key(max_v)`
/// over the indexable values.
fn index_range(alpha: f64) -> RangeInclusive<i32> {
    let inv_log_gamma = 1.0 / ((1.0 + alpha) / (1.0 - alpha)).ln();
    let key = |v: f64| (v.ln() * inv_log_gamma).floor() as i32;
    let (min_v, max_v) = ddsketch_indexable_bounds(alpha);
    key(min_v)..=key(max_v)
}

/// Whether `store` stays inside `i32` — offset, highest index and length —
/// after [`Buckets::ensure`] grows it for each of `indices` in turn, computed
/// in `i64` without touching the store.
fn grows_within_i32(store: &Buckets, indices: &[i32]) -> bool {
    let chunk = GROW_CHUNK as i64;
    let mut span = store.range().map(|(l, r)| (i64::from(l), i64::from(r)));
    for &k in indices {
        let k = i64::from(k);
        let (l, r) = match span {
            None => (k - chunk / 2, k + chunk / 2 - 1),
            Some((l, r)) if k < l => (l - (l - k).max(chunk), r),
            Some((l, r)) if k > r => (l, r + (k - r).max(chunk)),
            Some(lr) => lr,
        };
        if [l, r, r - l].iter().any(|&v| i32::try_from(v).is_err()) {
            return false;
        }
        span = Some((l, r));
    }
    true
}

impl DdSketchDelta {
    /// Builds a delta from the changed buckets of a sketch of relative accuracy
    /// `alpha`. Fails when `alpha` is outside `(0, 1)` or the buckets break the
    /// rules above.
    pub fn new(
        alpha: f64,
        indices: Vec<i32>,
        deltas: Vec<u64>,
        negative_indices: Vec<i32>,
        negative_deltas: Vec<u64>,
        zero_count: u64,
    ) -> Result<Self, String> {
        check_alpha(alpha).map_err(|e| format!("DDSketch delta: {e}"))?;
        let range = index_range(alpha);
        check_cells(&indices, &deltas, range.clone())
            .map_err(|e| format!("DDSketch delta: {e}"))?;
        check_cells(&negative_indices, &negative_deltas, range)
            .map_err(|e| format!("DDSketch delta: negative store: {e}"))?;
        Ok(Self {
            alpha,
            indices,
            deltas,
            negative_indices,
            negative_deltas,
            zero_count,
            admission_rate: admission_rate::admit_all(),
        })
    }

    /// Sets the admission rate of the state this delta changes; `1` unless set.
    pub fn with_admission_rate(mut self, p: f64) -> Self {
        self.admission_rate = p;
        self
    }

    /// Serializes the delta into an ASAPv1 MessagePack envelope.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let metadata = encode_metadata(self.alpha, self.admission_rate)?;
        let payload = rmp_serde::to_vec(&DdDeltaPayload {
            indices: self.indices.clone(),
            deltas: self.deltas.clone(),
            negative_indices: self.negative_indices.clone(),
            negative_deltas: self.negative_deltas.clone(),
            zero_count: self.zero_count,
        })?;
        Ok(envelope::encode(kind::DDSKETCH_DELTA, &metadata, &payload))
    }

    /// Deserializes a delta from an ASAPv1 MessagePack envelope.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let (kind_id, metadata, payload) =
            envelope::split(bytes).map_err(RmpDecodeError::Uncategorized)?;
        if kind_id != kind::DDSKETCH_DELTA {
            return Err(RmpDecodeError::Uncategorized(format!(
                "DDSketch delta kind_id mismatch: stored {kind_id:?}, expected {DDSKETCH_DELTA:?}"
            )));
        }
        let (alpha, admission_rate) = decode_metadata(metadata)?;
        let p: DdDeltaPayload = envelope::decode_block(payload)?;
        let delta = Self::new(
            alpha,
            p.indices,
            p.deltas,
            p.negative_indices,
            p.negative_deltas,
            p.zero_count,
        )
        .map_err(RmpDecodeError::Uncategorized)?;
        Ok(delta.with_admission_rate(admission_rate))
    }
}

impl DdSketch {
    /// Adds a delta to the sketch. Each positive bucket, then each negative
    /// one, adds its count `c` and folds the bucket representative `r` into the
    /// scalars (`sum += r * c`, saturating; `min` / `max`); a non-zero
    /// `zero_count` adds to the zero count and folds `0.0` into `min` / `max`.
    /// Fails, changing nothing, when the delta's `alpha` or admission rate
    /// differs, the total sample count overflows, or a store would span past
    /// `i32`.
    pub fn merge_delta(&mut self, delta: &DdSketchDelta) -> Result<(), String> {
        if self.alpha != delta.alpha {
            return Err(format!(
                "DDSketch delta: metadata mismatch: an alpha {} delta for an alpha {} sketch",
                delta.alpha, self.alpha
            ));
        }
        admission_rate::check_equal(self.admission_rate, delta.admission_rate)
            .map_err(|e| format!("DDSketch delta: metadata mismatch: {e}"))?;
        let count = delta
            .deltas
            .iter()
            .chain(&delta.negative_deltas)
            .chain([&delta.zero_count])
            .try_fold(self.count, |acc, &c| acc.checked_add(c))
            .ok_or_else(|| "DDSketch delta: the total sample count overflows".to_string())?;
        if !grows_within_i32(&self.positive_store, &delta.indices)
            || !grows_within_i32(&self.negative_store, &delta.negative_indices)
        {
            return Err("DDSketch delta: a store would span past i32".to_string());
        }
        let stores = [
            (false, &delta.indices, &delta.deltas),
            (true, &delta.negative_indices, &delta.negative_deltas),
        ];
        for (negative, indices, deltas) in stores {
            for (&k, &c) in indices.iter().zip(deltas) {
                let store = if negative {
                    &mut self.negative_store
                } else {
                    &mut self.positive_store
                };
                store.ensure(k);
                store.counts.as_mut_slice()[(k - store.offset) as usize] += c;
                let magnitude = self.bin_representative(k);
                let r = if negative { -magnitude } else { magnitude };
                self.add_to_sum(r * c as f64);
                self.min = self.min.min(r);
                self.max = self.max.max(r);
            }
        }
        if delta.zero_count != 0 {
            self.zero_count += delta.zero_count;
            self.min = self.min.min(0.0);
            self.max = self.max.max(0.0);
        }
        self.count = count;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::structures::Vector1D;
    use crate::sketches::ddsketch::DdSketchState;

    const ALPHA: f64 = 0.01;
    /// `key(min_v)` and `key(max_v)` of §3.23 rule 5 at `ALPHA`.
    const LOWEST: i32 = -35418;
    const HIGHEST: i32 = 35448;

    type Cells<'a> = (&'a [i32], &'a [u64]);

    /// An `ALPHA` delta envelope around the given payload arrays.
    fn delta_bytes(positive: Cells, negative: Cells, zero_count: u64) -> Vec<u8> {
        let metadata = encode_metadata(ALPHA, 1.0).unwrap();
        let payload =
            rmp_serde::to_vec(&(positive.0, positive.1, negative.0, negative.1, zero_count))
                .unwrap();
        envelope::encode(kind::DDSKETCH_DELTA, &metadata, &payload)
    }

    #[test]
    fn decode_rejects_each_payload_rule() {
        let bounds: Cells = (&[LOWEST, HIGHEST], &[1, 1]);
        assert!(DdSketchDelta::from_asapv1(&delta_bytes(bounds, bounds, 0)).is_ok());
        let none: Cells = (&[], &[]);
        let rows: [(&str, Cells, Cells); 3] = [
            ("index below key(min_v)", (&[LOWEST - 1], &[1]), none),
            ("index above key(max_v)", (&[HIGHEST + 1], &[1]), none),
            (
                "negative index above key(max_v)",
                none,
                (&[HIGHEST + 1], &[1]),
            ),
        ];
        for (label, positive, negative) in rows {
            assert!(
                DdSketchDelta::from_asapv1(&delta_bytes(positive, negative, 0)).is_err(),
                "{label}"
            );
        }
    }

    #[test]
    fn decode_rejects_another_kind_and_new_another_alpha() {
        let mut bytes = delta_bytes((&[0], &[1]), (&[], &[]), 0);
        assert!(DdSketchDelta::from_asapv1(&bytes).is_ok());
        bytes[9] = 0x00;
        assert!(DdSketchDelta::from_asapv1(&bytes).is_err());
        assert!(DdSketchDelta::new(1.0, vec![], vec![], vec![], vec![], 1).is_err());
    }

    /// A delta onto an empty sketch moves `min` / `max` to the representatives
    /// it adds, and its zero count folds in `0.0`.
    #[test]
    fn merge_delta_folds_the_scalars() {
        let mut sketch = DdSketch::new(ALPHA);
        let delta = DdSketchDelta::new(ALPHA, vec![10], vec![2], vec![], vec![], 0).unwrap();
        sketch.merge_delta(&delta).unwrap();
        let r = (1.0 + ALPHA) * ((1.0 + ALPHA) / (1.0 - ALPHA)).powf(10.0);
        assert_eq!(
            (sketch.min(), sketch.max(), sketch.sum()),
            (Some(r), Some(r), 2.0 * r)
        );

        let zeros = DdSketchDelta::new(ALPHA, vec![], vec![], vec![], vec![], 3).unwrap();
        sketch.merge_delta(&zeros).unwrap();
        assert_eq!(
            (sketch.min(), sketch.zero_count(), sketch.get_count()),
            (Some(0.0), 3, 5)
        );
    }

    #[test]
    fn merge_delta_rejects_without_changing_the_state() {
        let mut sketch = DdSketch::new(ALPHA);
        sketch.add(&3.0f64);
        let next_alpha = f64::from_bits(ALPHA.to_bits() + 1);
        let other = DdSketchDelta::new(next_alpha, vec![0], vec![1], vec![], vec![], 0).unwrap();
        assert!(sketch.merge_delta(&other).is_err());
        assert_eq!(sketch.get_count(), 1);

        sketch.positive_store.counts = Vector1D::from_vec(vec![u64::MAX - 1]);
        sketch.positive_store.offset = 55;
        sketch.count = u64::MAX - 1;
        let before = sketch.to_asapv1().unwrap();
        let rows = [
            DdSketchDelta::new(ALPHA, vec![0], vec![1], vec![5], vec![1], 0),
            DdSketchDelta::new(ALPHA, vec![], vec![], vec![], vec![], 2),
        ];
        for delta in rows {
            assert!(sketch.merge_delta(&delta.unwrap()).is_err());
            assert_eq!(sketch.get_count(), u64::MAX - 1);
            assert_eq!(sketch.to_asapv1().unwrap(), before);
        }
    }

    /// A store that would grow past `i32` is refused, unchanged, instead of
    /// overflowing the index arithmetic.
    #[test]
    fn merge_delta_rejects_a_store_span_past_i32() {
        let mut sketch = DdSketch::try_from(DdSketchState {
            alpha: ALPHA,
            positive_store: Buckets {
                counts: Vector1D::from_vec(vec![1]),
                offset: i32::MAX - 1,
            },
            sum: 1e300,
            min: 1e300,
            max: 1e300,
            negative_store: Buckets::new(),
            zero_count: 0,
            admission_rate: 1.0,
        })
        .unwrap();
        let before = sketch.to_asapv1().unwrap();
        let delta = DdSketchDelta::new(ALPHA, vec![-10], vec![1], vec![], vec![], 0).unwrap();
        assert!(sketch.merge_delta(&delta).is_err());
        assert_eq!(sketch.to_asapv1().unwrap(), before);
    }
}
