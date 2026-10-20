//! ASAPv1 wire serialization for the dynamic KLL quantile sketch.
//!
//! Child submodule of [`crate::sketches::kll_dynamic`]: it holds the
//! `to_asapv1` / `from_asapv1` impls for [`KllDynamic`]
//! (kind_id `0x06 0x01`). The wire DTOs (metadata, payload, coin) and the
//! fail-closed validation are **shared** with the compact KLL and live in
//! [`crate::sketches::kll::wire`]; only the kind_id differs. See
//! `docs/asapv1_wire_format.md` §3.
//!
//! Unlike the compact KLL, `KllDynamic` already stores its retained samples in
//! the wire's top-most-level-first order with level 0 in input order (it appends
//! to the end of a growable buffer), so encode/decode are a direct copy — no
//! buffer reversal is needed.

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};

use crate::message_pack_format::kind;
use crate::Vector1D;
use crate::message_pack_format::envelope;
use crate::sketches::kll::{
    Coin, KllCoinWire, KllPayload, KllWireItem, kll_metadata,
    split_and_validate_meta, validate_kll_payload,
};

use super::{CAPACITY_CACHE_LEN, KllDynamic};

// `wire` is a descendant of the sketch module, so these impls read the private
// fields and construct the struct directly.
impl<T> KllDynamic<T>
where
    T: crate::common::numerical::NumericalValue
        + KllWireItem
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>,
{
    /// Serializes the sketch into an ASAPv1 MessagePack envelope
    /// (kind_id `0x06 0x01`). `KllDynamic`'s buffer is already in the wire's
    /// top-most-level-first / input-order-L0 layout, so `levels` and `items` are
    /// copied straight out.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        // `KllDynamic` has no reproducible-seed concept, so `compaction_seed` is always
        // `None` and the metadata key is omitted.
        let metadata =
            rmp_serde::to_vec_named(&kll_metadata::<T>(self.k as u32, self.m as u32, None))?;
        let (state, bit_cache, remaining_bits) = self.co.to_wire();
        let payload = rmp_serde::to_vec(&KllPayload {
            levels: self.levels.as_slice().iter().map(|&l| l as u32).collect(),
            items: self.items.as_slice().to_vec(),
            coin: KllCoinWire {
                state,
                bit_cache,
                remaining_bits,
            },
        })?;
        Ok(envelope::encode(kind::KLL_DYNAMIC, &metadata, &payload))
    }

    /// Deserializes a `KllDynamic` from an ASAPv1 MessagePack envelope. Bytes
    /// whose metadata does not match this item type are rejected (fail closed),
    /// as are inconsistent level layouts.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let (meta, payload_bytes) = split_and_validate_meta::<T>(bytes, kind::KLL_DYNAMIC)?;
        let payload: KllPayload<T> = envelope::decode_block(payload_bytes)?;
        let num_levels = validate_kll_payload(&payload.levels, &payload.items, &payload.coin)?;

        let KllPayload {
            levels,
            items,
            coin,
        } = payload;
        let mut sketch = KllDynamic {
            items: Vector1D::from_vec(items),
            levels: Vector1D::from_vec(levels.into_iter().map(|l| l as usize).collect()),
            k: meta.k as usize,
            m: meta.m as usize,
            num_levels,
            co: Coin::from_wire(coin.state, coin.bit_cache, coin.remaining_bits as u8),
            // The wire carries the coin's live state, not the construction
            // seed, so a decoded sketch has no seed to re-derive on `clear()`.
            seed: None,
            capacity_cache: [0; CAPACITY_CACHE_LEN],
            top_height: 0,
            level0_capacity: 0,
        };
        sketch.rebuild_capacity_cache();
        Ok(sketch)
    }
}

#[cfg(test)]
mod tests {
    use crate::message_pack_format::kind;
    use crate::message_pack_format::envelope;
    use crate::sketches::kll_dynamic::KllDynamic;

    fn build_dynamic(k: i32, n: u64) -> KllDynamic<f64> {
        let mut sketch = KllDynamic::<f64>::with_k(k);
        for v in 1..=n {
            sketch.update(&(v as f64));
        }
        sketch
    }

    #[test]
    fn kll_dynamic_envelope_structure_and_round_trip() {
        let sketch = build_dynamic(200, 200_000);
        let bytes = sketch.to_asapv1().expect("serialize");

        assert!(bytes.starts_with(envelope::MAGIC));
        assert_eq!(bytes[6], envelope::VERSION);
        assert_eq!(bytes[7], 2, "kind_id_len");
        assert_eq!(&bytes[8..10], kind::KLL_DYNAMIC);

        let decoded = KllDynamic::<f64>::from_asapv1(&bytes).expect("decode");
        assert_eq!(
            decoded.to_asapv1().expect("re-serialize"),
            bytes,
            "KLLDynamic serialized bytes differed after round trip"
        );
        for &q in &[0.0, 0.01, 0.25, 0.5, 0.75, 0.99, 1.0] {
            assert_eq!(
                decoded.quantile(q),
                sketch.quantile(q),
                "quantile mismatch at q={q} after round trip"
            );
        }
    }

    #[test]
    fn kll_dynamic_empty_round_trip() {
        let sketch = KllDynamic::<f64>::with_k(200);
        let bytes = sketch.to_asapv1().expect("serialize");
        let decoded = KllDynamic::<f64>::from_asapv1(&bytes).expect("decode");
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), bytes);
    }

    #[test]
    fn kll_dynamic_i64_round_trip() {
        let mut sketch = KllDynamic::<i64>::with_k(200);
        for v in 1..=50_000i64 {
            sketch.update(&v);
        }
        let bytes = sketch.to_asapv1().expect("serialize");
        assert_eq!(&bytes[8..10], kind::KLL_DYNAMIC);
        let decoded = KllDynamic::<i64>::from_asapv1(&bytes).expect("decode");
        assert_eq!(decoded.to_asapv1().expect("re-serialize"), bytes);
    }

    #[test]
    fn kll_dynamic_item_type_cross_rejection() {
        let sketch = build_dynamic(200, 1000);
        let bytes = sketch.to_asapv1().expect("serialize");
        assert!(
            KllDynamic::<i64>::from_asapv1(&bytes).is_err(),
            "f64 KLLDynamic bytes must be rejected by an i64 decoder"
        );
    }
}
