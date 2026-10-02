//! Runtime-independent decoding and encoding of the protobuf
//! [`SketchEnvelope`](crate::proto::sketchlib::SketchEnvelope) for KLL and DDSketch states.
//!
//! These helpers hold no query-engine or runtime types, so any downstream
//! crate can read and write the envelope bytes with only this crate.

use crate::DdSketch;
use crate::proto::sketchlib::{
    DdSketchState, KllState, SketchEnvelope, sketch_envelope::SketchState,
};
use prost::Message;

pub fn envelope_state(bytes: &[u8]) -> Result<Option<SketchState>, String> {
    SketchEnvelope::decode(bytes)
        .map(|envelope| envelope.sketch_state)
        .map_err(|error| format!("decode SketchEnvelope: {error}"))
}

pub fn ddsketch_state(bytes: &[u8]) -> Result<(DdSketchState, f64), String> {
    let envelope =
        SketchEnvelope::decode(bytes).map_err(|error| format!("decode SketchEnvelope: {error}"))?;
    match envelope.sketch_state {
        Some(SketchState::Ddsketch(state)) => Ok((state, envelope.sample_p)),
        _ => Err("SketchEnvelope contains no DDSketch state".into()),
    }
}

pub fn reconstruct_ddsketch(bytes: &[u8]) -> Result<(DdSketch, f64), String> {
    let (state, sample_p) = ddsketch_state(bytes)?;
    if !state.alpha.is_finite() || !(0.0..1.0).contains(&state.alpha) || state.alpha == 0.0 {
        return Err("DDSketch alpha must be finite and between zero and one".into());
    }
    Ok((
        DdSketch::from_raw(state.alpha, state.store_counts, state.store_offset),
        sample_p,
    ))
}

pub fn kll_state(bytes: &[u8]) -> Result<KllState, String> {
    let envelope =
        SketchEnvelope::decode(bytes).map_err(|error| format!("decode SketchEnvelope: {error}"))?;
    match envelope.sketch_state {
        Some(SketchState::Kll(state)) => Ok(state),
        _ => Err("SketchEnvelope contains no KLL state".into()),
    }
}

pub fn encode_ddsketch(sketch: &DdSketch) -> Vec<u8> {
    let envelope = SketchEnvelope {
        format_version: 1,
        producer: None,
        hash_spec: None,
        sample_p: 0.0,
        sketch_state: Some(SketchState::Ddsketch(sketch.to_proto())),
    };
    envelope.encode_to_vec()
}

pub fn encode_kll(sketch: &crate::sketches::kll::KLL<f64>) -> Vec<u8> {
    use crate::proto::sketchlib::CoinState;
    let (state, bit_cache, remaining_bits) = sketch.wire_coin();
    SketchEnvelope {
        format_version: 1,
        producer: None,
        hash_spec: None,
        sample_p: 0.0,
        sketch_state: Some(SketchState::Kll(KllState {
            k: sketch.wire_k(),
            m: sketch.wire_m(),
            num_levels: sketch.wire_num_levels(),
            levels: sketch.wire_levels(),
            items: sketch.wire_items(),
            coin: Some(CoinState {
                state,
                bit_cache,
                remaining_bits,
            }),
            offset: 0.0,
            value_scale: 0,
            residuals: Vec::new(),
        })),
    }
    .encode_to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sketches::kll::KLL;

    fn envelope_bytes(sketch_state: Option<SketchState>) -> Vec<u8> {
        SketchEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            sample_p: 0.0,
            sketch_state,
        }
        .encode_to_vec()
    }

    // A DDSketch encoded into an envelope decodes back to the same stores and alpha.
    #[test]
    fn ddsketch_encode_decode_roundtrip() {
        let mut sketch = DdSketch::new(0.01);
        for value in [1.0, 2.5, 10.0, 10.0, 400.0] {
            sketch.update(value);
        }
        let bytes = encode_ddsketch(&sketch);

        let (restored, sample_p) = reconstruct_ddsketch(&bytes).unwrap();
        assert_eq!(sample_p, 0.0);
        assert_eq!(restored.alpha, sketch.wire_alpha());
        assert_eq!(restored.store_counts, sketch.store_counts);
        assert_eq!(restored.store_offset, sketch.store_offset);
        assert_eq!(restored.total_count(), 5);
    }

    // The DDSketch state keeps negative and zero stores through the envelope
    // (reused from ASAPPlanner's `signed_state_survives_codec_and_accumulator_roundtrip`).
    #[test]
    fn ddsketch_state_keeps_signed_stores() {
        let mut sketch = DdSketch::new(0.01);
        for value in [-4.0, 0.0, 8.0] {
            sketch.update(value);
        }
        let (wire, _) = ddsketch_state(&encode_ddsketch(&sketch)).unwrap();
        assert_eq!(wire.zero_count, 1);
        assert_eq!(wire.negative_store_counts.iter().sum::<u64>(), 1);
    }

    // An envelope holding a KLL state is rejected by the DDSketch decoders.
    #[test]
    fn ddsketch_decoders_reject_other_sketch_state() {
        let bytes = encode_kll(&KLL::<f64>::init_kll(200));
        assert!(ddsketch_state(&bytes).is_err());
        assert!(reconstruct_ddsketch(&bytes).is_err());
    }

    // An envelope holding a DDSketch state is rejected by the KLL decoder.
    #[test]
    fn kll_state_rejects_other_sketch_state() {
        let bytes = encode_ddsketch(&DdSketch::new(0.01));
        assert!(kll_state(&bytes).is_err());
    }

    // A DDSketch state whose alpha is zero, out of range or not finite is rejected.
    #[test]
    fn reconstruct_ddsketch_rejects_invalid_alpha() {
        for alpha in [0.0, -0.1, 1.0, 1.5, f64::NAN, f64::INFINITY] {
            let bytes = envelope_bytes(Some(SketchState::Ddsketch(DdSketchState {
                alpha,
                ..Default::default()
            })));
            assert!(
                reconstruct_ddsketch(&bytes).is_err(),
                "alpha {alpha} accepted"
            );
        }
    }

    // A KLL sketch encoded into an envelope yields its KLL state fields.
    #[test]
    fn kll_state_extracts_encoded_state() {
        let mut sketch = KLL::<f64>::init_kll(200);
        for value in 0..1000 {
            sketch.update(&(value as f64));
        }
        let state = kll_state(&encode_kll(&sketch)).unwrap();
        assert_eq!(state.k, sketch.wire_k());
        assert_eq!(state.m, sketch.wire_m());
        assert_eq!(state.num_levels, sketch.wire_num_levels());
        assert_eq!(state.levels, sketch.wire_levels());
        assert_eq!(state.items, sketch.wire_items());
        assert!(state.coin.is_some());
    }

    // Undecodable bytes are an error, and an envelope with no state decodes to `None`.
    #[test]
    fn envelope_state_rejects_garbage_and_reads_empty_state() {
        assert!(envelope_state(&[0xff, 0xff, 0xff]).is_err());
        assert_eq!(envelope_state(&envelope_bytes(None)).unwrap(), None);
    }
}
