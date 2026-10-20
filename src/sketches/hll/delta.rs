//! The HLL deltas, ASAPv1 kinds `0x01 0x81` (Classic) and `0x01 0x82`
//! (Ertl-MLE) (`docs/asapv1_wire_format.md` §3.23). Their metadata is the
//! state's (§3.1); their payload is `[indices, registers]`.

use std::marker::PhantomData;

use rmp_serde::{decode::Error as RmpDecodeError, encode::Error as RmpEncodeError};
use serde::{Deserialize, Serialize};

use crate::message_pack_format::admission_rate;
use crate::message_pack_format::delta::check_cells;
use crate::message_pack_format::envelope;
use crate::structures::fixed_structure::HllRegisterStorage;
use crate::{DefaultXxHasher, HashProfile, SketchHasher};

use super::HyperLogLogImpl;
use super::wire::{HllWireVariant, check_register_range, encode_metadata, validated_hll_payload};

/// The delta kind_id of a variant: its state kind_id with the variant's top bit
/// set.
fn delta_kind<V: HllWireVariant>() -> [u8; 2] {
    [V::WIRE_KIND_ID[0], V::WIRE_KIND_ID[1] | 0x80]
}

/// A change to a [`HyperLogLogImpl`] state: each value of `registers` is a new
/// value for the register its position names in `indices`. Indices are strictly
/// ascending and below `2^precision`; each value is non-zero and at most the
/// maximum rank, `65 - precision`.
#[derive(Clone, Debug)]
pub struct HyperLogLogDelta<Variant, Registers, H = DefaultXxHasher> {
    indices: Vec<u64>,
    registers: Vec<u8>,
    admission_rate: f64,
    _config: PhantomData<(Variant, Registers, H)>,
}

/// The delta payload: `[indices, registers]`, `registers` a msgpack `bin`.
#[derive(Serialize, Deserialize)]
struct HllDeltaPayload {
    indices: Vec<u64>,
    #[serde(with = "serde_bytes")]
    registers: Vec<u8>,
}

impl<Variant, Registers, H> HyperLogLogDelta<Variant, Registers, H>
where
    Variant: HllWireVariant,
    Registers: HllRegisterStorage,
    H: SketchHasher + HashProfile,
{
    /// Builds a delta from the changed registers. Fails when they break the
    /// rules above.
    pub fn new(indices: Vec<u64>, registers: Vec<u8>) -> Result<Self, String> {
        check_cells(&indices, &registers, ..Registers::NUM_REGISTERS as u64)
            .map_err(|e| format!("HLL delta: {e}"))?;
        check_register_range::<Registers>(&registers)?;
        Ok(Self {
            indices,
            registers,
            admission_rate: admission_rate::admit_all(),
            _config: PhantomData,
        })
    }

    /// Sets the admission rate of the state this delta changes; `1` unless set.
    pub fn with_admission_rate(mut self, p: f64) -> Self {
        self.admission_rate = p;
        self
    }

    /// Serializes the delta into an ASAPv1 MessagePack envelope.
    pub fn to_asapv1(&self) -> Result<Vec<u8>, RmpEncodeError> {
        let metadata = encode_metadata::<Registers, H>(self.admission_rate)?;
        let payload = rmp_serde::to_vec(&HllDeltaPayload {
            indices: self.indices.clone(),
            registers: self.registers.clone(),
        })?;
        Ok(envelope::encode(
            &delta_kind::<Variant>(),
            &metadata,
            &payload,
        ))
    }

    /// Deserializes a delta from an ASAPv1 MessagePack envelope.
    pub fn from_asapv1(bytes: &[u8]) -> Result<Self, RmpDecodeError> {
        let (payload, admission_rate) = validated_hll_payload::<H>(
            bytes,
            &delta_kind::<Variant>(),
            Registers::PRECISION as u32,
        )?;
        let p: HllDeltaPayload = envelope::decode_block(payload)?;
        let delta = Self::new(p.indices, p.registers).map_err(RmpDecodeError::Uncategorized)?;
        Ok(delta.with_admission_rate(admission_rate))
    }
}

impl<Variant, Registers: HllRegisterStorage, H: SketchHasher>
    HyperLogLogImpl<Variant, Registers, H>
{
    /// Raises each register the delta names to the delta's value, keeping the
    /// larger of the two. Fails, changing nothing, when the admission rates
    /// differ.
    pub fn merge_delta(
        &mut self,
        delta: &HyperLogLogDelta<Variant, Registers, H>,
    ) -> Result<(), String> {
        admission_rate::check_equal(self.admission_rate, delta.admission_rate)
            .map_err(|e| format!("HLL delta: metadata mismatch: {e}"))?;
        let registers = self.registers.as_mut_slice();
        for (&index, &value) in delta.indices.iter().zip(&delta.registers) {
            let register = &mut registers[index as usize];
            *register = (*register).max(value);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sketches::hll::Classic;
    use crate::structures::fixed_structure::{HllRegistersP12, HllRegistersP14};

    type Delta = HyperLogLogDelta<Classic, HllRegistersP14>;

    /// A P14 Classic delta envelope around the given indices and registers.
    fn delta_bytes(indices: &[u64], registers: &[u8]) -> Vec<u8> {
        let metadata = encode_metadata::<HllRegistersP14, DefaultXxHasher>(1.0).unwrap();
        let payload = rmp_serde::to_vec(&HllDeltaPayload {
            indices: indices.to_vec(),
            registers: registers.to_vec(),
        })
        .unwrap();
        envelope::encode(&delta_kind::<Classic>(), &metadata, &payload)
    }

    #[test]
    fn decode_rejects_each_payload_rule() {
        assert!(Delta::from_asapv1(&delta_bytes(&[0, 16383], &[1, 51])).is_ok());
        let rows: [(&str, &[u64], &[u8]); 2] = [
            ("index 2^precision", &[16384], &[1]),
            ("register past the maximum rank", &[0], &[52]),
        ];
        for (label, indices, registers) in rows {
            assert!(
                Delta::from_asapv1(&delta_bytes(indices, registers)).is_err(),
                "{label}"
            );
        }
    }

    #[test]
    fn decode_pins_the_precision() {
        let delta = delta_bytes(&[0], &[1]);
        assert!(Delta::from_asapv1(&delta).is_ok());
        assert!(HyperLogLogDelta::<Classic, HllRegistersP12>::from_asapv1(&delta).is_err());
    }
}
