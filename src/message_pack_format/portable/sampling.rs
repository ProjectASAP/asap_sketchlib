//! Query-time rescaling for NitroSketch / geometric-sampled sketches.
//!
//! Sampling lives on the wire as a single `sample_p` field on the
//! [`SketchEnvelope`] (NOT inside any
//! per-sketch state struct — so downstream code that builds state structs by
//! literal is unaffected). The producer admits each stream update with
//! probability `p` and stores the RAW SAMPLED state. The backend reads `p` here
//! and applies the `× 1/p` rescale at QUERY time:
//!
//! - **count-like** estimators (HLL cardinality, CountMin / CountSketch
//!   frequency, SUM / COUNT) are inverse-probability unbiased: multiply by
//!   `1/p`. See [`rescale_count`].
//! - **quantile** estimators (KLL, DDSketch) are scale-invariant under uniform
//!   sampling — every retained item carries the same `1/p` weight, which
//!   cancels in the rank computation — so the quantile is read unchanged. The
//!   carried `p` is informational (it widens the error budget, §7). See
//!   [`is_quantile_scale_invariant`].
//!
//! Dual-read default: an absent / `0.0` `sample_p` (every pre-sampling
//! producer, and every `p = 1.0` exact producer, which leaves the field at the
//! proto3 default) is interpreted as `1.0` — i.e. no rescale, a true no-op.

use crate::proto::sketchlib::SketchEnvelope;

/// Read the effective sampling probability from an envelope, applying the
/// dual-read default: a `0.0` (unset / pre-sampling) `sample_p` means `1.0`
/// (exact, no sampling). Values are clamped to `(0, 1]`; a malformed negative or
/// `>1` value falls back to `1.0` rather than corrupting the rescale.
#[inline]
pub fn effective_sample_p(env: &SketchEnvelope) -> f64 {
    sample_p_or_default(env.sample_p)
}

/// Apply the dual-read default to a raw `sample_p` scalar (e.g. read directly
/// off a decoded envelope field). `0.0` → `1.0`; out-of-range → `1.0`.
#[inline]
pub fn sample_p_or_default(p: f64) -> f64 {
    if p > 0.0 && p <= 1.0 {
        p
    } else {
        // 0.0 (unset / exact), NaN, negative, or >1 all mean "no sampling".
        1.0
    }
}

/// Rescale a count-like estimate (cardinality / frequency / sum / count) read
/// from a sampled sketch back to its unbiased full-stream estimate: `raw / p`.
/// With `p == 1.0` this returns `raw` unchanged.
#[inline]
pub fn rescale_count(raw: f64, p: f64) -> f64 {
    let p = sample_p_or_default(p);
    raw / p
}

/// Rescale a count-like estimate using the `sample_p` carried by `env`.
#[inline]
pub fn rescale_count_with_env(raw: f64, env: &SketchEnvelope) -> f64 {
    rescale_count(raw, effective_sample_p(env))
}

/// Whether a quantile estimate needs NO rescale under uniform sampling.
/// Always `true`: KLL and DDSketch quantiles are scale-invariant because every
/// retained item carries the identical `1/p` weight, which cancels in the rank.
/// Provided as a named, self-documenting guard for call sites that branch on
/// family.
#[inline]
pub const fn is_quantile_scale_invariant() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::sketchlib::{HyperLogLogState, SketchEnvelope, sketch_envelope::SketchState};

    fn env_with_p(p: f64) -> SketchEnvelope {
        SketchEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            sample_p: p,
            sketch_state: Some(SketchState::Hll(HyperLogLogState {
                variant: 2,
                precision: 14,
                registers: Vec::new(),
                hip_kxq0: 0.0,
                hip_kxq1: 0.0,
                hip_est: 0.0,
                registers_sparse: None,
            })),
        }
    }

    #[test]
    fn dual_read_zero_means_one() {
        // A pre-sampling / exact envelope leaves sample_p at the proto3 default.
        assert_eq!(effective_sample_p(&env_with_p(0.0)), 1.0);
        assert_eq!(sample_p_or_default(0.0), 1.0);
    }

    #[test]
    fn out_of_range_falls_back_to_one() {
        assert_eq!(sample_p_or_default(-0.5), 1.0);
        assert_eq!(sample_p_or_default(1.5), 1.0);
        assert_eq!(sample_p_or_default(f64::NAN), 1.0);
    }

    #[test]
    fn valid_p_passes_through() {
        assert_eq!(effective_sample_p(&env_with_p(0.1)), 0.1);
        assert_eq!(sample_p_or_default(1.0), 1.0);
    }

    #[test]
    fn rescale_count_inverts_probability() {
        // Sampled raw count ≈ p·n; raw/p recovers n.
        assert!((rescale_count(10_000.0, 0.1) - 100_000.0).abs() < 1e-6);
        assert_eq!(rescale_count(123.0, 1.0), 123.0);
        // Dual-read: a 0.0 p must not divide-by-zero; it means no sampling.
        assert_eq!(rescale_count(123.0, 0.0), 123.0);
    }

    #[test]
    fn rescale_with_env() {
        assert!((rescale_count_with_env(5_000.0, &env_with_p(0.05)) - 100_000.0).abs() < 1e-6);
        assert_eq!(rescale_count_with_env(42.0, &env_with_p(0.0)), 42.0);
    }

    #[test]
    fn quantiles_are_scale_invariant() {
        assert!(is_quantile_scale_invariant());
    }
}
