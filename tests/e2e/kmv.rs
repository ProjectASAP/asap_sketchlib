//! E2E suites for the KMV distinct-count sketch: its estimate across the
//! regimes below, at and above `k`, over independent hash seeds, and its
//! merge.

use crate::common;

use common::specs::{CardinalityConfidenceSpec, Tally};
use common::uniform_u64;

use asap_sketchlib::{DataInput, Kmv};

/// Gaussian quantile for KMV's bands.
///
/// `z = 4` is a two-sided tail of 6.3e-5 **under the normal approximation**.
/// KMV's estimator is a reciprocal Beta variate and is only asymptotically
/// normal, so this is an asymptotic, model-based band, not an exact tail —
/// which is why the coverage matrix files it as `asymptotic model` rather than
/// `theorem`. See `CardinalityConfidenceSpec` for the derivation.
const KMV_Z: f64 = 4.0;

/// Seed-list indices used as independent hash functions.
///
/// `Kmv::update_with_hash` is public and `DefaultXxHasher::hash64_seeded(d, ..)`
/// selects seed `d` from the library's 20-entry table, so a trial can be run
/// under a genuinely different hash rather than under a relabelled key set. The
/// randomness KMV's error model quantifies over *is* the hash, so this is the
/// only construction that makes a binomial over trials legitimate.
const KMV_HASH_SEEDS: [usize; 8] = [0, 1, 2, 3, 7, 11, 13, 17];

/// Distinct-count regimes, expressed relative to `k`, so every `k` is probed
/// below, exactly at, and above the point where the estimator switches on.
#[derive(Clone, Copy, Debug)]
enum KmvRegime {
    /// `n < k`: every hash is retained and `estimate()` returns the count.
    BelowK(usize),
    /// `n == k`: the buffer is full, so `estimate()` uses `(k-1)/U_(k)`.
    AtK,
    /// `n = multiple * k`.
    AboveK(usize),
}

impl KmvRegime {
    fn n(self, k: usize) -> usize {
        match self {
            KmvRegime::BelowK(sub) => k.saturating_sub(sub),
            KmvRegime::AtK => k,
            KmvRegime::AboveK(mult) => k * mult,
        }
    }
}

const KMV_REGIMES: [KmvRegime; 6] = [
    KmvRegime::BelowK(1),
    KmvRegime::BelowK(2),
    KmvRegime::AtK,
    KmvRegime::AboveK(2),
    KmvRegime::AboveK(8),
    KmvRegime::AboveK(32),
];

/// Feeds `n` distinct identities from a private namespace under hash seed `d`.
///
/// The namespace makes two trials that share a hash seed still see disjoint
/// identities, so no two trials in the battery share any randomness at all.
fn kmv_trial(k: usize, n: usize, seed_index: usize, namespace: u64) -> Kmv {
    let mut sketch: Kmv = Kmv::new(k);
    for i in 0..n as u64 {
        let hashed =
            <asap_sketchlib::DefaultXxHasher as asap_sketchlib::SketchHasher>::hash64_seeded(
                seed_index,
                &DataInput::U64(namespace.wrapping_add(i)),
            );
        sketch.update_with_hash(hashed);
    }
    sketch
}

/// KMV's relative standard error across every regime, over independent hash
/// seeds.
///
/// # The estimator, and the two numbers that follow from it
///
/// `Kmv::estimate` returns `(k - 1) / U_(k)`, where `U_(k)` is the largest of
/// the `k` smallest normalized hashes. With `n` distinct uniform hashes
/// `U_(k) ~ Beta(k, n-k+1)`, so
///
/// ```text
///   E[(k-1)/U_(k)] = n                                  (unbiased for k > 1)
///   Var           = n (n - k + 1) / (k - 2)
///   RSE(n, k)     = sqrt( (n - k + 1) / (n (k - 2)) )  ->  1/sqrt(k - 2)
/// ```
///
/// The bands use the exact finite-`n` form, not the `1/sqrt(k - 2)` limit: it
/// is tighter at small `n` and correct at every `n`.
///
/// # Trial unit
///
/// One `(k, hash seed, regime)` triple is one sketch, one estimate, one trial —
/// and no two trials share a hash function or an identity. Rising checkpoints
/// read off one accumulating sketch would instead be *nested* estimates
/// sharing every retained hash.
#[test]
fn kmv_estimates_stay_inside_their_relative_standard_error_band_over_independent_hash_seeds() {
    const KS: [usize; 3] = [64, 1_024, 4_096];

    let mut tally = Tally::default();
    let mut namespace = 0u64;
    for &k in &KS {
        let spec = CardinalityConfidenceSpec::kmv(k, KMV_Z);
        for (s, &seed_index) in KMV_HASH_SEEDS.iter().enumerate() {
            for regime in KMV_REGIMES {
                let n = regime.n(k);
                namespace = namespace.wrapping_add(NAMESPACE_STRIDE);
                let sketch = kmv_trial(k, n, seed_index, namespace);
                let estimate = sketch.estimate();
                let outcome = spec.check(estimate, n);
                tally.record(outcome.is_ok(), || {
                    format!(
                        "k={k} seed_index={seed_index} (trial {s}) {regime:?} n={n} \
                         namespace={namespace:#x}: {}",
                        outcome.unwrap_err()
                    )
                });
            }
        }
    }
    tally.assert_independent_binomial(
        "KMV / relative standard error band",
        CardinalityConfidenceSpec::kmv(4_096, KMV_Z).per_check_failure(),
        &format!(
            "one trial = one sketch under one of {} independent hash seeds over its own \
             identity namespace; k in {KS:?}, regimes {KMV_REGIMES:?}, z={KMV_Z}, \
             sigma_rel = sqrt((n-k+1)/(n(k-2)))",
            KMV_HASH_SEEDS.len()
        ),
    );
}

/// Identity namespaces are spaced far enough apart that no two trials collide.
const NAMESPACE_STRIDE: u64 = 1 << 40;

/// The exact/estimated boundary is at `n < k`, **not** `n <= k`.
///
/// `Kmv::estimate` returns the retained count verbatim only while
/// `retained_hashes.len() < k`. At `n == k` the buffer is full and the estimator runs,
/// so the answer is `(k-1)/U_(k)` over the maximum of `k` uniforms — unbiased,
/// but with a standard deviation of `sqrt(k / (k-2))`, about one element. A
/// spec that treated `n == k` as exact would be demanding exactness of a
/// genuinely random number; the earlier `n <= k` form did exactly that and only
/// passed because its checkpoint grid never landed on `k`.
#[test]
fn kmv_is_exact_below_k_and_estimates_at_k() {
    const K: usize = 512;
    let spec = CardinalityConfidenceSpec::kmv(K, KMV_Z);

    assert!(
        spec.is_exact_regime(K - 1),
        "n = k-1 must be the exact regime"
    );
    assert!(
        !spec.is_exact_regime(K),
        "n = k must be the estimated regime: the buffer is full, so estimate() \
         switches to (k-1)/U_(k)"
    );

    for n in [1usize, 2, K / 2, K - 2, K - 1] {
        let sketch: Kmv = kmv_trial(K, n, 0, NAMESPACE_STRIDE * 900 + n as u64 * 4096);
        assert_eq!(
            sketch.estimate(),
            n as f64,
            "n={n} < k={K}: KMV retains every hash it has seen, so the count is exact"
        );
    }

    // At n == k the estimator runs. Averaged over independent hash seeds it is
    // unbiased; the point of this assertion is that the answer is *not* pinned
    // to k, so the exact-regime branch must not claim it.
    let mut estimates = Vec::new();
    for (i, &seed_index) in KMV_HASH_SEEDS.iter().enumerate() {
        let sketch = kmv_trial(K, K, seed_index, NAMESPACE_STRIDE * (1_000 + i as u64));
        let est = sketch.estimate();
        assert!(
            spec.check(est, K).is_ok(),
            "n = k = {K} under seed {seed_index}: {}",
            spec.check(est, K).unwrap_err()
        );
        estimates.push(est);
    }
    assert!(
        estimates.iter().any(|e| *e != K as f64),
        "n = k = {K}: the estimator must be running here, but every seed returned \
         exactly {K}, which would mean the exact-regime branch is still active. \
         estimates: {estimates:?}"
    );
}

/// Duplicates are inert and a shard merge is **exact**, not merely in-band.
///
/// KMV retains the `k` smallest hashes of everything it has seen. Any hash
/// among the global `k` smallest is also among the `k` smallest of whichever
/// shard produced it, so merging the shards recovers exactly the single pass's
/// retained set — the estimate is the *same number*, not a second draw, so it
/// is asserted as an equality rather than scored against a confidence band.
#[test]
fn kmv_duplicates_are_inert_and_a_shard_merge_reproduces_the_single_pass_exactly() {
    const K: usize = 4_096;
    const STREAM_SEED: u64 = 5001;

    let spec = CardinalityConfidenceSpec::kmv(K, KMV_Z);
    let stream = uniform_u64(60_000, 500_000, STREAM_SEED);
    let truth: std::collections::HashSet<u64> = stream.iter().copied().collect();

    let mut full: Kmv = Kmv::new(K);
    let mut a: Kmv = Kmv::new(K);
    let mut b: Kmv = Kmv::new(K);
    for (i, k) in stream.iter().enumerate() {
        full.update(&DataInput::U64(*k));
        if i % 2 == 0 {
            a.update(&DataInput::U64(*k));
        } else {
            b.update(&DataInput::U64(*k));
        }
    }

    let single = full.estimate();
    assert!(
        spec.check(single, truth.len()).is_ok(),
        "single pass over a duplicate-bearing stream: {}",
        spec.check(single, truth.len()).unwrap_err()
    );

    // Replaying the whole stream must not move the estimate at all: a hash
    // already retained is skipped, and one that was not retained cannot
    // displace anything smaller.
    for k in &stream {
        full.update(&DataInput::U64(*k));
    }
    assert_eq!(
        full.estimate(),
        single,
        "replaying a stream KMV has already seen must leave the estimate untouched \
         (k={K}, stream_seed={STREAM_SEED}, distinct={})",
        truth.len()
    );

    let mut merged = a.clone();
    merged.merge(&b);
    assert_eq!(
        merged.estimate(),
        single,
        "an even/odd shard merge must reproduce the single pass's retained set exactly \
         (k={K}, stream_seed={STREAM_SEED}, distinct={})",
        truth.len()
    );
}

// ---------------------------------------------------------------------------
// The documented input matrix
// ---------------------------------------------------------------------------

/// The document's KMV row is the twelve numbered inputs at `k = 32` and
/// `k = 128`.
///
/// # The two KMV bands
///
/// KMV's estimator is `(k - 1) / U_(k)`, whose relative standard error is
/// `1 / sqrt(k - 2)`: 18.3% at `k = 32` and 8.9% at `k = 128`. The document's
/// per-`k` bands are `z = 4` of that — 73% and 36% — and are asserted with no
/// tolerance. Each input is additionally checked against the spec's own band at
/// its cardinality, which uses the exact finite-`n` standard error and is
/// therefore slightly tighter than the asymptotic number the document quotes.
mod documented_matrix {
    use super::common::inputs::{KEY_INPUT_IDS, key_input};
    use super::common::specs::CardinalityConfidenceSpec;
    use super::*;

    use std::collections::HashSet;

    /// The document's relative-error band per `k`.
    fn documented_band(k: usize) -> f64 {
        match k {
            32 => 0.73,
            128 => 0.36,
            other => panic!("({other}) is not one of the documented k values"),
        }
    }

    fn kmv_documented_input(k: usize) {
        let spec = CardinalityConfidenceSpec::kmv(k, KMV_Z);
        let band = documented_band(k);

        for id in KEY_INPUT_IDS {
            let input = key_input(id);
            let distinct = input.keys.iter().copied().collect::<HashSet<i64>>().len();
            let mut sketch: Kmv = Kmv::new(k);
            for key in &input.keys {
                sketch.update(&input.data(*key));
            }
            let estimate = sketch.estimate();
            let context = input.context();

            if let Err(detail) = spec.check(estimate, distinct) {
                panic!("KMV k={k}: {detail}. distinct={distinct} {context}");
            }

            let rel = ((estimate - distinct as f64) / distinct as f64).abs();
            assert!(
                rel <= band,
                "KMV k={k} on ({id}): estimate {estimate:.0} against {distinct} distinct is \
                 {:.2}% off, past the documented {:.0}%. {context}",
                rel * 100.0,
                band * 100.0
            );

            // Duplicate replay is inert: KMV retains the k smallest hashes of
            // what it has seen, and it has seen all of these already.
            let before = sketch.estimate();
            for key in &input.keys {
                sketch.update(&input.data(*key));
            }
            assert_eq!(
                sketch.estimate(),
                before,
                "replaying ({id}) moved the k={k} KMV estimate"
            );
        }
    }

    #[test]
    fn kmv_k32_over_the_documented_inputs_holds_its_error_model() {
        kmv_documented_input(32);
    }

    #[test]
    fn kmv_k128_over_the_documented_inputs_holds_its_error_model() {
        kmv_documented_input(128);
    }
}
