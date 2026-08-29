//! Common data structure that is served as basic building block
//! Vector1D:
//! Vector2D:
//! Vector3D:
//! CommonHeap:
// use rand::rngs::SmallRng;
// use rand::{Rng, SeedableRng, rng};
use serde::{Deserialize, Serialize};

use crate::PRECOMPUTED_SAMPLE;
use crate::common::precompute_sample::PRECOMPUTED_SAMPLE_LEN;
/// Helper trait for converting sketch counter types to f64 for median calculation.
pub trait ToF64 {
    /// Converts the value into `f64`.
    fn to_f64(self) -> f64;
}

impl ToF64 for u64 {
    fn to_f64(self) -> f64 {
        self as f64
    }
}

impl ToF64 for i64 {
    fn to_f64(self) -> f64 {
        self as f64
    }
}

impl ToF64 for u32 {
    fn to_f64(self) -> f64 {
        self as f64
    }
}

impl ToF64 for i32 {
    fn to_f64(self) -> f64 {
        self as f64
    }
}

/// DPDK member sketch implementation. Reference:
/// <https://github.com/DPDK/dpdk/blob/main/lib/member/rte_member_sketch.c>.
/// Structure to hold data for Nitro Mode
/// Default to be off (i.e., not Nitro Mode)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Nitro {
    /// Whether Nitro sampling is enabled.
    pub is_nitro_mode: bool,
    sampling_rate: f64,
    /// Remaining items to skip before the next sampled update.
    pub to_skip: usize,
    /// Precomputed: 1.0 / ln(1 - sampling_rate) for geometric sampling
    inv_ln_one_minus_p: f64,
    // #[serde(skip)]
    // #[serde(default = "new_small_rng")]
    // // generator: SmallRng,
    /// Weight applied to each sampled update.
    pub delta: u64,
    idx: usize,
    /// Retained only so the serialized shape is unchanged.
    ///
    /// It used to be the wrap mask for `idx`, written as `0x10000` — the skip
    /// table's *length* rather than `length - 1`. `(idx + 1) & 0x10000` is `0`
    /// for every `idx` below `0xFFFF`, so the cursor never left entry 0. The
    /// cursor now wraps through [`Nitro::next_cursor`], which uses the table's
    /// real length and cannot be desynchronised by a stale or hostile value
    /// decoded from an old payload; this field is no longer read.
    mask: usize,
    /// Independent state for the stochastic-rounding draw.
    ///
    /// `#[serde(skip)]`, exactly like [`crate::NitroBatch`]'s sampling RNG: it
    /// describes how sampling proceeds from here, not what the sketch holds, so
    /// it is not part of the wire form and a decoded sketch restarts the
    /// rounding stream from the fixed seed.
    #[serde(skip, default = "default_rounding_state")]
    rounding_state: u64,
}

/// Seed for the rounding stream. Fixed, so a run reproduces.
const ROUNDING_SEED: u64 = 0x2545_F491_4F6C_DD1D;

fn default_rounding_state() -> u64 {
    ROUNDING_SEED
}

// fn new_small_rng() -> SmallRng {
//     let mut seed_rng = rng();
//     SmallRng::from_rng(&mut seed_rng)
// }

impl Default for Nitro {
    fn default() -> Self {
        Self {
            is_nitro_mode: false,
            sampling_rate: 0.0,
            to_skip: 0,
            inv_ln_one_minus_p: 0.0, // not used unless Nitro mode is enabled
            // generator: new_small_rng(), // not used unless Nitro mode is enabled
            delta: 0,
            idx: 0,
            mask: PRECOMPUTED_SAMPLE_LEN - 1,
            rounding_state: ROUNDING_SEED,
        }
    }
}

impl Nitro {
    /// Creates a Nitro state with the given sampling rate.
    pub fn init_nitro(rate: f64) -> Self {
        assert!(
            !rate.is_nan() && rate > 0.0 && rate <= 1.0,
            "sample_rate must be within (0.0, 1.0]"
        );
        let inv_ln = if (rate - 1.0).abs() <= f64::EPSILON {
            0.0 // Not used for full sampling
        } else {
            1.0 / (1.0 - rate).ln()
        };
        let mut nitro = Self {
            is_nitro_mode: true,
            sampling_rate: rate,
            to_skip: 0,
            inv_ln_one_minus_p: inv_ln,
            // generator: new_small_rng(),
            delta: 0,
            idx: 0,
            mask: PRECOMPUTED_SAMPLE_LEN - 1,
            rounding_state: ROUNDING_SEED,
        };
        nitro.delta = nitro.scaled_increment(1);
        nitro
    }

    /// Creates a Nitro state whose sampling is reproducible from `seed`.
    ///
    /// Both sources of randomness move with the seed:
    ///
    /// - the **skip schedule**, by starting the cursor at `seed % table_len`.
    ///   The table is a fixed stream of `ln(1 - u)` draws, so two far-apart
    ///   offsets read disjoint stretches of it and give independent admission
    ///   patterns — the unseeded path always starts at 0, which makes every
    ///   sketch at a given rate admit exactly the same subset;
    /// - the **stochastic-rounding stream**.
    ///
    /// Without this there is no way to run a Nitro accuracy battery over
    /// independent trials: the row-level sampler has no other entropy.
    pub fn init_nitro_seeded(rate: f64, seed: u64) -> Self {
        let mut nitro = Self::init_nitro(rate);
        nitro.idx = (seed % PRECOMPUTED_SAMPLE_LEN as u64) as usize;
        nitro.rounding_state = if seed == 0 { ROUNDING_SEED } else { seed };
        nitro
    }

    /// The skip-table cursor, for tests that need to show it advances and
    /// wraps rather than sticking at one entry.
    #[inline]
    pub fn table_cursor(&self) -> usize {
        self.cursor()
    }

    /// Length of the shared skip table the cursor wraps at.
    pub const fn skip_table_len() -> usize {
        PRECOMPUTED_SAMPLE_LEN
    }

    /// The cursor actually used to index the skip table.
    ///
    /// Taken modulo the table length on every read, so an `idx` decoded from an
    /// old payload — where the broken mask could leave it anywhere — can never
    /// index out of bounds.
    #[inline(always)]
    fn cursor(&self) -> usize {
        self.idx % PRECOMPUTED_SAMPLE_LEN
    }

    /// Advances the cursor by one, wrapping at the table's real length.
    #[inline(always)]
    fn next_cursor(&mut self) {
        self.idx = (self.cursor() + 1) % PRECOMPUTED_SAMPLE_LEN;
    }

    /// The weight one admitted row-slot carries, by stochastic rounding of
    /// `1 / p`.
    ///
    /// Same correction as [`crate::NitroBatch::admitted_weight`], and for the
    /// same reason: writing `ceil(1/p)` every time biases every estimate by the
    /// rounding error, which at `p = 0.3` is a flat +20%. With
    /// `q = floor(1/p)` and `r = frac(1/p)` the weight is
    ///
    /// ```text
    ///   W = q + Bernoulli(r)      E[W] = 1/p      Var[W] = r (1 - r)
    /// ```
    ///
    /// # Where the Bernoulli comes from, and why not from the cursor
    ///
    /// The draw comes from `Nitro`'s own `rounding_state`, a splitmix64 stream
    /// advanced once per admitted slot and **separate from the skip cursor**.
    /// An earlier revision derived it by hashing the cursor instead, which was
    /// wrong twice over: the cursor was frozen at 0 by the mask bug, so the
    /// draw was constant (and, at `u = 0 < r`, always 1 — i.e. `ceil(1/p)`
    /// again); and even with a working cursor the dither would be a fixed
    /// function of the position in the skip table, so a key whose arrivals
    /// happened to land on cursor positions with `u < r` would keep the full
    /// rounding bias. Advancing an independent stream once per admission is
    /// what makes `E[W] = 1/p` hold for each key separately, which is the whole
    /// point of the correction.
    ///
    /// The unbiasedness is exact given a uniform draw; treating the splitmix64
    /// stream as that uniform source is the same modelling assumption the
    /// geometric skip table already rests on, and the coverage matrix files
    /// Nitro as `asymptotic model` accordingly.
    ///
    /// When `1 / p` is an integer the fractional part is zero, **no draw is
    /// consumed**, and this returns `delta` unchanged — so the common rates
    /// (1, 1/2, 1/10, 1/100) emit exactly what they always did and their
    /// rounding stream never advances.
    #[inline(always)]
    pub fn admitted_delta(&mut self) -> u64 {
        if self.is_full_sampling() {
            return self.delta;
        }
        let exact = 1.0 / self.sampling_rate;
        let frac = exact - exact.floor();
        if frac <= 0.0 {
            return self.delta;
        }
        self.delta + u64::from(self.next_rounding_uniform() < frac)
    }

    /// Next uniform in `[0, 1)` from the rounding stream (splitmix64).
    #[inline(always)]
    fn next_rounding_uniform(&mut self) -> f64 {
        self.rounding_state = self.rounding_state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rounding_state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        (z >> 11) as f64 / (1u64 << 53) as f64
    }

    // for profiling
    #[inline(always)]
    /// Draws the next geometric skip distance.
    /// Draws the next geometric skip distance **at the configured rate**.
    ///
    /// The table holds `ln(1 - u)` for a fixed stream of uniforms; multiplying
    /// by `inv_ln_one_minus_p = 1 / ln(1 - p)` turns each entry into an
    /// inverse-CDF draw of `Geometric(p)` on `{0, 1, ...}`. An earlier revision
    /// read [`crate::PRECOMPUTED_SAMPLE_RATE_1PERCENT`], whose entries are
    /// already divided by `ln(0.99)`, so **every** rate got the schedule for
    /// `p = 0.01`: at `p = 0.5` the sketch skipped about 99 slots between
    /// admissions instead of 1, admitting roughly 1% of the stream while
    /// weighting each admission as if it were 50%.
    ///
    /// `floor`, not `ceil`: the caller's `+1` stride supplies the sampled slot
    /// itself, so `ceil` adds one to every skip distance and roughly halves the
    /// effective rate — `E[skip] = (1-p)/p` holds only under `floor`.
    pub fn draw_geometric(&mut self) {
        if self.is_full_sampling() {
            self.to_skip = 0;
            return;
        }
        self.to_skip =
            (PRECOMPUTED_SAMPLE[self.cursor()] * self.inv_ln_one_minus_p).floor() as usize;
        self.next_cursor();
    }

    /// Consumes one update's worth of row slots from the sampling schedule and
    /// returns the `(row, weight)` pairs that were admitted.
    ///
    /// NitroSketch samples **per row slot**, not per update: a `d`-row sketch
    /// turns an `n`-update stream into `n * d` slots, each admitted
    /// independently with probability `p`. `to_skip` is the number of slots
    /// still to skip, carried across update boundaries, so this walks it
    /// forward by `rows` and reports every landing inside the window.
    ///
    /// # Why this replaces the callers' own arithmetic
    ///
    /// `fast_insert_nitro` used to carry the cursor with
    /// `(r + to_skip + 1) - rows` on `usize`. That expression is negative — and
    /// so underflows — whenever the next admitted slot falls inside the same
    /// update, which is the common case at any rate above roughly `1/d`. It
    /// never fired before only because the skip was frozen at one large
    /// constant by the two bugs above; making the schedule rate-correct makes
    /// small skips normal, so the walk has to be correct rather than lucky. It
    /// also admits *every* slot the schedule lands on within the update, where
    /// the Count-Min path previously admitted at most one and silently dropped
    /// the rest.
    ///
    /// At `p = 1` every slot is admitted with weight 1.
    pub fn admit_rows(&mut self, rows: usize, out: &mut Vec<(usize, u64)>) {
        out.clear();
        if rows == 0 {
            return;
        }
        if self.is_full_sampling() {
            out.extend((0..rows).map(|r| (r, 1u64)));
            return;
        }
        let mut row = self.to_skip;
        while row < rows {
            let weight = self.admitted_delta();
            out.push((row, weight));
            self.draw_geometric();
            // `+ 1` for the admitted slot itself, then the freshly drawn skip.
            row = row + 1 + self.to_skip;
        }
        // Whatever is left of the skip once this update's window is exhausted.
        self.to_skip = row - rows;
    }

    #[inline(always)]
    /// Decrements the current skip counter by one.
    pub fn reduce_to_skip(&mut self) {
        self.to_skip -= 1;
    }

    #[inline(always)]
    /// Decrements the current skip counter by `c`.
    pub fn reduce_to_skip_by_count(&mut self, c: usize) {
        self.to_skip -= c;
    }

    #[inline(always)]
    /// Returns the configured sampling rate.
    pub fn get_sampling_rate(&self) -> f64 {
        self.sampling_rate
    }

    // #[inline]
    #[inline(always)]
    /// The integer part of the weight one admitted update carries. The
    /// fractional remainder is paid per update by [`Nitro::admitted_delta`].
    pub fn scaled_increment(&self, weight: u64) -> u64 {
        if self.is_full_sampling() {
            weight
        } else {
            ((weight as f64) / self.sampling_rate).floor() as u64
        }
    }

    // #[inline]
    #[inline(always)]
    pub fn is_full_sampling(&self) -> bool {
        (self.sampling_rate - 1.0).abs() <= f64::EPSILON
    }

    #[inline(always)]
    /// Returns the cached Nitro sampling state.
    pub fn get_ctx(&self) -> (usize, f64, usize, usize) {
        (self.idx, self.inv_ln_one_minus_p, self.to_skip, self.mask)
    }

    #[inline(always)]
    /// Restores the cached Nitro sampling state.
    pub fn commit_ctx(&mut self, idx: usize, to_skip: usize) {
        self.idx = idx;
        self.to_skip = to_skip;
    }
}

/// Compute median from a mutable slice of f64 values (inline helper)
/// This is used by query_median_with_custom_hash for HydraCounter queries
#[inline(always)]
pub fn compute_median_inline_f64(values: &mut [f64]) -> f64 {
    match values.len() {
        0 => 0.0,
        1 => values[0],
        2 => (values[0] + values[1]) / 2.0,
        // starting here is an assumption that LLVM and compiler
        // will load var into register and perform simple register swap
        // no heavy sort or memory swap
        3 => {
            let (mut v0, mut v1, v2) = (values[0], values[1], values[2]);
            // ensure v0 is smaller than v1
            if v0 > v1 {
                std::mem::swap(&mut v0, &mut v1);
            }
            // ensure v1 is smaller than v2, and ignore the actual v2 value
            if v1 > v2 {
                v1 = v2;
            }
            // ensure v1 is still greater than v0
            if v0 > v1 {
                v1 = v0;
            }
            v1
        }
        4 => {
            let (mut v0, mut v1, mut v2, mut v3) = (values[0], values[1], values[2], values[3]);
            // ensure the order of v0 and v1
            if v0 > v1 {
                std::mem::swap(&mut v0, &mut v1);
            }
            // ensure the order of v2 and v3
            if v2 > v3 {
                std::mem::swap(&mut v2, &mut v3);
            }
            // the smaller of v0 and v2 will be smaller than v1 anyway
            // ignore the smaller one, which will be min (dropped)
            if v0 > v2 {
                v2 = v0;
            }
            // the greater of v1 and v3 will be greater than v2 anyway
            // ignore the greeater one, which will be max (dropped)
            if v1 > v3 {
                v1 = v3;
            }
            (v1 + v2) / 2.0
        }
        5 => {
            let (mut v0, mut v1, mut v2, mut v3, mut v4) =
                (values[0], values[1], values[2], values[3], values[4]);
            // ensure the order of v0 and v1
            if v0 > v1 {
                std::mem::swap(&mut v0, &mut v1);
            }
            // ensure the order of v3 and v4
            if v3 > v4 {
                std::mem::swap(&mut v3, &mut v4);
            }
            // the smaller of v0 v3 will be smaller than v1 v4 and the other
            // smaller than 3 value, so not median of 5
            if v0 > v3 {
                v3 = v0;
            }
            // the greater of v1 v4 will be greater than v0 v3 and the other
            // greater than 3 value, so not median of 5
            if v1 > v4 {
                v1 = v4;
            }
            // median of 5 is reduced to median of v1 v2 v3
            // v0 and v4 will not change the order
            // v0 will be one of the two smallest
            // v4 will be one of the two greatest
            // safely ignored
            if v1 > v2 {
                std::mem::swap(&mut v1, &mut v2);
            }
            if v2 > v3 {
                v2 = v3;
            }
            if v1 > v2 {
                v2 = v1;
            }
            v2
        }
        _ => {
            values.sort_unstable_by(f64::total_cmp);
            let mid = values.len() / 2;
            if values.len() % 2 == 1 {
                values[mid]
            } else {
                (values[mid - 1] + values[mid]) / 2.0
            }
        }
    }
}

/// Trait defining heap ordering behavior.
#[cfg(test)]
mod tests {

    use super::*;
    use rand::{Rng, SeedableRng, rngs::StdRng};

    fn build_three() -> Vec<[f64; 3]> {
        let mut rng = StdRng::seed_from_u64(0x5eed_c0de_1234_5678);
        (0..1_000)
            .map(|_| {
                [
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                ]
            })
            .collect()
    }

    fn build_four() -> Vec<[f64; 4]> {
        let mut rng = StdRng::seed_from_u64(0x5eed_c0de_1234_5678);
        (0..1_000)
            .map(|_| {
                [
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                ]
            })
            .collect()
    }

    fn build_five() -> Vec<[f64; 5]> {
        let mut rng = StdRng::seed_from_u64(0x5eed_c0de_1234_5678);
        (0..1_000)
            .map(|_| {
                [
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                    rng.random::<f64>(),
                ]
            })
            .collect()
    }

    fn median_three_sort(values: &mut [f64; 3]) -> f64 {
        values.sort_unstable_by(f64::total_cmp);
        let mid = values.len() / 2;
        if values.len() % 2 == 1 {
            values[mid]
        } else {
            (values[mid - 1] + values[mid]) / 2.0
        }
    }

    fn median_four_sort(values: &mut [f64; 4]) -> f64 {
        values.sort_unstable_by(f64::total_cmp);
        let mid = values.len() / 2;
        if values.len() % 2 == 1 {
            values[mid]
        } else {
            (values[mid - 1] + values[mid]) / 2.0
        }
    }

    fn median_five_sort(values: &mut [f64; 5]) -> f64 {
        values.sort_unstable_by(f64::total_cmp);
        let mid = values.len() / 2;
        if values.len() % 2 == 1 {
            values[mid]
        } else {
            (values[mid - 1] + values[mid]) / 2.0
        }
    }

    #[test]
    fn median_test() {
        let mut three_vec = build_three();
        let mut four_vec = build_four();
        let mut five_vec = build_five();
        for v in &mut three_vec {
            let fast_median = compute_median_inline_f64(v);
            let sort_median = median_three_sort(v);
            assert_eq!(
                fast_median, sort_median,
                "median for sort is {sort_median} but fast gives {fast_median}, input is {v:?}"
            );
        }
        for v in &mut four_vec {
            let fast_median = compute_median_inline_f64(v);
            let sort_median = median_four_sort(v);
            assert_eq!(
                fast_median, sort_median,
                "median for sort is {sort_median} but fast gives {fast_median}, input is {v:?}"
            );
        }
        for v in &mut five_vec {
            let fast_median = compute_median_inline_f64(v);
            let sort_median = median_five_sort(v);
            assert_eq!(
                fast_median, sort_median,
                "median for sort is {sort_median} but fast gives {fast_median}, input is {v:?}"
            );
        }
    }
}
