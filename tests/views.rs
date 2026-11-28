//! Zero-copy views: every view family answers exactly as the owned sketch
//! its bytes were written from, and merging views into an accumulator
//! gives the same accumulator as merging the owned sketches.

use asap_sketchlib::views::{aligned_bytes, aligned_copy};
use asap_sketchlib::{
    CountMinF64, CountMinF64View, DataInput, DdSketch, DdSketchView, Kll, KllView,
};

/// A deterministic stream of `n` values from `seed`.
fn values(seed: u64, n: usize) -> Vec<f64> {
    let mut x = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x % 100_000) as f64 / 7.0 - 3_000.0
        })
        .collect()
}

fn dd(seed: u64, n: usize) -> DdSketch {
    let mut sketch = DdSketch::new(0.01);
    for v in values(seed, n) {
        sketch.add(&v);
    }
    sketch.shrink_to_fit();
    sketch
}

fn kll(seed: u64, n: usize) -> Kll<f64> {
    let mut sketch = Kll::with_k_and_seed(200, seed);
    for v in values(seed, n) {
        sketch.update(&v);
    }
    sketch
}

fn cms(seed: u64, n: usize) -> CountMinF64 {
    let mut sketch = CountMinF64::with_dimensions(3, 64);
    for v in values(seed, n) {
        sketch.update_weighted(&DataInput::I64((v as i64) % 50), 1.5);
    }
    sketch
}

const QS: [f64; 7] = [0.0, 0.01, 0.25, 0.5, 0.9, 0.99, 1.0];

#[test]
fn ddsketch_view_answers_as_the_owned_sketch() {
    for n in [0, 1, 15, 1_000] {
        let owned = dd(n as u64 + 1, n);
        let words = aligned_copy(&owned.to_view_bytes());
        let bytes = aligned_bytes(&words, owned.to_view_bytes().len());
        let view = DdSketchView::from_bytes(bytes).unwrap();
        assert_eq!(view.get_count(), owned.get_count());
        for q in QS {
            assert_eq!(
                view.get_value_at_quantile(q),
                owned.get_value_at_quantile(q),
                "{n} {q}"
            );
            assert_eq!(
                view.quantile_interpolated(q),
                owned.quantile_interpolated(q),
                "{n} {q}"
            );
        }
        assert_eq!(
            view.to_owned_sketch().to_view_bytes(),
            owned.to_view_bytes()
        );
    }
}

#[test]
fn ddsketch_views_merge_as_owned_sketches() {
    let windows: Vec<DdSketch> = (0..240).map(|w| dd(w + 10, 15)).collect();
    let stored: Vec<(Vec<u64>, usize)> = windows
        .iter()
        .map(|w| {
            let bytes = w.to_view_bytes();
            (aligned_copy(&bytes), bytes.len())
        })
        .collect();
    let mut owned = DdSketch::new(0.01);
    let mut viewed = DdSketch::new(0.01);
    // A view holds occupied buckets only, so `merge_view` equals merging
    // the owned sketch shrunk to fit (answers equal the unshrunk merge).
    for (window, (words, len)) in windows.iter().zip(&stored) {
        let mut shrunk = window.clone();
        shrunk.shrink_to_fit();
        owned.merge(&shrunk).unwrap();
        viewed
            .merge_view(&DdSketchView::from_bytes(aligned_bytes(words, *len)).unwrap())
            .unwrap();
    }
    assert_eq!(viewed.to_view_bytes(), owned.to_view_bytes());
    for q in QS {
        assert_eq!(
            viewed.quantile_interpolated(q),
            owned.quantile_interpolated(q)
        );
    }
    let other = DdSketch::new(0.05);
    let bytes = other.to_view_bytes();
    let words = aligned_copy(&bytes);
    assert!(
        viewed
            .merge_view(&DdSketchView::from_bytes(aligned_bytes(&words, bytes.len())).unwrap())
            .is_err()
    );
}

#[test]
fn kll_view_answers_and_merges_as_the_owned_sketch() {
    for n in [1, 15, 5_000] {
        let owned = kll(n as u64, n);
        let bytes = owned.to_view_bytes();
        let words = aligned_copy(&bytes);
        let view = KllView::from_bytes(aligned_bytes(&words, bytes.len())).unwrap();
        assert_eq!(view.count(), owned.count());
        for q in QS {
            assert_eq!(view.quantile(q), owned.quantile(q), "{n} {q}");
        }
    }
    // Folding 240 windows: the same accumulator, coin tosses included.
    let windows: Vec<Kll<f64>> = (0..240).map(|w| kll(w + 3, 15)).collect();
    let mut owned = Kll::with_k_and_seed(200, 7);
    let mut viewed = Kll::with_k_and_seed(200, 7);
    for window in &windows {
        owned.merge(window);
        let bytes = window.to_view_bytes();
        let words = aligned_copy(&bytes);
        viewed
            .merge_view(&KllView::from_bytes(aligned_bytes(&words, bytes.len())).unwrap())
            .unwrap();
    }
    assert_eq!(viewed.to_view_bytes(), owned.to_view_bytes());
    let mismatched = Kll::<f64>::with_k_and_seed(100, 1).to_view_bytes();
    let words = aligned_copy(&mismatched);
    assert!(
        owned
            .merge_view(&KllView::from_bytes(aligned_bytes(&words, mismatched.len())).unwrap())
            .is_err()
    );
}

#[test]
fn count_min_view_answers_and_merges_as_the_owned_sketch() {
    let a = cms(1, 500);
    let b = cms(2, 500);
    let bytes = b.to_view_bytes();
    let words = aligned_copy(&bytes);
    let view = CountMinF64View::from_bytes(aligned_bytes(&words, bytes.len())).unwrap();
    for key in 0..50 {
        let key = DataInput::I64(key);
        assert_eq!(
            view.estimate::<asap_sketchlib::DefaultXxHasher>(&key),
            b.estimate(&key)
        );
    }
    let mut owned = a.clone();
    owned.merge(&b);
    let mut viewed = a.clone();
    viewed.merge_view(&view).unwrap();
    assert_eq!(viewed.to_view_bytes(), owned.to_view_bytes());
}

#[test]
fn bad_bytes_are_errors_not_panics() {
    let bytes = dd(1, 100).to_view_bytes();
    let words = aligned_copy(&bytes);
    let aligned = aligned_bytes(&words, bytes.len());
    assert!(DdSketchView::from_bytes(&aligned[..aligned.len() - 8]).is_err());
    assert!(DdSketchView::from_bytes(&aligned[8..]).is_err());
    assert!(KllView::from_bytes(aligned).is_err());
    let mut garbage = words.clone();
    for word in garbage.iter_mut().skip(1) {
        *word = u64::MAX;
    }
    assert!(DdSketchView::from_bytes(aligned_bytes(&garbage, bytes.len())).is_err());
    let kll_bytes = kll(1, 1000).to_view_bytes();
    let mut kll_words = aligned_copy(&kll_bytes);
    kll_words[3] = u64::MAX; // the item count
    assert!(KllView::from_bytes(aligned_bytes(&kll_words, kll_bytes.len())).is_err());
    // Misaligned: one byte in.
    let shifted: Vec<u8> = std::iter::once(0).chain(bytes.iter().copied()).collect();
    assert!(
        DdSketchView::from_bytes(&shifted[1..]).is_err()
            || (shifted[1..].as_ptr() as usize) % 8 == 0
    );
}

/// Bucket counts are stored as `u32` when they all fit (a window's counts
/// do), and as `u64` when one does not; both read and merge alike.
#[test]
fn ddsketch_counts_are_stored_narrow_and_widen_on_overflow() {
    use asap_sketchlib::sketches::octo_delta::{DdSign, DdSketchBucketDelta};
    let small = dd(1, 1_000);
    let small_bytes = small.to_view_bytes();
    let words = aligned_copy(&small_bytes);
    let view = DdSketchView::from_bytes(aligned_bytes(&words, small_bytes.len())).unwrap();
    // 1,000 samples over ~100 buckets: every count fits a u8 or u16.
    assert!(view.count_width() <= 2);
    let mut big = dd(2, 1_000);
    big.apply_delta(DdSketchBucketDelta {
        sign: DdSign::Positive,
        index: 300,
        count: u64::from(u32::MAX) + 5,
    });
    let big_bytes = big.to_view_bytes();
    let big_words = aligned_copy(&big_bytes);
    let big_view = DdSketchView::from_bytes(aligned_bytes(&big_words, big_bytes.len())).unwrap();
    assert_eq!(big_view.count_width(), 8);
    for q in QS {
        assert_eq!(
            big_view.quantile_interpolated(q),
            big.quantile_interpolated(q)
        );
    }
    // An accumulator merging both widths equals the owned merge, and its
    // own count exceeds u32 (accumulators stay u64).
    let mut owned = small.clone();
    owned.merge(&big).unwrap();
    let mut viewed = DdSketch::new(0.01);
    viewed.merge_view(&view).unwrap();
    viewed.merge_view(&big_view).unwrap();
    assert_eq!(viewed.to_view_bytes(), owned.to_view_bytes());
    assert!(viewed.get_count() > u64::from(u32::MAX));
}

/// A sketch whose samples spread over many buckets is written sparse
/// (occupied buckets only), one packed into a narrow range dense; both
/// answer and merge as their owned sketches, and sparse takes fewer bytes.
#[test]
fn spread_ddsketches_are_written_sparse() {
    let mut spread = DdSketch::new(0.01);
    for i in 1..=60u32 {
        spread.add(&(f64::from(i).powi(4)));
    }
    let mut packed = DdSketch::new(0.01);
    for i in 0..150u32 {
        packed.add(&(100.0 + f64::from(i % 5)));
    }
    for (sketch, sparse) in [(&spread, true), (&packed, false)] {
        let bytes = sketch.to_view_bytes();
        let words = aligned_copy(&bytes);
        let view = DdSketchView::from_bytes(aligned_bytes(&words, bytes.len())).unwrap();
        assert_eq!(view.is_sparse(), sparse);
        assert_eq!(view.count_width(), 1);
        for q in QS {
            assert_eq!(
                view.quantile_interpolated(q),
                sketch.quantile_interpolated(q)
            );
            assert_eq!(
                view.get_value_at_quantile(q),
                sketch.get_value_at_quantile(q)
            );
        }
        let mut owned = dd(1, 100);
        owned.merge(sketch).unwrap();
        let mut viewed = dd(1, 100);
        viewed.merge_view(&view).unwrap();
        assert_eq!(viewed.to_view_bytes(), owned.to_view_bytes());
    }
    // Dense over the spread range would hold every bucket between 1 and
    // 60^4 (about 820) at one byte each; sparse holds 60 keys and counts.
    assert!(spread.to_view_bytes().len() < 600);
}

/// A tiny alpha puts a few samples millions of buckets apart: a view of
/// about a hundred bytes whose alpha or bucket keys are rewritten so its
/// store spans that far (or past the indexable range) is refused, never
/// turned into a multi-gigabyte owned sketch.
#[test]
fn tiny_alpha_views_cannot_force_huge_allocations() {
    // Two samples, 1 and 1e6, at alpha 0.01: a sparse view with two keys.
    let mut sketch = DdSketch::new(0.01);
    sketch.add(&1.0);
    sketch.add(&1e6);
    let bytes = sketch.to_view_bytes();
    let words = aligned_copy(&bytes);
    let view = DdSketchView::from_bytes(aligned_bytes(&words, bytes.len())).unwrap();
    assert!(view.is_sparse());
    // Layout: alpha at byte 8, the positive store's offset at 48 and its
    // two u32 keys (relative to the offset) at 64 and 68.
    let patched = |alpha: f64, offset: i32, last_key: u32| {
        let mut b = bytes.clone();
        b[8..16].copy_from_slice(&alpha.to_le_bytes());
        b[48..52].copy_from_slice(&offset.to_le_bytes());
        b[68..72].copy_from_slice(&last_key.to_le_bytes());
        let words = aligned_copy(&b);
        DdSketchView::from_bytes(aligned_bytes(&words, b.len())).map(|v| v.get_count())
    };
    let (offset, last_key) = (
        i32::from_le_bytes(bytes[48..52].try_into().unwrap()),
        u32::from_le_bytes(bytes[68..72].try_into().unwrap()),
    );
    // Unchanged keys at a tiny alpha are a small sketch: accepted.
    assert_eq!(patched(1e-9, offset, last_key), Ok(2));
    // Below the smallest indexable magnitude's bucket at alpha 0.01 (about
    // -35,400): refused, so no accumulator grows down to it.
    assert!(patched(0.01, -1_000_000, last_key).is_err());
    // Indexable at alpha 1e-9 / 1e-6, but spanning 2^30 / 6e6 buckets
    // (8 GB / 48 MB dense): refused.
    assert!(patched(1e-9, 0, 1 << 30).is_err());
    assert!(patched(1e-6, 0, 6_000_000).is_err());
    // Keys under the span cap read.
    assert_eq!(patched(1e-6, 0, 200_000), Ok(2));
}

/// Every view family, its bytes mutated one byte at a time (several values
/// at every position) and in random multi-byte bursts: `from_bytes` either
/// refuses the bytes or accepts a view on which every operation (answers,
/// merges into an accumulator, the owned sketch) returns without panicking.
#[test]
fn mutated_views_never_panic() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    let mut big = dd(9, 300);
    big.apply_delta(asap_sketchlib::sketches::octo_delta::DdSketchBucketDelta {
        sign: asap_sketchlib::sketches::octo_delta::DdSign::Positive,
        index: 300,
        count: u64::from(u32::MAX) + 5,
    });
    let seeds: Vec<(&str, Vec<u8>)> = vec![
        ("dd", dd(3, 300).to_view_bytes()),
        ("dd-signed", {
            let mut s = DdSketch::new(0.02);
            for v in values(4, 200) {
                s.add(&(v - 50_000.0 / 7.0));
            }
            s.to_view_bytes()
        }),
        ("dd-wide", big.to_view_bytes()),
        ("dd-sparse", {
            let mut s = DdSketch::new(0.01);
            for i in 1..=60u32 {
                s.add(&(f64::from(i).powi(4)));
            }
            s.to_view_bytes()
        }),
        // Alpha rewritten to 1e-9: mutated keys may land anywhere in a
        // range of billions of buckets.
        ("dd-tiny-alpha", {
            let mut s = DdSketch::new(0.01);
            s.add(&1.0);
            s.add(&1e6);
            let mut b = s.to_view_bytes();
            b[8..16].copy_from_slice(&1e-9f64.to_le_bytes());
            b
        }),
        ("kll", kll(5, 3_000).to_view_bytes()),
        ("cms", cms(6, 300).to_view_bytes()),
    ];
    let exercise = |family: &str, bytes: &[u8]| match family {
        f if f.starts_with("dd") => {
            if let Ok(view) = DdSketchView::from_bytes(bytes) {
                for q in QS {
                    let _ = view.get_value_at_quantile(q);
                    let _ = view.quantile_interpolated(q);
                }
                let _ = view.get_count();
                let mut owned = view.to_owned_sketch();
                let _ = owned.merge_view(&view);
                let mut other = dd(7, 50);
                let _ = other.merge_view(&view);
                let _ = other.quantile_interpolated(0.5);
            }
        }
        "kll" => {
            if let Ok(view) = KllView::from_bytes(bytes) {
                for q in QS {
                    let _ = view.quantile(q);
                }
                let _ = (view.count(), view.rank(10.0), view.cdf());
                let mut owned = view.to_owned_sketch(1);
                let _ = owned.merge_view(&view);
                let _ = owned.quantile(0.5);
            }
        }
        _ => {
            if let Ok(view) = CountMinF64View::from_bytes(bytes) {
                for key in 0..10 {
                    let _ = view.estimate::<asap_sketchlib::DefaultXxHasher>(&DataInput::I64(key));
                }
                let _ = view.total();
                let mut owned = view.to_owned_sketch();
                let _ = owned.merge_view(&view);
            }
        }
    };
    let mut accepted = 0;
    let mut tried = 0;
    let mut x = 0x9e37_79b9_7f4a_7c15_u64;
    for (family, bytes) in &seeds {
        let mut cases: Vec<Vec<u8>> = Vec::new();
        for at in 0..bytes.len() {
            for value in [0x00, 0x01, 0x7f, 0x80, 0xfe, 0xff] {
                let mut mutated = bytes.clone();
                mutated[at] = value;
                cases.push(mutated);
            }
        }
        for _ in 0..2_000 {
            let mut mutated = bytes.clone();
            for _ in 0..4 {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let at = (x as usize) % mutated.len();
                mutated[at] = (x >> 32) as u8;
            }
            cases.push(mutated);
        }
        for mutated in cases {
            let words = aligned_copy(&mutated);
            let bytes = aligned_bytes(&words, mutated.len());
            tried += 1;
            if DdSketchView::from_bytes(bytes).is_ok()
                || KllView::from_bytes(bytes).is_ok()
                || CountMinF64View::from_bytes(bytes).is_ok()
            {
                accepted += 1;
            }
            let outcome = catch_unwind(AssertUnwindSafe(|| exercise(family, bytes)));
            assert!(outcome.is_ok(), "{family}: a mutated view panicked");
        }
    }
    assert!(
        accepted > 0 && accepted < tried,
        "{accepted} of {tried} accepted"
    );
}

/// A KLL view whose level above 0 is not sorted is refused: a merge
/// treats every such level as one sorted run, so an unsorted one would
/// corrupt the accumulator's levels and every later answer.
#[test]
fn kll_views_with_unsorted_upper_levels_are_refused() {
    let bytes = kll(4, 3_000).to_view_bytes();
    let words = aligned_copy(&bytes);
    let view = KllView::from_bytes(aligned_bytes(&words, bytes.len())).unwrap();
    assert!(view.count() > 3_000 / 2);
    // Layout: magic, version, k, m, num_levels, len (8 bytes each from
    // byte 8), the level boundaries as u32 from byte 40, then the items.
    let num_levels = u64::from_le_bytes(bytes[24..32].try_into().unwrap()) as usize;
    assert!(num_levels >= 2);
    let boundary = |h: usize| {
        let at = 40 + 4 * h;
        u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize
    };
    let items_at = (40 + 4 * (num_levels + 1)).div_ceil(8) * 8;
    let item = |i: usize| items_at + 8 * i;
    // A level above 0 holding at least two items.
    let h = (1..num_levels)
        .find(|&h| boundary(h + 1) - boundary(h) >= 2)
        .expect("a level above 0 with two items");
    let (first, last) = (boundary(h), boundary(h + 1) - 1);
    let mut forged = bytes.clone();
    // Swap that level's smallest and largest items.
    let (low, high) = (
        bytes[item(first)..item(first) + 8].to_vec(),
        bytes[item(last)..item(last) + 8].to_vec(),
    );
    assert_ne!(low, high);
    forged[item(first)..item(first) + 8].copy_from_slice(&high);
    forged[item(last)..item(last) + 8].copy_from_slice(&low);
    let words = aligned_copy(&forged);
    assert!(KllView::from_bytes(aligned_bytes(&words, forged.len())).is_err());
    // Level 0 is unsorted in a real sketch: reordering it is accepted.
    let level0 = boundary(1);
    if level0 >= 2 {
        let mut reordered = bytes.clone();
        let (a, b) = (
            bytes[item(0)..item(0) + 8].to_vec(),
            bytes[item(1)..item(1) + 8].to_vec(),
        );
        reordered[item(0)..item(0) + 8].copy_from_slice(&b);
        reordered[item(1)..item(1) + 8].copy_from_slice(&a);
        let words = aligned_copy(&reordered);
        assert!(KllView::from_bytes(aligned_bytes(&words, reordered.len())).is_ok());
    }
}

/// Every view operation counts the same analytics ops as the owned
/// sketch's method it mirrors, so op-count measurements of a query read
/// from views equal those of the owned sketches. A sparse DDSketch view
/// holds only its occupied buckets, so it reads (and counts) at most the
/// owned sketch's ops.
#[cfg(feature = "op-count")]
#[test]
fn views_count_the_ops_of_the_owned_sketch() {
    use asap_sketchlib::analytics::ops::count;
    // KLL: answers, and merges into the same accumulator.
    for n in [0, 15, 5_000] {
        let owned = kll(n as u64 + 2, n);
        let bytes = owned.to_view_bytes();
        let words = aligned_copy(&bytes);
        let view = KllView::from_bytes(aligned_bytes(&words, bytes.len())).unwrap();
        assert_eq!(count(|| view.count()), count(|| owned.count()), "count {n}");
        for x in [-3_000.0, 0.0, 5_000.0] {
            assert_eq!(count(|| view.rank(x)), count(|| owned.rank(x)), "rank {n}");
        }
        assert_eq!(
            count(|| view.quantile(0.5)).1,
            count(|| owned.cdf().value_at(0.5)).1,
            "quantile {n}"
        );
        let accumulator = kll(9, 2_000);
        let (mut by_owned, mut by_view) = (accumulator.clone(), accumulator.clone());
        let ((), owned_ops) = count(|| by_owned.merge(&owned));
        let (merged, view_ops) = count(|| by_view.merge_view(&view));
        merged.unwrap();
        assert_eq!(view_ops, owned_ops, "merge {n}");
    }
    // DDSketch: answers, and merges into an empty and a non-empty sketch,
    // from dense and sparse views.
    let mut sparse_seen = false;
    let mut dense_seen = false;
    for n in [0, 15, 1_000, 20_000] {
        let owned = dd(n as u64 + 2, n);
        let bytes = owned.to_view_bytes();
        let words = aligned_copy(&bytes);
        let view = DdSketchView::from_bytes(aligned_bytes(&words, bytes.len())).unwrap();
        let sparse = view.is_sparse();
        sparse_seen |= sparse;
        dense_seen |= !sparse && n > 0;
        let same_ops = |what: &str, view_ops: u64, owned_ops: u64| {
            if sparse {
                assert!(
                    view_ops <= owned_ops,
                    "{what} {n}: {view_ops} > {owned_ops}"
                );
            } else {
                assert_eq!(view_ops, owned_ops, "{what} {n}");
            }
        };
        for q in QS {
            let (a, view_ops) = count(|| view.get_value_at_quantile(q));
            let (b, owned_ops) = count(|| owned.get_value_at_quantile(q));
            assert_eq!(a, b);
            same_ops("quantile", view_ops, owned_ops);
            let (a, view_ops) = count(|| view.quantile_interpolated(q));
            let (b, owned_ops) = count(|| owned.quantile_interpolated(q));
            assert_eq!(a, b);
            same_ops("interpolated", view_ops, owned_ops);
        }
        for accumulator in [DdSketch::new(0.01), dd(9, 500)] {
            let (mut by_owned, mut by_view) = (accumulator.clone(), accumulator.clone());
            let (a, owned_ops) = count(|| by_owned.merge(&owned));
            let (b, view_ops) = count(|| by_view.merge_view(&view));
            a.unwrap();
            b.unwrap();
            same_ops("merge", view_ops, owned_ops);
        }
    }
    assert!(sparse_seen && dense_seen);
    // Count-Min: point estimates and merges.
    let owned = cms(2, 500);
    let bytes = owned.to_view_bytes();
    let words = aligned_copy(&bytes);
    let view = CountMinF64View::from_bytes(aligned_bytes(&words, bytes.len())).unwrap();
    for key in 0..20 {
        let key = DataInput::I64(key);
        assert_eq!(
            count(|| view.estimate::<asap_sketchlib::DefaultXxHasher>(&key)).1,
            count(|| owned.estimate(&key)).1
        );
    }
    let (mut by_owned, mut by_view) = (cms(1, 500), cms(1, 500));
    let ((), owned_ops) = count(|| by_owned.merge(&owned));
    let (merged, view_ops) = count(|| by_view.merge_view(&view));
    merged.unwrap();
    assert_eq!(view_ops, owned_ops, "Count-Min merge");
}
