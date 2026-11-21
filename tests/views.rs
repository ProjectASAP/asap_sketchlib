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
        assert_eq!(view.min(), owned.min());
        assert_eq!(view.max(), owned.max());
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
        // The view reads the bytes in place.
        if let Some(ptr) = view.bucket_counts_ptr() {
            let range = bytes.as_ptr_range();
            assert!(range.contains(&ptr.cast::<u8>()));
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
    for (window, (words, len)) in windows.iter().zip(&stored) {
        owned.merge(window).unwrap();
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
        let range = aligned_bytes(&words, bytes.len()).as_ptr_range();
        assert!(range.contains(&view.items_ptr().cast::<u8>()));
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
