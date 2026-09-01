use super::truth::NumericTruth;

pub fn assert_rel_close(actual: f64, expected: f64, rel_tol: f64, label: &str) {
    let rel = if expected == 0.0 {
        actual.abs()
    } else {
        ((actual - expected) / expected).abs()
    };
    assert!(
        rel <= rel_tol,
        "{label}: expected ~{expected:.6}, got {actual:.6} (rel err {rel:.5} > tol {rel_tol})"
    );
}

pub fn assert_between(actual: f64, lo: f64, hi: f64, label: &str) {
    assert!(
        actual >= lo && actual <= hi,
        "{label}: got {actual:.6}, outside allowed band [{lo:.6}, {hi:.6}]"
    );
}

pub fn assert_in_rank_band(est: f64, truth: &NumericTruth, q: f64, tol: f64, label: &str) {
    let (lo, hi) = truth.quantile_band(q, tol);
    assert!(
        est >= lo && est <= hi,
        "{label}: q={q} estimate {est:.4} outside rank band [{lo:.4}, {hi:.4}]"
    );
}
