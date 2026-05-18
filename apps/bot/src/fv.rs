//! Fair-value model: P(BTC closes above strike) under GBM with rolling
//! realised volatility.
//!
//! `compute_fv(S, K, T, σ)` → P(BTC_T > K) where
//!   d = (ln(S/K) − σ²·T/2) / (σ·√T)
//!   P = Φ(d)
//!
//! Drift is taken as 0 — 5-minute horizons are too short for drift to
//! matter against σ.
//!
//! All inputs are unitless except T (seconds) and σ (per-second).

use std::collections::VecDeque;

// ---------------------------------------------------------------------------
// Φ — standard-normal CDF, Abramowitz & Stegun 7.1.26 approximation
// ---------------------------------------------------------------------------

/// Standard-normal CDF. Max abs error < 7.5e-8 per A&S 26.2.17.
pub fn norm_cdf(x: f64) -> f64 {
    if x.is_nan() {
        return f64::NAN;
    }
    // Symmetry: Φ(-x) = 1 - Φ(x).
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let xa = x.abs();

    // Coefficients.
    const A1: f64 = 0.319_381_530;
    const A2: f64 = -0.356_563_782;
    const A3: f64 = 1.781_477_937;
    const A4: f64 = -1.821_255_978;
    const A5: f64 = 1.330_274_429;
    const P: f64 = 0.231_641_9;

    let k = 1.0 / (1.0 + P * xa);
    let phi = (1.0 / (2.0 * std::f64::consts::PI).sqrt()) * (-0.5 * xa * xa).exp();
    let approx =
        1.0 - phi * (A1 * k + A2 * k.powi(2) + A3 * k.powi(3) + A4 * k.powi(4) + A5 * k.powi(5));

    // approx is Φ(xa) for xa ≥ 0. Reflect for negative inputs.
    if sign < 0.0 {
        1.0 - approx
    } else {
        approx
    }
}

// ---------------------------------------------------------------------------
// FairValue
// ---------------------------------------------------------------------------

/// Continuous probability output. `p_yes + p_no == 1`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FairValue {
    pub p_yes: f64,
    pub p_no: f64,
}

impl FairValue {
    pub fn from_p_yes(p: f64) -> Self {
        let p = p.clamp(0.0, 1.0);
        Self {
            p_yes: p,
            p_no: 1.0 - p,
        }
    }
}

/// Inverse of `norm_cdf` via 40-iteration bisection on [-10, 10].
/// Sufficient precision for diagnostic / logging use (≈ 1e-11 abs error
/// on the input axis). Not optimised for hot paths.
pub fn norm_cdf_inverse(p: f64) -> f64 {
    if p.is_nan() || !(0.0..=1.0).contains(&p) {
        return f64::NAN;
    }
    if p <= 1e-15 {
        return -10.0;
    }
    if p >= 1.0 - 1e-15 {
        return 10.0;
    }
    let (mut lo, mut hi) = (-10.0_f64, 10.0_f64);
    for _ in 0..40 {
        let mid = 0.5 * (lo + hi);
        if norm_cdf(mid) < p {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    0.5 * (lo + hi)
}

/// Inverse of `compute_fv` on K: find the strike that makes the model
/// predict `p_yes` for P(BTC_T > K) given the other inputs.
///
/// Used by the bot's diagnostic logging to back out what strike
/// Polymarket appears to be pricing against. Comparing this to the
/// Binance-snapped strike quantifies any cross-venue oracle divergence.
///
/// Returns `None` if inputs are degenerate or `p_yes` is too close to
/// 0/1 for a reliable inverse.
pub fn implied_strike(
    btc_now: f64,
    secs_to_resolution: f64,
    sigma_per_sec: f64,
    p_yes: f64,
) -> Option<f64> {
    if !(btc_now.is_finite()
        && secs_to_resolution.is_finite()
        && sigma_per_sec.is_finite()
        && p_yes.is_finite())
    {
        return None;
    }
    if btc_now <= 0.0 || secs_to_resolution <= 0.0 || sigma_per_sec <= 0.0 {
        return None;
    }
    if !(0.001..=0.999).contains(&p_yes) {
        return None;
    }
    let sigma_t = sigma_per_sec * secs_to_resolution.sqrt();
    let d = norm_cdf_inverse(p_yes);
    // From compute_fv: d = (ln(S/K) - σ²T/2) / (σ√T), so
    //   ln(S/K) = σ√T·d + σ²T/2
    //   K = S · exp(-(σ√T·d + σ²T/2))
    let log_ratio = sigma_t * d + 0.5 * sigma_t * sigma_t;
    Some(btc_now * (-log_ratio).exp())
}

/// Compute P(BTC_T > K) under zero-drift GBM.
///
/// - `btc_now`: current BTC mid, USD.
/// - `strike`: BTC mid at market open, USD.
/// - `secs_to_resolution`: T, seconds. Must be > 0.
/// - `sigma_per_sec`: σ on log-returns, per second. Must be > 0.
pub fn compute_fv(
    btc_now: f64,
    strike: f64,
    secs_to_resolution: f64,
    sigma_per_sec: f64,
) -> FairValue {
    if !(btc_now.is_finite()
        && strike.is_finite()
        && secs_to_resolution.is_finite()
        && sigma_per_sec.is_finite())
        || btc_now <= 0.0
        || strike <= 0.0
        || secs_to_resolution <= 0.0
        || sigma_per_sec <= 0.0
    {
        // Degenerate input: snap to current price comparison.
        let p = if btc_now > strike {
            1.0
        } else if btc_now < strike {
            0.0
        } else {
            0.5
        };
        return FairValue::from_p_yes(p);
    }
    let sigma_t = sigma_per_sec * secs_to_resolution.sqrt();
    let d = ((btc_now / strike).ln() - 0.5 * sigma_t * sigma_t) / sigma_t;
    FairValue::from_p_yes(norm_cdf(d))
}

// ---------------------------------------------------------------------------
// VolEstimator — rolling realised σ from a stream of (t_secs, btc_mid)
// ---------------------------------------------------------------------------

/// Rolling realised-vol estimator. Holds a window of `(t_secs, mid)` and
/// produces σ per second from log-returns.
///
/// Numerically simple: we don't try to be incremental — recompute over the
/// window on demand. The window is small (≤ a few hundred samples for a
/// 60-second window) so this is cheap.
#[derive(Debug, Clone)]
pub struct VolEstimator {
    window_secs: f64,
    samples: VecDeque<(f64, f64)>,
    min_samples: usize,
}

impl VolEstimator {
    pub fn new(window_secs: f64) -> Self {
        Self {
            window_secs,
            samples: VecDeque::new(),
            min_samples: 5,
        }
    }

    /// Push an observation. Pruning of stale samples happens here.
    pub fn observe(&mut self, t_secs: f64, mid: f64) {
        if !(t_secs.is_finite() && mid.is_finite()) || mid <= 0.0 {
            return;
        }
        self.samples.push_back((t_secs, mid));
        let cutoff = t_secs - self.window_secs;
        while let Some(&(t, _)) = self.samples.front() {
            if t < cutoff {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    /// Number of currently retained observations.
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// Estimate σ on log-returns, per second. `None` if not enough data.
    pub fn sigma_per_sec(&self) -> Option<f64> {
        if self.samples.len() < self.min_samples {
            return None;
        }
        // Compute log-returns paired with their dt.
        let mut sum_r2_per_dt = 0.0;
        let mut n = 0usize;
        let mut prev: Option<(f64, f64)> = None;
        for &(t, mid) in &self.samples {
            if let Some((tp, mp)) = prev {
                let dt = t - tp;
                if dt > 0.0 && mp > 0.0 && mid > 0.0 {
                    let r = (mid / mp).ln();
                    sum_r2_per_dt += (r * r) / dt;
                    n += 1;
                }
            }
            prev = Some((t, mid));
        }
        if n == 0 {
            return None;
        }
        let var_per_sec = sum_r2_per_dt / (n as f64);
        Some(var_per_sec.sqrt())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64, eps: f64) -> bool {
        (a - b).abs() < eps
    }

    #[test]
    fn norm_cdf_known_values() {
        assert!(approx(norm_cdf(0.0), 0.5, 1e-6));
        assert!(approx(norm_cdf(1.0), 0.841_345, 1e-4));
        assert!(approx(norm_cdf(-1.0), 0.158_655, 1e-4));
        assert!(approx(norm_cdf(1.96), 0.975_002, 1e-4));
        assert!(approx(norm_cdf(-1.96), 0.024_998, 1e-4));
        assert!(approx(norm_cdf(3.0), 0.998_650, 1e-4));
    }

    #[test]
    fn fv_is_half_when_at_strike() {
        let fv = compute_fv(100_000.0, 100_000.0, 300.0, 5e-5);
        assert!(approx(fv.p_yes, 0.5, 1e-3));
        assert!(approx(fv.p_no, 0.5, 1e-3));
    }

    #[test]
    fn fv_saturates_to_one_when_deep_itm() {
        // BTC $1000 above strike with 60s to go and modest vol — should be
        // very near 1.0.
        let fv = compute_fv(101_000.0, 100_000.0, 60.0, 5e-5);
        assert!(fv.p_yes > 0.99, "got p_yes = {}", fv.p_yes);
    }

    #[test]
    fn fv_saturates_to_zero_when_deep_otm() {
        let fv = compute_fv(99_000.0, 100_000.0, 60.0, 5e-5);
        assert!(fv.p_yes < 0.01, "got p_yes = {}", fv.p_yes);
    }

    #[test]
    fn fv_is_monotonic_in_btc_price() {
        let strike = 100_000.0;
        let mut last = -1.0;
        for delta in [-500.0, -250.0, -100.0, 0.0, 100.0, 250.0, 500.0] {
            let fv = compute_fv(strike + delta, strike, 120.0, 5e-5);
            assert!(fv.p_yes >= last, "non-monotonic at delta={delta}");
            last = fv.p_yes;
        }
    }

    #[test]
    fn user_deep_itm_example_self_suppresses() {
        // $192 above strike, 60s to resolution, then a $4 move. Edge gap
        // should be tiny in both states.
        let strike = 100_000.0;
        let sigma = 5e-5;
        let fv_before = compute_fv(strike + 192.0, strike, 60.0, sigma);
        let fv_after = compute_fv(strike + 196.0, strike, 60.0, sigma);
        // Both are already ~1.0; a $4 move shouldn't matter.
        assert!(fv_before.p_yes > 0.99);
        assert!(fv_after.p_yes > 0.99);
        // The delta in FV from the $4 move is negligible.
        assert!((fv_after.p_yes - fv_before.p_yes).abs() < 0.005);
    }

    #[test]
    fn vol_estimator_returns_none_with_few_samples() {
        let est = VolEstimator::new(60.0);
        assert_eq!(est.sigma_per_sec(), None);
    }

    #[test]
    fn vol_estimator_recovers_a_known_sigma_roughly() {
        // Generate a deterministic walk with a known per-step σ.
        let mut est = VolEstimator::new(120.0);
        let mut price = 100_000.0;
        let n = 100;
        // 1-second steps, log-return magnitude alternates ±0.0001 → σ ≈ 1e-4
        for i in 0..n {
            let sign = if i % 2 == 0 { 1.0 } else { -1.0 };
            price *= (sign * 1.0e-4_f64).exp();
            est.observe(i as f64, price);
        }
        let s = est.sigma_per_sec().expect("enough data");
        // Should be roughly 1e-4 per second.
        assert!(s > 5e-5 && s < 2e-4, "got σ = {}", s);
    }

    #[test]
    fn vol_estimator_prunes_old_samples() {
        let mut est = VolEstimator::new(10.0);
        for i in 0..20 {
            est.observe(i as f64, 100_000.0);
        }
        // Window is 10s; we wrote at t = 0..19, so only t in (9, 19] survive.
        assert!(est.len() <= 11);
    }

    #[test]
    fn norm_cdf_inverse_round_trips() {
        for p in [0.01, 0.10, 0.34, 0.50, 0.66, 0.90, 0.99] {
            let x = norm_cdf_inverse(p);
            let back = norm_cdf(x);
            assert!(
                (back - p).abs() < 1e-6,
                "round-trip p={p}: x={x}, back={back}"
            );
        }
    }

    #[test]
    fn norm_cdf_inverse_known_points() {
        assert!((norm_cdf_inverse(0.5) - 0.0).abs() < 1e-6);
        assert!((norm_cdf_inverse(0.84134) - 1.0).abs() < 1e-3);
        assert!((norm_cdf_inverse(0.15866) + 1.0).abs() < 1e-3);
    }

    #[test]
    fn implied_strike_round_trips() {
        // For a chosen K, compute fv → p, then invert → K_back ≈ K.
        let s = 78_000.0;
        let t = 300.0;
        let sigma = 1e-4;
        for k_offset in [-100.0, -25.0, 0.0, 25.0, 100.0] {
            let k = s + k_offset;
            let fv = compute_fv(s, k, t, sigma);
            let k_back = implied_strike(s, t, sigma, fv.p_yes).expect("invertible");
            assert!(
                (k_back - k).abs() / k < 1e-3,
                "k={k}, k_back={k_back}, fv={}",
                fv.p_yes
            );
        }
    }

    #[test]
    fn implied_strike_returns_none_for_degenerate_inputs() {
        assert!(implied_strike(0.0, 300.0, 1e-4, 0.5).is_none());
        assert!(implied_strike(78_000.0, 0.0, 1e-4, 0.5).is_none());
        assert!(implied_strike(78_000.0, 300.0, 0.0, 0.5).is_none());
        assert!(implied_strike(78_000.0, 300.0, 1e-4, 0.0).is_none());
        assert!(implied_strike(78_000.0, 300.0, 1e-4, 1.0).is_none());
        assert!(implied_strike(78_000.0, 300.0, 1e-4, f64::NAN).is_none());
    }
}
