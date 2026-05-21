//! Offline 5-minute market simulation for FV calibration.
//!
//! Runs a noisy-but-reproducible BTC price path through the production
//! scoring code (real `VolEstimator`, real `score()` function, real
//! default weights), simulates a Polymarket mid that lags the true
//! Black-Scholes probability, and prints the calibration gap
//! distribution.
//!
//! Run with:
//!   cargo run -p bot --bin sim_5min
//!
//! BTC path is deterministic via a seeded PRNG — same numbers every
//! run. The point isn't statistical coverage; it's "show me a concrete
//! example of what the model does against a plausible market".

use std::collections::VecDeque;

use bot::fv::VolEstimator;
use bot::signals::scoring::{score, Features, Regime, ScoringConfig};
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;

// ---------------------------------------------------------------------------
// Simulation parameters
// ---------------------------------------------------------------------------

const STRIKE: f64 = 100_000.0;
const MARKET_DURATION_SECS: f64 = 300.0;
const TICK_DT_SECS: f64 = 1.0;
/// Realistic BTC vol — about 50% annualised → 3% daily → 5e-5/sec.
const BTC_TRUE_SIGMA_PER_SEC: f64 = 5.0e-5;
/// Polymarket consensus is approximated as a low-pass filter of the
/// "true" probability Φ(z_BS). At α=0.20 per second, the time constant
/// is ~5s — matches the observed cadence at which poly mids respond to
/// upstream BTC moves.
const POLY_LAG_ALPHA: f64 = 0.20;

// ---------------------------------------------------------------------------
// BTC path: noisy GBM-style walk with piecewise drift. Three phases:
//   t ∈ [0, 60):    no drift, just noise (uncertain regime)
//   t ∈ [60, 180):  drift up ~$200 over 120s (clear bullish signal)
//   t ∈ [180, 240): no drift, noise only (consolidation)
//   t ∈ [240, 300]: drift down ~$100 over 60s (reversal)
// Scale is meaningful: BTC at $100k with σ ≈ 5e-5/sec means a
// 1σ move over 60s is ~$40, so a deterministic drift of $200 over
// 120s is ~3σ — strong but not absurd.
// ---------------------------------------------------------------------------

fn drift_at(t: f64) -> f64 {
    if t < 60.0 {
        0.0
    } else if t < 180.0 {
        200.0 / 120.0
    } else if t < 240.0 {
        0.0
    } else {
        -100.0 / 60.0
    }
}

// ---------------------------------------------------------------------------
// BTC history ring for drift z-scores
// ---------------------------------------------------------------------------

struct BtcRing {
    samples: VecDeque<(f64, f64)>,
}

impl BtcRing {
    fn new() -> Self {
        Self {
            samples: VecDeque::new(),
        }
    }
    fn push(&mut self, t: f64, price: f64) {
        self.samples.push_back((t, price));
        while let Some(&(t0, _)) = self.samples.front() {
            if t - t0 > 600.0 {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }
    fn log_return_over(&self, window_secs: f64) -> Option<f64> {
        let (t_last, p_last) = *self.samples.back()?;
        let target_t = t_last - window_secs;
        // Find the sample at or just before target_t.
        let (_, p_then) = self
            .samples
            .iter()
            .find(|(t, _)| *t >= target_t)
            .copied()?;
        if p_then > 0.0 {
            Some((p_last / p_then).ln())
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Synthetic flow + momentum signals derived from the BTC path.
// In production these come from the Binance @trade feed; here we
// approximate them as proportional to recent log-returns.
// ---------------------------------------------------------------------------

fn synthetic_flow(drift_30s_z: Option<f64>) -> Option<f64> {
    // Flow imbalance ∈ [-1, 1]. Tracks drift but with diminishing
    // returns at the extremes (real flow saturates).
    drift_30s_z.map(|z| (z / 2.0).tanh())
}

// ---------------------------------------------------------------------------
// Main loop
// ---------------------------------------------------------------------------

/// Standard normal CDF via Abramowitz & Stegun 26.2.17 approximation
/// — same as `bot::fv::norm_cdf` but inlined to avoid pulling in a
/// private module here.
fn phi(z: f64) -> f64 {
    let z = z.clamp(-10.0, 10.0);
    // Use the same form `bot::fv` uses.
    0.5 * (1.0 + libm_erf(z / std::f64::consts::SQRT_2))
}

fn libm_erf(x: f64) -> f64 {
    // Abramowitz & Stegun 7.1.26 — same accuracy class as Φ.
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + 0.3275911 * x);
    let y = 1.0
        - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592)
            * t
            * (-x * x).exp();
    sign * y
}

fn main() {
    let cfg = ScoringConfig::default();
    let mut vol = VolEstimator::new(60.0); // 60s rolling σ — matches bot default
    let mut btc_history = BtcRing::new();
    let mut rng = StdRng::seed_from_u64(2026_05_21);
    // Standard-normal sampler via Box-Muller.
    let mut box_muller_buffer: Option<f64> = None;
    let mut sample_std_normal = |rng: &mut StdRng,
                                 buffer: &mut Option<f64>|
     -> f64 {
        if let Some(z) = buffer.take() {
            return z;
        }
        let u1: f64 = rng.gen_range(1e-12..1.0);
        let u2: f64 = rng.gen_range(0.0..1.0);
        let r = (-2.0 * u1.ln()).sqrt();
        let theta = 2.0 * std::f64::consts::PI * u2;
        *buffer = Some(r * theta.sin());
        r * theta.cos()
    };
    // Simulated poly mid: lags Φ(true_z), independent of bot output.
    let mut sim_poly_mid: f64 = 0.50;
    let mut btc: f64 = STRIKE;

    // For the longer-window drift we also need a 5s and 300s view —
    // those use independent VolEstimators in the bot. Here we'll just
    // use the same one for simplicity.

    // Stats
    let mut gaps: Vec<f64> = Vec::new();
    let mut n_within_2pp: u32 = 0;
    let mut n_within_5pp: u32 = 0;
    let mut n_within_10pp: u32 = 0;

    println!(
        "{:>5}  {:>8}  {:>6}  {:>7}  {:>5}  {:>7}  {:>7}  {:>6}",
        "t_s", "btc", "z_BS", "drift30", "flow", "fv_yes", "poly", "gap"
    );
    println!("{}", "-".repeat(70));

    let num_steps = (MARKET_DURATION_SECS / TICK_DT_SECS) as usize;
    for step in 0..=num_steps {
        let t = step as f64 * TICK_DT_SECS;
        let ttr = (MARKET_DURATION_SECS - t).max(0.0);

        // BTC step: deterministic drift + Gaussian noise scaled by σ.
        // σ × √Δt is the 1-tick stdev; this is plain GBM-style update
        // (linearised for $-units since price ≫ price changes).
        let drift = drift_at(t) * TICK_DT_SECS;
        let z_noise = sample_std_normal(&mut rng, &mut box_muller_buffer);
        let noise = STRIKE * BTC_TRUE_SIGMA_PER_SEC * TICK_DT_SECS.sqrt() * z_noise;
        if step > 0 {
            btc += drift + noise;
        }

        // Feed vol estimator + history.
        vol.observe(t, btc);
        btc_history.push(t, btc);

        if ttr <= 0.0 {
            break;
        }

        // Pull σ. Fallback to truth value early before window fills.
        let sigma = vol
            .sigma_per_sec()
            .filter(|s| s.is_finite() && *s > 1e-10)
            .unwrap_or(BTC_TRUE_SIGMA_PER_SEC);

        // Features.
        let z_bs = {
            let sigma_t = sigma * ttr.sqrt();
            if sigma_t > 0.0 {
                Some((btc / STRIKE).ln() / sigma_t)
            } else {
                None
            }
        };
        let drift_30s_z = btc_history.log_return_over(30.0).and_then(|r| {
            let sigma_t = sigma * 30.0_f64.sqrt();
            if sigma_t > 0.0 { Some(r / sigma_t) } else { None }
        });
        let drift_300s_z = btc_history.log_return_over(300.0).and_then(|r| {
            let sigma_t = sigma * 300.0_f64.sqrt();
            if sigma_t > 0.0 { Some(r / sigma_t) } else { None }
        });
        let momentum = match (drift_30s_z, drift_300s_z) {
            (Some(s), Some(l)) => Some(s - l),
            _ => None,
        };
        let flow = synthetic_flow(drift_30s_z);

        let features = Features {
            btc_strike_distance_z: z_bs,
            btc_drift_30s_z: drift_30s_z,
            btc_momentum: momentum,
            binance_flow_imbalance_60s: flow,
            // No yes_book_imbalance — assume balanced book.
            ..Default::default()
        };

        let regime = Regime::from_ttr_secs(ttr);
        let fv_p_yes = match score(&features, regime, &cfg) {
            Some(o) => o.p_yes,
            None => continue,
        };

        // Simulate poly mid: low-pass filter on the TRUE BS
        // probability — `Φ(true_z)` where true_z uses the known σ,
        // not the bot's estimated σ. This decouples the poly
        // simulator from the bot's output (no circular dependency).
        let true_sigma_t = BTC_TRUE_SIGMA_PER_SEC * ttr.sqrt();
        let true_z = if true_sigma_t > 0.0 {
            (btc / STRIKE).ln() / true_sigma_t
        } else {
            0.0
        };
        let true_p = phi(true_z);
        sim_poly_mid = POLY_LAG_ALPHA * true_p + (1.0 - POLY_LAG_ALPHA) * sim_poly_mid;

        let gap = fv_p_yes - sim_poly_mid;
        gaps.push(gap);
        let abs_gap = gap.abs();
        if abs_gap < 0.02 { n_within_2pp += 1; }
        if abs_gap < 0.05 { n_within_5pp += 1; }
        if abs_gap < 0.10 { n_within_10pp += 1; }

        // Print every 10s and at key transitions.
        let print_row = (step % 10 == 0)
            || (step as f64 - 60.0).abs() < 0.5
            || (step as f64 - 180.0).abs() < 0.5
            || (step as f64 - 240.0).abs() < 0.5;
        if print_row {
            println!(
                "{:>5.0}  {:>8.1}  {:>6.2}  {:>7}  {:>5}  {:>7.3}  {:>7.3}  {:>+6.3}",
                t,
                btc,
                z_bs.unwrap_or(f64::NAN),
                drift_30s_z.map(|v| format!("{:>7.2}", v)).unwrap_or_else(|| "    -  ".to_string()),
                flow.map(|v| format!("{:>5.2}", v)).unwrap_or_else(|| "  -  ".to_string()),
                fv_p_yes,
                sim_poly_mid,
                gap,
            );
        }
    }

    // Summary.
    println!();
    println!("=== Calibration summary (n = {} ticks) ===", gaps.len());
    let n = gaps.len() as f64;
    if n > 0.0 {
        let mean = gaps.iter().sum::<f64>() / n;
        let variance = gaps.iter().map(|g| (g - mean).powi(2)).sum::<f64>() / n;
        let stdev = variance.sqrt();
        let abs_gaps: Vec<f64> = gaps.iter().map(|g| g.abs()).collect();
        let mut sorted = abs_gaps.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = sorted[sorted.len() / 2];
        let p95 = sorted[(sorted.len() as f64 * 0.95) as usize];
        let max_gap = sorted.last().copied().unwrap_or(0.0);
        println!("  mean signed gap (fv − poly):  {:+.4}", mean);
        println!("  stdev of gap:                 {:.4}", stdev);
        println!("  median |gap|:                 {:.4}", median);
        println!("  p95 |gap|:                    {:.4}", p95);
        println!("  max |gap|:                    {:.4}", max_gap);
        println!();
        println!("  within  2pp:  {:>3.0}% ({}/{})", 100.0 * n_within_2pp as f64 / n, n_within_2pp, gaps.len());
        println!("  within  5pp:  {:>3.0}% ({}/{})", 100.0 * n_within_5pp as f64 / n, n_within_5pp, gaps.len());
        println!("  within 10pp:  {:>3.0}% ({}/{})", 100.0 * n_within_10pp as f64 / n, n_within_10pp, gaps.len());
        println!();
        // DQ gate interpretation.
        let n_above_10pp = gaps.len() - n_within_10pp as usize;
        if n_above_10pp > 0 {
            println!(
                "  ⚠ {} ticks ({:.0}%) would trip the ModelDivergence DQ gate (|gap| > 10pp)",
                n_above_10pp,
                100.0 * n_above_10pp as f64 / n
            );
        } else {
            println!("  ✓ no ticks exceed the 10pp ModelDivergence threshold");
        }
    }
}
