//! Multi-factor hand-coded scoring model.
//!
//! Given a [`Features`] snapshot (extracted by the bot from runtime
//! state) and a per-regime weight set, produces a "true P(YES)"
//! estimate via a weighted linear combination passed through the
//! standard-normal CDF Φ.
//!
//! Default weights deliberately recover the GBM-around-strike model as
//! a special case: only `w_btc_strike_distance_z = 1.0`, every other
//! weight is `0.0`. That makes the v1 scoring engine behave identically
//! to the previous `compute_fv()` baseline, and gives a calibrated
//! starting point for tuning each additional feature's weight by hand
//! against observed outcomes.
//!
//! Per-regime structure: weights vary between `early` (TTR > 240s),
//! `mid` (60–240s) and `late` (≤ 60s) — different microstructure
//! effects dominate at each phase of a 5-minute market.
//!
//! No ML training. All weights are human-readable, tunable in
//! `configs/bot.toml`, and explicit about which feature contributes
//! how much. See `docs/BOT_ARCHITECTURE_AND_BUILD_PLAN.md` §4.

use serde::{Deserialize, Serialize};

use crate::fv::norm_cdf;

// ---------------------------------------------------------------------------
// Features
// ---------------------------------------------------------------------------

/// Snapshot of model inputs at one decision tick. Each field is `Option`
/// because some features aren't computable on cold start (no history
/// yet) or in degenerate regimes (σ = 0).
///
/// Normalisation convention: all features should be in units of
/// "standard deviations" or ratios in [-1, 1], so weights can be
/// reasoned about on a common scale. Magnitudes much larger than ~3
/// are capped at extraction time (in the bot, not here).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Features {
    /// `(BTC_mid − strike) / (σ × √TTR)`. Sole feature required for a
    /// non-degenerate score; absence → `score` returns `None`.
    pub btc_strike_distance_z: Option<f64>,

    /// BTC log-return over 5s, normalised by σ_5s. Sign = direction,
    /// magnitude = standard deviations of recent move.
    pub btc_drift_5s_z: Option<f64>,
    pub btc_drift_30s_z: Option<f64>,
    pub btc_drift_60s_z: Option<f64>,

    /// `(yes_bid_size − yes_ask_size) / (yes_bid_size + yes_ask_size)`.
    /// Positive = more weight on the bid (bullish for YES). Range
    /// `[-1, 1]`.
    pub yes_book_imbalance: Option<f64>,
    pub no_book_imbalance: Option<f64>,

    /// Normalised YES spread: `(yes_spread − baseline) / baseline`.
    /// Wider-than-baseline = positive = penalty signal (less confident).
    pub yes_spread_normalized: Option<f64>,

    /// Lag feature for the YES side: an estimate of "by how much has
    /// Polymarket failed to catch up to recent BTC moves?". Positive →
    /// BTC moved up and Polymarket hasn't responded → YES under-priced.
    /// Exact formula owned by the bot's feature extractor; the scoring
    /// model treats it as a pre-normalised signed magnitude.
    pub lag_yes: Option<f64>,
}

// ---------------------------------------------------------------------------
// Regimes + weights
// ---------------------------------------------------------------------------

/// Coarse regime for choosing weights. We split only on TTR for v1 —
/// vol regime / trend regime can be added as future axes if data shows
/// they matter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Regime {
    /// TTR > 240s — most of the market's life; FV near 0.5; little
    /// edge for option-theoretic features alone.
    Early,
    /// 60s < TTR ≤ 240s — directional moves start mattering; book
    /// imbalance still informative.
    Mid,
    /// TTR ≤ 60s — endgame; Z-score sensitivity spikes; spreads
    /// typically widen.
    Late,
}

impl Regime {
    pub fn from_ttr_secs(ttr_secs: f64) -> Self {
        if ttr_secs > 240.0 {
            Self::Early
        } else if ttr_secs > 60.0 {
            Self::Mid
        } else {
            Self::Late
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Regime::Early => "early",
            Regime::Mid => "mid",
            Regime::Late => "late",
        }
    }
}

/// Weights for one regime. The naming convention is `w_<feature>`,
/// matching the field on [`Features`] exactly so tuning is mechanical.
///
/// Default = pure GBM recovery: only `w_btc_strike_distance_z = 1.0`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RegimeWeights {
    /// Additive bias before the Φ transform. Use to skew the at-strike
    /// probability away from 0.5 if a regime systematically biases
    /// one side. Default 0.
    pub bias: f64,

    pub w_btc_strike_distance_z: f64,
    pub w_btc_drift_5s_z: f64,
    pub w_btc_drift_30s_z: f64,
    pub w_btc_drift_60s_z: f64,
    pub w_yes_book_imbalance: f64,
    pub w_no_book_imbalance: f64,
    pub w_yes_spread_normalized: f64,
    pub w_lag_yes: f64,
}

impl Default for RegimeWeights {
    fn default() -> Self {
        Self {
            bias: 0.0,
            w_btc_strike_distance_z: 1.0,
            w_btc_drift_5s_z: 0.0,
            w_btc_drift_30s_z: 0.0,
            w_btc_drift_60s_z: 0.0,
            w_yes_book_imbalance: 0.0,
            w_no_book_imbalance: 0.0,
            w_yes_spread_normalized: 0.0,
            w_lag_yes: 0.0,
        }
    }
}

/// All regimes' weights. TOML-loaded; one section per regime.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ScoringConfig {
    pub early: RegimeWeights,
    pub mid: RegimeWeights,
    pub late: RegimeWeights,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            early: RegimeWeights::default(),
            mid: RegimeWeights::default(),
            late: RegimeWeights::default(),
        }
    }
}

impl ScoringConfig {
    pub fn weights_for(&self, regime: Regime) -> &RegimeWeights {
        match regime {
            Regime::Early => &self.early,
            Regime::Mid => &self.mid,
            Regime::Late => &self.late,
        }
    }
}

// ---------------------------------------------------------------------------
// Scoring
// ---------------------------------------------------------------------------

/// Output of one scoring pass.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoringOutcome {
    pub p_yes: f64,
    pub p_no: f64,
    /// Pre-Φ linear combination — diagnostic. Logged so we can see
    /// which features were pulling which way.
    pub raw: f64,
}

/// Score the features. Returns `None` if the anchor feature
/// (`btc_strike_distance_z`) is missing — without it we have no model.
///
/// Other features contribute zero when missing (i.e. `weight × None`
/// behaves as `weight × 0`). That means feature outages degrade
/// gracefully to the simpler subset of the model, instead of failing
/// the whole evaluation.
pub fn score(features: &Features, regime: Regime, cfg: &ScoringConfig) -> Option<ScoringOutcome> {
    let z = features.btc_strike_distance_z?;
    let w = cfg.weights_for(regime);
    let raw = w.bias
        + w.w_btc_strike_distance_z * z
        + w.w_btc_drift_5s_z * features.btc_drift_5s_z.unwrap_or(0.0)
        + w.w_btc_drift_30s_z * features.btc_drift_30s_z.unwrap_or(0.0)
        + w.w_btc_drift_60s_z * features.btc_drift_60s_z.unwrap_or(0.0)
        + w.w_yes_book_imbalance * features.yes_book_imbalance.unwrap_or(0.0)
        + w.w_no_book_imbalance * features.no_book_imbalance.unwrap_or(0.0)
        + w.w_yes_spread_normalized * features.yes_spread_normalized.unwrap_or(0.0)
        + w.w_lag_yes * features.lag_yes.unwrap_or(0.0);
    let p_yes = norm_cdf(raw).clamp(0.0, 1.0);
    Some(ScoringOutcome {
        p_yes,
        p_no: 1.0 - p_yes,
        raw,
    })
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

    fn z_only(z: f64) -> Features {
        Features {
            btc_strike_distance_z: Some(z),
            ..Default::default()
        }
    }

    #[test]
    fn regime_thresholds() {
        assert_eq!(Regime::from_ttr_secs(300.0), Regime::Early);
        assert_eq!(Regime::from_ttr_secs(241.0), Regime::Early);
        assert_eq!(Regime::from_ttr_secs(240.0), Regime::Mid);
        assert_eq!(Regime::from_ttr_secs(120.0), Regime::Mid);
        assert_eq!(Regime::from_ttr_secs(61.0), Regime::Mid);
        assert_eq!(Regime::from_ttr_secs(60.0), Regime::Late);
        assert_eq!(Regime::from_ttr_secs(10.0), Regime::Late);
    }

    #[test]
    fn default_weights_at_strike_returns_half() {
        let cfg = ScoringConfig::default();
        let out = score(&z_only(0.0), Regime::Mid, &cfg).expect("scored");
        assert!(approx(out.p_yes, 0.5, 1e-6));
        assert!(approx(out.p_no, 0.5, 1e-6));
        assert!(approx(out.raw, 0.0, 1e-9));
    }

    #[test]
    fn default_weights_recover_gbm_phi() {
        // With default weights (only Z has weight 1.0), score(z) = Φ(z).
        let cfg = ScoringConfig::default();
        for z in [-1.5, -0.5, 0.5, 1.5] {
            let out = score(&z_only(z), Regime::Mid, &cfg).unwrap();
            assert!(
                approx(out.p_yes, norm_cdf(z), 1e-12),
                "default scoring should equal Φ(z) at z={z}"
            );
        }
    }

    #[test]
    fn missing_z_returns_none() {
        let cfg = ScoringConfig::default();
        let f = Features::default();
        assert!(score(&f, Regime::Mid, &cfg).is_none());
    }

    #[test]
    fn missing_other_features_treated_as_zero() {
        // Only Z is provided; with default weights this still scores fine.
        let cfg = ScoringConfig::default();
        let out = score(&z_only(1.0), Regime::Mid, &cfg).expect("scored");
        assert!(approx(out.p_yes, norm_cdf(1.0), 1e-12));
    }

    #[test]
    fn regime_weights_resolve_independently() {
        let mut cfg = ScoringConfig::default();
        cfg.early.bias = -1.0;
        cfg.late.bias = 1.0;
        let f = z_only(0.0);
        let early = score(&f, Regime::Early, &cfg).unwrap().p_yes;
        let mid = score(&f, Regime::Mid, &cfg).unwrap().p_yes;
        let late = score(&f, Regime::Late, &cfg).unwrap().p_yes;
        // early biases negative → < 0.5; late biases positive → > 0.5;
        // mid is default → 0.5.
        assert!(early < 0.5 - 1e-3);
        assert!(approx(mid, 0.5, 1e-3));
        assert!(late > 0.5 + 1e-3);
    }

    #[test]
    fn yes_book_imbalance_weight_lifts_p_yes() {
        let mut cfg = ScoringConfig::default();
        cfg.mid.w_yes_book_imbalance = 0.5;
        // Z=0 (at strike), but heavy bid pressure on YES.
        let f = Features {
            btc_strike_distance_z: Some(0.0),
            yes_book_imbalance: Some(1.0),
            ..Default::default()
        };
        let out = score(&f, Regime::Mid, &cfg).expect("scored");
        // raw = 0 + 0.5*1 = 0.5 → Φ(0.5) ≈ 0.6915
        assert!(out.p_yes > 0.6 && out.p_yes < 0.75, "got {}", out.p_yes);
    }

    #[test]
    fn negative_weight_inverts_contribution() {
        let mut cfg = ScoringConfig::default();
        cfg.late.w_yes_spread_normalized = -1.0;
        let f = Features {
            btc_strike_distance_z: Some(0.0),
            yes_spread_normalized: Some(1.0), // wider than baseline
            ..Default::default()
        };
        // raw = 0 + (-1)*1 = -1 → Φ(-1) ≈ 0.1587
        let out = score(&f, Regime::Late, &cfg).unwrap();
        assert!(out.p_yes < 0.25, "wide spread should push p_yes down");
    }

    #[test]
    fn raw_score_exposed_for_diagnostic_logging() {
        let mut cfg = ScoringConfig::default();
        cfg.mid.bias = 0.5;
        let out = score(&z_only(1.0), Regime::Mid, &cfg).unwrap();
        // raw = 0.5 + 1.0*1.0 = 1.5
        assert!(approx(out.raw, 1.5, 1e-9));
        assert!(approx(out.p_yes, norm_cdf(1.5), 1e-12));
    }

    #[test]
    fn p_yes_and_p_no_sum_to_one() {
        let cfg = ScoringConfig::default();
        for z in [-2.0, -1.0, 0.0, 0.7, 2.5] {
            let out = score(&z_only(z), Regime::Early, &cfg).unwrap();
            assert!(approx(out.p_yes + out.p_no, 1.0, 1e-12));
        }
    }

    #[test]
    fn round_trip_scoring_config_through_toml() {
        let mut cfg = ScoringConfig::default();
        cfg.mid.w_yes_book_imbalance = 0.3;
        cfg.late.bias = -0.2;
        let s = toml::to_string(&cfg).unwrap();
        let parsed: ScoringConfig = toml::from_str(&s).unwrap();
        assert!(approx(parsed.mid.w_yes_book_imbalance, 0.3, 1e-9));
        assert!(approx(parsed.late.bias, -0.2, 1e-9));
        assert!(approx(parsed.early.w_btc_strike_distance_z, 1.0, 1e-9));
    }
}
