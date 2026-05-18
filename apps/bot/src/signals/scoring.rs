//! Multi-factor hand-coded scoring model — **poly-anchored**.
//!
//! ## Framing
//!
//! Polymarket's mid-price is the market's aggregate consensus. By
//! default we trust it: the bot's `p_yes` equals `poly_mid` exactly.
//! Every other feature is a *correction*: a small log-odds adjustment
//! representing a specific belief that poly is mispriced.
//!
//! ```text
//! raw = bias
//!     + w_poly_logit  × logit(poly_mid)        ← anchor
//!     + w_strike_z    × z_BS                   ← Black-Scholes correction
//!     + w_momentum    × btc_momentum           ← microstructure corrections
//!     + w_book_imbal  × yes_book_imbalance
//!     + …
//! p_yes = sigmoid(raw)
//! ```
//!
//! Default weights: `w_poly_logit = 1.0`, every other weight = 0.
//! Result: `p_yes = sigmoid(logit(poly_mid)) = poly_mid`. The bot will
//! NEVER see an edge until a feature has non-zero weight.
//!
//! ## Why log-odds, not Φ(z)?
//!
//! Earlier versions used `p_yes = Φ(weighted_z_scores)`, which is the
//! Black-Scholes risk-neutral binary call price for the dominant term.
//! That model has two problems:
//!   1. At long TTR with realistic vol, `σ√T` is large and Z is small,
//!      so `Φ(z) ≈ 0.5` regardless of microstructure. The model is
//!      almost always uncertain, even when poly is showing 0.7.
//!   2. It ignores poly's own signal entirely. Polymarket's mid already
//!      aggregates information we don't have (whale flow, off-platform
//!      sentiment). Throwing it away is wasteful.
//!
//! Log-odds anchoring on poly mid fixes both. The BS Z-score is still
//! useful — as a *correction*, scaled by a small coefficient and added
//! to poly's logit.
//!
//! ## Per-regime structure
//!
//! Weights vary between `early` (TTR > 240s), `mid` (60–240s) and
//! `late` (≤ 60s) — different microstructure effects dominate at each
//! phase of a 5-minute market. Each regime has its own
//! `RegimeWeights` block.

use serde::{Deserialize, Serialize};

use crate::fv::norm_cdf;

// ---------------------------------------------------------------------------
// Log-odds helpers
// ---------------------------------------------------------------------------

/// `logit(p) = ln(p / (1-p))`. Maps probability to log-odds space.
/// Clamps to `[1e-6, 1 - 1e-6]` to avoid `±∞` at the boundaries —
/// poly prices of exactly 0 or 1 don't happen on the wire but
/// near-boundary values do at the very end of a market.
pub fn logit(p: f64) -> f64 {
    let p = p.clamp(1e-6, 1.0 - 1e-6);
    (p / (1.0 - p)).ln()
}

/// `sigmoid(x) = 1 / (1 + e^-x)`. Inverse of `logit`.
pub fn sigmoid(x: f64) -> f64 {
    // Numerically stable form: avoid `exp(-x)` overflow at very negative x.
    if x >= 0.0 {
        1.0 / (1.0 + (-x).exp())
    } else {
        let e = x.exp();
        e / (1.0 + e)
    }
}

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
    /// Polymarket's own YES mid — the market's consensus. Anchor of
    /// the log-odds model. Absent → `score` falls back to a pure-BS
    /// path (degraded but still useful when poly book is stale).
    pub poly_mid: Option<f64>,

    /// `(BTC_mid − strike) / (σ × √TTR)`. Black-Scholes Z-score —
    /// formerly the anchor, now a correction feature. Required when
    /// `poly_mid` is absent.
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

    /// Z-score of current YES spread vs its rolling distribution.
    /// Replaces the old `(spread − static_baseline) / static_baseline`
    /// with a self-adapting baseline (rolling median + stdev). Positive
    /// = wider than recent norm = less confidence.
    pub yes_spread_z: Option<f64>,

    /// Lag feature for the YES side: an estimate of "by how much has
    /// Polymarket failed to catch up to recent BTC moves?". Positive →
    /// BTC moved up and Polymarket hasn't responded → YES under-priced.
    pub lag_yes: Option<f64>,

    /// Total Binance trade volume in BTC over the last 60s. Raw value;
    /// scaled by its weight in `RegimeWeights`. Pair with
    /// `binance_flow_imbalance_60s` to give "directional volume".
    pub binance_volume_60s_btc: Option<f64>,

    /// Binance signed trade flow imbalance over the last 60s.
    /// `(buy_volume − sell_volume) / (buy_volume + sell_volume)`,
    /// range `[-1, 1]`. Positive = aggressive buying.
    pub binance_flow_imbalance_60s: Option<f64>,

    /// Momentum: difference between short-window drift (30s) and
    /// long-window drift (300s), both normalised by σ. Captures
    /// "acceleration" — short-term direction relative to longer trend.
    pub btc_momentum: Option<f64>,
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
/// **Default**: only `w_poly_logit = 1.0`. Everything else is 0. That
/// makes `p_yes = poly_mid` exactly — the bot trusts poly's consensus
/// completely. Online learning (or hand-tuning) then adds non-zero
/// weights to features that empirically predict deviations.
///
/// All weights are coefficients in **log-odds space**. A weight of 0.5
/// on a feature that ranges in [-1, 1] adds up to ±0.5 to the logit.
/// At `poly_mid = 0.5` that translates to roughly ±12pp in
/// probability space.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RegimeWeights {
    /// Additive bias before the sigmoid. Use to skew the at-anchor
    /// probability if a regime systematically biases one side.
    /// Default 0.
    pub bias: f64,

    /// Anchor weight: multiplies `logit(poly_mid)`. Default 1.0 —
    /// fully trust poly's mid unless a correction feature says
    /// otherwise. Setting this to 0 disables poly anchoring entirely.
    pub w_poly_logit: f64,

    pub w_btc_strike_distance_z: f64,
    pub w_btc_drift_5s_z: f64,
    pub w_btc_drift_30s_z: f64,
    pub w_btc_drift_60s_z: f64,
    pub w_yes_book_imbalance: f64,
    pub w_no_book_imbalance: f64,
    pub w_yes_spread_z: f64,
    pub w_lag_yes: f64,
    pub w_binance_volume_60s_btc: f64,
    pub w_binance_flow_imbalance_60s: f64,
    pub w_btc_momentum: f64,
}

impl Default for RegimeWeights {
    fn default() -> Self {
        Self {
            bias: 0.0,
            // Anchor on poly mid. All correction features start at 0.
            w_poly_logit: 1.0,
            w_btc_strike_distance_z: 0.0,
            w_btc_drift_5s_z: 0.0,
            w_btc_drift_30s_z: 0.0,
            w_btc_drift_60s_z: 0.0,
            w_yes_book_imbalance: 0.0,
            w_no_book_imbalance: 0.0,
            w_yes_spread_z: 0.0,
            w_lag_yes: 0.0,
            w_binance_volume_60s_btc: 0.0,
            w_binance_flow_imbalance_60s: 0.0,
            w_btc_momentum: 0.0,
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

/// Score the features in log-odds space, anchored on `poly_mid`.
///
/// Requires at least one of `poly_mid` or `btc_strike_distance_z` to
/// be present — without any anchor we have no model. Returns `None`
/// otherwise.
///
/// When `poly_mid` is present, it's the primary anchor (weight 1.0 by
/// default). When absent, the model falls back to BS-only: every
/// feature still contributes through its weight, but the anchor is
/// the BS Z-score's contribution instead of `logit(poly_mid)`. This
/// keeps the bot scoring when poly's book is briefly stale.
///
/// Missing correction features contribute zero (`weight × None` →
/// `weight × 0`). Feature outages degrade gracefully.
pub fn score(features: &Features, regime: Regime, cfg: &ScoringConfig) -> Option<ScoringOutcome> {
    if features.poly_mid.is_none() && features.btc_strike_distance_z.is_none() {
        return None;
    }
    let w = cfg.weights_for(regime);
    // Anchor term: poly_logit (preferred) or BS-only fallback.
    let anchor_contribution = match features.poly_mid {
        Some(p) => w.w_poly_logit * logit(p),
        None => 0.0,
    };
    let raw = w.bias
        + anchor_contribution
        + w.w_btc_strike_distance_z * features.btc_strike_distance_z.unwrap_or(0.0)
        + w.w_btc_drift_5s_z * features.btc_drift_5s_z.unwrap_or(0.0)
        + w.w_btc_drift_30s_z * features.btc_drift_30s_z.unwrap_or(0.0)
        + w.w_btc_drift_60s_z * features.btc_drift_60s_z.unwrap_or(0.0)
        + w.w_yes_book_imbalance * features.yes_book_imbalance.unwrap_or(0.0)
        + w.w_no_book_imbalance * features.no_book_imbalance.unwrap_or(0.0)
        + w.w_yes_spread_z * features.yes_spread_z.unwrap_or(0.0)
        + w.w_lag_yes * features.lag_yes.unwrap_or(0.0)
        + w.w_binance_volume_60s_btc * features.binance_volume_60s_btc.unwrap_or(0.0)
        + w.w_binance_flow_imbalance_60s * features.binance_flow_imbalance_60s.unwrap_or(0.0)
        + w.w_btc_momentum * features.btc_momentum.unwrap_or(0.0);
    let p_yes = sigmoid(raw).clamp(0.0, 1.0);
    Some(ScoringOutcome {
        p_yes,
        p_no: 1.0 - p_yes,
        raw,
    })
}

/// Legacy alias for the previous BS-Φ behaviour, kept only for
/// diagnostic comparison in the decision log (so we can plot
/// `bs_only_p_yes` vs `calibrated_p_yes` and see how often they
/// agree). NOT used for trade decisions.
pub fn score_bs_only(features: &Features) -> Option<f64> {
    let z = features.btc_strike_distance_z?;
    Some(norm_cdf(z).clamp(0.0, 1.0))
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

    fn poly_only(p: f64) -> Features {
        Features {
            poly_mid: Some(p),
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
    fn logit_and_sigmoid_round_trip() {
        for p in [0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99] {
            let back = sigmoid(logit(p));
            assert!(approx(back, p, 1e-12), "round-trip failed at p={p}: got {back}");
        }
    }

    #[test]
    fn sigmoid_handles_extreme_inputs_without_overflow() {
        assert!(approx(sigmoid(-100.0), 0.0, 1e-30));
        assert!(approx(sigmoid(100.0), 1.0, 1e-30));
        // No NaN at boundaries.
        assert!(sigmoid(-1000.0).is_finite());
        assert!(sigmoid(1000.0).is_finite());
    }

    #[test]
    fn default_weights_recover_poly_mid_exactly() {
        // The headline guarantee: with default weights, FV == poly_mid.
        let cfg = ScoringConfig::default();
        for p in [0.10, 0.30, 0.50, 0.70, 0.90] {
            let out = score(&poly_only(p), Regime::Mid, &cfg).expect("scored");
            assert!(
                approx(out.p_yes, p, 1e-9),
                "default scoring should equal poly_mid at p={p}; got {}",
                out.p_yes
            );
        }
    }

    #[test]
    fn missing_both_anchors_returns_none() {
        let cfg = ScoringConfig::default();
        let f = Features::default();
        assert!(score(&f, Regime::Mid, &cfg).is_none());
    }

    #[test]
    fn fallback_to_bs_when_poly_missing() {
        // Without poly_mid the model uses the BS Z-score as anchor.
        // With default weights w_btc_strike_distance_z=0, but raw is
        // computed from the Z-score's correction contribution (0 at
        // default). Need w_btc_strike_distance_z > 0 to trade in this
        // fallback regime.
        let cfg = ScoringConfig::default();
        let out = score(&z_only(1.0), Regime::Mid, &cfg).expect("scored");
        // raw = 0 (anchor missing → 0) + 0×1 = 0 → sigmoid(0) = 0.5
        assert!(approx(out.p_yes, 0.5, 1e-9));
    }

    #[test]
    fn momentum_weight_pushes_above_poly_anchor() {
        let mut cfg = ScoringConfig::default();
        cfg.mid.w_btc_momentum = 0.5;
        let f = Features {
            poly_mid: Some(0.50),
            btc_momentum: Some(1.0), // +1 stdev acceleration
            ..Default::default()
        };
        let out = score(&f, Regime::Mid, &cfg).expect("scored");
        // raw = logit(0.5) + 0.5×1.0 = 0 + 0.5 = 0.5 → sigmoid(0.5) ≈ 0.622
        assert!(
            out.p_yes > 0.60 && out.p_yes < 0.64,
            "got p_yes={}, expected ~0.622",
            out.p_yes
        );
        // FV-poly gap is 0.122 — substantial but bounded.
    }

    #[test]
    fn negative_weight_inverts_contribution() {
        let mut cfg = ScoringConfig::default();
        cfg.late.w_yes_spread_z = -1.0;
        let f = Features {
            poly_mid: Some(0.50),
            yes_spread_z: Some(1.0), // wider than rolling baseline
            ..Default::default()
        };
        let out = score(&f, Regime::Late, &cfg).unwrap();
        // raw = 0 + (-1)*1 = -1 → sigmoid(-1) ≈ 0.269
        assert!(
            out.p_yes < 0.30,
            "wide spread should push below poly anchor; got {}",
            out.p_yes
        );
    }

    #[test]
    fn poly_anchor_dominates_when_corrections_are_small() {
        // Three features each contribute ±0.1 in log-odds — total
        // correction magnitude ≤ 0.3, well below the 10pp gate.
        let mut cfg = ScoringConfig::default();
        cfg.mid.w_btc_momentum = 0.1;
        cfg.mid.w_binance_flow_imbalance_60s = 0.1;
        cfg.mid.w_yes_book_imbalance = 0.1;
        let f = Features {
            poly_mid: Some(0.40),
            btc_momentum: Some(1.0),
            binance_flow_imbalance_60s: Some(1.0),
            yes_book_imbalance: Some(1.0),
            ..Default::default()
        };
        let out = score(&f, Regime::Mid, &cfg).unwrap();
        // raw = logit(0.4) + 0.3 = -0.405 + 0.3 = -0.105 → sigmoid(-0.105) ≈ 0.474
        // FV is just 0.074 above poly anchor — exactly the kind of small correction
        // the framework is designed for.
        let gap = (out.p_yes - 0.40).abs();
        assert!(gap < 0.10, "small corrections should stay within 10pp of poly anchor; got gap={gap}");
    }

    #[test]
    fn bs_only_path_returns_phi_z() {
        // Diagnostic: BS-only prediction should still match Φ(z) for
        // comparison logging.
        for z in [-1.5, -0.5, 0.5, 1.5] {
            let p = score_bs_only(&z_only(z)).unwrap();
            assert!(approx(p, norm_cdf(z), 1e-12));
        }
    }

    #[test]
    fn regime_weights_resolve_independently() {
        let mut cfg = ScoringConfig::default();
        cfg.early.bias = -1.0;
        cfg.late.bias = 1.0;
        let f = poly_only(0.50);
        let early = score(&f, Regime::Early, &cfg).unwrap().p_yes;
        let mid = score(&f, Regime::Mid, &cfg).unwrap().p_yes;
        let late = score(&f, Regime::Late, &cfg).unwrap().p_yes;
        // poly anchor = 0.5 (logit = 0). Bias pushes ±1.
        // early: sigmoid(0 + -1) ≈ 0.269 ; mid: sigmoid(0) = 0.5 ; late: sigmoid(1) ≈ 0.731
        assert!(early < 0.30);
        assert!(approx(mid, 0.5, 1e-9));
        assert!(late > 0.70);
    }

    #[test]
    fn p_yes_and_p_no_sum_to_one() {
        let cfg = ScoringConfig::default();
        for p in [0.05, 0.25, 0.50, 0.75, 0.95] {
            let out = score(&poly_only(p), Regime::Early, &cfg).unwrap();
            assert!(approx(out.p_yes + out.p_no, 1.0, 1e-12));
        }
    }

    #[test]
    fn poly_mid_near_zero_or_one_does_not_blow_up() {
        let cfg = ScoringConfig::default();
        // logit(0.001) ≈ -6.9; logit(0.999) ≈ 6.9. Sigmoid of those
        // returns ~0.001 / ~0.999 — no NaN, no infinity.
        let very_low = score(&poly_only(0.001), Regime::Late, &cfg).unwrap();
        assert!(very_low.p_yes < 0.01);
        let very_high = score(&poly_only(0.999), Regime::Late, &cfg).unwrap();
        assert!(very_high.p_yes > 0.99);
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
        assert!(approx(parsed.early.w_poly_logit, 1.0, 1e-9));
    }
}
