//! Multi-factor hand-coded FV model derived from BTC microstructure.
//!
//! ## Framing
//!
//! The bot computes its own `p_yes(BTC_state)` from first principles —
//! Black-Scholes baseline plus microstructure corrections. Polymarket's
//! mid is **not** an input to this model. Instead, the bot's output
//! is compared to poly mid externally:
//!   * If they're close, the model is calibrated and the small gap
//!     (after fees) is where edge lives.
//!   * If the gap is huge (>10pp configurable), the model is probably
//!     broken — refuse to fire via the `ModelDivergence` DQ gate.
//!
//! ```text
//! raw = bias
//!     + w_strike_z    × z_BS                   ← Black-Scholes anchor
//!     + w_drift_30s   × btc_drift_30s_z        ← recent directionality
//!     + w_momentum    × btc_momentum           ← acceleration
//!     + w_flow_60s    × binance_flow_imbalance ← order-flow bias
//!     + w_book_imbal  × yes_book_imbalance     ← poly's own book pressure
//!     + …
//! p_yes = sigmoid(raw)
//! ```
//!
//! Note `yes_book_imbalance` is computed FROM poly's book sizes but
//! doesn't reference poly's mid; it captures directional pressure
//! independently of the price level. Lag-style "BTC moved but poly
//! hasn't" comes for free: when BTC moves, the BS Z-score reacts
//! immediately while poly mid lags — so our fv changes, the gap to
//! poly opens, and the strategy fires.
//!
//! ## Why log-odds (sigmoid), not Φ(z)?
//!
//! Earlier versions used `p_yes = Φ(weighted_z_scores)` — the
//! Black-Scholes binary call price for the dominant Z-score term.
//! Sigmoid + log-odds coefficients lets us combine signals from
//! different units (Z-scores, ratios, signed values) on a common
//! scale, with weights interpretable as "log-odds bump per unit
//! feature". The default `w_strike_z = 1.0` reproduces a model very
//! close to `Φ(z)` for moderate `|z|` and avoids the numerical edge
//! cases at the tails.
//!
//! ## Starting weights
//!
//! The defaults below are **plausible starting points**, not
//! calibrated. They reflect prior beliefs about which signals matter
//! at 5-minute horizons:
//!   * Z_BS is the dominant feature (weight 1.0).
//!   * Recent drift and momentum carry directional information
//!     beyond the static BS estimate.
//!   * Binance trade-flow imbalance is the cleanest order-flow signal.
//!   * Polymarket book imbalance captures informed positioning.
//!
//! All weights are tunable in `configs/bot.toml`. Online learning
//! from logged (features, outcome) pairs is future work.
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
    /// `(BTC_mid − strike) / (σ × √TTR)`. Black-Scholes Z-score —
    /// the anchor of the model. Absence → `score` returns `None`.
    /// Polymarket's mid is intentionally NOT a feature here; we
    /// derive our own probability from BTC fundamentals and validate
    /// against poly externally.
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

/// Weights for one regime. Field naming is `w_<feature>`, matching
/// the field on [`Features`] exactly so tuning is mechanical.
///
/// **Defaults are plausible starting values, not calibrated.** The
/// model needs to produce a meaningful probability out-of-the-box,
/// so every feature has a non-zero starting weight reflecting our
/// prior beliefs about which signals matter at 5-min horizons. The
/// model-divergence DQ gate prevents extreme outputs from these
/// starting weights from being acted on if they turn out to be wrong.
///
/// All weights are coefficients in **log-odds space**. A weight of
/// 0.5 on a feature in [-1, 1] adds ±0.5 to the logit, which is
/// ~±12pp in probability space near 0.5.
///
/// `Early`/`Mid`/`Late` regimes get separate weight blocks so
/// different microstructure effects can dominate at each TTR phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RegimeWeights {
    /// Additive bias before the sigmoid. Use to skew the at-Z=0
    /// probability if a regime systematically biases one side.
    pub bias: f64,

    /// Black-Scholes Z-score: `ln(S/K) / (σ√T)`. The anchor of the
    /// model — captures the fundamental "how close is BTC to strike
    /// vs how much can it move before resolution" probability.
    pub w_btc_strike_distance_z: f64,

    /// Recent BTC drift, z-scored over its respective window.
    /// Positive = BTC moved up recently → bias toward YES.
    pub w_btc_drift_5s_z: f64,
    pub w_btc_drift_30s_z: f64,
    pub w_btc_drift_60s_z: f64,

    /// Polymarket YES-side book imbalance:
    /// `(bid_size - ask_size) / (bid_size + ask_size)` ∈ [-1, 1].
    /// Positive = informed bid pressure → YES likely under-priced.
    pub w_yes_book_imbalance: f64,
    pub w_no_book_imbalance: f64,

    /// Z-score of YES spread vs rolling distribution. Wide spread =
    /// less confidence in current price → less informative signal.
    pub w_yes_spread_z: f64,

    /// Lag feature (currently unpopulated). Future signal addition.
    pub w_lag_yes: f64,

    /// Total Binance trade volume in 60s. Raw value; correlates with
    /// volatility-of-volatility — high volume often precedes bigger
    /// BTC moves.
    pub w_binance_volume_60s_btc: f64,

    /// Signed Binance trade flow imbalance ∈ [-1, 1]. Positive =
    /// aggressive buying on Binance → BTC tends to continue up at
    /// 5-min horizon.
    pub w_binance_flow_imbalance_60s: f64,

    /// Momentum: short-window drift minus long-window drift.
    /// Captures acceleration / trend continuation.
    pub w_btc_momentum: f64,
}

impl Default for RegimeWeights {
    fn default() -> Self {
        // Starting weights. Tuned from the offline 5-min simulation
        // in `src/bin/sim_5min.rs` — earlier values caused the model
        // to disagree with poly by >10pp on ~30% of ticks, well above
        // the divergence gate's tolerance.
        //
        // Drift / flow z-scores can swing ±2σ on noise alone over
        // 30s; multiplying that by 0.30+ produces large probability
        // jumps that aren't supported by the underlying signal.
        // These smaller weights still let those features contribute
        // when they ARE informative (drift +3σ on a real move still
        // adds meaningful log-odds), without amplifying noise.
        //
        // Tune in configs/bot.toml or via future online learning.
        Self {
            bias: 0.0,
            // BS Z-score is the dominant feature — full weight.
            w_btc_strike_distance_z: 1.0,
            // Drift carries directional information beyond static BS,
            // but its z-scores are noisy. Modest weight.
            w_btc_drift_5s_z: 0.0,
            w_btc_drift_30s_z: 0.15,
            w_btc_drift_60s_z: 0.0,
            // Book imbalance — small positive weight; both sides
            // included so net pressure compounds.
            w_yes_book_imbalance: 0.10,
            w_no_book_imbalance: -0.10,
            // Wide spread → less reliable signal. Weight 0 by
            // default — only useful as a confidence dampener once
            // calibrated.
            w_yes_spread_z: 0.0,
            // Lag feature not yet computed.
            w_lag_yes: 0.0,
            // Volume — weak directional signal on its own.
            w_binance_volume_60s_btc: 0.0,
            // Flow imbalance is informative but already saturates
            // near ±1; smaller weight than drift since the magnitude
            // doesn't add additional info.
            w_binance_flow_imbalance_60s: 0.20,
            // Momentum / acceleration. Smallest weight of the
            // microstructure signals — noisiest.
            w_btc_momentum: 0.10,
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

/// Score the features in log-odds space. Computes `p_yes` from BTC
/// microstructure only — `poly_mid` is intentionally not an input.
///
/// Requires `btc_strike_distance_z` (the BS anchor). Returns `None`
/// without it. Other features contribute zero when missing
/// (`weight × None` → `weight × 0`) so feature outages degrade
/// gracefully to the simpler subset of the model.
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
    fn missing_z_anchor_returns_none() {
        let cfg = ScoringConfig::default();
        let f = Features::default();
        assert!(
            score(&f, Regime::Mid, &cfg).is_none(),
            "no BS Z-score = no model"
        );
    }

    #[test]
    fn default_weights_at_z_zero_returns_half() {
        // With Z=0 (BTC exactly at strike) and no other feature inputs,
        // we expect a coin-flip — even though correction weights are
        // non-zero, the features they multiply are all 0/None.
        let cfg = ScoringConfig::default();
        let out = score(&z_only(0.0), Regime::Mid, &cfg).expect("scored");
        assert!(approx(out.p_yes, 0.5, 1e-12), "got {}", out.p_yes);
        assert!(approx(out.raw, 0.0, 1e-12));
    }

    #[test]
    fn z_score_alone_dominates_p_yes() {
        // With Z=+2 (BTC well above strike) and all correction features
        // absent, p_yes should be high (close to but not exactly Φ(2)
        // since we use sigmoid not Φ).
        let cfg = ScoringConfig::default();
        let out = score(&z_only(2.0), Regime::Mid, &cfg).expect("scored");
        // sigmoid(2.0) ≈ 0.881
        assert!(out.p_yes > 0.85 && out.p_yes < 0.90, "got {}", out.p_yes);
        // Symmetric: Z=-2 gives p_yes ≈ 0.119.
        let out = score(&z_only(-2.0), Regime::Mid, &cfg).expect("scored");
        assert!(out.p_yes > 0.10 && out.p_yes < 0.15, "got {}", out.p_yes);
    }

    #[test]
    fn drift_30s_correction_shifts_p_yes_above_baseline() {
        // Default drift_30s weight is 0.15; +1σ drift adds 0.15 to log-odds.
        let cfg = ScoringConfig::default();
        let f = Features {
            btc_strike_distance_z: Some(0.0),
            btc_drift_30s_z: Some(1.0),
            ..Default::default()
        };
        let out = score(&f, Regime::Mid, &cfg).expect("scored");
        // raw = 0 + 0.15×1 = 0.15 → sigmoid(0.15) ≈ 0.537
        assert!(out.p_yes > 0.53 && out.p_yes < 0.55, "got {}", out.p_yes);
    }

    #[test]
    fn flow_imbalance_pushes_p_yes_directionally() {
        let cfg = ScoringConfig::default(); // w_binance_flow_imbalance_60s = 0.20
        // Heavy aggressive buying on Binance with Z=0 (at strike).
        let f = Features {
            btc_strike_distance_z: Some(0.0),
            binance_flow_imbalance_60s: Some(1.0), // 100% buyer-aggressor
            ..Default::default()
        };
        let out = score(&f, Regime::Mid, &cfg).expect("scored");
        // raw = 0 + 0.20×1 = 0.20 → sigmoid(0.20) ≈ 0.550
        assert!(out.p_yes > 0.54 && out.p_yes < 0.56, "got {}", out.p_yes);
        // Reverse: heavy selling pushes p_yes down.
        let f = Features {
            btc_strike_distance_z: Some(0.0),
            binance_flow_imbalance_60s: Some(-1.0),
            ..Default::default()
        };
        let out = score(&f, Regime::Mid, &cfg).expect("scored");
        assert!(out.p_yes < 0.46, "got {}", out.p_yes);
    }

    #[test]
    fn book_imbalance_yes_lifts_no_imbalance_drops() {
        // Default weights: w_yes_book_imbalance=+0.10, w_no_book_imbalance=-0.10.
        // YES bid-heavy AND NO ask-heavy means both signals say "YES winning".
        let cfg = ScoringConfig::default();
        let f = Features {
            btc_strike_distance_z: Some(0.0),
            yes_book_imbalance: Some(0.5),  // bid > ask on YES
            no_book_imbalance: Some(-0.5),  // ask > bid on NO
            ..Default::default()
        };
        let out = score(&f, Regime::Mid, &cfg).expect("scored");
        // raw = 0 + 0.10×0.5 + (-0.10)×(-0.5) = 0.05 + 0.05 = 0.10
        // sigmoid(0.10) ≈ 0.525
        assert!(out.p_yes > 0.51 && out.p_yes < 0.54, "got {}", out.p_yes);
    }

    #[test]
    fn combined_features_compound_log_odds() {
        let cfg = ScoringConfig::default();
        let f = Features {
            btc_strike_distance_z: Some(0.5),       // BS lift
            btc_drift_30s_z: Some(1.0),             // drift lift
            binance_flow_imbalance_60s: Some(0.5),  // flow lift
            btc_momentum: Some(0.5),                // momentum lift
            ..Default::default()
        };
        let out = score(&f, Regime::Mid, &cfg).expect("scored");
        // raw = 1.0×0.5 + 0.15×1.0 + 0.20×0.5 + 0.10×0.5
        //     = 0.5 + 0.15 + 0.10 + 0.05 = 0.80
        assert!(approx(out.raw, 0.80, 1e-9));
        // sigmoid(0.80) ≈ 0.690
        assert!(out.p_yes > 0.67 && out.p_yes < 0.71, "got {}", out.p_yes);
    }

    #[test]
    fn bs_only_path_returns_phi_z() {
        // Diagnostic: BS-only prediction should still match Φ(z) for
        // comparison logging vs the calibrated model.
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
        let f = z_only(0.0);
        let early = score(&f, Regime::Early, &cfg).unwrap().p_yes;
        let mid = score(&f, Regime::Mid, &cfg).unwrap().p_yes;
        let late = score(&f, Regime::Late, &cfg).unwrap().p_yes;
        assert!(early < 0.30);            // sigmoid(-1) ≈ 0.269
        assert!(approx(mid, 0.5, 1e-9));  // sigmoid(0) = 0.5
        assert!(late > 0.70);             // sigmoid(+1) ≈ 0.731
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
    fn extreme_z_does_not_blow_up() {
        let cfg = ScoringConfig::default();
        let very_low = score(&z_only(-50.0), Regime::Late, &cfg).unwrap();
        assert!(very_low.p_yes < 1e-9 && very_low.p_yes >= 0.0);
        let very_high = score(&z_only(50.0), Regime::Late, &cfg).unwrap();
        assert!(very_high.p_yes > 1.0 - 1e-9 && very_high.p_yes <= 1.0);
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
