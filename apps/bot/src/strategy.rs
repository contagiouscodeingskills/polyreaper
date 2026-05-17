//! Strategy v0: edge = fair_value − polymarket_mid; size scales with edge.

use market_registry::{MarketId, Outcome};
use serde::{Deserialize, Serialize};

use crate::config::StrategyConfig;
use crate::fv::FairValue;

/// Which side of the binary market a signal wants. Aliased so strategy
/// code reads naturally; the underlying type is `market_registry::Outcome`.
pub type Side = Outcome;

/// A trade intent emitted by the strategy. Orders haven't been placed yet
/// — risk gating + execution still need to consume this.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Signal {
    pub market_id: MarketId,
    pub side: Outcome,
    /// Target size in USD (max amount we're willing to spend on this fill).
    pub size_usd: f64,
    /// Target limit price in probability units (0..1) — the price of one
    /// share of `side`. We aim to take liquidity at or better than this.
    pub price: f64,
    /// Fair value of `side` we computed. For audit / logging.
    pub fv_for_side: f64,
    /// Polymarket mid for `side` at decision time.
    pub mid_for_side: f64,
    /// Signed edge for `side` = `fv_for_side - mid_for_side`. Positive
    /// means we think the side is undervalued.
    pub edge: f64,
    /// Time-to-resolution at decision time, seconds.
    pub ttr_secs: f64,
}

/// Inputs to one decision tick. Caller assembles this per (market, snapshot).
#[derive(Debug, Clone)]
pub struct DecisionInputs<'a> {
    pub market_id: &'a MarketId,
    pub fair_value: FairValue,
    /// Polymarket YES-side mid in [0,1].
    pub poly_yes_mid: f64,
    /// Seconds until market resolution.
    pub ttr_secs: f64,
    /// Max USD we'd spend if conviction is full.
    pub max_per_trade_usd: f64,
}

/// Pure function: given the inputs and config, optionally emit a Signal.
pub fn decide(inputs: DecisionInputs<'_>, cfg: &StrategyConfig) -> Option<Signal> {
    // Time gate.
    if !inputs.ttr_secs.is_finite() || inputs.ttr_secs < cfg.min_ttr_secs {
        return None;
    }
    // Sanity-bound the Polymarket mid.
    if !(0.0..=1.0).contains(&inputs.poly_yes_mid) {
        return None;
    }
    // YES-side edge. Symmetric — we trade YES if FV>mid, NO if FV<mid.
    let yes_edge = inputs.fair_value.p_yes - inputs.poly_yes_mid;
    let abs_edge = yes_edge.abs();
    if abs_edge < cfg.min_edge {
        return None;
    }
    // EV-scaled sizing.
    let scale = ((abs_edge - cfg.min_edge) / (cfg.edge_scale - cfg.min_edge))
        .clamp(0.0, 1.0);
    let size_usd = inputs.max_per_trade_usd * scale;
    if size_usd <= 0.0 {
        return None;
    }
    // Side, target price.
    let (side, fv_for_side, mid_for_side, edge) = if yes_edge > 0.0 {
        (
            Outcome::Yes,
            inputs.fair_value.p_yes,
            inputs.poly_yes_mid,
            yes_edge,
        )
    } else {
        (
            Outcome::No,
            inputs.fair_value.p_no,
            1.0 - inputs.poly_yes_mid,
            -yes_edge,
        )
    };
    Some(Signal {
        market_id: inputs.market_id.clone(),
        side,
        size_usd,
        price: mid_for_side,
        fv_for_side,
        mid_for_side,
        edge,
        ttr_secs: inputs.ttr_secs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StrategyConfig;
    use crate::fv::FairValue;

    fn ctx() -> (MarketId, StrategyConfig) {
        (MarketId::new("M"), StrategyConfig::default())
    }

    #[test]
    fn no_signal_below_min_edge() {
        let (m, cfg) = ctx();
        let sig = decide(
            DecisionInputs {
                market_id: &m,
                fair_value: FairValue::from_p_yes(0.51),
                poly_yes_mid: 0.50,
                ttr_secs: 120.0,
                max_per_trade_usd: 1.0,
            },
            &cfg,
        );
        assert!(sig.is_none(), "0.01 edge < min_edge=0.02 should not fire");
    }

    #[test]
    fn no_signal_in_freeze_window() {
        let (m, cfg) = ctx();
        let sig = decide(
            DecisionInputs {
                market_id: &m,
                fair_value: FairValue::from_p_yes(0.70),
                poly_yes_mid: 0.50,
                ttr_secs: 5.0,
                max_per_trade_usd: 1.0,
            },
            &cfg,
        );
        assert!(sig.is_none(), "ttr below min_ttr should not fire");
    }

    #[test]
    fn fires_yes_when_fv_above_mid() {
        let (m, cfg) = ctx();
        let sig = decide(
            DecisionInputs {
                market_id: &m,
                fair_value: FairValue::from_p_yes(0.60),
                poly_yes_mid: 0.50,
                ttr_secs: 120.0,
                max_per_trade_usd: 1.0,
            },
            &cfg,
        )
        .expect("should fire");
        assert_eq!(sig.side, Outcome::Yes);
        assert!(sig.size_usd > 0.0 && sig.size_usd <= 1.0);
        assert!((sig.edge - 0.10).abs() < 1e-9);
    }

    #[test]
    fn fires_no_when_fv_below_mid() {
        let (m, cfg) = ctx();
        let sig = decide(
            DecisionInputs {
                market_id: &m,
                fair_value: FairValue::from_p_yes(0.40),
                poly_yes_mid: 0.50,
                ttr_secs: 120.0,
                max_per_trade_usd: 1.0,
            },
            &cfg,
        )
        .expect("should fire");
        assert_eq!(sig.side, Outcome::No);
        // NO mid = 1 - 0.5 = 0.5; NO fv = 1 - 0.4 = 0.6; edge = 0.1.
        assert!((sig.edge - 0.10).abs() < 1e-9);
        assert!((sig.fv_for_side - 0.60).abs() < 1e-9);
        assert!((sig.mid_for_side - 0.50).abs() < 1e-9);
    }

    #[test]
    fn size_scales_with_edge() {
        let (m, cfg) = ctx();
        let s_small = decide(
            DecisionInputs {
                market_id: &m,
                fair_value: FairValue::from_p_yes(0.53),
                poly_yes_mid: 0.50,
                ttr_secs: 120.0,
                max_per_trade_usd: 1.0,
            },
            &cfg,
        )
        .unwrap();
        let s_big = decide(
            DecisionInputs {
                market_id: &m,
                fair_value: FairValue::from_p_yes(0.60),
                poly_yes_mid: 0.50,
                ttr_secs: 120.0,
                max_per_trade_usd: 1.0,
            },
            &cfg,
        )
        .unwrap();
        assert!(s_big.size_usd > s_small.size_usd);
        assert!(s_big.size_usd <= 1.0);
    }

    #[test]
    fn size_caps_at_full_at_or_above_edge_scale() {
        let (m, cfg) = ctx();
        let sig = decide(
            DecisionInputs {
                market_id: &m,
                fair_value: FairValue::from_p_yes(0.20),
                poly_yes_mid: 0.50,
                ttr_secs: 120.0,
                max_per_trade_usd: 1.0,
            },
            &cfg,
        )
        .unwrap();
        // |edge| = 0.30 > edge_scale=0.08 → max size
        assert!((sig.size_usd - 1.0).abs() < 1e-9);
    }
}
