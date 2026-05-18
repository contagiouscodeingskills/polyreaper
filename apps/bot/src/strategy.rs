//! Strategy v1: scoring-driven taker with strict fee-aware edge gate.
//!
//! Takes a [`ScoringOutcome`] from `signals::scoring` plus the
//! Polymarket top-of-book on both YES and NO sides, picks the best
//! buy-side trade (highest expected-value after fees), gates it against
//! the Polymarket fee curve + safety margin, and emits a `Signal`
//! sized linearly with how much the edge exceeds the gate.
//!
//! Fee formula (Polymarket crypto markets, verified May 2026):
//!   `fee_as_fraction_of_notional = taker_fee_rate × p × (1 − p)`
//! Required edge (probability units) to break even taking at price `p`:
//!   `required_edge = taker_fee_rate × p² × (1 − p) + safety_margin`
//! See `docs/BOT_ARCHITECTURE_AND_BUILD_PLAN.md` §4 and the fee math
//! in the changelog commits for the derivation.

use market_registry::{MarketId, Outcome};
use serde::{Deserialize, Serialize};

use crate::config::StrategyConfig;
use crate::signals::scoring::ScoringOutcome;

/// A trade intent emitted by the strategy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Signal {
    pub market_id: MarketId,
    pub side: Outcome,
    /// Target USD size for this fill.
    pub size_usd: f64,
    /// Price we'd actually fill at — the relevant side's ask.
    pub price: f64,
    /// Fair-value for the chosen side at decision time.
    pub fv_for_side: f64,
    /// The Polymarket mid for the chosen side (diagnostic; we DON'T
    /// trade against mid — we trade against ask).
    pub mid_for_side: f64,
    /// Signed edge for the chosen side at the ask, after the fee gate.
    pub edge: f64,
    pub ttr_secs: f64,
}

/// Inputs to one decision tick.
#[derive(Debug, Clone)]
pub struct DecisionInputs<'a> {
    pub market_id: &'a MarketId,
    pub scoring_outcome: ScoringOutcome,
    pub poly_yes_bid: Option<f64>,
    pub poly_yes_ask: Option<f64>,
    pub poly_no_bid: Option<f64>,
    pub poly_no_ask: Option<f64>,
    pub ttr_secs: f64,
    /// Hard ceiling on a single trade's size.
    pub max_per_trade_usd: f64,
    /// Current bankroll for edge-scaled sizing. Live trades scale to
    /// `bankroll × edge × StrategyConfig::bankroll_pct_per_edge`.
    pub bankroll_usd: f64,
}

/// Outcome of one strategy tick.
#[derive(Debug, Clone, PartialEq)]
pub enum StrategyOutcome {
    Fire(Signal),
    NoSignal(NoSignalReason),
}

/// Why the strategy chose not to fire. Serialised for the decision log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoSignalReason {
    /// `ttr_secs` below configured floor.
    TtrBelowMin,
    /// Missing YES or NO ask price.
    PolyAskMissing,
    /// Ask price outside `(0, 1)` — venue glitch.
    PolyAskOutOfRange,
    /// Edge on best side didn't clear the fee + safety gate.
    EdgeBelowGate,
    /// Sized to non-positive — defensive guard.
    SizeNonPositive,
}

/// One side's candidate buy: ask price + edge after subtracting ask.
struct SideCandidate {
    side: Outcome,
    ask: f64,
    fv: f64,
    edge_after_ask: f64,
}

/// Required edge (probability units) to break even taking at price `p`
/// given the Polymarket crypto fee curve. Plus safety margin.
fn required_edge(p: f64, cfg: &StrategyConfig) -> f64 {
    cfg.taker_fee_rate * p * p * (1.0 - p) + cfg.taker_safety_margin
}

/// Pure function: from scoring outcome + book + config, emit a fire or
/// a labeled no-signal.
pub fn decide(inputs: DecisionInputs<'_>, cfg: &StrategyConfig) -> StrategyOutcome {
    if !inputs.ttr_secs.is_finite() || inputs.ttr_secs < cfg.min_ttr_secs {
        return StrategyOutcome::NoSignal(NoSignalReason::TtrBelowMin);
    }

    // Build both candidates. Missing asks → side is ineligible.
    let yes_cand = match inputs.poly_yes_ask {
        Some(a) if (0.0..=1.0).contains(&a) => Some(SideCandidate {
            side: Outcome::Yes,
            ask: a,
            fv: inputs.scoring_outcome.p_yes,
            edge_after_ask: inputs.scoring_outcome.p_yes - a,
        }),
        Some(_) => return StrategyOutcome::NoSignal(NoSignalReason::PolyAskOutOfRange),
        None => None,
    };
    let no_cand = match inputs.poly_no_ask {
        Some(a) if (0.0..=1.0).contains(&a) => Some(SideCandidate {
            side: Outcome::No,
            ask: a,
            fv: inputs.scoring_outcome.p_no,
            edge_after_ask: inputs.scoring_outcome.p_no - a,
        }),
        Some(_) => return StrategyOutcome::NoSignal(NoSignalReason::PolyAskOutOfRange),
        None => None,
    };

    let candidates: Vec<SideCandidate> = [yes_cand, no_cand].into_iter().flatten().collect();
    if candidates.is_empty() {
        return StrategyOutcome::NoSignal(NoSignalReason::PolyAskMissing);
    }

    // Best side = largest edge after ask. Fire only if it clears the gate.
    let best = candidates
        .into_iter()
        .max_by(|a, b| {
            a.edge_after_ask
                .partial_cmp(&b.edge_after_ask)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .expect("non-empty");

    let required = required_edge(best.ask, cfg);
    if best.edge_after_ask <= required {
        return StrategyOutcome::NoSignal(NoSignalReason::EdgeBelowGate);
    }

    // Bankroll-fraction sizing:
    //   size = bankroll × edge × bankroll_pct_per_edge
    // Capped by max_per_trade. Uses the FULL edge (not excess over gate)
    // so sizing scales smoothly with conviction. Below the gate we don't
    // fire at all (above check); above the gate, size scales with edge.
    let raw_size_usd =
        inputs.bankroll_usd.max(0.0) * best.edge_after_ask * cfg.bankroll_pct_per_edge.max(0.0);
    let size_usd = raw_size_usd.min(inputs.max_per_trade_usd).max(0.0);
    if size_usd <= 0.0 {
        return StrategyOutcome::NoSignal(NoSignalReason::SizeNonPositive);
    }

    let bid_for_side = match best.side {
        Outcome::Yes => inputs.poly_yes_bid,
        Outcome::No => inputs.poly_no_bid,
    };
    let mid_for_side = match (bid_for_side, Some(best.ask)) {
        (Some(b), Some(a)) if a > b => 0.5 * (a + b),
        _ => best.ask,
    };

    StrategyOutcome::Fire(Signal {
        market_id: inputs.market_id.clone(),
        side: best.side,
        size_usd,
        price: best.ask,
        fv_for_side: best.fv,
        mid_for_side,
        edge: best.edge_after_ask,
        ttr_secs: inputs.ttr_secs,
    })
}

/// Diagnostic: what's the required edge at this price, given config?
/// Exposed so the decision log can record what the gate was for each tick.
pub fn taker_required_edge(price: f64, cfg: &StrategyConfig) -> f64 {
    required_edge(price, cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signals::scoring::ScoringOutcome;

    fn cfg() -> StrategyConfig {
        StrategyConfig::default()
    }

    fn scoring(p_yes: f64) -> ScoringOutcome {
        ScoringOutcome {
            p_yes,
            p_no: 1.0 - p_yes,
            raw: 0.0,
        }
    }

    fn inputs<'a>(
        market: &'a MarketId,
        p_yes: f64,
        yes_ask: f64,
        no_ask: f64,
    ) -> DecisionInputs<'a> {
        DecisionInputs {
            market_id: market,
            scoring_outcome: scoring(p_yes),
            poly_yes_bid: Some(yes_ask - 0.01),
            poly_yes_ask: Some(yes_ask),
            poly_no_bid: Some(no_ask - 0.01),
            poly_no_ask: Some(no_ask),
            ttr_secs: 120.0,
            max_per_trade_usd: 1.0,
            bankroll_usd: 1000.0,
        }
    }

    fn expect_fire(o: StrategyOutcome) -> Signal {
        match o {
            StrategyOutcome::Fire(s) => s,
            StrategyOutcome::NoSignal(r) => panic!("expected fire, got NoSignal({r:?})"),
        }
    }
    fn expect_no(o: StrategyOutcome) -> NoSignalReason {
        match o {
            StrategyOutcome::NoSignal(r) => r,
            StrategyOutcome::Fire(s) => panic!("expected NoSignal, got Fire({s:?})"),
        }
    }

    #[test]
    fn ttr_below_min_blocks() {
        let m = MarketId::new("M");
        let mut i = inputs(&m, 0.6, 0.5, 0.5);
        i.ttr_secs = 5.0;
        assert_eq!(expect_no(decide(i, &cfg())), NoSignalReason::TtrBelowMin);
    }

    #[test]
    fn at_strike_with_small_edge_does_not_fire() {
        // scoring p_yes = 0.50 + 0.5%; yes_ask = 0.50. Edge = 0.005.
        // Required at p=0.5: 0.072 × 0.25 × 0.5 + 0.005 = 0.014. Below gate.
        let m = MarketId::new("M");
        let out = decide(inputs(&m, 0.505, 0.50, 0.50), &cfg());
        assert_eq!(expect_no(out), NoSignalReason::EdgeBelowGate);
    }

    #[test]
    fn fires_yes_when_edge_clears_gate() {
        // p_yes = 0.58, yes_ask = 0.50 → edge = 0.08 > 0.014 required.
        let m = MarketId::new("M");
        let sig = expect_fire(decide(inputs(&m, 0.58, 0.50, 0.50), &cfg()));
        assert_eq!(sig.side, Outcome::Yes);
        assert!((sig.price - 0.50).abs() < 1e-9);
        assert!(sig.size_usd > 0.0 && sig.size_usd <= 1.0);
    }

    #[test]
    fn fires_no_when_no_edge_clears_gate() {
        // p_yes = 0.20 → p_no = 0.80; no_ask = 0.55. NO edge = 0.25.
        let m = MarketId::new("M");
        let sig = expect_fire(decide(inputs(&m, 0.20, 0.95, 0.55), &cfg()));
        assert_eq!(sig.side, Outcome::No);
        assert!((sig.price - 0.55).abs() < 1e-9);
    }

    #[test]
    fn picks_higher_edge_side_when_both_clear() {
        // p_yes = 0.62, yes_ask = 0.40 → yes_edge = 0.22.
        // p_no = 0.38, no_ask = 0.30 → no_edge = 0.08.
        // YES wins.
        let m = MarketId::new("M");
        let sig = expect_fire(decide(inputs(&m, 0.62, 0.40, 0.30), &cfg()));
        assert_eq!(sig.side, Outcome::Yes);
    }

    #[test]
    fn required_edge_matches_break_even_math() {
        // required = taker_fee_rate × p² × (1 − p) + safety_margin.
        // (Derivation: buying YES at p has 1/p shares per $; fee per $
        // = fee_rate × p × (1-p); required true edge to break even =
        // fee_rate × p² × (1-p).)
        let c = cfg();
        let expected = |p: f64| c.taker_fee_rate * p * p * (1.0 - p) + c.taker_safety_margin;
        for p in [0.10, 0.30, 0.50, 0.67, 0.90] {
            assert!(
                (taker_required_edge(p, &c) - expected(p)).abs() < 1e-12,
                "p={p}"
            );
        }
    }

    #[test]
    fn required_edge_peaks_near_two_thirds() {
        // p²(1-p) is maximised at p = 2/3 (d/dp = 2p − 3p² = 0). The
        // asymmetry is real: expensive YES costs more in absolute edge
        // because of leverage, not because the venue is asymmetric.
        let c = cfg();
        let at_half = taker_required_edge(0.50, &c);
        let at_two_thirds = taker_required_edge(2.0 / 3.0, &c);
        let at_low = taker_required_edge(0.10, &c);
        let at_high = taker_required_edge(0.90, &c);
        assert!(at_two_thirds > at_half);
        assert!(at_two_thirds > at_high);
        assert!(at_half > at_low);
        // The asymmetry is the point: at_high should be GREATER than at_low.
        assert!(at_high > at_low);
    }

    #[test]
    fn missing_ask_rejects_with_labeled_reason() {
        let m = MarketId::new("M");
        let i = DecisionInputs {
            market_id: &m,
            scoring_outcome: scoring(0.7),
            poly_yes_bid: None,
            poly_yes_ask: None,
            poly_no_bid: None,
            poly_no_ask: None,
            ttr_secs: 120.0,
            max_per_trade_usd: 1.0,
            bankroll_usd: 1000.0,
        };
        assert_eq!(expect_no(decide(i, &cfg())), NoSignalReason::PolyAskMissing);
    }

    #[test]
    fn bankroll_sizing_matches_user_spec() {
        // User spec: 5% edge → 0.1% of bankroll, 10% edge → 0.2%.
        // Formula: size = bankroll × edge × bankroll_pct_per_edge.
        // With defaults (bankroll=$1000, pct=0.02):
        //   edge=0.05 → 0.05 × 0.02 = 0.001 of bankroll = $1
        //   edge=0.10 → 0.10 × 0.02 = 0.002 of bankroll = $2
        let m = MarketId::new("M");
        let c = cfg();
        // Construct a 0.10 edge YES: scoring p_yes = 0.50, yes_ask = 0.40
        // (edge_after_ask = 0.10; required at 0.40 ≈ 0.011; passes gate).
        let mut i = inputs(&m, 0.50, 0.40, 0.99);
        i.bankroll_usd = 1000.0;
        i.max_per_trade_usd = 10.0; // raise so bankroll math isn't capped
        let sig = expect_fire(decide(i, &c));
        // size = 1000 × 0.10 × 0.02 = $2.00
        assert!((sig.size_usd - 2.0).abs() < 1e-6, "got {}", sig.size_usd);
    }

    #[test]
    fn bankroll_sizing_capped_by_max_per_trade() {
        let m = MarketId::new("M");
        let c = cfg();
        let mut i = inputs(&m, 0.20, 0.99, 0.20); // huge NO edge ~0.60
        i.bankroll_usd = 100_000.0;
        i.max_per_trade_usd = 5.0;
        let sig = expect_fire(decide(i, &c));
        // Without cap: 100k × 0.60 × 0.02 = $1200; capped at $5
        assert!((sig.size_usd - 5.0).abs() < 1e-6);
    }

    #[test]
    fn bankroll_sizing_scales_linearly_with_bankroll() {
        let m = MarketId::new("M");
        let c = cfg();
        let mut i_small = inputs(&m, 0.50, 0.40, 0.99);
        i_small.bankroll_usd = 500.0;
        i_small.max_per_trade_usd = 50.0;
        let s_small = expect_fire(decide(i_small.clone(), &c));
        let mut i_big = inputs(&m, 0.50, 0.40, 0.99);
        i_big.bankroll_usd = 2000.0;
        i_big.max_per_trade_usd = 50.0;
        let s_big = expect_fire(decide(i_big, &c));
        // 4× the bankroll → 4× the size (until cap).
        assert!((s_big.size_usd / s_small.size_usd - 4.0).abs() < 1e-6);
    }

    #[test]
    fn ask_out_of_range_rejected() {
        let m = MarketId::new("M");
        let mut i = inputs(&m, 0.7, 0.5, 0.5);
        i.poly_yes_ask = Some(1.5);
        assert_eq!(
            expect_no(decide(i, &cfg())),
            NoSignalReason::PolyAskOutOfRange
        );
    }

    #[test]
    fn size_scales_with_edge() {
        let m = MarketId::new("M");
        let c = cfg();
        // Small edge above gate → small size.
        let small = expect_fire(decide(inputs(&m, 0.516, 0.50, 0.50), &c));
        // Large edge above gate → larger size, capped at max.
        let large = expect_fire(decide(inputs(&m, 0.70, 0.50, 0.50), &c));
        assert!(large.size_usd > small.size_usd);
        assert!(large.size_usd <= 1.0);
    }

    #[test]
    fn deep_otm_lower_fee_lets_smaller_edge_fire() {
        // At p = 0.10 the required edge is much smaller, so smaller edges can fire.
        let m = MarketId::new("M");
        let c = cfg();
        // p_yes = 0.13, yes_ask = 0.10 → edge = 0.03.
        // Required at 0.10: 0.072 * 0.01 * 0.9 + 0.005 = 0.00565. 0.03 > 0.005. Fires.
        let _ = expect_fire(decide(inputs(&m, 0.13, 0.10, 0.95), &c));
        // Same edge magnitude at ATM (p_yes=0.53, ask=0.50) → 0.03 vs required ~0.014.
        // Both fire but the OTM one fires at a smaller threshold.
        let small_atm = expect_fire(decide(inputs(&m, 0.53, 0.50, 0.50), &c));
        // Sanity: the size for an OTM 3-cent edge should be larger relative to its gate
        // than the same nominal edge at ATM. (Both fire; that's what we're testing.)
        assert!(small_atm.size_usd > 0.0);
    }
}
