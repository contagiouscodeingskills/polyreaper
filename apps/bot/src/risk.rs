//! Risk engine — sits between strategy and execution. Vetoes signals.
//!
//! Checks in v0, applied in order:
//! 1. Sanity (finite numbers, valid price).
//! 2. P&L → trip kill switch if loss cap breached.
//! 3. Kill-switch check (per market).
//! 4. Cooldown — minimum seconds between fills on the same market.
//! 5. Concurrent-positions cap — only blocks opening a *new* market.
//! 6. Size clip — to `max_per_trade_usd` AND to `max_notional_per_market_usd`
//!    minus existing cost basis. If headroom is zero or negative → reject.
//!
//! Fail-closed: any internal error → reject.

use std::collections::HashMap;

use market_registry::MarketId;

use crate::config::RiskConfig;
use crate::position::PositionStore;
use crate::strategy::Signal;

#[derive(Debug, Clone, PartialEq)]
pub enum RiskDecision {
    Approve(Signal),
    Reject(RejectReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    /// Per-market loss cap tripped; this market is killed.
    MarketKilled,
    /// Too many concurrent positions to open a new market.
    TooManyConcurrent,
    /// Re-fired too soon after the previous fill on this market.
    Cooldown,
    /// Existing cost basis on this market has reached the notional cap;
    /// no headroom left for any new order.
    NotionalCapReached,
    /// Internal sanity check failed (NaN, negative size, etc.).
    InternalError,
}

#[derive(Debug, Clone, Default)]
pub struct RiskEngine {
    killed: HashMap<MarketId, KillReason>,
    last_fire_at: HashMap<MarketId, f64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KillReason {
    LossCap,
    ManualOverride,
}

impl RiskEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_killed(&self, market_id: &MarketId) -> bool {
        self.killed.contains_key(market_id)
    }

    pub fn kill(&mut self, market_id: MarketId, reason: KillReason) {
        self.killed.insert(market_id, reason);
    }

    /// Record that a fill happened on `market_id` at `now_secs`. Caller
    /// must invoke this AFTER a successful executor submission so failed
    /// submits don't lock out re-tries.
    pub fn record_fill(&mut self, market_id: MarketId, now_secs: f64) {
        self.last_fire_at.insert(market_id, now_secs);
    }

    /// Evaluate a signal. `mark_side_mid` is the current Polymarket mid of
    /// the side any existing position is long (for unrealised-P&L view).
    /// `now_secs` is the caller's clock for the cooldown check; can be
    /// session-relative or wall-clock as long as it's monotonic and
    /// consistent with prior `record_fill` calls.
    pub fn evaluate(
        &mut self,
        signal: Signal,
        positions: &PositionStore,
        cfg: &RiskConfig,
        mark_side_mid: Option<f64>,
        now_secs: f64,
    ) -> RiskDecision {
        // 1. Sanity.
        if !signal.size_usd.is_finite() || signal.size_usd <= 0.0 {
            return RiskDecision::Reject(RejectReason::InternalError);
        }
        if !signal.price.is_finite() || signal.price <= 0.0 || signal.price > 1.0 {
            return RiskDecision::Reject(RejectReason::InternalError);
        }

        // 2. Update kill state from latest P&L.
        let realised = positions.realised(&signal.market_id);
        let unrealised = positions
            .get(&signal.market_id)
            .zip(mark_side_mid)
            .map(|(pos, mid)| pos.unrealised_pnl_usd(mid))
            .unwrap_or(0.0);
        let pnl = realised + unrealised;
        if -pnl >= cfg.max_loss_per_market_usd {
            self.killed
                .insert(signal.market_id.clone(), KillReason::LossCap);
        }

        // 3. Killed → reject.
        if self.is_killed(&signal.market_id) {
            return RiskDecision::Reject(RejectReason::MarketKilled);
        }

        // 4. Cooldown.
        if let Some(&last) = self.last_fire_at.get(&signal.market_id) {
            if now_secs.is_finite() && (now_secs - last) < cfg.min_secs_between_fires_per_market {
                return RiskDecision::Reject(RejectReason::Cooldown);
            }
        }

        // 5. Concurrent-positions cap — only blocks opening a new market.
        let is_new_market = positions.get(&signal.market_id).is_none();
        if is_new_market && positions.open_count() >= cfg.max_concurrent_positions {
            return RiskDecision::Reject(RejectReason::TooManyConcurrent);
        }

        // 6. Size clip — to per-trade cap and to per-market notional headroom.
        let mut sig = signal;
        if sig.size_usd > cfg.max_per_trade_usd {
            sig.size_usd = cfg.max_per_trade_usd;
        }
        let existing_cost = positions
            .get(&sig.market_id)
            .map(|p| p.cost_usd)
            .unwrap_or(0.0);
        let notional_headroom = cfg.max_notional_per_market_usd - existing_cost;
        if notional_headroom <= 0.0 {
            return RiskDecision::Reject(RejectReason::NotionalCapReached);
        }
        if sig.size_usd > notional_headroom {
            sig.size_usd = notional_headroom;
        }
        if sig.size_usd <= 0.0 {
            return RiskDecision::Reject(RejectReason::InternalError);
        }

        RiskDecision::Approve(sig)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RiskConfig;
    use crate::position::PositionStore;
    use market_registry::{MarketId, Outcome};

    fn sig(market: &str, size: f64) -> Signal {
        Signal {
            market_id: MarketId::new(market),
            side: Outcome::Yes,
            size_usd: size,
            price: 0.50,
            fv_for_side: 0.60,
            mid_for_side: 0.50,
            edge: 0.10,
            ttr_secs: 120.0,
        }
    }

    #[test]
    fn approves_clean_signal() {
        let mut eng = RiskEngine::new();
        let store = PositionStore::new();
        let cfg = RiskConfig::default();
        let out = eng.evaluate(sig("M", 0.5), &store, &cfg, None, 0.0);
        assert!(matches!(out, RiskDecision::Approve(_)));
    }

    #[test]
    fn clips_oversized_signal_to_max_per_trade() {
        let mut eng = RiskEngine::new();
        let store = PositionStore::new();
        let cfg = RiskConfig {
            max_per_trade_usd: 1.0,
            ..RiskConfig::default()
        };
        let out = eng.evaluate(sig("M", 5.0), &store, &cfg, None, 0.0);
        match out {
            RiskDecision::Approve(s) => assert!((s.size_usd - 1.0).abs() < 1e-9),
            _ => panic!("expected approve"),
        }
    }

    #[test]
    fn kills_market_when_loss_breaches_cap() {
        let mut eng = RiskEngine::new();
        let mut store = PositionStore::new();
        let m = MarketId::new("M");
        store.apply_fill(&m, Outcome::Yes, 6.0, 0.50);
        store.close_market(&m, 0.0);
        let cfg = RiskConfig {
            max_loss_per_market_usd: 5.0,
            ..RiskConfig::default()
        };
        let out = eng.evaluate(sig("M", 0.5), &store, &cfg, None, 0.0);
        assert_eq!(out, RiskDecision::Reject(RejectReason::MarketKilled));
        assert!(eng.is_killed(&MarketId::new("M")));
    }

    #[test]
    fn rejects_new_market_when_concurrent_cap_hit() {
        let mut eng = RiskEngine::new();
        let mut store = PositionStore::new();
        store.apply_fill(&MarketId::new("M1"), Outcome::Yes, 1.0, 0.50);
        let cfg = RiskConfig {
            max_concurrent_positions: 1,
            ..RiskConfig::default()
        };
        let out = eng.evaluate(sig("M2", 0.5), &store, &cfg, None, 0.0);
        assert_eq!(out, RiskDecision::Reject(RejectReason::TooManyConcurrent));
    }

    #[test]
    fn allows_adding_to_existing_position_at_concurrent_cap() {
        let mut eng = RiskEngine::new();
        let mut store = PositionStore::new();
        store.apply_fill(&MarketId::new("M"), Outcome::Yes, 0.5, 0.50);
        let cfg = RiskConfig {
            max_concurrent_positions: 1,
            ..RiskConfig::default()
        };
        let out = eng.evaluate(sig("M", 0.3), &store, &cfg, None, 0.0);
        assert!(matches!(out, RiskDecision::Approve(_)));
    }

    #[test]
    fn cooldown_blocks_quick_refire() {
        let mut eng = RiskEngine::new();
        let store = PositionStore::new();
        let cfg = RiskConfig {
            min_secs_between_fires_per_market: 2.0,
            ..RiskConfig::default()
        };
        // First fire approved.
        let _ = eng.evaluate(sig("M", 0.5), &store, &cfg, None, 0.0);
        eng.record_fill(MarketId::new("M"), 0.0);
        // 1 second later — still inside cooldown.
        let out = eng.evaluate(sig("M", 0.5), &store, &cfg, None, 1.0);
        assert_eq!(out, RiskDecision::Reject(RejectReason::Cooldown));
    }

    #[test]
    fn cooldown_lifts_after_window() {
        let mut eng = RiskEngine::new();
        let store = PositionStore::new();
        let cfg = RiskConfig {
            min_secs_between_fires_per_market: 2.0,
            ..RiskConfig::default()
        };
        let _ = eng.evaluate(sig("M", 0.5), &store, &cfg, None, 0.0);
        eng.record_fill(MarketId::new("M"), 0.0);
        // 2.5s later — past the cooldown.
        let out = eng.evaluate(sig("M", 0.5), &store, &cfg, None, 2.5);
        assert!(matches!(out, RiskDecision::Approve(_)));
    }

    #[test]
    fn cooldown_is_per_market() {
        let mut eng = RiskEngine::new();
        let mut store = PositionStore::new();
        // Open M1 so M2 doesn't have to deal with concurrent cap.
        store.apply_fill(&MarketId::new("M1"), Outcome::Yes, 0.1, 0.50);
        let cfg = RiskConfig {
            min_secs_between_fires_per_market: 2.0,
            max_concurrent_positions: 5,
            ..RiskConfig::default()
        };
        eng.record_fill(MarketId::new("M1"), 0.0);
        // M2 has no recent fire — should approve immediately.
        let out = eng.evaluate(sig("M2", 0.5), &store, &cfg, None, 0.5);
        assert!(matches!(out, RiskDecision::Approve(_)));
    }

    #[test]
    fn notional_cap_clips_size_when_headroom_partial() {
        let mut eng = RiskEngine::new();
        let mut store = PositionStore::new();
        let m = MarketId::new("M");
        // $4.50 already spent on M.
        store.apply_fill(&m, Outcome::Yes, 4.5, 0.50);
        let cfg = RiskConfig {
            max_per_trade_usd: 1.0,
            max_notional_per_market_usd: 5.0,
            ..RiskConfig::default()
        };
        let out = eng.evaluate(sig("M", 1.0), &store, &cfg, None, 0.0);
        match out {
            RiskDecision::Approve(s) => assert!((s.size_usd - 0.5).abs() < 1e-9),
            _ => panic!("expected clipped approve, got {:?}", out),
        }
    }

    #[test]
    fn notional_cap_rejects_when_at_cap() {
        let mut eng = RiskEngine::new();
        let mut store = PositionStore::new();
        let m = MarketId::new("M");
        store.apply_fill(&m, Outcome::Yes, 5.0, 0.50);
        let cfg = RiskConfig {
            max_notional_per_market_usd: 5.0,
            ..RiskConfig::default()
        };
        let out = eng.evaluate(sig("M", 0.5), &store, &cfg, None, 0.0);
        assert_eq!(out, RiskDecision::Reject(RejectReason::NotionalCapReached));
    }

    #[test]
    fn sanity_rejects_nan_or_negative_size() {
        let mut eng = RiskEngine::new();
        let store = PositionStore::new();
        let cfg = RiskConfig::default();

        let mut nan_sig = sig("M", 0.5);
        nan_sig.size_usd = f64::NAN;
        assert_eq!(
            eng.evaluate(nan_sig, &store, &cfg, None, 0.0),
            RiskDecision::Reject(RejectReason::InternalError)
        );

        let mut neg = sig("M", 0.5);
        neg.size_usd = -1.0;
        assert_eq!(
            eng.evaluate(neg, &store, &cfg, None, 0.0),
            RiskDecision::Reject(RejectReason::InternalError)
        );

        let mut bad_price = sig("M", 0.5);
        bad_price.price = 1.5;
        assert_eq!(
            eng.evaluate(bad_price, &store, &cfg, None, 0.0),
            RiskDecision::Reject(RejectReason::InternalError)
        );
    }
}
