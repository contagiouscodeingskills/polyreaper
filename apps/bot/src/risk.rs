//! Risk engine — sits between strategy and execution. Vetoes signals.
//!
//! Checks in v0, applied in order:
//! 1. Sanity (finite numbers, valid price).
//! 2. Portfolio kill switch — sticky once tripped; halts all trading
//!    on every market when aggregate session loss breaches
//!    `max_session_loss_usd`.
//! 3. Per-market P&L → trip per-market kill switch if loss cap breached.
//! 4. Per-market kill-switch check.
//! 5. Cooldown — minimum seconds between fills on the same market.
//! 6. Concurrent-positions cap — only blocks opening a *new* market.
//! 7. Size clip — to `max_per_trade_usd` AND to `max_notional_per_market_usd`
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
    /// Portfolio-level kill switch tripped — aggregate session loss
    /// exceeded `max_session_loss_usd`. Halts ALL trading on every
    /// market for the rest of the session.
    KillSwitchTripped,
    /// Internal sanity check failed (NaN, negative size, etc.).
    InternalError,
}

#[derive(Debug, Clone, Default)]
pub struct RiskEngine {
    killed: HashMap<MarketId, KillReason>,
    last_fire_at: HashMap<MarketId, f64>,
    /// Cumulative gross notional fired per market — sum of all fill sizes,
    /// regardless of side. Survives side-flips so the bot can't churn
    /// between YES and NO and reset the per-market notional cap each time.
    /// Defect class: see flip-cap bug notes in `docs/`.
    cumulative_notional: HashMap<MarketId, f64>,
    /// Portfolio-level kill switch. Sticky once tripped — only `reset()`
    /// clears it. Once true, every signal on every market is rejected.
    /// Tripped when aggregate session loss breaches `max_session_loss_usd`.
    kill_switch_tripped: bool,
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

    /// Record that a fill happened on `market_id` at `now_secs` with the
    /// given `size_usd`. Caller must invoke this AFTER a successful
    /// executor submission so failed submits don't lock out re-tries.
    /// Updates per-market cumulative gross notional for the cap check.
    pub fn record_fill(&mut self, market_id: MarketId, size_usd: f64, now_secs: f64) {
        self.last_fire_at.insert(market_id.clone(), now_secs);
        if size_usd > 0.0 && size_usd.is_finite() {
            *self.cumulative_notional.entry(market_id).or_insert(0.0) += size_usd;
        }
    }

    /// Read-only view of cumulative gross notional fired on a market.
    pub fn cumulative_notional(&self, market_id: &MarketId) -> f64 {
        self.cumulative_notional
            .get(market_id)
            .copied()
            .unwrap_or(0.0)
    }

    /// Borrow the full cumulative-notional map (for state persistence).
    pub fn cumulative_notional_map(&self) -> &HashMap<MarketId, f64> {
        &self.cumulative_notional
    }

    /// Is the portfolio-level kill switch currently tripped?
    pub fn is_kill_switch_tripped(&self) -> bool {
        self.kill_switch_tripped
    }

    /// Manually arm the portfolio kill switch (e.g., emergency stop).
    pub fn trip_kill_switch(&mut self) {
        self.kill_switch_tripped = true;
    }

    /// Reset the portfolio kill switch — used by ops/tests when the
    /// operator has reviewed the loss and chooses to resume trading.
    /// Not invoked automatically. Per-market kill state is independent.
    pub fn reset_kill_switch(&mut self) {
        self.kill_switch_tripped = false;
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

        // 2. Portfolio kill switch. Evaluate aggregate session loss
        // against `max_session_loss_usd`. Once tripped it stays tripped
        // until an operator calls `reset_kill_switch()` — we don't
        // auto-reset on P&L recovery (a "blow-up + bounce" is exactly
        // the pattern that should pause us for review).
        //
        // LIMITATION: the unrealised component is read off the signal's
        // own market only. v0 runs with `max_concurrent_positions = 1`,
        // so this captures the entire portfolio's unrealised. If that
        // cap is ever raised, this check will UNDER-COUNT loss on other
        // open positions. The debug_assert below catches that statically
        // in tests; production should rewire to take a price-lookup
        // closure (see `PositionStore::total_unrealised`) before
        // raising the concurrency cap.
        debug_assert!(
            positions.open_count() <= 1,
            "portfolio kill switch only aggregates unrealised P&L on the signal's market; \
             multi-market concurrency requires rewiring evaluate() to take a price lookup"
        );
        if cfg.max_session_loss_usd.is_finite() && cfg.max_session_loss_usd > 0.0 {
            let session_realised = positions.total_realised();
            let session_unrealised = positions
                .get(&signal.market_id)
                .zip(mark_side_mid)
                .map(|(pos, mid)| pos.unrealised_pnl_usd(mid))
                .unwrap_or(0.0);
            let session_total = session_realised + session_unrealised;
            if -session_total >= cfg.max_session_loss_usd {
                self.kill_switch_tripped = true;
            }
        }
        if self.kill_switch_tripped {
            return RiskDecision::Reject(RejectReason::KillSwitchTripped);
        }

        // 3. Per-market kill state from latest P&L.
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

        // 4. Killed → reject.
        if self.is_killed(&signal.market_id) {
            return RiskDecision::Reject(RejectReason::MarketKilled);
        }

        // 5. Cooldown.
        if let Some(&last) = self.last_fire_at.get(&signal.market_id) {
            if now_secs.is_finite() && (now_secs - last) < cfg.min_secs_between_fires_per_market {
                return RiskDecision::Reject(RejectReason::Cooldown);
            }
        }

        // 6. Concurrent-positions cap — only blocks opening a new market.
        let is_new_market = positions.get(&signal.market_id).is_none();
        if is_new_market && positions.open_count() >= cfg.max_concurrent_positions {
            return RiskDecision::Reject(RejectReason::TooManyConcurrent);
        }

        // 7. Size clip — to per-trade cap and to per-market notional headroom.
        let mut sig = signal;
        if sig.size_usd > cfg.max_per_trade_usd {
            sig.size_usd = cfg.max_per_trade_usd;
        }
        // Notional cap uses CUMULATIVE GROSS FIRED notional on this market
        // (sum of all fills, both sides), NOT just the current side's cost.
        // This prevents the cap-reset bug where flipping sides would
        // auto-close the existing position (cost → small) and allow the
        // bot to churn between sides repeatedly. `positions.get(...).cost_usd`
        // alone would be a per-side measure; this is per-market.
        let existing_cost = self.cumulative_notional(&sig.market_id);
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
        eng.record_fill(MarketId::new("M"), 0.5, 0.0);
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
        eng.record_fill(MarketId::new("M"), 0.5, 0.0);
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
        eng.record_fill(MarketId::new("M1"), 0.5, 0.0);
        // M2 has no recent fire — should approve immediately.
        let out = eng.evaluate(sig("M2", 0.5), &store, &cfg, None, 0.5);
        assert!(matches!(out, RiskDecision::Approve(_)));
    }

    #[test]
    fn notional_cap_clips_size_when_headroom_partial() {
        let mut eng = RiskEngine::new();
        let store = PositionStore::new();
        let m = MarketId::new("M");
        // $4.50 already fired (gross) on M.
        eng.record_fill(m.clone(), 4.5, 0.0);
        let cfg = RiskConfig {
            max_per_trade_usd: 1.0,
            max_notional_per_market_usd: 5.0,
            ..RiskConfig::default()
        };
        let out = eng.evaluate(sig("M", 1.0), &store, &cfg, None, 100.0);
        match out {
            RiskDecision::Approve(s) => assert!((s.size_usd - 0.5).abs() < 1e-9),
            _ => panic!("expected clipped approve, got {:?}", out),
        }
    }

    #[test]
    fn notional_cap_rejects_when_at_cap() {
        let mut eng = RiskEngine::new();
        let store = PositionStore::new();
        let m = MarketId::new("M");
        eng.record_fill(m.clone(), 5.0, 0.0);
        let cfg = RiskConfig {
            max_notional_per_market_usd: 5.0,
            ..RiskConfig::default()
        };
        let out = eng.evaluate(sig("M", 0.5), &store, &cfg, None, 100.0);
        assert_eq!(out, RiskDecision::Reject(RejectReason::NotionalCapReached));
    }

    #[test]
    fn cap_does_not_reset_on_side_flip() {
        // Regression test for the cap-reset bug observed in live paper
        // trading: bot fires NO up to ~$5, then strategy flips and tries
        // to fire YES. `positions.apply_fill` would auto-close the NO
        // position, making position.cost_usd small for YES — so the OLD
        // notional check (against position.cost_usd) would let the bot
        // fire $5 more. With cumulative-notional tracking it should reject.
        let mut eng = RiskEngine::new();
        let mut store = PositionStore::new();
        let m = MarketId::new("M");
        // Fire NO 5 times $1 each.
        for _ in 0..5 {
            eng.record_fill(m.clone(), 1.0, 0.0);
            store.apply_fill(&m, Outcome::No, 1.0, 0.50);
        }
        let cfg = RiskConfig {
            max_notional_per_market_usd: 5.0,
            ..RiskConfig::default()
        };
        // Now try to fire YES — same market.
        let mut yes_sig = sig("M", 1.0);
        yes_sig.side = Outcome::Yes;
        let out = eng.evaluate(yes_sig, &store, &cfg, None, 10.0);
        assert_eq!(
            out,
            RiskDecision::Reject(RejectReason::NotionalCapReached),
            "cap must include the prior side's notional"
        );
    }

    #[test]
    fn portfolio_kill_switch_trips_on_session_loss_breach() {
        let mut eng = RiskEngine::new();
        let mut store = PositionStore::new();
        // Realise a $20 loss across two markets ($10 + $10).
        for name in ["MA", "MB"] {
            let m = MarketId::new(name);
            store.apply_fill(&m, Outcome::Yes, 10.0, 0.50);
            store.close_market(&m, 0.0); // -$10 each
        }
        let cfg = RiskConfig {
            // $15 cap → already breached.
            max_session_loss_usd: 15.0,
            ..RiskConfig::default()
        };
        let out = eng.evaluate(sig("MC", 0.5), &store, &cfg, None, 100.0);
        assert_eq!(out, RiskDecision::Reject(RejectReason::KillSwitchTripped));
        assert!(eng.is_kill_switch_tripped());
    }

    #[test]
    fn portfolio_kill_switch_is_sticky() {
        let mut eng = RiskEngine::new();
        let mut store = PositionStore::new();
        let m = MarketId::new("M");
        store.apply_fill(&m, Outcome::Yes, 20.0, 0.50);
        store.close_market(&m, 0.0); // -$20 realised
        let cfg = RiskConfig {
            max_session_loss_usd: 15.0,
            ..RiskConfig::default()
        };
        // First call trips the kill switch.
        let _ = eng.evaluate(sig("M2", 0.5), &store, &cfg, None, 100.0);
        assert!(eng.is_kill_switch_tripped());
        // Realise a big win that takes net session P&L positive ($40 > $20 loss).
        for i in 0..4 {
            let mw = MarketId::new(&format!("MWIN-{}", i));
            store.apply_fill(&mw, Outcome::Yes, 10.0, 0.50);
            let _ = store.settle_resolution(&mw, Outcome::Yes); // +$10 each
        }
        assert!(
            store.total_realised() > 0.0,
            "session should be net positive now"
        );
        // Kill switch is still tripped — only `reset_kill_switch()` clears it.
        let out = eng.evaluate(sig("MNEW", 0.5), &store, &cfg, None, 200.0);
        assert_eq!(out, RiskDecision::Reject(RejectReason::KillSwitchTripped));
    }

    #[test]
    fn portfolio_kill_switch_can_be_manually_reset() {
        let mut eng = RiskEngine::new();
        let mut store = PositionStore::new();
        let m = MarketId::new("M");
        store.apply_fill(&m, Outcome::Yes, 20.0, 0.50);
        store.close_market(&m, 0.0);
        let cfg = RiskConfig {
            max_session_loss_usd: 15.0,
            ..RiskConfig::default()
        };
        let _ = eng.evaluate(sig("M2", 0.5), &store, &cfg, None, 100.0);
        assert!(eng.is_kill_switch_tripped());
        eng.reset_kill_switch();
        assert!(!eng.is_kill_switch_tripped());
        // After reset, the session loss is still bad, so the next
        // evaluate call re-trips it. That's correct behaviour — the
        // operator presumably reset *and* knew they'd re-trip; this
        // path is mostly used by tests.
        let out = eng.evaluate(sig("M2", 0.5), &store, &cfg, None, 100.0);
        assert_eq!(out, RiskDecision::Reject(RejectReason::KillSwitchTripped));
    }

    #[test]
    fn portfolio_kill_switch_disabled_when_cap_is_zero() {
        let mut eng = RiskEngine::new();
        let mut store = PositionStore::new();
        let m = MarketId::new("M");
        store.apply_fill(&m, Outcome::Yes, 100.0, 0.50);
        store.close_market(&m, 0.0); // -$100 realised
        let cfg = RiskConfig {
            max_session_loss_usd: 0.0, // disabled
            // Bump the per-market loss cap out of the way so that doesn't
            // trip and confuse this test.
            max_loss_per_market_usd: 10_000.0,
            ..RiskConfig::default()
        };
        let out = eng.evaluate(sig("M2", 0.5), &store, &cfg, None, 100.0);
        assert!(matches!(out, RiskDecision::Approve(_)));
        assert!(!eng.is_kill_switch_tripped());
    }

    #[test]
    fn portfolio_kill_switch_counts_unrealised_on_current_market() {
        let mut eng = RiskEngine::new();
        let mut store = PositionStore::new();
        let m = MarketId::new("M");
        // Open YES at 0.50 for $10 (20 shares).
        store.apply_fill(&m, Outcome::Yes, 10.0, 0.50);
        // Mark side mid drops to 0.05 → unrealised = 20×0.05 − 10 = −$9.
        let cfg = RiskConfig {
            max_session_loss_usd: 5.0,      // cap below unrealised loss
            max_loss_per_market_usd: 100.0, // don't trip per-market
            ..RiskConfig::default()
        };
        // Signal targets the same market so the unrealised lookup hits.
        let out = eng.evaluate(sig("M", 0.5), &store, &cfg, Some(0.05), 100.0);
        assert_eq!(out, RiskDecision::Reject(RejectReason::KillSwitchTripped));
    }

    #[test]
    fn manual_trip_kill_switch_blocks_all_signals() {
        let mut eng = RiskEngine::new();
        let store = PositionStore::new();
        let cfg = RiskConfig::default();
        eng.trip_kill_switch();
        let out = eng.evaluate(sig("ANY", 0.5), &store, &cfg, None, 0.0);
        assert_eq!(out, RiskDecision::Reject(RejectReason::KillSwitchTripped));
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
