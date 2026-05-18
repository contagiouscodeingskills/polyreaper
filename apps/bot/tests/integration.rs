//! End-to-end integration scenarios for the trading core.
//!
//! These tests exercise multiple components together (strategy → risk →
//! executor → position → bankroll → metrics) to catch interactions that
//! single-module unit tests miss. They are the regression net for
//! defects we've already burned on (cap-reset bug, σ degeneracy, etc.)
//! and a sanity check that the production state machine holds invariants
//! under stress.
//!
//! Conventions:
//! - All time is bot-relative seconds (so cooldowns + freshness gates
//!   are deterministic).
//! - The "bankroll" invariant: `bankroll + open_shares × current_mid
//!   = initial_bankroll + total_realised_pnl` (sanity check on every
//!   tick, by construction).

use bot::bot::check_data_quality;
use bot::config::{RiskConfig, StrategyConfig};
use bot::decision_log::{DecisionKind, DecisionRecord, IncompleteReason, SCHEMA_VERSION};
use bot::execution::PaperExecutor;
use bot::live::{reconcile, LiveOrder, LiveOrderId, OrderState};
use bot::metrics::{DecisionKindMetric, MetricsRegistry, MetricsSnapshot};
use bot::position::PositionStore;
use bot::risk::{RejectReason, RiskDecision, RiskEngine};
use bot::strategy::Signal;
use market_registry::{MarketId, Outcome};

/// Hand-build an approved signal so we don't have to push through
/// scoring + decide() (those are unit-tested separately).
fn signal(market: &str, side: Outcome, size_usd: f64, price: f64) -> Signal {
    Signal {
        market_id: MarketId::new(market),
        side,
        size_usd,
        price,
        fv_for_side: price + 0.10,
        mid_for_side: price,
        edge: 0.10,
        ttr_secs: 120.0,
    }
}

/// Settle a full position at $1 (winning) or $0 (losing) and update the
/// bankroll the way the bot does on a market_resolved event.
fn settle_and_credit(
    positions: &mut PositionStore,
    bankroll: &mut f64,
    market: &MarketId,
    winner: Outcome,
) -> f64 {
    let pos = positions.get(market).cloned();
    let pnl = positions.settle_resolution(market, winner).unwrap_or(0.0);
    // Proceeds returned to bankroll = shares × (1.0 if win else 0.0).
    if let Some(p) = pos {
        let proceeds = if p.side == winner {
            p.shares * 1.0
        } else {
            0.0
        };
        *bankroll += proceeds;
    }
    pnl
}

/// Apply a fill the way the bot does: deduct cost from bankroll, update
/// positions, record on risk for the notional cap + cooldown.
fn apply_fill(
    executor: &mut PaperExecutor,
    positions: &mut PositionStore,
    risk: &mut RiskEngine,
    bankroll: &mut f64,
    sig: Signal,
    now_s: f64,
) {
    let market = sig.market_id.clone();
    let fill = executor.submit(sig, positions);
    *bankroll -= fill.fill_size_usd;
    risk.record_fill(market, fill.fill_size_usd, now_s);
}

// ---------------------------------------------------------------------------
// Bankroll conservation
// ---------------------------------------------------------------------------

#[test]
fn bankroll_invariant_holds_through_win_and_loss_cycle() {
    let mut positions = PositionStore::new();
    let mut risk = RiskEngine::new();
    let mut executor = PaperExecutor::new();
    let mut bankroll = 1000.0_f64;
    let initial_bankroll = bankroll;

    let cfg = RiskConfig::default();

    // Trade 1: $5 YES @ 0.50 on M1 → 10 shares; M1 resolves YES.
    let s = signal("M1", Outcome::Yes, 5.0, 0.50);
    let approved = match risk.evaluate(s, &positions, &cfg, None, 0.0) {
        RiskDecision::Approve(a) => a,
        other => panic!("expected approve, got {:?}", other),
    };
    apply_fill(
        &mut executor,
        &mut positions,
        &mut risk,
        &mut bankroll,
        approved,
        0.0,
    );
    // After fill: bankroll = $995; 10 shares of YES on M1.
    assert!((bankroll - 995.0).abs() < 1e-9);
    let _pnl_m1 = settle_and_credit(
        &mut positions,
        &mut bankroll,
        &MarketId::new("M1"),
        Outcome::Yes,
    );
    // YES won → proceeds $10 → bankroll = $995 + $10 = $1005.
    assert!((bankroll - 1005.0).abs() < 1e-9);

    // Trade 2: $5 NO @ 0.40 on M2 → 12.5 shares; M2 resolves YES (loser).
    let s = signal("M2", Outcome::No, 5.0, 0.40);
    let approved = match risk.evaluate(s, &positions, &cfg, None, 100.0) {
        RiskDecision::Approve(a) => a,
        other => panic!("expected approve, got {:?}", other),
    };
    apply_fill(
        &mut executor,
        &mut positions,
        &mut risk,
        &mut bankroll,
        approved,
        100.0,
    );
    // bankroll = $1000.
    assert!((bankroll - 1000.0).abs() < 1e-9);
    settle_and_credit(
        &mut positions,
        &mut bankroll,
        &MarketId::new("M2"),
        Outcome::Yes, // loser for NO position
    );
    // NO lost → proceeds $0 → bankroll = $1000.
    assert!((bankroll - 1000.0).abs() < 1e-9);

    // Invariant: bankroll - initial_bankroll == total_realised
    let expected_delta = positions.total_realised();
    assert!(
        (bankroll - initial_bankroll - expected_delta).abs() < 1e-9,
        "bankroll - initial != realised; bankroll={} realised={}",
        bankroll,
        expected_delta
    );
}

// ---------------------------------------------------------------------------
// Cap-reset bug regression — full pipeline
// ---------------------------------------------------------------------------

#[test]
fn side_flip_does_not_reset_per_market_cap_through_full_pipeline() {
    // This is the production bug: NO position fills to cap, strategy
    // flips to YES, prior code would auto-close NO and treat it as
    // "fresh cap headroom" for YES. With cumulative-notional tracking
    // the second fire must reject.
    let mut positions = PositionStore::new();
    let mut risk = RiskEngine::new();
    let mut executor = PaperExecutor::new();
    let mut bankroll = 1000.0_f64;

    let cfg = RiskConfig {
        max_notional_per_market_usd: 5.0,
        max_per_trade_usd: 1.0,
        min_secs_between_fires_per_market: 0.1, // tight so we can fire 5x quick
        ..RiskConfig::default()
    };

    // Fire NO 5 times at $1 each (taps cap).
    for i in 0..5 {
        let now = i as f64 * 0.5;
        let s = signal("MFLIP", Outcome::No, 1.0, 0.50);
        if let RiskDecision::Approve(a) = risk.evaluate(s, &positions, &cfg, None, now) {
            apply_fill(
                &mut executor,
                &mut positions,
                &mut risk,
                &mut bankroll,
                a,
                now,
            );
        } else {
            panic!("fire {} on NO should have approved", i);
        }
    }
    assert!((risk.cumulative_notional(&MarketId::new("MFLIP")) - 5.0).abs() < 1e-9);

    // Now flip to YES. The cap is at 5; cumulative is 5; must reject.
    let s = signal("MFLIP", Outcome::Yes, 1.0, 0.50);
    let out = risk.evaluate(s, &positions, &cfg, Some(0.50), 10.0);
    assert!(matches!(
        out,
        RiskDecision::Reject(RejectReason::NotionalCapReached)
    ));
}

// ---------------------------------------------------------------------------
// Kill switch end-to-end
// ---------------------------------------------------------------------------

#[test]
fn portfolio_kill_switch_halts_all_markets_after_breach() {
    let mut positions = PositionStore::new();
    let mut risk = RiskEngine::new();
    let mut executor = PaperExecutor::new();
    let mut bankroll = 1000.0_f64;

    let cfg = RiskConfig {
        max_session_loss_usd: 15.0,
        max_per_trade_usd: 10.0,
        // Generous cap so we can fire on multiple markets quickly.
        max_concurrent_positions: 10,
        ..RiskConfig::default()
    };

    // Lose $20 on MA.
    let s = signal("MA", Outcome::Yes, 10.0, 0.50);
    let approved = match risk.evaluate(s, &positions, &cfg, None, 0.0) {
        RiskDecision::Approve(a) => a,
        _ => panic!(),
    };
    apply_fill(
        &mut executor,
        &mut positions,
        &mut risk,
        &mut bankroll,
        approved,
        0.0,
    );
    // YES lost → $0 proceeds → -$10 PnL.
    settle_and_credit(
        &mut positions,
        &mut bankroll,
        &MarketId::new("MA"),
        Outcome::No,
    );
    assert!((positions.total_realised() - -10.0).abs() < 1e-9);

    // Lose another $10 on MB to push past the cap.
    let s = signal("MB", Outcome::Yes, 10.0, 0.50);
    let approved = match risk.evaluate(s, &positions, &cfg, None, 5.0) {
        RiskDecision::Approve(a) => a,
        _ => panic!(),
    };
    apply_fill(
        &mut executor,
        &mut positions,
        &mut risk,
        &mut bankroll,
        approved,
        5.0,
    );
    settle_and_credit(
        &mut positions,
        &mut bankroll,
        &MarketId::new("MB"),
        Outcome::No,
    );
    assert!(positions.total_realised() <= -20.0);

    // Next signal on a fresh market MC should be killed.
    let s = signal("MC", Outcome::Yes, 1.0, 0.50);
    let out = risk.evaluate(s, &positions, &cfg, None, 10.0);
    assert!(matches!(
        out,
        RiskDecision::Reject(RejectReason::KillSwitchTripped)
    ));
    assert!(risk.is_kill_switch_tripped());

    // Even after manual reset, the cap is still breached → re-trips.
    risk.reset_kill_switch();
    assert!(!risk.is_kill_switch_tripped());
    let s = signal("MC", Outcome::Yes, 1.0, 0.50);
    let out = risk.evaluate(s, &positions, &cfg, None, 15.0);
    assert!(matches!(
        out,
        RiskDecision::Reject(RejectReason::KillSwitchTripped)
    ));
}

// ---------------------------------------------------------------------------
// Chaos sequence: many fills, settles, verify invariants
// ---------------------------------------------------------------------------

#[test]
fn chaos_alternating_fills_and_settles_preserves_invariants() {
    let mut positions = PositionStore::new();
    let mut risk = RiskEngine::new();
    let mut executor = PaperExecutor::new();
    let mut bankroll = 10_000.0_f64;
    let initial_bankroll = bankroll;

    let cfg = RiskConfig {
        max_per_trade_usd: 5.0,
        max_notional_per_market_usd: 5.0,
        max_loss_per_market_usd: 50.0,
        max_session_loss_usd: 9_999.0, // effectively unlimited for this test
        min_secs_between_fires_per_market: 0.0,
        max_concurrent_positions: 100,
        ..RiskConfig::default()
    };

    // Pseudo-random walk: alternate winner/loser, vary side and price.
    let scenarios: &[(&str, Outcome, f64, Outcome)] = &[
        ("CA", Outcome::Yes, 0.60, Outcome::Yes), // win
        ("CB", Outcome::No, 0.45, Outcome::Yes),  // lose
        ("CC", Outcome::Yes, 0.30, Outcome::Yes), // win
        ("CD", Outcome::Yes, 0.80, Outcome::No),  // lose
        ("CE", Outcome::No, 0.20, Outcome::No),   // win
        ("CF", Outcome::No, 0.50, Outcome::Yes),  // lose
        ("CG", Outcome::Yes, 0.75, Outcome::Yes), // win
    ];
    let mut t = 0.0;
    for (mid, side, price, winner) in scenarios.iter().copied() {
        let s = signal(mid, side, 5.0, price);
        let approved = match risk.evaluate(s, &positions, &cfg, None, t) {
            RiskDecision::Approve(a) => a,
            other => panic!("approve expected, got {:?}", other),
        };
        apply_fill(
            &mut executor,
            &mut positions,
            &mut risk,
            &mut bankroll,
            approved,
            t,
        );
        t += 10.0;
        settle_and_credit(&mut positions, &mut bankroll, &MarketId::new(mid), winner);
    }

    // Invariant: no open positions remain after all markets settle.
    assert_eq!(positions.open_count(), 0);

    // Invariant: bankroll - initial == total realised.
    let realised = positions.total_realised();
    assert!(
        (bankroll - initial_bankroll - realised).abs() < 1e-9,
        "bankroll={}, initial={}, realised={}",
        bankroll,
        initial_bankroll,
        realised
    );

    // Sanity: realised should be finite and non-NaN.
    assert!(realised.is_finite());

    // The executor's fill count should match the number of fired signals.
    assert_eq!(executor.fill_count(), scenarios.len());
}

// ---------------------------------------------------------------------------
// State persistence: round-trip across simulated restart
// ---------------------------------------------------------------------------

#[test]
fn state_persists_bankroll_across_simulated_restart() {
    use bot::position::Position;
    use bot::state_persist::{load_latest, MarketNotional, MarketPnl, StatePersister};

    let tmp = tempdir_path();
    let path = tmp.join("bot_state.ndjson");

    {
        // First "session": deposit $1000, lose $250, persist.
        let mut persister = StatePersister::open(path.clone()).unwrap();
        let snap = bot::state_persist::StateSnapshot {
            schema_version: bot::state_persist::SCHEMA_VERSION,
            saved_at_local_ts_ns: "1".into(),
            session_id: "s1".into(),
            bot_version: "test".into(),
            bankroll_usd: 750.0,
            total_realised_pnl_usd: -250.0,
            open_positions: Vec::<Position>::new(),
            cumulative_notional_per_market: Vec::<MarketNotional>::new(),
            realised_pnl_per_market: Vec::<MarketPnl>::new(),
        };
        persister.write(&snap).unwrap();
    }

    // Second "session": restore.
    let restored = load_latest(&path).unwrap().expect("snapshot should exist");
    assert!((restored.bankroll_usd - 750.0).abs() < 1e-9);
    assert!((restored.total_realised_pnl_usd - -250.0).abs() < 1e-9);
    assert_eq!(restored.session_id, "s1");

    // Clean up.
    let _ = std::fs::remove_file(&path);
}

fn tempdir_path() -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let unique = format!(
        "polybot-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    p.push(unique);
    std::fs::create_dir_all(&p).expect("temp dir");
    p
}

// ---------------------------------------------------------------------------
// Data-quality gates surface through the public DecisionRecord path
// ---------------------------------------------------------------------------

/// Build an otherwise-clean `DecisionRecord` so each test only has to
/// set the field it wants to stress.
fn dq_record() -> DecisionRecord {
    DecisionRecord {
        schema_version: SCHEMA_VERSION,
        local_ts_ns: "0".into(),
        session_id: "it".into(),
        bot_version: "test".into(),
        market_id: "m".into(),
        market_slug: "s".into(),
        yes_token: "y".into(),
        no_token: "n".into(),
        end_epoch: 0,
        effective_start_epoch: 0,
        ttr_secs: 60.0,
        binance_btc_mid_usd: None,
        binance_strike_usd: None,
        btc_last_update_age_ms: None,
        btc_history_len: 0,
        poly_yes_bid: None,
        poly_yes_ask: None,
        poly_yes_mid: None,
        poly_yes_bid_size: None,
        poly_yes_ask_size: None,
        poly_yes_spread: None,
        poly_no_bid: None,
        poly_no_ask: None,
        poly_no_mid: None,
        poly_no_bid_size: None,
        poly_no_ask_size: None,
        poly_no_spread: None,
        poly_book_age_ms: None,
        btc_log_return_5s: None,
        btc_log_return_30s: None,
        btc_log_return_60s: None,
        btc_log_return_300s: None,
        sigma_per_sec_5s: None,
        sigma_per_sec_30s: None,
        sigma_per_sec_60s: None,
        sigma_per_sec_300s: None,
        time_bucket: None,
        scoring_p_yes: None,
        scoring_p_no: None,
        scoring_raw: None,
        scoring_regime: None,
        feat_btc_strike_distance_z: None,
        feat_btc_drift_5s_z: None,
        feat_btc_drift_30s_z: None,
        feat_btc_drift_60s_z: None,
        feat_yes_book_imbalance: None,
        feat_no_book_imbalance: None,
        feat_yes_spread_z: None,
        feat_btc_momentum: None,
        feat_binance_volume_60s_btc: None,
        feat_binance_flow_imbalance_60s: None,
        taker_required_edge_yes: None,
        taker_required_edge_no: None,
        implied_strike_usd: None,
        strike_gap_usd: None,
        strike_gap_bps: None,
        sigma_per_sec_used: None,
        sigma_source: None,
        vol_samples: 0,
        fv_yes: None,
        fv_no: None,
        edge_yes: None,
        decision_kind: DecisionKind::Incomplete,
        decision_side: None,
        decision_size_usd: None,
        decision_price: None,
        no_signal_reason: None,
        reject_reason: None,
        incomplete_reason: None,
    }
}

#[test]
fn dq_gate_is_reachable_via_public_api_and_passes_clean_record() {
    let rec = dq_record();
    let strategy = StrategyConfig::default();
    assert!(check_data_quality(&rec, &strategy).is_none());
}

#[test]
fn dq_gate_routes_stale_binance_to_incomplete_via_public_api() {
    let mut rec = dq_record();
    rec.btc_last_update_age_ms = Some(60_000.0); // far past default 5s cap
    let strategy = StrategyConfig::default();
    assert_eq!(
        check_data_quality(&rec, &strategy),
        Some(IncompleteReason::StaleBinanceFeed)
    );
}

#[test]
fn dq_gate_routes_stale_poly_book_to_incomplete_via_public_api() {
    let mut rec = dq_record();
    rec.poly_book_age_ms = Some(10_000.0); // past default 3s cap
    let strategy = StrategyConfig::default();
    assert_eq!(
        check_data_quality(&rec, &strategy),
        Some(IncompleteReason::StalePolyBook)
    );
}

#[test]
fn dq_gate_routes_out_of_range_sigma_to_incomplete_via_public_api() {
    let mut rec = dq_record();
    rec.sigma_per_sec_used = Some(0.5);
    rec.sigma_source = Some("estimated".into());
    let strategy = StrategyConfig::default();
    assert_eq!(
        check_data_quality(&rec, &strategy),
        Some(IncompleteReason::SigmaOutOfRange)
    );
}

#[test]
fn dq_gate_observes_config_thresholds_from_strategy() {
    // Tighten the cap in StrategyConfig and confirm the gate fires
    // at the new threshold — proves the config plumbing is honoured.
    let mut rec = dq_record();
    rec.btc_last_update_age_ms = Some(2_000.0); // 2s old
    let tight_strategy = StrategyConfig {
        max_btc_tick_age_secs: 1.0,
        ..StrategyConfig::default()
    };
    assert_eq!(
        check_data_quality(&rec, &tight_strategy),
        Some(IncompleteReason::StaleBinanceFeed)
    );
    // Same record with the default 5s cap → passes.
    assert!(check_data_quality(&rec, &StrategyConfig::default()).is_none());
}

// ---------------------------------------------------------------------------
// Live reconciler: full state-machine cycle
// ---------------------------------------------------------------------------

#[test]
fn reconciler_walks_an_order_from_pending_to_filled() {
    let id = LiveOrderId::new("ord-1");
    let mk = |state: OrderState, filled: f64| LiveOrder {
        id: id.clone(),
        market_id: MarketId::new("LM"),
        side: Outcome::Yes,
        limit_price: 0.55,
        size_usd: 4.0,
        filled_size_usd: filled,
        state,
    };

    // Tick 1: local says Pending, venue says Acked.
    let local = vec![mk(OrderState::Pending, 0.0)];
    let venue = vec![mk(OrderState::Acked, 0.0)];
    let diff = reconcile(&local, &venue);
    assert_eq!(diff.updates.len(), 1);
    assert_eq!(diff.updates[0].new_state, OrderState::Acked);

    // Tick 2: partial fill.
    let local = vec![mk(OrderState::Acked, 0.0)];
    let venue = vec![mk(OrderState::PartiallyFilled, 1.5)];
    let diff = reconcile(&local, &venue);
    assert_eq!(diff.updates.len(), 1);
    assert_eq!(diff.updates[0].new_state, OrderState::PartiallyFilled);
    assert_eq!(diff.updates[0].new_filled_size_usd, Some(1.5));

    // Tick 3: complete fill.
    let local = vec![mk(OrderState::PartiallyFilled, 1.5)];
    let venue = vec![mk(OrderState::Filled, 4.0)];
    let diff = reconcile(&local, &venue);
    assert_eq!(diff.updates.len(), 1);
    assert_eq!(diff.updates[0].new_state, OrderState::Filled);
    assert_eq!(diff.updates[0].new_filled_size_usd, Some(4.0));

    // Tick 4: order is Filled (terminal), venue forgets about it — no diff.
    let local = vec![mk(OrderState::Filled, 4.0)];
    let venue: Vec<LiveOrder> = vec![];
    let diff = reconcile(&local, &venue);
    assert!(diff.updates.is_empty());
}

// ---------------------------------------------------------------------------
// Metrics end-to-end: bot writes, server renders
// ---------------------------------------------------------------------------

#[test]
fn metrics_registry_renders_full_bot_state() {
    let reg = MetricsRegistry::new();
    reg.set(MetricsSnapshot {
        bankroll_usd: 950.0,
        total_realised_pnl_usd: -50.0,
        open_positions: 1,
        total_fills: 3,
        seen_markets: 5,
        resolved_markets: 4,
        btc_history_len: 900,
        vol_samples: 120,
        kill_switch_tripped: false,
        latest_btc_mid_usd: Some(99_500.0),
        sigma_per_sec: Some(7e-5),
        latest_strike_usd: Some(100_000.0),
        latest_ttr_secs: Some(180.0),
        realised_pnl_per_market_usd: [("ma".to_string(), -25.0), ("mb".to_string(), -25.0)]
            .into_iter()
            .collect(),
        decisions_fire_total: 3,
        decisions_rejected_total: 0,
        decisions_no_signal_total: 200,
        decisions_incomplete_total: 50,
        ..Default::default()
    });
    reg.record_decision(DecisionKindMetric::Fire); // bump to 4
    let text = bot::metrics::render(&reg.snapshot());
    assert!(text.contains("bot_bankroll_usd 950"));
    assert!(text.contains("bot_total_realised_pnl_usd -50"));
    assert!(text.contains("bot_open_positions 1"));
    assert!(text.contains("bot_kill_switch_tripped 0"));
    assert!(text.contains("bot_btc_mid_usd 99500"));
    assert!(text.contains("bot_decisions_total{kind=\"fire\"} 4"));
    assert!(text.contains("bot_realised_pnl_per_market_usd{market_id=\"ma\"} -25"));
}
