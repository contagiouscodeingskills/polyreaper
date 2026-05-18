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
use bot::live::{reconcile, LiveCredentials, LiveExecutor, LiveOrder, LiveOrderId, MarketContext, OrderState};
use bot::live::client::ApiCredentials;
use bot::metrics::{DecisionKindMetric, MetricsRegistry, MetricsSnapshot};
use bot::position::PositionStore;
use bot::risk::{RejectReason, RiskDecision, RiskEngine};
use bot::signals::scoring::{score, Features, Regime, ScoringConfig};
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

// ---------------------------------------------------------------------------
// Live executor end-to-end: a Signal driven through the executor lands
// on the wire as a signed POST, the response promotes the order to
// Acked, and a follow-up reconcile call from a second mock terminates
// it cleanly.
// ---------------------------------------------------------------------------

use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn read_full_http_request(socket: &mut tokio::net::TcpStream) -> (String, String) {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        let n = socket.read(&mut tmp).await.unwrap();
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..pos]).to_string();
            let cl: usize = head
                .lines()
                .find_map(|l| {
                    let mut s = l.splitn(2, ':');
                    let (n, v) = (s.next()?.trim(), s.next()?.trim());
                    if n.eq_ignore_ascii_case("content-length") {
                        v.parse().ok()
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            while buf.len() < pos + 4 + cl {
                let m = socket.read(&mut tmp).await.unwrap();
                if m == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..m]);
            }
            let body = String::from_utf8_lossy(&buf[pos + 4..]).to_string();
            let req_line = head.lines().next().unwrap_or("").to_string();
            return (req_line, body);
        }
    }
    (String::new(), String::new())
}

fn http_ok_body(body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
    .into_bytes()
}

const TEST_EOA_KEY: &str =
    "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const TEST_PROXY_ADDR: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";

fn test_api_creds() -> ApiCredentials {
    use base64::engine::general_purpose::URL_SAFE as B64URL;
    use base64::Engine;
    ApiCredentials {
        api_key: "key".into(),
        secret_b64url: B64URL.encode(b"secret-bytes-32-aaaaaaaaaaaaaaaa"),
        passphrase: "pass".into(),
    }
}

#[tokio::test]
async fn live_executor_submits_signed_order_and_reconciles_to_terminal_state() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Mock server handles two requests in sequence:
    //   1. POST /order — returns orderID "0xLIVE1"
    //   2. GET  /data/orders — returns one entry showing FILLED state
    let server = tokio::spawn(async move {
        // First request: the submit.
        let (mut sock1, _) = listener.accept().await.unwrap();
        let (req_line, body) = read_full_http_request(&mut sock1).await;
        assert!(req_line.starts_with("POST /order"), "got: {req_line}");
        // Body parses as a SignedOrderRequest with side=BUY and the YES token.
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["order"]["side"].as_str(), Some("BUY"));
        assert_eq!(v["order"]["tokenId"].as_str(), Some("71321045679252212594626385532"));
        let resp = http_ok_body(r#"{"success":true,"errorMsg":"","orderID":"0xLIVE1"}"#);
        sock1.write_all(&resp).await.unwrap();
        sock1.shutdown().await.ok();

        // Second request: the reconciliation fetch.
        let (mut sock2, _) = listener.accept().await.unwrap();
        let (req_line, _) = read_full_http_request(&mut sock2).await;
        assert!(req_line.starts_with("GET /data/orders"), "got: {req_line}");
        let resp_body = r#"{"data":[{"id":"0xLIVE1","status":"FILLED","market":"M1","side":"BUY","price":"0.50","original_size":"10","size_matched":"10","asset_id":"71321045679252212594626385532"}],"next_cursor":"LTE="}"#;
        sock2.write_all(&http_ok_body(resp_body)).await.unwrap();
        sock2.shutdown().await.ok();
    });

    let creds = LiveCredentials::new(TEST_EOA_KEY, TEST_PROXY_ADDR);
    let mut exec =
        LiveExecutor::new(Some(creds), Some(test_api_creds()), format!("http://{addr}"))
            .unwrap();

    // Submit a Signal — picks YES side / 71321045679252212594626385532 by virtue of signal.side.
    let signal = Signal {
        market_id: MarketId::new("M1"),
        side: Outcome::Yes,
        size_usd: 5.0,
        price: 0.50,
        fv_for_side: 0.60,
        mid_for_side: 0.50,
        edge: 0.10,
        ttr_secs: 120.0,
    };
    let ctx = MarketContext {
        yes_token_id: "71321045679252212594626385532".into(),
        no_token_id: "52341098765432109876543210987".into(),
        neg_risk: false,
        fee_rate_bps: 0,
        chain_id: 137,
    };
    let id = exec.submit(signal, &ctx).await.expect("submit ok");
    assert_eq!(id.as_str(), "0xLIVE1");
    assert_eq!(exec.open_count(), 1);

    // Reconcile: fetch venue view, diff against local, apply.
    let local = exec.open_orders();
    let venue = exec
        .fetch_open_orders_from_venue()
        .await
        .expect("fetch ok");
    let diff = reconcile(&local, &venue);
    assert_eq!(diff.updates.len(), 1);
    assert_eq!(diff.updates[0].new_state, OrderState::Filled);
    exec.apply_diff(diff);
    // Filled is terminal → drops from open book.
    assert_eq!(exec.open_count(), 0);

    server.await.unwrap();
}

// ---------------------------------------------------------------------------
// FV model — BTC-only, validated against poly mid
// ---------------------------------------------------------------------------

/// Default model produces a sensible probability from a clean BTC
/// state: BTC near strike + neutral microstructure → close to 0.5.
#[test]
fn default_scoring_at_strike_with_neutral_features_is_roughly_half() {
    let cfg = ScoringConfig::default();
    let features = Features {
        btc_strike_distance_z: Some(0.0),  // BTC == strike
        btc_drift_30s_z: Some(0.0),
        binance_flow_imbalance_60s: Some(0.0),
        btc_momentum: Some(0.0),
        ..Default::default()
    };
    let out = score(&features, Regime::Mid, &cfg).expect("scored");
    assert!((out.p_yes - 0.5).abs() < 1e-9, "got {}", out.p_yes);
}

/// BTC strongly above strike + bullish flow → model says YES very
/// likely, with no input from poly at all. This is the bot doing its
/// own thinking.
#[test]
fn btc_above_strike_with_bullish_flow_predicts_yes_independently_of_poly() {
    let cfg = ScoringConfig::default();
    let features = Features {
        btc_strike_distance_z: Some(1.5),       // BTC clearly above strike
        btc_drift_30s_z: Some(1.0),             // recent upward drift
        binance_flow_imbalance_60s: Some(0.6),  // aggressive buyers
        btc_momentum: Some(0.8),                // accelerating
        // Note: NO poly_mid input.
        ..Default::default()
    };
    let out = score(&features, Regime::Mid, &cfg).expect("scored");
    // raw = 1.0×1.5 + 0.30×1.0 + 0.40×0.6 + 0.15×0.8
    //     = 1.5 + 0.30 + 0.24 + 0.12 = 2.16
    // sigmoid(2.16) ≈ 0.897
    assert!(
        out.p_yes > 0.85,
        "strong bullish BTC state should produce high p_yes; got {}",
        out.p_yes
    );
}

/// Model and poly being CLOSE is the calibration we care about — but
/// it's measured externally, not enforced as an input. This test
/// simulates a tick where BTC features yield a probability near poly's
/// observed mid (which lives only in the bot's calibration tracker).
#[test]
fn model_close_to_poly_demonstrates_calibration() {
    let cfg = ScoringConfig::default();
    // Suppose poly is trading at 0.55 (YES slight favorite) and BTC
    // signals are mildly bullish. A calibrated model should produce
    // ~0.55 ± a few percent.
    let features = Features {
        btc_strike_distance_z: Some(0.2),
        btc_drift_30s_z: Some(0.3),
        binance_flow_imbalance_60s: Some(0.1),
        btc_momentum: Some(0.0),
        ..Default::default()
    };
    let out = score(&features, Regime::Mid, &cfg).expect("scored");
    // raw = 1.0×0.2 + 0.30×0.3 + 0.40×0.1 + 0.15×0 = 0.33
    // sigmoid(0.33) ≈ 0.582
    let imagined_poly_mid = 0.55;
    let gap = (out.p_yes - imagined_poly_mid).abs();
    assert!(
        gap < 0.05,
        "modestly bullish BTC features should produce p_yes close to poly's view (poly~0.55, fv={}, gap={gap})",
        out.p_yes
    );
}

/// Bad weights → wild model output → calibration gap explodes. This
/// is exactly the "60-vs-30" scenario: poly says 30c, model says ~95c,
/// which the divergence DQ gate catches before any trade goes out.
#[test]
fn pathological_weights_blow_up_the_gap_demonstrating_dq_gate_need() {
    let mut cfg = ScoringConfig::default();
    cfg.mid.w_btc_drift_30s_z = 5.0; // pathological weight
    let features = Features {
        btc_strike_distance_z: Some(0.0),
        btc_drift_30s_z: Some(1.0),
        ..Default::default()
    };
    let out = score(&features, Regime::Mid, &cfg).expect("scored");
    // raw ≈ 5.0 → sigmoid(5.0) ≈ 0.993
    assert!(out.p_yes > 0.95);
    // If poly is at 0.30 (the user's example), gap is 0.99 - 0.30 ≈ 0.69.
    let imagined_poly_mid = 0.30;
    let gap = (out.p_yes - imagined_poly_mid).abs();
    assert!(gap > 0.60, "this is the 'fv=99 poly=30' broken-model scenario");
    // The DQ gate with default `max_fv_divergence_pp = 0.10` would
    // refuse to fire on this.
}

