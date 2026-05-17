//! Bot orchestration loop.
//!
//! Owns the strategy/risk core's mutable state. Consumes a single
//! `mpsc::Receiver<BotEvent>` fed by the feed tasks (Binance WS,
//! Polymarket book poller, Gamma discovery). Emits paper fills via the
//! `PaperExecutor` and structured logs.

use std::time::Duration;

use market_registry::{Market, MarketId, Outcome};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use crate::config::BotConfig;
use crate::execution::PaperExecutor;
use crate::feeds;
use crate::fv::{compute_fv, VolEstimator};
use crate::market_state::{ActiveMarket, BtcHistory};
use crate::position::PositionStore;
use crate::risk::{RejectReason, RiskDecision, RiskEngine};
use crate::strategy::{decide, DecisionInputs};

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/// All inputs the bot reacts to. Feeds push these; the bot loop pulls them.
/// Internal channel message — not serialised to disk.
#[derive(Debug, Clone)]
pub enum BotEvent {
    /// A Binance bookTicker mid update.
    BtcTick {
        /// Local receive timestamp, ns since epoch.
        t_ns: u128,
        mid_usd: f64,
    },
    /// A Polymarket CLOB book snapshot for the currently-active market.
    PolyBook {
        t_ns: u128,
        market_id: MarketId,
        yes_mid: f64,
    },
    /// Gamma discovery has selected a (possibly new) active market.
    MarketChanged { market: Market },
}

// ---------------------------------------------------------------------------
// Real paper-mode entry
// ---------------------------------------------------------------------------

/// Run the bot in paper mode against live feeds. Spawns the Binance and
/// Polymarket tasks, then drives the strategy loop.
pub async fn run_paper(cfg: BotConfig) {
    info!(?cfg, "starting bot — live paper mode");

    let (events_tx, mut events_rx) = mpsc::channel::<BotEvent>(1024);
    let (active_market_tx, active_market_rx) = watch::channel::<Option<Market>>(None);

    // Binance: BTC mid ticks.
    let binance_cfg = cfg.feeds.binance.clone();
    let binance_tx = events_tx.clone();
    tokio::spawn(async move {
        feeds::binance::run(binance_cfg, binance_tx).await;
    });

    // Polymarket: market discovery.
    let gamma_cfg = cfg.feeds.gamma.clone();
    let disc_tx = events_tx.clone();
    let disc_active = active_market_tx.clone();
    tokio::spawn(async move {
        feeds::polymarket::run_market_discovery(gamma_cfg, disc_tx, disc_active).await;
    });

    // Polymarket: book poller. Reads active market from watch, emits PolyBook.
    let poly_cfg = cfg.feeds.polymarket.clone();
    let book_tx = events_tx.clone();
    let book_active = active_market_rx.clone();
    tokio::spawn(async move {
        feeds::polymarket::run_book_poller(poly_cfg, book_active, book_tx).await;
    });

    drop(events_tx); // close the original; feeds keep their clones

    // BTC history — enough to span more than one 5m market so we can
    // capture a strike on rollover even if the previous market was just
    // starting when we connected.
    let mut btc_history = BtcHistory::new(15.0 * 60.0);
    let mut vol = VolEstimator::new(cfg.strategy.vol_window_secs);
    let mut positions = PositionStore::new();
    let mut risk = RiskEngine::new();
    let mut executor = PaperExecutor::new();
    let mut active: Option<ActiveMarket> = None;

    // Periodic status line so a quiet feed is distinguishable from a stuck bot.
    let mut status_ticker = tokio::time::interval(Duration::from_secs(30));
    status_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    status_ticker.tick().await; // skip immediate

    loop {
        tokio::select! {
            ev = events_rx.recv() => {
                match ev {
                    Some(ev) => handle_event(
                        ev,
                        &cfg,
                        &mut btc_history,
                        &mut vol,
                        &mut active,
                        &mut positions,
                        &mut risk,
                        &mut executor,
                    ),
                    None => {
                        warn!("event channel closed; exiting bot loop");
                        break;
                    }
                }
            }
            _ = status_ticker.tick() => {
                emit_status(&active, &btc_history, &vol, &positions, &executor);
            }
        }
    }
}

fn handle_event(
    ev: BotEvent,
    cfg: &BotConfig,
    btc_history: &mut BtcHistory,
    vol: &mut VolEstimator,
    active: &mut Option<ActiveMarket>,
    positions: &mut PositionStore,
    risk: &mut RiskEngine,
    executor: &mut PaperExecutor,
) {
    match ev {
        BotEvent::BtcTick { t_ns: _, mid_usd } => {
            let now_s = now_epoch_secs_f64();
            btc_history.observe(now_s, mid_usd);
            vol.observe(now_s, mid_usd);

            // If we have an active market but no strike yet (because its
            // effective start was before history was full), try again —
            // maybe the buffer has grown to cover it by now.
            if let Some(am) = active.as_mut() {
                if am.strike.is_none() {
                    am.strike = btc_history.at_time(am.effective_start_epoch() as f64);
                }
            }
        }
        BotEvent::PolyBook {
            t_ns: _,
            market_id,
            yes_mid,
        } => {
            // Only honour book events for the current active market —
            // late-arriving messages from a previous market are dropped.
            let now_s = now_epoch_secs_f64();
            if let Some(am) = active.as_mut() {
                if am.market.id == market_id {
                    am.last_poly_yes_mid = Some(yes_mid);
                    try_fire(am, cfg, btc_history, vol, positions, risk, executor, now_s);
                }
            }
        }
        BotEvent::MarketChanged { market } => {
            info!(
                event = "active_market_changed",
                market_id = %market.id,
                slug = %market.slug,
                end_epoch = market.end_time_epoch,
                "switching active market"
            );
            *active = Some(ActiveMarket::new(market, btc_history));
            if let Some(am) = active.as_ref() {
                if am.strike.is_some() {
                    info!(
                        event = "strike_snapped",
                        market_id = %am.market.id,
                        strike = am.strike,
                        "strike captured from btc history"
                    );
                } else {
                    warn!(
                        event = "strike_missing",
                        market_id = %am.market.id,
                        effective_start_epoch = am.effective_start_epoch(),
                        history_len = btc_history.len(),
                        "no btc history at market open — will not trade this market until next change"
                    );
                }
            }
        }
    }
}

fn try_fire(
    am: &ActiveMarket,
    cfg: &BotConfig,
    _btc_history: &BtcHistory,
    vol: &VolEstimator,
    positions: &mut PositionStore,
    risk: &mut RiskEngine,
    executor: &mut PaperExecutor,
    now_s: f64,
) {
    let strike = match am.strike {
        Some(s) => s,
        None => return,
    };
    let yes_mid = match am.last_poly_yes_mid {
        Some(m) => m,
        None => return,
    };
    let (latest_t, latest_btc) = match _btc_history.latest() {
        Some(x) => x,
        None => return,
    };
    let _ = latest_t;
    let ttr = am.ttr_secs(now_s);
    if ttr <= 0.0 {
        return;
    }
    // Treat σ ≈ 0 as "no estimate" and use the fallback. Binance
    // bookTicker only emits on best-bid/ask change, so a perfectly flat
    // BTC over the recent window legitimately produces zero log-return
    // variance — but feeding σ=0 into compute_fv would hit its degenerate
    // branch (FV snaps to 0/0.5/1 based on BTC vs strike comparison),
    // generating spurious high-confidence signals.
    let sigma = vol
        .sigma_per_sec()
        .filter(|s| s.is_finite() && *s > 1e-10)
        .unwrap_or(cfg.strategy.fallback_sigma_per_sec);
    let fv = compute_fv(latest_btc, strike, ttr.max(1.0), sigma);

    let market_id = am.market.id.clone();
    let signal = decide(
        DecisionInputs {
            market_id: &market_id,
            fair_value: fv,
            poly_yes_mid: yes_mid,
            ttr_secs: ttr,
            max_per_trade_usd: cfg.risk.max_per_trade_usd,
        },
        &cfg.strategy,
    );
    let signal = match signal {
        Some(s) => s,
        None => return,
    };

    let mark = positions.get(&market_id).map(|pos| match pos.side {
        Outcome::Yes => yes_mid,
        Outcome::No => 1.0 - yes_mid,
    });
    match risk.evaluate(signal.clone(), positions, &cfg.risk, mark, now_s) {
        RiskDecision::Approve(approved) => {
            let fill = executor.submit(approved.clone(), positions);
            risk.record_fill(approved.market_id.clone(), now_s);
            info!(
                event = "paper_fill",
                market_id = %approved.market_id,
                slug = %am.market.slug,
                side = ?approved.side,
                size_usd = format!("{:.4}", approved.size_usd),
                price = format!("{:.4}", fill.fill_price),
                btc = format!("{:.2}", latest_btc),
                strike = format!("{:.2}", strike),
                ttr_secs = format!("{:.1}", ttr),
                sigma_per_sec = format!("{:.6}", sigma),
                fv_yes = format!("{:.4}", fv.p_yes),
                poly_yes = format!("{:.4}", yes_mid),
                edge = format!("{:.4}", approved.edge),
                "paper fill"
            );
        }
        RiskDecision::Reject(reason) => {
            if matches!(reason, RejectReason::Cooldown | RejectReason::NotionalCapReached) {
                tracing::debug!(?reason, "signal rejected by risk");
            } else {
                warn!(?reason, "signal rejected by risk");
            }
        }
    }
}

fn emit_status(
    active: &Option<ActiveMarket>,
    btc_history: &BtcHistory,
    vol: &VolEstimator,
    positions: &PositionStore,
    executor: &PaperExecutor,
) {
    let latest_btc = btc_history.latest().map(|(_, m)| m);
    let sigma = vol.sigma_per_sec();
    let (market_id, slug, ttr, strike, poly_yes) = match active {
        Some(am) => (
            Some(am.market.id.as_str().to_string()),
            Some(am.market.slug.clone()),
            Some(am.ttr_secs(now_epoch_secs_f64())),
            am.strike,
            am.last_poly_yes_mid,
        ),
        None => (None, None, None, None, None),
    };
    info!(
        event = "status",
        market_id = ?market_id,
        slug = ?slug,
        ttr_secs = ?ttr,
        strike = ?strike,
        latest_btc = ?latest_btc,
        sigma_per_sec = ?sigma,
        btc_history_len = btc_history.len(),
        vol_samples = vol.len(),
        poly_yes_mid = ?poly_yes,
        open_positions = positions.open_count(),
        total_fills = executor.fill_count(),
        total_realised_pnl_usd = format!("{:.4}", positions.total_realised()),
        "status"
    );
}

fn now_epoch_secs_f64() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

// ---------------------------------------------------------------------------
// Synthetic-feed demo (preserved from v0; kept for smoke-testing the core
// without needing live feeds).
// ---------------------------------------------------------------------------

/// Synthetic-feed demo: simulates one 5-minute Polymarket BTC up/down
/// market with a deterministic divergence between BTC-implied FV and the
/// Polymarket mid. Demonstrates the full strategy → risk → paper exec
/// pipeline end-to-end without needing real feeds. Invoked from the
/// `demo` binary.
pub async fn run_synthetic_demo(cfg: BotConfig) {
    let market_id = MarketId::new("DEMO-btc-updown-5m");
    let strike = 100_000.0_f64;
    let market_duration_secs = 300.0_f64;

    let mut vol = VolEstimator::new(cfg.strategy.vol_window_secs);
    let mut positions = PositionStore::new();
    let mut risk = RiskEngine::new();
    let mut executor = PaperExecutor::new();

    let mut t = 0.0_f64;
    let dt = 0.25_f64;
    let total_steps = (market_duration_secs / dt) as usize;

    let mut btc = strike;
    let mut poly_yes_mid = 0.50_f64;
    let mut last_poly_update_t = 0.0_f64;

    for step in 0..total_steps {
        let drift_per_dt = if t < 120.0 { 3.0 / 40.0 } else { 0.0 };
        let zig = if step % 2 == 0 { 1.0 } else { -1.0 };
        btc += drift_per_dt + zig * 0.5;
        vol.observe(t, btc);

        let ttr = (market_duration_secs - t).max(0.0);
        let sigma = vol
            .sigma_per_sec()
            .filter(|s| s.is_finite() && *s > 1e-10)
            .unwrap_or(cfg.strategy.fallback_sigma_per_sec);
        let fv = compute_fv(btc, strike, ttr.max(1.0), sigma);

        if t - last_poly_update_t >= 3.0 {
            poly_yes_mid = 0.5 * poly_yes_mid + 0.5 * (fv.p_yes - 0.04);
            poly_yes_mid = poly_yes_mid.clamp(0.01, 0.99);
            last_poly_update_t = t;
        }

        if let Some(sig) = decide(
            DecisionInputs {
                market_id: &market_id,
                fair_value: fv,
                poly_yes_mid,
                ttr_secs: ttr,
                max_per_trade_usd: cfg.risk.max_per_trade_usd,
            },
            &cfg.strategy,
        ) {
            let mark = positions.get(&market_id).map(|pos| match pos.side {
                Outcome::Yes => poly_yes_mid,
                Outcome::No => 1.0 - poly_yes_mid,
            });
            match risk.evaluate(sig.clone(), &positions, &cfg.risk, mark, t) {
                RiskDecision::Approve(approved) => {
                    let fill = executor.submit(approved.clone(), &mut positions);
                    risk.record_fill(approved.market_id.clone(), t);
                    info!(
                        event = "paper_fill",
                        t = format!("{:.1}", t),
                        market_id = %approved.market_id,
                        side = ?approved.side,
                        size_usd = format!("{:.4}", approved.size_usd),
                        price = format!("{:.4}", fill.fill_price),
                        fv_yes = format!("{:.4}", fv.p_yes),
                        poly_yes = format!("{:.4}", poly_yes_mid),
                        edge = format!("{:.4}", approved.edge),
                        "paper fill"
                    );
                }
                RiskDecision::Reject(reason) => {
                    if matches!(reason, RejectReason::Cooldown | RejectReason::NotionalCapReached) {
                        tracing::debug!(?reason, "signal rejected by risk");
                    } else {
                        warn!(?reason, "signal rejected by risk");
                    }
                }
            }
        }

        t += dt;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let winner = if btc > strike { Outcome::Yes } else { Outcome::No };
    if let Some(pnl) = positions.settle_resolution(&market_id, winner) {
        info!(
            event = "market_resolved",
            winner = ?winner,
            realised_pnl_usd = format!("{:.4}", pnl),
            total_fills = executor.fill_count(),
            "market resolved"
        );
    }
    info!(
        total_realised_pnl_usd = format!("{:.4}", positions.total_realised()),
        "demo complete"
    );
}
