//! Bot orchestration loop.
//!
//! Owns the strategy/risk core's mutable state. Consumes a single
//! `mpsc::Receiver<BotEvent>` fed by the feed tasks (Binance WS,
//! Polymarket book poller, Gamma discovery). Emits paper fills via
//! `PaperExecutor`, structured tracing logs for humans, and one
//! `DecisionRecord` per evaluation tick into `decisions.ndjson` for
//! durable audit.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use market_registry::{Market, MarketId, Outcome};
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex};
use tracing::{info, warn};

use crate::config::BotConfig;
use crate::decision_log::{
    make_session_id, reject_reason_to_str, time_bucket_for, write_session_meta, DecisionKind,
    DecisionLogger, DecisionRecord, IncompleteReason, ResolutionLogger, ResolutionRecord,
    SessionMeta, BOT_VERSION, SCHEMA_VERSION,
};
use crate::execution::PaperExecutor;
use crate::feeds;
use crate::fv::{compute_fv, implied_strike, VolEstimator};
use crate::market_state::{ActiveMarket, BtcHistory, PolyBookSnapshot};
use crate::position::PositionStore;
use crate::risk::{RiskDecision, RiskEngine};
use crate::signals::scoring::{self, Features, Regime};
use crate::strategy::{decide, taker_required_edge, DecisionInputs, StrategyOutcome};

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
    /// A Binance executed trade — drives volume + flow features.
    BinanceTrade {
        t_ns: u128,
        price_usd: f64,
        qty: f64,
        /// True when the buyer was the taker (aggressive buy, hit ask).
        buyer_is_taker: bool,
    },
    /// A Polymarket CLOB book snapshot for the currently-active market.
    /// Carries full TOB for both YES and NO sides so the bot can log
    /// the full state, not just the YES mid.
    PolyBook {
        t_ns: u128,
        market_id: MarketId,
        snapshot: PolyBookSnapshot,
    },
    /// Gamma discovery has selected a (possibly new) active market.
    MarketChanged { market: Market },
    /// Gamma resolution sweep observed a market settling.
    MarketResolved {
        market_id: MarketId,
        market_slug: String,
        end_epoch: i64,
        outcome: Outcome,
    },
}

// ---------------------------------------------------------------------------
// Real paper-mode entry
// ---------------------------------------------------------------------------

/// Bot session paths and identifiers.
struct SessionPaths {
    session_id: String,
    session_dir: PathBuf,
    decisions_path: PathBuf,
    resolutions_path: PathBuf,
    meta_path: PathBuf,
}

fn build_session_paths(now_secs: i64) -> SessionPaths {
    let session_id = make_session_id(now_secs);
    let session_dir = PathBuf::from("data").join(&session_id);
    let decisions_path = session_dir.join("decisions.ndjson");
    let resolutions_path = session_dir.join("resolutions.ndjson");
    let meta_path = session_dir.join("_session_meta.json");
    SessionPaths {
        session_id,
        session_dir,
        decisions_path,
        resolutions_path,
        meta_path,
    }
}

/// Run the bot in paper mode against live feeds. Spawns the Binance and
/// Polymarket tasks, then drives the strategy loop.
pub async fn run_paper(cfg: BotConfig) {
    run_bot(cfg, None).await
}

/// Run the bot against the real Polymarket CLOB. Requires both
/// `LiveCredentials` (EOA + proxy) and `client::ApiCredentials` (HMAC)
/// in the environment.
///
/// Mostly the same orchestration as `run_paper`, plus:
/// * Constructs a shared [`crate::live::LiveExecutor`] and threads it
///   into `BotState` via `set_live_mode`. The strategy fire path then
///   submits real signed orders instead of paper fills.
/// * Spawns a reconciliation task that polls open orders every 5
///   seconds and applies the diff back to the executor's local view.
///
/// Settlement of resolved positions still flows through the existing
/// resolution sweeper + paper-style P&L recording. Real on-chain
/// settlement verification is a v2 task — see `live.rs` module docs.
pub async fn run_live(cfg: BotConfig) -> Result<(), crate::live::LiveExecError> {
    let live = crate::live::LiveExecutor::new(
        crate::live::LiveCredentials::from_env(),
        crate::live::client::ApiCredentials::from_env(),
        cfg.feeds.polymarket.clob_url.clone(),
    )?;
    info!(
        signer = %live.signer_address_hex(),
        proxy = %live.proxy_wallet_address(),
        "live executor initialised"
    );
    let live = Arc::new(AsyncMutex::new(live));

    // Reconciliation task — runs forever (or until the executor is
    // dropped). Locks the executor briefly to fetch + apply diffs;
    // submits in the main loop must wait while this runs.
    let recon = live.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(5));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // skip immediate
        loop {
            ticker.tick().await;
            // Snapshot local view under the lock.
            let local_orders = {
                let guard = recon.lock().await;
                guard.open_orders()
            };
            if local_orders.is_empty() {
                continue;
            }
            // Fetch venue view (holds the lock for the network round-trip).
            let venue_orders = {
                let guard = recon.lock().await;
                match guard.fetch_open_orders_from_venue().await {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(error = %e, "reconcile: fetch_open_orders failed");
                        continue;
                    }
                }
            };
            // Pure diff outside the lock.
            let diff = crate::live::reconcile(&local_orders, &venue_orders);
            if !diff.updates.is_empty() {
                let mut guard = recon.lock().await;
                let updates = diff.updates.len();
                guard.apply_diff(diff);
                info!(event = "reconcile_applied", updates, "reconciler applied diff");
            }
        }
    });

    run_bot(cfg, Some(live)).await;
    Ok(())
}

/// Inner orchestrator shared by both modes. `live_executor` is `Some`
/// in live mode; the main fire path then awaits it instead of using
/// the in-memory paper executor.
async fn run_bot(
    cfg: BotConfig,
    live_executor: Option<Arc<AsyncMutex<crate::live::LiveExecutor>>>,
) {
    let now_s_int = now_epoch_secs_i64();
    let paths = build_session_paths(now_s_int);
    info!(
        session_id = %paths.session_id,
        session_dir = %paths.session_dir.display(),
        "starting bot — live paper mode"
    );

    // Provenance sidecar — written once, captures the full config.
    let meta = SessionMeta {
        schema_version: SCHEMA_VERSION,
        session_id: paths.session_id.clone(),
        bot_version: BOT_VERSION.into(),
        started_at_epoch_ns: common::LocalTimestamp::now().as_nanos().to_string(),
        started_at_iso: crate::decision_log::epoch_secs_to_compact_iso(now_s_int),
        config: cfg.clone(),
    };
    if let Err(e) = write_session_meta(&paths.meta_path, &meta) {
        warn!(error = %e, path = %paths.meta_path.display(), "failed to write session meta");
    }

    // Decision log — created/opened up front so we know it works before
    // we start consuming events.
    let logger = match DecisionLogger::open(paths.decisions_path.clone()) {
        Ok(l) => Some(l),
        Err(e) => {
            warn!(error = %e, path = %paths.decisions_path.display(), "failed to open decision log; continuing without it");
            None
        }
    };
    if let Some(l) = &logger {
        info!(path = %l.path().display(), "decision log open");
    }
    let resolution_logger = match ResolutionLogger::open(paths.resolutions_path.clone()) {
        Ok(l) => Some(l),
        Err(e) => {
            warn!(error = %e, path = %paths.resolutions_path.display(), "failed to open resolution log; continuing without it");
            None
        }
    };
    if let Some(l) = &resolution_logger {
        info!(path = %l.path().display(), "resolution log open");
    }

    let (events_tx, mut events_rx) = mpsc::channel::<BotEvent>(1024);
    let (active_market_tx, active_market_rx) = watch::channel::<Option<Market>>(None);

    // Binance
    let binance_cfg = cfg.feeds.binance.clone();
    let binance_tx = events_tx.clone();
    tokio::spawn(async move {
        feeds::binance::run(binance_cfg, binance_tx).await;
    });

    // Polymarket: market discovery
    let gamma_cfg = cfg.feeds.gamma.clone();
    let disc_tx = events_tx.clone();
    let disc_active = active_market_tx.clone();
    tokio::spawn(async move {
        feeds::polymarket::run_market_discovery(gamma_cfg, disc_tx, disc_active).await;
    });

    // Polymarket: book poller (YES + NO in parallel)
    let poly_cfg = cfg.feeds.polymarket.clone();
    let book_tx = events_tx.clone();
    let book_active = active_market_rx.clone();
    tokio::spawn(async move {
        feeds::polymarket::run_book_poller(poly_cfg, book_active, book_tx).await;
    });

    // Binance: trade feed — drives volume + flow features.
    let trade_cfg = cfg.feeds.binance.clone();
    let trade_tx = events_tx.clone();
    tokio::spawn(async move {
        feeds::binance::run_trades(trade_cfg, trade_tx).await;
    });

    // Polymarket: resolution sweeper (Gamma ?closed=true poll).
    let resolution_gamma_cfg = cfg.feeds.gamma.clone();
    let resolution_tx = events_tx.clone();
    tokio::spawn(async move {
        // Polling cadence: 30s. Resolutions land within a few seconds of
        // market end, but we don't need second-level latency for the
        // settlement log — the data is post-hoc.
        feeds::polymarket::run_resolution_sweeper(resolution_gamma_cfg, resolution_tx, 30).await;
    });

    drop(events_tx);

    // State persistence — restore bankroll from stable path if a prior
    // run wrote one. Position state is NOT restored (paper v1 — see
    // module docs on `state_persist`); live mode will reconcile via
    // Phase 7 CLOB queries instead.
    let state_path = PathBuf::from("data").join("bot_state.ndjson");
    let restored_bankroll = match crate::state_persist::load_latest(&state_path) {
        Ok(Some(snap)) => {
            info!(
                event = "state_restored",
                bankroll_usd = snap.bankroll_usd,
                saved_at_ns = %snap.saved_at_local_ts_ns,
                prior_session = %snap.session_id,
                "restoring bankroll from previous session"
            );
            Some(snap.bankroll_usd)
        }
        Ok(None) => {
            info!("no prior state snapshot — starting fresh");
            None
        }
        Err(e) => {
            warn!(error = %e, path = %state_path.display(), "failed to load state; starting fresh");
            None
        }
    };
    let initial_bankroll = restored_bankroll.unwrap_or(cfg.risk.bankroll_initial_usd);
    let state_persister = match crate::state_persist::StatePersister::open(state_path.clone()) {
        Ok(sp) => Some(sp),
        Err(e) => {
            warn!(error = %e, path = %state_path.display(), "failed to open state persister; continuing without persistence");
            None
        }
    };

    // Metrics: registry is shared between bot + server. Server runs as
    // an independent task so a slow scrape can never block the FV loop.
    let metrics = crate::metrics::MetricsRegistry::new();
    let metrics_addr = cfg.metrics.listen_addr.clone();
    if !metrics_addr.is_empty() {
        let metrics_for_server = metrics.clone();
        tokio::spawn(async move {
            if let Err(e) = crate::metrics::run_server(&metrics_addr, metrics_for_server).await {
                warn!(error = %e, "metrics server task ended");
            }
        });
    } else {
        info!("metrics listen_addr is empty; metrics server disabled");
    }

    let mut state = BotState::new(
        cfg.clone(),
        paths.session_id.clone(),
        logger,
        resolution_logger,
        state_persister,
        initial_bankroll,
        metrics,
    );
    if let Some(live) = live_executor.as_ref() {
        state.set_live_mode(live.clone());
        info!("bot state switched to live mode — strategy fires submit to CLOB");
    }

    let mut status_ticker = tokio::time::interval(Duration::from_secs(30));
    status_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    status_ticker.tick().await;

    // FV engine ticker — drives strategy evaluation independently of
    // feed events, so the bot reacts to BTC moves between poly book
    // polls. Cadence in StrategyConfig.fv_tick_ms (default 100ms).
    let fv_tick_ms = cfg.strategy.fv_tick_ms.max(10);
    let mut fv_ticker = tokio::time::interval(Duration::from_millis(fv_tick_ms));
    fv_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    fv_ticker.tick().await; // skip immediate

    loop {
        tokio::select! {
            ev = events_rx.recv() => {
                match ev {
                    Some(ev) => state.handle_event(ev),
                    None => {
                        warn!("event channel closed; exiting bot loop");
                        break;
                    }
                }
            }
            _ = fv_ticker.tick() => {
                state.evaluate_and_log(now_epoch_secs_f64()).await;
            }
            _ = status_ticker.tick() => {
                state.emit_status();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// BotState — owns all mutable state
// ---------------------------------------------------------------------------

/// Multi-window realized-vol estimators. `vol` (the strategy's σ source)
/// uses the configured window; the others are diagnostic — written into
/// every decision record so we can characterise vol-regime offline.
struct VolEstimators {
    primary: VolEstimator,
    w5: VolEstimator,
    w30: VolEstimator,
    w60: VolEstimator,
    w300: VolEstimator,
}

impl VolEstimators {
    fn new(primary_window_secs: f64) -> Self {
        Self {
            primary: VolEstimator::new(primary_window_secs),
            w5: VolEstimator::new(5.0),
            w30: VolEstimator::new(30.0),
            w60: VolEstimator::new(60.0),
            w300: VolEstimator::new(300.0),
        }
    }

    fn observe(&mut self, t: f64, mid: f64) {
        self.primary.observe(t, mid);
        self.w5.observe(t, mid);
        self.w30.observe(t, mid);
        self.w60.observe(t, mid);
        self.w300.observe(t, mid);
    }
}

struct BotState {
    cfg: BotConfig,
    session_id: String,
    logger: Option<DecisionLogger>,
    resolution_logger: Option<ResolutionLogger>,

    btc_history: BtcHistory,
    vol: VolEstimators,
    positions: PositionStore,
    risk: RiskEngine,
    executor: PaperExecutor,
    active: Option<ActiveMarket>,

    /// Tracked USDC bankroll. Initialised from `RiskConfig::bankroll_initial_usd`;
    /// mutated on every resolution (paper) by `settled_pnl_usd` and on
    /// every fill by the fill's cost basis (since we hold non-cash
    /// shares between fill and settle).
    bankroll_usd: f64,

    /// Markets the bot has evaluated at least once (i.e. had as `active`
    /// long enough to write a decision). Used to filter resolution-sweep
    /// events down to "ours" — we don't log resolutions for the ~12 BTC
    /// 5m markets per hour we never traded on.
    seen_markets: HashSet<MarketId>,
    /// Markets we've already logged a resolution for. Prevents
    /// double-settlement if the sweeper re-emits.
    resolved_markets: HashSet<MarketId>,

    /// Local-clock ns of the most recent BtcTick — for the freshness flag
    /// in the decision log.
    last_btc_tick_ns: Option<u128>,

    /// Optional persister writes a state snapshot on every mutation
    /// (fill / settle). Reloaded on next boot for bankroll continuity.
    state_persister: Option<crate::state_persist::StatePersister>,

    /// Rolling 60s window of Binance trade qty (BTC). Sum = volume.
    binance_volume_60s_stats: crate::stats::RollingStats,
    /// Rolling 60s window of signed Binance trade qty (positive when
    /// buyer is taker). Sum = signed flow imbalance.
    binance_flow_60s_stats: crate::stats::RollingStats,
    /// Cross-market rolling distribution of observed YES spreads.
    /// Powers the adaptive `yes_spread_z` feature.
    yes_spread_stats: crate::stats::RollingStats,

    /// Rolling distribution of `(fv_p_yes − poly_mid)` over the last
    /// 5 minutes. Drives the model-calibration metric exposed at
    /// `/metrics`. The DQ gate checks the *instantaneous* gap; the
    /// rolling stats give the operator a live read on average drift.
    fv_poly_gap_stats: crate::stats::RollingStats,

    /// Prometheus-style metrics registry. Updated each tick; scraped by
    /// the metrics server task. Cloning is cheap (Arc).
    metrics: crate::metrics::MetricsRegistry,

    /// Execution mode. `Paper` uses the synchronous in-memory
    /// `PaperExecutor`; `Live` awaits the shared `LiveExecutor` and
    /// reads per-market signing context from `live_contexts`.
    mode: BotMode,
    /// EIP-712 signing context per market — populated when a market
    /// becomes active (token IDs + neg-risk flag + fee rate). Only
    /// consulted in live mode.
    live_contexts: HashMap<MarketId, crate::live::MarketContext>,
}

/// Selects which executor `evaluate_and_log` drives.
#[derive(Clone)]
pub enum BotMode {
    /// Synchronous paper fills at the observed mid. No network.
    Paper,
    /// Real signed orders against the Polymarket CLOB. Shared with the
    /// reconciliation task — wrap in `Arc<AsyncMutex>` to allow both
    /// the main loop and the recon task to call into it.
    Live(Arc<AsyncMutex<crate::live::LiveExecutor>>),
}

impl BotState {
    fn new(
        cfg: BotConfig,
        session_id: String,
        logger: Option<DecisionLogger>,
        resolution_logger: Option<ResolutionLogger>,
        state_persister: Option<crate::state_persist::StatePersister>,
        initial_bankroll: f64,
        metrics: crate::metrics::MetricsRegistry,
    ) -> Self {
        let vol_window = cfg.strategy.vol_window_secs;
        let _ = &cfg.risk.bankroll_initial_usd; // documented: initial_bankroll wins
        Self {
            cfg,
            session_id,
            logger,
            resolution_logger,
            // 15 min of BTC history — covers ~3 full 5m market windows so
            // a strike can usually be snapped even if we just started.
            btc_history: BtcHistory::new(15.0 * 60.0),
            vol: VolEstimators::new(vol_window),
            positions: PositionStore::new(),
            risk: RiskEngine::new(),
            executor: PaperExecutor::new(),
            active: None,
            bankroll_usd: initial_bankroll,
            seen_markets: HashSet::new(),
            resolved_markets: HashSet::new(),
            last_btc_tick_ns: None,
            state_persister,
            binance_volume_60s_stats: crate::stats::RollingStats::with_min_samples(60.0, 1),
            binance_flow_60s_stats: crate::stats::RollingStats::with_min_samples(60.0, 1),
            // 600s = 10 min rolling distribution of YES spreads.
            yes_spread_stats: crate::stats::RollingStats::with_min_samples(600.0, 20),
            // 5-min window of (fv − poly) gaps. min_samples=10 → don't
            // expose a calibration number until we have at least 10
            // ticks of data (~1 sec at 100ms cadence).
            fv_poly_gap_stats: crate::stats::RollingStats::with_min_samples(300.0, 10),
            metrics,
            mode: BotMode::Paper,
            live_contexts: HashMap::new(),
        }
    }

    /// Swap the executor into live mode. After this returns, every
    /// strategy fire routes through the shared `LiveExecutor` instead
    /// of the in-memory paper executor.
    fn set_live_mode(&mut self, live: Arc<AsyncMutex<crate::live::LiveExecutor>>) {
        self.mode = BotMode::Live(live);
    }

    /// Sync all gauge values into the metrics registry. Counters are
    /// updated incrementally elsewhere (see `MetricsRegistry::record_decision`).
    /// Called at the bottom of each FV tick + once on each status emit.
    fn refresh_metrics(&self) {
        let realised_pnl_map = self
            .positions
            .realised_pnl_map()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), *v))
            .collect();
        let latest_btc = self.btc_history.latest().map(|(_, m)| m);
        let sigma = self.vol.primary.sigma_per_sec();
        let (strike, ttr) = match &self.active {
            Some(am) => (am.strike, Some(am.ttr_secs(now_epoch_secs_f64()))),
            None => (None, None),
        };
        let gap_mean = self.fv_poly_gap_stats.mean();
        let gap_stdev = self.fv_poly_gap_stats.stdev();
        self.metrics.update_with(|s| {
            s.bankroll_usd = self.bankroll_usd;
            s.total_realised_pnl_usd = self.positions.total_realised();
            s.open_positions = self.positions.open_count();
            s.total_fills = self.executor.fill_count() as u64;
            s.seen_markets = self.seen_markets.len();
            s.resolved_markets = self.resolved_markets.len();
            s.btc_history_len = self.btc_history.len();
            s.vol_samples = self.vol.primary.len();
            s.kill_switch_tripped = self.risk.is_kill_switch_tripped();
            s.latest_btc_mid_usd = latest_btc;
            s.sigma_per_sec = sigma;
            s.latest_strike_usd = strike;
            s.latest_ttr_secs = ttr;
            s.realised_pnl_per_market_usd = realised_pnl_map;
            s.fv_poly_gap_mean_pp = gap_mean;
            s.fv_poly_gap_stdev_pp = gap_stdev;
            // decisions_*_total are written by record_decision elsewhere
            // and intentionally preserved across this refresh.
        });
    }

    fn handle_event(&mut self, ev: BotEvent) {
        match ev {
            BotEvent::BtcTick { t_ns, mid_usd } => self.on_btc_tick(t_ns, mid_usd),
            BotEvent::BinanceTrade {
                t_ns: _,
                price_usd: _,
                qty,
                buyer_is_taker,
            } => self.on_binance_trade(qty, buyer_is_taker),
            BotEvent::PolyBook {
                t_ns: _,
                market_id,
                snapshot,
            } => self.on_poly_book(market_id, snapshot),
            BotEvent::MarketChanged { market } => self.on_market_changed(market),
            BotEvent::MarketResolved {
                market_id,
                market_slug,
                end_epoch,
                outcome,
            } => self.on_market_resolved(market_id, market_slug, end_epoch, outcome),
        }
    }

    fn on_binance_trade(&mut self, qty: f64, buyer_is_taker: bool) {
        let now_s = now_epoch_secs_f64();
        self.binance_volume_60s_stats.observe(now_s, qty);
        let signed_qty = if buyer_is_taker { qty } else { -qty };
        self.binance_flow_60s_stats.observe(now_s, signed_qty);
    }

    fn on_btc_tick(&mut self, t_ns: u128, mid_usd: f64) {
        let now_s = now_epoch_secs_f64();
        self.btc_history.observe(now_s, mid_usd);
        self.vol.observe(now_s, mid_usd);
        self.last_btc_tick_ns = Some(t_ns);

        // Retry strike snapping if a market is active but its strike
        // wasn't yet captureable when it was selected.
        if let Some(am) = self.active.as_mut() {
            if am.strike.is_none() {
                am.strike = self.btc_history.at_time(am.effective_start_epoch() as f64);
                if am.strike.is_some() {
                    info!(
                        event = "strike_snapped_late",
                        market_id = %am.market.id,
                        strike = am.strike,
                        "strike captured on later btc tick"
                    );
                }
            }
        }
    }

    fn persist_snapshot(&mut self) {
        let Some(persister) = self.state_persister.as_mut() else {
            return;
        };
        let snap = crate::state_persist::snapshot(
            &self.session_id,
            BOT_VERSION,
            self.bankroll_usd,
            self.positions.total_realised(),
            self.positions.open_positions(),
            self.risk.cumulative_notional_map(),
            self.positions.realised_pnl_map(),
        );
        if let Err(e) = persister.write(&snap) {
            warn!(error = %e, "failed to write state snapshot");
        }
    }

    fn on_market_resolved(
        &mut self,
        market_id: MarketId,
        market_slug: String,
        end_epoch: i64,
        outcome: Outcome,
    ) {
        // Only log resolutions for markets we actually evaluated (active'd).
        if !self.seen_markets.contains(&market_id) {
            return;
        }
        if !self.resolved_markets.insert(market_id.clone()) {
            return; // already logged
        }

        // Snapshot the open position (if any) BEFORE settling.
        let position_snapshot = self.positions.get(&market_id).cloned();
        let settled_pnl = self.positions.settle_resolution(&market_id, outcome);
        // Update tracked bankroll: cost basis was reserved on each fill,
        // proceeds are returned now. Net bankroll delta = settled_pnl
        // (which already nets cost vs proceeds) + cost_basis_returned.
        // Since we deducted cost_usd on each fill from bankroll and
        // settle_resolution returns (proceeds - cost), the delta to
        // bankroll on settle is `proceeds = cost + pnl`.
        if let (Some(pos), Some(pnl)) = (position_snapshot.as_ref(), settled_pnl) {
            self.bankroll_usd += pos.cost_usd + pnl;
        }

        let position_side = position_snapshot
            .as_ref()
            .map(|p| side_to_str(p.side).to_string());
        let position_shares = position_snapshot.as_ref().map(|p| p.shares);
        let position_cost_usd = position_snapshot.as_ref().map(|p| p.cost_usd);
        let position_avg_price = position_snapshot.as_ref().map(|p| p.avg_price);
        let winning_side = position_snapshot.as_ref().map(|p| p.side == outcome);
        let settled_proceeds_usd =
            position_snapshot
                .as_ref()
                .map(|p| if p.side == outcome { p.shares } else { 0.0 });

        info!(
            event = "market_resolved",
            market_id = %market_id,
            slug = %market_slug,
            outcome = ?outcome,
            settled_pnl_usd = ?settled_pnl,
            winning_side = ?winning_side,
            "market resolved"
        );

        let rec = ResolutionRecord {
            schema_version: SCHEMA_VERSION,
            local_ts_ns: common::LocalTimestamp::now().as_nanos().to_string(),
            session_id: self.session_id.clone(),
            bot_version: BOT_VERSION.into(),
            market_id: market_id.as_str().to_string(),
            market_slug,
            end_epoch,
            resolved_outcome: side_to_str(outcome).to_string(),
            position_side,
            position_shares,
            position_cost_usd,
            position_avg_price,
            settled_proceeds_usd,
            settled_pnl_usd: settled_pnl,
            winning_side,
        };
        if let Some(logger) = self.resolution_logger.as_mut() {
            if let Err(e) = logger.write(&rec) {
                warn!(error = %e, "failed to write resolution record");
            }
        }
        self.persist_snapshot();
    }

    fn on_poly_book(&mut self, market_id: MarketId, snapshot: PolyBookSnapshot) {
        // Update aggregate spread stats from EVERY observed YES spread,
        // regardless of which market — this builds the cross-market
        // baseline used by the `yes_spread_z` feature.
        if let Some(sp) = snapshot.yes_spread() {
            self.yes_spread_stats.observe(now_epoch_secs_f64(), sp);
        }
        let Some(am) = self.active.as_mut() else {
            return;
        };
        if am.market.id != market_id {
            // Late delivery from the previous market; ignore.
            return;
        }
        am.last_poly_snapshot = Some(snapshot);
    }

    /// Build the [`Features`] snapshot for the scoring model from current
    /// state. Each field is `Some(_)` only when the underlying input is
    /// available; missing inputs degrade the model gracefully.
    fn extract_features(&self, am: &ActiveMarket, now_s: f64, sigma_used: f64) -> Features {
        let mut f = Features::default();

        let ttr = am.ttr_secs(now_s);
        let btc_now = self.btc_history.latest().map(|(_, m)| m);

        // Anchor feature: Z-score of BTC vs strike given σ × √TTR.
        if let (Some(btc), Some(k)) = (btc_now, am.strike) {
            if btc > 0.0 && k > 0.0 && sigma_used > 0.0 && ttr > 0.0 {
                let sigma_t = sigma_used * ttr.sqrt();
                if sigma_t > 0.0 {
                    // ln(S/K) / σ√T  — dominant GBM Z-score term.
                    f.btc_strike_distance_z = Some((btc / k).ln() / sigma_t);
                }
            }
        }

        // Multi-window BTC drift Z-scores.
        let drift = |window: f64, vol: &VolEstimator, hist: &BtcHistory| -> Option<f64> {
            let r = hist.log_return_over(window)?;
            let s = vol
                .sigma_per_sec()
                .filter(|s| s.is_finite() && *s > 1e-10)?;
            let sigma_t = s * window.sqrt();
            if sigma_t > 0.0 {
                Some(r / sigma_t)
            } else {
                None
            }
        };
        f.btc_drift_5s_z = drift(5.0, &self.vol.w5, &self.btc_history);
        f.btc_drift_30s_z = drift(30.0, &self.vol.w30, &self.btc_history);
        f.btc_drift_60s_z = drift(60.0, &self.vol.w60, &self.btc_history);

        // Momentum: short-window drift minus long-window drift, both in σ units.
        // Captures acceleration / short-vs-long trend divergence.
        let drift_300s_z = drift(300.0, &self.vol.w300, &self.btc_history);
        f.btc_momentum = match (f.btc_drift_30s_z, drift_300s_z) {
            (Some(s), Some(l)) => Some(s - l),
            _ => None,
        };

        // Polymarket book features.
        if let Some(snap) = am.last_poly_snapshot.as_ref() {
            // Anchor feature: poly's YES mid. Drives the log-odds
            // model — `p_yes = poly_mid` exactly under default weights.
            f.poly_mid = snap.yes_mid();
            if let (Some(bs), Some(asz)) = (snap.yes_bid_size, snap.yes_ask_size) {
                let total = bs + asz;
                if total > 0.0 {
                    f.yes_book_imbalance = Some((bs - asz) / total);
                }
            }
            if let (Some(bs), Some(asz)) = (snap.no_bid_size, snap.no_ask_size) {
                let total = bs + asz;
                if total > 0.0 {
                    f.no_book_imbalance = Some((bs - asz) / total);
                }
            }
            // Spread z-score against rolling distribution. Self-adapts as
            // typical Polymarket spreads change across regimes.
            if let Some(spread) = snap.yes_spread() {
                f.yes_spread_z = self.yes_spread_stats.z_score(spread);
            }
        }

        // Binance volume + flow over last 60s. Volume is raw BTC qty;
        // flow imbalance is already normalised in [-1, 1].
        let vol_sum = self.binance_volume_60s_stats.sum();
        if vol_sum > 0.0 {
            f.binance_volume_60s_btc = Some(vol_sum);
            let signed = self.binance_flow_60s_stats.sum();
            f.binance_flow_imbalance_60s = Some(signed / vol_sum);
        }

        // lag_yes deliberately left None in v1 — needs a separate
        // "expected poly response" estimator. Future signal addition.

        f
    }

    fn on_market_changed(&mut self, market: Market) {
        info!(
            event = "active_market_changed",
            market_id = %market.id,
            slug = %market.slug,
            end_epoch = market.end_time_epoch,
            "switching active market"
        );
        // Cache the live-signing context for this market. BTC up/down 5m
        // markets are standard (not neg-risk) and have historically had
        // zero on-chain fee — both are hardcoded for v1. A future
        // commit will fetch `/neg-risk` and `/fee-rate-bps` per market.
        let ctx = crate::live::MarketContext {
            yes_token_id: market.yes_token.as_str().to_string(),
            no_token_id: market.no_token.as_str().to_string(),
            neg_risk: false,
            fee_rate_bps: 0,
            chain_id: 137,
        };
        self.live_contexts.insert(market.id.clone(), ctx);
        let new_active = ActiveMarket::new(market, &self.btc_history);
        if let Some(strike) = new_active.strike {
            info!(
                event = "strike_snapped",
                market_id = %new_active.market.id,
                strike = strike,
                "strike captured from btc history"
            );
        } else {
            warn!(
                event = "strike_missing",
                market_id = %new_active.market.id,
                effective_start_epoch = new_active.effective_start_epoch(),
                history_len = self.btc_history.len(),
                "no btc history at market open — will not trade this market until next change"
            );
        }
        self.seen_markets.insert(new_active.market.id.clone());
        self.active = Some(new_active);
    }

    /// Compute FV, decide, evaluate risk, and emit one DecisionRecord.
    ///
    /// Async because in live mode the fill path awaits the shared
    /// `LiveExecutor` (which holds an HTTP client + mutex). Paper mode
    /// stays synchronous internally — there's no `.await` on the paper
    /// path, the `async` keyword is just for the type signature.
    async fn evaluate_and_log(&mut self, now_s: f64) {
        let Some(am) = self.active.as_ref().cloned() else {
            return;
        };

        let mut rec = base_record(&am, &self.session_id, now_s);

        // Polymarket snapshot fields.
        let snapshot = am.last_poly_snapshot.clone();
        if let Some(snap) = &snapshot {
            rec.poly_yes_bid = snap.yes_bid;
            rec.poly_yes_ask = snap.yes_ask;
            rec.poly_yes_mid = snap.yes_mid();
            rec.poly_yes_bid_size = snap.yes_bid_size;
            rec.poly_yes_ask_size = snap.yes_ask_size;
            rec.poly_yes_spread = snap.yes_spread();
            rec.poly_no_bid = snap.no_bid;
            rec.poly_no_ask = snap.no_ask;
            rec.poly_no_mid = snap.no_mid();
            rec.poly_no_bid_size = snap.no_bid_size;
            rec.poly_no_ask_size = snap.no_ask_size;
            rec.poly_no_spread = snap.no_spread();
            let now_ns = common::LocalTimestamp::now().as_nanos();
            let age_ns = now_ns.saturating_sub(snap.captured_local_ts_ns);
            rec.poly_book_age_ms = Some(age_ns as f64 / 1.0e6);
        }

        // Multi-window BTC log returns + σ. All optional — None until the
        // history buffer is wide enough.
        rec.btc_log_return_5s = self.btc_history.log_return_over(5.0);
        rec.btc_log_return_30s = self.btc_history.log_return_over(30.0);
        rec.btc_log_return_60s = self.btc_history.log_return_over(60.0);
        rec.btc_log_return_300s = self.btc_history.log_return_over(300.0);
        rec.sigma_per_sec_5s = self.vol.w5.sigma_per_sec();
        rec.sigma_per_sec_30s = self.vol.w30.sigma_per_sec();
        rec.sigma_per_sec_60s = self.vol.w60.sigma_per_sec();
        rec.sigma_per_sec_300s = self.vol.w300.sigma_per_sec();

        // Time-bucket label.
        rec.time_bucket = Some(time_bucket_for(am.ttr_secs(now_s)).to_string());

        // Binance reference.
        let (latest_btc, btc_age_ms) = match self.btc_history.latest() {
            Some((_t, mid)) => {
                let age_ms = self.last_btc_tick_ns.map(|t_ns| {
                    let now_ns = common::LocalTimestamp::now().as_nanos();
                    now_ns.saturating_sub(t_ns) as f64 / 1.0e6
                });
                (Some(mid), age_ms)
            }
            None => (None, None),
        };
        rec.binance_btc_mid_usd = latest_btc;
        rec.btc_last_update_age_ms = btc_age_ms;
        rec.binance_strike_usd = am.strike;
        rec.btc_history_len = self.btc_history.len();
        rec.vol_samples = self.vol.primary.len();

        // σ source.
        let (sigma_used, sigma_source) = match self
            .vol
            .primary
            .sigma_per_sec()
            .filter(|s| s.is_finite() && *s > 1e-10)
        {
            Some(s) => (s, "estimated"),
            None => (self.cfg.strategy.fallback_sigma_per_sec, "fallback"),
        };

        let strike = am.strike;
        let yes_mid = rec.poly_yes_mid;
        let ttr = am.ttr_secs(now_s);

        // Diagnostic: GBM fv + implied-strike still logged (independent of
        // the scoring model). Useful for comparing the old vs new path.
        if let (Some(btc), Some(k)) = (latest_btc, strike) {
            if ttr > 0.0 {
                let fv_gbm = compute_fv(btc, k, ttr.max(1.0), sigma_used);
                rec.fv_yes = Some(fv_gbm.p_yes);
                rec.fv_no = Some(fv_gbm.p_no);
                rec.sigma_per_sec_used = Some(sigma_used);
                rec.sigma_source = Some(sigma_source.to_string());
                if let Some(mid) = yes_mid {
                    rec.edge_yes = Some(fv_gbm.p_yes - mid);
                }
            }
        }
        if let (Some(btc), Some(mid)) = (latest_btc, yes_mid) {
            if ttr > 0.0 {
                rec.implied_strike_usd = implied_strike(btc, ttr.max(1.0), sigma_used, mid);
                if let (Some(impl_k), Some(k)) = (rec.implied_strike_usd, strike) {
                    rec.strike_gap_usd = Some(impl_k - k);
                    if k > 0.0 {
                        rec.strike_gap_bps = Some((impl_k - k) / k * 10_000.0);
                    }
                }
            }
        }

        // Build features + run the scoring model.
        let features = self.extract_features(&am, now_s, sigma_used);
        let regime = Regime::from_ttr_secs(ttr);
        let scoring_outcome = scoring::score(&features, regime, &self.cfg.scoring);

        if let Some(s) = scoring_outcome {
            rec.scoring_p_yes = Some(s.p_yes);
            rec.scoring_p_no = Some(s.p_no);
            rec.scoring_raw = Some(s.raw);
            rec.scoring_regime = Some(regime.as_str().to_string());
        }
        // Per-feature contribution diagnostics.
        rec.feat_btc_strike_distance_z = features.btc_strike_distance_z;
        rec.feat_btc_drift_5s_z = features.btc_drift_5s_z;
        rec.feat_btc_drift_30s_z = features.btc_drift_30s_z;
        rec.feat_btc_drift_60s_z = features.btc_drift_60s_z;
        rec.feat_yes_book_imbalance = features.yes_book_imbalance;
        rec.feat_no_book_imbalance = features.no_book_imbalance;
        rec.feat_yes_spread_z = features.yes_spread_z;
        rec.feat_btc_momentum = features.btc_momentum;
        rec.feat_binance_volume_60s_btc = features.binance_volume_60s_btc;
        rec.feat_binance_flow_imbalance_60s = features.binance_flow_imbalance_60s;

        // Calibration check: how far is our FV from poly's mid?
        // Tracked every tick (whether we fire or not) so the rolling
        // calibration metric reflects the model's true accuracy, not
        // just accuracy on tradeable ticks.
        let fv_minus_poly = match (scoring_outcome, features.poly_mid) {
            (Some(s), Some(p)) => {
                let gap = s.p_yes - p;
                self.fv_poly_gap_stats.observe(now_s, gap);
                Some(gap)
            }
            _ => None,
        };

        // Data-quality gates — refuse to evaluate on stale or
        // implausible inputs. These run BEFORE the
        // strike/scoring/decide branches because a stale tick can't be
        // recovered by the strategy.
        let mut dq_block = check_data_quality(&rec, &self.cfg.strategy);
        // Add the FV-divergence gate. Runs only when we have both
        // signals (so it can't false-trigger on a missing poly book).
        if dq_block.is_none() {
            if let Some(gap) = fv_minus_poly {
                if gap.abs() > self.cfg.strategy.max_fv_divergence_pp {
                    dq_block = Some(IncompleteReason::ModelDivergence);
                }
            }
        }

        // Decide on the outcome.
        let outcome = if let Some(reason) = dq_block {
            Outcome3::Incomplete(reason)
        } else {
            match (strike, scoring_outcome) {
                (None, _) => Outcome3::Incomplete(IncompleteReason::NoStrike),
                (Some(_), None) => Outcome3::Incomplete(IncompleteReason::NoBtcMid),
                (Some(_), Some(s_out)) => {
                    if ttr <= 0.0 {
                        Outcome3::Incomplete(IncompleteReason::TtrNonPositive)
                    } else if am.last_poly_snapshot.is_none() {
                        Outcome3::Incomplete(IncompleteReason::NoPolyYesMid)
                    } else {
                        let snap = am.last_poly_snapshot.as_ref().unwrap();
                        // Diagnostic: log required edges at each side's ask.
                        if let Some(a) = snap.yes_ask {
                            rec.taker_required_edge_yes =
                                Some(taker_required_edge(a, &self.cfg.strategy));
                        }
                        if let Some(a) = snap.no_ask {
                            rec.taker_required_edge_no =
                                Some(taker_required_edge(a, &self.cfg.strategy));
                        }
                        let strat = decide(
                            DecisionInputs {
                                market_id: &am.market.id,
                                scoring_outcome: s_out,
                                poly_yes_bid: snap.yes_bid,
                                poly_yes_ask: snap.yes_ask,
                                poly_no_bid: snap.no_bid,
                                poly_no_ask: snap.no_ask,
                                ttr_secs: ttr,
                                max_per_trade_usd: self.cfg.risk.max_per_trade_usd,
                                bankroll_usd: self.bankroll_usd,
                            },
                            &self.cfg.strategy,
                        );
                        match strat {
                            StrategyOutcome::Fire(signal) => {
                                let mark_mid = match signal.side {
                                    Outcome::Yes => snap.yes_mid(),
                                    Outcome::No => snap.no_mid(),
                                };
                                match self.risk.evaluate(
                                    signal.clone(),
                                    &self.positions,
                                    &self.cfg.risk,
                                    mark_mid,
                                    now_s,
                                ) {
                                    RiskDecision::Approve(approved) => {
                                        match &self.mode {
                                            BotMode::Paper => {
                                                let fill = self
                                                    .executor
                                                    .submit(approved.clone(), &mut self.positions);
                                                // Deduct cost from bankroll (we now hold shares, not cash).
                                                self.bankroll_usd -= fill.fill_size_usd;
                                                self.risk.record_fill(
                                                    approved.market_id.clone(),
                                                    fill.fill_size_usd,
                                                    now_s,
                                                );
                                                self.persist_snapshot();
                                                info!(
                                                    event = "paper_fill",
                                                    market_id = %approved.market_id,
                                                    slug = %am.market.slug,
                                                    side = ?approved.side,
                                                    size_usd = format!("{:.4}", approved.size_usd),
                                                    price = format!("{:.4}", fill.fill_price),
                                                    edge = format!("{:.4}", approved.edge),
                                                    "paper fill"
                                                );
                                                Outcome3::Fired {
                                                    side: approved.side,
                                                    size_usd: fill.fill_size_usd,
                                                    price: fill.fill_price,
                                                }
                                            }
                                            BotMode::Live(live) => {
                                                let live = live.clone();
                                                let ctx = match self
                                                    .live_contexts
                                                    .get(&approved.market_id)
                                                {
                                                    Some(c) => c.clone(),
                                                    None => {
                                                        warn!(
                                                            event = "live_fill_skipped",
                                                            market_id = %approved.market_id,
                                                            reason = "no_market_context",
                                                            "missing live context for active market"
                                                        );
                                                        return;
                                                    }
                                                };
                                                let mut guard = live.lock().await;
                                                let submit_res =
                                                    guard.submit(approved.clone(), &ctx).await;
                                                drop(guard);
                                                match submit_res {
                                                    Ok(order_id) => {
                                                        // Optimistic accounting: assume fill at the
                                                        // signal price. The reconciliation loop will
                                                        // correct if the venue cancels or
                                                        // partial-fills (v1 limitation; see
                                                        // `live.rs` module docs).
                                                        self.positions.apply_fill(
                                                            &approved.market_id,
                                                            approved.side,
                                                            approved.size_usd,
                                                            approved.price,
                                                        );
                                                        self.bankroll_usd -= approved.size_usd;
                                                        self.risk.record_fill(
                                                            approved.market_id.clone(),
                                                            approved.size_usd,
                                                            now_s,
                                                        );
                                                        self.persist_snapshot();
                                                        info!(
                                                            event = "live_fill",
                                                            market_id = %approved.market_id,
                                                            slug = %am.market.slug,
                                                            order_id = %order_id.as_str(),
                                                            side = ?approved.side,
                                                            size_usd = format!("{:.4}", approved.size_usd),
                                                            price = format!("{:.4}", approved.price),
                                                            edge = format!("{:.4}", approved.edge),
                                                            "live fill submitted"
                                                        );
                                                        Outcome3::Fired {
                                                            side: approved.side,
                                                            size_usd: approved.size_usd,
                                                            price: approved.price,
                                                        }
                                                    }
                                                    Err(e) => {
                                                        warn!(
                                                            event = "live_submit_error",
                                                            market_id = %approved.market_id,
                                                            error = %e,
                                                            "live submit failed; not booking fill"
                                                        );
                                                        Outcome3::Rejected {
                                                            reason: format!("live_submit: {e}"),
                                                            side: approved.side,
                                                            intended_size_usd: approved.size_usd,
                                                            intended_price: approved.price,
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    RiskDecision::Reject(reason) => Outcome3::Rejected {
                                        reason: reject_reason_to_str(&reason).to_string(),
                                        side: signal.side,
                                        intended_size_usd: signal.size_usd,
                                        intended_price: signal.price,
                                    },
                                }
                            }
                            StrategyOutcome::NoSignal(reason) => Outcome3::NoSignal { reason },
                        }
                    }
                }
            }
        };

        // Populate decision fields on the record.
        match outcome {
            Outcome3::Fired {
                side,
                size_usd,
                price,
            } => {
                rec.decision_kind = DecisionKind::Fire;
                rec.decision_side = Some(side_to_str(side).to_string());
                rec.decision_size_usd = Some(size_usd);
                rec.decision_price = Some(price);
            }
            Outcome3::Rejected {
                reason,
                side,
                intended_size_usd,
                intended_price,
            } => {
                rec.decision_kind = DecisionKind::Rejected;
                rec.decision_side = Some(side_to_str(side).to_string());
                rec.decision_size_usd = Some(intended_size_usd);
                rec.decision_price = Some(intended_price);
                rec.reject_reason = Some(reason);
            }
            Outcome3::NoSignal { reason } => {
                rec.decision_kind = DecisionKind::NoSignal;
                rec.no_signal_reason = Some(reason);
            }
            Outcome3::Incomplete(reason) => {
                rec.decision_kind = DecisionKind::Incomplete;
                rec.incomplete_reason = Some(reason);
            }
        }

        // Bump the per-kind decision counter for /metrics.
        use crate::metrics::DecisionKindMetric;
        let metric_kind = match rec.decision_kind {
            DecisionKind::Fire => DecisionKindMetric::Fire,
            DecisionKind::Rejected => DecisionKindMetric::Rejected,
            DecisionKind::NoSignal => DecisionKindMetric::NoSignal,
            DecisionKind::Incomplete => DecisionKindMetric::Incomplete,
        };
        self.metrics.record_decision(metric_kind);

        if let Some(logger) = self.logger.as_mut() {
            if let Err(e) = logger.write(&rec) {
                warn!(error = %e, "failed to write decision record");
            }
        }

        // Sync gauges. Counter was bumped above so the snapshot is
        // self-consistent for the next scrape.
        self.refresh_metrics();
    }

    fn emit_status(&self) {
        let latest_btc = self.btc_history.latest().map(|(_, m)| m);
        let sigma = self.vol.primary.sigma_per_sec();
        let (market_id, slug, ttr, strike, poly_yes_mid) = match &self.active {
            Some(am) => (
                Some(am.market.id.as_str().to_string()),
                Some(am.market.slug.clone()),
                Some(am.ttr_secs(now_epoch_secs_f64())),
                am.strike,
                am.last_poly_snapshot.as_ref().and_then(|s| s.yes_mid()),
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
            btc_history_len = self.btc_history.len(),
            vol_samples = self.vol.primary.len(),
            poly_yes_mid = ?poly_yes_mid,
            seen_markets = self.seen_markets.len(),
            resolved_markets = self.resolved_markets.len(),
            open_positions = self.positions.open_count(),
            total_fills = self.executor.fill_count(),
            total_realised_pnl_usd = format!("{:.4}", self.positions.total_realised()),
            bankroll_usd = format!("{:.4}", self.bankroll_usd),
            kill_switch_tripped = self.risk.is_kill_switch_tripped(),
            "status"
        );
        // Keep metrics snapshot fresh even on ticks where the FV path
        // didn't refresh (e.g. when running but no active market).
        self.refresh_metrics();
    }
}

enum Outcome3 {
    Fired {
        side: Outcome,
        size_usd: f64,
        price: f64,
    },
    Rejected {
        reason: String,
        side: Outcome,
        intended_size_usd: f64,
        intended_price: f64,
    },
    NoSignal {
        reason: crate::strategy::NoSignalReason,
    },
    Incomplete(IncompleteReason),
}

fn side_to_str(o: Outcome) -> &'static str {
    match o {
        Outcome::Yes => "yes",
        Outcome::No => "no",
    }
}

/// Data-quality gates. Refuse to evaluate the strategy on stale or
/// implausible inputs. Returns the first failing reason, or `None` if
/// all checks pass. Run BEFORE strike/scoring/decide because no
/// downstream stage can recover from a stale tick.
///
/// Gates:
/// * Binance tick age — protects against trading the strike off a
///   disconnected upstream feed.
/// * Polymarket book age — protects against firing into a stale book
///   where our edge calc is using an obsolete price.
/// * σ sanity — only checked when σ was estimated (the fallback is by
///   definition within range). Out-of-range σ usually indicates a venue
///   glitch the vol estimator picked up.
pub fn check_data_quality(
    rec: &DecisionRecord,
    strategy: &crate::config::StrategyConfig,
) -> Option<IncompleteReason> {
    if let Some(age) = rec.btc_last_update_age_ms {
        if age / 1000.0 > strategy.max_btc_tick_age_secs {
            return Some(IncompleteReason::StaleBinanceFeed);
        }
    }
    if let Some(age) = rec.poly_book_age_ms {
        if age / 1000.0 > strategy.max_poly_book_age_secs {
            return Some(IncompleteReason::StalePolyBook);
        }
    }
    if rec.sigma_source.as_deref() == Some("estimated") {
        if let Some(s) = rec.sigma_per_sec_used {
            if !(strategy.min_sigma_per_sec..=strategy.max_sigma_per_sec).contains(&s) {
                return Some(IncompleteReason::SigmaOutOfRange);
            }
        }
    }
    None
}

/// Build a record with the market-identity fields populated. Other
/// fields are filled in by the caller as state becomes available.
fn base_record(am: &ActiveMarket, session_id: &str, now_s: f64) -> DecisionRecord {
    let local_ts_ns = common::LocalTimestamp::now().as_nanos().to_string();
    DecisionRecord {
        schema_version: SCHEMA_VERSION,
        local_ts_ns,
        session_id: session_id.to_string(),
        bot_version: BOT_VERSION.into(),
        market_id: am.market.id.as_str().to_string(),
        market_slug: am.market.slug.clone(),
        yes_token: am.market.yes_token.as_str().to_string(),
        no_token: am.market.no_token.as_str().to_string(),
        end_epoch: am.market.end_time_epoch,
        effective_start_epoch: am.effective_start_epoch(),
        ttr_secs: am.ttr_secs(now_s),
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

fn now_epoch_secs_f64() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn now_epoch_secs_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Synthetic-feed demo
// ---------------------------------------------------------------------------

/// Synthetic-feed demo: simulates one 5-minute Polymarket BTC up/down
/// market with a deterministic divergence between BTC-implied FV and the
/// Polymarket mid. Demonstrates the full strategy → risk → paper exec
/// pipeline end-to-end without needing real feeds.
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

        // Use the scoring model directly: with default scoring weights
        // (only Z carries weight 1.0), this exactly recovers compute_fv's
        // Φ(d). The demo's purpose is to exercise the strategy/risk
        // pipeline end-to-end, not to produce calibrated trades.
        let regime = Regime::from_ttr_secs(ttr);
        let s_out = match scoring::score(
            &Features {
                btc_strike_distance_z: {
                    // d ≈ ln(S/K)/σ√T (drop the σ²T/2 term — negligible).
                    let sigma_t = sigma * ttr.max(1.0).sqrt();
                    if sigma_t > 0.0 {
                        Some((btc / strike).ln() / sigma_t)
                    } else {
                        None
                    }
                },
                ..Default::default()
            },
            regime,
            &cfg.scoring,
        ) {
            Some(s) => s,
            None => {
                t += dt;
                tokio::time::sleep(Duration::from_millis(2)).await;
                continue;
            }
        };
        // Tight synthetic book: bid 1c below mid, ask 1c above.
        let yes_bid = (poly_yes_mid - 0.005).clamp(0.01, 0.99);
        let yes_ask = (poly_yes_mid + 0.005).clamp(0.01, 0.99);
        let no_bid = (1.0 - poly_yes_mid - 0.005).clamp(0.01, 0.99);
        let no_ask = (1.0 - poly_yes_mid + 0.005).clamp(0.01, 0.99);
        let strat = decide(
            DecisionInputs {
                market_id: &market_id,
                scoring_outcome: s_out,
                poly_yes_bid: Some(yes_bid),
                poly_yes_ask: Some(yes_ask),
                poly_no_bid: Some(no_bid),
                poly_no_ask: Some(no_ask),
                ttr_secs: ttr,
                max_per_trade_usd: cfg.risk.max_per_trade_usd,
                bankroll_usd: cfg.risk.bankroll_initial_usd,
            },
            &cfg.strategy,
        );
        // Re-bind so the legacy `fv` variable shadow below stays valid
        // for the existing logging without restructuring further.
        let _ = fv;
        if let StrategyOutcome::Fire(sig) = strat {
            let mark = positions.get(&market_id).map(|pos| match pos.side {
                Outcome::Yes => poly_yes_mid,
                Outcome::No => 1.0 - poly_yes_mid,
            });
            match risk.evaluate(sig.clone(), &positions, &cfg.risk, mark, t) {
                RiskDecision::Approve(approved) => {
                    let fill = executor.submit(approved.clone(), &mut positions);
                    risk.record_fill(approved.market_id.clone(), fill.fill_size_usd, t);
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
                    use crate::risk::RejectReason;
                    if matches!(
                        reason,
                        RejectReason::Cooldown | RejectReason::NotionalCapReached
                    ) {
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

    let winner = if btc > strike {
        Outcome::Yes
    } else {
        Outcome::No
    };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StrategyConfig;

    fn empty_record() -> DecisionRecord {
        DecisionRecord {
            schema_version: SCHEMA_VERSION,
            local_ts_ns: "0".into(),
            session_id: "test".into(),
            bot_version: BOT_VERSION.into(),
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
    fn dq_passes_on_clean_record() {
        let rec = empty_record();
        let strategy = StrategyConfig::default();
        assert!(check_data_quality(&rec, &strategy).is_none());
    }

    #[test]
    fn dq_blocks_stale_binance_tick() {
        let mut rec = empty_record();
        let strategy = StrategyConfig::default();
        // Default max_btc_tick_age_secs = 5.0 → 5000ms threshold.
        rec.btc_last_update_age_ms = Some(6_000.0);
        assert_eq!(
            check_data_quality(&rec, &strategy),
            Some(IncompleteReason::StaleBinanceFeed)
        );
    }

    #[test]
    fn dq_allows_fresh_binance_tick() {
        let mut rec = empty_record();
        let strategy = StrategyConfig::default();
        rec.btc_last_update_age_ms = Some(1_000.0);
        assert!(check_data_quality(&rec, &strategy).is_none());
    }

    #[test]
    fn dq_blocks_stale_poly_book() {
        let mut rec = empty_record();
        let strategy = StrategyConfig::default();
        // Default max_poly_book_age_secs = 3.0 → 3000ms threshold.
        rec.poly_book_age_ms = Some(4_000.0);
        assert_eq!(
            check_data_quality(&rec, &strategy),
            Some(IncompleteReason::StalePolyBook)
        );
    }

    #[test]
    fn dq_allows_fresh_poly_book() {
        let mut rec = empty_record();
        let strategy = StrategyConfig::default();
        rec.poly_book_age_ms = Some(500.0);
        assert!(check_data_quality(&rec, &strategy).is_none());
    }

    #[test]
    fn dq_blocks_sigma_too_high() {
        let mut rec = empty_record();
        let strategy = StrategyConfig::default();
        // Default max_sigma_per_sec = 1e-2.
        rec.sigma_per_sec_used = Some(0.05);
        rec.sigma_source = Some("estimated".into());
        assert_eq!(
            check_data_quality(&rec, &strategy),
            Some(IncompleteReason::SigmaOutOfRange)
        );
    }

    #[test]
    fn dq_blocks_sigma_too_low() {
        let mut rec = empty_record();
        let strategy = StrategyConfig::default();
        // Default min_sigma_per_sec = 1e-7.
        rec.sigma_per_sec_used = Some(1e-10);
        rec.sigma_source = Some("estimated".into());
        assert_eq!(
            check_data_quality(&rec, &strategy),
            Some(IncompleteReason::SigmaOutOfRange)
        );
    }

    #[test]
    fn dq_ignores_out_of_range_when_sigma_is_fallback() {
        let mut rec = empty_record();
        let strategy = StrategyConfig::default();
        // Fallback σ is "trusted" — out-of-range checks don't apply.
        rec.sigma_per_sec_used = Some(0.5);
        rec.sigma_source = Some("fallback".into());
        assert!(check_data_quality(&rec, &strategy).is_none());
    }

    #[test]
    fn dq_first_failing_reason_wins_btc_before_poly() {
        let mut rec = empty_record();
        let strategy = StrategyConfig::default();
        // Both stale; the BTC check is first in the function, so it wins.
        rec.btc_last_update_age_ms = Some(10_000.0);
        rec.poly_book_age_ms = Some(10_000.0);
        assert_eq!(
            check_data_quality(&rec, &strategy),
            Some(IncompleteReason::StaleBinanceFeed)
        );
    }
}
