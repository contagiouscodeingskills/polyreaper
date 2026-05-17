//! Bot orchestration loop.
//!
//! Owns the strategy/risk core's mutable state. Consumes a single
//! `mpsc::Receiver<BotEvent>` fed by the feed tasks (Binance WS,
//! Polymarket book poller, Gamma discovery). Emits paper fills via
//! `PaperExecutor`, structured tracing logs for humans, and one
//! `DecisionRecord` per evaluation tick into `decisions.ndjson` for
//! durable audit.

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use market_registry::{Market, MarketId, Outcome};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use crate::config::BotConfig;
use crate::decision_log::{
    make_session_id, reject_reason_to_str, time_bucket_for, write_session_meta, BOT_VERSION,
    DecisionKind, DecisionLogger, DecisionRecord, IncompleteReason, ResolutionLogger,
    ResolutionRecord, SessionMeta, SCHEMA_VERSION,
};
use crate::execution::PaperExecutor;
use crate::feeds;
use crate::fv::{compute_fv, implied_strike, FairValue, VolEstimator};
use crate::market_state::{ActiveMarket, BtcHistory, PolyBookSnapshot};
use crate::position::PositionStore;
use crate::risk::{RiskDecision, RiskEngine};
use crate::strategy::{decide, DecisionInputs, StrategyOutcome};

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

    let mut state = BotState::new(
        cfg.clone(),
        paths.session_id.clone(),
        logger,
        resolution_logger,
    );

    let mut status_ticker = tokio::time::interval(Duration::from_secs(30));
    status_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    status_ticker.tick().await;

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
}

impl BotState {
    fn new(
        cfg: BotConfig,
        session_id: String,
        logger: Option<DecisionLogger>,
        resolution_logger: Option<ResolutionLogger>,
    ) -> Self {
        let vol_window = cfg.strategy.vol_window_secs;
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
            seen_markets: HashSet::new(),
            resolved_markets: HashSet::new(),
            last_btc_tick_ns: None,
        }
    }

    fn handle_event(&mut self, ev: BotEvent) {
        match ev {
            BotEvent::BtcTick { t_ns, mid_usd } => self.on_btc_tick(t_ns, mid_usd),
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

        let position_side = position_snapshot
            .as_ref()
            .map(|p| side_to_str(p.side).to_string());
        let position_shares = position_snapshot.as_ref().map(|p| p.shares);
        let position_cost_usd = position_snapshot.as_ref().map(|p| p.cost_usd);
        let position_avg_price = position_snapshot.as_ref().map(|p| p.avg_price);
        let winning_side = position_snapshot.as_ref().map(|p| p.side == outcome);
        let settled_proceeds_usd = position_snapshot
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
    }

    fn on_poly_book(&mut self, market_id: MarketId, snapshot: PolyBookSnapshot) {
        let Some(am) = self.active.as_mut() else {
            return;
        };
        if am.market.id != market_id {
            // Late delivery from the previous market; ignore.
            return;
        }
        am.last_poly_snapshot = Some(snapshot);
        let now_s = now_epoch_secs_f64();
        self.evaluate_and_log(now_s);
    }

    fn on_market_changed(&mut self, market: Market) {
        info!(
            event = "active_market_changed",
            market_id = %market.id,
            slug = %market.slug,
            end_epoch = market.end_time_epoch,
            "switching active market"
        );
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
    fn evaluate_and_log(&mut self, now_s: f64) {
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
                let age_ms = self
                    .last_btc_tick_ns
                    .map(|t_ns| {
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

        // Strategy state: σ source + FV + implied strike + edge.
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

        // Compute FV + implied strike only when we have enough inputs.
        let fv: Option<FairValue> = match (latest_btc, strike) {
            (Some(btc), Some(k)) => {
                let ttr = am.ttr_secs(now_s);
                if ttr > 0.0 {
                    Some(compute_fv(btc, k, ttr.max(1.0), sigma_used))
                } else {
                    None
                }
            }
            _ => None,
        };
        if let Some(fv) = fv {
            rec.fv_yes = Some(fv.p_yes);
            rec.fv_no = Some(fv.p_no);
            rec.sigma_per_sec_used = Some(sigma_used);
            rec.sigma_source = Some(sigma_source.to_string());
            if let Some(mid) = yes_mid {
                rec.edge_yes = Some(fv.p_yes - mid);
            }
        }
        if let (Some(btc), Some(mid)) = (latest_btc, yes_mid) {
            let ttr = am.ttr_secs(now_s);
            if ttr > 0.0 {
                rec.implied_strike_usd =
                    implied_strike(btc, ttr.max(1.0), sigma_used, mid);
                if let (Some(impl_k), Some(k)) = (rec.implied_strike_usd, strike) {
                    rec.strike_gap_usd = Some(impl_k - k);
                    if k > 0.0 {
                        rec.strike_gap_bps = Some((impl_k - k) / k * 10_000.0);
                    }
                }
            }
        }

        // Decide on the outcome.
        let outcome = match (strike, yes_mid) {
            (None, _) => Outcome3::Incomplete(IncompleteReason::NoStrike),
            (_, None) => Outcome3::Incomplete(IncompleteReason::NoPolyYesMid),
            (Some(_), Some(mid)) => {
                let ttr = am.ttr_secs(now_s);
                if latest_btc.is_none() {
                    Outcome3::Incomplete(IncompleteReason::NoBtcMid)
                } else if ttr <= 0.0 {
                    Outcome3::Incomplete(IncompleteReason::TtrNonPositive)
                } else {
                    let fv = fv.expect("fv present when strike + btc present");
                    let strat = decide(
                        DecisionInputs {
                            market_id: &am.market.id,
                            fair_value: fv,
                            poly_yes_mid: mid,
                            ttr_secs: ttr,
                            max_per_trade_usd: self.cfg.risk.max_per_trade_usd,
                        },
                        &self.cfg.strategy,
                    );
                    match strat {
                        StrategyOutcome::Fire(signal) => {
                            let mark = self
                                .positions
                                .get(&am.market.id)
                                .map(|pos| match pos.side {
                                    Outcome::Yes => mid,
                                    Outcome::No => 1.0 - mid,
                                });
                            match self.risk.evaluate(
                                signal.clone(),
                                &self.positions,
                                &self.cfg.risk,
                                mark,
                                now_s,
                            ) {
                                RiskDecision::Approve(approved) => {
                                    let fill = self
                                        .executor
                                        .submit(approved.clone(), &mut self.positions);
                                    self.risk
                                        .record_fill(approved.market_id.clone(), now_s);
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

        if let Some(logger) = self.logger.as_mut() {
            if let Err(e) = logger.write(&rec) {
                warn!(error = %e, "failed to write decision record");
            }
        }
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
            "status"
        );
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

        let strat = decide(
            DecisionInputs {
                market_id: &market_id,
                fair_value: fv,
                poly_yes_mid,
                ttr_secs: ttr,
                max_per_trade_usd: cfg.risk.max_per_trade_usd,
            },
            &cfg.strategy,
        );
        if let StrategyOutcome::Fire(sig) = strat {
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
                    use crate::risk::RejectReason;
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
