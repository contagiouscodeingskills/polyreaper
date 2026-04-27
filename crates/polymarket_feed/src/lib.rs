//! Polymarket CLOB market WebSocket feed.
//!
//! Connects to `config::PolymarketFeedConfig::ws_url`, snapshots the
//! current set of `(yes_token, no_token)` pairs from the registry, sends
//! a `MARKET` subscribe for all of them, and streams every inbound frame
//! through to storage as a [`common::RawEvent`].
//!
//! Three responsibilities, split into modules:
//! * [`conn`] — connection lifecycle, subscribe, read-until-error,
//!   reconnect with exponential backoff. Re-subscribes with the current
//!   registry snapshot on every reconnect, so newly-discovered markets
//!   are picked up at most one reconnect later.
//! * [`frame`] — per-frame processing: parse JSON to extract `asset_id`,
//!   look the token up against the registry, derive the stream name from
//!   the market's slug, write the raw payload to storage.
//! * this file — public surface ([`run`], [`FeedStats`]) + module glue,
//!   plus the periodic health emitter.
//!
//! Phase 1 scope: text frames only; the feed records, it does not
//! reconstruct books or compute signals.

mod conn;
mod frame;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use telemetry::{AtomicTs, Counter};

pub const NAME: &str = "polymarket_feed";

/// Cadence for the periodic health-snapshot log line.
const HEALTH_INTERVAL: Duration = Duration::from_secs(60);

/// Feed-level counters. Shared by clone — the connection loop and the
/// health emitter touch the same atomics.
#[derive(Debug, Clone, Default)]
pub struct FeedStats {
    pub messages: Counter,
    pub parse_failures: Counter,
    pub write_failures: Counter,
    pub reconnects: Counter,
    /// Total tokens subscribed across all (re)connects. Rough indicator
    /// of subscription churn.
    pub subscriptions: Counter,
    /// Local wall-clock at the moment this feed last received a Text
    /// frame off its websocket — *not* a parsed event.
    pub last_msg: AtomicTs,
}

impl FeedStats {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Run the feed. Drives the connection loop and periodic health emitter
/// in parallel. Only returns when the enclosing tokio task is cancelled.
pub async fn run(
    cfg: &config::PolymarketFeedConfig,
    registry: Arc<Mutex<market_registry::Registry>>,
    store: Arc<Mutex<storage::Store>>,
    stats: FeedStats,
) {
    let health_stats = stats.clone();
    tokio::select! {
        _ = conn::connect_forever(cfg, registry, store, stats) => {},
        _ = emit_health_forever(health_stats, HEALTH_INTERVAL) => {},
    }
}

async fn emit_health_forever(stats: FeedStats, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // consume the immediate first tick
    loop {
        ticker.tick().await;
        tracing::info!(
            component = "polymarket_feed",
            venue = "polymarket",
            event = "health",
            messages = stats.messages.get(),
            reconnects = stats.reconnects.get(),
            parse_failures = stats.parse_failures.get(),
            write_failures = stats.write_failures.get(),
            subscriptions = stats.subscriptions.get(),
            "health snapshot"
        );
    }
}
