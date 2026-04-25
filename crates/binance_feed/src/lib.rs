//! Binance Spot BTCUSDT WebSocket feed.
//!
//! Connects to the URL from `config::BinanceFeedConfig`, sends a
//! `SUBSCRIBE` for the configured streams (e.g. `btcusdt@trade`,
//! `btcusdt@depth@100ms`), wraps every inbound text frame in
//! [`common::RawEvent`] and persists it through [`storage::Store`].
//!
//! Three responsibilities, split into modules:
//! * [`conn`] — connection lifecycle, subscribe, read-until-error,
//!   reconnect with exponential backoff.
//! * [`frame`] — per-frame processing: event-type classification, stream
//!   derivation, `RawEvent` construction, storage write.
//! * this file — public surface ([`run`], [`FeedStats`]) + module glue,
//!   plus the periodic health emitter.
//!
//! Phase 1 scope: text frames only; no book reconstruction.

mod conn;
mod frame;
mod snapshot;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use telemetry::Counter;

pub const NAME: &str = "binance_feed";

/// How often the health emitter logs a snapshot of the counters. Hardcoded
/// for Phase 1 — moves to config if we ever want per-feed tuning.
const HEALTH_INTERVAL: Duration = Duration::from_secs(60);

/// Feed-level atomic counters. Cloning shares state, so the read loop and
/// the health task both touch the same numbers without locking.
#[derive(Debug, Clone, Default)]
pub struct FeedStats {
    pub messages: Counter,
    pub parse_failures: Counter,
    pub write_failures: Counter,
    pub reconnects: Counter,
}

impl FeedStats {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Run the feed. Drives the connection loop and a periodic health emitter
/// in parallel. Only returns when the enclosing tokio task is cancelled
/// (e.g. `handle.abort()` in the recorder).
pub async fn run(
    cfg: &config::BinanceFeedConfig,
    store: Arc<Mutex<storage::Store>>,
    stats: FeedStats,
) {
    let health_stats = stats.clone();
    tokio::select! {
        _ = conn::connect_forever(cfg, store, stats) => {},
        _ = emit_health_forever(health_stats, HEALTH_INTERVAL) => {},
    }
}

/// Emit a `event="health"` log line every `interval`, containing a
/// snapshot of the shared counters. Never returns on its own — designed to
/// race against the connect loop inside [`run`].
///
/// Deliberately skips the first immediate tick from `interval()` so the
/// first emitted line contains real counts rather than an all-zero
/// snapshot taken before the connection is up.
async fn emit_health_forever(stats: FeedStats, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // consume the immediate first tick
    loop {
        ticker.tick().await;
        tracing::info!(
            component = "binance_feed",
            venue = "binance",
            event = "health",
            messages = stats.messages.get(),
            reconnects = stats.reconnects.get(),
            parse_failures = stats.parse_failures.get(),
            write_failures = stats.write_failures.get(),
            "health snapshot"
        );
    }
}
