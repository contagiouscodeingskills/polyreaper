//! Coinbase Advanced Trade WebSocket feed.
//!
//! Connects to `config::CoinbaseFeedConfig::ws_url`, subscribes to the
//! configured channel for the configured product ids, and writes every
//! inbound text frame to storage as a [`common::RawEvent`].
//!
//! Phase 1 scope: trades only (`channel = "market_trades"`). The feed
//! records, it does not reconstruct or compute. Same modular shape as
//! `binance_feed` / `polymarket_feed`.

mod conn;
mod frame;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use telemetry::Counter;

pub const NAME: &str = "coinbase_feed";

const HEALTH_INTERVAL: Duration = Duration::from_secs(60);

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

pub async fn run(
    cfg: &config::CoinbaseFeedConfig,
    store: Arc<Mutex<storage::Store>>,
    stats: FeedStats,
) {
    let health_stats = stats.clone();
    tokio::select! {
        _ = conn::connect_forever(cfg, store, stats) => {},
        _ = emit_health_forever(health_stats, HEALTH_INTERVAL) => {},
    }
}

async fn emit_health_forever(stats: FeedStats, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        tracing::info!(
            component = "coinbase_feed",
            venue = "coinbase",
            event = "health",
            messages = stats.messages.get(),
            reconnects = stats.reconnects.get(),
            parse_failures = stats.parse_failures.get(),
            write_failures = stats.write_failures.get(),
            "health snapshot"
        );
    }
}
