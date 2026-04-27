//! Chainlink on-chain price-feed recorder.
//!
//! Connects to an Ethereum JSON-RPC WebSocket, sends an
//! `eth_subscribe(['logs', {address: <contract>}])` for the configured
//! contract (default: BTC/USD `AggregatorV3` on mainnet), and writes
//! every inbound notification to storage as a [`common::RawEvent`].
//!
//! No event-topic filter is applied — we capture all logs from the
//! contract address. That includes `AnswerUpdated` (the primary price
//! event) plus any auxiliary events the aggregator emits. Replay can
//! decode by examining `topics[0]`.
//!
//! ## Caveat
//!
//! Polymarket BTC 5-minute markets actually resolve via Chainlink Data
//! Streams (Mercury), a different product from the on-chain aggregator
//! we read here. Both are Chainlink-sourced BTC/USD feeds and tend to
//! track each other closely, but for absolute resolution-clock
//! correctness the recorder would need a Mercury client (signed
//! subscription, paid). This on-chain feed is the closest free
//! alternative.

mod conn;
mod frame;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use telemetry::{AtomicTs, Counter, LatencyHistogram};

pub const NAME: &str = "chainlink_feed";

const HEALTH_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Default)]
pub struct FeedStats {
    pub messages: Counter,
    pub parse_failures: Counter,
    pub write_failures: Counter,
    pub reconnects: Counter,
    /// Local wall-clock at the moment this feed last received a Text
    /// frame off its websocket — *not* a parsed event.
    pub last_msg: AtomicTs,
    /// Cumulative-since-process-start histogram of storage critical-
    /// section durations (Mutex acquire + write + guard drop), in
    /// microseconds.
    pub store_us: LatencyHistogram,
}

impl FeedStats {
    pub fn new() -> Self {
        Self::default()
    }
}

pub async fn run(
    cfg: &config::ChainlinkFeedConfig,
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
            component = "chainlink_feed",
            venue = "chainlink",
            event = "health",
            messages = stats.messages.get(),
            reconnects = stats.reconnects.get(),
            parse_failures = stats.parse_failures.get(),
            write_failures = stats.write_failures.get(),
            "health snapshot"
        );
    }
}
