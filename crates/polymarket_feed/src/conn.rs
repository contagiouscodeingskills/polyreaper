//! Connection lifecycle for the Polymarket CLOB market channel.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use market_registry::Registry;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{frame, FeedStats};

/// How long to wait between connect attempts when the registry is empty
/// (no markets discovered yet, so nothing to subscribe to). Keeps us from
/// hammering the websocket while gamma is still doing its first poll.
const EMPTY_REGISTRY_BACKOFF_MS: u64 = 2_000;

pub(crate) async fn connect_forever(
    cfg: &config::PolymarketFeedConfig,
    registry: Arc<Mutex<Registry>>,
    store: Arc<Mutex<storage::Store>>,
    stats: FeedStats,
) {
    let mut attempt: u32 = 0;

    loop {
        let outcome = connect_once(cfg, &registry, &store, &stats, &mut attempt).await;
        match outcome {
            Ok(()) => tracing::warn!(
                component = "polymarket_feed",
                venue = "polymarket",
                event = "connection_closed",
                attempt = attempt,
                "connection closed cleanly, reconnecting"
            ),
            Err(reason) => tracing::warn!(
                component = "polymarket_feed",
                venue = "polymarket",
                event = "connection_error",
                attempt = attempt,
                reason = %reason,
                "connection error, will back off"
            ),
        }
        stats.reconnects.incr();

        let delay = backoff_delay(attempt, &cfg.reconnect);
        tracing::info!(
            component = "polymarket_feed",
            venue = "polymarket",
            event = "backoff_sleep",
            attempt = attempt,
            delay_ms = delay.as_millis() as u64,
            "sleeping before reconnect"
        );
        sleep(delay).await;
        attempt = attempt.saturating_add(1);
    }
}

async fn connect_once(
    cfg: &config::PolymarketFeedConfig,
    registry: &Arc<Mutex<Registry>>,
    store: &Arc<Mutex<storage::Store>>,
    stats: &FeedStats,
    attempt: &mut u32,
) -> Result<(), String> {
    // Snapshot the current token ids from the registry.
    let token_ids = current_token_ids(registry);
    if token_ids.is_empty() {
        // Nothing to subscribe to yet — wait for gamma discovery to fill
        // the registry. Non-fatal: the outer reconnect loop sleeps and
        // retries anyway.
        sleep(Duration::from_millis(EMPTY_REGISTRY_BACKOFF_MS)).await;
        return Err("registry empty (no token ids to subscribe to)".into());
    }

    tracing::info!(
        component = "polymarket_feed",
        venue = "polymarket",
        event = "connecting",
        url = %cfg.ws_url,
        attempt = *attempt,
        token_count = token_ids.len(),
        "connecting"
    );

    let (mut ws, _resp) = connect_async(&cfg.ws_url)
        .await
        .map_err(|e| format!("connect: {e}"))?;

    tracing::info!(
        component = "polymarket_feed",
        venue = "polymarket",
        event = "connected",
        "connected"
    );

    // Subscribe to the MARKET channel for every known token id.
    let subscribe = serde_json::json!({
        "type": "MARKET",
        "assets_ids": &token_ids,
    })
    .to_string();

    ws.send(Message::Text(subscribe.into()))
        .await
        .map_err(|e| format!("send subscribe: {e}"))?;

    stats.subscriptions.incr_by(token_ids.len() as u64);
    tracing::info!(
        component = "polymarket_feed",
        venue = "polymarket",
        event = "subscribed",
        token_count = token_ids.len(),
        "subscribed"
    );

    // Read loop. Same shape as binance_feed.
    let idle = Duration::from_secs(cfg.read_idle_secs);
    let mut got_any = false;

    loop {
        let next = timeout(idle, ws.next()).await;
        match next {
            Ok(Some(Ok(Message::Text(text)))) => {
                stats.messages.incr();
                if !got_any {
                    got_any = true;
                    *attempt = 0;
                }
                frame::process_text(text.as_str(), registry, store, stats);
            }
            Ok(Some(Ok(Message::Binary(_)))) => {
                stats.parse_failures.incr();
                tracing::warn!(
                    component = "polymarket_feed",
                    venue = "polymarket",
                    event = "unexpected_binary",
                    "polymarket shouldn't emit binary; counting as parse failure"
                );
            }
            Ok(Some(Ok(Message::Ping(payload)))) => {
                if let Err(e) = ws.send(Message::Pong(payload)).await {
                    return Err(format!("send pong: {e}"));
                }
            }
            Ok(Some(Ok(Message::Pong(_)))) => {}
            Ok(Some(Ok(Message::Close(frame)))) => {
                tracing::info!(
                    component = "polymarket_feed",
                    venue = "polymarket",
                    event = "close_frame",
                    close = ?frame,
                    "server sent close"
                );
                return Ok(());
            }
            Ok(Some(Ok(Message::Frame(_)))) => {}
            Ok(Some(Err(e))) => return Err(format!("read: {e}")),
            Ok(None) => return Err("stream ended".into()),
            Err(_elapsed) => {
                tracing::warn!(
                    component = "polymarket_feed",
                    venue = "polymarket",
                    event = "read_idle_timeout",
                    idle_secs = cfg.read_idle_secs,
                    "no messages within idle window; reconnecting"
                );
                return Err(format!("idle timeout {}s", cfg.read_idle_secs));
            }
        }
    }
}

/// Snapshot the registry's current token ids — yes_token + no_token for
/// every known market, in arbitrary but deterministic order per call.
fn current_token_ids(registry: &Arc<Mutex<Registry>>) -> Vec<String> {
    let guard = registry.lock().unwrap_or_else(|p| p.into_inner());
    let mut ids = Vec::with_capacity(guard.len() * 2);
    for m in guard.iter() {
        ids.push(m.yes_token.as_str().to_string());
        ids.push(m.no_token.as_str().to_string());
    }
    ids
}

/// Same backoff formula as `binance_feed::conn::backoff_delay`. Kept
/// duplicated rather than extracted because the feed crates are otherwise
/// independent and a shared helper would complicate the dependency graph.
pub(crate) fn backoff_delay(attempt: u32, cfg: &config::ReconnectBackoff) -> Duration {
    let exp = cfg.multiplier.powi(attempt as i32);
    let ms = (cfg.initial_ms as f64 * exp).min(cfg.max_ms as f64);
    let ms = if ms.is_finite() && ms >= 0.0 {
        ms as u64
    } else {
        cfg.initial_ms
    };
    Duration::from_millis(ms)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use config::ReconnectBackoff;

    fn bo(initial: u64, max: u64, mul: f64) -> ReconnectBackoff {
        ReconnectBackoff {
            initial_ms: initial,
            max_ms: max,
            multiplier: mul,
        }
    }

    #[test]
    fn attempt_zero_returns_initial() {
        assert_eq!(
            backoff_delay(0, &bo(500, 30_000, 2.0)),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn backoff_grows_geometrically() {
        let cfg = bo(500, 30_000, 2.0);
        assert_eq!(backoff_delay(1, &cfg), Duration::from_millis(1_000));
        assert_eq!(backoff_delay(2, &cfg), Duration::from_millis(2_000));
        assert_eq!(backoff_delay(3, &cfg), Duration::from_millis(4_000));
    }

    #[test]
    fn backoff_clamps_at_max() {
        let cfg = bo(500, 30_000, 2.0);
        assert_eq!(
            backoff_delay(20, &cfg),
            Duration::from_millis(30_000)
        );
    }

    #[test]
    fn current_token_ids_collects_yes_and_no_per_market() {
        use market_registry::{Market, MarketId, Registry, TokenId};
        let m = Market {
            id: MarketId::new("M1"),
            title: "test".into(),
            slug: "m1".into(),
            yes_token: TokenId::new("T-YES"),
            no_token: TokenId::new("T-NO"),
            start_time_epoch: None,
            end_time_epoch: 100,
            resolved_outcome: None,
        };
        let mut r = Registry::new();
        r.upsert_all([m]);
        let r = Arc::new(Mutex::new(r));
        let mut ids = current_token_ids(&r);
        ids.sort();
        assert_eq!(ids, vec!["T-NO".to_string(), "T-YES".to_string()]);
    }
}
