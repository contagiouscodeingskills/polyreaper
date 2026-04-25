//! Connection lifecycle.
//!
//! `connect_forever` owns the outer loop: one attempt, then backoff+retry
//! regardless of success/failure of the inner attempt. `connect_once`
//! handles the handshake, subscribe, and inbound read loop until the first
//! error (or clean close).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{frame, FeedStats};

pub(crate) async fn connect_forever(
    cfg: &config::BinanceFeedConfig,
    store: Arc<Mutex<storage::Store>>,
    stats: FeedStats,
) {
    // Attempt counter drives backoff. Reset on successful reads so that a
    // socket that stays up for hours then drops doesn't start from 30 s.
    let mut attempt: u32 = 0;

    loop {
        let outcome = connect_once(cfg, &store, &stats, &mut attempt).await;
        match outcome {
            Ok(()) => tracing::warn!(
                component = "binance_feed",
                venue = "binance",
                event = "connection_closed",
                attempt = attempt,
                "connection closed cleanly, reconnecting"
            ),
            Err(reason) => tracing::warn!(
                component = "binance_feed",
                venue = "binance",
                event = "connection_error",
                attempt = attempt,
                reason = %reason,
                "connection error, will back off"
            ),
        }
        stats.reconnects.incr();

        let delay = backoff_delay(attempt, &cfg.reconnect);
        tracing::info!(
            component = "binance_feed",
            venue = "binance",
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
    cfg: &config::BinanceFeedConfig,
    store: &Arc<Mutex<storage::Store>>,
    stats: &FeedStats,
    attempt: &mut u32,
) -> Result<(), String> {
    tracing::info!(
        component = "binance_feed",
        venue = "binance",
        event = "connecting",
        url = %cfg.ws_url,
        attempt = *attempt,
        "connecting"
    );

    let (mut ws, _response) = connect_async(&cfg.ws_url)
        .await
        .map_err(|e| format!("connect: {e}"))?;

    tracing::info!(
        component = "binance_feed",
        venue = "binance",
        event = "connected",
        url = %cfg.ws_url,
        "connected"
    );

    // Binance accepts a JSON SUBSCRIBE on /ws. `id` is echoed back in the
    // reply and is otherwise unused by us.
    let subscribe = serde_json::json!({
        "method": "SUBSCRIBE",
        "params": cfg.streams,
        "id": 1u64,
    })
    .to_string();

    ws.send(Message::Text(subscribe.into()))
        .await
        .map_err(|e| format!("send subscribe: {e}"))?;

    tracing::info!(
        component = "binance_feed",
        venue = "binance",
        event = "subscribed",
        streams = ?cfg.streams,
        "subscribed"
    );

    // Read loop. Any error, idle timeout, or clean close returns and the
    // outer loop backs off.
    let idle = Duration::from_secs(cfg.read_idle_secs);
    let mut got_any_message = false;

    loop {
        let next = timeout(idle, ws.next()).await;
        match next {
            Ok(Some(Ok(Message::Text(text)))) => {
                stats.messages.incr();
                // First successful read proves the connection is useful —
                // reset backoff so the next drop retries fast.
                if !got_any_message {
                    got_any_message = true;
                    *attempt = 0;
                }
                frame::process_text(text.as_str(), &cfg.streams, store, stats);
            }
            Ok(Some(Ok(Message::Binary(_)))) => {
                stats.parse_failures.incr();
                tracing::warn!(
                    component = "binance_feed",
                    venue = "binance",
                    event = "unexpected_binary",
                    "binance shouldn't emit binary; counting as parse failure"
                );
            }
            Ok(Some(Ok(Message::Ping(payload)))) => {
                // Manual pong — tokio-tungstenite does not auto-respond.
                if let Err(e) = ws.send(Message::Pong(payload)).await {
                    return Err(format!("send pong: {e}"));
                }
            }
            Ok(Some(Ok(Message::Pong(_)))) => {
                // We don't ping, so a pong is unexpected but harmless.
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                tracing::info!(
                    component = "binance_feed",
                    venue = "binance",
                    event = "close_frame",
                    close = ?frame,
                    "server sent close"
                );
                return Ok(());
            }
            Ok(Some(Ok(Message::Frame(_)))) => {
                // Low-level Frame is not expected on the high-level reader.
            }
            Ok(Some(Err(e))) => return Err(format!("read: {e}")),
            Ok(None) => return Err("stream ended".into()),
            Err(_elapsed) => {
                tracing::warn!(
                    component = "binance_feed",
                    venue = "binance",
                    event = "read_idle_timeout",
                    idle_secs = cfg.read_idle_secs,
                    "no messages within idle window; reconnecting"
                );
                return Err(format!("idle timeout {}s", cfg.read_idle_secs));
            }
        }
    }
}

/// Exponential backoff: `initial_ms * multiplier^attempt`, clamped to
/// `max_ms`. Caller decides when to reset `attempt`.
pub(crate) fn backoff_delay(attempt: u32, cfg: &config::ReconnectBackoff) -> Duration {
    let exp = cfg.multiplier.powi(attempt as i32);
    let ms = (cfg.initial_ms as f64 * exp).min(cfg.max_ms as f64);
    // ms could be NaN / negative if multiplier is weird — config validation
    // already rules that out, but clamp defensively anyway.
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
}
