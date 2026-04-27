//! Connection lifecycle for the Ethereum JSON-RPC WebSocket.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{frame, FeedStats};

pub(crate) async fn connect_forever(
    cfg: &config::ChainlinkFeedConfig,
    store: Arc<Mutex<storage::Store>>,
    stats: FeedStats,
) {
    let mut attempt: u32 = 0;
    loop {
        let outcome = connect_once(cfg, &store, &stats, &mut attempt).await;
        match outcome {
            Ok(()) => tracing::warn!(
                component = "chainlink_feed",
                venue = "chainlink",
                event = "connection_closed",
                attempt = attempt,
                "connection closed cleanly, reconnecting"
            ),
            Err(reason) => tracing::warn!(
                component = "chainlink_feed",
                venue = "chainlink",
                event = "connection_error",
                attempt = attempt,
                reason = %reason,
                "connection error, will back off"
            ),
        }
        stats.reconnects.incr();

        let delay = backoff_delay(attempt, &cfg.reconnect);
        tracing::info!(
            component = "chainlink_feed",
            venue = "chainlink",
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
    cfg: &config::ChainlinkFeedConfig,
    store: &Arc<Mutex<storage::Store>>,
    stats: &FeedStats,
    attempt: &mut u32,
) -> Result<(), String> {
    tracing::info!(
        component = "chainlink_feed",
        venue = "chainlink",
        event = "connecting",
        url = %cfg.ws_url,
        contract = %cfg.contract_address,
        attempt = *attempt,
        "connecting"
    );

    let (mut ws, _resp) = connect_async(&cfg.ws_url)
        .await
        .map_err(|e| format!("connect: {e}"))?;

    tracing::info!(
        component = "chainlink_feed",
        venue = "chainlink",
        event = "connected",
        "connected"
    );

    // JSON-RPC eth_subscribe to logs from this contract address.
    // No topics filter — capture all events from the contract.
    let subscribe = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": ["logs", {
            "address": cfg.contract_address,
        }]
    })
    .to_string();

    ws.send(Message::Text(subscribe.into()))
        .await
        .map_err(|e| format!("send subscribe: {e}"))?;

    tracing::info!(
        component = "chainlink_feed",
        venue = "chainlink",
        event = "subscribed",
        contract = %cfg.contract_address,
        "subscribed to all logs from contract"
    );

    let idle = Duration::from_secs(cfg.read_idle_secs);
    let mut got_any = false;

    loop {
        let next = timeout(idle, ws.next()).await;
        match next {
            Ok(Some(Ok(Message::Text(text)))) => {
                stats.messages.incr();
                let now_ns = common::LocalTimestamp::now().as_nanos();
                stats.last_msg.set_ns(now_ns);
                if !got_any {
                    got_any = true;
                    *attempt = 0;
                }
                frame::process_text(text.as_str(), store, stats);
            }
            Ok(Some(Ok(Message::Binary(_)))) => {
                stats.parse_failures.incr();
                tracing::warn!(
                    component = "chainlink_feed",
                    venue = "chainlink",
                    event = "unexpected_binary",
                    "eth rpc shouldn't emit binary"
                );
            }
            Ok(Some(Ok(Message::Ping(p)))) => {
                if let Err(e) = ws.send(Message::Pong(p)).await {
                    return Err(format!("send pong: {e}"));
                }
            }
            Ok(Some(Ok(Message::Pong(_)))) => {}
            Ok(Some(Ok(Message::Close(frame)))) => {
                tracing::info!(
                    component = "chainlink_feed",
                    venue = "chainlink",
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
                // Eth events can legitimately be sparse — BTC/USD
                // updates roughly every heartbeat (~hour) or on price
                // deviation. Idle timeout should be set generously
                // (60s+ probably too short; 600s+ recommended).
                tracing::warn!(
                    component = "chainlink_feed",
                    venue = "chainlink",
                    event = "read_idle_timeout",
                    idle_secs = cfg.read_idle_secs,
                    "no messages within idle window; reconnecting"
                );
                return Err(format!("idle timeout {}s", cfg.read_idle_secs));
            }
        }
    }
}

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
    fn backoff_grows_geometrically() {
        let cfg = bo(1_000, 60_000, 2.0);
        assert_eq!(backoff_delay(0, &cfg), Duration::from_millis(1_000));
        assert_eq!(backoff_delay(1, &cfg), Duration::from_millis(2_000));
        assert_eq!(backoff_delay(20, &cfg), Duration::from_millis(60_000));
    }
}
