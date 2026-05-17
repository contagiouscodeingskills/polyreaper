//! Binance bookTicker WS → BTC mid events.
//!
//! Uses the single-stream URL form so no SUBSCRIBE message is needed —
//! every Text frame is a bookTicker payload. Reconnects with exponential
//! backoff on any error or idle timeout. Reuses the same backoff shape as
//! the recorder's binance_feed crate but keeps logic local (the recorder
//! crate writes to disk and isn't a fit for streaming consumers).

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::bot::BotEvent;
use crate::config::BinanceFeedSettings;

/// Run the Binance feed forever. Returns only if the channel receiver is
/// dropped (i.e. the bot is shutting down).
pub async fn run(cfg: BinanceFeedSettings, tx: mpsc::Sender<BotEvent>) {
    let mut attempt: u32 = 0;
    loop {
        match connect_once(&cfg, &tx, &mut attempt).await {
            Ok(()) => warn!(component = "binance", "ws closed cleanly; reconnecting"),
            Err(reason) => warn!(
                component = "binance",
                reason = %reason,
                "ws error; backing off"
            ),
        }
        if tx.is_closed() {
            info!(component = "binance", "channel closed; exiting feed");
            return;
        }
        let delay = backoff(attempt);
        sleep(delay).await;
        attempt = attempt.saturating_add(1);
    }
}

async fn connect_once(
    cfg: &BinanceFeedSettings,
    tx: &mpsc::Sender<BotEvent>,
    attempt: &mut u32,
) -> Result<(), String> {
    info!(component = "binance", url = %cfg.ws_url, attempt = *attempt, "connecting");
    let (mut ws, _resp) = connect_async(&cfg.ws_url)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    info!(component = "binance", "connected");

    let idle = Duration::from_secs(cfg.read_idle_secs);
    let mut got_any = false;

    loop {
        match timeout(idle, ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                if !got_any {
                    got_any = true;
                    *attempt = 0;
                }
                if let Some(mid) = parse_book_ticker_mid(text.as_str()) {
                    let t_ns = common::LocalTimestamp::now().as_nanos();
                    let ev = BotEvent::BtcTick { t_ns, mid_usd: mid };
                    if tx.send(ev).await.is_err() {
                        return Err("channel closed".into());
                    }
                } else {
                    debug!(component = "binance", payload = %truncate(&text, 200), "parse miss");
                }
            }
            Ok(Some(Ok(Message::Binary(_)))) => {
                debug!(component = "binance", "unexpected binary frame ignored");
            }
            Ok(Some(Ok(Message::Ping(p)))) => {
                if let Err(e) = ws.send(Message::Pong(p)).await {
                    return Err(format!("pong: {e}"));
                }
            }
            Ok(Some(Ok(Message::Pong(_)))) => {}
            Ok(Some(Ok(Message::Close(frame)))) => {
                info!(component = "binance", close = ?frame, "server close");
                return Ok(());
            }
            Ok(Some(Ok(Message::Frame(_)))) => {}
            Ok(Some(Err(e))) => return Err(format!("read: {e}")),
            Ok(None) => return Err("stream ended".into()),
            Err(_) => return Err(format!("idle timeout {}s", cfg.read_idle_secs)),
        }
    }
}

// ---------------------------------------------------------------------------
// Frame parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct BookTickerFrame {
    /// Best bid price, stringified decimal.
    #[serde(rename = "b")]
    bid: String,
    /// Best ask price, stringified decimal.
    #[serde(rename = "a")]
    ask: String,
}

/// Parse a single bookTicker JSON payload into a mid price. Returns
/// `None` for any shape we don't recognise.
fn parse_book_ticker_mid(text: &str) -> Option<f64> {
    let frame: BookTickerFrame = serde_json::from_str(text).ok()?;
    let bid: f64 = frame.bid.parse().ok()?;
    let ask: f64 = frame.ask.parse().ok()?;
    if !(bid.is_finite() && ask.is_finite()) || bid <= 0.0 || ask <= 0.0 {
        return None;
    }
    Some((bid + ask) / 2.0)
}

// ---------------------------------------------------------------------------
// Backoff
// ---------------------------------------------------------------------------

fn backoff(attempt: u32) -> Duration {
    let base_ms = 500u64;
    let max_ms = 30_000u64;
    let mult = 2.0_f64;
    let ms = (base_ms as f64 * mult.powi(attempt as i32)).min(max_ms as f64);
    Duration::from_millis(ms as u64)
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("{}...", &s[..n])
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_canonical_book_ticker_frame() {
        let frame = r#"{"u":12345,"s":"BTCUSDT","b":"50000.10","B":"5.0","a":"50000.30","A":"3.0"}"#;
        let mid = parse_book_ticker_mid(frame).expect("should parse");
        assert!((mid - 50000.20).abs() < 1e-6);
    }

    #[test]
    fn rejects_zero_or_negative_prices() {
        let frame = r#"{"b":"0","a":"50000.00"}"#;
        assert!(parse_book_ticker_mid(frame).is_none());
    }

    #[test]
    fn rejects_non_numeric_prices() {
        let frame = r#"{"b":"abc","a":"50000.00"}"#;
        assert!(parse_book_ticker_mid(frame).is_none());
    }

    #[test]
    fn rejects_missing_fields() {
        let frame = r#"{"s":"BTCUSDT"}"#;
        assert!(parse_book_ticker_mid(frame).is_none());
    }

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(backoff(0), Duration::from_millis(500));
        assert_eq!(backoff(1), Duration::from_millis(1_000));
        assert_eq!(backoff(2), Duration::from_millis(2_000));
        assert!(backoff(20) <= Duration::from_millis(30_000));
        assert!(backoff(20) >= Duration::from_millis(15_000));
    }
}
