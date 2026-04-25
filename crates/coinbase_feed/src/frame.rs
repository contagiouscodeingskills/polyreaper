//! Inbound frame processing.
//!
//! Coinbase Advanced Trade messages have shape:
//! ```json
//! {
//!   "channel": "market_trades",
//!   "events": [{"trades": [{"product_id": "BTC-USD", "price":"...", ...}]}]
//! }
//! ```
//!
//! Routing strategy: stream = `"<product_id>@<channel>"` lowercased,
//! pulling the product id from the first nested trade. Subscription acks
//! and other channel control messages route to `_<channel>` (e.g.
//! `_subscriptions`).

use std::sync::{Arc, Mutex};

use common::{LocalTimestamp, RawEvent, Venue};
use serde_json::Value;

use crate::FeedStats;

const RAW_LOG_TRUNCATE: usize = 256;

pub(crate) fn process_text(
    payload: &str,
    store: &Arc<Mutex<storage::Store>>,
    stats: &FeedStats,
) {
    let local_ts = LocalTimestamp::now();

    let stream = match classify(payload) {
        Some(s) => s,
        None => {
            stats.parse_failures.incr();
            tracing::warn!(
                component = "coinbase_feed",
                venue = "coinbase",
                event = "parse_failure",
                reason = "no channel field",
                raw = %truncate(payload),
                "storing under _unrouted"
            );
            "_unrouted".to_string()
        }
    };

    let event = RawEvent {
        venue: Venue::Coinbase,
        stream,
        local_ts_ns: local_ts,
        venue_ts_ms: None,
        payload: payload.to_string(),
    };

    let mut guard = match store.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    if let Err(e) = guard.write(&event) {
        stats.write_failures.incr();
        tracing::error!(
            component = "coinbase_feed",
            venue = "coinbase",
            event = "write_failure",
            reason = %e,
            "storage write failed"
        );
    }
}

fn classify(payload: &str) -> Option<String> {
    let v: Value = serde_json::from_str(payload).ok()?;
    let channel = v.get("channel").and_then(|c| c.as_str())?;
    let product = first_product_id(&v);

    let stream = match product {
        Some(p) => format!("{}@{}", p.to_lowercase(), channel),
        // Subscription acks / heartbeats — no product, route by channel only.
        None => format!("_{channel}"),
    };
    Some(stream)
}

fn first_product_id(v: &Value) -> Option<String> {
    // Coinbase nests trades inside events[].trades[]. Pull the first.
    let events = v.get("events")?.as_array()?;
    for ev in events {
        if let Some(trades) = ev.get("trades").and_then(|t| t.as_array()) {
            for t in trades {
                if let Some(pid) = t.get("product_id").and_then(|p| p.as_str()) {
                    return Some(pid.to_string());
                }
            }
        }
        // ticker / level2 / etc may put product_id at the event level.
        if let Some(pid) = ev.get("product_id").and_then(|p| p.as_str()) {
            return Some(pid.to_string());
        }
    }
    None
}

fn truncate(s: &str) -> String {
    if s.len() <= RAW_LOG_TRUNCATE {
        s.to_string()
    } else {
        let mut out = String::with_capacity(RAW_LOG_TRUNCATE + 3);
        out.push_str(&s[..RAW_LOG_TRUNCATE]);
        out.push_str("...");
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_trade_message() {
        let p = r#"{"channel":"market_trades","events":[{"trades":[{"product_id":"BTC-USD","price":"50000","size":"0.1","side":"BUY"}]}]}"#;
        assert_eq!(classify(p), Some("btc-usd@market_trades".into()));
    }

    #[test]
    fn classifies_subscription_ack() {
        let p = r#"{"channel":"subscriptions","events":[{"subscriptions":{"market_trades":["BTC-USD"]}}]}"#;
        assert_eq!(classify(p), Some("_subscriptions".into()));
    }

    #[test]
    fn classifies_event_level_product_id() {
        // Some channels (ticker, level2) put product_id at the event level.
        let p = r#"{"channel":"ticker","events":[{"product_id":"BTC-USD","price":"50000"}]}"#;
        assert_eq!(classify(p), Some("btc-usd@ticker".into()));
    }

    #[test]
    fn returns_none_for_no_channel() {
        let p = r#"{"foo":"bar"}"#;
        assert_eq!(classify(p), None);
    }

    #[test]
    fn returns_none_for_non_json() {
        assert_eq!(classify("hello"), None);
    }
}
