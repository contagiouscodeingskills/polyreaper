//! Inbound frame processing.
//!
//! Two message kinds arrive on a JSON-RPC WebSocket subscription:
//!
//! * **Subscribe ack**: `{"jsonrpc":"2.0","id":1,"result":"0x<sub-id>"}`
//!   → routed to `_subscription_ack`.
//! * **Log notification**: `{"jsonrpc":"2.0","method":"eth_subscription",
//!   "params":{"subscription":"0x...","result":{"address":"0x...",
//!   "topics":[...],"data":"0x...",...}}}` → routed by the contract
//!   address inside `params.result.address`, lowercased.
//!
//! Stream name format: `<contract-address-lowercase>@logs` for log
//! events, `_subscription_ack` for the initial reply, `_unrouted` for
//! anything else (errors, unrecognised shapes).

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
                component = "chainlink_feed",
                venue = "chainlink",
                event = "parse_failure",
                reason = "unrecognised JSON-RPC payload",
                raw = %truncate(payload),
                "storing under _unrouted"
            );
            "_unrouted".to_string()
        }
    };

    let event = RawEvent {
        venue: Venue::Chainlink,
        stream,
        local_ts_ns: local_ts,
        venue_ts_ms: None,
        payload: payload.to_string(),
        ..Default::default()
    };

    let store_t0 = std::time::Instant::now();
    {
        let mut guard = match store.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if let Err(e) = guard.write(&event) {
            stats.write_failures.incr();
            tracing::error!(
                component = "chainlink_feed",
                venue = "chainlink",
                event = "write_failure",
                reason = %e,
                "storage write failed"
            );
        }
    }
    let store_us = store_t0.elapsed().as_micros() as u64;
    stats.store_us.record_micros(store_us);
}

fn classify(payload: &str) -> Option<String> {
    let v: Value = serde_json::from_str(payload).ok()?;

    // Subscribe ack: top-level `id` + `result`.
    if v.get("id").is_some() && v.get("result").is_some() && v.get("method").is_none() {
        return Some("_subscription_ack".to_string());
    }

    // Log notification: method == "eth_subscription".
    if v.get("method").and_then(|m| m.as_str()) == Some("eth_subscription") {
        let log = v.get("params")?.get("result")?;
        let addr = log.get("address").and_then(|a| a.as_str())?;
        return Some(format!("{}@logs", addr.to_lowercase()));
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
    fn classifies_subscribe_ack() {
        let p = r#"{"jsonrpc":"2.0","id":1,"result":"0xabc123"}"#;
        assert_eq!(classify(p), Some("_subscription_ack".into()));
    }

    #[test]
    fn classifies_log_notification() {
        let p = r#"{
            "jsonrpc":"2.0",
            "method":"eth_subscription",
            "params":{
                "subscription":"0xabc",
                "result":{
                    "address":"0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c",
                    "topics":["0xtopic"],
                    "data":"0xdata",
                    "blockNumber":"0x100"
                }
            }
        }"#;
        assert_eq!(
            classify(p),
            Some("0xf4030086522a5beea4988f8ca5b36dbc97bee88c@logs".into())
        );
    }

    #[test]
    fn returns_none_for_unrelated_method() {
        let p = r#"{"jsonrpc":"2.0","method":"net_subscription","params":{}}"#;
        assert_eq!(classify(p), None);
    }

    #[test]
    fn returns_none_for_non_json() {
        assert_eq!(classify("hello"), None);
    }

    #[test]
    fn returns_none_for_log_without_address() {
        let p = r#"{
            "jsonrpc":"2.0",
            "method":"eth_subscription",
            "params":{"subscription":"0xabc","result":{"topics":[]}}
        }"#;
        assert_eq!(classify(p), None);
    }
}
