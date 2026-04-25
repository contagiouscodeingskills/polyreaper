//! Inbound frame processing.
//!
//! Strategy:
//! 1. Stamp `local_ts_ns` immediately.
//! 2. Try to extract an `asset_id` from the JSON — single object or
//!    homogeneous array (all same asset_id). Mixed arrays go to `_multi`,
//!    unparseable to `_unrouted`. Nothing is silently dropped.
//! 3. Look up the registry for the market that owns that token; derive
//!    the stream name from `<market.slug>-{yes|no}` so each side of each
//!    market lands in its own file.
//! 4. Write the raw payload as-is.

use std::sync::{Arc, Mutex};

use common::{LocalTimestamp, RawEvent, Venue};
use market_registry::{Registry, TokenId};

use crate::FeedStats;

const RAW_LOG_TRUNCATE: usize = 256;

pub(crate) fn process_text(
    payload: &str,
    registry: &Arc<Mutex<Registry>>,
    store: &Arc<Mutex<storage::Store>>,
    stats: &FeedStats,
) {
    let local_ts = LocalTimestamp::now();

    let stream = match classify_and_route(payload, registry) {
        RouteResult::Routed(s) => s,
        RouteResult::Multi => "_multi".to_string(),
        RouteResult::Unknown => {
            stats.parse_failures.incr();
            tracing::warn!(
                component = "polymarket_feed",
                venue = "polymarket",
                event = "parse_failure",
                reason = "no asset_id in payload",
                raw = %truncate(payload),
                "storing under _unrouted"
            );
            "_unrouted".to_string()
        }
        RouteResult::UnknownToken(token) => {
            stats.parse_failures.incr();
            tracing::warn!(
                component = "polymarket_feed",
                venue = "polymarket",
                event = "unknown_token",
                token = %token,
                raw = %truncate(payload),
                "asset_id not in registry; storing under _unknown_token"
            );
            format!("_unknown_token-{token}")
        }
    };

    let event = RawEvent {
        venue: Venue::Polymarket,
        stream,
        local_ts_ns: local_ts,
        venue_ts_ms: None, // Polymarket events don't carry a uniform ts field
        payload: payload.to_string(),
    };

    let mut guard = match store.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            tracing::error!(
                component = "polymarket_feed",
                venue = "polymarket",
                event = "store_poisoned",
                "recovering poisoned store mutex"
            );
            poisoned.into_inner()
        }
    };
    if let Err(e) = guard.write(&event) {
        stats.write_failures.incr();
        tracing::error!(
            component = "polymarket_feed",
            venue = "polymarket",
            event = "write_failure",
            reason = %e,
            "storage write failed"
        );
    }
}

#[derive(Debug, PartialEq)]
enum RouteResult {
    /// Successfully matched to a known market — derived stream name.
    Routed(String),
    /// Heterogeneous array of events covering >1 asset_id.
    Multi,
    /// Could not even extract an asset_id (non-JSON, missing field).
    Unknown,
    /// Extracted an asset_id, but no market in the registry owns it.
    UnknownToken(String),
}

fn classify_and_route(payload: &str, registry: &Arc<Mutex<Registry>>) -> RouteResult {
    let v: serde_json::Value = match serde_json::from_str(payload) {
        Ok(v) => v,
        Err(_) => return RouteResult::Unknown,
    };

    let asset_id = match &v {
        serde_json::Value::Object(obj) => match obj.get("asset_id").and_then(|x| x.as_str()) {
            Some(id) => id.to_string(),
            None => return RouteResult::Unknown,
        },
        serde_json::Value::Array(arr) if !arr.is_empty() => {
            let first = arr[0].get("asset_id").and_then(|x| x.as_str());
            let all_same = arr
                .iter()
                .all(|x| x.get("asset_id").and_then(|v| v.as_str()) == first);
            match (first, all_same) {
                (Some(id), true) => id.to_string(),
                (None, _) => return RouteResult::Unknown,
                (_, false) => return RouteResult::Multi,
            }
        }
        _ => return RouteResult::Unknown,
    };

    let token = TokenId::new(&asset_id);
    let guard = registry.lock().unwrap_or_else(|p| p.into_inner());
    let market = match guard.market_by_token(&token) {
        Some(m) => m,
        None => return RouteResult::UnknownToken(asset_id),
    };

    let base = if !market.slug.is_empty() {
        market.slug.clone()
    } else {
        market.id.as_str().to_string()
    };
    // Per-side files: each token has its own order book on Polymarket
    // CLOB; keeping yes/no separate makes per-side analysis trivial.
    let suffix = if token == market.yes_token {
        "yes"
    } else {
        "no"
    };
    RouteResult::Routed(format!("{base}-{suffix}"))
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use market_registry::{Market, MarketId, Outcome, Registry};

    fn registry_with(market: Market) -> Arc<Mutex<Registry>> {
        let mut r = Registry::new();
        r.upsert_all([market]);
        Arc::new(Mutex::new(r))
    }

    fn sample_market() -> Market {
        Market {
            id: MarketId::new("0xCONDITION"),
            title: "Bitcoin Up or Down".into(),
            slug: "btc-updown-5m-1776415200".into(),
            yes_token: TokenId::new("T-YES"),
            no_token: TokenId::new("T-NO"),
            start_time_epoch: None,
            end_time_epoch: 100,
            resolved_outcome: None,
        }
    }

    #[test]
    fn routes_single_object_to_yes_stream() {
        let r = registry_with(sample_market());
        let payload = r#"{"event_type":"book","asset_id":"T-YES","bids":[]}"#;
        let result = classify_and_route(payload, &r);
        assert_eq!(result, RouteResult::Routed("btc-updown-5m-1776415200-yes".into()));
    }

    #[test]
    fn routes_single_object_to_no_stream() {
        let r = registry_with(sample_market());
        let payload = r#"{"event_type":"book","asset_id":"T-NO","bids":[]}"#;
        let result = classify_and_route(payload, &r);
        assert_eq!(result, RouteResult::Routed("btc-updown-5m-1776415200-no".into()));
    }

    #[test]
    fn routes_homogeneous_array() {
        let r = registry_with(sample_market());
        let payload = r#"[{"asset_id":"T-YES","x":1},{"asset_id":"T-YES","x":2}]"#;
        let result = classify_and_route(payload, &r);
        assert_eq!(result, RouteResult::Routed("btc-updown-5m-1776415200-yes".into()));
    }

    #[test]
    fn mixed_array_routes_to_multi() {
        let r = registry_with(sample_market());
        let payload = r#"[{"asset_id":"T-YES"},{"asset_id":"T-NO"}]"#;
        let result = classify_and_route(payload, &r);
        assert_eq!(result, RouteResult::Multi);
    }

    #[test]
    fn missing_asset_id_routes_to_unknown() {
        let r = registry_with(sample_market());
        let payload = r#"{"event_type":"pong"}"#;
        let result = classify_and_route(payload, &r);
        assert_eq!(result, RouteResult::Unknown);
    }

    #[test]
    fn non_json_routes_to_unknown() {
        let r = registry_with(sample_market());
        let result = classify_and_route("PONG", &r);
        assert_eq!(result, RouteResult::Unknown);
    }

    #[test]
    fn unknown_token_logged_separately() {
        let r = registry_with(sample_market());
        let payload = r#"{"asset_id":"NEVER-SEEN"}"#;
        let result = classify_and_route(payload, &r);
        assert_eq!(result, RouteResult::UnknownToken("NEVER-SEEN".into()));
    }

    #[test]
    fn empty_array_routes_to_unknown() {
        let r = registry_with(sample_market());
        let result = classify_and_route("[]", &r);
        assert_eq!(result, RouteResult::Unknown);
    }

    #[test]
    fn falls_back_to_market_id_when_slug_empty() {
        let mut m = sample_market();
        m.slug = String::new();
        let r = registry_with(m);
        let payload = r#"{"asset_id":"T-YES"}"#;
        let result = classify_and_route(payload, &r);
        assert_eq!(result, RouteResult::Routed("0xCONDITION-yes".into()));
    }

    #[test]
    fn truncate_shortens_long_payloads() {
        let long = "a".repeat(1_000);
        let t = truncate(&long);
        assert!(t.ends_with("..."));
        assert_eq!(t.len(), RAW_LOG_TRUNCATE + 3);
    }
}
