//! Inbound frame processing.
//!
//! ## Wire format (verified from live Polymarket CLOB)
//!
//! Polymarket sends JSON objects (sometimes batches) keyed by either:
//! * a top-level `market` field — the on-chain `conditionId` (hex). Used
//!   on `price_change` style messages where one event covers many price
//!   levels for one market.
//! * a top-level `asset_id` field — the CLOB token id. Used on per-side
//!   messages like `book` snapshots and `last_trade_price`.
//!
//! ## Routing
//!
//! One file per **market** (not per side). Yes/No data is interleaved in
//! the same NDJSON file; downstream replay demuxes by reading `asset_id`
//! from each row's payload. This keeps file count manageable
//! (~37 markets right now, ~200 at peak) and matches how the venue's
//! own messages are scoped.
//!
//! Unrecognised payloads land in `_unrouted` / `_unknown_market-<id>` /
//! `_unknown_token-<id>` so nothing is silently dropped.

use std::sync::{Arc, Mutex};

use common::{LocalTimestamp, RawEvent, Venue};
use market_registry::{MarketId, Registry, TokenId};
use serde_json::Value;

use crate::FeedStats;

const RAW_LOG_TRUNCATE: usize = 256;

pub(crate) fn process_text(
    payload: &str,
    registry: &Arc<Mutex<Registry>>,
    store: &Arc<Mutex<storage::Store>>,
    stats: &FeedStats,
) {
    let local_ts = LocalTimestamp::now();
    let stream = stream_for_payload(payload, registry, stats);

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

/// Determine the storage stream name for a payload, logging into `stats`
/// for each fallback path.
fn stream_for_payload(
    payload: &str,
    registry: &Arc<Mutex<Registry>>,
    stats: &FeedStats,
) -> String {
    match classify_and_route(payload, registry) {
        RouteResult::Routed(s) => s,
        RouteResult::Multi => {
            tracing::debug!(
                component = "polymarket_feed",
                venue = "polymarket",
                event = "multi_market_frame",
                raw = %truncate(payload),
                "frame spans >1 market; storing under _multi"
            );
            "_multi".to_string()
        }
        RouteResult::Unknown => {
            stats.parse_failures.incr();
            tracing::warn!(
                component = "polymarket_feed",
                venue = "polymarket",
                event = "parse_failure",
                reason = "no market or asset_id in payload",
                raw = %truncate(payload),
                "storing under _unrouted"
            );
            "_unrouted".to_string()
        }
        RouteResult::UnknownMarket(id) => {
            stats.parse_failures.incr();
            tracing::warn!(
                component = "polymarket_feed",
                venue = "polymarket",
                event = "unknown_market",
                market = %id,
                raw = %truncate(payload),
                "market not in registry; storing under _unknown_market-<id>"
            );
            // Sanitised by storage; the conditionId hex is alphanumeric+0x
            // which sanitises to alnum after stripping non-word chars.
            format!("_unknown_market-{id}")
        }
        RouteResult::UnknownToken(id) => {
            stats.parse_failures.incr();
            tracing::warn!(
                component = "polymarket_feed",
                venue = "polymarket",
                event = "unknown_token",
                token = %id,
                raw = %truncate(payload),
                "asset_id not in registry; storing under _unknown_token-<id>"
            );
            format!("_unknown_token-{id}")
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RouteResult {
    /// Successfully matched to a known market — derived stream name.
    Routed(String),
    /// Heterogeneous batch covering >1 market.
    Multi,
    /// Could not even extract an identifier (non-JSON, missing fields).
    Unknown,
    /// Extracted a `market` (conditionId) but no market in registry.
    UnknownMarket(String),
    /// Extracted an `asset_id` but no market in registry owns that token.
    UnknownToken(String),
}

fn classify_and_route(
    payload: &str,
    registry: &Arc<Mutex<Registry>>,
) -> RouteResult {
    let v: Value = match serde_json::from_str(payload) {
        Ok(v) => v,
        Err(_) => return RouteResult::Unknown,
    };

    let guard = registry.lock().unwrap_or_else(|p| p.into_inner());
    let resolve = |item: &Value| -> ItemRoute {
        if let Some(market_id) = item.get("market").and_then(|x| x.as_str()) {
            return match guard.get(&MarketId::new(market_id)) {
                Some(m) => ItemRoute::Stream(stream_for(m)),
                None => ItemRoute::UnknownMarket(market_id.to_string()),
            };
        }
        if let Some(asset_id) = item.get("asset_id").and_then(|x| x.as_str()) {
            return match guard.market_by_token(&TokenId::new(asset_id)) {
                Some(m) => ItemRoute::Stream(stream_for(m)),
                None => ItemRoute::UnknownToken(asset_id.to_string()),
            };
        }
        ItemRoute::Unknown
    };

    match &v {
        Value::Object(_) => resolve(&v).into_route_result(),
        Value::Array(arr) if !arr.is_empty() => {
            let routes: Vec<_> = arr.iter().map(resolve).collect();
            let first = &routes[0];
            let all_same = routes.iter().all(|r| r == first);
            if !all_same {
                RouteResult::Multi
            } else {
                first.clone().into_route_result()
            }
        }
        _ => RouteResult::Unknown,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ItemRoute {
    Stream(String),
    UnknownMarket(String),
    UnknownToken(String),
    Unknown,
}

impl ItemRoute {
    fn into_route_result(self) -> RouteResult {
        match self {
            ItemRoute::Stream(s) => RouteResult::Routed(s),
            ItemRoute::UnknownMarket(id) => RouteResult::UnknownMarket(id),
            ItemRoute::UnknownToken(id) => RouteResult::UnknownToken(id),
            ItemRoute::Unknown => RouteResult::Unknown,
        }
    }
}

fn stream_for(market: &market_registry::Market) -> String {
    if !market.slug.is_empty() {
        market.slug.clone()
    } else {
        market.id.as_str().to_string()
    }
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
    use market_registry::{Market, Outcome};

    fn market(slug: &str, condition: &str, yes: &str, no: &str) -> Market {
        Market {
            id: MarketId::new(condition),
            title: "Bitcoin Up or Down".into(),
            slug: slug.into(),
            yes_token: TokenId::new(yes),
            no_token: TokenId::new(no),
            start_time_epoch: None,
            end_time_epoch: 100,
            resolved_outcome: None,
        }
    }

    fn registry_with(markets: Vec<Market>) -> Arc<Mutex<Registry>> {
        let mut r = Registry::new();
        r.upsert_all(markets);
        Arc::new(Mutex::new(r))
    }

    #[test]
    fn routes_price_change_batch_by_market_field() {
        // Live Polymarket wire format: top-level `market`, `price_changes` array.
        let r = registry_with(vec![market(
            "btc-updown-5m-1776415200",
            "0xCONDITION",
            "T-YES",
            "T-NO",
        )]);
        let payload = r#"{
            "market": "0xCONDITION",
            "price_changes": [
                {"asset_id": "T-YES", "price": "0.62", "size": "10", "side": "BUY"},
                {"asset_id": "T-NO",  "price": "0.38", "size": "10", "side": "SELL"}
            ]
        }"#;
        let result = classify_and_route(payload, &r);
        assert_eq!(result, RouteResult::Routed("btc-updown-5m-1776415200".into()));
    }

    #[test]
    fn routes_book_event_by_top_level_asset_id() {
        let r = registry_with(vec![market(
            "btc-updown-5m-1776415200",
            "0xCONDITION",
            "T-YES",
            "T-NO",
        )]);
        let payload = r#"{"event_type":"book","asset_id":"T-YES","bids":[]}"#;
        let result = classify_and_route(payload, &r);
        assert_eq!(result, RouteResult::Routed("btc-updown-5m-1776415200".into()));
    }

    #[test]
    fn array_all_same_market_routes_to_that_market() {
        let r = registry_with(vec![market(
            "btc-updown-5m-1776415200",
            "0xC1",
            "T-YES",
            "T-NO",
        )]);
        let payload = r#"[
            {"market":"0xC1","price_changes":[]},
            {"market":"0xC1","price_changes":[]}
        ]"#;
        let result = classify_and_route(payload, &r);
        assert_eq!(result, RouteResult::Routed("btc-updown-5m-1776415200".into()));
    }

    #[test]
    fn array_mixed_markets_routes_to_multi() {
        let r = registry_with(vec![
            market("slug-a", "0xA", "TA-YES", "TA-NO"),
            market("slug-b", "0xB", "TB-YES", "TB-NO"),
        ]);
        let payload = r#"[
            {"market":"0xA","price_changes":[]},
            {"market":"0xB","price_changes":[]}
        ]"#;
        let result = classify_and_route(payload, &r);
        assert_eq!(result, RouteResult::Multi);
    }

    #[test]
    fn unknown_market_id_reported_separately() {
        let r = registry_with(vec![]);
        let payload = r#"{"market":"0xMYSTERY","price_changes":[]}"#;
        let result = classify_and_route(payload, &r);
        assert_eq!(result, RouteResult::UnknownMarket("0xMYSTERY".into()));
    }

    #[test]
    fn unknown_asset_id_reported_separately() {
        let r = registry_with(vec![]);
        let payload = r#"{"event_type":"book","asset_id":"NEVER-SEEN"}"#;
        let result = classify_and_route(payload, &r);
        assert_eq!(result, RouteResult::UnknownToken("NEVER-SEEN".into()));
    }

    #[test]
    fn no_identifier_routes_to_unknown() {
        let r = registry_with(vec![]);
        let payload = r#"{"event_type":"pong"}"#;
        let result = classify_and_route(payload, &r);
        assert_eq!(result, RouteResult::Unknown);
    }

    #[test]
    fn non_json_routes_to_unknown() {
        let r = registry_with(vec![]);
        let result = classify_and_route("hello world", &r);
        assert_eq!(result, RouteResult::Unknown);
    }

    #[test]
    fn empty_array_routes_to_unknown() {
        let r = registry_with(vec![]);
        let result = classify_and_route("[]", &r);
        assert_eq!(result, RouteResult::Unknown);
    }

    #[test]
    fn falls_back_to_market_id_when_slug_empty() {
        let mut m = market("", "0xC1", "T-YES", "T-NO");
        m.slug = String::new();
        let r = registry_with(vec![m]);
        let payload = r#"{"market":"0xC1"}"#;
        let result = classify_and_route(payload, &r);
        assert_eq!(result, RouteResult::Routed("0xC1".into()));
    }

    #[test]
    fn truncate_shortens_long_payloads() {
        let long = "a".repeat(1_000);
        let t = truncate(&long);
        assert!(t.ends_with("..."));
        assert_eq!(t.len(), RAW_LOG_TRUNCATE + 3);
    }

    #[test]
    fn _unused_outcome_import_keeps_compile_clean() {
        // Touch Outcome so the `use market_registry::...Outcome` import
        // isn't flagged as unused. (Outcome is referenced indirectly via
        // Market::resolved_outcome but the linter likes explicit use.)
        let _ = Outcome::Yes;
    }
}
