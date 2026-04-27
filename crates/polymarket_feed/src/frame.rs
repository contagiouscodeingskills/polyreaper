//! Inbound frame processing.
//!
//! ## Wire format (verified from live Polymarket CLOB)
//!
//! Polymarket sends JSON in two shapes:
//! * **Single object** — one event for one market:
//!   ```json
//!   {"market":"0x...","price_changes":[{"asset_id":"...","price":"0.62"}]}
//!   ```
//! * **Array** — a *batch* of events that may target several different
//!   markets at once. The batching is a wire-level optimisation; logically
//!   each array element is independent.
//!
//! Each event identifies its market by either:
//! * top-level `market` field — the on-chain `conditionId` (hex). Used on
//!   `price_change` style events.
//! * top-level `asset_id` field — the CLOB token id. Used on `book` /
//!   `last_trade_price` style events.
//!
//! ## Routing — one file per market
//!
//! Goal: per-market replay is trivial (`cat polymarket/<slug>.ndjson |
//! jq ...`).
//!
//! Strategy:
//! * Object payloads → write as-is, routed by their identifier.
//! * Array payloads → **demux**: write one record per array element, each
//!   routed individually. All elements from one wire frame share the same
//!   `local_ts_ns` so wire-batching can still be recovered after the fact
//!   by grouping on that timestamp.
//!
//! Trade-off: the per-record `payload` for a demuxed event is the
//! re-serialised JSON of that single element, not the original substring
//! of the wire frame. Field ordering and whitespace may differ; semantic
//! content is preserved. This is the price of per-market replay.
//!
//! Unrecognised events go to `_unrouted` / `_unknown_market-<id>` /
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

    let parsed: Result<Value, _> = serde_json::from_str(payload);
    match parsed {
        Ok(Value::Array(arr)) if !arr.is_empty() => {
            // Demux: each array element becomes its own RawEvent.
            for item in &arr {
                let item_payload = match serde_json::to_string(item) {
                    Ok(s) => s,
                    Err(_) => {
                        // Re-serialising a parsed Value should never fail
                        // in practice, but skip cleanly if it does.
                        stats.parse_failures.incr();
                        continue;
                    }
                };
                let stream = stream_for_value(item, registry, stats);
                write_one(local_ts, stream, item_payload, store, stats);
            }
        }
        Ok(_) => {
            // Single object (or scalar/null) — route as-is.
            let stream = stream_for_payload(payload, registry, stats);
            write_one(local_ts, stream, payload.to_string(), store, stats);
        }
        Err(_) => {
            stats.parse_failures.incr();
            tracing::warn!(
                component = "polymarket_feed",
                venue = "polymarket",
                event = "parse_failure",
                reason = "non-JSON payload",
                raw = %truncate(payload),
                "storing under _unrouted"
            );
            write_one(
                local_ts,
                "_unrouted".to_string(),
                payload.to_string(),
                store,
                stats,
            );
        }
    }
}

/// Pick a stream name for a payload that's already been parsed and
/// re-serialised (no need to re-parse).
fn stream_for_value(
    item: &Value,
    registry: &Arc<Mutex<Registry>>,
    stats: &FeedStats,
) -> String {
    let route = {
        let guard = registry.lock().unwrap_or_else(|p| p.into_inner());
        resolve_one(item, &guard)
    };
    label_route(route, item, stats, /*is_demuxed=*/ true)
}

/// Pick a stream name for a payload by re-parsing it. Used for
/// non-array (single-object) payloads.
fn stream_for_payload(
    payload: &str,
    registry: &Arc<Mutex<Registry>>,
    stats: &FeedStats,
) -> String {
    let value: Value = match serde_json::from_str(payload) {
        Ok(v) => v,
        Err(_) => return "_unrouted".to_string(),
    };
    let route = {
        let guard = registry.lock().unwrap_or_else(|p| p.into_inner());
        resolve_one(&value, &guard)
    };
    label_route(route, &value, stats, /*is_demuxed=*/ false)
}

fn label_route(
    route: ItemRoute,
    item: &Value,
    stats: &FeedStats,
    is_demuxed: bool,
) -> String {
    match route {
        ItemRoute::Stream(s) => s,
        ItemRoute::UnknownMarket(id) => {
            stats.parse_failures.incr();
            tracing::warn!(
                component = "polymarket_feed",
                venue = "polymarket",
                event = "unknown_market",
                market = %id,
                demuxed = is_demuxed,
                raw = %truncate(&item.to_string()),
                "market not in registry"
            );
            format!("_unknown_market-{id}")
        }
        ItemRoute::UnknownToken(id) => {
            stats.parse_failures.incr();
            tracing::warn!(
                component = "polymarket_feed",
                venue = "polymarket",
                event = "unknown_token",
                token = %id,
                demuxed = is_demuxed,
                raw = %truncate(&item.to_string()),
                "asset_id not in registry"
            );
            format!("_unknown_token-{id}")
        }
        ItemRoute::Unknown => {
            stats.parse_failures.incr();
            tracing::warn!(
                component = "polymarket_feed",
                venue = "polymarket",
                event = "parse_failure",
                reason = "no market or asset_id",
                demuxed = is_demuxed,
                raw = %truncate(&item.to_string()),
                "no identifier in payload"
            );
            "_unrouted".to_string()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ItemRoute {
    Stream(String),
    UnknownMarket(String),
    UnknownToken(String),
    Unknown,
}

/// Resolve one parsed JSON object against a registry snapshot.
/// Caller holds the registry lock for the duration.
fn resolve_one(item: &Value, registry: &Registry) -> ItemRoute {
    if let Some(market_id) = item.get("market").and_then(|x| x.as_str()) {
        return match registry.get(&MarketId::new(market_id)) {
            Some(m) => ItemRoute::Stream(stream_for(m)),
            None => ItemRoute::UnknownMarket(market_id.to_string()),
        };
    }
    if let Some(asset_id) = item.get("asset_id").and_then(|x| x.as_str()) {
        return match registry.market_by_token(&TokenId::new(asset_id)) {
            Some(m) => ItemRoute::Stream(stream_for(m)),
            None => ItemRoute::UnknownToken(asset_id.to_string()),
        };
    }
    ItemRoute::Unknown
}

fn stream_for(market: &market_registry::Market) -> String {
    if !market.slug.is_empty() {
        market.slug.clone()
    } else {
        market.id.as_str().to_string()
    }
}

fn write_one(
    local_ts: LocalTimestamp,
    stream: String,
    payload: String,
    store: &Arc<Mutex<storage::Store>>,
    stats: &FeedStats,
) {
    let event = RawEvent {
        venue: Venue::Polymarket,
        stream,
        local_ts_ns: local_ts,
        venue_ts_ms: None, // Polymarket events don't carry a uniform ts field
        payload,
    };
    let store_t0 = std::time::Instant::now();
    {
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
    let store_us = store_t0.elapsed().as_micros() as u64;
    stats.store_us.record_micros(store_us);
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
    use market_registry::Market;

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

    fn resolve(item: &Value, r: &Arc<Mutex<Registry>>) -> ItemRoute {
        let g = r.lock().unwrap();
        resolve_one(item, &g)
    }

    #[test]
    fn resolves_object_by_market_field() {
        let r = registry_with(vec![market("slug-A", "0xA", "Y", "N")]);
        let v: Value = serde_json::from_str(r#"{"market":"0xA"}"#).unwrap();
        assert_eq!(resolve(&v, &r), ItemRoute::Stream("slug-A".into()));
    }

    #[test]
    fn resolves_object_by_asset_id() {
        let r = registry_with(vec![market("slug-A", "0xA", "Y", "N")]);
        let v: Value = serde_json::from_str(r#"{"asset_id":"Y"}"#).unwrap();
        assert_eq!(resolve(&v, &r), ItemRoute::Stream("slug-A".into()));
    }

    #[test]
    fn unknown_market_id_returned_distinctly() {
        let r = registry_with(vec![]);
        let v: Value = serde_json::from_str(r#"{"market":"0xMYSTERY"}"#).unwrap();
        assert_eq!(
            resolve(&v, &r),
            ItemRoute::UnknownMarket("0xMYSTERY".into())
        );
    }

    #[test]
    fn unknown_asset_id_returned_distinctly() {
        let r = registry_with(vec![]);
        let v: Value = serde_json::from_str(r#"{"asset_id":"NOPE"}"#).unwrap();
        assert_eq!(resolve(&v, &r), ItemRoute::UnknownToken("NOPE".into()));
    }

    #[test]
    fn no_identifier_returns_unknown() {
        let r = registry_with(vec![]);
        let v: Value = serde_json::from_str(r#"{"event_type":"pong"}"#).unwrap();
        assert_eq!(resolve(&v, &r), ItemRoute::Unknown);
    }

    #[test]
    fn falls_back_to_market_id_when_slug_empty() {
        let mut m = market("slug-A", "0xA", "Y", "N");
        m.slug = String::new();
        let r = registry_with(vec![m]);
        let v: Value = serde_json::from_str(r#"{"market":"0xA"}"#).unwrap();
        assert_eq!(resolve(&v, &r), ItemRoute::Stream("0xA".into()));
    }

    #[test]
    fn truncate_shortens_long_payloads() {
        let long = "a".repeat(1_000);
        let t = truncate(&long);
        assert!(t.ends_with("..."));
        assert_eq!(t.len(), RAW_LOG_TRUNCATE + 3);
    }

    #[test]
    fn stream_for_uses_slug_then_id() {
        assert_eq!(
            stream_for(&market("nice-slug", "0xC", "Y", "N")),
            "nice-slug"
        );
        let mut m = market("", "0xC", "Y", "N");
        m.slug = String::new();
        assert_eq!(stream_for(&m), "0xC");
    }
}
