//! Inbound frame processing.
//!
//! Keeps the read loop dumb: this module owns everything between "raw
//! text" and "row appended to storage".
//!
//! Design: the only thing we *look at* in the payload is the event type
//! (`"e"`) and the venue-reported timestamp (`"E"`, ms). We never rewrite
//! the payload — storage receives the exact bytes we received.

use std::sync::{Arc, Mutex};

use common::{LocalTimestamp, RawEvent, Venue};

use crate::FeedStats;

const RAW_LOG_TRUNCATE: usize = 256;

/// Process a single inbound text frame. Never panics, never drops the
/// payload silently: parse failures go to storage under a fallback stream
/// name and bump `FeedStats.parse_failures`.
pub(crate) fn process_text(
    payload: &str,
    cfg_streams: &[String],
    store: &Arc<Mutex<storage::Store>>,
    stats: &FeedStats,
) {
    let local_ts = LocalTimestamp::now();

    let (stream, venue_ts_ms) = match classify(payload) {
        Some(c) => {
            let stream = pick_stream(cfg_streams, &c.event_type)
                .unwrap_or_else(|| fallback_stream_name(&c.event_type));
            (stream, c.venue_ts_ms)
        }
        None => {
            stats.parse_failures.incr();
            tracing::warn!(
                component = "binance_feed",
                venue = "binance",
                event = "parse_failure",
                reason = "no event type recognised",
                raw = %truncate(payload),
                "storing under _unrouted"
            );
            ("_unrouted".to_string(), None)
        }
    };

    let event = RawEvent {
        venue: Venue::Binance,
        stream,
        local_ts_ns: local_ts,
        venue_ts_ms,
        payload: payload.to_string(),
        // Binance frames are 1:1 with events (no demux), so wire_batch_id
        // semantics don't apply.
        ..Default::default()
    };

    let store_t0 = std::time::Instant::now();
    {
        let mut guard = match store.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                tracing::error!(
                    component = "binance_feed",
                    venue = "binance",
                    event = "store_poisoned",
                    "recovering poisoned store mutex"
                );
                poisoned.into_inner()
            }
        };
        if let Err(e) = guard.write(&event) {
            stats.write_failures.incr();
            tracing::error!(
                component = "binance_feed",
                venue = "binance",
                event = "write_failure",
                reason = %e,
                "storage write failed"
            );
        }
    }
    let store_us = store_t0.elapsed().as_micros() as u64;
    stats.store_us.record_micros(store_us);
}

/// What we pull out of a Binance stream frame. Owning so the `RawEvent`
/// consumer doesn't have to manage borrow lifetimes.
#[derive(Debug, PartialEq, Eq)]
struct Classified {
    event_type: String,
    venue_ts_ms: Option<i64>,
}

/// Peek at `"e"` (or, for `bookTicker`, the structural fingerprint)
/// and `"E"`. Returns `None` for non-JSON, subscribe acks
/// (`{"result":null,"id":1}`), or any other payload we can't pin to a
/// known event type — the caller falls those back to `_unrouted`.
///
/// `bookTicker` payloads have no `"e"` field on Binance Spot, so we
/// identify them structurally: they carry `u`/`s`/`b`/`B`/`a`/`A`,
/// where `b`,`B`,`a`,`A` are decimal strings. This is narrow enough
/// to misclassify only if Binance ever ships another `e`-less event
/// with the exact same six fields — at which point we extend.
fn classify(payload: &str) -> Option<Classified> {
    let v: serde_json::Value = serde_json::from_str(payload).ok()?;

    if let Some(event_type) = v.get("e").and_then(|x| x.as_str()) {
        return Some(Classified {
            event_type: event_type.to_string(),
            venue_ts_ms: v.get("E").and_then(|x| x.as_i64()),
        });
    }

    if is_book_ticker_shape(&v) {
        return Some(Classified {
            event_type: "bookTicker".to_string(),
            // bookTicker has no venue-side timestamp; researchers rely
            // on local_ts_ns (which is exactly the right choice for
            // a sub-100 ms latency-sensitive signal anyway).
            venue_ts_ms: None,
        });
    }

    None
}

fn is_book_ticker_shape(v: &serde_json::Value) -> bool {
    v.get("u").is_some()
        && v.get("s").and_then(|x| x.as_str()).is_some()
        && v.get("b").and_then(|x| x.as_str()).is_some()
        && v.get("B").and_then(|x| x.as_str()).is_some()
        && v.get("a").and_then(|x| x.as_str()).is_some()
        && v.get("A").and_then(|x| x.as_str()).is_some()
}

/// Given a Binance event type, pick the matching entry from the config's
/// subscribed streams. The mapping is intentionally narrow — unknown types
/// return `None` so the caller can route to a fallback.
fn pick_stream(cfg_streams: &[String], event_type: &str) -> Option<String> {
    match event_type {
        "trade" => cfg_streams.iter().find(|s| s.ends_with("@trade")).cloned(),
        "depthUpdate" => cfg_streams
            .iter()
            // `@depth_snapshot` is a recorder-internal stream — exclude
            // it so depth diffs don't accidentally land under the
            // snapshot file.
            .find(|s| s.contains("@depth") && !s.ends_with("@depth_snapshot"))
            .cloned(),
        "kline" => cfg_streams.iter().find(|s| s.contains("@kline")).cloned(),
        "aggTrade" => cfg_streams.iter().find(|s| s.ends_with("@aggTrade")).cloned(),
        "bookTicker" => cfg_streams
            .iter()
            .find(|s| s.ends_with("@bookTicker"))
            .cloned(),
        _ => None,
    }
}

/// Fallback stream name for events we couldn't route. Keeps everything in
/// storage so nothing is silently dropped, and the event type is visible
/// in the filename so the problem is obvious.
fn fallback_stream_name(event_type: &str) -> String {
    format!("_unrouted_{event_type}")
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

    #[test]
    fn classifies_trade_frame() {
        let payload = r#"{"e":"trade","E":123,"s":"BTCUSDT","p":"100"}"#;
        let c = classify(payload).unwrap();
        assert_eq!(c.event_type, "trade");
        assert_eq!(c.venue_ts_ms, Some(123));
    }

    #[test]
    fn classifies_depth_frame() {
        let payload = r#"{"e":"depthUpdate","E":456,"s":"BTCUSDT","b":[],"a":[]}"#;
        let c = classify(payload).unwrap();
        assert_eq!(c.event_type, "depthUpdate");
        assert_eq!(c.venue_ts_ms, Some(456));
    }

    #[test]
    fn classify_returns_none_for_non_json() {
        assert!(classify("not json").is_none());
    }

    #[test]
    fn classify_returns_none_for_subscribe_ack() {
        // Binance's SUBSCRIBE reply has no `"e"` field.
        assert!(classify(r#"{"result":null,"id":1}"#).is_none());
    }

    #[test]
    fn classify_tolerates_missing_venue_ts() {
        let c = classify(r#"{"e":"trade","s":"X"}"#).unwrap();
        assert_eq!(c.event_type, "trade");
        assert_eq!(c.venue_ts_ms, None);
    }

    #[test]
    fn pick_stream_maps_known_event_types() {
        let cfg = vec![
            "btcusdt@trade".to_string(),
            "btcusdt@depth@100ms".to_string(),
        ];
        assert_eq!(
            pick_stream(&cfg, "trade"),
            Some("btcusdt@trade".to_string())
        );
        assert_eq!(
            pick_stream(&cfg, "depthUpdate"),
            Some("btcusdt@depth@100ms".to_string())
        );
    }

    #[test]
    fn pick_stream_returns_none_when_not_subscribed() {
        let cfg = vec!["btcusdt@trade".to_string()];
        // kline not subscribed — caller will fall back.
        assert_eq!(pick_stream(&cfg, "kline"), None);
        assert_eq!(pick_stream(&cfg, "gibberish"), None);
    }

    #[test]
    fn classifies_book_ticker_without_e_field() {
        // Real bookTicker shape — no "e", no "E", just BBO snapshot.
        let payload = r#"{"u":400900217,"s":"BTCUSDT","b":"78326.28","B":"0.069","a":"78326.29","A":"2.232"}"#;
        let c = classify(payload).unwrap();
        assert_eq!(c.event_type, "bookTicker");
        assert_eq!(c.venue_ts_ms, None);
    }

    #[test]
    fn book_ticker_shape_detector_rejects_partial_payloads() {
        // Has `u` but missing `B` qty — not a full BBO frame.
        let v: serde_json::Value =
            serde_json::from_str(r#"{"u":1,"s":"X","b":"1","a":"2","A":"3"}"#).unwrap();
        assert!(!is_book_ticker_shape(&v));
    }

    #[test]
    fn pick_stream_routes_book_ticker() {
        let cfg = vec![
            "btcusdt@trade".to_string(),
            "btcusdt@depth@100ms".to_string(),
            "btcusdt@bookTicker".to_string(),
        ];
        assert_eq!(
            pick_stream(&cfg, "bookTicker"),
            Some("btcusdt@bookTicker".to_string())
        );
    }

    #[test]
    fn pick_stream_depth_excludes_snapshot_pseudo_stream() {
        // `btcusdt@depth_snapshot` is what the recorder uses for the REST
        // snapshot — must not absorb diff routing.
        let cfg = vec![
            "btcusdt@depth_snapshot".to_string(),
            "btcusdt@depth@100ms".to_string(),
        ];
        assert_eq!(
            pick_stream(&cfg, "depthUpdate"),
            Some("btcusdt@depth@100ms".to_string())
        );
    }

    #[test]
    fn fallback_stream_name_includes_event_type() {
        assert_eq!(fallback_stream_name("kline"), "_unrouted_kline");
    }

    #[test]
    fn truncate_shortens_long_payloads() {
        let long = "a".repeat(1_000);
        let t = truncate(&long);
        assert!(t.ends_with("..."));
        assert_eq!(t.len(), RAW_LOG_TRUNCATE + 3);
    }

    #[test]
    fn truncate_leaves_short_payloads_intact() {
        assert_eq!(truncate("short"), "short");
    }
}
