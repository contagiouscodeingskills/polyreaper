//! Shared primitive types for the PolyBot workspace.
//!
//! Keep this crate small: only types genuinely shared across recorder /
//! replayer / researcher belong here. The single biggest thing it owns is
//! [`RawEvent`] — the replay contract.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

pub const NAME: &str = "common";

// ---------------------------------------------------------------------------
// Venue
// ---------------------------------------------------------------------------

/// Venue identifier. Closed enum so downstream code cannot accidentally
/// route Binance data into a Polymarket code path or vice versa.
///
/// Serializes as the lowercase venue name, keeping the wire format stable
/// even if the Rust variant names are ever renamed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Venue {
    Binance,
    Polymarket,
    Coinbase,
    Chainlink,
}

impl Venue {
    pub fn as_str(self) -> &'static str {
        match self {
            Venue::Binance => "binance",
            Venue::Polymarket => "polymarket",
            Venue::Coinbase => "coinbase",
            Venue::Chainlink => "chainlink",
        }
    }
}

// ---------------------------------------------------------------------------
// LocalTimestamp
// ---------------------------------------------------------------------------

/// Local receive timestamp, nanoseconds since the UNIX epoch.
///
/// Every inbound market-data event must carry one of these, stamped at the
/// moment the bytes are handed to our process. This is the only clock we
/// fully trust for ordering across venues.
///
/// **Wire format:** serialized as a decimal string (not a JSON number).
/// Nanoseconds-since-epoch exceed JSON's 2^53 safe-integer range, so any
/// consumer that parses JSON through an f64 (Python's `json` module,
/// JavaScript, `jq`) would lose precision on a numeric encoding. A string
/// preserves the value exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct LocalTimestamp(u128);

impl LocalTimestamp {
    /// Capture the current wall-clock time.
    pub fn now() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        Self(nanos)
    }

    pub fn from_nanos(nanos: u128) -> Self {
        Self(nanos)
    }

    pub fn as_nanos(self) -> u128 {
        self.0
    }
}

impl std::fmt::Display for LocalTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Matches the serialized form so log interpolation reads identically
        // to NDJSON records.
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl Serialize for LocalTimestamp {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for LocalTimestamp {
    /// Accepts both the current wire format (decimal string) and the
    /// legacy format (JSON integer) used by sessions captured before
    /// 2026-04-23. New writes always emit the string form; the lenient
    /// reader keeps historical data reachable from the replayer.
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = u128;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("decimal string or non-negative integer of nanoseconds since UNIX epoch")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<u128, E> {
                v.parse::<u128>().map_err(E::custom)
            }
            fn visit_string<E: serde::de::Error>(self, v: String) -> Result<u128, E> {
                self.visit_str(&v)
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<u128, E> {
                Ok(v as u128)
            }
            fn visit_u128<E: serde::de::Error>(self, v: u128) -> Result<u128, E> {
                Ok(v)
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<u128, E> {
                if v < 0 {
                    Err(E::custom("negative timestamp"))
                } else {
                    Ok(v as u128)
                }
            }
            fn visit_i128<E: serde::de::Error>(self, v: i128) -> Result<u128, E> {
                if v < 0 {
                    Err(E::custom("negative timestamp"))
                } else {
                    Ok(v as u128)
                }
            }
        }
        d.deserialize_any(V).map(LocalTimestamp)
    }
}

// ---------------------------------------------------------------------------
// RawEvent — the replay contract
// ---------------------------------------------------------------------------

/// The canonical shape every inbound venue message is wrapped in before
/// persistence. NDJSON rows produced by the recorder deserialize into this
/// exact type; this type is the replay contract.
///
/// Fields are owning so the event can move across channels / threads
/// without lifetime juggling. The per-message allocations (one `String` for
/// `stream`, one for `payload`) are deliberate — Phase 1 prioritises
/// correctness and simplicity over allocator work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RawEvent {
    pub venue: Venue,

    /// Venue-specific stream / channel identifier — e.g. `"btcusdt@trade"`
    /// for Binance or a market id for Polymarket. Opaque to this crate;
    /// kept so the replayer can route records without re-parsing payload.
    pub stream: String,

    /// Local receive timestamp, nanoseconds since epoch.
    pub local_ts_ns: LocalTimestamp,

    /// Venue-reported timestamp, milliseconds since epoch, if the payload
    /// carried one. Kept separate from `local_ts_ns` so readers always know
    /// which clock they're looking at.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub venue_ts_ms: Option<i64>,

    /// Raw payload, exactly as received. UTF-8 text for Phase 1 (see
    /// `docs/TECH_DEBT.md` §2 for binary support).
    pub payload: String,

    /// Per-WS-frame identifier for venues that demux a single wire frame
    /// into multiple events (Polymarket array frames). Events with the
    /// same `wire_batch_id` came from the same `ws.next()` call. Always
    /// `None` for venues that don't demux. Populated for Polymarket
    /// captures from recorder v0.1.0+ onwards; older captures have it
    /// absent (handled via `serde(default)`).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub wire_batch_id: Option<u64>,

    /// 0-based position of this event within its WS frame. `0` for
    /// non-demuxed events; `0..n-1` for the n elements of a demuxed
    /// array. Pairs with `wire_batch_id` to reconstruct the original
    /// frame at replay time.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub event_index_in_batch: Option<u32>,
}

impl Default for Venue {
    fn default() -> Self {
        Venue::Binance
    }
}

// ---------------------------------------------------------------------------
// ResolutionRecord — sidecar resolution metadata
// ---------------------------------------------------------------------------

/// One per-market resolution record. Written one-per-line into
/// `<session>/_resolutions.ndjson` by the recorder's resolution sweeper,
/// read by the replayer and any downstream analysis that needs
/// ground-truth Up/Down outcomes.
///
/// **Why a sidecar, not a `RawEvent`?** Resolutions come from a REST API
/// (Polymarket Gamma `/events?closed=true`), not a venue WebSocket. They
/// describe metadata about the *market*, not a market event. Mixing them
/// into the venue stream would muddy replay; keeping them in a single
/// session-level file is also robust against the per-slug 0-byte-file
/// failure mode the previous sweeper hit during disk-pressure events.
///
/// **Schema versioning.** `schema_version` lets future readers detect
/// breaking changes; bump when the shape changes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolutionRecord {
    pub schema_version: u32,
    /// Local wall-clock when the sweeper observed this resolution, ns
    /// since epoch. Stringified for the same precision-preserving reason
    /// `RawEvent.local_ts_ns` is.
    pub ts_ns: String,
    /// Identifier of the source/method, e.g. `"gamma_v1"`. Lets analysis
    /// distinguish records produced by different sources if we ever add
    /// alternates.
    pub source: String,
    pub market: ResolutionMarket,
    pub tokens: ResolutionTokens,
    pub outcome: ResolutionOutcome,
    /// The full re-serialised gamma event JSON for forensics. Kept so
    /// future analysis can recover any field this struct doesn't expose
    /// without re-querying gamma.
    pub raw_gamma_event: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolutionMarket {
    pub slug: String,
    pub condition_id: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub question: Option<String>,
    /// ISO-8601 strings as gamma reports them. Kept verbatim.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub start_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub end_date: Option<String>,
    /// Epoch seconds parsed from the slug suffix or end_date. Useful for
    /// numeric range filters.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub start_time_epoch: Option<i64>,
    pub end_time_epoch: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolutionTokens {
    /// `clobTokenIds[0]` from gamma — the token that pays out when the
    /// first outcome (e.g. `"Up"` for the BTC up/down series) wins.
    /// Known at market *creation* time, not at resolution time, so this
    /// field is leakage-free for any predictive use.
    pub up_token: String,
    pub down_token: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolutionOutcome {
    /// The label of the winning outcome — e.g. `"Up"` or `"Down"` for
    /// the BTC series, or `"Yes"`/`"No"` for binary markets in other
    /// series. Comes from gamma's `outcomes[i]` where `outcomePrices[i] == "1"`.
    /// **Never** inferred from terminal market price.
    pub winner_label: String,
    /// Both outcome labels, in declaration order (matches `clobTokenIds`).
    pub outcome_labels: Vec<String>,
    /// `outcomePrices` from gamma, declaration order. Will be `["1","0"]`
    /// or `["0","1"]` for cleanly-resolved markets.
    pub outcome_prices: Vec<String>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn venue_serializes_lowercase_and_round_trips() {
        for v in [Venue::Binance, Venue::Polymarket] {
            let s = serde_json::to_string(&v).unwrap();
            assert_eq!(s, format!("\"{}\"", v.as_str()));
            let parsed: Venue = serde_json::from_str(&s).unwrap();
            assert_eq!(v, parsed);
        }
    }

    #[test]
    fn local_timestamp_deserialize_accepts_legacy_integer_form() {
        // Sessions captured before 2026-04-23 wrote local_ts_ns as a JSON
        // integer. The replayer must still load them.
        let legacy = r#"1776944389779198900"#;
        let parsed: LocalTimestamp = serde_json::from_str(legacy).unwrap();
        assert_eq!(parsed.as_nanos(), 1776944389779198900);
    }

    #[test]
    fn local_timestamp_serializes_as_string_to_preserve_precision() {
        // 2^53 + 1 — the first integer that f64 cannot represent exactly.
        // A numeric encoding would collapse this to 2^53 in any JSON parser
        // that goes through f64.
        let t = LocalTimestamp::from_nanos(9_007_199_254_740_993);
        let s = serde_json::to_string(&t).unwrap();
        assert_eq!(s, r#""9007199254740993""#);
        let round: LocalTimestamp = serde_json::from_str(&s).unwrap();
        assert_eq!(t, round);
    }

    #[test]
    fn raw_event_round_trips_through_serde_json() {
        let event = RawEvent {
            venue: Venue::Polymarket,
            stream: "market-X".into(),
            local_ts_ns: LocalTimestamp::from_nanos(1_776_768_000_000_000_000),
            venue_ts_ms: Some(1_776_768_000_000),
            payload: r#"{"hello":"world"}"#.into(),
            ..Default::default()
        };
        let line = serde_json::to_string(&event).unwrap();
        let parsed: RawEvent = serde_json::from_str(&line).unwrap();
        assert_eq!(event, parsed);
    }

    #[test]
    fn venue_ts_ms_is_omitted_when_none() {
        let event = RawEvent {
            venue: Venue::Binance,
            stream: "btcusdt@trade".into(),
            local_ts_ns: LocalTimestamp::from_nanos(1),
            venue_ts_ms: None,
            payload: "x".into(),
            ..Default::default()
        };
        let line = serde_json::to_string(&event).unwrap();
        assert!(!line.contains("venue_ts_ms"), "got {line}");
    }

    #[test]
    fn raw_event_loads_legacy_pre_phase5_lines() {
        // Lines captured before Phase 5 had no wire_batch_id /
        // event_index_in_batch fields. Loading them must succeed with
        // both new fields defaulting to None.
        let legacy_line = r#"{"venue":"polymarket","stream":"x","local_ts_ns":"1","payload":"p"}"#;
        let parsed: RawEvent = serde_json::from_str(legacy_line).unwrap();
        assert_eq!(parsed.venue, Venue::Polymarket);
        assert_eq!(parsed.local_ts_ns.as_nanos(), 1);
        assert_eq!(parsed.wire_batch_id, None);
        assert_eq!(parsed.event_index_in_batch, None);
    }

    #[test]
    fn raw_event_wire_format_is_self_documenting() {
        // Locks the on-disk shape. Changing this test means changing the
        // replay contract — NDJSON rows already on disk may stop loading.
        let event = RawEvent {
            venue: Venue::Binance,
            stream: "btcusdt@trade".into(),
            local_ts_ns: LocalTimestamp::from_nanos(1),
            venue_ts_ms: Some(42),
            payload: "p".into(),
            ..Default::default()
        };
        let line = serde_json::to_string(&event).unwrap();
        assert_eq!(
            line,
            r#"{"venue":"binance","stream":"btcusdt@trade","local_ts_ns":"1","venue_ts_ms":42,"payload":"p"}"#
        );
    }

    #[test]
    fn resolution_record_round_trips_through_serde() {
        let r = ResolutionRecord {
            schema_version: 1,
            ts_ns: "1777523971487807791".into(),
            source: "gamma_v1".into(),
            market: ResolutionMarket {
                slug: "btc-updown-5m-1777206000".into(),
                condition_id: "0xabc".into(),
                question: Some("Bitcoin Up or Down — 12:30 PM ET".into()),
                start_date: Some("2026-04-26T12:25:00Z".into()),
                end_date: Some("2026-04-26T12:30:00Z".into()),
                start_time_epoch: Some(1_777_205_700),
                end_time_epoch: 1_777_206_000,
            },
            tokens: ResolutionTokens {
                up_token: "10161".into(),
                down_token: "11061".into(),
            },
            outcome: ResolutionOutcome {
                winner_label: "Up".into(),
                outcome_labels: vec!["Up".into(), "Down".into()],
                outcome_prices: vec!["1".into(), "0".into()],
            },
            raw_gamma_event: r#"{"slug":"btc-updown-5m-1777206000"}"#.into(),
        };
        let line = serde_json::to_string(&r).unwrap();
        assert!(!line.contains('\n'));
        let parsed: ResolutionRecord = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed, r);
    }

    #[test]
    fn resolution_record_omits_optional_fields_when_none() {
        let r = ResolutionRecord {
            schema_version: 1,
            ts_ns: "0".into(),
            source: "gamma_v1".into(),
            market: ResolutionMarket {
                slug: "x".into(),
                condition_id: "0x0".into(),
                question: None,
                start_date: None,
                end_date: None,
                start_time_epoch: None,
                end_time_epoch: 0,
            },
            tokens: ResolutionTokens {
                up_token: "u".into(),
                down_token: "d".into(),
            },
            outcome: ResolutionOutcome {
                winner_label: "Up".into(),
                outcome_labels: vec!["Up".into(), "Down".into()],
                outcome_prices: vec!["1".into(), "0".into()],
            },
            raw_gamma_event: "{}".into(),
        };
        let line = serde_json::to_string(&r).unwrap();
        assert!(!line.contains("question"));
        assert!(!line.contains("start_date"));
        assert!(!line.contains("end_date"));
        assert!(!line.contains("start_time_epoch"));
    }
}
