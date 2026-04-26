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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        };
        let line = serde_json::to_string(&event).unwrap();
        assert!(!line.contains("venue_ts_ms"), "got {line}");
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
        };
        let line = serde_json::to_string(&event).unwrap();
        assert_eq!(
            line,
            r#"{"venue":"binance","stream":"btcusdt@trade","local_ts_ns":"1","venue_ts_ms":42,"payload":"p"}"#
        );
    }
}
