//! Binance Spot WebSocket + REST snapshot decoders.
//!
//! Field names follow the Binance spec, **not** the abbreviated wire
//! letters: `event_time_ms` not `E`, `buyer_is_maker` not `m`. The
//! `serde(rename = "...")` attributes do the bridging. Researchers
//! reading these structs should never need the venue API docs open
//! beside them.
//!
//! ## Decimal vs f64
//!
//! Prices and quantities are [`rust_decimal::Decimal`]. Binance prints
//! prices as decimal strings (`"78326.29000000"`); `serde-with-str`
//! parses them losslessly. f64 round-trips lose digits on wide books
//! (BTC at 5 decimals, dust at 8) — Decimal does not.

use rust_decimal::Decimal;
use serde::Deserialize;

use common::Venue;

use crate::decode::{parse_json, unknown, DecodedEvent};
use crate::ReplayError;

// ---------------------------------------------------------------------------
// Trade
// ---------------------------------------------------------------------------

/// Single executed trade — Binance `<symbol>@trade` stream.
///
/// Wire docs: <https://binance-docs.github.io/apidocs/spot/en/#trade-streams>
#[derive(Debug, Clone, PartialEq)]
pub struct BinanceTrade {
    /// Local receive timestamp, ns since epoch. Carried through from the
    /// wrapping [`RawEvent`] so callers don't need to keep both.
    pub local_ts_ns: u128,
    /// Exchange event time (`E`), ms since epoch.
    pub event_time_ms: i64,
    /// Trade execution time (`T`), ms since epoch. Usually `event_time_ms - 1`.
    pub trade_time_ms: i64,
    /// Uppercased symbol, e.g. `"BTCUSDT"`.
    pub symbol: String,
    /// Aggregate trade id (`t`).
    pub trade_id: u64,
    pub price: Decimal,
    pub qty: Decimal,
    /// `m` field — true if the buyer was the maker (i.e. trade hit the bid).
    pub buyer_is_maker: bool,
}

#[derive(Deserialize)]
struct WireTrade {
    #[serde(rename = "E")]
    event_time_ms: i64,
    #[serde(rename = "T")]
    trade_time_ms: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "t")]
    trade_id: u64,
    #[serde(rename = "p", with = "rust_decimal::serde::str")]
    price: Decimal,
    #[serde(rename = "q", with = "rust_decimal::serde::str")]
    qty: Decimal,
    #[serde(rename = "m")]
    buyer_is_maker: bool,
}

// ---------------------------------------------------------------------------
// Depth diff
// ---------------------------------------------------------------------------

/// One price level: `(price, qty)`. `qty == 0` means "remove this level"
/// per the Binance diff semantics.
#[derive(Debug, Clone, PartialEq)]
pub struct PriceLevel {
    pub price: Decimal,
    pub qty: Decimal,
}

impl PriceLevel {
    pub fn new(price: Decimal, qty: Decimal) -> Self {
        Self { price, qty }
    }
}

/// Custom deserializer for Binance's `[["price","qty"], ...]` shape —
/// each element is a two-string array, not an object.
impl<'de> Deserialize<'de> for PriceLevel {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = PriceLevel;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a [price, qty] tuple of decimal strings")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<PriceLevel, A::Error> {
                let p: &str = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let q: &str = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let price = p.parse::<Decimal>().map_err(serde::de::Error::custom)?;
                let qty = q.parse::<Decimal>().map_err(serde::de::Error::custom)?;
                Ok(PriceLevel { price, qty })
            }
        }
        d.deserialize_seq(V)
    }
}

/// Diff update from the `<symbol>@depth@100ms` stream. `first_update_id`
/// (`U`) and `final_update_id` (`u`) are the Binance sequence numbers
/// used to splice diffs against a snapshot's `last_update_id`.
#[derive(Debug, Clone, PartialEq)]
pub struct BinanceDepthDiff {
    pub local_ts_ns: u128,
    pub event_time_ms: i64,
    pub symbol: String,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

#[derive(Deserialize)]
struct WireDepthDiff {
    #[serde(rename = "E")]
    event_time_ms: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "b")]
    bids: Vec<PriceLevel>,
    #[serde(rename = "a")]
    asks: Vec<PriceLevel>,
}

// ---------------------------------------------------------------------------
// Book ticker
// ---------------------------------------------------------------------------

/// Top-of-book snapshot from `<symbol>@bookTicker` — emitted on every
/// BBO change (sub-ms cadence).
///
/// Wire docs: <https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#individual-symbol-book-ticker-streams>
///
/// `update_id` is the same monotonic counter Binance uses for depth
/// diffs (`u`). Useful for lining bookTicker events up against the
/// reconstructed L2 book.
///
/// No venue timestamp — Binance doesn't publish one for bookTicker.
/// Researchers use `local_ts_ns`, which is the right call anyway: any
/// signal that lives below 100 ms can't tolerate a venue timestamp
/// rounded to milliseconds.
#[derive(Debug, Clone, PartialEq)]
pub struct BinanceBookTicker {
    pub local_ts_ns: u128,
    pub update_id: u64,
    pub symbol: String,
    pub best_bid: Decimal,
    pub best_bid_qty: Decimal,
    pub best_ask: Decimal,
    pub best_ask_qty: Decimal,
}

#[derive(Deserialize)]
struct WireBookTicker {
    #[serde(rename = "u")]
    update_id: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b", with = "rust_decimal::serde::str")]
    best_bid: Decimal,
    #[serde(rename = "B", with = "rust_decimal::serde::str")]
    best_bid_qty: Decimal,
    #[serde(rename = "a", with = "rust_decimal::serde::str")]
    best_ask: Decimal,
    #[serde(rename = "A", with = "rust_decimal::serde::str")]
    best_ask_qty: Decimal,
}

// ---------------------------------------------------------------------------
// Depth snapshot
// ---------------------------------------------------------------------------

/// REST `/api/v3/depth?symbol=...` response, persisted by the recorder
/// once per WebSocket connect under stream `<symbol>@depth_snapshot`.
///
/// `last_update_id` is the splice point for subsequent diffs: a diff
/// applies cleanly when `U <= last_update_id + 1 <= u`.
#[derive(Debug, Clone, PartialEq)]
pub struct BinanceDepthSnapshot {
    pub local_ts_ns: u128,
    pub last_update_id: u64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

#[derive(Deserialize)]
struct WireDepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub(super) fn decode(
    local_ts_ns: u128,
    stream: &str,
    payload: &str,
) -> Result<DecodedEvent, ReplayError> {
    // Suffix dispatch chooses *which decoder to try*. The lenient retry
    // below distinguishes "wrong shape on this stream" (→ Unknown) from
    // "right shape but malformed" (→ Decode error). The smoke-record
    // payloads the recorder writes at boot land in the Unknown bucket.
    if stream.ends_with("@trade") {
        return try_typed_or_unknown(
            local_ts_ns, stream, payload, "trade", "e",
            |w: WireTrade| DecodedEvent::BinanceTrade(BinanceTrade {
                local_ts_ns,
                event_time_ms: w.event_time_ms,
                trade_time_ms: w.trade_time_ms,
                symbol: w.symbol,
                trade_id: w.trade_id,
                price: w.price,
                qty: w.qty,
                buyer_is_maker: w.buyer_is_maker,
            }),
        );
    }

    if stream.ends_with("@bookTicker") {
        return try_typed_or_unknown(
            local_ts_ns, stream, payload, "", "u",
            |w: WireBookTicker| DecodedEvent::BinanceBookTicker(BinanceBookTicker {
                local_ts_ns,
                update_id: w.update_id,
                symbol: w.symbol,
                best_bid: w.best_bid,
                best_bid_qty: w.best_bid_qty,
                best_ask: w.best_ask,
                best_ask_qty: w.best_ask_qty,
            }),
        );
    }

    if stream.ends_with("@depth_snapshot") {
        // REST snapshot has no `e` discriminator — fall back on
        // structural match: if `lastUpdateId` is missing it's not a
        // snapshot.
        return try_typed_or_unknown(
            local_ts_ns, stream, payload, "", "lastUpdateId",
            |w: WireDepthSnapshot| DecodedEvent::BinanceDepthSnapshot(BinanceDepthSnapshot {
                local_ts_ns,
                last_update_id: w.last_update_id,
                bids: w.bids,
                asks: w.asks,
            }),
        );
    }

    // Anything matching `*@depth*` after we've ruled out @depth_snapshot.
    if stream.contains("@depth") {
        return try_typed_or_unknown(
            local_ts_ns, stream, payload, "depthUpdate", "e",
            |w: WireDepthDiff| DecodedEvent::BinanceDepthDiff(BinanceDepthDiff {
                local_ts_ns,
                event_time_ms: w.event_time_ms,
                symbol: w.symbol,
                first_update_id: w.first_update_id,
                final_update_id: w.final_update_id,
                bids: w.bids,
                asks: w.asks,
            }),
        );
    }

    // Unrecognised stream — preserve as Unknown so caller can poke at it.
    let value = parse_json(stream, payload)?;
    Ok(unknown(local_ts_ns, Venue::Binance, stream, value))
}

/// Try to typed-decode `payload` into `T`. On failure, fall back to a
/// Value parse and inspect `discriminator_field`:
/// * If the field is absent or `expected_value` doesn't match → return
///   `DecodedEvent::Unknown` (this isn't a `T`-shaped event, just a
///   stranger sharing the stream).
/// * If the field is present and matches → return `Decode` error
///   (genuinely malformed venue payload, surface it).
///
/// Pass `expected_value = ""` to mean "any value of the field is OK,
/// just check for presence" — used for the snapshot path which has no
/// `e` discriminator.
fn try_typed_or_unknown<T, F>(
    local_ts_ns: u128,
    stream: &str,
    payload: &str,
    expected_value: &str,
    discriminator_field: &str,
    map: F,
) -> Result<DecodedEvent, ReplayError>
where
    T: serde::de::DeserializeOwned,
    F: FnOnce(T) -> DecodedEvent,
{
    match serde_json::from_str::<T>(payload) {
        Ok(w) => Ok(map(w)),
        Err(typed_err) => {
            let value = parse_json(stream, payload)?;
            let actual = value.get(discriminator_field).and_then(|v| v.as_str());
            let claims_to_be_target = if expected_value.is_empty() {
                value.get(discriminator_field).is_some()
            } else {
                actual == Some(expected_value)
            };
            if claims_to_be_target {
                Err(ReplayError::Decode {
                    stream: stream.to_string(),
                    reason: format!("{stream}: {typed_err}"),
                })
            } else {
                Ok(unknown(local_ts_ns, Venue::Binance, stream, value))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    // Real captured payload from data/session_20260424T005555Z/binance/btcusdt_trade.0000.ndjson
    const REAL_TRADE: &str = r#"{"e":"trade","E":1776992156622,"s":"BTCUSDT","t":6249003819,"p":"78326.29000000","q":"0.00007000","T":1776992156621,"m":false,"M":true}"#;

    #[test]
    fn decodes_real_binance_trade() {
        let e = decode(1, "btcusdt@trade", REAL_TRADE).unwrap();
        match e {
            DecodedEvent::BinanceTrade(t) => {
                assert_eq!(t.local_ts_ns, 1);
                assert_eq!(t.event_time_ms, 1776992156622);
                assert_eq!(t.trade_time_ms, 1776992156621);
                assert_eq!(t.symbol, "BTCUSDT");
                assert_eq!(t.trade_id, 6249003819);
                assert_eq!(t.price, Decimal::from_str("78326.29000000").unwrap());
                assert_eq!(t.qty, Decimal::from_str("0.00007000").unwrap());
                assert!(!t.buyer_is_maker);
            }
            _ => panic!("expected BinanceTrade"),
        }
    }

    #[test]
    fn trade_decimal_precision_is_exact() {
        // Same price as REAL_TRADE — round-trips through Decimal exactly.
        let e = decode(0, "btcusdt@trade", REAL_TRADE).unwrap();
        if let DecodedEvent::BinanceTrade(t) = e {
            // 8-dp form preserved in Decimal (Decimal stores scale).
            assert_eq!(t.price.to_string(), "78326.29000000");
            assert_eq!(t.qty.to_string(), "0.00007000");
        } else {
            panic!("expected BinanceTrade");
        }
    }

    const DEPTH_DIFF_TINY: &str = r#"{
        "e":"depthUpdate","E":100,"s":"BTCUSDT","U":10,"u":11,
        "b":[["78326.28000000","0.06963000"],["78320.42000000","0.00000000"]],
        "a":[["78326.29000000","2.23216000"]]
    }"#;

    #[test]
    fn decodes_depth_diff_with_qty_zero_remove() {
        let e = decode(7, "btcusdt@depth@100ms", DEPTH_DIFF_TINY).unwrap();
        match e {
            DecodedEvent::BinanceDepthDiff(d) => {
                assert_eq!(d.local_ts_ns, 7);
                assert_eq!(d.first_update_id, 10);
                assert_eq!(d.final_update_id, 11);
                assert_eq!(d.bids.len(), 2);
                assert_eq!(d.bids[1].qty, Decimal::ZERO); // qty=0 = remove
                assert_eq!(d.asks.len(), 1);
                assert_eq!(d.asks[0].price, Decimal::from_str("78326.29000000").unwrap());
            }
            _ => panic!("expected BinanceDepthDiff"),
        }
    }

    #[test]
    fn depth_snapshot_round_trip() {
        let p = r#"{"lastUpdateId":42,"bids":[["100","1"]],"asks":[["101","2"]]}"#;
        let e = decode(0, "btcusdt@depth_snapshot", p).unwrap();
        match e {
            DecodedEvent::BinanceDepthSnapshot(s) => {
                assert_eq!(s.last_update_id, 42);
                assert_eq!(s.bids[0].price, Decimal::from(100));
                assert_eq!(s.asks[0].qty, Decimal::from(2));
            }
            _ => panic!("expected BinanceDepthSnapshot"),
        }
    }

    #[test]
    fn decodes_book_ticker() {
        let p = r#"{"u":400900217,"s":"BTCUSDT","b":"78326.28","B":"0.069","a":"78326.29","A":"2.232"}"#;
        let e = decode(42, "btcusdt@bookTicker", p).unwrap();
        match e {
            DecodedEvent::BinanceBookTicker(b) => {
                assert_eq!(b.local_ts_ns, 42);
                assert_eq!(b.update_id, 400_900_217);
                assert_eq!(b.symbol, "BTCUSDT");
                assert_eq!(b.best_bid, Decimal::from_str("78326.28").unwrap());
                assert_eq!(b.best_ask, Decimal::from_str("78326.29").unwrap());
                assert_eq!(b.best_bid_qty, Decimal::from_str("0.069").unwrap());
                assert_eq!(b.best_ask_qty, Decimal::from_str("2.232").unwrap());
            }
            _ => panic!("expected BinanceBookTicker"),
        }
    }

    #[test]
    fn book_ticker_missing_required_field_surfaces_decode_error() {
        // Missing "A" — best ask qty.
        let p = r#"{"u":1,"s":"X","b":"1","B":"1","a":"1"}"#;
        let r = decode(0, "btcusdt@bookTicker", p);
        // No "u"-less alternative — our discriminator is presence of "u",
        // which IS present. So malformed -> Decode error.
        assert!(matches!(r, Err(ReplayError::Decode { .. })));
    }

    #[test]
    fn unknown_stream_returns_unknown_variant() {
        let e = decode(0, "_unrouted", r#"{"foo":"bar"}"#).unwrap();
        assert!(matches!(e, DecodedEvent::Unknown { .. }));
    }

    #[test]
    fn malformed_trade_returns_decode_error() {
        // `e == "trade"` claims this is a trade — must surface the error.
        let r = decode(0, "btcusdt@trade", r#"{"e":"trade"}"#);
        assert!(matches!(r, Err(ReplayError::Decode { .. })));
    }

    #[test]
    fn smoke_record_on_trade_stream_routes_to_unknown() {
        // The recorder writes this synthetic frame at boot to confirm
        // disk wiring — the replayer must not treat it as malformed.
        let p = r#"{"sample":true,"note":"phase1 smoke record"}"#;
        let e = decode(0, "btcusdt@trade", p).unwrap();
        assert!(matches!(e, DecodedEvent::Unknown { .. }));
    }

    #[test]
    fn non_depth_payload_on_depth_stream_routes_to_unknown() {
        let p = r#"{"e":"trade","E":1,"s":"X","t":1,"p":"1","q":"1","T":1,"m":false}"#;
        let e = decode(0, "btcusdt@depth@100ms", p).unwrap();
        // It's a trade payload landing on the depth stream — Unknown,
        // not Decode, because the `e` field disagrees with the stream.
        assert!(matches!(e, DecodedEvent::Unknown { .. }));
    }

    #[test]
    fn malformed_depth_level_surfaces_as_decode_error() {
        // Three-element price level — invalid.
        let p = r#"{"e":"depthUpdate","E":1,"s":"X","U":1,"u":2,
                    "b":[["1","2","3"]],"a":[]}"#;
        let r = decode(0, "x@depth@100ms", p);
        assert!(matches!(r, Err(ReplayError::Decode { .. })));
    }
}
