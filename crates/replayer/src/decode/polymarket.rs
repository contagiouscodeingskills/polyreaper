//! Polymarket CLOB market-channel decoders.
//!
//! Wire docs: <https://docs.polymarket.com/#websocket-api>
//!
//! Dispatch is by `event_type` inside the payload — the recorder's
//! `stream` (slug or condition id) only tells us *which market*. So we
//! always parse the payload, look at `event_type`, then fan out.
//!
//! ## Timestamps
//!
//! Polymarket sends `timestamp` as a *string* of milliseconds since
//! epoch. We parse to `i64` for convenience; the optional helper
//! [`parse_ts_ms`] also tolerates the (rare) integer form some
//! historical archives used.
//!
//! ## Decimal
//!
//! Polymarket prices are 0.0–1.0 with up to 4 decimals; sizes are USDC
//! quantities. f64 is *almost* fine but ULP errors can flip the sign of
//! `mid - 0.5` near 50¢, which is exactly where the BTC-up-down markets
//! live. `Decimal` removes that risk.

use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

use common::Venue;

use crate::decode::{parse_json, unknown, DecodedEvent};
use crate::ReplayError;

// ---------------------------------------------------------------------------
// book
// ---------------------------------------------------------------------------

/// Full book snapshot for one outcome side (one `asset_id` = one
/// Yes/No token). Polymarket sends one of these per side, **not** a
/// combined book per market.
///
/// [`crate::book::polymarket::PolymarketMarketBook`] composes two of
/// these (Yes-side + No-side) into a single market view.
#[derive(Debug, Clone, PartialEq)]
pub struct PolymarketBook {
    pub local_ts_ns: u128,
    /// Token id this book describes — corresponds to one Yes/No outcome.
    pub asset_id: String,
    /// Market condition id (hex). Useful for cross-asset routing.
    pub market: String,
    /// Polymarket-reported timestamp, ms since epoch. `None` if absent
    /// (the field is required by the spec but we tolerate missing).
    pub timestamp_ms: Option<i64>,
    /// Order-book hash from Polymarket — useful for sequence checks.
    pub hash: Option<String>,
    pub bids: Vec<PolymarketLevel>,
    pub asks: Vec<PolymarketLevel>,
}

/// One `{price, size}` level. Polymarket uses object form (named fields),
/// not the two-element array form Binance uses.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PolymarketLevel {
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size: Decimal,
}

#[derive(Deserialize)]
struct WireBook {
    asset_id: String,
    market: String,
    timestamp: Option<String>,
    hash: Option<String>,
    #[serde(default)]
    bids: Vec<PolymarketLevel>,
    #[serde(default)]
    asks: Vec<PolymarketLevel>,
}

// ---------------------------------------------------------------------------
// price_change
// ---------------------------------------------------------------------------

/// Batch of price-level diffs for one market — possibly spanning both
/// outcome tokens (Yes and No) in a single wire frame. Each
/// [`PolymarketPriceChangeItem`] carries its own `asset_id`, so routing
/// to a per-side book is per-item, not per-event. The batch is just a
/// market-scoped envelope sharing `timestamp`; `asset_id` and `hash`
/// live on the items, not the batch (verified against live wire on
/// 2026-04-26).
#[derive(Debug, Clone, PartialEq)]
pub struct PolymarketPriceChange {
    pub local_ts_ns: u128,
    pub market: String,
    pub timestamp_ms: Option<i64>,
    pub price_changes: Vec<PolymarketPriceChangeItem>,
}

/// Side of a price change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PolymarketSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PolymarketPriceChangeItem {
    /// Token id this item updates. One wire batch can carry items for
    /// both the Yes-token and No-token of the same market, so this
    /// drives per-item routing during book reconstruction.
    pub asset_id: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size: Decimal,
    pub side: PolymarketSide,
    /// Best opposing prices reported alongside the change. Optional
    /// because older Polymarket versions omitted them; live wire
    /// (verified 2026-04-26) always emits both.
    #[serde(default, with = "opt_decimal_str")]
    pub best_bid: Option<Decimal>,
    #[serde(default, with = "opt_decimal_str")]
    pub best_ask: Option<Decimal>,
    pub hash: Option<String>,
}

// Real wire never carries `asset_id` or `hash` at the top level of a
// price_change event (verified 2026-04-26 against a 13 h capture);
// `asset_id` lives on each `price_changes[]` item. Serde silently
// ignores extra wire fields, so this stays robust if Polymarket adds
// more later.
#[derive(Deserialize)]
struct WirePriceChange {
    market: String,
    timestamp: Option<String>,
    #[serde(default)]
    price_changes: Vec<PolymarketPriceChangeItem>,
}

// ---------------------------------------------------------------------------
// last_trade_price
// ---------------------------------------------------------------------------

/// One executed trade reported by the market channel.
#[derive(Debug, Clone, PartialEq)]
pub struct PolymarketLastTradePrice {
    pub local_ts_ns: u128,
    pub asset_id: String,
    pub market: String,
    pub timestamp_ms: Option<i64>,
    pub price: Decimal,
    pub size: Decimal,
    pub side: PolymarketSide,
    /// Maker-rebate basis points. Optional because old payloads omit it.
    pub fee_rate_bps: Option<i32>,
}

#[derive(Deserialize)]
struct WireLastTradePrice {
    asset_id: String,
    market: String,
    timestamp: Option<String>,
    #[serde(with = "rust_decimal::serde::str")]
    price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    size: Decimal,
    side: PolymarketSide,
    /// Polymarket sends as either string or int — accept both.
    #[serde(default)]
    fee_rate_bps: Option<Value>,
}

// ---------------------------------------------------------------------------
// tick_size_change
// ---------------------------------------------------------------------------

/// Tick-size change for one market — affects which prices are valid
/// going forward. Important for book hygiene if you compare to a
/// historical book that used a coarser tick.
#[derive(Debug, Clone, PartialEq)]
pub struct PolymarketTickSizeChange {
    pub local_ts_ns: u128,
    pub asset_id: String,
    pub market: String,
    pub timestamp_ms: Option<i64>,
    pub old_tick_size: Decimal,
    pub new_tick_size: Decimal,
}

#[derive(Deserialize)]
struct WireTickSizeChange {
    asset_id: String,
    market: String,
    timestamp: Option<String>,
    #[serde(with = "rust_decimal::serde::str")]
    old_tick_size: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    new_tick_size: Decimal,
}

// ---------------------------------------------------------------------------
// Resolution (sweeper-side, not market channel)
// ---------------------------------------------------------------------------

/// Final outcome captured by the recorder's resolution sweeper.
///
/// Lives on a separate file (`<slug>-resolved.ndjson`) and a separate
/// stream from the live CLOB market channel. Payload is a serialised
/// Gamma `/events` entry — *not* a CLOB event, so dispatch on this is
/// stream-name-driven, not `event_type`-driven.
///
/// Use `resolved_outcome` for label generation (`"Up"`/`"Down"`/`"Yes"`/`"No"`).
/// `None` when the captured payload doesn't carry a definitive 1.0/0.0
/// `outcomePrices` pair (shouldn't happen post-sweep, but possible if
/// the sweeper races a still-settling market).
#[derive(Debug, Clone, PartialEq)]
pub struct PolymarketResolution {
    pub local_ts_ns: u128,
    /// Polymarket condition id (hex). Same as `market` on CLOB events.
    pub market: String,
    /// Slug, e.g. `"btc-updown-5m-1776415200"`. May be empty.
    pub slug: String,
    /// Market end-date as epoch seconds. `None` if the payload's
    /// `endDate` was missing or unparseable.
    pub end_time_secs: Option<i64>,
    /// `Some("Up")` / `Some("Down")` / `Some("Yes")` / `Some("No")` when
    /// settled; `None` otherwise.
    pub resolved_outcome: Option<String>,
    /// Labels in wire order: `("Up","Down")` or `("Yes","No")`.
    /// `None` when missing or malformed.
    pub outcome_labels: Option<(String, String)>,
    /// Prices in same wire order as labels. `None` when missing.
    pub outcome_prices: Option<(Decimal, Decimal)>,
}

#[derive(Deserialize)]
struct WireResolutionEvent {
    #[serde(default)]
    slug: Option<String>,
    #[serde(rename = "endDate", default)]
    end_date: Option<String>,
    #[serde(default)]
    markets: Vec<WireResolutionMarket>,
}

#[derive(Deserialize)]
struct WireResolutionMarket {
    #[serde(rename = "conditionId", default)]
    condition_id: Option<String>,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    outcomes: Option<String>,
    #[serde(rename = "outcomePrices", default)]
    outcome_prices: Option<String>,
    #[serde(rename = "endDate", default)]
    end_date: Option<String>,
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub(super) fn decode(
    local_ts_ns: u128,
    stream: &str,
    payload: &str,
) -> Result<DecodedEvent, ReplayError> {
    // Resolution sweeper writes its own files (`<slug>-resolved.ndjson`).
    // Their payload shape is GammaEvent, not a CLOB market event — they
    // have no `event_type` field, so they'd fall through to Unknown if
    // we tried event_type dispatch first.
    if stream.ends_with("-resolved") {
        return decode_resolution(local_ts_ns, stream, payload);
    }

    let value = parse_json(stream, payload)?;
    let event_type = value
        .get("event_type")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    match event_type {
        "book" => {
            let w: WireBook = serde_json::from_value(value).map_err(|e| ReplayError::Decode {
                stream: stream.to_string(),
                reason: format!("book: {e}"),
            })?;
            Ok(DecodedEvent::PolymarketBook(PolymarketBook {
                local_ts_ns,
                asset_id: w.asset_id,
                market: w.market,
                timestamp_ms: parse_ts_ms(w.timestamp.as_deref()),
                hash: w.hash,
                bids: w.bids,
                asks: w.asks,
            }))
        }
        "price_change" => {
            let w: WirePriceChange =
                serde_json::from_value(value).map_err(|e| ReplayError::Decode {
                    stream: stream.to_string(),
                    reason: format!("price_change: {e}"),
                })?;
            Ok(DecodedEvent::PolymarketPriceChange(PolymarketPriceChange {
                local_ts_ns,
                market: w.market,
                timestamp_ms: parse_ts_ms(w.timestamp.as_deref()),
                price_changes: w.price_changes,
            }))
        }
        "last_trade_price" => {
            let w: WireLastTradePrice =
                serde_json::from_value(value).map_err(|e| ReplayError::Decode {
                    stream: stream.to_string(),
                    reason: format!("last_trade_price: {e}"),
                })?;
            let fee_rate_bps = w.fee_rate_bps.as_ref().and_then(value_to_i32);
            Ok(DecodedEvent::PolymarketLastTradePrice(
                PolymarketLastTradePrice {
                    local_ts_ns,
                    asset_id: w.asset_id,
                    market: w.market,
                    timestamp_ms: parse_ts_ms(w.timestamp.as_deref()),
                    price: w.price,
                    size: w.size,
                    side: w.side,
                    fee_rate_bps,
                },
            ))
        }
        "tick_size_change" => {
            let w: WireTickSizeChange =
                serde_json::from_value(value).map_err(|e| ReplayError::Decode {
                    stream: stream.to_string(),
                    reason: format!("tick_size_change: {e}"),
                })?;
            Ok(DecodedEvent::PolymarketTickSizeChange(
                PolymarketTickSizeChange {
                    local_ts_ns,
                    asset_id: w.asset_id,
                    market: w.market,
                    timestamp_ms: parse_ts_ms(w.timestamp.as_deref()),
                    old_tick_size: w.old_tick_size,
                    new_tick_size: w.new_tick_size,
                },
            ))
        }
        _ => Ok(unknown(local_ts_ns, Venue::Polymarket, stream, value)),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn decode_resolution(
    local_ts_ns: u128,
    stream: &str,
    payload: &str,
) -> Result<DecodedEvent, ReplayError> {
    let event: WireResolutionEvent =
        serde_json::from_str(payload).map_err(|e| ReplayError::Decode {
            stream: stream.to_string(),
            reason: format!("resolution: {e}"),
        })?;

    // Resolution sweeper guarantees markets[0] exists for valid captures,
    // but be defensive — a malformed payload should surface as Unknown
    // rather than panic, since the sweeper sometimes ingests racy data.
    let market = match event.markets.first() {
        Some(m) => m,
        None => {
            return Ok(unknown(
                local_ts_ns,
                Venue::Polymarket,
                stream,
                serde_json::Value::Null,
            ));
        }
    };

    let condition_id = market.condition_id.clone().unwrap_or_default();
    let slug = market
        .slug
        .clone()
        .or_else(|| event.slug.clone())
        .unwrap_or_default();
    let end_date_str = market
        .end_date
        .as_ref()
        .or(event.end_date.as_ref());
    let end_time_secs = end_date_str.and_then(|s| parse_iso8601_to_epoch(s));

    let outcome_labels = market.outcomes.as_deref().and_then(parse_pair_strings);
    let outcome_prices = market
        .outcome_prices
        .as_deref()
        .and_then(parse_pair_decimals);

    // Settled when one outcomePrices entry is exactly 1 and the other 0.
    // Both pieces have to land for `resolved_outcome` to be Some.
    let resolved_outcome = match (&outcome_labels, &outcome_prices) {
        (Some((a, b)), Some((pa, pb))) => {
            if *pa == Decimal::ONE && *pb == Decimal::ZERO {
                Some(a.clone())
            } else if *pa == Decimal::ZERO && *pb == Decimal::ONE {
                Some(b.clone())
            } else {
                None
            }
        }
        _ => None,
    };

    Ok(DecodedEvent::PolymarketResolution(PolymarketResolution {
        local_ts_ns,
        market: condition_id,
        slug,
        end_time_secs,
        resolved_outcome,
        outcome_labels,
        outcome_prices,
    }))
}

/// Parse a JSON-encoded string of a 2-element string array, e.g.
/// `"[\"Up\",\"Down\"]"`. Returns `None` if not a 2-element array of
/// strings.
fn parse_pair_strings(s: &str) -> Option<(String, String)> {
    let v: Vec<String> = serde_json::from_str(s).ok()?;
    if v.len() == 2 {
        let mut it = v.into_iter();
        Some((it.next()?, it.next()?))
    } else {
        None
    }
}

/// Parse a JSON-encoded string of a 2-element string array of decimals,
/// e.g. `"[\"0.62\",\"0.38\"]"`.
fn parse_pair_decimals(s: &str) -> Option<(Decimal, Decimal)> {
    let v: Vec<String> = serde_json::from_str(s).ok()?;
    if v.len() == 2 {
        let a = v[0].parse::<Decimal>().ok()?;
        let b = v[1].parse::<Decimal>().ok()?;
        Some((a, b))
    } else {
        None
    }
}

/// Best-effort ISO-8601 `YYYY-MM-DDTHH:MM:SS[.SSS][Z|+HH:MM]` → epoch
/// seconds. Mirrors `market_registry::gamma::parse_iso8601_to_epoch`
/// so we don't drag chrono in for one parser. Returns `None` on any
/// malformed input.
fn parse_iso8601_to_epoch(s: &str) -> Option<i64> {
    // Split date and time on the literal 'T'.
    let (date, rest) = s.split_once('T')?;
    let date_parts: Vec<&str> = date.split('-').collect();
    if date_parts.len() != 3 {
        return None;
    }
    let year: i32 = date_parts[0].parse().ok()?;
    let month: u32 = date_parts[1].parse().ok()?;
    let day: u32 = date_parts[2].parse().ok()?;

    // Strip trailing timezone marker. We treat "Z" and "+HH:MM" /
    // "-HH:MM" as UTC (same as recorder's parser — drift relative to
    // a non-zero offset is at most a few hours and irrelevant for
    // labelling).
    let time = rest
        .find(|c: char| c == 'Z' || c == '+' || c == '-')
        .map(|i| &rest[..i])
        .unwrap_or(rest);
    let mut bits = time.split(':');
    let hour: u32 = bits.next()?.parse().ok()?;
    let minute: u32 = bits.next()?.parse().ok()?;
    let sec_part = bits.next().unwrap_or("0");
    let second: u32 = sec_part.split('.').next()?.parse().ok()?;

    let days = days_from_civil(year, month, day)?;
    let total = (days as i64) * 86_400 + (hour as i64) * 3600 + (minute as i64) * 60 + second as i64;
    Some(total)
}

/// Howard Hinnant's `days_from_civil`: Gregorian date → days since
/// 1970-01-01. Returns `None` on out-of-range input.
fn days_from_civil(y: i32, m: u32, d: u32) -> Option<i64> {
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some((era as i64) * 146_097 + doe as i64 - 719_468)
}

/// Parse a Polymarket `timestamp` (string of ms since epoch) into i64.
/// Returns `None` if absent or unparseable — callers usually fall back
/// to `local_ts_ns` for ordering.
fn parse_ts_ms(s: Option<&str>) -> Option<i64> {
    s.and_then(|s| s.parse::<i64>().ok())
}

fn value_to_i32(v: &Value) -> Option<i32> {
    if let Some(s) = v.as_str() {
        return s.parse::<i32>().ok();
    }
    v.as_i64().and_then(|i| i32::try_from(i).ok())
}

/// Optional `Decimal` field that arrives as a string. Treat `null` /
/// missing as `None`.
mod opt_decimal_str {
    use rust_decimal::Decimal;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    /// Kept symmetric with [`deserialize`] so that adding `#[derive(Serialize)]`
    /// to a parent struct doesn't break the `#[serde(with = ...)]` contract.
    #[allow(dead_code)]
    pub fn serialize<S: Serializer>(v: &Option<Decimal>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(d) => d.to_string().serialize(s),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Decimal>, D::Error> {
        let opt: Option<String> = Option::deserialize(d)?;
        match opt {
            Some(s) => s
                .parse::<Decimal>()
                .map(Some)
                .map_err(serde::de::Error::custom),
            None => Ok(None),
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

    const BOOK: &str = r#"{
        "event_type":"book",
        "asset_id":"65818619657568813474341868652308942079804919287380422192892211131408793125422",
        "market":"0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af",
        "timestamp":"1750000000123",
        "hash":"0xabcd",
        "bids":[{"price":"0.5000","size":"100"},{"price":"0.4999","size":"200"}],
        "asks":[{"price":"0.5001","size":"50"}]
    }"#;

    #[test]
    fn decodes_book_event() {
        let e = decode(7, "btc-updown-5m-1", BOOK).unwrap();
        match e {
            DecodedEvent::PolymarketBook(b) => {
                assert_eq!(b.local_ts_ns, 7);
                assert_eq!(b.timestamp_ms, Some(1_750_000_000_123));
                assert_eq!(b.bids.len(), 2);
                assert_eq!(b.bids[0].price, Decimal::from_str("0.5000").unwrap());
                assert_eq!(b.asks[0].size, Decimal::from(50));
                assert_eq!(b.hash.as_deref(), Some("0xabcd"));
            }
            _ => panic!("expected PolymarketBook"),
        }
    }

    const PRICE_CHANGE: &str = r#"{
        "event_type":"price_change",
        "market":"0xMKT",
        "timestamp":"1750000000999",
        "price_changes":[
            {"asset_id":"YES-TOK","price":"0.6200","size":"100","side":"BUY","best_bid":"0.6200","best_ask":"0.6300","hash":"0xa"},
            {"asset_id":"YES-TOK","price":"0.6300","size":"0","side":"SELL"}
        ]
    }"#;

    #[test]
    fn decodes_price_change_with_optional_fields() {
        let e = decode(0, "btc-updown-5m-1", PRICE_CHANGE).unwrap();
        match e {
            DecodedEvent::PolymarketPriceChange(p) => {
                assert_eq!(p.market, "0xMKT");
                assert_eq!(p.price_changes.len(), 2);
                // First item: full real-wire shape — asset_id + best_bid/ask + hash.
                assert_eq!(p.price_changes[0].asset_id, "YES-TOK");
                assert_eq!(p.price_changes[0].side, PolymarketSide::Buy);
                assert_eq!(
                    p.price_changes[0].best_bid,
                    Some(Decimal::from_str("0.6200").unwrap())
                );
                assert_eq!(p.price_changes[0].hash.as_deref(), Some("0xa"));
                // Second item: legacy/edge case — Optionals absent.
                assert_eq!(p.price_changes[1].asset_id, "YES-TOK");
                assert_eq!(p.price_changes[1].side, PolymarketSide::Sell);
                assert_eq!(p.price_changes[1].best_bid, None);
                assert_eq!(p.price_changes[1].best_ask, None);
                assert_eq!(p.price_changes[1].hash, None);
                assert_eq!(p.price_changes[1].size, Decimal::ZERO);
            }
            _ => panic!("expected PolymarketPriceChange"),
        }
    }

    /// Mirror of the real wire shape captured 2026-04-26: no top-level
    /// asset_id, every item carries its own asset_id + best_bid/ask +
    /// hash, two items per batch (one BUY one SELL).
    const PRICE_CHANGE_REAL_WIRE: &str = r#"{
        "event_type":"price_change",
        "market":"0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af",
        "timestamp":"1750000001234",
        "price_changes":[
            {"asset_id":"658186","best_ask":"0.5500","best_bid":"0.5400","hash":"0xh1","price":"0.5400","side":"BUY","size":"42"},
            {"asset_id":"658186","best_ask":"0.5500","best_bid":"0.5400","hash":"0xh2","price":"0.5500","side":"SELL","size":"17"}
        ]
    }"#;

    #[test]
    fn decodes_real_wire_shape_price_change() {
        let e = decode(0, "btc-updown-5m-1", PRICE_CHANGE_REAL_WIRE).unwrap();
        match e {
            DecodedEvent::PolymarketPriceChange(p) => {
                assert_eq!(p.price_changes.len(), 2);
                assert_eq!(p.price_changes[0].asset_id, "658186");
                assert_eq!(p.price_changes[1].asset_id, "658186");
                assert_eq!(p.price_changes[0].side, PolymarketSide::Buy);
                assert_eq!(p.price_changes[1].side, PolymarketSide::Sell);
                // Real wire always populates these per-item Optional fields.
                assert!(p.price_changes[0].best_bid.is_some());
                assert!(p.price_changes[0].best_ask.is_some());
                assert!(p.price_changes[0].hash.is_some());
                assert!(p.price_changes[1].best_bid.is_some());
                assert!(p.price_changes[1].hash.is_some());
            }
            _ => panic!("expected PolymarketPriceChange"),
        }
    }

    const LAST_TRADE: &str = r#"{
        "event_type":"last_trade_price",
        "asset_id":"YES","market":"0xM",
        "timestamp":"1750000000000",
        "price":"0.5500","size":"42","side":"BUY",
        "fee_rate_bps":"0"
    }"#;

    #[test]
    fn decodes_last_trade_with_string_fee_bps() {
        let e = decode(0, "x", LAST_TRADE).unwrap();
        match e {
            DecodedEvent::PolymarketLastTradePrice(t) => {
                assert_eq!(t.price, Decimal::from_str("0.5500").unwrap());
                assert_eq!(t.size, Decimal::from(42));
                assert_eq!(t.fee_rate_bps, Some(0));
                assert_eq!(t.side, PolymarketSide::Buy);
            }
            _ => panic!("expected PolymarketLastTradePrice"),
        }
    }

    #[test]
    fn last_trade_handles_int_fee_bps() {
        // Older Polymarket versions sent fee_rate_bps as an integer.
        let p = r#"{"event_type":"last_trade_price","asset_id":"Y","market":"M",
                    "timestamp":"1","price":"0.5","size":"1","side":"BUY","fee_rate_bps":3}"#;
        let e = decode(0, "x", p).unwrap();
        match e {
            DecodedEvent::PolymarketLastTradePrice(t) => assert_eq!(t.fee_rate_bps, Some(3)),
            _ => panic!("expected PolymarketLastTradePrice"),
        }
    }

    #[test]
    fn decodes_tick_size_change() {
        let p = r#"{"event_type":"tick_size_change","asset_id":"Y","market":"M",
                    "timestamp":"1","old_tick_size":"0.01","new_tick_size":"0.001"}"#;
        let e = decode(0, "x", p).unwrap();
        match e {
            DecodedEvent::PolymarketTickSizeChange(t) => {
                assert_eq!(t.old_tick_size, Decimal::from_str("0.01").unwrap());
                assert_eq!(t.new_tick_size, Decimal::from_str("0.001").unwrap());
            }
            _ => panic!("expected PolymarketTickSizeChange"),
        }
    }

    #[test]
    fn unknown_event_type_returns_unknown() {
        let p = r#"{"event_type":"pong"}"#;
        let e = decode(0, "x", p).unwrap();
        assert!(matches!(e, DecodedEvent::Unknown { .. }));
    }

    #[test]
    fn missing_event_type_returns_unknown() {
        let p = r#"{"foo":"bar"}"#;
        let e = decode(0, "x", p).unwrap();
        assert!(matches!(e, DecodedEvent::Unknown { .. }));
    }

    #[test]
    fn malformed_book_payload_surfaces_decode_error() {
        // Missing required "asset_id".
        let p = r#"{"event_type":"book","market":"M","bids":[],"asks":[]}"#;
        assert!(matches!(decode(0, "x", p), Err(ReplayError::Decode { .. })));
    }

    #[test]
    fn parse_ts_ms_round_trips() {
        assert_eq!(parse_ts_ms(Some("123")), Some(123));
        assert_eq!(parse_ts_ms(Some("not-a-number")), None);
        assert_eq!(parse_ts_ms(None), None);
    }

    // ----- resolution sweeper payloads -----

    /// Mirrors the Gamma fixture in `crates/market_registry/src/gamma.rs`.
    /// `outcomePrices` `[1, 0]` with outcomes `[Up, Down]` → settled Up.
    const RESOLUTION_UP: &str = r#"{
        "id":"384681",
        "slug":"btc-updown-5m-1776415200",
        "title":"Bitcoin Up or Down - April 17",
        "endDate":"2026-04-17T08:45:00Z",
        "closed":true,
        "markets":[{
            "conditionId":"0xb56bbed2f9f79f81d0511b3570d9d21072465b00c7e9b021ae44bb73cf1c06c9",
            "slug":"btc-updown-5m-1776415200",
            "outcomes":"[\"Up\",\"Down\"]",
            "outcomePrices":"[\"1\",\"0\"]",
            "endDate":"2026-04-17T08:45:00Z"
        }]
    }"#;

    #[test]
    fn decodes_settled_up_resolution() {
        let e = decode(7, "btc-updown-5m-1776415200-resolved", RESOLUTION_UP).unwrap();
        match e {
            DecodedEvent::PolymarketResolution(r) => {
                assert_eq!(r.local_ts_ns, 7);
                assert_eq!(
                    r.market,
                    "0xb56bbed2f9f79f81d0511b3570d9d21072465b00c7e9b021ae44bb73cf1c06c9"
                );
                assert_eq!(r.slug, "btc-updown-5m-1776415200");
                assert_eq!(r.resolved_outcome.as_deref(), Some("Up"));
                assert_eq!(
                    r.outcome_labels,
                    Some(("Up".into(), "Down".into()))
                );
                assert_eq!(
                    r.outcome_prices,
                    Some((Decimal::ONE, Decimal::ZERO))
                );
                assert_eq!(r.end_time_secs, Some(1_776_415_500));
            }
            _ => panic!("expected PolymarketResolution"),
        }
    }

    #[test]
    fn decodes_settled_down_resolution() {
        let p = RESOLUTION_UP.replace(
            "\"outcomePrices\":\"[\\\"1\\\",\\\"0\\\"]\"",
            "\"outcomePrices\":\"[\\\"0\\\",\\\"1\\\"]\"",
        );
        let e = decode(0, "x-resolved", &p).unwrap();
        match e {
            DecodedEvent::PolymarketResolution(r) => {
                assert_eq!(r.resolved_outcome.as_deref(), Some("Down"));
            }
            _ => panic!("expected PolymarketResolution"),
        }
    }

    #[test]
    fn yes_no_outcomes_round_trip() {
        let p = RESOLUTION_UP
            .replace(
                "\"outcomes\":\"[\\\"Up\\\",\\\"Down\\\"]\"",
                "\"outcomes\":\"[\\\"Yes\\\",\\\"No\\\"]\"",
            );
        let e = decode(0, "x-resolved", &p).unwrap();
        match e {
            DecodedEvent::PolymarketResolution(r) => {
                assert_eq!(r.outcome_labels, Some(("Yes".into(), "No".into())));
                assert_eq!(r.resolved_outcome.as_deref(), Some("Yes"));
            }
            _ => panic!("expected PolymarketResolution"),
        }
    }

    #[test]
    fn fractional_outcome_prices_leave_resolved_outcome_none() {
        // Mid-life sample (still trading) — neither price is 1.0.
        let p = RESOLUTION_UP.replace(
            "\"outcomePrices\":\"[\\\"1\\\",\\\"0\\\"]\"",
            "\"outcomePrices\":\"[\\\"0.62\\\",\\\"0.38\\\"]\"",
        );
        let e = decode(0, "x-resolved", &p).unwrap();
        match e {
            DecodedEvent::PolymarketResolution(r) => {
                assert!(r.resolved_outcome.is_none());
                assert_eq!(
                    r.outcome_prices,
                    Some((
                        Decimal::from_str("0.62").unwrap(),
                        Decimal::from_str("0.38").unwrap()
                    ))
                );
            }
            _ => panic!("expected PolymarketResolution"),
        }
    }

    #[test]
    fn empty_markets_array_returns_unknown() {
        let p = r#"{"slug":"x","markets":[],"endDate":"2026-01-01T00:00:00Z"}"#;
        let e = decode(0, "x-resolved", p).unwrap();
        assert!(matches!(e, DecodedEvent::Unknown { .. }));
    }

    #[test]
    fn malformed_resolution_payload_surfaces_decode_error() {
        // markets must be an array, not a string.
        let p = r#"{"slug":"x","markets":"oops"}"#;
        assert!(matches!(
            decode(0, "x-resolved", p),
            Err(ReplayError::Decode { .. })
        ));
    }

    #[test]
    fn iso8601_parser_matches_expected_epoch() {
        // 2026-04-17T08:45:00Z = 1_776_415_500
        assert_eq!(
            parse_iso8601_to_epoch("2026-04-17T08:45:00Z"),
            Some(1_776_415_500)
        );
        // ms suffix tolerated.
        assert_eq!(
            parse_iso8601_to_epoch("2026-04-17T08:45:00.123Z"),
            Some(1_776_415_500)
        );
        assert_eq!(parse_iso8601_to_epoch("not-a-date"), None);
    }
}
