//! Decoding [`RawEvent`] payloads into venue-specific typed structs.
//!
//! [`RawEvent`] is the on-disk contract: opaque JSON payload + routing
//! metadata. Research code wants strongly-typed structs (`BinanceTrade`,
//! `PolymarketBook`, …) so that field accesses are checked at compile
//! time and prices arrive as [`rust_decimal::Decimal`] rather than
//! `&str`.
//!
//! This module is the bridge.
//!
//! # Dispatch
//!
//! [`decode`] picks a parser based on `(venue, stream)`:
//!
//! | Venue        | Stream pattern                | Decoded variant              |
//! |--------------|-------------------------------|------------------------------|
//! | Binance      | `*@trade`                     | `BinanceTrade`               |
//! | Binance      | `*@depth_snapshot`            | `BinanceDepthSnapshot`       |
//! | Binance      | `*@depth*`                    | `BinanceDepthDiff`           |
//! | Polymarket   | (object `event_type=book`)    | `PolymarketBook`             |
//! | Polymarket   | (object `event_type=price_change`) | `PolymarketPriceChange` |
//! | Polymarket   | (object `event_type=last_trade_price`) | `PolymarketLastTradePrice` |
//! | Polymarket   | (object `event_type=tick_size_change`) | `PolymarketTickSizeChange` |
//! | Coinbase     | `*@market_trades`             | `CoinbaseMarketTrades` (one frame, possibly many trades) |
//! | Chainlink    | `*@logs`                      | `ChainlinkLog` + best-effort `AnswerUpdated` decode |
//!
//! Anything that doesn't match (subscription acks, control frames,
//! `_unrouted`) decodes to [`DecodedEvent::Unknown`] carrying the parsed
//! `serde_json::Value` so research code can still poke at it.
//!
//! # Errors
//!
//! Decoding failures (malformed JSON, missing required fields, bad
//! decimal text) bubble up as [`ReplayError::Decode`]. The local
//! receive timestamp from the wrapping `RawEvent` is propagated into
//! every decoded variant so callers don't need to keep both around.

use common::{RawEvent, Venue};

use crate::ReplayError;

pub mod binance;
pub mod chainlink;
pub mod coinbase;
pub mod polymarket;

pub use binance::{
    BinanceBookTicker, BinanceDepthDiff, BinanceDepthSnapshot, BinanceTrade, PriceLevel,
};
pub use chainlink::{ChainlinkAnswerUpdated, ChainlinkLog};
pub use coinbase::{CoinbaseMarketTrades, CoinbaseTrade};
pub use polymarket::{
    PolymarketBook, PolymarketLastTradePrice, PolymarketLevel, PolymarketPriceChange,
    PolymarketPriceChangeItem, PolymarketResolution, PolymarketSide, PolymarketTickSizeChange,
};

/// One decoded venue event.
///
/// Variant selection is driven by `(venue, stream, payload-shape)` —
/// see [`decode`] for the dispatch table.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodedEvent {
    BinanceTrade(BinanceTrade),
    BinanceDepthDiff(BinanceDepthDiff),
    BinanceDepthSnapshot(BinanceDepthSnapshot),
    BinanceBookTicker(BinanceBookTicker),

    PolymarketBook(PolymarketBook),
    PolymarketPriceChange(PolymarketPriceChange),
    PolymarketLastTradePrice(PolymarketLastTradePrice),
    PolymarketTickSizeChange(PolymarketTickSizeChange),
    PolymarketResolution(PolymarketResolution),

    /// Coinbase frames the inner trades inside `events[*].trades[*]`.
    /// We surface the whole frame so caller can iterate without losing
    /// the channel-level metadata; flatten with [`CoinbaseMarketTrades::flatten`].
    CoinbaseMarketTrades(CoinbaseMarketTrades),

    /// A raw Chainlink log notification, plus an optional best-effort
    /// `AnswerUpdated` decode if the topic0 matches.
    ChainlinkLog(ChainlinkLog),

    /// Payload didn't match any known shape for its venue. Carries the
    /// parsed JSON so caller can inspect it. Subscription acks,
    /// heartbeats, `_unrouted` records all land here.
    Unknown {
        local_ts_ns: u128,
        venue: Venue,
        stream: String,
        value: serde_json::Value,
    },
}

impl DecodedEvent {
    /// Local receive timestamp, regardless of which variant.
    pub fn local_ts_ns(&self) -> u128 {
        match self {
            DecodedEvent::BinanceTrade(t) => t.local_ts_ns,
            DecodedEvent::BinanceDepthDiff(d) => d.local_ts_ns,
            DecodedEvent::BinanceDepthSnapshot(s) => s.local_ts_ns,
            DecodedEvent::BinanceBookTicker(b) => b.local_ts_ns,
            DecodedEvent::PolymarketBook(b) => b.local_ts_ns,
            DecodedEvent::PolymarketPriceChange(p) => p.local_ts_ns,
            DecodedEvent::PolymarketLastTradePrice(t) => t.local_ts_ns,
            DecodedEvent::PolymarketTickSizeChange(t) => t.local_ts_ns,
            DecodedEvent::PolymarketResolution(r) => r.local_ts_ns,
            DecodedEvent::CoinbaseMarketTrades(t) => t.local_ts_ns,
            DecodedEvent::ChainlinkLog(l) => l.local_ts_ns,
            DecodedEvent::Unknown { local_ts_ns, .. } => *local_ts_ns,
        }
    }
}

/// Decode one [`RawEvent`] into a typed [`DecodedEvent`].
///
/// Dispatch is based on `event.venue` and `event.stream`. Payload
/// re-parsing happens once; per-venue helpers walk the parsed
/// `serde_json::Value`.
pub fn decode(event: &RawEvent) -> Result<DecodedEvent, ReplayError> {
    let local_ts = event.local_ts_ns.as_nanos();

    match event.venue {
        Venue::Binance => binance::decode(local_ts, &event.stream, &event.payload),
        Venue::Polymarket => polymarket::decode(local_ts, &event.stream, &event.payload),
        Venue::Coinbase => coinbase::decode(local_ts, &event.stream, &event.payload),
        Venue::Chainlink => chainlink::decode(local_ts, &event.stream, &event.payload),
    }
}

/// Helper for per-venue decoders: parse JSON or surface a [`ReplayError::Decode`]
/// with `stream` context so the caller can spot which file misbehaved.
pub(crate) fn parse_json(stream: &str, payload: &str) -> Result<serde_json::Value, ReplayError> {
    serde_json::from_str(payload).map_err(|e| ReplayError::Decode {
        stream: stream.to_string(),
        reason: format!("invalid JSON: {e}"),
    })
}

/// Build a [`DecodedEvent::Unknown`] for cases where dispatch
/// couldn't pick a variant. Caller has already parsed the payload.
pub(crate) fn unknown(
    local_ts_ns: u128,
    venue: Venue,
    stream: &str,
    value: serde_json::Value,
) -> DecodedEvent {
    DecodedEvent::Unknown {
        local_ts_ns,
        venue,
        stream: stream.to_string(),
        value,
    }
}
