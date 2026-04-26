//! Coinbase Advanced Trade WebSocket decoders.
//!
//! Wire docs: <https://docs.cdp.coinbase.com/advanced-trade/docs/ws-overview>
//!
//! Coinbase wraps trades inside a frame:
//! ```json
//! {"channel":"market_trades",
//!  "client_id":"...",
//!  "timestamp":"2026-04-25T05:30:13.123Z",
//!  "sequence_num":42,
//!  "events":[
//!    {"type":"snapshot","trades":[{...}]},
//!    {"type":"update","trades":[{...},{...}]}
//!  ]}
//! ```
//! We surface the whole frame as [`CoinbaseMarketTrades`]. Helper
//! [`CoinbaseMarketTrades::flatten`] iterates trades irrespective of
//! which inner event they belong to.

use rust_decimal::Decimal;
use serde::Deserialize;

use common::Venue;

use crate::decode::{parse_json, unknown, DecodedEvent};
use crate::ReplayError;

// ---------------------------------------------------------------------------
// market_trades
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CoinbaseTrade {
    pub trade_id: String,
    pub product_id: String,
    /// `BUY` / `SELL` — taker side. Coinbase uppercases.
    pub side: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size: Decimal,
    /// Coinbase emits an ISO-8601 string here (not ms-since-epoch). We
    /// keep it as a String so caller can choose between chrono / time
    /// crates without us pulling in a dep.
    pub time: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoinbaseMarketTrades {
    pub local_ts_ns: u128,
    /// Frame timestamp string from Coinbase (ISO-8601 UTC).
    pub frame_time: Option<String>,
    /// Monotonic per-channel sequence number from Coinbase. Useful for
    /// gap detection.
    pub sequence_num: Option<u64>,
    /// One inner event group, may be `snapshot` or `update`.
    pub events: Vec<CoinbaseTradeBatch>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CoinbaseTradeBatch {
    /// `"snapshot"` (reconnect) or `"update"` (live).
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub trades: Vec<CoinbaseTrade>,
}

impl CoinbaseMarketTrades {
    /// Iterate every trade in the frame, regardless of which inner
    /// batch (`snapshot` vs `update`) it belongs to.
    pub fn flatten(&self) -> impl Iterator<Item = &CoinbaseTrade> {
        self.events.iter().flat_map(|b| b.trades.iter())
    }
}

#[derive(Deserialize)]
struct WireFrame {
    timestamp: Option<String>,
    sequence_num: Option<u64>,
    #[serde(default)]
    events: Vec<CoinbaseTradeBatch>,
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub(super) fn decode(
    local_ts_ns: u128,
    stream: &str,
    payload: &str,
) -> Result<DecodedEvent, ReplayError> {
    if stream.ends_with("@market_trades") {
        let w: WireFrame = serde_json::from_str(payload).map_err(|e| ReplayError::Decode {
            stream: stream.to_string(),
            reason: format!("market_trades: {e}"),
        })?;
        return Ok(DecodedEvent::CoinbaseMarketTrades(CoinbaseMarketTrades {
            local_ts_ns,
            frame_time: w.timestamp,
            sequence_num: w.sequence_num,
            events: w.events,
        }));
    }

    let value = parse_json(stream, payload)?;
    Ok(unknown(local_ts_ns, Venue::Coinbase, stream, value))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    const FRAME: &str = r#"{
        "channel":"market_trades",
        "client_id":"abc",
        "timestamp":"2026-04-25T05:30:13.123Z",
        "sequence_num":42,
        "events":[
            {"type":"snapshot","trades":[
                {"trade_id":"1","product_id":"BTC-USD","side":"BUY",
                 "price":"78326.29","size":"0.001","time":"2026-04-25T05:30:13.000Z"}
            ]},
            {"type":"update","trades":[
                {"trade_id":"2","product_id":"BTC-USD","side":"SELL",
                 "price":"78326.30","size":"0.002","time":"2026-04-25T05:30:13.500Z"}
            ]}
        ]
    }"#;

    #[test]
    fn decodes_coinbase_market_trades_frame() {
        let e = decode(99, "btc-usd@market_trades", FRAME).unwrap();
        match e {
            DecodedEvent::CoinbaseMarketTrades(t) => {
                assert_eq!(t.local_ts_ns, 99);
                assert_eq!(t.sequence_num, Some(42));
                assert_eq!(t.events.len(), 2);
                let trades: Vec<_> = t.flatten().collect();
                assert_eq!(trades.len(), 2);
                assert_eq!(trades[0].trade_id, "1");
                assert_eq!(trades[0].price, Decimal::from_str("78326.29").unwrap());
                assert_eq!(trades[1].side, "SELL");
            }
            _ => panic!("expected CoinbaseMarketTrades"),
        }
    }

    #[test]
    fn flatten_handles_empty_inner_trades() {
        let p = r#"{"channel":"market_trades","timestamp":"t",
                   "events":[{"type":"snapshot","trades":[]}]}"#;
        let e = decode(0, "btc-usd@market_trades", p).unwrap();
        match e {
            DecodedEvent::CoinbaseMarketTrades(t) => {
                assert_eq!(t.flatten().count(), 0);
            }
            _ => panic!("expected CoinbaseMarketTrades"),
        }
    }

    #[test]
    fn subscription_ack_routes_to_unknown() {
        // Coinbase subscription acks land in `_subscriptions` per the
        // recorder's classify(); decoder doesn't recognise the stream.
        let p = r#"{"channel":"subscriptions","events":[]}"#;
        let e = decode(0, "_subscriptions", p).unwrap();
        assert!(matches!(e, DecodedEvent::Unknown { .. }));
    }

    #[test]
    fn malformed_market_trades_surfaces_decode_error() {
        // A `price` field that isn't parseable as Decimal.
        let p = r#"{"channel":"market_trades","events":[
            {"type":"update","trades":[
                {"trade_id":"1","product_id":"BTC-USD","side":"BUY",
                 "price":"abc","size":"0","time":"t"}]}]}"#;
        assert!(matches!(
            decode(0, "btc-usd@market_trades", p),
            Err(ReplayError::Decode { .. })
        ));
    }
}
