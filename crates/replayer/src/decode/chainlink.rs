//! Chainlink AggregatorV3 log decoders.
//!
//! Chainlink price feeds expose their oracle updates as Ethereum logs:
//! we subscribe via JSON-RPC `eth_subscribe("logs")` and capture the
//! envelope as-is. This module turns the envelope into a typed struct
//! and best-effort-decodes the `AnswerUpdated` event payload.
//!
//! ## `AnswerUpdated` decoding
//!
//! The Solidity event:
//! ```solidity
//! event AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 updatedAt);
//! ```
//! Topic0 = `keccak256("AnswerUpdated(int256,uint256,uint256)")`
//!        = `0x0559884fd3a460db3073f0b6f53c12e92e9a7d3e6c8cda85ddc8c7e90db8c5e3`.
//!
//! For non-AggregatorV3 contracts we surface only the [`ChainlinkLog`] —
//! caller can decode by topic0 themselves.

use rust_decimal::Decimal;
use serde::Deserialize;

use common::Venue;

use crate::decode::{parse_json, unknown, DecodedEvent};
use crate::ReplayError;

/// keccak256("AnswerUpdated(int256,uint256,uint256)")
pub const ANSWER_UPDATED_TOPIC0: &str =
    "0x0559884fd3a460db3073f0b6f53c12e92e9a7d3e6c8cda85ddc8c7e90db8c5e3";

// ---------------------------------------------------------------------------
// ChainlinkLog
// ---------------------------------------------------------------------------

/// One JSON-RPC log notification (`eth_subscription` method).
///
/// `answer_updated` is `Some(...)` when topic0 matches the AggregatorV3
/// `AnswerUpdated` event AND the indexed `roundId` + non-indexed
/// `updatedAt` decoded cleanly.
#[derive(Debug, Clone, PartialEq)]
pub struct ChainlinkLog {
    pub local_ts_ns: u128,
    /// Contract address (lowercased — that's how the recorder routes,
    /// match the convention).
    pub address: String,
    /// Hex-encoded indexed topics, raw (with `0x` prefix).
    pub topics: Vec<String>,
    /// Hex-encoded ABI data, raw (with `0x` prefix).
    pub data: String,
    /// Block number — usually `"0xHEX"`. `None` for pending logs.
    pub block_number: Option<String>,
    /// Best-effort decoded AggregatorV3 `AnswerUpdated`. `None` when
    /// topics don't match or data is malformed.
    pub answer_updated: Option<ChainlinkAnswerUpdated>,
}

/// Decoded `AnswerUpdated(int256 current, uint256 roundId, uint256 updatedAt)`.
///
/// `current_price` is the raw int256 — multiply by `10^-decimals` for the
/// human-readable price (decimals comes from the contract's `decimals()`
/// view; for BTC/USD that's 8). We don't auto-scale because the decoder
/// has no way to know decimals at parse time.
#[derive(Debug, Clone, PartialEq)]
pub struct ChainlinkAnswerUpdated {
    pub current_price: Decimal,
    pub round_id: u128,
    pub updated_at_secs: u64,
}

#[derive(Deserialize)]
struct WireLog {
    address: String,
    #[serde(default)]
    topics: Vec<String>,
    #[serde(default)]
    data: String,
    #[serde(rename = "blockNumber", default)]
    block_number: Option<String>,
}

#[derive(Deserialize)]
struct WireFrame {
    method: Option<String>,
    params: Option<WireParams>,
}

#[derive(Deserialize)]
struct WireParams {
    result: Option<WireLog>,
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub(super) fn decode(
    local_ts_ns: u128,
    stream: &str,
    payload: &str,
) -> Result<DecodedEvent, ReplayError> {
    let value = parse_json(stream, payload)?;

    // Only `eth_subscription` notifications carry a log. Everything else
    // (subscribe acks, errors) routes to Unknown.
    let frame: WireFrame =
        serde_json::from_value(value.clone()).map_err(|e| ReplayError::Decode {
            stream: stream.to_string(),
            reason: format!("frame: {e}"),
        })?;

    if frame.method.as_deref() != Some("eth_subscription") {
        return Ok(unknown(local_ts_ns, Venue::Chainlink, stream, value));
    }

    let log = match frame.params.and_then(|p| p.result) {
        Some(l) => l,
        None => return Ok(unknown(local_ts_ns, Venue::Chainlink, stream, value)),
    };

    let answer_updated =
        if log.topics.first().map(|s| s.to_ascii_lowercase()).as_deref() == Some(ANSWER_UPDATED_TOPIC0)
        {
            decode_answer_updated(&log.topics, &log.data)
        } else {
            None
        };

    Ok(DecodedEvent::ChainlinkLog(ChainlinkLog {
        local_ts_ns,
        address: log.address.to_ascii_lowercase(),
        topics: log.topics,
        data: log.data,
        block_number: log.block_number,
        answer_updated,
    }))
}

// ---------------------------------------------------------------------------
// AnswerUpdated decoding
// ---------------------------------------------------------------------------

/// Decode `AnswerUpdated(int256 current, uint256 roundId, uint256 updatedAt)`.
///
/// Topic layout (Solidity):
/// * `topics[0]` = event signature hash
/// * `topics[1]` = indexed `current` (int256, two's-complement)
/// * `topics[2]` = indexed `roundId` (uint256)
/// * `data` = ABI-encoded `updatedAt` (32 bytes, uint256)
fn decode_answer_updated(topics: &[String], data: &str) -> Option<ChainlinkAnswerUpdated> {
    let current_topic = topics.get(1)?;
    let round_topic = topics.get(2)?;

    let current_price = i256_topic_to_decimal(current_topic)?;
    let round_id = u128_from_topic(round_topic)?;
    let updated_at_secs = u64_from_data_word0(data)?;

    Some(ChainlinkAnswerUpdated {
        current_price,
        round_id,
        updated_at_secs,
    })
}

/// Convert a 32-byte two's-complement int256 topic into [`Decimal`].
///
/// We narrow to `i64` because Chainlink price feeds comfortably fit:
/// even a 1.0 USD price at 18 decimals is 10^18, well under i64::MAX
/// (9.2 × 10^18); BTC/USD at 8 decimals is ~7.8 × 10^12.
///
/// Returns `None` when sign-extension doesn't match the sign bit (i.e.
/// the value would overflow i64), so callers can spot rare exotic
/// feeds rather than getting silently-truncated answers.
fn i256_topic_to_decimal(topic: &str) -> Option<Decimal> {
    let bytes = parse_hex32(topic)?;
    let sign_bit = bytes[0] & 0x80; // sign bit of int256

    if sign_bit == 0 {
        // Positive: high 24 bytes must be zero (else > i64::MAX).
        if bytes[..24].iter().any(|&b| b != 0) {
            return None;
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes[24..]);
        let v = i64::from_be_bytes(buf);
        // If the low 8 bytes' top bit is set, the value would parse
        // negative as i64 even though int256 said positive — overflow.
        if v < 0 {
            return None;
        }
        Some(Decimal::from(v))
    } else {
        // Negative: high 24 bytes must all be 0xff (sign-extended).
        if bytes[..24].iter().any(|&b| b != 0xff) {
            return None;
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes[24..]);
        let v = i64::from_be_bytes(buf);
        // Sign extension says negative, but i64 disagrees → overflow.
        if v >= 0 {
            return None;
        }
        Some(Decimal::from(v))
    }
}

fn u128_from_topic(topic: &str) -> Option<u128> {
    let bytes = parse_hex32(topic)?;
    let high = &bytes[..16];
    if high.iter().any(|&b| b != 0) {
        return None;
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&bytes[16..]);
    Some(u128::from_be_bytes(buf))
}

fn u64_from_data_word0(data: &str) -> Option<u64> {
    let stripped = data.strip_prefix("0x").unwrap_or(data);
    if stripped.len() < 64 {
        return None;
    }
    let word0 = &stripped[..64];
    let bytes = decode_hex(word0)?;
    let high = &bytes[..24];
    if high.iter().any(|&b| b != 0) {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[24..]);
    Some(u64::from_be_bytes(buf))
}

fn parse_hex32(topic: &str) -> Option<[u8; 32]> {
    let stripped = topic.strip_prefix("0x").unwrap_or(topic);
    if stripped.len() != 64 {
        return None;
    }
    let v = decode_hex(stripped)?;
    let mut out = [0u8; 32];
    out.copy_from_slice(&v);
    Some(out)
}

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for chunk in s.as_bytes().chunks(2) {
        let hi = nibble(chunk[0])?;
        let lo = nibble(chunk[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

fn nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn pad_topic_uint(hex_no_prefix: &str) -> String {
        format!("0x{:0>64}", hex_no_prefix)
    }

    fn pad_data_uint(hex_no_prefix: &str) -> String {
        format!("0x{:0>64}", hex_no_prefix)
    }

    #[test]
    fn decodes_subscription_ack_as_unknown() {
        let p = r#"{"jsonrpc":"2.0","id":1,"result":"0xabc"}"#;
        let e = decode(0, "_subscription_ack", p).unwrap();
        assert!(matches!(e, DecodedEvent::Unknown { .. }));
    }

    #[test]
    fn decodes_log_without_answer_updated_topic() {
        // Random topic0 — not AnswerUpdated.
        let p = r#"{
            "jsonrpc":"2.0","method":"eth_subscription",
            "params":{"subscription":"0xS","result":{
                "address":"0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c",
                "topics":["0xdeadbeef0000000000000000000000000000000000000000000000000000beef"],
                "data":"0x",
                "blockNumber":"0x100"
            }}
        }"#;
        let e = decode(7, "0xf4030086522a5beea4988f8ca5b36dbc97bee88c@logs", p).unwrap();
        match e {
            DecodedEvent::ChainlinkLog(l) => {
                assert_eq!(l.local_ts_ns, 7);
                assert_eq!(l.address, "0xf4030086522a5beea4988f8ca5b36dbc97bee88c");
                assert_eq!(l.block_number.as_deref(), Some("0x100"));
                assert!(l.answer_updated.is_none());
            }
            _ => panic!("expected ChainlinkLog"),
        }
    }

    #[test]
    fn decodes_real_answer_updated() {
        // current = 0x71F1B66B400 = 7,830,185,096,192. Roughly the
        // shape of a BTC/USD AggregatorV3 reading at 8-decimal scale.
        let current = pad_topic_uint("71F1B66B400");
        let round = pad_topic_uint("123456");
        let data = pad_data_uint("65fa12c0"); // updatedAt = 1710930112

        let p = format!(
            r#"{{
                "jsonrpc":"2.0","method":"eth_subscription",
                "params":{{"subscription":"0xS","result":{{
                    "address":"0xCONTRACT",
                    "topics":["{}", "{}", "{}"],
                    "data":"{}",
                    "blockNumber":"0x100"
                }}}}
            }}"#,
            ANSWER_UPDATED_TOPIC0, current, round, data
        );

        let e = decode(0, "0xcontract@logs", &p).unwrap();
        match e {
            DecodedEvent::ChainlinkLog(l) => {
                let au = l.answer_updated.expect("AnswerUpdated decoded");
                assert_eq!(au.current_price, Decimal::from(7_830_185_096_192i64));
                assert_eq!(au.round_id, 0x123456);
                assert_eq!(au.updated_at_secs, 0x65fa12c0);
            }
            _ => panic!("expected ChainlinkLog"),
        }
    }

    #[test]
    fn answer_updated_handles_negative_int256() {
        // -1 in int256 = 0xff..ff (32 bytes of 0xff).
        let neg_one = "0x".to_string() + &"ff".repeat(32);
        let round = pad_topic_uint("1");
        let data = pad_data_uint("0");

        let p = format!(
            r#"{{
                "jsonrpc":"2.0","method":"eth_subscription",
                "params":{{"subscription":"0xS","result":{{
                    "address":"0xC",
                    "topics":["{}", "{}", "{}"],
                    "data":"{}"
                }}}}
            }}"#,
            ANSWER_UPDATED_TOPIC0, neg_one, round, data
        );

        let e = decode(0, "x", &p).unwrap();
        match e {
            DecodedEvent::ChainlinkLog(l) => {
                let au = l.answer_updated.expect("decoded");
                assert_eq!(au.current_price, Decimal::from(-1));
            }
            _ => panic!("expected ChainlinkLog"),
        }
    }

    #[test]
    fn invalid_data_word_returns_log_without_answer_updated() {
        // Topic0 is AnswerUpdated but data is too short — we still
        // surface the log, just with answer_updated == None.
        let p = format!(
            r#"{{
                "jsonrpc":"2.0","method":"eth_subscription",
                "params":{{"subscription":"0xS","result":{{
                    "address":"0xC",
                    "topics":["{}", "{}", "{}"],
                    "data":"0x12"
                }}}}
            }}"#,
            ANSWER_UPDATED_TOPIC0,
            pad_topic_uint("1"),
            pad_topic_uint("1"),
        );
        let e = decode(0, "x", &p).unwrap();
        match e {
            DecodedEvent::ChainlinkLog(l) => assert!(l.answer_updated.is_none()),
            _ => panic!("expected ChainlinkLog"),
        }
    }

    #[test]
    fn malformed_envelope_surfaces_decode_error() {
        let p = r#"{"jsonrpc":"2.0","method":"eth_subscription","params":42}"#;
        assert!(matches!(decode(0, "x", p), Err(ReplayError::Decode { .. })));
    }
}
