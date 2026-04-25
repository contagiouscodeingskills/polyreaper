//! Pre-flight depth snapshot fetch.
//!
//! Binance's `@depth@100ms` stream emits *diffs* — to reconstruct the full
//! book at any moment, replay needs an absolute baseline. This module
//! makes one REST call to `/api/v3/depth?symbol=...&limit=1000` on each
//! successful WebSocket connect and writes the response as a [`RawEvent`]
//! under the stream `<symbol>@depth_snapshot`.
//!
//! The snapshot's `lastUpdateId` lets the replayer line up subsequent
//! diffs against this baseline (Binance's diff messages carry `U`/`u`
//! sequence numbers; replay applies a diff when `u >= lastUpdateId`).
//!
//! Failure is non-fatal: if the REST call errors, we log and proceed with
//! diffs only. Diffs alone are still useful for order-flow / pressure
//! analysis even without a baseline.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{LocalTimestamp, RawEvent, Venue};

const SNAPSHOT_LIMIT: u32 = 1000;
const REST_TIMEOUT_SECS: u64 = 10;

/// Fetch the depth snapshot for `symbol` (e.g. "BTCUSDT") and write it
/// to the store as a single `RawEvent`. Stream name is
/// `"<symbol>@depth_snapshot"` (lowercased to match other Binance streams).
pub(crate) async fn fetch_and_persist(
    symbol: &str,
    store: &Arc<Mutex<storage::Store>>,
) -> Result<(), String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(REST_TIMEOUT_SECS))
        .user_agent(concat!("polybot-recorder/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| format!("reqwest client: {e}"))?;

    let url = format!(
        "https://api.binance.com/api/v3/depth?symbol={}&limit={}",
        symbol.to_uppercase(),
        SNAPSHOT_LIMIT
    );

    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("snapshot GET: {e}"))?
        .error_for_status()
        .map_err(|e| format!("snapshot HTTP: {e}"))?;

    let body = resp
        .text()
        .await
        .map_err(|e| format!("snapshot body: {e}"))?;

    let event = RawEvent {
        venue: Venue::Binance,
        stream: format!("{}@depth_snapshot", symbol.to_lowercase()),
        local_ts_ns: LocalTimestamp::now(),
        venue_ts_ms: None, // snapshot carries lastUpdateId, not a timestamp
        payload: body,
    };

    let mut guard = store.lock().unwrap_or_else(|p| p.into_inner());
    guard
        .write(&event)
        .map_err(|e| format!("snapshot write: {e}"))?;
    Ok(())
}

/// Extract the venue symbol (e.g. `"BTCUSDT"`) from a stream name like
/// `"btcusdt@trade"`. Returns the part before the first `@`, uppercased.
pub(crate) fn extract_symbol(streams: &[String]) -> Option<String> {
    streams
        .first()
        .and_then(|s| s.split('@').next())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_uppercase())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_symbol_from_first_stream() {
        let streams = vec![
            "btcusdt@trade".to_string(),
            "btcusdt@depth@100ms".to_string(),
        ];
        assert_eq!(extract_symbol(&streams), Some("BTCUSDT".to_string()));
    }

    #[test]
    fn extracts_none_from_empty() {
        let streams: Vec<String> = vec![];
        assert_eq!(extract_symbol(&streams), None);
    }

    #[test]
    fn extracts_none_from_empty_first() {
        let streams = vec!["".to_string()];
        assert_eq!(extract_symbol(&streams), None);
    }

    #[test]
    fn handles_stream_without_at() {
        let streams = vec!["bare".to_string()];
        assert_eq!(extract_symbol(&streams), Some("BARE".to_string()));
    }

    #[test]
    fn uppercases_lowercase_symbol() {
        let streams = vec!["ethusdt@kline_1m".to_string()];
        assert_eq!(extract_symbol(&streams), Some("ETHUSDT".to_string()));
    }
}
