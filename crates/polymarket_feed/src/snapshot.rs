//! Pre-flight REST snapshot of the Polymarket order book.
//!
//! Mirrors `binance_feed::snapshot` — the WS subscription delivers diffs
//! (price_change events) that build on the initial `book` event. If a
//! disconnect happens between subscribe and that first event, the diff
//! stream has no baseline to apply to and replay-time book reconstruction
//! breaks.
//!
//! On every successful WS connect we GET `/book?token_id=<id>` for each
//! subscribed token in parallel and persist the response as a
//! `RawEvent` under stream `<token_id>@book_snapshot`. Each response is
//! the full book state at the moment of the REST call, so it serves as
//! the absolute reference the diff stream amends.
//!
//! Failure is non-fatal: if any individual snapshot 404s or times out
//! we log the per-token error and continue. Diffs alone are still
//! useful for order-flow analysis even without a baseline.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{LocalTimestamp, RawEvent, Venue};

const REST_TIMEOUT_SECS: u64 = 10;

/// Fetch `/book?token_id=...` for every token in `token_ids` and
/// persist each response. Concurrent requests via `tokio::join_all`.
pub(crate) async fn fetch_and_persist(
    clob_base_url: &str,
    token_ids: &[String],
    store: &Arc<Mutex<storage::Store>>,
) -> Result<(), String> {
    if token_ids.is_empty() {
        return Ok(());
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(REST_TIMEOUT_SECS))
        .user_agent(concat!("polybot-recorder/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| format!("reqwest client: {e}"))?;

    let base = clob_base_url.trim_end_matches('/');
    let futures = token_ids.iter().map(|id| {
        let client = client.clone();
        let url = format!("{base}/book?token_id={id}");
        let id = id.clone();
        async move {
            let outcome = client.get(&url).send().await;
            let resp = match outcome {
                Ok(r) => r,
                Err(e) => return (id, Err(format!("send: {e}"))),
            };
            let status = resp.status();
            let body = match resp.text().await {
                Ok(b) => b,
                Err(e) => return (id, Err(format!("body: {e}"))),
            };
            if !status.is_success() {
                return (id, Err(format!("HTTP {status}: {body}")));
            }
            (id, Ok(body))
        }
    });
    let results = futures_util::future::join_all(futures).await;

    let mut ok_count = 0usize;
    let mut err_count = 0usize;
    for (id, res) in results {
        match res {
            Ok(body) => {
                let event = RawEvent {
                    venue: Venue::Polymarket,
                    stream: format!("{id}@book_snapshot"),
                    local_ts_ns: LocalTimestamp::now(),
                    venue_ts_ms: None,
                    payload: body,
                    ..Default::default()
                };
                match store.lock() {
                    Ok(mut g) => match g.write(&event) {
                        Ok(()) => ok_count += 1,
                        Err(e) => {
                            err_count += 1;
                            tracing::warn!(
                                component = "polymarket_feed",
                                event = "snapshot_write_failed",
                                token = %id,
                                error = %e,
                                "snapshot persist failed"
                            );
                        }
                    },
                    Err(p) => {
                        // Mutex was poisoned by an earlier panic. Recover by
                        // taking the inner store anyway — losing one snapshot
                        // is better than crashing the whole feed task.
                        let mut g = p.into_inner();
                        match g.write(&event) {
                            Ok(()) => ok_count += 1,
                            Err(e) => {
                                err_count += 1;
                                tracing::warn!(
                                    component = "polymarket_feed",
                                    event = "snapshot_write_failed_poisoned",
                                    token = %id,
                                    error = %e,
                                    "snapshot persist failed (poisoned mutex recovered)"
                                );
                            }
                        }
                    }
                }
            }
            Err(reason) => {
                err_count += 1;
                tracing::warn!(
                    component = "polymarket_feed",
                    event = "snapshot_fetch_failed",
                    token = %id,
                    reason = %reason,
                    "REST snapshot fetch failed; relying on first WS book event"
                );
            }
        }
    }
    tracing::info!(
        component = "polymarket_feed",
        event = "snapshot_fetch_complete",
        ok = ok_count,
        err = err_count,
        total = token_ids.len(),
        "book snapshots persisted"
    );
    Ok(())
}
