//! Resolution sweeper.
//!
//! Polls Gamma for recently-closed markets in the configured series and
//! writes one `RawEvent` per never-before-seen resolution. Each event:
//! * `venue = polymarket`
//! * `stream = "<market.slug>-resolved"` (separate file from the
//!   market's trading data — `<slug>.ndjson` vs `<slug>-resolved.ndjson`)
//! * `payload = serialised Gamma event JSON` (carries `outcomePrices`,
//!   end times, etc. — replay reads the resolution from there)
//!
//! Dedup is in-memory only. A recorder restart re-captures all
//! resolutions in the latest 500 events; replay must dedup by
//! `local_ts_ns + market_id`.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{LocalTimestamp, RawEvent, Venue};
use market_registry::{GammaAdapter, MarketId};

pub async fn run_resolution_sweep_loop(
    adapter: GammaAdapter,
    store: Arc<Mutex<storage::Store>>,
    interval: Duration,
) {
    let mut seen: HashSet<MarketId> = HashSet::new();

    loop {
        match adapter.fetch_resolved_events().await {
            Ok(events) => {
                let mut new_count = 0usize;
                for re in events {
                    if seen.contains(&re.market.id) {
                        continue;
                    }
                    let stream = stream_for_resolved(&re.market);
                    let event = RawEvent {
                        venue: Venue::Polymarket,
                        stream,
                        local_ts_ns: LocalTimestamp::now(),
                        venue_ts_ms: Some(re.market.end_time_epoch.saturating_mul(1_000)),
                        payload: re.raw_event_json,
                    };
                    let mut guard = match store.lock() {
                        Ok(g) => g,
                        Err(p) => p.into_inner(),
                    };
                    match guard.write(&event) {
                        Ok(()) => {
                            seen.insert(re.market.id.clone());
                            new_count += 1;
                        }
                        Err(e) => tracing::error!(
                            component = "recorder",
                            event = "resolution_write_failed",
                            market = %re.market.id,
                            error = %e,
                            "failed to persist resolution"
                        ),
                    }
                }
                tracing::info!(
                    component = "recorder",
                    event = "resolution_sweep",
                    new = new_count,
                    seen_total = seen.len(),
                    "resolution sweep complete"
                );
            }
            Err(e) => tracing::warn!(
                component = "recorder",
                event = "resolution_sweep_failed",
                reason = %e,
                "resolution sweep failed; will retry"
            ),
        }
        tokio::time::sleep(interval).await;
    }
}

fn stream_for_resolved(m: &market_registry::Market) -> String {
    let base = if !m.slug.is_empty() {
        m.slug.clone()
    } else {
        m.id.as_str().to_string()
    };
    format!("{base}-resolved")
}
