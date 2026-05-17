//! Polymarket integration:
//! 1. Market discovery via Gamma → emit `MarketChanged` when the active
//!    BTC up/down 5m market rolls over.
//! 2. CLOB `/book?token_id=...` REST polling → emit `PolyBook` snapshots
//!    of YES-side mid for the currently active market.
//!
//! No order placement in v0 — that's behind the live-mode gate (and needs
//! wallet creds we don't have yet).

use std::time::Duration;

use market_registry::{GammaAdapter, Market};
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::{mpsc, watch};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::bot::BotEvent;
use crate::config::{GammaSettings, PolymarketFeedSettings};

// ---------------------------------------------------------------------------
// Market discovery loop
// ---------------------------------------------------------------------------

/// Poll Gamma for the active BTC up/down 5m market. Emits `MarketChanged`
/// when the chosen market ID changes from the previous tick. Also writes
/// the currently-active market into the `watch` channel so the book poller
/// always knows which token_id to fetch.
///
/// We do NOT use `GammaAdapter::discover()` directly because its query
/// (`order=startDate&ascending=false&limit=500`) returns Polymarket's
/// 100-result cap of *newest-by-creation* markets, all of which are
/// scheduled to trade 15-24h from now. The currently-trading market
/// (created ~24h ago) gets pushed off the bottom of the list.
///
/// Instead we query with `order=endDate&ascending=true` — same `limit=500`
/// cap of 100, but the window now spans (oldest-ending closed markets) →
/// currently-trading → near-future. The currently-trading market is
/// always present and filtered for client-side by `pick_active_market`.
pub async fn run_market_discovery(
    cfg: GammaSettings,
    tx: mpsc::Sender<BotEvent>,
    active_market_tx: watch::Sender<Option<Market>>,
) {
    // Adapter is used only for its `map_response()` (parses the gamma
    // JSON envelope into Market objects, handles Up/Down vs Yes/No
    // outcome labels, etc.). The HTTP fetch is local because we need a
    // different sort than the adapter's hardcoded one.
    let rec_cfg = config::MarketDiscoveryConfig {
        gamma_url: cfg.url.clone(),
        poll_interval_secs: cfg.poll_interval_secs,
        series_slug: cfg.series_slug.clone(),
    };
    let adapter = match GammaAdapter::new(&rec_cfg) {
        Ok(a) => a,
        Err(e) => {
            warn!(component = "polymarket_disc", error = %e, "failed to build adapter; feed disabled");
            return;
        }
    };

    let client = match Client::builder()
        .timeout(Duration::from_secs(15))
        .user_agent(concat!("polybot/", env!("CARGO_PKG_VERSION")))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            warn!(component = "polymarket_disc", error = %e, "reqwest client; feed disabled");
            return;
        }
    };

    let mut last_emitted_id: Option<String> = None;

    loop {
        let now_secs = chrono_now_epoch_secs();
        match fetch_active_markets(&client, &cfg, &adapter).await {
            Ok(markets) => {
                if let Some(chosen) = pick_active_market(&markets, now_secs) {
                    let id_str = chosen.id.as_str().to_string();
                    let _ = active_market_tx.send(Some(chosen.clone()));
                    if last_emitted_id.as_deref() != Some(id_str.as_str()) {
                        info!(
                            component = "polymarket_disc",
                            market_id = %id_str,
                            slug = %chosen.slug,
                            end_epoch = chosen.end_time_epoch,
                            ttr_secs = chosen.end_time_epoch.saturating_sub(now_secs),
                            "active market changed"
                        );
                        last_emitted_id = Some(id_str);
                        if tx
                            .send(BotEvent::MarketChanged { market: chosen })
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                } else {
                    debug!(component = "polymarket_disc", "no tradeable BTC 5m market right now");
                    let _ = active_market_tx.send(None);
                }
            }
            Err(e) => {
                warn!(component = "polymarket_disc", error = %e, "gamma discovery failed; will retry");
            }
        }
        sleep(Duration::from_secs(cfg.poll_interval_secs.max(1))).await;
    }
}

/// Custom Gamma fetch using `order=endDate&ascending=true` so the
/// currently-trading market is in the result set. Returns parsed
/// `Market` objects (via the adapter's existing `map_response`).
async fn fetch_active_markets(
    client: &Client,
    cfg: &GammaSettings,
    adapter: &GammaAdapter,
) -> Result<Vec<Market>, String> {
    let resp = client
        .get(&cfg.url)
        .query(&[
            ("active", "true"),
            ("closed", "false"),
            ("series_slug", cfg.series_slug.as_str()),
            ("order", "endDate"),
            ("ascending", "true"),
            ("limit", "500"),
        ])
        .send()
        .await
        .map_err(|e| format!("gamma GET: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("gamma HTTP {status}"));
    }
    let body = resp
        .text()
        .await
        .map_err(|e| format!("gamma body: {e}"))?;
    adapter
        .map_response(&body)
        .map_err(|e| format!("gamma parse: {e}"))
}

/// Pick the currently-trading 5-minute BTC up/down market.
///
/// Gamma's `startDate` for these markets points at the series-creation
/// time (or some similar bulk timestamp) — sometimes ~24 h before the
/// actual 5-minute trading window. So we can't trust `start_time_epoch`.
/// Instead we filter on `end_time_epoch`: the market currently trading is
/// the one resolving in `(now + freeze, now + market_duration]`. For the
/// BTC 5m series that's `(now + 10, now + 300]`. The slug encodes
/// `start = end - 300`.
///
/// Returns `None` when no market is in that window — happens briefly
/// during the rollover between one market closing and the next reaching
/// its trading window.
fn pick_active_market(markets: &[Market], now_secs: i64) -> Option<Market> {
    const MARKET_DURATION_SECS: i64 = 300;
    const FREEZE_WINDOW_SECS: i64 = 10;
    markets
        .iter()
        .filter(|m| {
            let end = m.end_time_epoch;
            end > now_secs + FREEZE_WINDOW_SECS && end <= now_secs + MARKET_DURATION_SECS
        })
        // If somehow multiple match, pick the one closest to resolving
        // (smallest end) — that's the freshest currently-trading market.
        .min_by_key(|m| m.end_time_epoch)
        .cloned()
}

fn chrono_now_epoch_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// CLOB book poller
// ---------------------------------------------------------------------------

/// Poll the CLOB book for the currently-active market's YES token. Emits
/// `PolyBook` snapshots on every successful poll.
///
/// The poller reads `active_market_rx` to know which token to fetch.
/// Restarts polling cleanly when the active market changes — no stale
/// snapshots from the previous market.
pub async fn run_book_poller(
    cfg: PolymarketFeedSettings,
    mut active_market_rx: watch::Receiver<Option<Market>>,
    tx: mpsc::Sender<BotEvent>,
) {
    let client = match Client::builder()
        .timeout(Duration::from_secs(5))
        .user_agent(concat!("polybot/", env!("CARGO_PKG_VERSION")))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            warn!(component = "polymarket_book", error = %e, "reqwest client; poller disabled");
            return;
        }
    };
    let mut interval = tokio::time::interval(Duration::from_millis(cfg.book_poll_ms.max(50)));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        // Wait for either an active market or a tick.
        tokio::select! {
            _ = active_market_rx.changed() => {}
            _ = interval.tick() => {}
        }

        let market = match active_market_rx.borrow().clone() {
            Some(m) => m,
            None => continue,
        };
        let token_id = market.yes_token.as_str().to_string();
        let url = format!("{}/book?token_id={}", cfg.clob_url.trim_end_matches('/'), token_id);

        match client.get(&url).send().await {
            Ok(resp) => {
                let status = resp.status();
                if !status.is_success() {
                    warn!(component = "polymarket_book", status = %status, "non-200 from clob");
                    continue;
                }
                let body = match resp.text().await {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(component = "polymarket_book", error = %e, "body read");
                        continue;
                    }
                };
                match parse_book_mid(&body) {
                    Some(yes_mid) => {
                        let t_ns = common::LocalTimestamp::now().as_nanos();
                        let ev = BotEvent::PolyBook {
                            t_ns,
                            market_id: market.id.clone(),
                            yes_mid,
                        };
                        if tx.send(ev).await.is_err() {
                            return;
                        }
                    }
                    None => {
                        debug!(component = "polymarket_book", "no usable mid in book response");
                    }
                }
            }
            Err(e) => {
                warn!(component = "polymarket_book", error = %e, "http error");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Book parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct CloBookResponse {
    #[serde(default)]
    bids: Vec<CloBookLevel>,
    #[serde(default)]
    asks: Vec<CloBookLevel>,
}

#[derive(Debug, Deserialize)]
struct CloBookLevel {
    price: String,
    #[allow(dead_code)]
    #[serde(default)]
    size: Option<String>,
}

/// Compute YES-side mid = (best_bid + best_ask) / 2 from a CLOB book
/// response. Returns `None` if either side is empty or unparseable.
///
/// Sorts defensively rather than assuming the venue's ordering: best_bid
/// = max(bids.price), best_ask = min(asks.price).
fn parse_book_mid(body: &str) -> Option<f64> {
    let parsed: CloBookResponse = serde_json::from_str(body).ok()?;
    let best_bid = parsed
        .bids
        .iter()
        .filter_map(|l| l.price.parse::<f64>().ok())
        .filter(|p| p.is_finite() && *p > 0.0 && *p < 1.0)
        .fold(None::<f64>, |acc, p| match acc {
            None => Some(p),
            Some(cur) => Some(cur.max(p)),
        })?;
    let best_ask = parsed
        .asks
        .iter()
        .filter_map(|l| l.price.parse::<f64>().ok())
        .filter(|p| p.is_finite() && *p > 0.0 && *p < 1.0)
        .fold(None::<f64>, |acc, p| match acc {
            None => Some(p),
            Some(cur) => Some(cur.min(p)),
        })?;
    if best_ask <= best_bid {
        // Crossed or zero spread — distrust and skip this tick.
        return None;
    }
    Some((best_bid + best_ask) / 2.0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use market_registry::{MarketId, TokenId};

    fn mk_market(id: &str, start: Option<i64>, end: i64) -> Market {
        Market {
            id: MarketId::new(id),
            title: "T".into(),
            slug: id.into(),
            yes_token: TokenId::new(format!("{id}-Y")),
            no_token: TokenId::new(format!("{id}-N")),
            start_time_epoch: start,
            end_time_epoch: end,
            resolved_outcome: None,
        }
    }

    #[test]
    fn pick_active_returns_none_when_all_in_future_or_past() {
        let now = 1000;
        let markets = vec![
            mk_market("A", None, now - 300),      // closed (end in the past)
            mk_market("B", None, now + 360),      // future (end > now + 300)
            mk_market("C", None, now + 1_000_000), // far future
        ];
        assert!(pick_active_market(&markets, now).is_none());
    }

    #[test]
    fn pick_active_picks_market_with_end_in_next_5_minutes() {
        // Mimics live Gamma: ~100 markets returned, only one currently
        // trading. start_time_epoch may be far in the past or even unset.
        let now = 1000;
        let markets = vec![
            mk_market("upcoming-1", Some(now - 86400), now + 360),
            mk_market("upcoming-2", Some(now - 86400), now + 600),
            mk_market("currently-trading", Some(now - 86400), now + 240),
            mk_market("closed", Some(now - 86400), now - 60),
        ];
        let chosen = pick_active_market(&markets, now).expect("one is active");
        assert_eq!(chosen.id.as_str(), "currently-trading");
    }

    #[test]
    fn pick_active_filters_freeze_window() {
        let now = 1000;
        // 5-second TTR — inside the 10s freeze window of the picker.
        let markets = vec![mk_market("almost-done", None, now + 5)];
        assert!(pick_active_market(&markets, now).is_none());
    }

    #[test]
    fn pick_active_ignores_polymarket_garbage_start_date() {
        // The bug from the live test: Polymarket's gamma sometimes gives
        // a start_date 24h in the past for what's actually a future market.
        // The picker must not be fooled.
        let now = 1000;
        let markets = vec![
            mk_market("far-future-with-stale-start", Some(now - 86400), now + 1000),
        ];
        assert!(pick_active_market(&markets, now).is_none());
    }

    #[test]
    fn parse_book_picks_best_levels_defensively() {
        // Both sides emitted out-of-order — best_bid should be highest,
        // best_ask should be lowest.
        let body = r#"{
            "bids":[{"price":"0.48","size":"100"},{"price":"0.49","size":"50"}],
            "asks":[{"price":"0.52","size":"100"},{"price":"0.51","size":"50"}]
        }"#;
        let mid = parse_book_mid(body).expect("should parse");
        assert!((mid - 0.50).abs() < 1e-9);
    }

    #[test]
    fn parse_book_returns_none_on_empty_side() {
        let body = r#"{"bids":[{"price":"0.5","size":"1"}],"asks":[]}"#;
        assert!(parse_book_mid(body).is_none());
    }

    #[test]
    fn parse_book_rejects_crossed_or_inverted_book() {
        // best_ask < best_bid — venue glitch; refuse.
        let body = r#"{"bids":[{"price":"0.60","size":"1"}],"asks":[{"price":"0.40","size":"1"}]}"#;
        assert!(parse_book_mid(body).is_none());
    }

    #[test]
    fn parse_book_ignores_garbage_prices() {
        let body = r#"{"bids":[{"price":"abc"},{"price":"0.49"}],"asks":[{"price":"xx"},{"price":"0.51"}]}"#;
        let mid = parse_book_mid(body).expect("should parse");
        assert!((mid - 0.50).abs() < 1e-9);
    }
}
