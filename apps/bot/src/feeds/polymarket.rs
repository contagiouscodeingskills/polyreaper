//! Polymarket integration:
//! 1. Market discovery via Gamma → emit `MarketChanged` when the active
//!    BTC up/down 5m market rolls over.
//! 2. CLOB `/book?token_id=...` REST polling → emit `PolyBook` snapshots
//!    of YES-side mid for the currently active market.
//!
//! No order placement in v0 — that's behind the live-mode gate (and needs
//! wallet creds we don't have yet).

use std::collections::HashSet;
use std::time::Duration;

use market_registry::{GammaAdapter, Market};
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::{mpsc, watch};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::bot::BotEvent;
use crate::config::{GammaSettings, PolymarketFeedSettings};
use crate::market_state::PolyBookSnapshot;

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
                    debug!(
                        component = "polymarket_disc",
                        "no tradeable BTC 5m market right now"
                    );
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
    let body = resp.text().await.map_err(|e| format!("gamma body: {e}"))?;
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

/// Poll the CLOB book for the currently-active market's YES *and* NO
/// tokens, in parallel. Emits one `PolyBook` event per poll carrying a
/// full `PolyBookSnapshot` (both sides' bid/ask), so the bot can use
/// the YES mid for trading decisions and log the NO side + ask/bid
/// detail for diagnostics.
///
/// The poller reads `active_market_rx` to know which market to fetch.
/// Late responses from a previous market are dropped by the bot loop
/// (which checks `snapshot.market_id` against its `active`).
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
        tokio::select! {
            _ = active_market_rx.changed() => {}
            _ = interval.tick() => {}
        }

        let market = match active_market_rx.borrow().clone() {
            Some(m) => m,
            None => continue,
        };

        let base = cfg.clob_url.trim_end_matches('/');
        let yes_url = format!("{}/book?token_id={}", base, market.yes_token);
        let no_url = format!("{}/book?token_id={}", base, market.no_token);

        // Fetch both sides concurrently — saves ~one round-trip per poll
        // and keeps the YES/NO snapshot from straddling a price move.
        let (yes_res, no_res) = tokio::join!(
            fetch_one_tob(&client, &yes_url),
            fetch_one_tob(&client, &no_url),
        );

        let (yes_bid, yes_ask, yes_bid_size, yes_ask_size) = match yes_res {
            Some(t) => (
                Some(t.bid_price),
                Some(t.ask_price),
                Some(t.bid_size),
                Some(t.ask_size),
            ),
            None => (None, None, None, None),
        };
        let (no_bid, no_ask, no_bid_size, no_ask_size) = match no_res {
            Some(t) => (
                Some(t.bid_price),
                Some(t.ask_price),
                Some(t.bid_size),
                Some(t.ask_size),
            ),
            None => (None, None, None, None),
        };

        if yes_bid.is_none() && no_bid.is_none() {
            debug!(
                component = "polymarket_book",
                "no usable book on either side"
            );
            continue;
        }

        let t_ns = common::LocalTimestamp::now().as_nanos();
        let snapshot = PolyBookSnapshot {
            yes_bid,
            yes_ask,
            yes_bid_size,
            yes_ask_size,
            no_bid,
            no_ask,
            no_bid_size,
            no_ask_size,
            captured_local_ts_ns: t_ns,
        };
        let ev = BotEvent::PolyBook {
            t_ns,
            market_id: market.id.clone(),
            snapshot,
        };
        if tx.send(ev).await.is_err() {
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Resolution sweeper
// ---------------------------------------------------------------------------

/// Periodically poll Gamma `?closed=true` for resolved markets and emit
/// `BotEvent::MarketResolved` for any market we haven't already seen
/// resolve.
///
/// Uses the recorder's existing `GammaAdapter::fetch_resolved_events()`
/// which already filters to the configured series + sorts by endDate
/// descending. Dedups in-process via a HashSet of seen market IDs.
pub async fn run_resolution_sweeper(
    cfg: GammaSettings,
    tx: mpsc::Sender<BotEvent>,
    poll_secs: u64,
) {
    let rec_cfg = config::MarketDiscoveryConfig {
        gamma_url: cfg.url.clone(),
        poll_interval_secs: cfg.poll_interval_secs,
        series_slug: cfg.series_slug.clone(),
    };
    let adapter = match GammaAdapter::new(&rec_cfg) {
        Ok(a) => a,
        Err(e) => {
            warn!(component = "resolution_sweeper", error = %e, "adapter; sweeper disabled");
            return;
        }
    };

    let mut seen_ids: HashSet<String> = HashSet::new();

    loop {
        match adapter.fetch_resolved_events().await {
            Ok(resolved) => {
                let mut emitted = 0usize;
                for rm in resolved {
                    let id_str = rm.market.id.as_str().to_string();
                    if seen_ids.contains(&id_str) {
                        continue;
                    }
                    if let Some(outcome) = rm.market.resolved_outcome {
                        let ev = BotEvent::MarketResolved {
                            market_id: rm.market.id.clone(),
                            market_slug: rm.market.slug.clone(),
                            end_epoch: rm.market.end_time_epoch,
                            outcome,
                        };
                        if tx.send(ev).await.is_err() {
                            return;
                        }
                        seen_ids.insert(id_str);
                        emitted += 1;
                    }
                }
                if emitted > 0 {
                    info!(
                        component = "resolution_sweeper",
                        emitted = emitted,
                        seen_total = seen_ids.len(),
                        "emitted new resolutions"
                    );
                }
            }
            Err(e) => {
                warn!(component = "resolution_sweeper", error = %e, "fetch failed; will retry");
            }
        }
        sleep(Duration::from_secs(poll_secs.max(5))).await;
    }
}

/// Top-of-book pair with sizes. `bid_size` / `ask_size` are USDC-denominated
/// share counts at that price level (Polymarket sizes are in shares; price ×
/// shares ≈ USDC notional).
#[derive(Debug, Clone, Copy)]
struct TopOfBook {
    bid_price: f64,
    bid_size: f64,
    ask_price: f64,
    ask_size: f64,
}

/// Fetch one side's book and return its top-of-book TOB if the response
/// is usable. Routine failures log at debug to avoid foreground spam.
async fn fetch_one_tob(client: &Client, url: &str) -> Option<TopOfBook> {
    match client.get(url).send().await {
        Ok(resp) => {
            let status = resp.status();
            if !status.is_success() {
                debug!(component = "polymarket_book", url = %url, status = %status, "non-200");
                return None;
            }
            match resp.text().await {
                Ok(body) => parse_book_tob(&body),
                Err(e) => {
                    debug!(component = "polymarket_book", error = %e, "body read");
                    None
                }
            }
        }
        Err(e) => {
            debug!(component = "polymarket_book", error = %e, "http error");
            None
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

/// Extract top-of-book (best bid+size, best ask+size) from a CLOB book
/// response. Returns `None` if either side is empty or the book is
/// crossed/zero-spread. Sorts defensively — `best_bid = max(bids.price)`,
/// `best_ask = min(asks.price)`. Sizes are paired with the chosen prices.
fn parse_book_tob(body: &str) -> Option<TopOfBook> {
    let parsed: CloBookResponse = serde_json::from_str(body).ok()?;
    let best_bid = parsed
        .bids
        .iter()
        .filter_map(|l| {
            let p = l.price.parse::<f64>().ok()?;
            if !(p.is_finite() && p > 0.0 && p < 1.0) {
                return None;
            }
            let s = l
                .size
                .as_ref()
                .and_then(|s| s.parse::<f64>().ok())
                .filter(|s| s.is_finite() && *s >= 0.0)
                .unwrap_or(0.0);
            Some((p, s))
        })
        .fold(None::<(f64, f64)>, |acc, (p, s)| match acc {
            None => Some((p, s)),
            Some((cp, cs)) => {
                if p > cp {
                    Some((p, s))
                } else if p == cp {
                    Some((cp, cs + s))
                } else {
                    Some((cp, cs))
                }
            }
        })?;
    let best_ask = parsed
        .asks
        .iter()
        .filter_map(|l| {
            let p = l.price.parse::<f64>().ok()?;
            if !(p.is_finite() && p > 0.0 && p < 1.0) {
                return None;
            }
            let s = l
                .size
                .as_ref()
                .and_then(|s| s.parse::<f64>().ok())
                .filter(|s| s.is_finite() && *s >= 0.0)
                .unwrap_or(0.0);
            Some((p, s))
        })
        .fold(None::<(f64, f64)>, |acc, (p, s)| match acc {
            None => Some((p, s)),
            Some((cp, cs)) => {
                if p < cp {
                    Some((p, s))
                } else if p == cp {
                    Some((cp, cs + s))
                } else {
                    Some((cp, cs))
                }
            }
        })?;
    if best_ask.0 <= best_bid.0 {
        return None;
    }
    Some(TopOfBook {
        bid_price: best_bid.0,
        bid_size: best_bid.1,
        ask_price: best_ask.0,
        ask_size: best_ask.1,
    })
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
            mk_market("A", None, now - 300),       // closed (end in the past)
            mk_market("B", None, now + 360),       // future (end > now + 300)
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
        let markets = vec![mk_market(
            "far-future-with-stale-start",
            Some(now - 86400),
            now + 1000,
        )];
        assert!(pick_active_market(&markets, now).is_none());
    }

    #[test]
    fn parse_book_picks_best_levels_defensively() {
        let body = r#"{
            "bids":[{"price":"0.48","size":"100"},{"price":"0.49","size":"50"}],
            "asks":[{"price":"0.52","size":"100"},{"price":"0.51","size":"50"}]
        }"#;
        let tob = parse_book_tob(body).expect("should parse");
        assert!((tob.bid_price - 0.49).abs() < 1e-9);
        assert!((tob.bid_size - 50.0).abs() < 1e-9);
        assert!((tob.ask_price - 0.51).abs() < 1e-9);
        assert!((tob.ask_size - 50.0).abs() < 1e-9);
    }

    #[test]
    fn parse_book_aggregates_size_at_same_best_price() {
        // Two levels at the same best price — sum their sizes.
        let body = r#"{
            "bids":[{"price":"0.49","size":"20"},{"price":"0.49","size":"30"}],
            "asks":[{"price":"0.51","size":"15"},{"price":"0.51","size":"25"}]
        }"#;
        let tob = parse_book_tob(body).expect("should parse");
        assert!((tob.bid_size - 50.0).abs() < 1e-9);
        assert!((tob.ask_size - 40.0).abs() < 1e-9);
    }

    #[test]
    fn parse_book_returns_none_on_empty_side() {
        let body = r#"{"bids":[{"price":"0.5","size":"1"}],"asks":[]}"#;
        assert!(parse_book_tob(body).is_none());
    }

    #[test]
    fn parse_book_rejects_crossed_or_inverted_book() {
        let body = r#"{"bids":[{"price":"0.60","size":"1"}],"asks":[{"price":"0.40","size":"1"}]}"#;
        assert!(parse_book_tob(body).is_none());
    }

    #[test]
    fn parse_book_ignores_garbage_prices() {
        let body = r#"{"bids":[{"price":"abc"},{"price":"0.49","size":"7"}],"asks":[{"price":"xx"},{"price":"0.51","size":"3"}]}"#;
        let tob = parse_book_tob(body).expect("should parse");
        assert!((tob.bid_price - 0.49).abs() < 1e-9);
        assert!((tob.ask_price - 0.51).abs() < 1e-9);
        assert!((tob.bid_size - 7.0).abs() < 1e-9);
        assert!((tob.ask_size - 3.0).abs() < 1e-9);
    }

    #[test]
    fn parse_book_handles_missing_size() {
        let body = r#"{"bids":[{"price":"0.49"}],"asks":[{"price":"0.51"}]}"#;
        let tob = parse_book_tob(body).expect("should parse");
        assert!((tob.bid_price - 0.49).abs() < 1e-9);
        assert!((tob.bid_size - 0.0).abs() < 1e-9);
    }
}
