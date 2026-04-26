//! Polymarket Gamma API adapter.
//!
//! Fetches live event metadata from `gamma-api.polymarket.com/events`,
//! filters to a configured series (e.g. `"btc-up-or-down-5m"`), and maps
//! each event's single market into a [`Market`] domain object.
//!
//! # Why events, not markets?
//!
//! Polymarket's recurring series (5-minute BTC up/down, hourly ETH ranges,
//! weekly Amazon hit price, …) are exposed as **events**, with each
//! event containing exactly one market. The standalone `/markets`
//! endpoint omits these — they're only reachable via the `/events`
//! endpoint, sorted by `startDate` descending and filtered by series.
//!
//! # Resolution source
//!
//! BTC up/down series resolves via the Chainlink BTC/USD price stream,
//! **not** Binance Spot. The recorder still captures both venues — the
//! research thesis "Binance microstructure → Polymarket pricing" treats
//! that as a feature: cross-venue resolution mismatch is a knob to study,
//! not a bug to fix.
//!
//! # Uncertainty policy
//!
//! Per-market mapping failures (missing required fields, malformed
//! timestamps, unexpected outcome labels) **log and skip**; they do not
//! fail the whole discovery pass. A malformed response *envelope* (not
//! valid JSON, not an array) fails loudly with [`DiscoveryError::Parse`].

use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::{DiscoveryError, Market, MarketDiscoverer, MarketId, Outcome, TokenId};

/// Live Polymarket Gamma discoverer.
pub struct GammaAdapter {
    client: Client,
    base_url: String,
    series_slug: String,
}

impl GammaAdapter {
    /// Build an adapter from recorder config. Fails if the HTTP client
    /// can't be constructed.
    pub fn new(cfg: &config::MarketDiscoveryConfig) -> Result<Self, DiscoveryError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .user_agent(concat!("polybot-recorder/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| DiscoveryError::Adapter(format!("reqwest client: {e}")))?;
        Ok(Self {
            client,
            base_url: cfg.gamma_url.clone(),
            series_slug: cfg.series_slug.clone(),
        })
    }

    /// Fetch the first page of currently-trading events sorted newest-first.
    /// Single page is enough for series with under ~500 concurrently-open
    /// markets (BTC 5m has ~200). Pagination is a future addition if a
    /// series ever exceeds that.
    async fn fetch_raw_json(&self) -> Result<String, DiscoveryError> {
        // `series_slug` is a gamma-side filter — surfaces only events in
        // our series, no need to scan + drop 90% client-side. Verified
        // 2026-04-26: returns 100% match.
        let resp = self
            .client
            .get(&self.base_url)
            .query(&[
                ("active", "true"),
                ("closed", "false"),
                ("series_slug", self.series_slug.as_str()),
                ("order", "startDate"),
                ("ascending", "false"),
                ("limit", "500"),
            ])
            .send()
            .await
            .map_err(|e| DiscoveryError::Transport(format!("gamma GET: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(DiscoveryError::Transport(format!(
                "gamma HTTP {}: {}",
                status,
                truncate(&body, 256)
            )));
        }

        resp.text()
            .await
            .map_err(|e| DiscoveryError::Parse(format!("gamma body: {e}")))
    }

    /// Parse the envelope, filter by series slug, map each kept event's
    /// single market into a [`Market`]. Pure function — exposed for unit
    /// testing without a network.
    pub fn map_response(&self, raw: &str) -> Result<Vec<Market>, DiscoveryError> {
        let parsed: Vec<GammaEvent> = serde_json::from_str(raw).map_err(|e| {
            DiscoveryError::Parse(format!("gamma JSON envelope: {e}"))
        })?;

        let (mut kept, mut series_rejected, mut map_errors) = (0usize, 0usize, 0usize);
        let mut out = Vec::with_capacity(parsed.len());

        for ev in parsed {
            // Series filter — skip events not in the configured series.
            if !ev
                .series
                .iter()
                .any(|s| s.slug.as_deref() == Some(self.series_slug.as_str()))
            {
                series_rejected += 1;
                continue;
            }

            // Each event in a recurring binary series should have exactly
            // one market. Defensive on the count.
            let raw_market = match ev.markets.first() {
                Some(m) => m,
                None => {
                    map_errors += 1;
                    tracing::warn!(
                        component = "market_registry",
                        venue = "polymarket",
                        event = "market_map_failure",
                        slug = %ev.slug.as_deref().unwrap_or("?"),
                        reason = "event has no markets",
                        "skipping malformed event"
                    );
                    continue;
                }
            };

            match map_event_market(&ev, raw_market) {
                Ok(market) => {
                    kept += 1;
                    out.push(market);
                }
                Err(reason) => {
                    map_errors += 1;
                    tracing::warn!(
                        component = "market_registry",
                        venue = "polymarket",
                        event = "market_map_failure",
                        slug = %ev.slug.as_deref().unwrap_or("?"),
                        reason = %reason,
                        "skipping malformed market"
                    );
                }
            }
        }

        tracing::info!(
            component = "market_registry",
            venue = "polymarket",
            event = "discovery_pass",
            series = %self.series_slug,
            kept = kept,
            series_rejected = series_rejected,
            map_errors = map_errors,
            "gamma discovery pass"
        );
        Ok(out)
    }
}

impl MarketDiscoverer for GammaAdapter {
    fn discover(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<Market>, DiscoveryError>> + Send {
        async move {
            let raw = self.fetch_raw_json().await?;
            self.map_response(&raw)
        }
    }
}

/// One resolved market plus the raw Gamma event JSON it came from. The
/// `market.resolved_outcome` is guaranteed `Some(_)` — that's the filter.
#[derive(Debug, Clone)]
pub struct ResolvedMarket {
    pub market: Market,
    /// Re-serialised JSON of the original Gamma event. Field order /
    /// whitespace are canonical, but keys and values are preserved.
    pub raw_event_json: String,
}

impl GammaAdapter {
    /// Fetch the most-recent closed events in the configured series and
    /// return only the ones that have actually resolved (one
    /// `outcomePrices` entry == `"1"`).
    ///
    /// Sorted newest-first by `endDate`. Used by the recorder's
    /// resolution-sweep loop to capture Up/Down ground-truth labels for
    /// markets whose trading window has ended.
    pub async fn fetch_resolved_events(&self) -> Result<Vec<ResolvedMarket>, DiscoveryError> {
        // Crucial: `series_slug` filter. Without it, closed btc-up-or-down-5m
        // events are paginated past offset 1000 — the first 500 results
        // contain none, so the original sweeper found nothing for 22 h.
        let resp = self
            .client
            .get(&self.base_url)
            .query(&[
                ("closed", "true"),
                ("archived", "false"),
                ("series_slug", self.series_slug.as_str()),
                ("order", "endDate"),
                ("ascending", "false"),
                ("limit", "500"),
            ])
            .send()
            .await
            .map_err(|e| DiscoveryError::Transport(format!("gamma resolved GET: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(DiscoveryError::Transport(format!(
                "gamma resolved HTTP {}: {}",
                status,
                truncate(&body, 256)
            )));
        }

        let raw = resp
            .text()
            .await
            .map_err(|e| DiscoveryError::Parse(format!("gamma resolved body: {e}")))?;

        let parsed: Vec<GammaEvent> = serde_json::from_str(&raw).map_err(|e| {
            DiscoveryError::Parse(format!("gamma resolved JSON envelope: {e}"))
        })?;

        let mut out = Vec::new();
        for ev in parsed {
            // Series filter.
            if !ev
                .series
                .iter()
                .any(|s| s.slug.as_deref() == Some(self.series_slug.as_str()))
            {
                continue;
            }
            let raw_market = match ev.markets.first() {
                Some(m) => m,
                None => continue,
            };
            let market = match map_event_market(&ev, raw_market) {
                Ok(m) => m,
                Err(_) => continue,
            };
            // Only keep events that actually resolved.
            if market.resolved_outcome.is_none() {
                continue;
            }
            let raw_event_json = serde_json::to_string(&ev).unwrap_or_default();
            out.push(ResolvedMarket {
                market,
                raw_event_json,
            });
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Raw API shapes — loose by design. Unknown fields are silently ignored so
// gamma can add fields upstream without breaking us.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Serialize)]
#[allow(dead_code)]
struct GammaEvent {
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    closed: Option<bool>,
    #[serde(default)]
    archived: Option<bool>,
    #[serde(rename = "startDate", default)]
    start_date: Option<String>,
    #[serde(rename = "endDate", default)]
    end_date: Option<String>,
    /// Each event belongs to zero-or-more series (the recurring template).
    /// We filter on this.
    #[serde(default)]
    series: Vec<GammaSeries>,
    /// Each event contains its constituent markets. For the recurring
    /// binary-outcome series we care about (BTC up/down 5m), this is a
    /// single-element array.
    #[serde(default)]
    markets: Vec<GammaMarket>,
}

#[derive(Debug, Deserialize, Serialize)]
#[allow(dead_code)]
struct GammaSeries {
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    title: Option<String>,
    /// `"5m"`, `"1h"`, etc. when set. Not used for filtering today, but
    /// useful for sanity-checking future config changes.
    #[serde(default)]
    recurrence: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[allow(dead_code)]
struct GammaMarket {
    #[serde(rename = "conditionId", default)]
    condition_id: Option<String>,

    #[serde(default)]
    question: Option<String>,

    #[serde(default)]
    slug: Option<String>,

    /// JSON-encoded string: `"[\"tokenIdYes\", \"tokenIdNo\"]"`. Same
    /// order as [`GammaMarket::outcomes`].
    #[serde(rename = "clobTokenIds", default)]
    clob_token_ids: Option<String>,

    /// JSON-encoded string. For BTC up/down: `"[\"Up\", \"Down\"]"`.
    /// For Yes/No markets: `"[\"Yes\", \"No\"]"`. Both are accepted.
    #[serde(default)]
    outcomes: Option<String>,

    /// JSON-encoded string. After resolution, one entry is `"1"` and the
    /// other `"0"`. Before resolution, both reflect current trading prices
    /// (e.g. `"0.62"` / `"0.38"`).
    #[serde(rename = "outcomePrices", default)]
    outcome_prices: Option<String>,

    #[serde(rename = "endDate", default)]
    end_date: Option<String>,
    #[serde(rename = "startDate", default)]
    start_date: Option<String>,
}

/// Build a [`Market`] from an event + its single market record.
///
/// Field precedence: market fields win when present; the event provides
/// fallbacks for things like dates and slug.
fn map_event_market(event: &GammaEvent, m: &GammaMarket) -> Result<Market, String> {
    let id = m
        .condition_id
        .clone()
        .ok_or_else(|| "missing market.conditionId".to_string())?;

    let title = m
        .question
        .clone()
        .or_else(|| event.title.clone())
        .ok_or_else(|| "missing question and event title".to_string())?;

    let slug = m
        .slug
        .clone()
        .or_else(|| event.slug.clone())
        .unwrap_or_default();

    // Tokens.
    let tokens_raw = m
        .clob_token_ids
        .as_ref()
        .ok_or_else(|| "missing clobTokenIds".to_string())?;
    let tokens: Vec<String> = serde_json::from_str(tokens_raw)
        .map_err(|e| format!("clobTokenIds not a JSON array: {e}"))?;
    if tokens.len() != 2 {
        return Err(format!("expected 2 clobTokenIds, got {}", tokens.len()));
    }

    // Outcomes — accept both `[Yes, No]` and `[Up, Down]` (Polymarket's
    // BTC up/down series uses the latter).
    let outcomes_raw = m
        .outcomes
        .as_ref()
        .ok_or_else(|| "missing outcomes".to_string())?;
    let outcomes: Vec<String> = serde_json::from_str(outcomes_raw)
        .map_err(|e| format!("outcomes not a JSON array: {e}"))?;
    if outcomes.len() != 2 {
        return Err(format!("expected 2 outcomes, got {}", outcomes.len()));
    }

    let (yes_idx, no_idx) = match (outcomes[0].as_str(), outcomes[1].as_str()) {
        ("Yes", "No") | ("Up", "Down") => (0usize, 1usize),
        ("No", "Yes") | ("Down", "Up") => (1usize, 0usize),
        (a, b) => return Err(format!("unexpected outcome labels: [{a:?}, {b:?}]")),
    };
    let yes_token = TokenId::new(&tokens[yes_idx]);
    let no_token = TokenId::new(&tokens[no_idx]);

    // End time is required; pull from market then fall back to event.
    let end_str = m
        .end_date
        .as_ref()
        .or(event.end_date.as_ref())
        .ok_or_else(|| "missing endDate".to_string())?;
    let end_time_epoch = parse_iso8601_to_epoch(end_str)
        .ok_or_else(|| format!("invalid endDate: {end_str:?}"))?;

    // Start time is optional.
    let start_time_epoch = m
        .start_date
        .as_ref()
        .or(event.start_date.as_ref())
        .and_then(|s| parse_iso8601_to_epoch(s));

    // Resolution — derived from `outcomePrices`. After settlement one is
    // exactly "1", the other "0". Active markets show fractional prices,
    // which won't match — `resolved_outcome` stays `None` until resolved.
    let resolved_outcome = m.outcome_prices.as_ref().and_then(|raw| {
        let prices: Vec<String> = serde_json::from_str(raw).ok()?;
        if prices.len() != 2 {
            return None;
        }
        if prices[yes_idx].trim() == "1" {
            Some(Outcome::Yes)
        } else if prices[no_idx].trim() == "1" {
            Some(Outcome::No)
        } else {
            None
        }
    });

    Ok(Market {
        id: MarketId::new(id),
        title,
        slug,
        yes_token,
        no_token,
        start_time_epoch,
        end_time_epoch,
        resolved_outcome,
    })
}

// ---------------------------------------------------------------------------
// ISO 8601 UTC -> epoch seconds. Hand-rolled; accepts the formats gamma
// emits in practice:
//   YYYY-MM-DDTHH:MM:SSZ
//   YYYY-MM-DDTHH:MM:SS.fffZ
//   YYYY-MM-DDTHH:MM:SS±00:00  (UTC offsets only)
// ---------------------------------------------------------------------------

fn parse_iso8601_to_epoch(s: &str) -> Option<i64> {
    let body = if let Some(b) = s.strip_suffix('Z') {
        b
    } else if let Some(b) = s.strip_suffix("+00:00") {
        b
    } else if let Some(b) = s.strip_suffix("-00:00") {
        b
    } else {
        return None;
    };
    let body = body.split_once('.').map(|(a, _)| a).unwrap_or(body);

    let (date, time) = body.split_once('T')?;

    let mut dp = date.split('-');
    let y: i64 = dp.next()?.parse().ok()?;
    let mo: u32 = dp.next()?.parse().ok()?;
    let da: u32 = dp.next()?.parse().ok()?;
    if dp.next().is_some() {
        return None;
    }

    let mut tp = time.split(':');
    let h: u32 = tp.next()?.parse().ok()?;
    let mi: u32 = tp.next()?.parse().ok()?;
    let sc: u32 = tp.next()?.parse().ok()?;
    if tp.next().is_some() {
        return None;
    }

    civil_to_epoch(y, mo, da, h, mi, sc)
}

fn civil_to_epoch(y: i64, m: u32, d: u32, h: u32, mi: u32, s: u32) -> Option<i64> {
    if !(1..=12).contains(&m) {
        return None;
    }
    if !(1..=31).contains(&d) {
        return None;
    }
    if h > 23 || mi > 59 || s > 59 {
        return None;
    }

    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y / 400 } else { (y - 399) / 400 };
    let yoe = y - era * 400;
    let mm = m as i64;
    let doy = (153 * (if mm > 2 { mm - 3 } else { mm + 9 }) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86_400 + h as i64 * 3_600 + mi as i64 * 60 + s as i64)
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("{}...", &s[..n])
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> config::MarketDiscoveryConfig {
        config::MarketDiscoveryConfig {
            gamma_url: "https://gamma-api.polymarket.com/events".to_string(),
            poll_interval_secs: 15,
            series_slug: "btc-up-or-down-5m".to_string(),
        }
    }

    #[test]
    fn iso_parse_known_points() {
        assert_eq!(parse_iso8601_to_epoch("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_iso8601_to_epoch("2024-01-01T00:00:00Z"),
            Some(1_704_067_200)
        );
        assert_eq!(
            parse_iso8601_to_epoch("2024-01-01T00:00:00.000Z"),
            Some(1_704_067_200)
        );
        assert_eq!(
            parse_iso8601_to_epoch("2024-01-01T00:00:00+00:00"),
            Some(1_704_067_200)
        );
    }

    #[test]
    fn iso_parse_rejects_non_utc_and_malformed() {
        assert_eq!(parse_iso8601_to_epoch("2024-01-01T00:00:00+05:00"), None);
        assert_eq!(parse_iso8601_to_epoch("2024/01/01 00:00:00"), None);
        assert_eq!(parse_iso8601_to_epoch("not a timestamp"), None);
        assert_eq!(parse_iso8601_to_epoch("2024-13-01T00:00:00Z"), None);
    }

    /// One event in the configured series, currently trading. This shape
    /// matches the live response from
    /// `gamma-api.polymarket.com/events?slug=btc-updown-5m-1776415200`.
    const FIXTURE_BTC_UPDOWN: &str = r#"[
  {
    "id": "384681",
    "slug": "btc-updown-5m-1776415200",
    "title": "Bitcoin Up or Down - April 17, 4:40AM-4:45AM ET",
    "startDate": "2026-04-16T08:48:00.000Z",
    "endDate": "2026-04-17T08:45:00Z",
    "active": true,
    "closed": false,
    "series": [
      {
        "slug": "btc-up-or-down-5m",
        "title": "BTC Up or Down 5m",
        "recurrence": "5m"
      }
    ],
    "markets": [
      {
        "conditionId": "0xb56bbed2f9f79f81d0511b3570d9d21072465b00c7e9b021ae44bb73cf1c06c9",
        "question": "Bitcoin Up or Down - April 17, 4:40AM-4:45AM ET",
        "slug": "btc-updown-5m-1776415200",
        "clobTokenIds": "[\"111\", \"222\"]",
        "outcomes": "[\"Up\", \"Down\"]",
        "outcomePrices": "[\"0.62\", \"0.38\"]",
        "startDate": "2026-04-16T08:48:16.000Z",
        "endDate": "2026-04-17T08:45:00Z"
      }
    ]
  }
]"#;

    #[test]
    fn maps_btc_updown_event_to_market() {
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        let out = adapter.map_response(FIXTURE_BTC_UPDOWN).unwrap();
        assert_eq!(out.len(), 1);
        let m = &out[0];
        assert_eq!(
            m.id.as_str(),
            "0xb56bbed2f9f79f81d0511b3570d9d21072465b00c7e9b021ae44bb73cf1c06c9"
        );
        assert!(m.title.starts_with("Bitcoin Up or Down"));
        assert_eq!(m.slug, "btc-updown-5m-1776415200");
        // "Up" comes first in `outcomes`, so token "111" is the Up/Yes side.
        assert_eq!(m.yes_token.as_str(), "111");
        assert_eq!(m.no_token.as_str(), "222");
        assert!(m.start_time_epoch.is_some());
        assert!(m.resolved_outcome.is_none(), "fractional prices = unresolved");
    }

    #[test]
    fn reversed_outcome_order_swaps_tokens() {
        let raw = FIXTURE_BTC_UPDOWN.replace(
            r#""outcomes": "[\"Up\", \"Down\"]""#,
            r#""outcomes": "[\"Down\", \"Up\"]""#,
        );
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        let out = adapter.map_response(&raw).unwrap();
        let m = &out[0];
        // Now token "222" is the Up/Yes side because Up is at index 1.
        assert_eq!(m.yes_token.as_str(), "222");
        assert_eq!(m.no_token.as_str(), "111");
    }

    #[test]
    fn yes_no_outcomes_still_work() {
        // Other Polymarket markets use Yes/No labels — ensure backward-compat.
        let raw = FIXTURE_BTC_UPDOWN.replace(
            r#""outcomes": "[\"Up\", \"Down\"]""#,
            r#""outcomes": "[\"Yes\", \"No\"]""#,
        );
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        let out = adapter.map_response(&raw).unwrap();
        assert_eq!(out[0].yes_token.as_str(), "111");
        assert_eq!(out[0].no_token.as_str(), "222");
    }

    #[test]
    fn wrong_series_filtered_out() {
        let raw = FIXTURE_BTC_UPDOWN
            .replace("\"slug\": \"btc-up-or-down-5m\"", "\"slug\": \"some-other-series\"");
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        let out = adapter.map_response(&raw).unwrap();
        assert!(out.is_empty(), "events outside the series should be skipped");
    }

    #[test]
    fn event_with_no_markets_skipped() {
        let raw = FIXTURE_BTC_UPDOWN.replace(
            "\"markets\": [",
            "\"markets\": [],\"_dropped\": [",
        );
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        let out = adapter.map_response(&raw).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn missing_condition_id_skips_market() {
        let bad = FIXTURE_BTC_UPDOWN.replace(
            r#""conditionId": "0xb56bbed2f9f79f81d0511b3570d9d21072465b00c7e9b021ae44bb73cf1c06c9","#,
            "",
        );
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        let out = adapter.map_response(&bad).unwrap();
        assert!(out.is_empty(), "should skip when conditionId missing");
    }

    #[test]
    fn bad_json_envelope_fails_loudly() {
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        assert!(matches!(
            adapter.map_response("not json").unwrap_err(),
            DiscoveryError::Parse(_)
        ));
    }

    #[test]
    fn malformed_end_date_skips_market() {
        let raw = FIXTURE_BTC_UPDOWN.replace("2026-04-17T08:45:00Z", "garbage");
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        let out = adapter.map_response(&raw).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn outcome_prices_one_zero_marks_resolved_yes() {
        // Settled with "Up" winning.
        let raw = FIXTURE_BTC_UPDOWN.replace(
            r#""outcomePrices": "[\"0.62\", \"0.38\"]""#,
            r#""outcomePrices": "[\"1\", \"0\"]""#,
        );
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        let out = adapter.map_response(&raw).unwrap();
        assert_eq!(out[0].resolved_outcome, Some(Outcome::Yes));
    }

    #[test]
    fn outcome_prices_zero_one_marks_resolved_no() {
        // Settled with "Down" winning.
        let raw = FIXTURE_BTC_UPDOWN.replace(
            r#""outcomePrices": "[\"0.62\", \"0.38\"]""#,
            r#""outcomePrices": "[\"0\", \"1\"]""#,
        );
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        let out = adapter.map_response(&raw).unwrap();
        assert_eq!(out[0].resolved_outcome, Some(Outcome::No));
    }
}
