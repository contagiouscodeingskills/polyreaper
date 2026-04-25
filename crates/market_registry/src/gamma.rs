//! Polymarket Gamma API adapter.
//!
//! Fetches live market metadata from `gamma-api.polymarket.com/markets`,
//! filters to BTC 5-minute up/down markets via `title_pattern`, and maps
//! the raw JSON into [`Market`] domain objects.
//!
//! # Uncertainty policy
//!
//! Every assumed gamma field name is either on [`GammaMarket`] with a
//! `#[serde(rename)]` or kept as `Option<_>` so missing fields don't blow
//! up deserialisation of the whole response.
//!
//! Per-market mapping failures (missing required fields, malformed
//! timestamps, unexpected outcome labels) **log and skip**; they do not
//! fail the whole discovery pass. A malformed response *envelope* (not
//! valid JSON or not an array) fails loudly with [`DiscoveryError::Parse`].
//!
//! The uncertain field mappings, in one place:
//!
//! | Domain field         | Gamma field             | Notes                                   |
//! |----------------------|-------------------------|-----------------------------------------|
//! | `MarketId`           | `conditionId` \| `id`   | hex for condition, stringified fallback |
//! | `title`              | `question`              | human-readable, used by title filter    |
//! | `slug`               | `slug`                  | URL slug                                |
//! | yes/no `TokenId`     | `clobTokenIds`          | JSON-encoded string: `"[\"y\",\"n\"]"`  |
//! | outcome labels       | `outcomes`              | JSON-encoded string: `"[\"Yes\",\"No\"]"` — same order as tokens |
//! | `start_time_epoch`   | `startDate`             | ISO-8601 UTC, optional                  |
//! | `end_time_epoch`     | `endDate`               | ISO-8601 UTC, required                  |
//! | `resolved_outcome`   | `winningToken`          | token id match selects Yes/No           |

use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;

use crate::{DiscoveryError, Market, MarketDiscoverer, MarketId, Outcome, TokenId};

/// Live Polymarket Gamma discoverer.
pub struct GammaAdapter {
    client: Client,
    base_url: String,
    title_pattern: regex::Regex,
}

impl GammaAdapter {
    /// Build an adapter from recorder config. Fails if the title regex
    /// doesn't compile or the HTTP client can't be built.
    pub fn new(cfg: &config::MarketDiscoveryConfig) -> Result<Self, DiscoveryError> {
        let title_pattern = regex::Regex::new(&cfg.title_pattern)
            .map_err(|e| DiscoveryError::Adapter(format!("title_pattern regex: {e}")))?;
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .user_agent(concat!("polybot-recorder/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| DiscoveryError::Adapter(format!("reqwest client: {e}")))?;
        Ok(Self {
            client,
            base_url: cfg.gamma_url.clone(),
            title_pattern,
        })
    }

    /// Issue the HTTP GET and return the raw body on 2xx, or an error.
    async fn fetch_raw_json(&self) -> Result<String, DiscoveryError> {
        // Single page with limit=500. For BTC 5-min scope at most a handful
        // of markets are active at any time; pagination can land later if
        // we ever expand scope.
        let resp = self
            .client
            .get(&self.base_url)
            .query(&[
                ("active", "true"),
                ("closed", "false"),
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

    /// Parse the envelope, filter by title regex, map to [`Market`]s,
    /// emit a single summary log line. Pure function — exposed for unit
    /// testing with fixture JSON (no network).
    pub fn map_response(&self, raw: &str) -> Result<Vec<Market>, DiscoveryError> {
        let parsed: Vec<GammaMarket> = serde_json::from_str(raw).map_err(|e| {
            DiscoveryError::Parse(format!("gamma JSON envelope: {e}"))
        })?;

        let (mut kept, mut title_rejected, mut map_errors) = (0usize, 0usize, 0usize);
        let mut out = Vec::with_capacity(parsed.len());

        for m in parsed {
            if !self.title_pattern.is_match(&m.question) {
                title_rejected += 1;
                continue;
            }
            match map_to_market(&m) {
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
                        question = %m.question,
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
            kept = kept,
            title_rejected = title_rejected,
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

// ---------------------------------------------------------------------------
// Raw API shape — loose by design. Unknown fields are silently ignored so
// gamma adding fields upstream doesn't break us.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct GammaMarket {
    /// Primary identifier. `conditionId` for v2 / on-chain markets;
    /// `id` (numeric or string) as fallback.
    #[serde(rename = "conditionId", default)]
    condition_id: Option<String>,
    #[serde(default)]
    id: Option<serde_json::Value>,

    /// Human-readable market question (used for title regex filtering).
    question: String,

    #[serde(default)]
    slug: Option<String>,

    /// JSON-encoded string: `"[\"tokenIdYes\", \"tokenIdNo\"]"`. Same
    /// order as [`GammaMarket::outcomes`].
    #[serde(rename = "clobTokenIds", default)]
    clob_token_ids: Option<String>,

    /// JSON-encoded string: `"[\"Yes\", \"No\"]"`.
    #[serde(default)]
    outcomes: Option<String>,

    #[serde(rename = "startDate", default)]
    start_date: Option<String>,
    #[serde(rename = "endDate", default)]
    end_date: Option<String>,

    #[serde(default)]
    active: Option<bool>,
    #[serde(default)]
    closed: Option<bool>,

    /// Token id that won at resolution, if the market has resolved.
    #[serde(rename = "winningToken", default)]
    winning_token: Option<String>,
}

fn map_to_market(m: &GammaMarket) -> Result<Market, String> {
    // Primary id: prefer conditionId, fall back to `id` as stringified value.
    let id = m
        .condition_id
        .clone()
        .or_else(|| {
            m.id.as_ref().map(|v| match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            })
        })
        .ok_or_else(|| "missing conditionId / id".to_string())?;

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

    // Outcomes (Yes/No labels).
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
        ("Yes", "No") => (0usize, 1usize),
        ("No", "Yes") => (1usize, 0usize),
        (a, b) => return Err(format!("unexpected outcome labels: [{a:?}, {b:?}]")),
    };
    let yes_token = TokenId::new(&tokens[yes_idx]);
    let no_token = TokenId::new(&tokens[no_idx]);

    // End time is required.
    let end_str = m
        .end_date
        .as_ref()
        .ok_or_else(|| "missing endDate".to_string())?;
    let end_time_epoch = parse_iso8601_to_epoch(end_str)
        .ok_or_else(|| format!("invalid endDate: {end_str:?}"))?;

    // Start time is optional.
    let start_time_epoch = m.start_date.as_ref().and_then(|s| parse_iso8601_to_epoch(s));

    // Resolution: if `winningToken` matches yes/no token id, that side won.
    let resolved_outcome = m.winning_token.as_ref().and_then(|tok| {
        if tok == &tokens[yes_idx] {
            Some(Outcome::Yes)
        } else if tok == &tokens[no_idx] {
            Some(Outcome::No)
        } else {
            None
        }
    });

    Ok(Market {
        id: MarketId::new(id),
        title: m.question.clone(),
        slug: m.slug.clone().unwrap_or_default(),
        yes_token,
        no_token,
        start_time_epoch,
        end_time_epoch,
        resolved_outcome,
    })
}

// ---------------------------------------------------------------------------
// ISO 8601 UTC -> epoch seconds. Hand-rolled so we don't pull chrono/time
// for one function. Accepts gamma's emitted forms:
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
    // Strip fractional seconds.
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

/// Howard Hinnant's days_from_civil (forward direction).
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
            gamma_url: "https://gamma-api.polymarket.com/markets".to_string(),
            poll_interval_secs: 15,
            title_pattern: r"(?i)bitcoin.*(up or down).*5.*(min|minute)".to_string(),
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
        assert_eq!(
            parse_iso8601_to_epoch("2024-01-01T00:00:00-00:00"),
            Some(1_704_067_200)
        );
    }

    #[test]
    fn iso_parse_rejects_non_utc_and_malformed() {
        assert_eq!(parse_iso8601_to_epoch("2024-01-01T00:00:00+05:00"), None);
        assert_eq!(parse_iso8601_to_epoch("2024/01/01 00:00:00"), None);
        assert_eq!(parse_iso8601_to_epoch("not a timestamp"), None);
        // Month 13.
        assert_eq!(parse_iso8601_to_epoch("2024-13-01T00:00:00Z"), None);
    }

    const FIXTURE_HAPPY: &str = r#"[
  {
    "conditionId": "0xabc123",
    "question": "Bitcoin Up or Down - 5 minutes",
    "slug": "btc-up-or-down-5min-2026-04-23-00-00",
    "clobTokenIds": "[\"111\", \"222\"]",
    "outcomes": "[\"Yes\", \"No\"]",
    "startDate": "2026-04-23T00:00:00.000Z",
    "endDate": "2026-04-23T00:05:00.000Z",
    "active": true,
    "closed": false
  }
]"#;

    #[test]
    fn maps_happy_fixture() {
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        let out = adapter.map_response(FIXTURE_HAPPY).unwrap();
        assert_eq!(out.len(), 1);
        let m = &out[0];
        assert_eq!(m.id.as_str(), "0xabc123");
        assert_eq!(m.title, "Bitcoin Up or Down - 5 minutes");
        assert_eq!(m.slug, "btc-up-or-down-5min-2026-04-23-00-00");
        assert_eq!(m.yes_token.as_str(), "111");
        assert_eq!(m.no_token.as_str(), "222");
        assert_eq!(m.end_time_epoch - m.start_time_epoch.unwrap(), 5 * 60);
        assert!(m.resolved_outcome.is_none());
    }

    #[test]
    fn reversed_outcome_labels_swap_tokens() {
        let raw = FIXTURE_HAPPY.replace(
            r#""outcomes": "[\"Yes\", \"No\"]""#,
            r#""outcomes": "[\"No\", \"Yes\"]""#,
        );
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        let out = adapter.map_response(&raw).unwrap();
        let m = &out[0];
        // Tokens are still in original order; outcomes tell us which is which.
        assert_eq!(m.yes_token.as_str(), "222");
        assert_eq!(m.no_token.as_str(), "111");
    }

    #[test]
    fn unrelated_question_is_title_filtered() {
        let raw = FIXTURE_HAPPY.replace(
            "Bitcoin Up or Down - 5 minutes",
            "Will ETH hit 5000 by year end?",
        );
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        let out = adapter.map_response(&raw).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn missing_required_field_skips_market() {
        // Remove conditionId; fixture has no `id` either, so mapping fails.
        let bad = FIXTURE_HAPPY.replace(r#""conditionId": "0xabc123","#, "");
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        let out = adapter.map_response(&bad).unwrap();
        assert!(out.is_empty(), "mapping should have skipped the market");
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
        let raw = FIXTURE_HAPPY.replace("2026-04-23T00:05:00.000Z", "garbage");
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        let out = adapter.map_response(&raw).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn winning_token_marks_resolved_yes() {
        let raw = FIXTURE_HAPPY.replace(
            r#""closed": false"#,
            r#""closed": true, "winningToken": "111""#,
        );
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        let out = adapter.map_response(&raw).unwrap();
        assert_eq!(out[0].resolved_outcome, Some(Outcome::Yes));
    }

    #[test]
    fn winning_token_marks_resolved_no() {
        let raw = FIXTURE_HAPPY.replace(
            r#""closed": false"#,
            r#""closed": true, "winningToken": "222""#,
        );
        let adapter = GammaAdapter::new(&cfg()).unwrap();
        let out = adapter.map_response(&raw).unwrap();
        assert_eq!(out[0].resolved_outcome, Some(Outcome::No));
    }
}
