//! Resolution sweeper.
//!
//! Polls Gamma for recently-closed markets in the configured series and
//! appends one [`ResolutionRecord`] per never-before-seen market into a
//! single session-level sidecar file:
//!
//!     <session_dir>/_resolutions.ndjson
//!
//! ## Why a sidecar, not per-slug `<slug>-resolved.ndjson`
//!
//! The previous implementation opened one file per resolved market via
//! [`storage::Store`], which under disk-pressure conditions left thousands
//! of 0-byte files (the integrity scan on session_20260427T100216Z found
//! 1,526 of them). That happened because `OpenOptions::create(true)`
//! creates the file *before* the line write — if the write then fails
//! with ENOSPC, the empty file stays. Per-slug semantics also produced an
//! unnecessary file-per-market explosion.
//!
//! The sidecar form has one open fd, one append target, and is trivially
//! validated. Same convention as `_health.ndjson`, `_latency_probes.ndjson`,
//! and `_session_meta.json`.
//!
//! ## Direction labelling
//!
//! `up_token` is `clobTokenIds[0]` from the gamma response — set at
//! market *creation* time. The `winner_label` comes from gamma's
//! `outcomes[i]` where `outcomePrices[i] == "1"` — that's gamma's
//! authoritative resolution. We **never** infer the winner from the
//! terminal market price.

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;

use common::{
    ResolutionMarket, ResolutionOutcome, ResolutionRecord, ResolutionTokens,
};
use market_registry::{GammaAdapter, MarketId};

const RESOLUTIONS_FILENAME: &str = "_resolutions.ndjson";
const SCHEMA_VERSION: u32 = 1;
const SOURCE_LABEL: &str = "gamma_v1";

/// Run the sweeper forever. `session_dir` is where `_resolutions.ndjson`
/// will be appended; usually `Store::session_dir()`.
pub async fn run_resolution_sweep_loop(
    adapter: GammaAdapter,
    session_dir: PathBuf,
    interval: Duration,
) {
    let path = session_dir.join(RESOLUTIONS_FILENAME);
    tracing::info!(
        component = "recorder",
        event = "resolution_sweep_started",
        path = %path.display(),
        interval_secs = interval.as_secs(),
        "resolution sweeper writing to single session sidecar"
    );

    let mut seen: HashSet<MarketId> = HashSet::new();

    loop {
        match adapter.fetch_resolved_events().await {
            Ok(events) => {
                let mut new_count = 0usize;
                let mut record_failures = 0usize;
                let mut write_failures = 0usize;
                for re in events {
                    if seen.contains(&re.market.id) {
                        continue;
                    }
                    let record = match build_resolution_record(&re) {
                        Ok(r) => r,
                        Err(reason) => {
                            record_failures += 1;
                            tracing::warn!(
                                component = "recorder",
                                event = "resolution_record_build_failed",
                                market = %re.market.id,
                                reason = %reason,
                                "skipping malformed resolution payload"
                            );
                            continue;
                        }
                    };
                    match append_resolution(&path, &record) {
                        Ok(()) => {
                            seen.insert(re.market.id.clone());
                            new_count += 1;
                        }
                        Err(e) => {
                            write_failures += 1;
                            tracing::error!(
                                component = "recorder",
                                event = "resolution_write_failed",
                                market = %re.market.id,
                                error = %e,
                                "failed to append resolution"
                            );
                        }
                    }
                }
                tracing::info!(
                    component = "recorder",
                    event = "resolution_sweep",
                    new = new_count,
                    seen_total = seen.len(),
                    record_failures = record_failures,
                    write_failures = write_failures,
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

/// Append one record as a single NDJSON line. Atomic at the line level
/// (one `write_all`); a partial write would leave a malformed trailing
/// line, which the validate-resolutions tool will surface.
pub(crate) fn append_resolution(
    path: &Path,
    record: &ResolutionRecord,
) -> std::io::Result<()> {
    let line = serde_json::to_string(record)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let mut buf = Vec::with_capacity(line.len() + 1);
    buf.extend_from_slice(line.as_bytes());
    buf.push(b'\n');
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    f.write_all(&buf)?;
    Ok(())
}

/// Build a [`ResolutionRecord`] from gamma's [`market_registry::gamma::ResolvedMarket`].
/// Parses the raw gamma event JSON to extract winner_label / outcome_labels /
/// outcome_prices / clobTokenIds — the values gamma considers authoritative.
fn build_resolution_record(
    re: &market_registry::gamma::ResolvedMarket,
) -> Result<ResolutionRecord, String> {
    let parsed: GammaEventLite = serde_json::from_str(&re.raw_event_json)
        .map_err(|e| format!("parse raw_event_json: {e}"))?;
    let raw_market = parsed
        .markets
        .first()
        .ok_or_else(|| "no markets in gamma event".to_string())?;

    let condition_id = raw_market
        .condition_id
        .as_deref()
        .ok_or_else(|| "missing conditionId".to_string())?
        .to_string();

    let outcome_labels: Vec<String> = raw_market
        .outcomes
        .as_deref()
        .map(parse_string_array_field)
        .transpose()?
        .ok_or_else(|| "missing outcomes".to_string())?;
    let outcome_prices: Vec<String> = raw_market
        .outcome_prices
        .as_deref()
        .map(parse_string_array_field)
        .transpose()?
        .ok_or_else(|| "missing outcomePrices".to_string())?;
    let clob_token_ids: Vec<String> = raw_market
        .clob_token_ids
        .as_deref()
        .map(parse_string_array_field)
        .transpose()?
        .ok_or_else(|| "missing clobTokenIds".to_string())?;

    if outcome_labels.len() != 2
        || outcome_prices.len() != 2
        || clob_token_ids.len() != 2
    {
        return Err(format!(
            "expected 2-outcome binary market, got labels={} prices={} tokens={}",
            outcome_labels.len(),
            outcome_prices.len(),
            clob_token_ids.len()
        ));
    }

    let winner_label = if outcome_prices[0] == "1" {
        outcome_labels[0].clone()
    } else if outcome_prices[1] == "1" {
        outcome_labels[1].clone()
    } else {
        return Err(format!(
            "no decisive winner: outcome_prices={outcome_prices:?}"
        ));
    };

    let slug = raw_market
        .slug
        .clone()
        .or_else(|| parsed.slug.clone())
        .unwrap_or_default();

    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);

    Ok(ResolutionRecord {
        schema_version: SCHEMA_VERSION,
        ts_ns: now_ns.to_string(),
        source: SOURCE_LABEL.to_string(),
        market: ResolutionMarket {
            slug,
            condition_id,
            question: raw_market.question.clone(),
            start_date: raw_market
                .start_date
                .clone()
                .or_else(|| parsed.start_date.clone()),
            end_date: raw_market
                .end_date
                .clone()
                .or_else(|| parsed.end_date.clone()),
            start_time_epoch: re.market.start_time_epoch,
            end_time_epoch: re.market.end_time_epoch,
        },
        tokens: ResolutionTokens {
            up_token: clob_token_ids[0].clone(),
            down_token: clob_token_ids[1].clone(),
        },
        outcome: ResolutionOutcome {
            winner_label,
            outcome_labels,
            outcome_prices,
        },
        raw_gamma_event: re.raw_event_json.clone(),
    })
}

fn parse_string_array_field(raw: &str) -> Result<Vec<String>, String> {
    serde_json::from_str::<Vec<String>>(raw)
        .map_err(|e| format!("parse string-array field {raw:?}: {e}"))
}

// Lightweight gamma-event shape for the fields we need. Mirrors
// `crates/market_registry/src/gamma.rs`'s GammaEvent but is private to
// the recorder so we don't have to make those types public.
#[derive(Deserialize)]
struct GammaEventLite {
    #[serde(default)]
    slug: Option<String>,
    #[serde(rename = "startDate", default)]
    start_date: Option<String>,
    #[serde(rename = "endDate", default)]
    end_date: Option<String>,
    #[serde(default)]
    markets: Vec<GammaMarketLite>,
}

#[derive(Deserialize)]
struct GammaMarketLite {
    #[serde(rename = "conditionId", default)]
    condition_id: Option<String>,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    question: Option<String>,
    #[serde(rename = "startDate", default)]
    start_date: Option<String>,
    #[serde(rename = "endDate", default)]
    end_date: Option<String>,
    #[serde(rename = "clobTokenIds", default)]
    clob_token_ids: Option<String>,
    #[serde(default)]
    outcomes: Option<String>,
    #[serde(rename = "outcomePrices", default)]
    outcome_prices: Option<String>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestDir(PathBuf);
    impl TestDir {
        fn new() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let ptr = &nanos as *const _ as usize;
            let dir = std::env::temp_dir().join(format!("polybot_sweep_test_{nanos}_{ptr:x}"));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn sample_resolved_market(slug: &str, prices: &str) -> market_registry::gamma::ResolvedMarket {
        // Build a minimal gamma event JSON containing exactly what
        // build_resolution_record needs.
        let raw_event_json = format!(
            r#"{{
              "slug": "{slug}",
              "startDate": "2026-04-26T12:25:00Z",
              "endDate": "2026-04-26T12:30:00Z",
              "markets": [{{
                "conditionId": "0xabc",
                "slug": "{slug}",
                "question": "Bitcoin Up or Down - April 26, 12:25PM-12:30PM ET",
                "startDate": "2026-04-26T12:25:00Z",
                "endDate": "2026-04-26T12:30:00Z",
                "clobTokenIds": "[\"10161\", \"11061\"]",
                "outcomes": "[\"Up\", \"Down\"]",
                "outcomePrices": "{prices}"
              }}]
            }}"#
        );
        market_registry::gamma::ResolvedMarket {
            market: market_registry::Market {
                id: MarketId::new("0xabc"),
                title: "Bitcoin Up or Down — 12:30 PM ET".into(),
                slug: slug.into(),
                yes_token: market_registry::TokenId::new("10161"),
                no_token: market_registry::TokenId::new("11061"),
                start_time_epoch: Some(1_777_205_700),
                end_time_epoch: 1_777_206_000,
                resolved_outcome: Some(market_registry::Outcome::Yes),
            },
            raw_event_json,
        }
    }

    #[test]
    fn build_record_parses_up_winner() {
        let re = sample_resolved_market("btc-updown-5m-1777206000", "[\\\"1\\\",\\\"0\\\"]");
        let r = build_resolution_record(&re).unwrap();
        assert_eq!(r.schema_version, SCHEMA_VERSION);
        assert_eq!(r.source, "gamma_v1");
        assert_eq!(r.market.slug, "btc-updown-5m-1777206000");
        assert_eq!(r.market.condition_id, "0xabc");
        assert_eq!(r.market.end_time_epoch, 1_777_206_000);
        assert_eq!(r.tokens.up_token, "10161");
        assert_eq!(r.tokens.down_token, "11061");
        assert_eq!(r.outcome.winner_label, "Up");
        assert_eq!(r.outcome.outcome_labels, vec!["Up", "Down"]);
        assert_eq!(r.outcome.outcome_prices, vec!["1", "0"]);
    }

    #[test]
    fn build_record_parses_down_winner() {
        let re = sample_resolved_market("btc-updown-5m-1777206000", "[\\\"0\\\",\\\"1\\\"]");
        let r = build_resolution_record(&re).unwrap();
        assert_eq!(r.outcome.winner_label, "Down");
    }

    #[test]
    fn build_record_rejects_indecisive_outcome_prices() {
        // A still-trading market would have prices like "0.62"/"0.38".
        // Sweeper should reject those, not accept ambiguous winner.
        let re = sample_resolved_market("btc-updown-5m-1777206000", "[\\\"0.62\\\",\\\"0.38\\\"]");
        let err = build_resolution_record(&re).unwrap_err();
        assert!(err.contains("no decisive winner"), "got {err}");
    }

    #[test]
    fn append_resolution_creates_and_appends_lines() {
        let tmp = TestDir::new();
        let path = tmp.path().join(RESOLUTIONS_FILENAME);
        let r1 = build_resolution_record(&sample_resolved_market(
            "btc-updown-5m-1",
            "[\\\"1\\\",\\\"0\\\"]",
        ))
        .unwrap();
        let r2 = build_resolution_record(&sample_resolved_market(
            "btc-updown-5m-2",
            "[\\\"0\\\",\\\"1\\\"]",
        ))
        .unwrap();

        append_resolution(&path, &r1).unwrap();
        append_resolution(&path, &r2).unwrap();

        let body = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2);

        let parsed1: ResolutionRecord = serde_json::from_str(lines[0]).unwrap();
        let parsed2: ResolutionRecord = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(parsed1.market.slug, "btc-updown-5m-1");
        assert_eq!(parsed1.outcome.winner_label, "Up");
        assert_eq!(parsed2.market.slug, "btc-updown-5m-2");
        assert_eq!(parsed2.outcome.winner_label, "Down");
    }

    #[test]
    fn append_resolution_emits_one_line_per_record_no_blank_lines() {
        let tmp = TestDir::new();
        let path = tmp.path().join(RESOLUTIONS_FILENAME);
        let r = build_resolution_record(&sample_resolved_market(
            "btc-updown-5m-x",
            "[\\\"1\\\",\\\"0\\\"]",
        ))
        .unwrap();
        for _ in 0..3 {
            append_resolution(&path, &r).unwrap();
        }
        let body = fs::read_to_string(&path).unwrap();
        let line_count = body.lines().count();
        assert_eq!(line_count, 3);
        assert!(body.ends_with('\n'));
        assert!(!body.contains("\n\n"));
    }
}

