//! Durable per-decision NDJSON log + session-metadata sidecar.
//!
//! Every time the bot evaluates the active market (one `try_fire` call,
//! i.e. one Polymarket book tick), we write one `DecisionRecord`
//! capturing the full state of the world at that moment: market
//! identity, Binance BTC reference, Binance-snapped strike, full
//! Polymarket YES+NO bid/ask, Polymarket-implied strike, fair value,
//! edge, the strategy decision, the risk decision, freshness flags,
//! and provenance.
//!
//! Records are flushed per-write — the rate is low (≈2/s) and
//! crash-safety matters more than throughput for paper-mode audit data.
//!
//! Schema is `schema_version = 1`; bump on breaking changes.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::config::BotConfig;
use crate::risk::RejectReason;
use crate::strategy::NoSignalReason;

pub const SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Record
// ---------------------------------------------------------------------------

/// One row in `decisions.ndjson`. Optionals capture the "missing data"
/// states explicitly — e.g. `binance_strike_usd = null` means we couldn't
/// snap a strike from history, distinct from `binance_strike_usd = 78089.79`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionRecord {
    pub schema_version: u32,
    /// Local wall-clock ns at decision time. Stringified for JSON
    /// precision (matches `common::RawEvent`/recorder convention).
    pub local_ts_ns: String,

    // Provenance
    pub session_id: String,
    pub bot_version: String,

    // Market identity
    pub market_id: String,
    pub market_slug: String,
    pub yes_token: String,
    pub no_token: String,
    pub end_epoch: i64,
    pub effective_start_epoch: i64,
    pub ttr_secs: f64,

    // BTC reference (Binance)
    pub binance_btc_mid_usd: Option<f64>,
    pub binance_strike_usd: Option<f64>,
    pub btc_last_update_age_ms: Option<f64>,
    pub btc_history_len: usize,

    // Polymarket book — full TOB for both sides
    pub poly_yes_bid: Option<f64>,
    pub poly_yes_ask: Option<f64>,
    pub poly_yes_mid: Option<f64>,
    pub poly_no_bid: Option<f64>,
    pub poly_no_ask: Option<f64>,
    pub poly_no_mid: Option<f64>,
    pub poly_book_age_ms: Option<f64>,

    // Cross-venue strike diagnostics (the open question — see memory
    // entry "Cross-venue strike open question").
    pub implied_strike_usd: Option<f64>,
    pub strike_gap_usd: Option<f64>,
    pub strike_gap_bps: Option<f64>,

    // Strategy state
    pub sigma_per_sec_used: Option<f64>,
    pub sigma_source: Option<String>, // "estimated" | "fallback"
    pub vol_samples: usize,
    pub fv_yes: Option<f64>,
    pub fv_no: Option<f64>,
    pub edge_yes: Option<f64>, // FV_yes - poly_yes_mid

    // Decision
    pub decision_kind: DecisionKind,
    pub decision_side: Option<String>, // "yes" / "no"
    pub decision_size_usd: Option<f64>,
    pub decision_price: Option<f64>,
    pub no_signal_reason: Option<NoSignalReason>,
    pub reject_reason: Option<String>,
    pub incomplete_reason: Option<IncompleteReason>,
}

/// What happened to this evaluation tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionKind {
    /// A signal was placed (paper).
    Fire,
    /// Strategy chose not to fire (see `no_signal_reason`).
    NoSignal,
    /// Strategy fired but risk vetoed (see `reject_reason`).
    Rejected,
    /// Couldn't even evaluate — missing strike, missing BTC, etc.
    Incomplete,
}

/// Why we couldn't run the strategy at all (state before `decide()`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncompleteReason {
    NoStrike,
    NoBtcMid,
    NoPolyYesMid,
    TtrNonPositive,
}

// ---------------------------------------------------------------------------
// Logger
// ---------------------------------------------------------------------------

pub struct DecisionLogger {
    writer: BufWriter<File>,
    path: PathBuf,
}

impl DecisionLogger {
    /// Open (or create) the decision log at `path`. Parent directories
    /// are created as needed.
    pub fn open(path: PathBuf) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Write one record. Flushes per call — paper audit data should
    /// survive a crash; volume is low.
    pub fn write(&mut self, rec: &DecisionRecord) -> std::io::Result<()> {
        let line = serde_json::to_string(rec)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        self.writer.write_all(line.as_bytes())?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()
    }
}

// ---------------------------------------------------------------------------
// Session metadata
// ---------------------------------------------------------------------------

/// Single-file sidecar written once at bot startup. Captures the full
/// config + version so a future replay of `decisions.ndjson` can be
/// pinned to a specific run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMeta {
    pub schema_version: u32,
    pub session_id: String,
    pub bot_version: String,
    pub started_at_epoch_ns: String,
    pub started_at_iso: String,
    pub config: BotConfig,
}

/// Build a session id of the form `bot_session_<YYYYMMDDTHHMMSSZ>` from
/// the current wall clock. Compact-ISO so the directory sorts by time.
pub fn make_session_id(now_secs: i64) -> String {
    format!("bot_session_{}", epoch_secs_to_compact_iso(now_secs))
}

/// Hinnant civil-from-days, packed into a `YYYYMMDDTHHMMSSZ` string.
pub fn epoch_secs_to_compact_iso(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let h = (sod / 3_600) as u32;
    let mi = ((sod % 3_600) / 60) as u32;
    let sc = (sod % 60) as u32;
    let z = days + 719_468;
    let era = if z >= 0 { z / 146_097 } else { (z - 146_096) / 146_097 };
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        y, m, d, h, mi, sc
    )
}

pub fn write_session_meta(path: &Path, meta: &SessionMeta) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(meta)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    std::fs::write(path, json)
}

pub const BOT_VERSION: &str = env!("CARGO_PKG_VERSION");

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_iso_known_points() {
        // 1970-01-01T00:00:00Z
        assert_eq!(epoch_secs_to_compact_iso(0), "19700101T000000Z");
        // 2024-01-01T00:00:00Z
        assert_eq!(epoch_secs_to_compact_iso(1_704_067_200), "20240101T000000Z");
        // 2024-12-31T23:59:59Z
        assert_eq!(epoch_secs_to_compact_iso(1_735_689_599), "20241231T235959Z");
    }

    #[test]
    fn session_id_uses_compact_iso() {
        let id = make_session_id(1_704_067_200);
        assert_eq!(id, "bot_session_20240101T000000Z");
    }

    #[test]
    fn round_trip_record_through_serde_json() {
        let r = DecisionRecord {
            schema_version: SCHEMA_VERSION,
            local_ts_ns: "1779008700123456789".into(),
            session_id: "bot_session_20260517T090000Z".into(),
            bot_version: BOT_VERSION.into(),
            market_id: "0xabc".into(),
            market_slug: "btc-updown-5m-1779008700".into(),
            yes_token: "y".into(),
            no_token: "n".into(),
            end_epoch: 1_779_009_000,
            effective_start_epoch: 1_779_008_700,
            ttr_secs: 280.0,
            binance_btc_mid_usd: Some(78_089.79),
            binance_strike_usd: Some(78_089.79),
            btc_last_update_age_ms: Some(100.0),
            btc_history_len: 1000,
            poly_yes_bid: Some(0.33),
            poly_yes_ask: Some(0.35),
            poly_yes_mid: Some(0.34),
            poly_no_bid: Some(0.65),
            poly_no_ask: Some(0.67),
            poly_no_mid: Some(0.66),
            poly_book_age_ms: Some(50.0),
            implied_strike_usd: Some(78_117.0),
            strike_gap_usd: Some(27.21),
            strike_gap_bps: Some(3.48),
            sigma_per_sec_used: Some(5.0e-5),
            sigma_source: Some("fallback".into()),
            vol_samples: 500,
            fv_yes: Some(0.50),
            fv_no: Some(0.50),
            edge_yes: Some(0.16),
            decision_kind: DecisionKind::Fire,
            decision_side: Some("yes".into()),
            decision_size_usd: Some(1.0),
            decision_price: Some(0.34),
            no_signal_reason: None,
            reject_reason: None,
            incomplete_reason: None,
        };
        let line = serde_json::to_string(&r).unwrap();
        let parsed: DecisionRecord = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed.market_id, r.market_id);
        assert_eq!(parsed.decision_kind, DecisionKind::Fire);
        assert_eq!(parsed.poly_yes_mid, Some(0.34));
    }

    #[test]
    fn logger_writes_and_appends() {
        let dir = std::env::temp_dir().join(format!("polybot_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("decisions.ndjson");
        let _ = std::fs::remove_file(&path);
        let mut logger = DecisionLogger::open(path.clone()).unwrap();

        let mut rec = DecisionRecord {
            schema_version: SCHEMA_VERSION,
            local_ts_ns: "1".into(),
            session_id: "s".into(),
            bot_version: BOT_VERSION.into(),
            market_id: "m".into(),
            market_slug: "ms".into(),
            yes_token: "y".into(),
            no_token: "n".into(),
            end_epoch: 100,
            effective_start_epoch: 0,
            ttr_secs: 100.0,
            binance_btc_mid_usd: None,
            binance_strike_usd: None,
            btc_last_update_age_ms: None,
            btc_history_len: 0,
            poly_yes_bid: None,
            poly_yes_ask: None,
            poly_yes_mid: None,
            poly_no_bid: None,
            poly_no_ask: None,
            poly_no_mid: None,
            poly_book_age_ms: None,
            implied_strike_usd: None,
            strike_gap_usd: None,
            strike_gap_bps: None,
            sigma_per_sec_used: None,
            sigma_source: None,
            vol_samples: 0,
            fv_yes: None,
            fv_no: None,
            edge_yes: None,
            decision_kind: DecisionKind::Incomplete,
            decision_side: None,
            decision_size_usd: None,
            decision_price: None,
            no_signal_reason: None,
            reject_reason: None,
            incomplete_reason: Some(IncompleteReason::NoStrike),
        };
        logger.write(&rec).unwrap();
        rec.local_ts_ns = "2".into();
        logger.write(&rec).unwrap();

        let body = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2);
        let parsed: DecisionRecord = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed.local_ts_ns, "1");
        let parsed2: DecisionRecord = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(parsed2.local_ts_ns, "2");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }
}

// Re-export so callers don't have to depend on internal modules to
// stringify a reject reason.
pub fn reject_reason_to_str(r: &RejectReason) -> &'static str {
    match r {
        RejectReason::MarketKilled => "market_killed",
        RejectReason::TooManyConcurrent => "too_many_concurrent",
        RejectReason::Cooldown => "cooldown",
        RejectReason::NotionalCapReached => "notional_cap_reached",
        RejectReason::InternalError => "internal_error",
    }
}
