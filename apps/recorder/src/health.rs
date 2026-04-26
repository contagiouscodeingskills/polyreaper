//! Periodic health snapshot writer.
//!
//! Every `interval`, samples per-feed counters + chrony state and
//! appends one NDJSON line to `<session_dir>/_health.ndjson`. This is
//! the file research notebooks use to:
//!
//! * Filter out windows where a feed was reconnecting (`reconnects`
//!   delta > 0 in a 30 s window suggests data was lost).
//! * Filter out windows where the local clock drifted (`chrony.last_offset_secs`
//!   above some bound).
//! * Spot quiet windows that aren't actually quiet, just disconnected.
//!
//! Format is plain NDJSON, NOT a [`common::RawEvent`] — this is
//! sidecar metadata, not a market event. The replayer's discovery
//! walk doesn't pick up `_health.ndjson` because it isn't under a
//! venue subdirectory.
//!
//! ## chrony parsing
//!
//! We shell out to `chronyc tracking` and parse the output. If
//! `chronyc` isn't installed (dev box), we record `available: false`
//! with the spawn error so the absence is visible to research code
//! rather than silently looking like a clean clock. Parse failures of
//! individual fields are also tolerated — we log raw output alongside
//! parsed values for any post-hoc deeper inspection.

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use telemetry::Counter;

/// Inputs the health writer needs from the rest of the recorder. Each
/// `FeedStats` is cloned (cheap — `Counter` is `Arc<AtomicU64>`).
pub struct HealthInputs {
    pub session_dir: PathBuf,
    pub binance: binance_feed::FeedStats,
    pub polymarket: polymarket_feed::FeedStats,
    pub coinbase: coinbase_feed::FeedStats,
    pub chainlink: chainlink_feed::FeedStats,
}

#[derive(Serialize)]
struct Snapshot {
    /// Local wall-clock when this snapshot was sampled, ns since epoch.
    /// Stringified for the same precision-preserving reason
    /// `RawEvent.local_ts_ns` is.
    ts_ns: String,
    feeds: Feeds,
    chrony: ChronyState,
}

#[derive(Serialize)]
struct Feeds {
    binance: FeedCounters,
    polymarket: FeedCounters,
    coinbase: FeedCounters,
    chainlink: FeedCounters,
}

#[derive(Serialize)]
struct FeedCounters {
    messages: u64,
    reconnects: u64,
    parse_failures: u64,
    write_failures: u64,
}

#[derive(Serialize)]
struct ChronyState {
    available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// Last offset (signed seconds). Positive = local clock ahead of NTP.
    #[serde(skip_serializing_if = "Option::is_none")]
    last_offset_secs: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rms_offset_secs: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stratum: Option<u32>,
    /// Full raw `chronyc tracking` output. Kept for post-hoc inspection
    /// of fields we don't currently parse.
    #[serde(skip_serializing_if = "Option::is_none")]
    raw: Option<String>,
}

/// Run forever, never returns. Designed to be `tokio::spawn`'d alongside
/// the feed tasks; aborts cleanly when the parent task aborts it.
pub async fn run_health_writer_loop(inputs: HealthInputs, interval: Duration) {
    let path = inputs.session_dir.join("_health.ndjson");
    tracing::info!(
        component = "recorder",
        event = "health_writer_started",
        path = %path.display(),
        interval_secs = interval.as_secs(),
        "health writer running"
    );

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // skip the immediate fire so first record has real counts

    loop {
        ticker.tick().await;
        let snap = capture_snapshot(&inputs);
        match append_line(&path, &snap) {
            Ok(()) => {}
            Err(e) => tracing::warn!(
                component = "recorder",
                event = "health_write_failed",
                path = %path.display(),
                error = %e,
                "health snapshot write failed"
            ),
        }
    }
}

fn capture_snapshot(inputs: &HealthInputs) -> Snapshot {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    Snapshot {
        ts_ns: now_ns.to_string(),
        feeds: Feeds {
            binance: counters(&inputs.binance),
            polymarket: counters_polymarket(&inputs.polymarket),
            coinbase: counters_coinbase(&inputs.coinbase),
            chainlink: counters_chainlink(&inputs.chainlink),
        },
        chrony: fetch_chrony(),
    }
}

// Each feed's FeedStats is its own type — same shape, no shared trait.
// Inline helpers keep the snapshot builder readable.
fn counters(s: &binance_feed::FeedStats) -> FeedCounters {
    FeedCounters {
        messages: get(&s.messages),
        reconnects: get(&s.reconnects),
        parse_failures: get(&s.parse_failures),
        write_failures: get(&s.write_failures),
    }
}
fn counters_polymarket(s: &polymarket_feed::FeedStats) -> FeedCounters {
    FeedCounters {
        messages: get(&s.messages),
        reconnects: get(&s.reconnects),
        parse_failures: get(&s.parse_failures),
        write_failures: get(&s.write_failures),
    }
}
fn counters_coinbase(s: &coinbase_feed::FeedStats) -> FeedCounters {
    FeedCounters {
        messages: get(&s.messages),
        reconnects: get(&s.reconnects),
        parse_failures: get(&s.parse_failures),
        write_failures: get(&s.write_failures),
    }
}
fn counters_chainlink(s: &chainlink_feed::FeedStats) -> FeedCounters {
    FeedCounters {
        messages: get(&s.messages),
        reconnects: get(&s.reconnects),
        parse_failures: get(&s.parse_failures),
        write_failures: get(&s.write_failures),
    }
}

fn get(c: &Counter) -> u64 {
    c.get()
}

fn fetch_chrony() -> ChronyState {
    let output = Command::new("chronyc").arg("tracking").output();
    match output {
        Ok(out) if out.status.success() => {
            let raw = String::from_utf8_lossy(&out.stdout).into_owned();
            let parsed = parse_chronyc_tracking(&raw);
            ChronyState {
                available: true,
                error: None,
                last_offset_secs: parsed.last_offset_secs,
                rms_offset_secs: parsed.rms_offset_secs,
                stratum: parsed.stratum,
                raw: Some(raw),
            }
        }
        Ok(out) => ChronyState {
            available: false,
            error: Some(format!(
                "chronyc exit {:?}: {}",
                out.status.code(),
                String::from_utf8_lossy(&out.stderr).trim()
            )),
            last_offset_secs: None,
            rms_offset_secs: None,
            stratum: None,
            raw: None,
        },
        Err(e) => ChronyState {
            available: false,
            error: Some(format!("chronyc spawn: {e}")),
            last_offset_secs: None,
            rms_offset_secs: None,
            stratum: None,
            raw: None,
        },
    }
}

#[derive(Default)]
struct ChronyParsed {
    last_offset_secs: Option<f64>,
    rms_offset_secs: Option<f64>,
    stratum: Option<u32>,
}

/// Parse the bits of `chronyc tracking` we care about. Tolerant —
/// missing or malformed lines just leave that field as `None`.
fn parse_chronyc_tracking(raw: &str) -> ChronyParsed {
    let mut p = ChronyParsed::default();
    for line in raw.lines() {
        let (key, value) = match line.split_once(':') {
            Some((k, v)) => (k.trim(), v.trim()),
            None => continue,
        };
        match key {
            // "Last offset     : -0.000234567 seconds"
            "Last offset" => {
                p.last_offset_secs = first_f64(value);
            }
            "RMS offset" => {
                p.rms_offset_secs = first_f64(value);
            }
            // "Stratum         : 2"
            "Stratum" => {
                p.stratum = value.parse::<u32>().ok();
            }
            _ => {}
        }
    }
    p
}

fn first_f64(s: &str) -> Option<f64> {
    s.split_whitespace().next()?.parse::<f64>().ok()
}

fn append_line(path: &std::path::Path, snap: &Snapshot) -> std::io::Result<()> {
    use std::fs::OpenOptions;
    use std::io::Write;
    let line = serde_json::to_string(snap).expect("snapshot serialises") + "\n";
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    f.write_all(line.as_bytes())?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_TRACKING: &str = "Reference ID    : C0A80101 (192.168.1.1)
Stratum         : 3
Ref time (UTC)  : Wed Apr 25 05:30:00 2026
System time     : 0.000123456 seconds slow of NTP time
Last offset     : -0.000234567 seconds
RMS offset      : 0.000345678 seconds
Frequency       : 12.345 ppm slow
Residual freq   : 0.000 ppm
Skew            : 0.123 ppm
Root delay      : 0.005678901 seconds
Root dispersion : 0.001234567 seconds
Update interval : 64.7 seconds
Leap status     : Normal";

    #[test]
    fn parses_chronyc_tracking_fields() {
        let p = parse_chronyc_tracking(SAMPLE_TRACKING);
        assert_eq!(p.stratum, Some(3));
        assert!((p.last_offset_secs.unwrap() - (-0.000234567)).abs() < 1e-12);
        assert!((p.rms_offset_secs.unwrap() - 0.000345678).abs() < 1e-12);
    }

    #[test]
    fn parser_tolerates_missing_lines() {
        let p = parse_chronyc_tracking("Stratum: 2");
        assert_eq!(p.stratum, Some(2));
        assert_eq!(p.last_offset_secs, None);
    }

    #[test]
    fn parser_tolerates_garbage() {
        let p = parse_chronyc_tracking("not a chronyc output\nLast offset: garbage seconds");
        assert_eq!(p.last_offset_secs, None);
    }

    #[test]
    fn snapshot_serialises_to_one_line_ndjson() {
        let snap = Snapshot {
            ts_ns: "1234567890".into(),
            feeds: Feeds {
                binance: FeedCounters { messages: 1, reconnects: 2, parse_failures: 3, write_failures: 4 },
                polymarket: FeedCounters { messages: 0, reconnects: 0, parse_failures: 0, write_failures: 0 },
                coinbase: FeedCounters { messages: 0, reconnects: 0, parse_failures: 0, write_failures: 0 },
                chainlink: FeedCounters { messages: 0, reconnects: 0, parse_failures: 0, write_failures: 0 },
            },
            chrony: ChronyState {
                available: true,
                error: None,
                last_offset_secs: Some(0.001),
                rms_offset_secs: Some(0.002),
                stratum: Some(3),
                raw: Some("Stratum: 3".into()),
            },
        };
        let line = serde_json::to_string(&snap).unwrap();
        assert!(!line.contains('\n'));
        assert!(line.contains(r#""ts_ns":"1234567890""#));
        assert!(line.contains(r#""last_offset_secs":0.001"#));
        assert!(line.contains(r#""available":true"#));
        // Nullable error field omitted because we set Some only when present.
        assert!(!line.contains(r#""error":null"#));
    }
}
