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

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use telemetry::{AtomicTs, Counter};

/// A feed is flagged `stalled` when no message has been received from it
/// for at least this many seconds. Polymarket and Binance are sub-second
/// chatty under any normal load; Coinbase BTC-USD is sparse (~1 msg/s)
/// but should never go silent for 60s. Chainlink is disabled and always
/// reports stalled — we tolerate that.
const STALL_THRESHOLD_SECS: u64 = 60;

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
    /// Recorder's session directory, repeated on every line so any
    /// _health.ndjson tail tells you which session it belongs to.
    session_dir: String,
    /// Free bytes on the filesystem holding the session directory. Read
    /// via `statvfs`-equivalent (we shell out to `df -B1`) to match what
    /// disk_guard.sh sees. Helps researchers correlate write_failures
    /// spikes with disk pressure without joining external logs.
    disk_free_bytes: u64,
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
    /// Local wall-clock at the moment this feed last received a Text
    /// frame off its websocket, ns since UNIX epoch. Stringified for
    /// the same precision-preserving reason `Snapshot.ts_ns` is.
    /// Omitted when the feed has never received a frame.
    #[serde(skip_serializing_if = "Option::is_none")]
    last_msg_local_ts_ns: Option<String>,
    /// Messages per second since the previous health snapshot. Useful
    /// for detecting silent rate drops (subscription drift, server-side
    /// throttling) without needing the previous snapshot to hand. `None`
    /// on the first snapshot of a session (no prior delta).
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_rate_per_sec: Option<f64>,
    /// True when last_msg_local_ts_ns is older than STALL_THRESHOLD_SECS.
    /// Calling code logs a WARN when this flips from false to true.
    /// Always false for chainlink (the feed is intentionally a no-op).
    stalled: bool,
    /// Storage critical-section duration quantiles, in microseconds.
    /// Each sample is one (`store.lock()` + `guard.write()` + drop)
    /// cycle. Cumulative since recorder start — *not* a rolling
    /// window — so warm-up samples after a restart linger in the
    /// distribution. Omitted until the feed records its first sample.
    /// Acceptance: p99 < 1000 healthy, > 10000 sustained → discuss
    /// per-feed storage writers.
    #[serde(skip_serializing_if = "Option::is_none")]
    store_p50_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    store_p99_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    store_p999_us: Option<u64>,
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

/// Per-feed state that persists across snapshots so we can compute
/// message-rate deltas and detect stall onset (transition from
/// "not stalled" to "stalled").
#[derive(Default)]
struct WriterState {
    /// Previous snapshot wall-clock ns (for rate denominator).
    prev_ts_ns: Option<u128>,
    /// Previous per-feed message count (for rate numerator).
    prev_msgs: PerFeed<u64>,
    /// Previous stalled flag per feed (so we only log WARN on transition,
    /// not on every snapshot while the feed is silent).
    prev_stalled: PerFeed<bool>,
}

#[derive(Default)]
struct PerFeed<T> {
    binance: T,
    polymarket: T,
    coinbase: T,
    chainlink: T,
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

    let mut state = WriterState::default();

    loop {
        ticker.tick().await;
        let snap = capture_snapshot(&inputs, &mut state);
        if let Err(e) = append_line(&path, &snap) {
            tracing::warn!(
                component = "recorder",
                event = "health_write_failed",
                path = %path.display(),
                error = %e,
                "health snapshot write failed"
            );
        }
    }
}

fn capture_snapshot(inputs: &HealthInputs, state: &mut WriterState) -> Snapshot {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);

    // Rate denominator (seconds since previous snapshot).
    let dt_secs: Option<f64> = state
        .prev_ts_ns
        .map(|prev| ((now_ns.saturating_sub(prev)) as f64) / 1e9)
        .filter(|s| *s > 0.0);

    // Per-feed counters, with rate computed against previous snapshot's
    // message count if available.
    let bin_msgs = inputs.binance.messages.get();
    let pm_msgs = inputs.polymarket.messages.get();
    let cb_msgs = inputs.coinbase.messages.get();
    let cl_msgs = inputs.chainlink.messages.get();

    let bin_rate = rate(state.prev_msgs.binance, bin_msgs, dt_secs);
    let pm_rate = rate(state.prev_msgs.polymarket, pm_msgs, dt_secs);
    let cb_rate = rate(state.prev_msgs.coinbase, cb_msgs, dt_secs);
    let cl_rate = rate(state.prev_msgs.chainlink, cl_msgs, dt_secs);

    // Stall detection. Chainlink is excluded because the feed is a
    // deliberate no-op (always silent); flagging it on every snapshot
    // would just be noise.
    let bin_stalled = is_stalled(&inputs.binance.last_msg, now_ns);
    let pm_stalled = is_stalled(&inputs.polymarket.last_msg, now_ns);
    let cb_stalled = is_stalled(&inputs.coinbase.last_msg, now_ns);
    let cl_stalled = false;

    // Log a WARN once per transition into the stalled state.
    log_stall_transition("binance", state.prev_stalled.binance, bin_stalled);
    log_stall_transition("polymarket", state.prev_stalled.polymarket, pm_stalled);
    log_stall_transition("coinbase", state.prev_stalled.coinbase, cb_stalled);

    let snap = Snapshot {
        ts_ns: now_ns.to_string(),
        session_dir: inputs.session_dir.display().to_string(),
        disk_free_bytes: disk_free_bytes(&inputs.session_dir).unwrap_or(0),
        feeds: Feeds {
            binance: counters(&inputs.binance, bin_rate, bin_stalled),
            polymarket: counters_polymarket(&inputs.polymarket, pm_rate, pm_stalled),
            coinbase: counters_coinbase(&inputs.coinbase, cb_rate, cb_stalled),
            chainlink: counters_chainlink(&inputs.chainlink, cl_rate, cl_stalled),
        },
        chrony: fetch_chrony(),
    };

    state.prev_ts_ns = Some(now_ns);
    state.prev_msgs = PerFeed {
        binance: bin_msgs,
        polymarket: pm_msgs,
        coinbase: cb_msgs,
        chainlink: cl_msgs,
    };
    state.prev_stalled = PerFeed {
        binance: bin_stalled,
        polymarket: pm_stalled,
        coinbase: cb_stalled,
        chainlink: cl_stalled,
    };

    snap
}

fn rate(prev: u64, cur: u64, dt_secs: Option<f64>) -> Option<f64> {
    let dt = dt_secs?;
    let delta = cur.saturating_sub(prev);
    Some((delta as f64) / dt)
}

fn is_stalled(ts: &AtomicTs, now_ns: u128) -> bool {
    let last = ts.get_ns();
    if last == 0 {
        // Feed has never received a frame yet. Don't flag — we don't
        // know if it's still warming up. The next snapshot will catch
        // a true stall once a first frame has been seen.
        return false;
    }
    let age_ns = now_ns.saturating_sub(last as u128);
    age_ns >= (STALL_THRESHOLD_SECS as u128) * 1_000_000_000
}

fn log_stall_transition(venue: &str, was_stalled: bool, now_stalled: bool) {
    if !was_stalled && now_stalled {
        tracing::warn!(
            component = "recorder",
            event = "feed_stalled",
            venue = venue,
            stall_threshold_secs = STALL_THRESHOLD_SECS,
            "feed has not received a message within the stall threshold"
        );
    } else if was_stalled && !now_stalled {
        tracing::info!(
            component = "recorder",
            event = "feed_recovered",
            venue = venue,
            "feed received a message again"
        );
    }
}

/// Free bytes on the filesystem holding `path`. We shell out to `df -B1`
/// to avoid pulling a `nix`/`rustix` dep just for one syscall; this is
/// only called once per snapshot (every 30s by default), so the spawn
/// cost is irrelevant.
fn disk_free_bytes(path: &Path) -> Option<u64> {
    let out = Command::new("df")
        .args(["-B1", "--output=avail"])
        .arg(path)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout);
    // Output:
    //     Avail
    //   12345
    s.lines().nth(1)?.trim().parse::<u64>().ok()
}

// Each feed's FeedStats is its own type — same shape, no shared trait.
// Inline helpers keep the snapshot builder readable.
fn counters(
    s: &binance_feed::FeedStats,
    msg_rate_per_sec: Option<f64>,
    stalled: bool,
) -> FeedCounters {
    FeedCounters {
        messages: get(&s.messages),
        reconnects: get(&s.reconnects),
        parse_failures: get(&s.parse_failures),
        write_failures: get(&s.write_failures),
        last_msg_local_ts_ns: ts_field(&s.last_msg),
        msg_rate_per_sec: msg_rate_per_sec.map(|r| (r * 1000.0).round() / 1000.0),
        stalled,
        store_p50_us: s.store_us.quantile_micros(0.50),
        store_p99_us: s.store_us.quantile_micros(0.99),
        store_p999_us: s.store_us.quantile_micros(0.999),
    }
}
fn counters_polymarket(
    s: &polymarket_feed::FeedStats,
    msg_rate_per_sec: Option<f64>,
    stalled: bool,
) -> FeedCounters {
    FeedCounters {
        messages: get(&s.messages),
        reconnects: get(&s.reconnects),
        parse_failures: get(&s.parse_failures),
        write_failures: get(&s.write_failures),
        last_msg_local_ts_ns: ts_field(&s.last_msg),
        msg_rate_per_sec: msg_rate_per_sec.map(|r| (r * 1000.0).round() / 1000.0),
        stalled,
        store_p50_us: s.store_us.quantile_micros(0.50),
        store_p99_us: s.store_us.quantile_micros(0.99),
        store_p999_us: s.store_us.quantile_micros(0.999),
    }
}
fn counters_coinbase(
    s: &coinbase_feed::FeedStats,
    msg_rate_per_sec: Option<f64>,
    stalled: bool,
) -> FeedCounters {
    FeedCounters {
        messages: get(&s.messages),
        reconnects: get(&s.reconnects),
        parse_failures: get(&s.parse_failures),
        write_failures: get(&s.write_failures),
        last_msg_local_ts_ns: ts_field(&s.last_msg),
        msg_rate_per_sec: msg_rate_per_sec.map(|r| (r * 1000.0).round() / 1000.0),
        stalled,
        store_p50_us: s.store_us.quantile_micros(0.50),
        store_p99_us: s.store_us.quantile_micros(0.99),
        store_p999_us: s.store_us.quantile_micros(0.999),
    }
}
fn counters_chainlink(
    s: &chainlink_feed::FeedStats,
    msg_rate_per_sec: Option<f64>,
    stalled: bool,
) -> FeedCounters {
    FeedCounters {
        messages: get(&s.messages),
        reconnects: get(&s.reconnects),
        parse_failures: get(&s.parse_failures),
        write_failures: get(&s.write_failures),
        last_msg_local_ts_ns: ts_field(&s.last_msg),
        msg_rate_per_sec: msg_rate_per_sec.map(|r| (r * 1000.0).round() / 1000.0),
        stalled,
        store_p50_us: s.store_us.quantile_micros(0.50),
        store_p99_us: s.store_us.quantile_micros(0.99),
        store_p999_us: s.store_us.quantile_micros(0.999),
    }
}

fn get(c: &Counter) -> u64 {
    c.get()
}

/// Format a feed's last-msg timestamp for the snapshot.
/// `0` (never set) maps to `None`; otherwise stringified ns since
/// UNIX epoch.
fn ts_field(t: &AtomicTs) -> Option<String> {
    let n = t.get_ns();
    if n == 0 {
        None
    } else {
        Some(n.to_string())
    }
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

    fn empty_counters(stalled: bool) -> FeedCounters {
        FeedCounters {
            messages: 0,
            reconnects: 0,
            parse_failures: 0,
            write_failures: 0,
            last_msg_local_ts_ns: None,
            msg_rate_per_sec: None,
            stalled,
            store_p50_us: None,
            store_p99_us: None,
            store_p999_us: None,
        }
    }

    #[test]
    fn snapshot_serialises_to_one_line_ndjson() {
        let snap = Snapshot {
            ts_ns: "1234567890".into(),
            session_dir: "/tmp/session_x".into(),
            disk_free_bytes: 1_073_741_824,
            feeds: Feeds {
                binance: FeedCounters {
                    messages: 1,
                    reconnects: 2,
                    parse_failures: 3,
                    write_failures: 4,
                    last_msg_local_ts_ns: Some("9999".into()),
                    msg_rate_per_sec: Some(42.5),
                    stalled: false,
                    store_p50_us: Some(123),
                    store_p99_us: Some(456),
                    store_p999_us: Some(789),
                },
                polymarket: empty_counters(false),
                coinbase: empty_counters(false),
                chainlink: empty_counters(false),
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
        assert!(line.contains(r#""session_dir":"/tmp/session_x""#));
        assert!(line.contains(r#""disk_free_bytes":1073741824"#));
        assert!(line.contains(r#""last_offset_secs":0.001"#));
        assert!(line.contains(r#""available":true"#));
        assert!(!line.contains(r#""error":null"#));
        assert!(line.contains(r#""last_msg_local_ts_ns":"9999""#));
        assert!(!line.contains(r#""last_msg_local_ts_ns":null"#));
        assert!(line.contains(r#""msg_rate_per_sec":42.5"#));
        assert!(line.contains(r#""stalled":false"#));
        assert!(line.contains(r#""store_p99_us":456"#));
    }

    #[test]
    fn rate_returns_none_on_first_snapshot() {
        // First snapshot has no prev_ts_ns so dt is None.
        assert_eq!(rate(0, 100, None), None);
    }

    #[test]
    fn rate_computes_messages_per_sec() {
        // 100 new messages over 10 seconds = 10 msg/s.
        assert_eq!(rate(0, 100, Some(10.0)), Some(10.0));
        // 200 new messages over 0.5 seconds = 400 msg/s.
        assert_eq!(rate(100, 300, Some(0.5)), Some(400.0));
    }

    #[test]
    fn rate_handles_counter_quirks() {
        // Counter only ever increases in the recorder, but if a snapshot
        // miss leaves prev > cur we should clamp to 0 not panic.
        assert_eq!(rate(200, 100, Some(1.0)), Some(0.0));
    }

    #[test]
    fn is_stalled_returns_false_when_never_received() {
        let ts = AtomicTs::default();
        // ts.get_ns() == 0 means feed has never received a frame.
        // Don't flag stalled — could be still warming up.
        let now_ns: u128 = 10_000_000_000_000_000_000; // arbitrary far future
        assert!(!is_stalled(&ts, now_ns));
    }

    #[test]
    fn is_stalled_true_when_last_msg_older_than_threshold() {
        let ts = AtomicTs::default();
        let last_ns: u128 = 1_000_000_000_000_000_000;
        ts.set_ns(last_ns);
        let now_ns = last_ns + (STALL_THRESHOLD_SECS as u128 + 1) * 1_000_000_000;
        assert!(is_stalled(&ts, now_ns));
    }

    #[test]
    fn is_stalled_false_when_last_msg_recent() {
        let ts = AtomicTs::default();
        let last_ns: u128 = 1_000_000_000_000_000_000;
        ts.set_ns(last_ns);
        let now_ns = last_ns + 1_000_000_000; // 1 second later
        assert!(!is_stalled(&ts, now_ns));
    }
}
