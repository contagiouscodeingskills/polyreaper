//! Cross-venue latency probe.
//!
//! Periodically opens a TCP connect to each venue's host:port and records
//! the time-to-establish. This is a *floor* on one-way latency — actual
//! WebSocket/HTTP latency includes TLS handshake + protocol overhead, but
//! the TCP connect is enough to detect routing changes, regional failover,
//! and gross VPS-to-venue path issues.
//!
//! Output goes to two places:
//! * **journald** (via `tracing::info!`) — for live operator visibility.
//! * **`<session_dir>/_latency_probes.ndjson`** — one NDJSON line per
//!   probe. Persisted alongside the captured event data so research code
//!   can apply per-venue latency floors when comparing arrival times.
//!
//! When this is useful:
//! * Cross-venue clock alignment (Binance Tokyo vs Polymarket US can be
//!   very different from a Frankfurt VPS).
//! * Detecting CDN region changes — sudden jumps in RTT suggest the venue
//!   started routing us to a different POP.
//! * Distinguishing a genuine venue outage from a network blip.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::net::TcpStream;
use tokio::time::timeout;

const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const PROBE_FILENAME: &str = "_latency_probes.ndjson";

/// Targets are `(label, "host:port")` pairs. Labels are stable so a
/// long-term `journalctl | grep` series is easy to chart.
const TARGETS: &[(&str, &str)] = &[
    ("binance_ws", "stream.binance.com:443"),
    ("binance_rest", "api.binance.com:443"),
    ("polymarket_ws", "ws-subscriptions-clob.polymarket.com:443"),
    ("polymarket_gamma", "gamma-api.polymarket.com:443"),
    ("coinbase_ws", "advanced-trade-ws.coinbase.com:443"),
    ("eth_rpc_ws", "ethereum-rpc.publicnode.com:443"),
];

#[derive(Serialize)]
struct ProbeRecord<'a> {
    /// Local wall-clock when the probe completed, ns since UNIX epoch.
    /// Stringified for the same precision-preserving reason
    /// `RawEvent.local_ts_ns` is.
    ts_ns: String,
    target: &'a str,
    addr: &'a str,
    /// "ok" | "error" | "timeout"
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    rtt_ms: Option<u64>,
    /// Set on `error`/`timeout` so the exact failure mode is visible
    /// without re-grepping journald.
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

pub async fn run_latency_probe_loop(session_dir: PathBuf, interval: Duration) {
    let probe_path = session_dir.join(PROBE_FILENAME);
    tracing::info!(
        component = "recorder",
        event = "latency_probe_started",
        path = %probe_path.display(),
        interval_secs = interval.as_secs(),
        "latency probe running"
    );
    loop {
        for (label, addr) in TARGETS {
            probe_once(label, addr, &probe_path).await;
        }
        tokio::time::sleep(interval).await;
    }
}

async fn probe_once(label: &str, addr: &str, path: &Path) {
    let start = Instant::now();
    let outcome = timeout(PROBE_TIMEOUT, TcpStream::connect(addr)).await;
    let elapsed_ms = start.elapsed().as_millis() as u64;

    let record = match outcome {
        Ok(Ok(_stream)) => {
            tracing::info!(
                component = "recorder",
                event = "latency_probe",
                target = label,
                addr = addr,
                rtt_ms = elapsed_ms,
                "tcp connect ok"
            );
            ProbeRecord {
                ts_ns: now_ns_string(),
                target: label,
                addr,
                status: "ok",
                rtt_ms: Some(elapsed_ms),
                reason: None,
            }
        }
        Ok(Err(e)) => {
            tracing::warn!(
                component = "recorder",
                event = "latency_probe_failed",
                target = label,
                addr = addr,
                reason = %e,
                "tcp connect error"
            );
            ProbeRecord {
                ts_ns: now_ns_string(),
                target: label,
                addr,
                status: "error",
                rtt_ms: None,
                reason: Some(e.to_string()),
            }
        }
        Err(_) => {
            tracing::warn!(
                component = "recorder",
                event = "latency_probe_timeout",
                target = label,
                addr = addr,
                timeout_ms = PROBE_TIMEOUT.as_millis() as u64,
                "tcp connect timed out"
            );
            ProbeRecord {
                ts_ns: now_ns_string(),
                target: label,
                addr,
                status: "timeout",
                rtt_ms: None,
                reason: Some(format!("{}ms", PROBE_TIMEOUT.as_millis())),
            }
        }
    };

    if let Err(e) = append_record(path, &record) {
        // Persistence failure is non-fatal: journald still has the line.
        tracing::warn!(
            component = "recorder",
            event = "latency_probe_persist_failed",
            target = label,
            error = %e,
            "could not append probe record"
        );
    }
}

fn now_ns_string() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
        .to_string()
}

fn append_record(path: &Path, rec: &ProbeRecord<'_>) -> std::io::Result<()> {
    let line = serde_json::to_string(rec).expect("probe record serialises") + "\n";
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
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestDir(PathBuf);
    impl TestDir {
        fn new() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let ptr = &nanos as *const _ as usize;
            let dir = std::env::temp_dir().join(format!("polybot_lat_test_{nanos}_{ptr:x}"));
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

    #[test]
    fn probe_record_ok_serialises_to_one_ndjson_line() {
        let r = ProbeRecord {
            ts_ns: "1234".into(),
            target: "binance_ws",
            addr: "stream.binance.com:443",
            status: "ok",
            rtt_ms: Some(243),
            reason: None,
        };
        let line = serde_json::to_string(&r).unwrap();
        assert!(!line.contains('\n'));
        assert!(line.contains(r#""status":"ok""#));
        assert!(line.contains(r#""rtt_ms":243"#));
        assert!(!line.contains(r#""reason":null"#));
    }

    #[test]
    fn probe_record_error_omits_rtt() {
        let r = ProbeRecord {
            ts_ns: "1234".into(),
            target: "polymarket_ws",
            addr: "ws.polymarket.com:443",
            status: "error",
            rtt_ms: None,
            reason: Some("connection refused".into()),
        };
        let line = serde_json::to_string(&r).unwrap();
        assert!(line.contains(r#""status":"error""#));
        assert!(!line.contains("rtt_ms"));
        assert!(line.contains(r#""reason":"connection refused""#));
    }

    #[test]
    fn append_creates_file_and_appends_lines() {
        let tmp = TestDir::new();
        let path = tmp.path().join(PROBE_FILENAME);

        let r1 = ProbeRecord {
            ts_ns: "1".into(),
            target: "a",
            addr: "a:1",
            status: "ok",
            rtt_ms: Some(10),
            reason: None,
        };
        let r2 = ProbeRecord {
            ts_ns: "2".into(),
            target: "b",
            addr: "b:2",
            status: "timeout",
            rtt_ms: None,
            reason: Some("5000ms".into()),
        };

        append_record(&path, &r1).unwrap();
        append_record(&path, &r2).unwrap();

        let body = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2);

        let v1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        let v2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(v1["target"], "a");
        assert_eq!(v1["status"], "ok");
        assert_eq!(v1["rtt_ms"], 10);
        assert_eq!(v2["target"], "b");
        assert_eq!(v2["status"], "timeout");
    }
}
