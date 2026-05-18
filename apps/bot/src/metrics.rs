//! Prometheus-style `/metrics` endpoint.
//!
//! The bot writes a [`MetricsSnapshot`] into [`MetricsRegistry`] at the
//! same cadence as the FV tick (default 100ms). A tiny async HTTP server
//! task serves the latest snapshot at `GET /metrics` in the Prometheus
//! exposition format. Any other path returns 404.
//!
//! We don't pull in a metrics framework: the exposition format is plain
//! text and our metric set is fixed and small. Adding a gauge means
//! adding one field and one `writeln!`.
//!
//! ## Why a shared snapshot, not atomics per gauge
//!
//! Bot writes are batched (one per tick); scrapes are sparse (every 15s
//! by default for Prometheus). A `Mutex<MetricsSnapshot>` has negligible
//! contention and lets us add gauges by adding a field. Atomics would
//! force a lookup-table or generated code for every new metric.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{info, warn};

/// Snapshot of bot state exposed as Prometheus gauges/counters.
/// `f64` is the Prometheus-native type. `i64` and `usize` are coerced.
///
/// Names follow the convention `bot_<noun>_<unit>`. Counters end in
/// `_total`. Per-market labels go in `realised_pnl_per_market_usd`.
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    pub uptime_secs: f64,
    pub bankroll_usd: f64,
    pub total_realised_pnl_usd: f64,
    pub open_positions: usize,
    pub total_fills: u64,
    pub seen_markets: usize,
    pub resolved_markets: usize,
    pub btc_history_len: usize,
    pub vol_samples: usize,
    pub kill_switch_tripped: bool,
    pub latest_btc_mid_usd: Option<f64>,
    pub sigma_per_sec: Option<f64>,
    pub latest_strike_usd: Option<f64>,
    pub latest_ttr_secs: Option<f64>,
    /// Realised P&L per market. The label is the market_id.
    pub realised_pnl_per_market_usd: HashMap<String, f64>,
    /// Counts of each decision kind seen this run.
    pub decisions_fire_total: u64,
    pub decisions_rejected_total: u64,
    pub decisions_no_signal_total: u64,
    pub decisions_incomplete_total: u64,
}

/// Shared metrics state — `Arc::clone` it into the bot task and the
/// metrics server task.
#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    inner: Arc<Mutex<MetricsSnapshot>>,
    start_instant: Arc<Instant>,
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MetricsSnapshot::default())),
            start_instant: Arc::new(Instant::now()),
        }
    }

    /// Replace the snapshot. The caller computes uptime via `uptime()`
    /// or leaves it 0; the server fills it in on render.
    pub fn set(&self, mut snap: MetricsSnapshot) {
        snap.uptime_secs = self.uptime().as_secs_f64();
        let mut guard = self.inner.lock().expect("metrics mutex poisoned");
        *guard = snap;
    }

    /// In-place mutation closure for callers that want to update only
    /// some fields. The closure receives the current snapshot by mut.
    pub fn update_with(&self, mutator: impl FnOnce(&mut MetricsSnapshot)) {
        let mut guard = self.inner.lock().expect("metrics mutex poisoned");
        mutator(&mut guard);
        guard.uptime_secs = self.uptime().as_secs_f64();
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        self.inner.lock().expect("metrics mutex poisoned").clone()
    }

    fn uptime(&self) -> Duration {
        self.start_instant.elapsed()
    }

    /// Increment a decision-kind counter.
    pub fn record_decision(&self, kind: DecisionKindMetric) {
        let mut guard = self.inner.lock().expect("metrics mutex poisoned");
        match kind {
            DecisionKindMetric::Fire => guard.decisions_fire_total += 1,
            DecisionKindMetric::Rejected => guard.decisions_rejected_total += 1,
            DecisionKindMetric::NoSignal => guard.decisions_no_signal_total += 1,
            DecisionKindMetric::Incomplete => guard.decisions_incomplete_total += 1,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DecisionKindMetric {
    Fire,
    Rejected,
    NoSignal,
    Incomplete,
}

/// Render the snapshot as a Prometheus exposition text block.
/// Each metric gets a `# HELP` + `# TYPE` line then one value line per
/// (label-set, value) pair. Missing optional gauges are omitted; the
/// scraper sees the metric simply not present, which Prometheus handles
/// gracefully.
pub fn render(snap: &MetricsSnapshot) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(2048);

    // Gauges.
    writeln!(
        out,
        "# HELP bot_uptime_seconds Time since the bot process started."
    )
    .ok();
    writeln!(out, "# TYPE bot_uptime_seconds gauge").ok();
    writeln!(out, "bot_uptime_seconds {}", snap.uptime_secs).ok();

    writeln!(
        out,
        "# HELP bot_bankroll_usd Current bankroll (cash + share value at last persist)."
    )
    .ok();
    writeln!(out, "# TYPE bot_bankroll_usd gauge").ok();
    writeln!(out, "bot_bankroll_usd {}", snap.bankroll_usd).ok();

    writeln!(
        out,
        "# HELP bot_total_realised_pnl_usd Cumulative realised P&L this run."
    )
    .ok();
    writeln!(out, "# TYPE bot_total_realised_pnl_usd gauge").ok();
    writeln!(
        out,
        "bot_total_realised_pnl_usd {}",
        snap.total_realised_pnl_usd
    )
    .ok();

    writeln!(
        out,
        "# HELP bot_open_positions Number of currently open positions."
    )
    .ok();
    writeln!(out, "# TYPE bot_open_positions gauge").ok();
    writeln!(out, "bot_open_positions {}", snap.open_positions).ok();

    writeln!(
        out,
        "# HELP bot_seen_markets Distinct markets the bot has evaluated."
    )
    .ok();
    writeln!(out, "# TYPE bot_seen_markets gauge").ok();
    writeln!(out, "bot_seen_markets {}", snap.seen_markets).ok();

    writeln!(
        out,
        "# HELP bot_resolved_markets Markets that have resolved this run."
    )
    .ok();
    writeln!(out, "# TYPE bot_resolved_markets gauge").ok();
    writeln!(out, "bot_resolved_markets {}", snap.resolved_markets).ok();

    writeln!(
        out,
        "# HELP bot_btc_history_len Samples in the BTC price ring."
    )
    .ok();
    writeln!(out, "# TYPE bot_btc_history_len gauge").ok();
    writeln!(out, "bot_btc_history_len {}", snap.btc_history_len).ok();

    writeln!(
        out,
        "# HELP bot_vol_samples Samples in the rolling-vol window."
    )
    .ok();
    writeln!(out, "# TYPE bot_vol_samples gauge").ok();
    writeln!(out, "bot_vol_samples {}", snap.vol_samples).ok();

    writeln!(
        out,
        "# HELP bot_kill_switch_tripped 1 if the portfolio kill switch is tripped, else 0."
    )
    .ok();
    writeln!(out, "# TYPE bot_kill_switch_tripped gauge").ok();
    writeln!(
        out,
        "bot_kill_switch_tripped {}",
        if snap.kill_switch_tripped { 1 } else { 0 }
    )
    .ok();

    if let Some(mid) = snap.latest_btc_mid_usd {
        writeln!(out, "# HELP bot_btc_mid_usd Latest Binance BTC/USDT mid.").ok();
        writeln!(out, "# TYPE bot_btc_mid_usd gauge").ok();
        writeln!(out, "bot_btc_mid_usd {}", mid).ok();
    }
    if let Some(sigma) = snap.sigma_per_sec {
        writeln!(
            out,
            "# HELP bot_sigma_per_sec Realised volatility (σ) per second."
        )
        .ok();
        writeln!(out, "# TYPE bot_sigma_per_sec gauge").ok();
        writeln!(out, "bot_sigma_per_sec {}", sigma).ok();
    }
    if let Some(s) = snap.latest_strike_usd {
        writeln!(
            out,
            "# HELP bot_strike_usd Strike of the currently active market."
        )
        .ok();
        writeln!(out, "# TYPE bot_strike_usd gauge").ok();
        writeln!(out, "bot_strike_usd {}", s).ok();
    }
    if let Some(t) = snap.latest_ttr_secs {
        writeln!(
            out,
            "# HELP bot_ttr_secs Time-to-resolution of the active market."
        )
        .ok();
        writeln!(out, "# TYPE bot_ttr_secs gauge").ok();
        writeln!(out, "bot_ttr_secs {}", t).ok();
    }

    // Counters.
    writeln!(
        out,
        "# HELP bot_total_fills Total paper-mode fills this run."
    )
    .ok();
    writeln!(out, "# TYPE bot_total_fills counter").ok();
    writeln!(out, "bot_total_fills {}", snap.total_fills).ok();

    writeln!(out, "# HELP bot_decisions_total Decisions by kind.").ok();
    writeln!(out, "# TYPE bot_decisions_total counter").ok();
    writeln!(
        out,
        "bot_decisions_total{{kind=\"fire\"}} {}",
        snap.decisions_fire_total
    )
    .ok();
    writeln!(
        out,
        "bot_decisions_total{{kind=\"rejected\"}} {}",
        snap.decisions_rejected_total
    )
    .ok();
    writeln!(
        out,
        "bot_decisions_total{{kind=\"no_signal\"}} {}",
        snap.decisions_no_signal_total
    )
    .ok();
    writeln!(
        out,
        "bot_decisions_total{{kind=\"incomplete\"}} {}",
        snap.decisions_incomplete_total
    )
    .ok();

    // Per-market series.
    if !snap.realised_pnl_per_market_usd.is_empty() {
        writeln!(
            out,
            "# HELP bot_realised_pnl_per_market_usd Realised P&L by market."
        )
        .ok();
        writeln!(out, "# TYPE bot_realised_pnl_per_market_usd gauge").ok();
        // Sort for deterministic output (helps tests + diffing scrapes).
        let mut keys: Vec<&String> = snap.realised_pnl_per_market_usd.keys().collect();
        keys.sort();
        for k in keys {
            let v = snap.realised_pnl_per_market_usd[k];
            // Escape `"` and `\` per Prometheus label value rules.
            let escaped = k.replace('\\', "\\\\").replace('"', "\\\"");
            writeln!(
                out,
                "bot_realised_pnl_per_market_usd{{market_id=\"{}\"}} {}",
                escaped, v
            )
            .ok();
        }
    }

    out
}

/// Tiny async HTTP server. Listens on `addr`, serves `GET /metrics` with
/// the rendered snapshot, returns 404 for any other path. Connection is
/// closed after each response (no keep-alive — Prometheus is fine with
/// this).
///
/// Designed to run forever; spawn it as a tokio task and forget about it.
/// Returns on listener failure (caller decides whether to restart).
pub async fn run_server(addr: &str, registry: MetricsRegistry) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr().ok();
    let is_loopback = local
        .map(|a| a.ip().is_loopback())
        .unwrap_or(false);
    if !is_loopback {
        // Endpoint is unauthenticated — exposing it on a public
        // interface leaks bankroll, P&L, and decision history. Loud
        // warning so misconfiguration is obvious in logs at boot.
        warn!(
            addr = %addr,
            "metrics endpoint is bound to a non-loopback address; \
             the endpoint is UNAUTHENTICATED and exposes bankroll + P&L. \
             Put an auth proxy in front, or bind to 127.0.0.1."
        );
    }
    info!(addr = %addr, "metrics server listening");
    let mut accept_backoff_ms: u64 = 0;
    loop {
        let (mut socket, peer) = match listener.accept().await {
            Ok(s) => {
                accept_backoff_ms = 0;
                s
            }
            Err(e) => {
                // Avoid spin-loop on persistent accept failure
                // (FD exhaustion, etc.) — back off up to 1s.
                warn!(error = %e, backoff_ms = accept_backoff_ms, "metrics: accept failed");
                if accept_backoff_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(accept_backoff_ms)).await;
                }
                accept_backoff_ms = (accept_backoff_ms.max(50) * 2).min(1_000);
                continue;
            }
        };
        let reg = registry.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            // Read just enough to see the request line. We don't care
            // about headers/body for GET /metrics.
            let n = match socket.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            let head = String::from_utf8_lossy(&buf[..n]);
            let response = handle_request(&head, &reg);
            if let Err(e) = socket.write_all(response.as_bytes()).await {
                warn!(error = %e, ?peer, "metrics: write failed");
            }
            let _ = socket.shutdown().await;
        });
    }
}

/// Pure request handler. Parses the request line from the head of the
/// raw bytes (everything we got off the socket) and returns the full
/// HTTP response as a string.
pub(crate) fn handle_request(head: &str, registry: &MetricsRegistry) -> String {
    // First line: "METHOD PATH HTTP/1.x\r\n"
    let mut parts = head.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    if method != "GET" {
        return http_response(405, "Method Not Allowed", "method not allowed\n");
    }
    match path {
        "/metrics" => {
            let body = render(&registry.snapshot());
            http_response(200, "OK", &body)
        }
        "/healthz" => http_response(200, "OK", "ok\n"),
        _ => http_response(404, "Not Found", "not found\n"),
    }
}

fn http_response(code: u16, status: &str, body: &str) -> String {
    let ctype = if code == 200 && status == "OK" && !body.starts_with("ok") {
        // Prometheus content type per their docs (omit version=0.0.4 charset for simplicity).
        "text/plain; version=0.0.4"
    } else {
        "text/plain"
    };
    format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nContent-Type: {}\r\nConnection: close\r\n\r\n{}",
        code,
        status,
        body.len(),
        ctype,
        body
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_emits_uptime_and_bankroll() {
        let reg = MetricsRegistry::new();
        reg.set(MetricsSnapshot {
            bankroll_usd: 1000.0,
            total_realised_pnl_usd: 0.0,
            ..Default::default()
        });
        let text = render(&reg.snapshot());
        assert!(text.contains("# TYPE bot_uptime_seconds gauge"));
        assert!(text.contains("bot_bankroll_usd 1000"));
        assert!(text.contains("bot_total_realised_pnl_usd 0"));
    }

    #[test]
    fn render_omits_optional_gauges_when_missing() {
        let reg = MetricsRegistry::new();
        reg.set(MetricsSnapshot::default());
        let text = render(&reg.snapshot());
        assert!(!text.contains("bot_btc_mid_usd "));
        assert!(!text.contains("bot_sigma_per_sec "));
        assert!(!text.contains("bot_strike_usd "));
    }

    #[test]
    fn render_emits_optional_gauges_when_present() {
        let reg = MetricsRegistry::new();
        reg.set(MetricsSnapshot {
            latest_btc_mid_usd: Some(100_000.0),
            sigma_per_sec: Some(5e-5),
            latest_strike_usd: Some(100_500.0),
            latest_ttr_secs: Some(60.0),
            ..Default::default()
        });
        let text = render(&reg.snapshot());
        assert!(text.contains("bot_btc_mid_usd 100000"));
        assert!(text.contains("bot_sigma_per_sec 0.00005"));
        assert!(text.contains("bot_strike_usd 100500"));
        assert!(text.contains("bot_ttr_secs 60"));
    }

    #[test]
    fn render_emits_kill_switch_as_zero_one() {
        let reg = MetricsRegistry::new();
        reg.set(MetricsSnapshot {
            kill_switch_tripped: false,
            ..Default::default()
        });
        assert!(render(&reg.snapshot()).contains("bot_kill_switch_tripped 0"));
        reg.update_with(|s| s.kill_switch_tripped = true);
        assert!(render(&reg.snapshot()).contains("bot_kill_switch_tripped 1"));
    }

    #[test]
    fn record_decision_increments_counters() {
        let reg = MetricsRegistry::new();
        reg.record_decision(DecisionKindMetric::Fire);
        reg.record_decision(DecisionKindMetric::Fire);
        reg.record_decision(DecisionKindMetric::NoSignal);
        let snap = reg.snapshot();
        assert_eq!(snap.decisions_fire_total, 2);
        assert_eq!(snap.decisions_no_signal_total, 1);
        assert_eq!(snap.decisions_rejected_total, 0);
        let text = render(&snap);
        assert!(text.contains("bot_decisions_total{kind=\"fire\"} 2"));
        assert!(text.contains("bot_decisions_total{kind=\"no_signal\"} 1"));
    }

    #[test]
    fn render_emits_per_market_pnl_labelled() {
        let reg = MetricsRegistry::new();
        let mut map = HashMap::new();
        map.insert("market-a".to_string(), 1.5);
        map.insert("market-b".to_string(), -0.7);
        reg.set(MetricsSnapshot {
            realised_pnl_per_market_usd: map,
            ..Default::default()
        });
        let text = render(&reg.snapshot());
        assert!(text.contains("bot_realised_pnl_per_market_usd{market_id=\"market-a\"} 1.5"));
        assert!(text.contains("bot_realised_pnl_per_market_usd{market_id=\"market-b\"} -0.7"));
    }

    #[test]
    fn render_escapes_quotes_in_market_id() {
        let reg = MetricsRegistry::new();
        let mut map = HashMap::new();
        // Pathological but possible; Prometheus rules: escape `"` → `\"`.
        map.insert(r#"weird"id"#.to_string(), 0.0);
        reg.set(MetricsSnapshot {
            realised_pnl_per_market_usd: map,
            ..Default::default()
        });
        let text = render(&reg.snapshot());
        assert!(text.contains(r#"market_id="weird\"id""#));
    }

    #[test]
    fn handle_request_returns_metrics_on_metrics_path() {
        let reg = MetricsRegistry::new();
        reg.set(MetricsSnapshot {
            bankroll_usd: 42.0,
            ..Default::default()
        });
        let resp = handle_request("GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n", &reg);
        assert!(resp.starts_with("HTTP/1.1 200 OK"));
        assert!(resp.contains("bot_bankroll_usd 42"));
    }

    #[test]
    fn handle_request_returns_healthz_ok() {
        let reg = MetricsRegistry::new();
        let resp = handle_request("GET /healthz HTTP/1.1\r\n\r\n", &reg);
        assert!(resp.starts_with("HTTP/1.1 200 OK"));
        assert!(resp.ends_with("ok\n"));
    }

    #[test]
    fn handle_request_returns_404_on_unknown_path() {
        let reg = MetricsRegistry::new();
        let resp = handle_request("GET /unknown HTTP/1.1\r\n\r\n", &reg);
        assert!(resp.starts_with("HTTP/1.1 404 Not Found"));
    }

    #[test]
    fn handle_request_returns_405_on_post() {
        let reg = MetricsRegistry::new();
        let resp = handle_request("POST /metrics HTTP/1.1\r\n\r\n", &reg);
        assert!(resp.starts_with("HTTP/1.1 405"));
    }

    #[tokio::test]
    async fn server_serves_metrics_over_real_socket() {
        let reg = MetricsRegistry::new();
        reg.set(MetricsSnapshot {
            bankroll_usd: 999.5,
            ..Default::default()
        });
        // Bind to an ephemeral port so the test is portable.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Re-run the same accept loop with this listener.
        let reg2 = reg.clone();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let n = sock.read(&mut buf).await.unwrap();
            let head = String::from_utf8_lossy(&buf[..n]);
            let resp = handle_request(&head, &reg2);
            sock.write_all(resp.as_bytes()).await.unwrap();
            let _ = sock.shutdown().await;
        });
        // Client side.
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let body = String::from_utf8_lossy(&response);
        assert!(body.contains("bot_bankroll_usd 999.5"));
    }
}
