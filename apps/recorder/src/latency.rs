//! Cross-venue latency probe.
//!
//! Periodically opens a TCP connect to each venue's host:port and logs
//! the time-to-establish. This is a *floor* on one-way latency — actual
//! WebSocket/HTTP latency includes TLS handshake + protocol overhead, but
//! the TCP connect is enough to detect routing changes, regional failover,
//! and gross VPS-to-venue path issues.
//!
//! Output goes to journald only (no storage write). Researchers wanting
//! a long-term series can `journalctl -u polybot-recorder | grep latency_probe`.
//!
//! When this is useful:
//! * Cross-venue clock alignment (Binance Tokyo vs Polymarket US can be
//!   very different from a Frankfurt VPS).
//! * Detecting CDN region changes — sudden jumps in RTT suggest the venue
//!   started routing us to a different POP.
//! * Distinguishing a genuine venue outage from a network blip.

use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::time::timeout;

const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

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

pub async fn run_latency_probe_loop(interval: Duration) {
    loop {
        for (label, addr) in TARGETS {
            probe_once(label, addr).await;
        }
        tokio::time::sleep(interval).await;
    }
}

async fn probe_once(label: &str, addr: &str) {
    let start = Instant::now();
    let outcome = timeout(PROBE_TIMEOUT, TcpStream::connect(addr)).await;
    let elapsed_ms = start.elapsed().as_millis() as u64;

    match outcome {
        Ok(Ok(_stream)) => tracing::info!(
            component = "recorder",
            event = "latency_probe",
            target = label,
            addr = addr,
            rtt_ms = elapsed_ms,
            "tcp connect ok"
        ),
        Ok(Err(e)) => tracing::warn!(
            component = "recorder",
            event = "latency_probe_failed",
            target = label,
            addr = addr,
            reason = %e,
            "tcp connect error"
        ),
        Err(_) => tracing::warn!(
            component = "recorder",
            event = "latency_probe_timeout",
            target = label,
            addr = addr,
            timeout_ms = PROBE_TIMEOUT.as_millis() as u64,
            "tcp connect timed out"
        ),
    }
}
