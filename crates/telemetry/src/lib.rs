//! Recorder-phase observability.
//!
//! Provides:
//! * [`init`] — installs a global `tracing` subscriber driven by
//!   [`config::TelemetryConfig`].
//! * [`Counter`] — cheap `Arc<AtomicU64>` wrapper so components can share
//!   running counts (messages received, parse failures, reconnects, …)
//!   without pulling in a metrics framework.
//!
//! # Structured logging conventions
//!
//! Every crate that emits events should attach a consistent set of fields
//! so logs stay filterable when they reach a collector:
//!
//! | field       | meaning                                                   |
//! |-------------|-----------------------------------------------------------|
//! | `component` | emitting crate, e.g. `"binance_feed"`                     |
//! | `venue`     | `"binance"` or `"polymarket"`                             |
//! | `event`     | short kind tag, e.g. `"reconnect"`, `"parse_failure"`     |
//! | `reason`    | human reason for reconnects and parse failures            |
//! | `raw`       | raw payload for parse failures (truncate to ~512 bytes)   |
//!
//! # Event kinds the recorder is expected to emit
//!
//! * `reconnect`     — before/after a websocket reconnect
//! * `parse_failure` — whenever a payload cannot be parsed
//! * `message`       — per inbound message, usually TRACE/DEBUG
//! * `health`        — periodic counters snapshot per component

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub const NAME: &str = "telemetry";

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

/// Handle returned by [`init`]. Drop to flush any future background writers.
///
/// Currently carries no state — reserved so we can add non-blocking writer
/// guards (e.g. `tracing_appender::non_blocking`) later without breaking the
/// public API.
#[derive(Debug)]
pub struct TelemetryGuard {
    _private: (),
}

/// Install a global `tracing` subscriber.
///
/// Must be called exactly once per process. A second call returns
/// [`TelemetryError::AlreadyInitialized`]; use that as a signal, not a panic.
pub fn init(cfg: &config::TelemetryConfig) -> Result<TelemetryGuard, TelemetryError> {
    let filter = tracing_subscriber::EnvFilter::try_new(&cfg.log_level).map_err(|e| {
        TelemetryError::BadFilter {
            filter: cfg.log_level.clone(),
            detail: e.to_string(),
        }
    })?;

    match cfg.log_format {
        config::LogFormat::Pretty => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .try_init()
            .map_err(|e| TelemetryError::AlreadyInitialized(e.to_string()))?,
        config::LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_target(true)
            .try_init()
            .map_err(|e| TelemetryError::AlreadyInitialized(e.to_string()))?,
    }

    Ok(TelemetryGuard { _private: () })
}

// ---------------------------------------------------------------------------
// Counter
// ---------------------------------------------------------------------------

/// Shared atomic counter. Cloning shares the underlying count.
///
/// Intended usage: a feed handler owns several counters, clones them into
/// worker tasks, and reads them from a periodic health-report task.
///
/// Uses `Relaxed` ordering — we only need eventual visibility of counts, not
/// happens-before ordering against other memory.
#[derive(Clone, Default, Debug)]
pub struct Counter(Arc<AtomicU64>);

impl Counter {
    pub fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    pub fn incr(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn incr_by(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.0.store(0, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// AtomicTs
// ---------------------------------------------------------------------------

/// Shared atomic timestamp (nanoseconds since UNIX epoch). Cloning shares
/// the underlying value, mirroring [`Counter`].
///
/// `set_ns` truncates its `u128` argument to `u64`, which is safe for any
/// time we'll see in practice (`u64` ns covers 1970 → year ~2554). `0`
/// is the "never set" sentinel — callers map it to `None` at the
/// serialization boundary.
///
/// Uses `Relaxed` ordering: only eventual visibility is needed, no
/// happens-before relationship to other memory.
#[derive(Clone, Default, Debug)]
pub struct AtomicTs(Arc<AtomicU64>);

impl AtomicTs {
    pub fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    pub fn set_ns(&self, ns: u128) {
        self.0.store(ns as u64, Ordering::Relaxed);
    }

    pub fn get_ns(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("invalid log filter {filter:?}: {detail}")]
    BadFilter { filter: String, detail: String },

    #[error("tracing subscriber already installed: {0}")]
    AlreadyInitialized(String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_basic_arithmetic() {
        let c = Counter::new();
        assert_eq!(c.get(), 0);
        c.incr();
        c.incr_by(4);
        assert_eq!(c.get(), 5);
        c.reset();
        assert_eq!(c.get(), 0);
    }

    #[test]
    fn counter_clone_shares_state() {
        let a = Counter::new();
        let b = a.clone();
        a.incr();
        b.incr_by(9);
        assert_eq!(a.get(), 10);
        assert_eq!(b.get(), 10);
    }

    #[test]
    fn bad_filter_returns_error() {
        // A bare `=` with nothing on either side is not a valid EnvFilter.
        let cfg = config::TelemetryConfig {
            log_level: "=".to_string(),
            log_format: config::LogFormat::Pretty,
        };
        let err = init(&cfg).unwrap_err();
        assert!(matches!(err, TelemetryError::BadFilter { .. }));
    }
}
