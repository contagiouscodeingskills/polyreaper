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
// LatencyHistogram
// ---------------------------------------------------------------------------

const LATENCY_BUCKETS: usize = 20;

/// Right-exclusive bucket boundaries in microseconds. Sample with value
/// `v` lands in bucket `i` where `BOUNDARIES[i-1] <= v < BOUNDARIES[i]`
/// (boundary 0 is implicitly 0). Anything `>= 1_000_000` lands in the
/// overflow bucket at index `LATENCY_BUCKETS - 1`.
const LATENCY_BOUNDARIES_US: [u64; 19] = [
    1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000,
    200_000, 500_000, 1_000_000,
];

/// Lock-free latency histogram with fixed 1/2/5×10ⁿ-µs buckets. Cloning
/// shares the underlying counters, mirroring [`Counter`] / [`AtomicTs`].
///
/// **Cumulative since process start, NOT a rolling window.** Every
/// `record_micros` call adds to a bucket counter that is never reset
/// for the lifetime of the recorder process. After a recorder restart
/// the histogram starts empty; the first hour or so of quantile
/// readings will be biased by warm-up samples until enough later
/// samples dominate the distribution. For the Phase 1 soak this is
/// acceptable — a rolling reservoir would need a richer dep we are
/// avoiding for now.
///
/// 19 boundary points cover 1 µs → 1 s in 1/2/5×10ⁿ steps; a 20th
/// "overflow" bucket catches anything `>= 1 s`. Memory: 20 × 8 = 160 B
/// per histogram. Quantile is conservative (returns the bucket *upper*
/// bound) — `quantile_micros(0.99)` returning 1000 means the real p99
/// lies in [500, 1000) µs, never above 1000 µs.
#[derive(Clone, Default, Debug)]
pub struct LatencyHistogram(Arc<[AtomicU64; LATENCY_BUCKETS]>);

impl LatencyHistogram {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one observation. Samples `>= 1 s` land in the overflow
    /// bucket and round-trip through [`Self::quantile_micros`] as
    /// `u64::MAX` — an alarm sentinel.
    pub fn record_micros(&self, us: u64) {
        let idx = LATENCY_BOUNDARIES_US
            .iter()
            .position(|&b| us < b)
            .unwrap_or(LATENCY_BUCKETS - 1);
        self.0[idx].fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the upper bound of the bucket containing the p-th
    /// sample (`p` in [0.0, 1.0]). Conservative — never under-reports.
    /// `None` if no samples have been recorded yet.
    pub fn quantile_micros(&self, p: f64) -> Option<u64> {
        let counts: [u64; LATENCY_BUCKETS] =
            std::array::from_fn(|i| self.0[i].load(Ordering::Relaxed));
        let total: u64 = counts.iter().sum();
        if total == 0 {
            return None;
        }
        let target = ((total as f64) * p).ceil() as u64;
        let mut cum = 0u64;
        for (i, &c) in counts.iter().enumerate() {
            cum += c;
            if cum >= target {
                return Some(if i < LATENCY_BOUNDARIES_US.len() {
                    LATENCY_BOUNDARIES_US[i]
                } else {
                    u64::MAX
                });
            }
        }
        None
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

    #[test]
    fn latency_histogram_records_and_quantiles() {
        let h = LatencyHistogram::new();
        // Empty histogram → no quantile.
        assert_eq!(h.quantile_micros(0.50), None);
        // 99 samples in [50, 100) bucket → upper bound 100.
        for _ in 0..99 {
            h.record_micros(50);
        }
        // 1 sample in [5_000, 10_000) bucket → upper bound 10_000.
        h.record_micros(5_000);
        assert_eq!(h.quantile_micros(0.50), Some(100));
        assert_eq!(h.quantile_micros(0.99), Some(100));
        assert_eq!(h.quantile_micros(1.0), Some(10_000));
    }

    #[test]
    fn latency_histogram_overflow_bucket() {
        let h = LatencyHistogram::new();
        h.record_micros(2_000_000); // >= 1 s → overflow bucket
        assert_eq!(h.quantile_micros(1.0), Some(u64::MAX));
    }
}
