//! Recorder-phase configuration.
//!
//! Load order: TOML file -> env overrides (small whitelist) -> validation.
//! The loader fails fast on missing files, bad TOML, and invalid values so
//! the recorder never starts against a silently broken config.
//!
//! Strategy / execution / risk config deliberately live elsewhere and are
//! not part of Phase 1.

use std::path::{Path, PathBuf};

use serde::Deserialize;

pub const NAME: &str = "config";

// ---------------------------------------------------------------------------
// Top-level
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub app: AppConfig,
    pub telemetry: TelemetryConfig,
    pub storage: StorageConfig,
    pub binance_feed: BinanceFeedConfig,
    pub polymarket_feed: PolymarketFeedConfig,
    pub market_discovery: MarketDiscoveryConfig,
}

// ---------------------------------------------------------------------------
// Sections
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    /// Free-form environment label ("dev", "staging", "prod"). Used only for
    /// log/metric tagging, not for behaviour branching.
    pub environment: String,
    /// How long the recorder waits for in-flight writes to flush on SIGINT
    /// before force-exit.
    pub shutdown_grace_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryConfig {
    /// `tracing` EnvFilter syntax, e.g. "info" or "recorder=debug,info".
    pub log_level: String,
    pub log_format: LogFormat,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Pretty,
    Json,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Base directory for raw event recordings. Created lazily by the
    /// storage crate; config only carries the path.
    pub base_dir: PathBuf,
    /// Rotate the active output file every N minutes. `0` disables rotation.
    pub rotate_minutes: u64,
    /// Call fsync after every write. Costs throughput, buys durability.
    pub fsync_on_write: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BinanceFeedConfig {
    /// Binance spot combined-stream websocket endpoint.
    pub ws_url: String,
    /// Stream names, Binance wire format
    /// (e.g. "btcusdt@trade", "btcusdt@depth@100ms").
    pub streams: Vec<String>,
    /// Reconnect if no inbound bytes arrive for this long.
    pub read_idle_secs: u64,
    pub reconnect: ReconnectBackoff,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolymarketFeedConfig {
    // TODO verify endpoint: current best guess is the CLOB "market" channel
    // at wss://ws-subscriptions-clob.polymarket.com/ws/market. Confirm
    // against live behaviour before relying on this.
    pub ws_url: String,
    pub read_idle_secs: u64,
    pub reconnect: ReconnectBackoff,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDiscoveryConfig {
    /// Polymarket Gamma `/events` endpoint URL.
    pub gamma_url: String,
    /// How often we re-poll the discovery endpoint for new markets.
    pub poll_interval_secs: u64,
    /// Polymarket "series" slug to filter to. Each series is a recurring
    /// template (e.g. `"btc-up-or-down-5m"`). Discovery keeps only events
    /// whose `series[*].slug` matches this value.
    pub series_slug: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconnectBackoff {
    pub initial_ms: u64,
    pub max_ms: u64,
    pub multiplier: f64,
}

// ---------------------------------------------------------------------------
// Loader
// ---------------------------------------------------------------------------

impl Config {
    /// Read a TOML file, apply env overrides, then validate.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_toml_str(&raw)
    }

    /// Parse a TOML string, apply env overrides, then validate. Exposed so
    /// tests and tooling don't need a file on disk.
    pub fn from_toml_str(raw: &str) -> Result<Self, ConfigError> {
        let mut cfg: Config = toml::from_str(raw)?;
        cfg.apply_env_overrides();
        cfg.validate()?;
        Ok(cfg)
    }

    fn apply_env_overrides(&mut self) {
        // Small whitelist. Prefix POLYBOT_ to avoid colliding with ambient
        // tooling env. Everything else requires editing the file.
        if let Ok(v) = std::env::var("POLYBOT_LOG_LEVEL") {
            self.telemetry.log_level = v;
        }
        if let Ok(v) = std::env::var("POLYBOT_DATA_DIR") {
            self.storage.base_dir = PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("POLYBOT_BINANCE_WS_URL") {
            self.binance_feed.ws_url = v;
        }
        if let Ok(v) = std::env::var("POLYBOT_POLYMARKET_WS_URL") {
            self.polymarket_feed.ws_url = v;
        }
        if let Ok(v) = std::env::var("POLYBOT_POLYMARKET_GAMMA_URL") {
            self.market_discovery.gamma_url = v;
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        fn require(cond: bool, msg: impl Into<String>) -> Result<(), ConfigError> {
            if cond {
                Ok(())
            } else {
                Err(ConfigError::Validate(msg.into()))
            }
        }

        require(
            !self.app.environment.trim().is_empty(),
            "app.environment must be non-empty",
        )?;

        require(
            !self.telemetry.log_level.trim().is_empty(),
            "telemetry.log_level must be non-empty",
        )?;

        require(
            !self.storage.base_dir.as_os_str().is_empty(),
            "storage.base_dir must be non-empty",
        )?;

        check_ws_url(&self.binance_feed.ws_url, "binance_feed.ws_url")?;
        require(
            !self.binance_feed.streams.is_empty(),
            "binance_feed.streams must list at least one stream",
        )?;
        require(
            self.binance_feed.read_idle_secs > 0,
            "binance_feed.read_idle_secs must be > 0",
        )?;
        check_backoff(&self.binance_feed.reconnect, "binance_feed.reconnect")?;

        check_ws_url(&self.polymarket_feed.ws_url, "polymarket_feed.ws_url")?;
        require(
            self.polymarket_feed.read_idle_secs > 0,
            "polymarket_feed.read_idle_secs must be > 0",
        )?;
        check_backoff(&self.polymarket_feed.reconnect, "polymarket_feed.reconnect")?;

        check_http_url(&self.market_discovery.gamma_url, "market_discovery.gamma_url")?;
        require(
            self.market_discovery.poll_interval_secs > 0,
            "market_discovery.poll_interval_secs must be > 0",
        )?;
        require(
            !self.market_discovery.series_slug.trim().is_empty(),
            "market_discovery.series_slug must be non-empty",
        )?;

        Ok(())
    }
}

fn check_ws_url(url: &str, field: &str) -> Result<(), ConfigError> {
    if url.starts_with("ws://") || url.starts_with("wss://") {
        Ok(())
    } else {
        Err(ConfigError::Validate(format!(
            "{field} must start with ws:// or wss:// (got {url:?})"
        )))
    }
}

fn check_http_url(url: &str, field: &str) -> Result<(), ConfigError> {
    if url.starts_with("http://") || url.starts_with("https://") {
        Ok(())
    } else {
        Err(ConfigError::Validate(format!(
            "{field} must start with http:// or https:// (got {url:?})"
        )))
    }
}

fn check_backoff(b: &ReconnectBackoff, field: &str) -> Result<(), ConfigError> {
    if b.initial_ms == 0 {
        return Err(ConfigError::Validate(format!("{field}.initial_ms must be > 0")));
    }
    if b.max_ms < b.initial_ms {
        return Err(ConfigError::Validate(format!(
            "{field}.max_ms ({}) must be >= initial_ms ({})",
            b.max_ms, b.initial_ms
        )));
    }
    if !(b.multiplier > 1.0) {
        return Err(ConfigError::Validate(format!(
            "{field}.multiplier must be > 1.0 (got {})",
            b.multiplier
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse config TOML: {0}")]
    Parse(#[from] toml::de::Error),

    #[error("invalid config: {0}")]
    Validate(String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
[app]
environment = "dev"
shutdown_grace_secs = 10

[telemetry]
log_level = "info"
log_format = "pretty"

[storage]
base_dir = "./data"
rotate_minutes = 60
fsync_on_write = false

[binance_feed]
ws_url = "wss://stream.binance.com:443/ws"
streams = ["btcusdt@trade", "btcusdt@depth@100ms"]
read_idle_secs = 30
[binance_feed.reconnect]
initial_ms = 500
max_ms = 30000
multiplier = 2.0

[polymarket_feed]
ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
read_idle_secs = 30
[polymarket_feed.reconnect]
initial_ms = 500
max_ms = 30000
multiplier = 2.0

[market_discovery]
gamma_url = "https://gamma-api.polymarket.com/events"
poll_interval_secs = 15
series_slug = "btc-up-or-down-5m"
"#;

    #[test]
    fn sample_parses_and_validates() {
        let cfg = Config::from_toml_str(SAMPLE).expect("sample must load");
        assert_eq!(cfg.app.environment, "dev");
        assert_eq!(cfg.telemetry.log_format, LogFormat::Pretty);
        assert_eq!(cfg.binance_feed.streams.len(), 2);
    }

    #[test]
    fn rejects_non_ws_binance_url() {
        let bad = SAMPLE.replace(
            "wss://stream.binance.com:443/ws",
            "https://stream.binance.com/ws",
        );
        let err = Config::from_toml_str(&bad).unwrap_err();
        assert!(matches!(err, ConfigError::Validate(_)));
    }

    #[test]
    fn rejects_bad_backoff() {
        let bad = SAMPLE.replace("multiplier = 2.0", "multiplier = 1.0");
        let err = Config::from_toml_str(&bad).unwrap_err();
        assert!(matches!(err, ConfigError::Validate(_)));
    }

    #[test]
    fn rejects_unknown_field() {
        let bad = SAMPLE.replace("[app]", "[app]\nmystery_knob = 42");
        let err = Config::from_toml_str(&bad).unwrap_err();
        assert!(matches!(err, ConfigError::Parse(_)));
    }
}
