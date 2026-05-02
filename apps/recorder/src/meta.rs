//! Per-session metadata sidecar.
//!
//! Writes one file per session:
//!
//! `<session_dir>/_session_meta.json`
//!
//! Contains everything needed to attribute a recorded session to a specific
//! recorder build, configuration, and host. Captured once at session open;
//! never appended to.
//!
//! Why a separate file (not a `RawEvent`):
//! * It's metadata about the *capture*, not a market event. Mixing it into
//!   the venue stream would muddy replay.
//! * The file is small (~1 KB) and read-once by analysis tools, so the
//!   leading-underscore-sidecar convention used by `_health.ndjson` fits.
//!
//! Why this matters for analysis:
//! Multi-month captures span recorder versions and config changes. Without
//! a per-session metadata stamp, you can't reconstruct what the recorder
//! was doing at any given moment — a config flip silently changes
//! semantics across sessions.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

/// Compile-time git revision, if injected via the build environment.
/// Set with e.g. `GIT_REV=$(git rev-parse --short HEAD) cargo build --release`.
/// Returns `None` when the env var was unset at build time, in which case
/// the field is omitted from the metadata output.
fn git_rev() -> Option<&'static str> {
    option_env!("GIT_REV")
}

#[derive(Serialize)]
struct SessionMeta<'a> {
    /// Schema version for this metadata file. Bump when the shape changes.
    schema_version: u32,
    recorder_version: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    git_rev: Option<&'a str>,
    /// Wall-clock at session open, ns since UNIX epoch. Stringified for the
    /// same precision-preserving reason `RawEvent.local_ts_ns` is.
    started_at_ns: String,
    /// ISO-8601 form of `started_at_ns` (UTC), for human inspection.
    started_at_iso: String,
    /// Session directory path the recorder used.
    session_dir: String,
    /// Path to the config file this run was loaded from.
    config_path: String,
    app: AppMeta<'a>,
    storage: StorageMeta<'a>,
    feeds: FeedsMeta<'a>,
    process: ProcessMeta,
}

#[derive(Serialize)]
struct AppMeta<'a> {
    environment: &'a str,
    shutdown_grace_secs: u64,
}

#[derive(Serialize)]
struct StorageMeta<'a> {
    base_dir: &'a str,
    rotate_minutes: u64,
    fsync_on_write: bool,
}

#[derive(Serialize)]
struct FeedsMeta<'a> {
    binance: BinanceMeta<'a>,
    polymarket: PolymarketMeta<'a>,
    coinbase: CoinbaseMeta<'a>,
    chainlink: ChainlinkMeta<'a>,
}

#[derive(Serialize)]
struct BinanceMeta<'a> {
    enabled: bool,
    ws_url: &'a str,
    streams: &'a [String],
    read_idle_secs: u64,
}

#[derive(Serialize)]
struct PolymarketMeta<'a> {
    enabled: bool,
    ws_url: &'a str,
    read_idle_secs: u64,
    series_slug: &'a str,
    gamma_url: &'a str,
    poll_interval_secs: u64,
}

#[derive(Serialize)]
struct CoinbaseMeta<'a> {
    enabled: bool,
    ws_url: &'a str,
    product_ids: &'a [String],
    channel: &'a str,
    read_idle_secs: u64,
}

#[derive(Serialize)]
struct ChainlinkMeta<'a> {
    /// Always false in current builds; the on-chain Chainlink feed is
    /// disabled (see comment in `apps/recorder/src/main.rs`). Field is
    /// kept so analysis can detect when it ever flips back on.
    enabled: bool,
    ws_url: &'a str,
    contract_address: &'a str,
}

#[derive(Serialize)]
struct ProcessMeta {
    pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    hostname: Option<String>,
}

const META_SCHEMA_VERSION: u32 = 1;
const META_FILENAME: &str = "_session_meta.json";

/// Write `<session_dir>/_session_meta.json` exactly once. Overwrites any
/// existing file at the same path (sessions are uniquely named per recorder
/// run, so this is only relevant if the recorder is started twice into
/// the same directory — which the storage layer does not do).
pub fn write_session_meta(
    session_dir: &Path,
    config_path: &Path,
    cfg: &config::Config,
) -> std::io::Result<()> {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let started_at_iso = iso_utc_from_ns(now_ns);

    let meta = SessionMeta {
        schema_version: META_SCHEMA_VERSION,
        recorder_version: env!("CARGO_PKG_VERSION"),
        git_rev: git_rev(),
        started_at_ns: now_ns.to_string(),
        started_at_iso,
        session_dir: session_dir.display().to_string(),
        config_path: config_path.display().to_string(),
        app: AppMeta {
            environment: &cfg.app.environment,
            shutdown_grace_secs: cfg.app.shutdown_grace_secs,
        },
        storage: StorageMeta {
            base_dir: cfg.storage.base_dir.to_str().unwrap_or(""),
            rotate_minutes: cfg.storage.rotate_minutes,
            fsync_on_write: cfg.storage.fsync_on_write,
        },
        feeds: FeedsMeta {
            binance: BinanceMeta {
                enabled: true,
                ws_url: &cfg.binance_feed.ws_url,
                streams: &cfg.binance_feed.streams,
                read_idle_secs: cfg.binance_feed.read_idle_secs,
            },
            polymarket: PolymarketMeta {
                enabled: true,
                ws_url: &cfg.polymarket_feed.ws_url,
                read_idle_secs: cfg.polymarket_feed.read_idle_secs,
                series_slug: &cfg.market_discovery.series_slug,
                gamma_url: &cfg.market_discovery.gamma_url,
                poll_interval_secs: cfg.market_discovery.poll_interval_secs,
            },
            coinbase: CoinbaseMeta {
                enabled: true,
                ws_url: &cfg.coinbase_feed.ws_url,
                product_ids: &cfg.coinbase_feed.product_ids,
                channel: &cfg.coinbase_feed.channel,
                read_idle_secs: cfg.coinbase_feed.read_idle_secs,
            },
            chainlink: ChainlinkMeta {
                enabled: false,
                ws_url: &cfg.chainlink_feed.ws_url,
                contract_address: &cfg.chainlink_feed.contract_address,
            },
        },
        process: ProcessMeta {
            pid: std::process::id(),
            hostname: hostname(),
        },
    };

    let path = session_dir.join(META_FILENAME);
    let body = serde_json::to_vec_pretty(&meta).expect("session meta serialises");
    std::fs::write(&path, body)?;
    Ok(())
}

/// `gethostname()` via the `HOSTNAME` env var or `/etc/hostname`. Avoids a
/// crate dep for one field. Returns `None` on any error so the metadata
/// file always lands.
fn hostname() -> Option<String> {
    if let Ok(h) = std::env::var("HOSTNAME") {
        if !h.is_empty() {
            return Some(h);
        }
    }
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Format `ns since epoch` as ISO-8601 UTC `YYYY-MM-DDTHH:MM:SS.mmmZ`.
/// Implemented in-tree (no chrono dep) using the same Howard-Hinnant
/// conversion the storage crate uses for session-dir names.
fn iso_utc_from_ns(ns: u128) -> String {
    let secs = (ns / 1_000_000_000) as u64;
    let millis = ((ns / 1_000_000) % 1_000) as u64;
    let (y, mo, d, h, mi, s) = epoch_secs_to_utc(secs);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{millis:03}Z")
}

fn epoch_secs_to_utc(secs: u64) -> (i64, u32, u32, u32, u32, u32) {
    let days = (secs / 86_400) as i64;
    let sod = secs % 86_400;
    let hour = (sod / 3_600) as u32;
    let min = ((sod / 60) % 60) as u32;
    let sec = (sod % 60) as u32;

    let z = days + 719_468;
    let era = if z >= 0 { z / 146_097 } else { (z - 146_096) / 146_097 };
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };

    (y, m, d, hour, min, sec)
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
            let dir = std::env::temp_dir().join(format!("polybot_meta_test_{nanos}_{ptr:x}"));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn sample_cfg() -> config::Config {
        let toml = r#"
[app]
environment = "test"
shutdown_grace_secs = 5

[telemetry]
log_level = "info"
log_format = "json"

[storage]
base_dir = "./data"
rotate_minutes = 60
fsync_on_write = false

[binance_feed]
ws_url = "wss://stream.binance.com:9443/ws"
streams = ["btcusdt@trade", "btcusdt@bookTicker"]
read_idle_secs = 30
[binance_feed.reconnect]
initial_ms = 500
max_ms = 30000
multiplier = 2.0

[polymarket_feed]
ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
read_idle_secs = 60
[polymarket_feed.reconnect]
initial_ms = 500
max_ms = 30000
multiplier = 2.0

[market_discovery]
gamma_url = "https://gamma-api.polymarket.com/events"
poll_interval_secs = 15
series_slug = "btc-up-or-down-5m"

[coinbase_feed]
ws_url = "wss://advanced-trade-ws.coinbase.com"
product_ids = ["BTC-USD"]
channel = "market_trades"
read_idle_secs = 60
[coinbase_feed.reconnect]
initial_ms = 500
max_ms = 30000
multiplier = 2.0

[chainlink_feed]
ws_url = "wss://ethereum-rpc.publicnode.com"
contract_address = "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c"
read_idle_secs = 60
[chainlink_feed.reconnect]
initial_ms = 500
max_ms = 30000
multiplier = 2.0
"#;
        config::Config::from_toml_str(toml).unwrap()
    }

    #[test]
    fn writes_meta_file_with_expected_shape() {
        let tmp = TestDir::new();
        let cfg = sample_cfg();
        let cfg_path = std::path::Path::new("/dev/null/recorder.toml");

        write_session_meta(tmp.path(), cfg_path, &cfg).unwrap();

        let body = std::fs::read_to_string(tmp.path().join(META_FILENAME)).unwrap();
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();

        assert_eq!(v["schema_version"], 1);
        assert_eq!(v["recorder_version"], env!("CARGO_PKG_VERSION"));
        assert!(v["started_at_ns"].as_str().unwrap().chars().all(|c| c.is_ascii_digit()));
        assert!(v["started_at_iso"]
            .as_str()
            .unwrap()
            .ends_with('Z'));
        assert_eq!(v["session_dir"], tmp.path().display().to_string());
        assert_eq!(v["app"]["environment"], "test");
        assert_eq!(v["storage"]["rotate_minutes"], 60);
        assert_eq!(v["storage"]["fsync_on_write"], false);
        assert_eq!(v["feeds"]["binance"]["enabled"], true);
        assert_eq!(v["feeds"]["chainlink"]["enabled"], false);
        assert_eq!(v["feeds"]["polymarket"]["series_slug"], "btc-up-or-down-5m");
        assert!(v["feeds"]["binance"]["streams"].as_array().unwrap().len() >= 2);
        assert!(v["process"]["pid"].as_u64().unwrap() > 0);
    }

    #[test]
    fn iso_format_matches_known_point() {
        // 2026-04-30 04:39:31.487 UTC
        let ns: u128 = 1_777_523_971_487_807_791;
        let iso = iso_utc_from_ns(ns);
        assert_eq!(iso, "2026-04-30T04:39:31.487Z");
    }

    #[test]
    fn epoch_to_utc_known_points() {
        assert_eq!(epoch_secs_to_utc(0), (1970, 1, 1, 0, 0, 0));
        assert_eq!(epoch_secs_to_utc(1_582_934_400), (2020, 2, 29, 0, 0, 0));
    }

    #[test]
    fn meta_file_is_valid_json_when_optional_fields_absent() {
        let tmp = TestDir::new();
        let cfg = sample_cfg();
        write_session_meta(tmp.path(), std::path::Path::new("x.toml"), &cfg).unwrap();
        let body = std::fs::read_to_string(tmp.path().join(META_FILENAME)).unwrap();
        // git_rev defaults to None when GIT_REV is unset at build time -
        // omitted, not null. Same for hostname when env+/etc/hostname
        // both fail. The JSON must still round-trip.
        let _v: serde_json::Value = serde_json::from_str(&body).unwrap();
    }
}
