//! CLI smoke test — spawn the `replayer` binary against a known
//! TestDir and confirm stdout matches expectations.
//!
//! Cargo gives us the built binary's path via `CARGO_BIN_EXE_replayer`
//! (the [[bin]] name in Cargo.toml). We don't need to know whether it's
//! a debug or release build — `cargo test` ensures it's compiled before
//! the test runs.
//!
//! Tests stay shallow on purpose: they assert the *shape* of CLI
//! output (counts, line counts, exit codes), not the per-byte format —
//! that's covered by unit + round-trip tests in `crates/replayer`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use common::{LocalTimestamp, RawEvent, Venue};
use storage::Store;

// ---------------------------------------------------------------------------
// Helpers (TestDir mirrors crates/storage/src/lib.rs:389-410)
// ---------------------------------------------------------------------------

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let ptr = &nanos as *const _ as usize;
        let dir = std::env::temp_dir().join(format!("polybot_cli_{nanos}_{ptr:x}"));
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

fn cfg(base: PathBuf) -> config::StorageConfig {
    config::StorageConfig {
        base_dir: base,
        rotate_minutes: 0,
        fsync_on_write: false,
    }
}

fn ev(venue: Venue, stream: &str, ts: u128, payload: &str) -> RawEvent {
    RawEvent {
        venue,
        stream: stream.into(),
        local_ts_ns: LocalTimestamp::from_nanos(ts),
        venue_ts_ms: None,
        payload: payload.into(),
    }
}

/// Path of the `replayer` binary built for this test run.
fn replayer_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_replayer"))
}

/// Write a known fixture into a fresh TestDir and return both. Caller
/// holds the TestDir to keep it alive for the test's duration.
fn fixture() -> (TestDir, PathBuf) {
    let tmp = TestDir::new();
    let mut store = Store::open(&cfg(tmp.path().to_path_buf())).unwrap();

    // 7 events: 4 binance trade, 2 binance depth, 1 polymarket.
    for ts in [100u128, 200, 300, 400] {
        store
            .write(&ev(Venue::Binance, "btcusdt@trade", ts, r#"{"e":"trade"}"#))
            .unwrap();
    }
    for ts in [110u128, 210] {
        store
            .write(&ev(
                Venue::Binance,
                "btcusdt@depth@100ms",
                ts,
                r#"{"e":"depthUpdate"}"#,
            ))
            .unwrap();
    }
    store
        .write(&ev(Venue::Polymarket, "sample-mkt", 150, r#"{"x":1}"#))
        .unwrap();
    store.flush_all().unwrap();

    let session_dir = store.session_dir().to_path_buf();
    drop(store);
    (tmp, session_dir)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn count_with_no_filter_matches_total_events() {
    let (_tmp, session) = fixture();
    let out = Command::new(replayer_bin())
        .args(["count", "--root"])
        .arg(&session)
        .output()
        .expect("spawn replayer");
    assert!(
        out.status.success(),
        "exit {:?}, stderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert_eq!(stdout.trim(), "7");
}

#[test]
fn count_filtered_by_venue_drops_other_venue() {
    let (_tmp, session) = fixture();
    let out = Command::new(replayer_bin())
        .args(["count", "--root"])
        .arg(&session)
        .args(["--venue", "binance"])
        .output()
        .expect("spawn replayer");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert_eq!(stdout.trim(), "6");
}

#[test]
fn count_filtered_by_stream_prefix_picks_family() {
    let (_tmp, session) = fixture();
    let out = Command::new(replayer_bin())
        .args(["count", "--root"])
        .arg(&session)
        .args(["--venue", "binance", "--stream-prefix", "btcusdt@trade"])
        .output()
        .expect("spawn replayer");
    assert!(out.status.success());
    assert_eq!(String::from_utf8(out.stdout).unwrap().trim(), "4");
}

#[test]
fn head_emits_n_ndjson_lines_in_order() {
    let (_tmp, session) = fixture();
    let out = Command::new(replayer_bin())
        .args(["head", "--root"])
        .arg(&session)
        .args(["--venue", "binance", "--stream-prefix", "btcusdt@trade", "-n", "2"])
        .output()
        .expect("spawn replayer");
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let lines: Vec<_> = std::str::from_utf8(&out.stdout)
        .unwrap()
        .lines()
        .collect();
    assert_eq!(lines.len(), 2);
    // Each line is JSON; first should have local_ts_ns "100", second "200".
    assert!(lines[0].contains(r#""local_ts_ns":"100""#), "got {}", lines[0]);
    assert!(lines[1].contains(r#""local_ts_ns":"200""#), "got {}", lines[1]);
}

#[test]
fn sessions_lists_the_fixture_dir() {
    let (tmp, _session) = fixture();
    let out = Command::new(replayer_bin())
        .args(["sessions", "--root"])
        .arg(tmp.path())
        .output()
        .expect("spawn replayer");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    // Header line + at least one session line.
    let nonblank: Vec<_> = stdout.lines().filter(|l| !l.is_empty()).collect();
    assert!(nonblank.len() >= 2, "got: {stdout}");
    assert!(nonblank[0].contains("session"), "header missing: {}", nonblank[0]);
}

#[test]
fn schema_subcommand_prints_known_fields() {
    let out = Command::new(replayer_bin())
        .args(["schema"])
        .output()
        .expect("spawn replayer");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    for f in ["venue", "stream", "local_ts_ns", "venue_ts_ms", "payload"] {
        assert!(stdout.contains(f), "schema missing field {f}:\n{stdout}");
    }
    assert!(stdout.contains("Decimal128(38, 0)"));
}

#[test]
fn dump_writes_parquet_and_reports_row_count() {
    let (_tmp, session) = fixture();
    let out_dir = TestDir::new();
    let parquet_path = out_dir.path().join("out.parquet");
    let out = Command::new(replayer_bin())
        .args(["dump", "--root"])
        .arg(&session)
        .args(["--out"])
        .arg(&parquet_path)
        .output()
        .expect("spawn replayer");
    assert!(
        out.status.success(),
        "exit {:?}, stderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("wrote 7 rows"), "stderr: {stderr}");
    let size = std::fs::metadata(&parquet_path).unwrap().len();
    assert!(size > 0, "parquet file is empty");
}

#[test]
fn unknown_command_exits_with_code_2() {
    let out = Command::new(replayer_bin())
        .args(["bogus"])
        .output()
        .expect("spawn replayer");
    assert_eq!(
        out.status.code(),
        Some(2),
        "expected exit 2, got {:?}",
        out.status.code()
    );
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("unknown command"), "stderr: {stderr}");
}

#[test]
fn missing_root_exits_with_code_2() {
    let out = Command::new(replayer_bin())
        .args(["count"])
        .output()
        .expect("spawn replayer");
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn integrity_text_runs_and_prints_sections() {
    let (_tmp, session) = fixture();
    let out = Command::new(replayer_bin())
        .args(["integrity", "--root"])
        .arg(&session)
        .output()
        .expect("spawn replayer");
    assert!(
        out.status.success(),
        "exit {:?}, stderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8(out.stdout).unwrap();
    for section in [
        "scanned",
        "per-venue",
        "structural",
        "decoder",
        "sequence integrity",
        "Binance depth chain breaks",
    ] {
        assert!(stdout.contains(section), "missing {section:?}:\n{stdout}");
    }
}

#[test]
fn integrity_json_emits_one_object_per_session() {
    let (_tmp, session) = fixture();
    let out = Command::new(replayer_bin())
        .args(["integrity"])
        .arg("--root")
        .arg(&session)
        .arg("--json")
        .output()
        .expect("spawn replayer");
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8(out.stdout).unwrap();
    let trimmed = stdout.trim();
    let parsed: serde_json::Value = serde_json::from_str(trimmed).expect("valid JSON");
    assert!(parsed.get("session_name").is_some(), "missing session_name: {trimmed}");
    assert!(parsed.get("structural").is_some());
    assert!(parsed.get("decoder").is_some());
    assert!(parsed.get("sequence").is_some());
}
