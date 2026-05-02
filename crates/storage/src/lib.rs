//! Recorder-phase raw event persistence.
//!
//! Layout:
//! ```text
//! {base_dir}/session_{UTC_YYYYMMDDTHHMMSSZ}/
//!     {venue}/
//!         {sanitized_stream}.ndjson             # rotate_minutes == 0
//!         {sanitized_stream}.{bucket:04}.ndjson # rotate_minutes > 0
//! ```
//!
//! Format: one JSON object per line (NDJSON). Each record is a serialised
//! [`common::RawEvent`] — the replay contract lives in the `common` crate.
//!
//! Rotation: when `rotate_minutes > 0`, each [`StreamWriter`] closes the
//! current file and opens a new one when the session-local bucket index
//! advances. Bucket indexing is deliberately session-relative rather than
//! wall-clock aligned — simpler to reason about inside a single process.
//!
//! Out of scope for Phase 1: compression, binary frame support, index
//! files, multi-process coordination, async I/O.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::{RawEvent, Venue};

pub const NAME: &str = "storage";

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/// Top-level store. Owns the session directory and a writer per
/// `(venue, stream)` pair. Not thread-safe; wrap in `Arc<Mutex<_>>` for
/// shared use (see `docs/TECH_DEBT.md` §3).
pub struct Store {
    session_dir: PathBuf,
    fsync_on_write: bool,
    rotate: Option<Duration>,
    session_start: Instant,
    writers: HashMap<(Venue, String), StreamWriter>,
}

impl Store {
    /// Create the session directory under `config.base_dir` and return a
    /// fresh store. Does not touch per-stream files — those are opened
    /// lazily on first [`write`](Self::write).
    pub fn open(cfg: &config::StorageConfig) -> Result<Self, StorageError> {
        std::fs::create_dir_all(&cfg.base_dir).map_err(|source| StorageError::Io {
            path: cfg.base_dir.clone(),
            source,
        })?;

        let session_name = format!("session_{}", utc_compact_now());
        let session_dir = cfg.base_dir.join(&session_name);
        std::fs::create_dir_all(&session_dir).map_err(|source| StorageError::Io {
            path: session_dir.clone(),
            source,
        })?;

        let rotate = if cfg.rotate_minutes == 0 {
            None
        } else {
            Some(Duration::from_secs(cfg.rotate_minutes * 60))
        };

        tracing::info!(
            component = "storage",
            event = "session_open",
            session_dir = %session_dir.display(),
            rotate_minutes = cfg.rotate_minutes,
            fsync_on_write = cfg.fsync_on_write,
            "opened recorder session"
        );

        Ok(Self {
            session_dir,
            fsync_on_write: cfg.fsync_on_write,
            rotate,
            session_start: Instant::now(),
            writers: HashMap::new(),
        })
    }

    pub fn session_dir(&self) -> &Path {
        &self.session_dir
    }

    /// Append one raw event. Opens/rotates the target file as needed.
    pub fn write(&mut self, event: &RawEvent) -> Result<(), StorageError> {
        let bucket = self.bucket_for(Instant::now());
        self.write_with_bucket(event, bucket)
    }

    /// Flush every open writer's buffer. Also fsyncs when `fsync_on_write`
    /// is enabled (to force in-flight data to disk on shutdown).
    pub fn flush_all(&mut self) -> Result<(), StorageError> {
        for w in self.writers.values_mut() {
            w.flush(self.fsync_on_write)?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------

    fn bucket_for(&self, now: Instant) -> u64 {
        match self.rotate {
            None => 0,
            Some(dur) => {
                let elapsed = now.saturating_duration_since(self.session_start);
                (elapsed.as_secs() / dur.as_secs().max(1)) as u64
            }
        }
    }

    fn write_with_bucket(
        &mut self,
        event: &RawEvent,
        bucket: u64,
    ) -> Result<(), StorageError> {
        let key = (event.venue, event.stream.clone());
        let writer = match self.writers.get_mut(&key) {
            Some(w) => {
                w.ensure_bucket(bucket, self.rotate.is_some())?;
                w
            }
            None => {
                let w = StreamWriter::open(
                    &self.session_dir,
                    event.venue,
                    &event.stream,
                    bucket,
                    self.rotate.is_some(),
                )?;
                self.writers.entry(key.clone()).or_insert(w)
            }
        };

        let mut line = serde_json::to_vec(event)?;
        line.push(b'\n');
        writer.write_line(&line, self.fsync_on_write)?;
        Ok(())
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // Best-effort flush on drop. Real shutdown should call
        // flush_all() first so errors surface.
        for w in self.writers.values_mut() {
            let _ = w.flush(false);
        }
    }
}

// ---------------------------------------------------------------------------
// StreamWriter
// ---------------------------------------------------------------------------

struct StreamWriter {
    venue_dir: PathBuf,
    sanitized_stream: String,
    bucket: u64,
    path: PathBuf,
    file: BufWriter<File>,
}

impl StreamWriter {
    fn open(
        session_dir: &Path,
        venue: Venue,
        stream: &str,
        bucket: u64,
        rotation_enabled: bool,
    ) -> Result<Self, StorageError> {
        let venue_dir = session_dir.join(venue.as_str());
        std::fs::create_dir_all(&venue_dir).map_err(|source| StorageError::Io {
            path: venue_dir.clone(),
            source,
        })?;

        let sanitized = sanitize_stream_name(stream);
        let path = file_path(&venue_dir, &sanitized, bucket, rotation_enabled);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|source| StorageError::Io {
                path: path.clone(),
                source,
            })?;

        tracing::info!(
            component = "storage",
            event = "file_open",
            venue = venue.as_str(),
            stream = stream,
            bucket = bucket,
            path = %path.display(),
            "opened stream file"
        );

        Ok(Self {
            venue_dir,
            sanitized_stream: sanitized,
            bucket,
            path,
            file: BufWriter::new(file),
        })
    }

    fn ensure_bucket(
        &mut self,
        bucket: u64,
        rotation_enabled: bool,
    ) -> Result<(), StorageError> {
        if bucket == self.bucket {
            return Ok(());
        }
        // Flush the outgoing bucket before swapping files — don't lose
        // buffered data at the rotation boundary.
        self.file.flush().map_err(|source| StorageError::Io {
            path: self.path.clone(),
            source,
        })?;

        let new_path = file_path(&self.venue_dir, &self.sanitized_stream, bucket, rotation_enabled);
        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)
            .map_err(|source| StorageError::Io {
                path: new_path.clone(),
                source,
            })?;

        tracing::info!(
            component = "storage",
            event = "file_rotate",
            old_path = %self.path.display(),
            new_path = %new_path.display(),
            from_bucket = self.bucket,
            to_bucket = bucket,
            "rotated stream file"
        );

        self.bucket = bucket;
        self.path = new_path;
        self.file = BufWriter::new(new_file);
        Ok(())
    }

    fn write_line(&mut self, bytes: &[u8], fsync: bool) -> Result<(), StorageError> {
        self.file.write_all(bytes).map_err(|source| StorageError::Io {
            path: self.path.clone(),
            source,
        })?;
        if fsync {
            self.flush(true)?;
        }
        Ok(())
    }

    fn flush(&mut self, fsync: bool) -> Result<(), StorageError> {
        self.file.flush().map_err(|source| StorageError::Io {
            path: self.path.clone(),
            source,
        })?;
        if fsync {
            self.file
                .get_ref()
                .sync_data()
                .map_err(|source| StorageError::Io {
                    path: self.path.clone(),
                    source,
                })?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// UTC timestamp formatted as `YYYYMMDDTHHMMSSZ`. Computed with no external
/// deps so we don't fight the Windows-GNU toolchain. Uses Howard Hinnant's
/// days-to-civil conversion.
fn utc_compact_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (y, mo, d, h, mi, s) = epoch_secs_to_utc(secs);
    format!("{y:04}{mo:02}{d:02}T{h:02}{mi:02}{s:02}Z")
}

fn epoch_secs_to_utc(secs: u64) -> (i64, u32, u32, u32, u32, u32) {
    let days = (secs / 86_400) as i64;
    let sod = secs % 86_400;
    let hour = (sod / 3_600) as u32;
    let min = ((sod / 60) % 60) as u32;
    let sec = (sod % 60) as u32;

    // civil_from_days: z = days since 1970-01-01.
    let z = days + 719_468;
    let era = if z >= 0 { z / 146_097 } else { (z - 146_096) / 146_097 };
    let doe = (z - era * 146_097) as i64; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };

    (y, m, d, hour, min, sec)
}

fn sanitize_stream_name(stream: &str) -> String {
    stream
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn file_path(
    venue_dir: &Path,
    sanitized_stream: &str,
    bucket: u64,
    rotation_enabled: bool,
) -> PathBuf {
    if rotation_enabled {
        venue_dir.join(format!("{sanitized_stream}.{bucket:04}.ndjson"))
    } else {
        venue_dir.join(format!("{sanitized_stream}.ndjson"))
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("io error at {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use common::LocalTimestamp;
    use std::io::BufRead;
    use std::path::PathBuf;

    fn cfg_with(dir: PathBuf, rotate_minutes: u64, fsync: bool) -> config::StorageConfig {
        config::StorageConfig {
            base_dir: dir,
            rotate_minutes,
            fsync_on_write: fsync,
        }
    }

    /// Unique temp dir per test, auto-removed on drop. Avoids pulling
    /// tempfile/getrandom into the build just for tests.
    struct TestDir(PathBuf);
    impl TestDir {
        fn new() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let ptr = &nanos as *const _ as usize; // mix in stack addr for uniqueness
            let dir = std::env::temp_dir().join(format!("polybot_storage_test_{nanos}_{ptr:x}"));
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

    fn ev(venue: Venue, stream: &str, ns: u128, venue_ts: Option<i64>, payload: &str) -> RawEvent {
        RawEvent {
            venue,
            stream: stream.into(),
            local_ts_ns: LocalTimestamp::from_nanos(ns),
            venue_ts_ms: venue_ts,
            payload: payload.into(),
        ..Default::default()
        }
    }

    #[test]
    fn epoch_to_utc_matches_known_points() {
        // 1970-01-01 00:00:00 UTC
        assert_eq!(epoch_secs_to_utc(0), (1970, 1, 1, 0, 0, 0));
        // 2020-02-29 00:00:00 UTC — leap day sanity check
        assert_eq!(epoch_secs_to_utc(1_582_934_400), (2020, 2, 29, 0, 0, 0));
        // 2026-04-23 11:22:33 UTC.
        let secs = 1_776_943_353;
        let (y, mo, d, h, mi, s) = epoch_secs_to_utc(secs);
        assert_eq!((y, mo, d, h, mi, s), (2026, 4, 23, 11, 22, 33));
    }

    #[test]
    fn sanitize_stream_replaces_special_chars() {
        assert_eq!(sanitize_stream_name("btcusdt@trade"), "btcusdt_trade");
        assert_eq!(
            sanitize_stream_name("btcusdt@depth@100ms"),
            "btcusdt_depth_100ms"
        );
        assert_eq!(sanitize_stream_name("abc-def_123"), "abc-def_123");
    }

    #[test]
    fn write_emits_one_ndjson_line_per_record() {
        let tmp = TestDir::new();
        let mut store = Store::open(&cfg_with(tmp.path().to_path_buf(), 0, false)).unwrap();

        store
            .write(&ev(
                Venue::Binance,
                "btcusdt@trade",
                1,
                Some(42),
                r#"{"e":"trade","p":"45000"}"#,
            ))
            .unwrap();
        store
            .write(&ev(
                Venue::Binance,
                "btcusdt@trade",
                2,
                None,
                r#"{"e":"trade","p":"45001"}"#,
            ))
            .unwrap();
        store.flush_all().unwrap();

        let session = store.session_dir().to_path_buf();
        let file = session.join("binance").join("btcusdt_trade.ndjson");
        let lines: Vec<String> = std::io::BufReader::new(std::fs::File::open(&file).unwrap())
            .lines()
            .map(|l| l.unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains(r#""venue":"binance""#));
        // local_ts_ns is a string — see common::LocalTimestamp wire format.
        assert!(lines[0].contains(r#""local_ts_ns":"1""#));
        assert!(lines[0].contains(r#""venue_ts_ms":42"#));
        // Second record omits venue_ts_ms because it's None.
        assert!(!lines[1].contains("venue_ts_ms"));
    }

    #[test]
    fn separate_streams_go_to_separate_files() {
        let tmp = TestDir::new();
        let mut store = Store::open(&cfg_with(tmp.path().to_path_buf(), 0, false)).unwrap();

        store.write(&ev(Venue::Binance, "btcusdt@trade", 1, None, "a")).unwrap();
        store.write(&ev(Venue::Binance, "btcusdt@depth@100ms", 2, None, "b")).unwrap();
        store.write(&ev(Venue::Polymarket, "market-id-0x01", 3, None, "c")).unwrap();
        store.flush_all().unwrap();

        let session = store.session_dir().to_path_buf();
        assert!(session.join("binance/btcusdt_trade.ndjson").exists());
        assert!(session.join("binance/btcusdt_depth_100ms.ndjson").exists());
        assert!(session.join("polymarket/market-id-0x01.ndjson").exists());
    }

    #[test]
    fn rotation_writes_to_new_bucket_file() {
        let tmp = TestDir::new();
        // Non-zero rotation so rotation path is active. We drive buckets
        // manually via the crate-private helper.
        let mut store = Store::open(&cfg_with(tmp.path().to_path_buf(), 60, false)).unwrap();

        let rec = ev(Venue::Binance, "btcusdt@trade", 1, None, "a");

        store.write_with_bucket(&rec, 0).unwrap();
        store.write_with_bucket(&rec, 0).unwrap();
        store.write_with_bucket(&rec, 1).unwrap(); // rotation
        store.write_with_bucket(&rec, 1).unwrap();
        store.flush_all().unwrap();

        let dir = store.session_dir().join("binance");
        let b0 = dir.join("btcusdt_trade.0000.ndjson");
        let b1 = dir.join("btcusdt_trade.0001.ndjson");
        assert!(b0.exists() && b1.exists());

        let count = |p: &std::path::Path| {
            std::io::BufReader::new(std::fs::File::open(p).unwrap())
                .lines()
                .count()
        };
        assert_eq!(count(&b0), 2);
        assert_eq!(count(&b1), 2);
    }

    #[test]
    fn bucket_for_returns_zero_without_rotation() {
        let tmp = TestDir::new();
        let store = Store::open(&cfg_with(tmp.path().to_path_buf(), 0, false)).unwrap();
        assert_eq!(store.bucket_for(Instant::now()), 0);
    }

    #[test]
    fn fsync_on_write_does_not_error() {
        let tmp = TestDir::new();
        let mut store = Store::open(&cfg_with(tmp.path().to_path_buf(), 0, true)).unwrap();
        store
            .write(&ev(Venue::Binance, "btcusdt@trade", 1, None, "ping"))
            .unwrap();
        store.flush_all().unwrap();
    }

    /// The replay contract: whatever goes into the store must come back out
    /// byte-equal after writing → reading from the file → serde round trip.
    ///
    /// Uses a timestamp beyond JSON's 2^53 safe-integer range to make sure
    /// the string-encoding of `LocalTimestamp` survives the file boundary.
    /// Also exercises both `Some` and `None` paths for `venue_ts_ms`.
    #[test]
    fn raw_events_round_trip_through_storage() {
        use std::io::Read as _;

        let tmp = TestDir::new();
        let mut store = Store::open(&cfg_with(tmp.path().to_path_buf(), 0, false)).unwrap();

        let events = [
            RawEvent {
                venue: Venue::Binance,
                stream: "btcusdt@trade".into(),
                // 2^53 + 1 — picks up any accidental f64 coercion.
                local_ts_ns: LocalTimestamp::from_nanos(9_007_199_254_740_993),
                venue_ts_ms: Some(1_776_900_000_000),
                payload: r#"{"e":"trade","s":"BTCUSDT","p":"45000.12"}"#.into(),
                ..Default::default()
            },
            RawEvent {
                venue: Venue::Polymarket,
                stream: "market-0xdeadbeef".into(),
                local_ts_ns: LocalTimestamp::from_nanos(1),
                venue_ts_ms: None,
                payload: "pm payload with \"quotes\" and \\ backslash".into(),
                ..Default::default()
            },
        ];

        for e in &events {
            store.write(e).unwrap();
        }
        store.flush_all().unwrap();

        let session = store.session_dir().to_path_buf();
        let cases = [
            (&events[0], session.join("binance").join("btcusdt_trade.ndjson")),
            (
                &events[1],
                session.join("polymarket").join("market-0xdeadbeef.ndjson"),
            ),
        ];

        for (expected, path) in cases {
            let mut buf = String::new();
            std::fs::File::open(&path)
                .unwrap_or_else(|e| panic!("open {}: {e}", path.display()))
                .read_to_string(&mut buf)
                .unwrap();
            let line = buf
                .lines()
                .next()
                .unwrap_or_else(|| panic!("no lines in {}", path.display()));

            let parsed: RawEvent = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("parsing {line:?}: {e}"));

            assert_eq!(&parsed, expected, "round trip changed the event");
        }
    }
}
