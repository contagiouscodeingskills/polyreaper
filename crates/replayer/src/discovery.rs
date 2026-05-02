//! Filesystem walk: turn a base directory or session directory into typed
//! [`SessionDir`] / [`FileBucket`] records.
//!
//! Naming conventions baked in here mirror the recorder's
//! `crates/storage/src/lib.rs`:
//! * Session dir: `session_<UTC_YYYYMMDDTHHMMSSZ>` (16-char suffix).
//! * Venue dir: lowercase venue name (`binance`, `polymarket`, …).
//! * File: `<sanitized_stream>.ndjson` (no rotation) or
//!   `<sanitized_stream>.NNNN.ndjson` (with rotation).
//!
//! Directories or files we don't recognise are silently skipped — the
//! replayer is read-only and tolerant.

use std::fs;
use std::path::{Path, PathBuf};

use common::Venue;

use crate::error::ReplayError;

#[derive(Debug, Clone)]
pub struct SessionDir {
    pub path: PathBuf,
    /// The 16-char `YYYYMMDDTHHMMSSZ` suffix from the directory name.
    /// Lexically sortable so `Vec::sort` orders sessions chronologically.
    pub start_utc: String,
}

#[derive(Debug, Clone)]
pub struct FileBucket {
    pub venue: Venue,
    pub stream: String,
    /// Bucket index. `0` for non-rotated files (no `.NNNN` suffix).
    pub bucket: u64,
    pub path: PathBuf,
    /// True for `*.ndjson.gz`. Set so the reader can wrap with GzDecoder
    /// without re-inspecting the path. Sessions compressed by `disk_guard.sh`
    /// have this set; in-flight sessions don't.
    pub compressed: bool,
}

impl SessionDir {
    /// Discover every `session_*` subdir under `base`, sorted chronologically.
    pub fn discover(base: &Path) -> Result<Vec<SessionDir>, ReplayError> {
        let mut out = Vec::new();
        let entries = fs::read_dir(base).map_err(|source| ReplayError::Io {
            path: base.to_path_buf(),
            source,
        })?;
        for entry in entries {
            let entry = entry.map_err(|source| ReplayError::Io {
                path: base.to_path_buf(),
                source,
            })?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            if let Some(s) = parse_session_name(&path) {
                out.push(s);
            }
        }
        out.sort_by(|a, b| a.start_utc.cmp(&b.start_utc));
        Ok(out)
    }

    /// Treat `p` as a session directory directly. Returns
    /// [`ReplayError::NotASessionDir`] if the name doesn't fit the pattern.
    pub fn from_path(p: &Path) -> Result<SessionDir, ReplayError> {
        parse_session_name(p).ok_or_else(|| ReplayError::NotASessionDir {
            path: p.to_path_buf(),
        })
    }

    /// Walk the session dir's `<venue>/<stream>.ndjson` files. Sorted
    /// `(venue, stream, bucket)` so callers get stable file ordering.
    pub fn list_files(&self) -> Result<Vec<FileBucket>, ReplayError> {
        let mut out = Vec::new();
        let entries = fs::read_dir(&self.path).map_err(|source| ReplayError::Io {
            path: self.path.clone(),
            source,
        })?;
        for entry in entries {
            let entry = entry.map_err(|source| ReplayError::Io {
                path: self.path.clone(),
                source,
            })?;
            let venue_path = entry.path();
            if !venue_path.is_dir() {
                continue;
            }
            let venue_name = match venue_path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n,
                None => continue,
            };
            let venue = match parse_venue(venue_name) {
                Some(v) => v,
                None => continue,
            };
            let venue_entries = fs::read_dir(&venue_path).map_err(|source| ReplayError::Io {
                path: venue_path.clone(),
                source,
            })?;
            for ve in venue_entries {
                let ve = ve.map_err(|source| ReplayError::Io {
                    path: venue_path.clone(),
                    source,
                })?;
                let p = ve.path();
                if p.is_file() {
                    if let Some(fb) = parse_file_name(venue, &p) {
                        out.push(fb);
                    }
                }
            }
        }
        out.sort_by(|a, b| {
            a.venue
                .cmp(&b.venue)
                .then_with(|| a.stream.cmp(&b.stream))
                .then_with(|| a.bucket.cmp(&b.bucket))
        });
        Ok(out)
    }
}

fn parse_session_name(path: &Path) -> Option<SessionDir> {
    let name = path.file_name()?.to_str()?;
    let suffix = name.strip_prefix("session_")?;
    // Must be exactly `YYYYMMDDTHHMMSSZ` — 16 chars. Looser checks would
    // false-positive on user-created session_anything dirs.
    if suffix.len() != 16 {
        return None;
    }
    Some(SessionDir {
        path: path.to_path_buf(),
        start_utc: suffix.to_string(),
    })
}

fn parse_venue(name: &str) -> Option<Venue> {
    match name {
        "binance" => Some(Venue::Binance),
        "polymarket" => Some(Venue::Polymarket),
        "coinbase" => Some(Venue::Coinbase),
        "chainlink" => Some(Venue::Chainlink),
        _ => None,
    }
}

/// Parse `<stream>.ndjson`, `<stream>.NNNN.ndjson`, or the gzip-compressed
/// variants `<stream>.ndjson.gz` / `<stream>.NNNN.ndjson.gz`. Stream names
/// from the recorder go through `sanitize_stream_name` which keeps
/// `[a-zA-Z0-9_-]` — so `.` only appears as the bucket separator or the
/// `.ndjson(.gz)?` extension.
fn parse_file_name(venue: Venue, path: &Path) -> Option<FileBucket> {
    let name = path.file_name()?.to_str()?;
    // Strip `.gz` first so the rest of the parser is identical for plain
    // and compressed files. Sessions become `.gz` once disk_guard rolls
    // over them.
    let (extless, compressed) = match name.strip_suffix(".gz") {
        Some(rest) => (rest, true),
        None => (name, false),
    };
    let stem = extless.strip_suffix(".ndjson")?;
    if let Some(dot_idx) = stem.rfind('.') {
        let (left, right) = (&stem[..dot_idx], &stem[dot_idx + 1..]);
        if right.len() == 4 && right.chars().all(|c| c.is_ascii_digit()) {
            if let Ok(bucket) = right.parse::<u64>() {
                return Some(FileBucket {
                    venue,
                    stream: left.to_string(),
                    bucket,
                    path: path.to_path_buf(),
                    compressed,
                });
            }
        }
    }
    Some(FileBucket {
        venue,
        stream: stem.to_string(),
        bucket: 0,
        path: path.to_path_buf(),
        compressed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rotated_filename() {
        let p = PathBuf::from("/tmp/session/binance/btcusdt_trade.0007.ndjson");
        let f = parse_file_name(Venue::Binance, &p).unwrap();
        assert_eq!(f.stream, "btcusdt_trade");
        assert_eq!(f.bucket, 7);
        assert!(!f.compressed);
    }

    #[test]
    fn parses_non_rotated_filename() {
        let p = PathBuf::from("/tmp/session/binance/btcusdt_trade.ndjson");
        let f = parse_file_name(Venue::Binance, &p).unwrap();
        assert_eq!(f.stream, "btcusdt_trade");
        assert_eq!(f.bucket, 0);
        assert!(!f.compressed);
    }

    #[test]
    fn parses_gzipped_rotated_filename() {
        let p = PathBuf::from("/tmp/session/binance/btcusdt_trade.0007.ndjson.gz");
        let f = parse_file_name(Venue::Binance, &p).unwrap();
        assert_eq!(f.stream, "btcusdt_trade");
        assert_eq!(f.bucket, 7);
        assert!(f.compressed);
    }

    #[test]
    fn parses_gzipped_non_rotated_filename() {
        let p = PathBuf::from("/tmp/session/binance/btcusdt_trade.ndjson.gz");
        let f = parse_file_name(Venue::Binance, &p).unwrap();
        assert_eq!(f.stream, "btcusdt_trade");
        assert_eq!(f.bucket, 0);
        assert!(f.compressed);
    }

    #[test]
    fn parses_polymarket_slug_with_dashes() {
        let p = PathBuf::from("/tmp/x/polymarket/btc-updown-5m-1777175400.0003.ndjson");
        let f = parse_file_name(Venue::Polymarket, &p).unwrap();
        assert_eq!(f.stream, "btc-updown-5m-1777175400");
        assert_eq!(f.bucket, 3);
    }

    #[test]
    fn rejects_non_ndjson() {
        let p = PathBuf::from("/tmp/x/binance/notes.txt");
        assert!(parse_file_name(Venue::Binance, &p).is_none());
    }

    #[test]
    fn parses_session_suffix() {
        let p = PathBuf::from("/data/session_20260425T053013Z");
        let sd = parse_session_name(&p).unwrap();
        assert_eq!(sd.start_utc, "20260425T053013Z");
    }

    #[test]
    fn rejects_short_session_suffix() {
        let p = PathBuf::from("/data/session_20260425");
        assert!(parse_session_name(&p).is_none());
    }
}
