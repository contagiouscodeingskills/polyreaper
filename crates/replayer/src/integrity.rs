//! Integrity checker for captured sessions.
//!
//! Reads each NDJSON file in a session directory and runs three layers
//! of checks:
//!
//! * **Tier 0 — structural** (file metadata + line-level): empty files,
//!   resolution-sweeper 0-byte files, `_unrouted` / `_unknown_market-*`
//!   / `_unknown_token-*` recorder-routing failures, gaps in rotated
//!   bucket indices, NDJSON parse errors, per-file `local_ts_ns`
//!   non-decreasing.
//! * **Tier 1 — decoder** (every event runs through [`crate::decode`]):
//!   counts of [`crate::decode::DecodedEvent::Unknown`] (informational —
//!   subscription acks and venue control frames are expected here) and
//!   `ReplayError::Decode` failures (a real problem — a real event the
//!   decoder can't shape).
//! * **Tier 2 — sequence**: the Binance depth-diff chain. Consecutive
//!   diffs in a stream must satisfy `first_update_id_{n+1} == prev.final_update_id + 1`.
//!   Some breaks are *expected*: every WS reconnect resets the venue's
//!   update counter, so a break adjacent to a `*@depth_snapshot`
//!   re-baseline is a normal boundary, not packet loss. The report
//!   carries the snapshot count alongside the break count so the caller
//!   can compare: `breaks <= snapshots` is the typical reconnect
//!   pattern; `breaks >> snapshots` is a real signal of dropped
//!   messages.
//!
//! One sequential pass per file. Memory is `O(num_streams)` for
//! per-stream running state.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Instant;

use serde::Serialize;

use common::Venue;

use crate::decode::{decode, DecodedEvent};
use crate::discovery::SessionDir;
use crate::error::ReplayError;
use crate::reader::FileReader;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Aggregate result of checking one session directory.
#[derive(Debug, Clone, Serialize)]
pub struct SessionIntegrity {
    /// Full session directory name, e.g. `session_20260101T000000Z`.
    /// Same string in both text and JSON output.
    pub session_name: String,
    pub elapsed_secs: f64,
    pub files_scanned: u64,
    pub total_events: u64,

    /// Keyed by venue lowercase name (`"binance"` etc.) for stable JSON
    /// serialisation.
    pub per_venue: BTreeMap<String, VenueStats>,

    pub structural: StructuralIssues,
    pub decoder: DecoderIssues,
    pub sequence: SequenceIssues,

    /// Per-file findings. Empty unless the caller passed `verbose = true`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<FileFinding>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct VenueStats {
    pub streams: u64,
    pub events: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct StructuralIssues {
    pub empty_files: u64,
    pub resolution_zero_byte_files: u64,
    pub unrouted_files: u64,
    pub unknown_market_files: u64,
    pub unknown_token_files: u64,
    pub bucket_gaps: u64,
    pub parse_errors: u64,
    pub ts_violations: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DecoderIssues {
    pub decode_errors: u64,
    /// Informational: subscription acks, heartbeats, anything that
    /// dispatched to [`crate::decode::DecodedEvent::Unknown`]. Not a
    /// failure — surfaces wire-format drift if the count is unexpectedly
    /// large for an active stream.
    pub unknown_variants: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SequenceIssues {
    /// Number of consecutive Binance depth diffs whose `first_update_id`
    /// did not equal the previous event's `final_update_id + 1`.
    /// Interpretation requires context — see the module-level docs.
    pub binance_depth_chain_breaks: u64,
    /// `BinanceDepthSnapshot` events seen in the session. Each snapshot
    /// re-baselines the diff chain, so breaks at snapshot boundaries are
    /// expected; `breaks <= snapshots` is the typical pattern.
    pub binance_depth_snapshots_observed: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FileFinding {
    pub path: PathBuf,
    pub kind: FindingKind,
    pub count: u64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub note: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingKind {
    EmptyFile,
    ResolutionZeroByte,
    UnroutedFile,
    UnknownMarketFile,
    UnknownTokenFile,
    BucketGap,
    ParseError,
    TsViolation,
    DecodeError,
    BinanceDepthChainBreak,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Run the integrity checks on a single session.
///
/// Pass `verbose = true` to populate [`SessionIntegrity::details`] with
/// per-file findings. The aggregate counters are populated either way.
pub fn check_session(
    session: &SessionDir,
    verbose: bool,
) -> Result<SessionIntegrity, ReplayError> {
    let started = Instant::now();
    let files = session.list_files()?;

    let mut report = SessionIntegrity {
        session_name: format!("session_{}", session.start_utc),
        elapsed_secs: 0.0,
        files_scanned: 0,
        total_events: 0,
        per_venue: BTreeMap::new(),
        structural: StructuralIssues::default(),
        decoder: DecoderIssues::default(),
        sequence: SequenceIssues::default(),
        details: Vec::new(),
    };

    // --- Structural pre-scan: file-level metadata, no decoding. ---

    let mut buckets_by_stream: BTreeMap<(Venue, String), Vec<u64>> = BTreeMap::new();
    let mut streams_by_venue: BTreeMap<Venue, BTreeSet<String>> = BTreeMap::new();

    for f in &files {
        buckets_by_stream
            .entry((f.venue, f.stream.clone()))
            .or_default()
            .push(f.bucket);
        streams_by_venue
            .entry(f.venue)
            .or_default()
            .insert(f.stream.clone());

        let len = std::fs::metadata(&f.path).map(|m| m.len()).unwrap_or(0);
        if len == 0 {
            if f.stream.contains("-resolved") {
                report.structural.resolution_zero_byte_files += 1;
                if verbose {
                    report.details.push(FileFinding {
                        path: f.path.clone(),
                        kind: FindingKind::ResolutionZeroByte,
                        count: 0,
                        note: String::new(),
                    });
                }
            } else {
                report.structural.empty_files += 1;
                if verbose {
                    report.details.push(FileFinding {
                        path: f.path.clone(),
                        kind: FindingKind::EmptyFile,
                        count: 0,
                        note: String::new(),
                    });
                }
            }
        }

        if f.stream == "_unrouted" {
            report.structural.unrouted_files += 1;
            if verbose {
                report.details.push(FileFinding {
                    path: f.path.clone(),
                    kind: FindingKind::UnroutedFile,
                    count: 0,
                    note: String::new(),
                });
            }
        } else if f.stream.starts_with("_unknown_market-") {
            report.structural.unknown_market_files += 1;
            if verbose {
                report.details.push(FileFinding {
                    path: f.path.clone(),
                    kind: FindingKind::UnknownMarketFile,
                    count: 0,
                    note: String::new(),
                });
            }
        } else if f.stream.starts_with("_unknown_token-") {
            report.structural.unknown_token_files += 1;
            if verbose {
                report.details.push(FileFinding {
                    path: f.path.clone(),
                    kind: FindingKind::UnknownTokenFile,
                    count: 0,
                    note: String::new(),
                });
            }
        }
    }

    for ((venue, stream), buckets) in &buckets_by_stream {
        let mut sorted = buckets.clone();
        sorted.sort_unstable();
        for w in sorted.windows(2) {
            let gap = w[1].saturating_sub(w[0]);
            if gap > 1 {
                let missing = (gap - 1) as u64;
                report.structural.bucket_gaps += missing;
                if verbose {
                    report.details.push(FileFinding {
                        path: session
                            .path
                            .join(format!("{}/{}.<missing>", venue.as_str(), stream)),
                        kind: FindingKind::BucketGap,
                        count: missing,
                        note: format!("between bucket {} and {}", w[0], w[1]),
                    });
                }
            }
        }
    }

    for (venue, streams) in &streams_by_venue {
        report
            .per_venue
            .entry(venue.as_str().to_string())
            .or_default()
            .streams = streams.len() as u64;
    }

    // --- Per-file data pass: parse, decode, check timestamps + sequences. ---

    // Persists across files within a stream so chain checks see rotated
    // buckets as one continuous sequence.
    let mut depth_chain_state: BTreeMap<(Venue, String), Option<u64>> = BTreeMap::new();

    for f in &files {
        report.files_scanned += 1;
        let len = std::fs::metadata(&f.path).map(|m| m.len()).unwrap_or(0);
        if len == 0 {
            continue;
        }

        let mut last_local_ts_ns: Option<u128> = None;
        let mut local_parse_errors_in_file: u64 = 0;
        let mut local_decode_errors_in_file: u64 = 0;
        let mut local_ts_violations_in_file: u64 = 0;
        let mut local_chain_breaks_in_file: u64 = 0;

        let depth_state = depth_chain_state
            .entry((f.venue, f.stream.clone()))
            .or_insert(None);

        let reader = FileReader::open(&f.path)?;
        for next in reader {
            match next {
                Err(ReplayError::ParseLine { .. }) => {
                    report.structural.parse_errors += 1;
                    local_parse_errors_in_file += 1;
                    continue;
                }
                Err(other) => return Err(other),
                Ok((_line_no, event)) => {
                    report.total_events += 1;
                    if let Some(stats) =
                        report.per_venue.get_mut(event.venue.as_str())
                    {
                        stats.events += 1;
                    }
                    let ts = event.local_ts_ns.as_nanos();
                    if let Some(prev) = last_local_ts_ns {
                        if ts < prev {
                            report.structural.ts_violations += 1;
                            local_ts_violations_in_file += 1;
                        }
                    }
                    last_local_ts_ns = Some(ts);

                    match decode(&event) {
                        Err(_) => {
                            report.decoder.decode_errors += 1;
                            local_decode_errors_in_file += 1;
                        }
                        Ok(DecodedEvent::Unknown { .. }) => {
                            report.decoder.unknown_variants += 1;
                        }
                        Ok(DecodedEvent::BinanceDepthSnapshot(_)) => {
                            report.sequence.binance_depth_snapshots_observed += 1;
                        }
                        Ok(DecodedEvent::BinanceDepthDiff(d)) => {
                            if let Some(prev_u) = *depth_state {
                                if d.first_update_id != prev_u + 1 {
                                    report.sequence.binance_depth_chain_breaks += 1;
                                    local_chain_breaks_in_file += 1;
                                }
                            }
                            *depth_state = Some(d.final_update_id);
                        }
                        Ok(_) => {}
                    }
                }
            }
        }

        if verbose {
            if local_parse_errors_in_file > 0 {
                report.details.push(FileFinding {
                    path: f.path.clone(),
                    kind: FindingKind::ParseError,
                    count: local_parse_errors_in_file,
                    note: String::new(),
                });
            }
            if local_decode_errors_in_file > 0 {
                report.details.push(FileFinding {
                    path: f.path.clone(),
                    kind: FindingKind::DecodeError,
                    count: local_decode_errors_in_file,
                    note: String::new(),
                });
            }
            if local_ts_violations_in_file > 0 {
                report.details.push(FileFinding {
                    path: f.path.clone(),
                    kind: FindingKind::TsViolation,
                    count: local_ts_violations_in_file,
                    note: String::new(),
                });
            }
            if local_chain_breaks_in_file > 0 {
                report.details.push(FileFinding {
                    path: f.path.clone(),
                    kind: FindingKind::BinanceDepthChainBreak,
                    count: local_chain_breaks_in_file,
                    note: String::new(),
                });
            }
        }
    }

    report.elapsed_secs = started.elapsed().as_secs_f64();
    Ok(report)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use common::{LocalTimestamp, RawEvent, Venue};

    struct TestDir(PathBuf);
    impl TestDir {
        fn new() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let ptr = &nanos as *const _ as usize;
            let dir = std::env::temp_dir().join(format!("polybot_integrity_{nanos}_{ptr:x}"));
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// Build a session dir at `tmp/session_<UTC>/`. Returns it as a
    /// SessionDir ready to pass to `check_session`.
    fn make_session(tmp: &Path) -> (PathBuf, SessionDir) {
        let session_path = tmp.join("session_20260427T100000Z");
        fs::create_dir_all(&session_path).unwrap();
        let sd = SessionDir::from_path(&session_path).unwrap();
        (session_path, sd)
    }

    fn write_file(session: &Path, venue: &str, file_name: &str, lines: &[&str]) -> PathBuf {
        let venue_dir = session.join(venue);
        fs::create_dir_all(&venue_dir).unwrap();
        let path = venue_dir.join(file_name);
        let mut content = String::new();
        for l in lines {
            content.push_str(l);
            content.push('\n');
        }
        fs::write(&path, content).unwrap();
        path
    }

    fn raw_event_line(venue: Venue, stream: &str, ts: u128, payload: &str) -> String {
        let ev = RawEvent {
            venue,
            stream: stream.into(),
            local_ts_ns: LocalTimestamp::from_nanos(ts),
            venue_ts_ms: None,
            payload: payload.into(),
        };
        serde_json::to_string(&ev).unwrap()
    }

    // -----------------------------------------------------------------
    // Tier 0 — structural
    // -----------------------------------------------------------------

    #[test]
    fn empty_file_counted_as_empty() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let _empty = write_file(&session, "binance", "btcusdt_trade.0000.ndjson", &[]);
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.structural.empty_files, 1);
        assert_eq!(r.structural.resolution_zero_byte_files, 0);
        assert_eq!(r.total_events, 0);
    }

    #[test]
    fn empty_resolved_file_counted_separately() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        write_file(
            &session,
            "polymarket",
            "btc-updown-5m-12345-resolved.0000.ndjson",
            &[],
        );
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.structural.resolution_zero_byte_files, 1);
        assert_eq!(r.structural.empty_files, 0);
    }

    #[test]
    fn parse_error_counted() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let good = raw_event_line(Venue::Binance, "btcusdt@trade", 100, r#"{"e":"trade"}"#);
        write_file(
            &session,
            "binance",
            "btcusdt_trade.0000.ndjson",
            &[&good, "this is not json"],
        );
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.structural.parse_errors, 1);
        assert_eq!(r.total_events, 1);
    }

    #[test]
    fn ts_violation_counted_within_file() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        // 100, 200, 150 — last one regresses.
        let lines = [
            raw_event_line(Venue::Binance, "btcusdt@trade", 100, r#"{"e":"trade"}"#),
            raw_event_line(Venue::Binance, "btcusdt@trade", 200, r#"{"e":"trade"}"#),
            raw_event_line(Venue::Binance, "btcusdt@trade", 150, r#"{"e":"trade"}"#),
        ];
        let refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
        write_file(&session, "binance", "btcusdt_trade.0000.ndjson", &refs);
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.structural.ts_violations, 1);
    }

    #[test]
    fn unrouted_and_unknown_files_counted() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let line = raw_event_line(Venue::Polymarket, "_unrouted", 1, r#"{}"#);
        write_file(&session, "polymarket", "_unrouted.ndjson", &[&line]);
        let line = raw_event_line(
            Venue::Polymarket,
            "_unknown_market-0xabc",
            1,
            r#"{}"#,
        );
        write_file(
            &session,
            "polymarket",
            "_unknown_market-0xabc.ndjson",
            &[&line],
        );
        let line = raw_event_line(
            Venue::Polymarket,
            "_unknown_token-XYZ",
            1,
            r#"{}"#,
        );
        write_file(
            &session,
            "polymarket",
            "_unknown_token-XYZ.ndjson",
            &[&line],
        );
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.structural.unrouted_files, 1);
        assert_eq!(r.structural.unknown_market_files, 1);
        assert_eq!(r.structural.unknown_token_files, 1);
    }

    #[test]
    fn bucket_gap_counted() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let line =
            raw_event_line(Venue::Binance, "btcusdt@trade", 1, r#"{"e":"trade"}"#);
        write_file(&session, "binance", "btcusdt_trade.0000.ndjson", &[&line]);
        write_file(&session, "binance", "btcusdt_trade.0002.ndjson", &[&line]);
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.structural.bucket_gaps, 1, "missing bucket 0001");
    }

    // -----------------------------------------------------------------
    // Tier 1 — decoder
    // -----------------------------------------------------------------

    #[test]
    fn unknown_variant_counted_not_failure() {
        // Stub binance trade payload that doesn't have `e`-field-driven
        // structure for a known type → falls to `Unknown`.
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let line = raw_event_line(
            Venue::Binance,
            "btcusdt@unknownStream",
            1,
            r#"{"result":null,"id":1}"#,
        );
        write_file(
            &session,
            "binance",
            "btcusdt_unknownStream.0000.ndjson",
            &[&line],
        );
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.decoder.unknown_variants, 1);
        assert_eq!(r.decoder.decode_errors, 0);
    }

    // -----------------------------------------------------------------
    // Tier 2 — Binance depth chain
    // -----------------------------------------------------------------

    fn depth_diff_payload(first_u: u64, final_u: u64) -> String {
        format!(
            r#"{{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":{first_u},"u":{final_u},"b":[],"a":[]}}"#
        )
    }

    #[test]
    fn binance_depth_chain_unbroken_reports_zero() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let lines = [
            raw_event_line(
                Venue::Binance,
                "btcusdt@depth@100ms",
                100,
                &depth_diff_payload(100, 110),
            ),
            raw_event_line(
                Venue::Binance,
                "btcusdt@depth@100ms",
                200,
                &depth_diff_payload(111, 120), // u_prev + 1 = 111 ✓
            ),
            raw_event_line(
                Venue::Binance,
                "btcusdt@depth@100ms",
                300,
                &depth_diff_payload(121, 130), // u_prev + 1 = 121 ✓
            ),
        ];
        let refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
        write_file(&session, "binance", "btcusdt_depth_100ms.0000.ndjson", &refs);
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.sequence.binance_depth_chain_breaks, 0);
    }

    #[test]
    fn binance_depth_chain_break_detected() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let lines = [
            raw_event_line(
                Venue::Binance,
                "btcusdt@depth@100ms",
                100,
                &depth_diff_payload(100, 110),
            ),
            raw_event_line(
                Venue::Binance,
                "btcusdt@depth@100ms",
                200,
                &depth_diff_payload(200, 210), // gap! 200 != 110 + 1
            ),
        ];
        let refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
        write_file(&session, "binance", "btcusdt_depth_100ms.0000.ndjson", &refs);
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.sequence.binance_depth_chain_breaks, 1);
    }

    #[test]
    fn verbose_populates_details() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        write_file(&session, "binance", "btcusdt_trade.0000.ndjson", &[]);
        let r = check_session(&sd, true).unwrap();
        assert_eq!(r.details.len(), 1);
        assert_eq!(r.details[0].kind, FindingKind::EmptyFile);
    }

    #[test]
    fn non_verbose_leaves_details_empty() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        write_file(&session, "binance", "btcusdt_trade.0000.ndjson", &[]);
        let r = check_session(&sd, false).unwrap();
        assert!(r.details.is_empty());
    }
}
