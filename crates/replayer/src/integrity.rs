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

    /// Network-arrival vs exchange-emit delta. Populated for venues where
    /// every event carries `venue_ts_ms` (Binance trades + depth diffs).
    /// Polymarket and Coinbase have inconsistent venue timestamps so are
    /// not summarised here.
    pub binance_arrival_delta: Option<TimingDelta>,

    /// Last clean event timestamp per venue (ns since epoch as string).
    /// "Clean" = before any structural or sequence issue surfaced for
    /// that venue. Use as a safe upper bound for replay/analysis.
    /// Stringified for the same precision-preserving reason
    /// `RawEvent.local_ts_ns` is.
    pub safe_replay_cutoff_ns: BTreeMap<String, String>,

    /// PASS / WARN / FAIL.
    pub verdict: String,
    /// Short human-readable reason for the verdict.
    pub verdict_reason: String,

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
    /// Subset of empty_files: per-slug `*-resolved.ndjson` files left empty
    /// by the legacy sweeper. Always 0 for sessions captured under the v1
    /// sidecar sweeper.
    pub resolution_zero_byte_files: u64,
    /// Files with a tail-only parse error (last line malformed, all
    /// preceding lines parse). Classic partial-write signature, recoverable
    /// by replay-truncating at the last valid line.
    pub tail_truncated_files: u64,
    /// Files with mid-file parse errors (any non-final line fails to parse).
    /// Concerning because data after the bad line may be misaligned.
    pub interspersed_corrupt_files: u64,
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

    /// Binance bookTicker `update_id` non-monotonic deltas. The wire
    /// guarantees strictly increasing update_id; any decrease or repeat
    /// (after the first event) is a real signal of dropped or reordered
    /// frames.
    pub binance_book_ticker_update_id_breaks: u64,
    pub binance_book_ticker_observed: u64,

    /// Polymarket per-asset timestamp_ms non-monotonic events. Many events
    /// share the same wire-frame timestamp by design (array demux), so we
    /// only count strict decreases. Missing timestamp_ms is *not* counted.
    pub polymarket_per_asset_ts_violations: u64,
    /// Polymarket book + price_change items observed (that carry a
    /// `hash` field). Recorded so callers can tell whether the next two
    /// fields are statistically meaningful.
    pub polymarket_hash_records_observed: u64,
    /// Records with a hash field set. `polymarket_hash_records_observed -
    /// polymarket_hash_records_with_hash = records missing hash`.
    pub polymarket_hash_records_with_hash: u64,
    /// Consecutive identical hashes for the *same* `asset_id`. Polymarket
    /// emits a fresh hash on every state change; consecutive duplicates
    /// suggest either a genuinely-unchanged book or a wire issue.
    pub polymarket_hash_duplicate_consecutive: u64,

    /// Coinbase trade_id non-monotonic events. Trade ids are strings on
    /// the wire; we parse to u64 when possible and compare numerically.
    pub coinbase_trade_id_breaks: u64,
    pub coinbase_trade_id_observed: u64,
}

/// Distribution summary of the delta `local_ts_ns - venue_ts_ms*1e6` for
/// venues that publish an exchange-side timestamp on every event. Useful
/// for (a) detecting routing changes / regional failover (median jumps),
/// (b) bounding the noise floor of any sub-second timing analysis.
#[derive(Debug, Clone, Default, Serialize)]
pub struct TimingDelta {
    pub n: u64,
    pub min_ms: i64,
    pub max_ms: i64,
    pub p10_ms: i64,
    pub p50_ms: i64,
    pub p90_ms: i64,
    pub p99_ms: i64,
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
    TailTruncated,
    InterspersedCorrupt,
    UnroutedFile,
    UnknownMarketFile,
    UnknownTokenFile,
    BucketGap,
    ParseError,
    TsViolation,
    DecodeError,
    BinanceDepthChainBreak,
    BinanceBookTickerUpdateIdBreak,
    PolymarketPerAssetTsViolation,
    PolymarketHashDuplicate,
    CoinbaseTradeIdBreak,
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
        binance_arrival_delta: None,
        safe_replay_cutoff_ns: BTreeMap::new(),
        verdict: String::new(),
        verdict_reason: String::new(),
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
    let mut book_ticker_state: BTreeMap<(Venue, String), Option<u64>> = BTreeMap::new();
    let mut coinbase_trade_id_state: BTreeMap<(Venue, String), Option<u64>> = BTreeMap::new();
    // Polymarket per-asset state (asset_id -> last_ts_ms / last_hash).
    let mut poly_asset_ts_state: BTreeMap<String, i64> = BTreeMap::new();
    let mut poly_asset_hash_state: BTreeMap<String, String> = BTreeMap::new();

    // Binance arrival-delta sample buffer. Capped to keep memory bounded
    // (a 13h capture has ~3M trade events; storing all i64 deltas = 24 MB,
    // not catastrophic but unnecessary). Reservoir-sampled.
    const BIN_DELTA_CAP: usize = 100_000;
    let mut bin_delta_samples: Vec<i64> = Vec::with_capacity(BIN_DELTA_CAP);
    let mut bin_delta_count: u64 = 0;

    // Per-venue last cleanly-scanned event ts. "Cleanly" = no parse error
    // in this event, no ts violation in this event, no decode error, and
    // no sequence break attributable to this event. Once a venue records
    // an issue, its last_clean_ns *stops advancing*.
    let mut last_clean_ns_by_venue: BTreeMap<Venue, u128> = BTreeMap::new();
    let mut venue_dirty: BTreeMap<Venue, bool> = BTreeMap::new();

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
        // Tail-truncation detection: track whether we've seen a parse error,
        // whether the last iteration was a parse error, and whether any
        // event came AFTER a parse error.
        let mut had_parse_error = false;
        let mut last_was_parse_error = false;
        let mut had_event_after_parse_error = false;

        let depth_state = depth_chain_state
            .entry((f.venue, f.stream.clone()))
            .or_insert(None);

        let reader = FileReader::open(&f.path)?;
        for next in reader {
            match next {
                Err(ReplayError::ParseLine { .. }) => {
                    report.structural.parse_errors += 1;
                    local_parse_errors_in_file += 1;
                    had_parse_error = true;
                    last_was_parse_error = true;
                    venue_dirty.insert(f.venue, true);
                    continue;
                }
                Err(other) => return Err(other),
                Ok((_line_no, event)) => {
                    if last_was_parse_error {
                        had_event_after_parse_error = true;
                    }
                    last_was_parse_error = false;

                    report.total_events += 1;
                    if let Some(stats) =
                        report.per_venue.get_mut(event.venue.as_str())
                    {
                        stats.events += 1;
                    }
                    let ts = event.local_ts_ns.as_nanos();

                    let mut event_has_issue = false;

                    // Per-file ts ordering.
                    if let Some(prev) = last_local_ts_ns {
                        if ts < prev {
                            report.structural.ts_violations += 1;
                            local_ts_violations_in_file += 1;
                            event_has_issue = true;
                        }
                    }
                    last_local_ts_ns = Some(ts);

                    // Binance arrival delta: local_ts_ns - venue_ts_ms*1e6.
                    if event.venue == Venue::Binance {
                        if let Some(venue_ms) = event.venue_ts_ms {
                            let local_ms = (ts / 1_000_000) as i64;
                            let delta_ms = local_ms - venue_ms;
                            bin_delta_count += 1;
                            if bin_delta_samples.len() < BIN_DELTA_CAP {
                                bin_delta_samples.push(delta_ms);
                            } else {
                                // Reservoir sample: each new element gets a
                                // 1/n chance of replacing a random slot.
                                let n = bin_delta_count as usize;
                                let r = pseudo_rand_index(n);
                                if r < BIN_DELTA_CAP {
                                    bin_delta_samples[r] = delta_ms;
                                }
                            }
                        }
                    }

                    // Decoder + sequence checks.
                    match decode(&event) {
                        Err(_) => {
                            report.decoder.decode_errors += 1;
                            local_decode_errors_in_file += 1;
                            event_has_issue = true;
                        }
                        Ok(DecodedEvent::Unknown { .. }) => {
                            report.decoder.unknown_variants += 1;
                        }
                        Ok(DecodedEvent::BinanceDepthSnapshot(_)) => {
                            report.sequence.binance_depth_snapshots_observed += 1;
                            // A snapshot resets the chain — clearing the
                            // stale state prevents the next diff from
                            // wrongly counting as a break.
                            *depth_state = None;
                        }
                        Ok(DecodedEvent::BinanceDepthDiff(d)) => {
                            if let Some(prev_u) = *depth_state {
                                if d.first_update_id != prev_u + 1 {
                                    report.sequence.binance_depth_chain_breaks += 1;
                                    local_chain_breaks_in_file += 1;
                                    // Chain breaks are expected at reconnect
                                    // boundaries (we have a snapshot count
                                    // to distinguish), so don't poison the
                                    // venue safe-cutoff.
                                }
                            }
                            *depth_state = Some(d.final_update_id);
                        }
                        Ok(DecodedEvent::BinanceBookTicker(bt)) => {
                            report.sequence.binance_book_ticker_observed += 1;
                            let key = (event.venue, event.stream.clone());
                            let entry = book_ticker_state.entry(key).or_insert(None);
                            if let Some(prev_u) = *entry {
                                if bt.update_id <= prev_u {
                                    report.sequence.binance_book_ticker_update_id_breaks += 1;
                                    event_has_issue = true;
                                }
                            }
                            *entry = Some(bt.update_id);
                        }
                        Ok(DecodedEvent::PolymarketBook(b)) => {
                            check_poly_per_asset_ts(
                                &b.asset_id,
                                b.timestamp_ms,
                                &mut poly_asset_ts_state,
                                &mut report.sequence.polymarket_per_asset_ts_violations,
                                &mut event_has_issue,
                            );
                            check_poly_hash(
                                &b.asset_id,
                                b.hash.as_deref(),
                                &mut poly_asset_hash_state,
                                &mut report.sequence,
                            );
                        }
                        Ok(DecodedEvent::PolymarketPriceChange(pc)) => {
                            for it in &pc.price_changes {
                                check_poly_per_asset_ts(
                                    &it.asset_id,
                                    pc.timestamp_ms,
                                    &mut poly_asset_ts_state,
                                    &mut report.sequence.polymarket_per_asset_ts_violations,
                                    &mut event_has_issue,
                                );
                                check_poly_hash(
                                    &it.asset_id,
                                    it.hash.as_deref(),
                                    &mut poly_asset_hash_state,
                                    &mut report.sequence,
                                );
                            }
                        }
                        Ok(DecodedEvent::CoinbaseMarketTrades(cm)) => {
                            // CoinbaseMarketTrades wraps inner events with
                            // a `snapshot` (reconnect) batch + `update`
                            // batches. `flatten()` iterates trades across
                            // all inner batches. A `snapshot` batch is a
                            // resync and resets the chain; we treat it as
                            // a baseline and don't count breaks against
                            // events seen after it.
                            let key = (event.venue, event.stream.clone());
                            let entry =
                                coinbase_trade_id_state.entry(key).or_insert(None);
                            let is_snapshot = cm
                                .events
                                .iter()
                                .any(|b| b.kind.eq_ignore_ascii_case("snapshot"));
                            if is_snapshot {
                                *entry = None;
                            }
                            for tr in cm.flatten() {
                                report.sequence.coinbase_trade_id_observed += 1;
                                if let Ok(id) = tr.trade_id.parse::<u64>() {
                                    if let Some(prev) = *entry {
                                        if id <= prev {
                                            report.sequence.coinbase_trade_id_breaks += 1;
                                            event_has_issue = true;
                                        }
                                    }
                                    *entry = Some(id);
                                }
                            }
                        }
                        Ok(_) => {}
                    }

                    if event_has_issue {
                        venue_dirty.insert(event.venue, true);
                    } else if !venue_dirty.get(&event.venue).copied().unwrap_or(false) {
                        let entry = last_clean_ns_by_venue.entry(event.venue).or_insert(0);
                        if ts > *entry {
                            *entry = ts;
                        }
                    }
                }
            }
        }

        // Classify file-level corruption (tail-only vs interspersed).
        if had_parse_error {
            if had_event_after_parse_error {
                report.structural.interspersed_corrupt_files += 1;
                if verbose {
                    report.details.push(FileFinding {
                        path: f.path.clone(),
                        kind: FindingKind::InterspersedCorrupt,
                        count: local_parse_errors_in_file,
                        note: "non-final parse errors -- data after them may be misaligned".into(),
                    });
                }
            } else if last_was_parse_error {
                report.structural.tail_truncated_files += 1;
                if verbose {
                    report.details.push(FileFinding {
                        path: f.path.clone(),
                        kind: FindingKind::TailTruncated,
                        count: local_parse_errors_in_file,
                        note: "tail truncation -- recoverable, replay can stop at last valid line".into(),
                    });
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

    // ---- Aggregation ----
    if !bin_delta_samples.is_empty() {
        bin_delta_samples.sort_unstable();
        let n = bin_delta_samples.len();
        let pick = |q: f64| -> i64 {
            let idx = ((n as f64 - 1.0) * q) as usize;
            bin_delta_samples[idx]
        };
        report.binance_arrival_delta = Some(TimingDelta {
            n: bin_delta_count,
            min_ms: bin_delta_samples[0],
            max_ms: bin_delta_samples[n - 1],
            p10_ms: pick(0.10),
            p50_ms: pick(0.50),
            p90_ms: pick(0.90),
            p99_ms: pick(0.99),
        });
    }
    for (venue, ts) in &last_clean_ns_by_venue {
        report
            .safe_replay_cutoff_ns
            .insert(venue.as_str().to_string(), ts.to_string());
    }

    // Verdict.
    let s = &report.structural;
    let q = &report.sequence;
    let book_ticker_breaks_unexplained = q.binance_book_ticker_update_id_breaks;
    // Depth chain breaks within snapshot count are "expected reconnect/refresh".
    let depth_breaks_unexplained = q
        .binance_depth_chain_breaks
        .saturating_sub(q.binance_depth_snapshots_observed);

    if s.interspersed_corrupt_files > 0 {
        report.verdict = "FAIL".into();
        report.verdict_reason = format!(
            "{} file(s) with mid-file parse errors -- data after the bad lines may be misaligned",
            s.interspersed_corrupt_files
        );
    } else if depth_breaks_unexplained > 0
        || book_ticker_breaks_unexplained > 0
        || q.coinbase_trade_id_breaks > 0
        || q.polymarket_per_asset_ts_violations > 0
        || s.ts_violations > 0
        || report.decoder.decode_errors > 0
    {
        report.verdict = "WARN".into();
        report.verdict_reason = format!(
            "tail_truncated_files={}, depth_breaks_unexplained={}, book_ticker_breaks={}, coinbase_trade_id_breaks={}, polymarket_ts_violations={}, ts_violations={}, decode_errors={}",
            s.tail_truncated_files,
            depth_breaks_unexplained,
            book_ticker_breaks_unexplained,
            q.coinbase_trade_id_breaks,
            q.polymarket_per_asset_ts_violations,
            s.ts_violations,
            report.decoder.decode_errors,
        );
    } else if s.tail_truncated_files > 0 {
        report.verdict = "WARN".into();
        report.verdict_reason = format!(
            "{} file(s) with tail-only truncation -- recoverable",
            s.tail_truncated_files
        );
    } else {
        report.verdict = "PASS".into();
        report.verdict_reason = "no integrity issues detected".into();
    }

    report.elapsed_secs = started.elapsed().as_secs_f64();
    Ok(report)
}

/// One Polymarket per-asset timestamp_ms ordering check. `ts` is the
/// best timestamp available for this event (book.timestamp_ms or
/// price_change.timestamp_ms). Missing timestamps are not counted; many
/// real events legitimately omit it.
fn check_poly_per_asset_ts(
    asset_id: &str,
    ts: Option<i64>,
    state: &mut BTreeMap<String, i64>,
    counter: &mut u64,
    event_has_issue: &mut bool,
) {
    if let Some(t) = ts {
        let entry = state.entry(asset_id.to_string()).or_insert(t);
        if t < *entry {
            *counter += 1;
            *event_has_issue = true;
        } else {
            *entry = t;
        }
    }
}

fn check_poly_hash(
    asset_id: &str,
    hash: Option<&str>,
    state: &mut BTreeMap<String, String>,
    seq: &mut SequenceIssues,
) {
    seq.polymarket_hash_records_observed += 1;
    if let Some(h) = hash {
        seq.polymarket_hash_records_with_hash += 1;
        let entry = state.entry(asset_id.to_string()).or_insert_with(String::new);
        if !entry.is_empty() && entry == h {
            seq.polymarket_hash_duplicate_consecutive += 1;
        }
        *entry = h.to_string();
    }
}

/// Tiny PRNG-free index sampler for reservoir sampling. Not crypto-strength —
/// used purely to spread sample replacement evenly across the stream so
/// quantile estimates aren't biased toward early samples.
fn pseudo_rand_index(n: usize) -> usize {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as usize)
        .unwrap_or(0);
    nanos.wrapping_mul(2_654_435_769) % n.max(1)
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
            ..Default::default()
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

    // -----------------------------------------------------------------
    // v2: tail-truncated vs interspersed-corrupt classification
    // -----------------------------------------------------------------

    #[test]
    fn tail_truncated_file_classified() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let good = raw_event_line(Venue::Binance, "btcusdt@trade", 100, r#"{"e":"trade"}"#);
        // Last line is malformed — classic partial-write signature.
        write_file(
            &session,
            "binance",
            "btcusdt_trade.0000.ndjson",
            &[&good, &good, "this is not json"],
        );
        let r = check_session(&sd, true).unwrap();
        assert_eq!(r.structural.tail_truncated_files, 1);
        assert_eq!(r.structural.interspersed_corrupt_files, 0);
        assert_eq!(r.structural.parse_errors, 1);
        assert!(r.details.iter().any(|d| d.kind == FindingKind::TailTruncated));
    }

    #[test]
    fn interspersed_corruption_classified_separately() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let good = raw_event_line(Venue::Binance, "btcusdt@trade", 100, r#"{"e":"trade"}"#);
        // Bad line in the middle, followed by a good line.
        write_file(
            &session,
            "binance",
            "btcusdt_trade.0000.ndjson",
            &[&good, "this is not json", &good],
        );
        let r = check_session(&sd, true).unwrap();
        assert_eq!(r.structural.tail_truncated_files, 0);
        assert_eq!(r.structural.interspersed_corrupt_files, 1);
        assert!(r.details.iter().any(|d| d.kind == FindingKind::InterspersedCorrupt));
    }

    // -----------------------------------------------------------------
    // v2: Binance bookTicker update_id monotonicity
    // -----------------------------------------------------------------

    fn book_ticker_payload(u: u64) -> String {
        format!(
            r#"{{"u":{u},"s":"BTCUSDT","b":"100","B":"1","a":"101","A":"1"}}"#
        )
    }

    #[test]
    fn binance_book_ticker_monotonic_passes() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let lines = [
            raw_event_line(Venue::Binance, "btcusdt@bookTicker", 100, &book_ticker_payload(10)),
            raw_event_line(Venue::Binance, "btcusdt@bookTicker", 110, &book_ticker_payload(11)),
            raw_event_line(Venue::Binance, "btcusdt@bookTicker", 120, &book_ticker_payload(12)),
        ];
        let refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
        write_file(&session, "binance", "btcusdt_bookTicker.0000.ndjson", &refs);
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.sequence.binance_book_ticker_observed, 3);
        assert_eq!(r.sequence.binance_book_ticker_update_id_breaks, 0);
    }

    #[test]
    fn binance_book_ticker_regression_caught() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let lines = [
            raw_event_line(Venue::Binance, "btcusdt@bookTicker", 100, &book_ticker_payload(10)),
            raw_event_line(Venue::Binance, "btcusdt@bookTicker", 110, &book_ticker_payload(8)), // regress
        ];
        let refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
        write_file(&session, "binance", "btcusdt_bookTicker.0000.ndjson", &refs);
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.sequence.binance_book_ticker_observed, 2);
        assert_eq!(r.sequence.binance_book_ticker_update_id_breaks, 1);
    }

    // -----------------------------------------------------------------
    // v2: Coinbase trade_id monotonicity
    // -----------------------------------------------------------------

    fn coinbase_payload(trade_ids: &[u64], kind: &str) -> String {
        let trades = trade_ids
            .iter()
            .map(|id| {
                format!(
                    r#"{{"trade_id":"{id}","product_id":"BTC-USD","side":"BUY","price":"100","size":"1","time":"2026-01-01T00:00:00Z"}}"#
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!(
            r#"{{"channel":"market_trades","sequence_num":1,"timestamp":"2026-01-01T00:00:00Z","events":[{{"type":"{kind}","trades":[{trades}]}}]}}"#
        )
    }

    #[test]
    fn coinbase_trade_id_monotonic_passes() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let lines = [
            raw_event_line(Venue::Coinbase, "btc-usd@market_trades", 100, &coinbase_payload(&[100, 101], "update")),
            raw_event_line(Venue::Coinbase, "btc-usd@market_trades", 110, &coinbase_payload(&[102], "update")),
        ];
        let refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
        write_file(&session, "coinbase", "btc-usd_market_trades.0000.ndjson", &refs);
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.sequence.coinbase_trade_id_observed, 3);
        assert_eq!(r.sequence.coinbase_trade_id_breaks, 0);
    }

    #[test]
    fn coinbase_trade_id_break_detected() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let lines = [
            raw_event_line(Venue::Coinbase, "btc-usd@market_trades", 100, &coinbase_payload(&[100], "update")),
            raw_event_line(Venue::Coinbase, "btc-usd@market_trades", 110, &coinbase_payload(&[99], "update")), // regress
        ];
        let refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
        write_file(&session, "coinbase", "btc-usd_market_trades.0000.ndjson", &refs);
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.sequence.coinbase_trade_id_breaks, 1);
    }

    #[test]
    fn coinbase_snapshot_resets_chain() {
        // After a `snapshot` batch, trade_id state must be cleared so a
        // post-reconnect resync doesn't trigger a spurious break.
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let lines = [
            raw_event_line(Venue::Coinbase, "btc-usd@market_trades", 100, &coinbase_payload(&[200, 201], "update")),
            raw_event_line(Venue::Coinbase, "btc-usd@market_trades", 110, &coinbase_payload(&[100, 101], "snapshot")),
            raw_event_line(Venue::Coinbase, "btc-usd@market_trades", 120, &coinbase_payload(&[102], "update")),
        ];
        let refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
        write_file(&session, "coinbase", "btc-usd_market_trades.0000.ndjson", &refs);
        let r = check_session(&sd, false).unwrap();
        // No break: the snapshot legitimately re-baselines the chain at
        // 100, and 102 > 101 within the snapshot batch is fine, then
        // 102 (the next update) > 101 is fine too.
        assert_eq!(r.sequence.coinbase_trade_id_breaks, 0);
    }

    // -----------------------------------------------------------------
    // v2: verdict + safe replay cutoff
    // -----------------------------------------------------------------

    #[test]
    fn verdict_pass_for_clean_session() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let line = raw_event_line(Venue::Binance, "btcusdt@trade", 100, r#"{"e":"trade","E":50,"T":50,"s":"BTCUSDT","t":1,"p":"100","q":"1","m":false}"#);
        write_file(&session, "binance", "btcusdt_trade.0000.ndjson", &[&line]);
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.verdict, "PASS");
    }

    #[test]
    fn verdict_warn_for_tail_truncation() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let good = raw_event_line(Venue::Binance, "btcusdt@trade", 100, r#"{"e":"trade","E":50,"T":50,"s":"BTCUSDT","t":1,"p":"100","q":"1","m":false}"#);
        write_file(&session, "binance", "btcusdt_trade.0000.ndjson", &[&good, "garbage"]);
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.verdict, "WARN");
        assert!(r.verdict_reason.contains("tail"));
    }

    #[test]
    fn verdict_fail_for_interspersed_corruption() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let good = raw_event_line(Venue::Binance, "btcusdt@trade", 100, r#"{"e":"trade","E":50,"T":50,"s":"BTCUSDT","t":1,"p":"100","q":"1","m":false}"#);
        write_file(&session, "binance", "btcusdt_trade.0000.ndjson", &[&good, "garbage", &good]);
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.verdict, "FAIL");
    }

    #[test]
    fn safe_cutoff_advances_for_clean_events_then_freezes() {
        let tmp = TestDir::new();
        let (session, sd) = make_session(tmp.path());
        let lines = [
            raw_event_line(Venue::Binance, "btcusdt@bookTicker", 100, &book_ticker_payload(10)),
            raw_event_line(Venue::Binance, "btcusdt@bookTicker", 200, &book_ticker_payload(11)),
            // Break here (8 < 11) → venue goes dirty after this event.
            raw_event_line(Venue::Binance, "btcusdt@bookTicker", 300, &book_ticker_payload(8)),
            // Subsequent events should NOT advance the cutoff past 200.
            raw_event_line(Venue::Binance, "btcusdt@bookTicker", 400, &book_ticker_payload(12)),
        ];
        let refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
        write_file(&session, "binance", "btcusdt_bookTicker.0000.ndjson", &refs);
        let r = check_session(&sd, false).unwrap();
        assert_eq!(r.verdict, "WARN");
        let cutoff: u128 = r
            .safe_replay_cutoff_ns
            .get("binance")
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(cutoff, 200, "cutoff should freeze at the last clean event");
    }
}
