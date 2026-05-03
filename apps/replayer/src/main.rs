//! Replayer CLI binary entry point.
//!
//! Sub-commands (see `cli::USAGE` for the full surface):
//!
//! * `sessions` — list session dirs under `--root`
//! * `count`    — count events matching the filter
//! * `head`     — first N events as NDJSON
//! * `tail`     — last N events as NDJSON
//! * `dump`     — write filtered events to a Parquet file
//! * `schema`   — print the Parquet export schema
//!
//! Exit codes (matching `apps/recorder`):
//! * 0  success
//! * 2  bad CLI args
//! * 4  IO / replay error

mod cli;

use std::collections::VecDeque;
use std::path::Path;
use std::process::ExitCode;

use common::ResolutionRecord;
use replayer::integrity::{check_session, FindingKind, SessionIntegrity};
use replayer::{open_base_dir, open_session, ReplayError, ReplayFilter, SessionDir};

use crate::cli::{Command, CliError};

fn main() -> ExitCode {
    let cmd = match cli::parse() {
        Ok(c) => c,
        Err(CliError::Usage(msg)) => {
            eprintln!("{msg}");
            return ExitCode::from(2);
        }
    };

    let result = match cmd {
        Command::Sessions { root } => run_sessions(&root),
        Command::Count { root, filter } => run_count(&root, filter),
        Command::Head { root, filter, n } => run_head(&root, filter, n),
        Command::Tail { root, filter, n } => run_tail(&root, filter, n),
        Command::Dump { root, filter, out } => run_dump(&root, filter, &out),
        Command::Integrity { root, verbose, json } => run_integrity(&root, verbose, json),
        Command::ValidateResolutions { root } => run_validate_resolutions(&root),
        Command::Schema => run_schema(),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(4)
        }
    }
}

/// Open `root` either as a single session dir or as a base dir holding many.
/// We try `from_path` first; if that says "not a session dir", fall back to
/// `discover` (base dir).
fn open_any(
    root: &Path,
    filter: ReplayFilter,
) -> Result<replayer::MergedReader, ReplayError> {
    match SessionDir::from_path(root) {
        Ok(_) => open_session(root, filter),
        Err(_) => open_base_dir(root, filter),
    }
}

fn run_sessions(root: &Path) -> Result<(), ReplayError> {
    // List immediate session dirs under `root`. If `root` IS a session dir,
    // list it as a single entry.
    let sessions = match SessionDir::from_path(root) {
        Ok(sd) => vec![sd],
        Err(_) => SessionDir::discover(root)?,
    };
    println!("{:<22}  {:>8}  {}", "session", "files", "path");
    for sd in &sessions {
        let files = sd.list_files()?;
        println!(
            "{:<22}  {:>8}  {}",
            sd.start_utc,
            files.len(),
            sd.path.display()
        );
    }
    Ok(())
}

fn run_count(root: &Path, filter: ReplayFilter) -> Result<(), ReplayError> {
    let mut n = 0usize;
    for ev in open_any(root, filter)? {
        ev?; // surface read errors
        n += 1;
    }
    println!("{n}");
    Ok(())
}

fn run_head(root: &Path, filter: ReplayFilter, n: usize) -> Result<(), ReplayError> {
    let mut count = 0usize;
    for ev in open_any(root, filter)? {
        if count >= n {
            break;
        }
        let ev = ev?;
        println!(
            "{}",
            serde_json::to_string(&ev).expect("RawEvent always serialises")
        );
        count += 1;
    }
    Ok(())
}

fn run_tail(root: &Path, filter: ReplayFilter, n: usize) -> Result<(), ReplayError> {
    // Streaming tail with a bounded ring buffer — never holds more than N events.
    let mut ring: VecDeque<replayer::RawEvent> = VecDeque::with_capacity(n.max(1));
    for ev in open_any(root, filter)? {
        let ev = ev?;
        if ring.len() == n {
            ring.pop_front();
        }
        ring.push_back(ev);
    }
    for ev in ring {
        println!(
            "{}",
            serde_json::to_string(&ev).expect("RawEvent always serialises")
        );
    }
    Ok(())
}

fn run_dump(root: &Path, filter: ReplayFilter, out: &Path) -> Result<(), ReplayError> {
    let merger = open_any(root, filter)?;
    let n = replayer::parquet::dump(out, merger)?;
    eprintln!("wrote {n} rows to {}", out.display());
    Ok(())
}

fn run_schema() -> Result<(), ReplayError> {
    let s = replayer::parquet::schema();
    // Print as the Schema's pretty form. Researchers typically pipe
    // this into a doc / commit message rather than parse it.
    println!("{}", s);
    Ok(())
}

/// Validate that each session's `_resolutions.ndjson` sidecar (and/or
/// any legacy `<slug>-resolved.0000.ndjson` files) is present and
/// parseable. PASS / WARN / FAIL per session, plus a final summary.
///
/// PASS: sidecar present, every line parses, schema_version recognised.
/// WARN: sidecar absent but legacy resolved files present (old session
///       captured before the v1 sweeper rewrite); legacy 0-byte files
///       are reported but not failures since they predate the fix.
/// FAIL: sidecar present but contains malformed records.
fn run_validate_resolutions(root: &Path) -> Result<(), ReplayError> {
    let sessions: Vec<SessionDir> = match SessionDir::from_path(root) {
        Ok(sd) => vec![sd],
        Err(_) => SessionDir::discover(root)?,
    };

    let mut total_sessions = 0usize;
    let mut total_pass = 0usize;
    let mut total_warn = 0usize;
    let mut total_fail = 0usize;

    for sd in &sessions {
        total_sessions += 1;
        let res = validate_session_resolutions(&sd.path);
        match res.verdict.as_str() {
            "PASS" => total_pass += 1,
            "WARN" => total_warn += 1,
            "FAIL" => total_fail += 1,
            _ => {}
        }
        println!("{}: {}", sd.start_utc, res.verdict);
        println!("  sidecar:           {}", res.sidecar_state);
        println!("  sidecar lines:     {}", res.sidecar_lines);
        println!("  parsed records:    {}", res.parsed_records);
        println!("  parse errors:      {}", res.parse_errors);
        if !res.schema_versions.is_empty() {
            println!(
                "  schema versions:   {}",
                res.schema_versions
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        println!(
            "  legacy resolved files (informational):  total={} non-empty={} zero-byte={}",
            res.legacy_total_files, res.legacy_nonempty_files, res.legacy_zero_byte_files
        );
        for note in &res.notes {
            println!("  note: {note}");
        }
    }

    println!();
    println!(
        "summary: {} sessions, {} PASS, {} WARN, {} FAIL",
        total_sessions, total_pass, total_warn, total_fail
    );

    if total_fail > 0 {
        // Non-fatal but signal the failure to the caller via stderr +
        // a non-zero exit on the way out. We use ReplayError::Io as a
        // close-fitting variant; main maps any Err to exit 4.
        return Err(ReplayError::Io {
            path: root.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "one or more sessions failed validation",
            ),
        });
    }
    Ok(())
}

#[derive(Default)]
struct ValidateResult {
    verdict: String,
    sidecar_state: String,
    sidecar_lines: usize,
    parsed_records: usize,
    parse_errors: usize,
    schema_versions: Vec<u32>,
    legacy_total_files: usize,
    legacy_nonempty_files: usize,
    legacy_zero_byte_files: usize,
    notes: Vec<String>,
}

fn validate_session_resolutions(session_dir: &Path) -> ValidateResult {
    use std::fs;
    use std::io::BufRead;
    let mut r = ValidateResult::default();
    let sidecar_path = session_dir.join("_resolutions.ndjson");
    let mut versions = std::collections::BTreeSet::new();

    if sidecar_path.is_file() {
        let size = fs::metadata(&sidecar_path).map(|m| m.len()).unwrap_or(0);
        r.sidecar_state = format!("present ({} bytes)", size);
        if let Ok(file) = fs::File::open(&sidecar_path) {
            let reader = std::io::BufReader::new(file);
            for line in reader.lines() {
                match line {
                    Ok(l) => {
                        if l.trim().is_empty() {
                            continue;
                        }
                        r.sidecar_lines += 1;
                        match serde_json::from_str::<ResolutionRecord>(&l) {
                            Ok(rec) => {
                                r.parsed_records += 1;
                                versions.insert(rec.schema_version);
                            }
                            Err(e) => {
                                r.parse_errors += 1;
                                if r.notes.len() < 3 {
                                    r.notes.push(format!(
                                        "parse error on line {}: {}",
                                        r.sidecar_lines, e
                                    ));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        r.parse_errors += 1;
                        r.notes.push(format!("read error: {}", e));
                        break;
                    }
                }
            }
        } else {
            r.notes.push("sidecar exists but couldn't open for reading".into());
        }
    } else {
        r.sidecar_state = "absent".into();
    }
    r.schema_versions = versions.into_iter().collect();

    // Legacy per-slug resolved files (informational).
    let poly_dir = session_dir.join("polymarket");
    if poly_dir.is_dir() {
        if let Ok(entries) = fs::read_dir(&poly_dir) {
            for e in entries.flatten() {
                let name = match e.file_name().into_string() {
                    Ok(n) => n,
                    Err(_) => continue,
                };
                if name.contains("-resolved")
                    && (name.ends_with(".ndjson") || name.ends_with(".ndjson.gz"))
                {
                    r.legacy_total_files += 1;
                    let size = e.metadata().map(|m| m.len()).unwrap_or(0);
                    if size == 0 {
                        r.legacy_zero_byte_files += 1;
                    } else {
                        r.legacy_nonempty_files += 1;
                    }
                }
            }
        }
    }

    // Verdict logic.
    r.verdict = if r.parse_errors > 0 {
        "FAIL".into()
    } else if !sidecar_path.is_file() {
        if r.legacy_nonempty_files > 0 {
            r.notes.push(
                "no sidecar but legacy resolved files present (pre-v1 sweeper)".into(),
            );
            "WARN".into()
        } else if r.legacy_zero_byte_files > 0 {
            r.notes.push(format!(
                "{} legacy resolved files exist but are 0 bytes (the bug we fixed)",
                r.legacy_zero_byte_files
            ));
            "WARN".into()
        } else {
            r.notes.push("no resolution data of any kind".into());
            "WARN".into()
        }
    } else if r.parsed_records == 0 {
        r.notes.push("sidecar present but empty".into());
        "WARN".into()
    } else {
        "PASS".into()
    };

    r
}

fn run_integrity(root: &Path, verbose: bool, json: bool) -> Result<(), ReplayError> {
    // Single session dir → check it. Otherwise treat as base dir and
    // emit one summary per discovered session, in chronological order.
    let sessions: Vec<SessionDir> = match SessionDir::from_path(root) {
        Ok(sd) => vec![sd],
        Err(_) => SessionDir::discover(root)?,
    };

    for (i, sd) in sessions.iter().enumerate() {
        let report = check_session(sd, verbose)?;
        if json {
            let line = serde_json::to_string(&report).expect("integrity report serialises");
            println!("{line}");
        } else {
            if i > 0 {
                println!();
            }
            print_text_report(&report);
        }
    }
    Ok(())
}

fn print_text_report(r: &SessionIntegrity) {
    println!("{}", r.session_name);
    println!("{}", "=".repeat(r.session_name.len()));
    println!(
        "scanned {} files, {} events in {:.1}s",
        r.files_scanned, r.total_events, r.elapsed_secs
    );
    println!();

    println!("VERDICT: {}", r.verdict);
    if !r.verdict_reason.is_empty() {
        println!("  reason: {}", r.verdict_reason);
    }
    println!();

    println!("per-venue");
    for (venue, stats) in &r.per_venue {
        println!(
            "  {:<12} {:>6} streams   {:>12} events",
            venue, stats.streams, stats.events
        );
    }
    println!();

    println!("structural");
    println!("  empty files                       {}", r.structural.empty_files);
    println!(
        "  resolution-sweeper 0-byte files   {}  (legacy pre-v1 sweeper; v1+ uses _resolutions.ndjson)",
        r.structural.resolution_zero_byte_files
    );
    println!(
        "  tail-truncated files              {}  (last-line parse error only -- recoverable)",
        r.structural.tail_truncated_files
    );
    println!(
        "  interspersed-corrupt files        {}  (mid-file parse errors -- DATA AFTER MAY BE WRONG)",
        r.structural.interspersed_corrupt_files
    );
    println!("  _unrouted files                   {}", r.structural.unrouted_files);
    println!(
        "  _unknown_market-* files           {}",
        r.structural.unknown_market_files
    );
    println!(
        "  _unknown_token-* files            {}",
        r.structural.unknown_token_files
    );
    println!("  bucket gaps                       {}", r.structural.bucket_gaps);
    println!("  parse errors                      {}", r.structural.parse_errors);
    println!("  ts violations (per file)          {}", r.structural.ts_violations);
    println!();

    println!("decoder");
    println!("  decode errors                     {}", r.decoder.decode_errors);
    println!(
        "  Unknown variants                  {}  (informational; subscription acks etc. expected)",
        r.decoder.unknown_variants
    );
    println!();

    println!("sequence integrity");
    println!(
        "  Binance depth chain breaks        {}",
        r.sequence.binance_depth_chain_breaks
    );
    println!(
        "  Binance depth snapshots observed  {}  (each snapshot resets the chain;",
        r.sequence.binance_depth_snapshots_observed
    );
    println!("                                       breaks <= snapshots -> expected reconnect/refresh,");
    println!("                                       breaks  > snapshots -> real packet loss)");
    println!(
        "  Binance bookTicker update_id breaks {}  (out of {} observed)",
        r.sequence.binance_book_ticker_update_id_breaks,
        r.sequence.binance_book_ticker_observed
    );
    println!(
        "  Polymarket per-asset ts violations {}",
        r.sequence.polymarket_per_asset_ts_violations
    );
    println!(
        "  Polymarket hash records           {}  (with hash: {}, consec dup: {})",
        r.sequence.polymarket_hash_records_observed,
        r.sequence.polymarket_hash_records_with_hash,
        r.sequence.polymarket_hash_duplicate_consecutive
    );
    println!(
        "  Coinbase trade_id breaks          {}  (out of {} observed)",
        r.sequence.coinbase_trade_id_breaks,
        r.sequence.coinbase_trade_id_observed
    );
    println!();

    if let Some(d) = &r.binance_arrival_delta {
        println!("binance arrival delta (local_ts_ns - venue_ts_ms*1e6)");
        println!(
            "  n={}  min={}ms  p10={}ms  p50={}ms  p90={}ms  p99={}ms  max={}ms",
            d.n, d.min_ms, d.p10_ms, d.p50_ms, d.p90_ms, d.p99_ms, d.max_ms
        );
        println!();
    }

    if !r.safe_replay_cutoff_ns.is_empty() {
        println!("safe replay cutoff (last clean event ns since epoch, per venue)");
        for (venue, ns) in &r.safe_replay_cutoff_ns {
            println!("  {:<12} {}", venue, ns);
        }
        println!();
    }

    if !r.details.is_empty() {
        println!("details");
        for f in &r.details {
            let kind = match f.kind {
                FindingKind::EmptyFile => "EmptyFile",
                FindingKind::ResolutionZeroByte => "ResolutionZeroByte",
                FindingKind::TailTruncated => "TailTruncated",
                FindingKind::InterspersedCorrupt => "InterspersedCorrupt",
                FindingKind::UnroutedFile => "UnroutedFile",
                FindingKind::UnknownMarketFile => "UnknownMarketFile",
                FindingKind::UnknownTokenFile => "UnknownTokenFile",
                FindingKind::BucketGap => "BucketGap",
                FindingKind::ParseError => "ParseError",
                FindingKind::TsViolation => "TsViolation",
                FindingKind::DecodeError => "DecodeError",
                FindingKind::BinanceDepthChainBreak => "BinanceDepthChainBreak",
                FindingKind::BinanceBookTickerUpdateIdBreak => "BinanceBookTickerUpdateIdBreak",
                FindingKind::PolymarketPerAssetTsViolation => "PolymarketPerAssetTsViolation",
                FindingKind::PolymarketHashDuplicate => "PolymarketHashDuplicate",
                FindingKind::CoinbaseTradeIdBreak => "CoinbaseTradeIdBreak",
            };
            if f.note.is_empty() {
                println!("  {} -- {} ({})", f.path.display(), kind, f.count);
            } else {
                println!(
                    "  {} -- {} ({}, {})",
                    f.path.display(),
                    kind,
                    f.count,
                    f.note
                );
            }
        }
    }
}
