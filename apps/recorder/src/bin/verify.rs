//! Recorder-session verification tool.
//!
//! Reads a recorder session directory (`{base_dir}/session_<UTC>/`) and
//! reports whether the captured data is complete enough for FV-model
//! training. Run after the recorder has been collecting for a while:
//!
//!     cargo run -p recorder --bin recorder_verify -- data/session_20260521T180000Z
//!
//! Exits 0 if all required ingredients are present; non-zero with a
//! summary if anything is missing.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

#[derive(Default)]
struct StreamStats {
    files: usize,
    total_lines: u64,
    parse_errors: u64,
    first_ts_ns: Option<u128>,
    last_ts_ns: Option<u128>,
}

impl StreamStats {
    fn merge_file(&mut self, path: &Path) -> std::io::Result<()> {
        self.files += 1;
        let f = fs::File::open(path)?;
        let r = BufReader::new(f);
        for line in r.lines() {
            let line = match line {
                Ok(l) => l,
                Err(_) => continue,
            };
            if line.trim().is_empty() {
                continue;
            }
            self.total_lines += 1;
            // Parse just enough JSON to extract local_ts_ns.
            let v: serde_json::Value = match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(_) => {
                    self.parse_errors += 1;
                    continue;
                }
            };
            let ts_ns = v.get("local_ts_ns").and_then(|t| match t {
                serde_json::Value::String(s) => s.parse::<u128>().ok(),
                serde_json::Value::Number(n) => n.as_u64().map(|n| n as u128),
                _ => None,
            });
            if let Some(ts) = ts_ns {
                self.first_ts_ns = Some(self.first_ts_ns.map_or(ts, |x| x.min(ts)));
                self.last_ts_ns = Some(self.last_ts_ns.map_or(ts, |x| x.max(ts)));
            }
        }
        Ok(())
    }
    fn span_secs(&self) -> Option<f64> {
        match (self.first_ts_ns, self.last_ts_ns) {
            (Some(a), Some(b)) if b >= a => Some((b - a) as f64 / 1.0e9),
            _ => None,
        }
    }
}

#[derive(Default)]
struct Findings {
    fatal_missing: Vec<String>,
    warnings: Vec<String>,
    ok: Vec<String>,
}

impl Findings {
    fn miss(&mut self, s: impl Into<String>) {
        self.fatal_missing.push(s.into());
    }
    fn warn(&mut self, s: impl Into<String>) {
        self.warnings.push(s.into());
    }
    fn ok(&mut self, s: impl Into<String>) {
        self.ok.push(s.into());
    }
}

fn collect_stream(session_dir: &Path, venue: &str, stream_glob_prefix: &str) -> StreamStats {
    let mut stats = StreamStats::default();
    let venue_dir = session_dir.join(venue);
    if !venue_dir.exists() {
        return stats;
    }
    let entries = match fs::read_dir(&venue_dir) {
        Ok(e) => e,
        Err(_) => return stats,
    };
    for entry in entries.flatten() {
        let p = entry.path();
        let name = match p.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };
        if !name.starts_with(stream_glob_prefix) || !name.ends_with(".ndjson") {
            continue;
        }
        let _ = stats.merge_file(&p);
    }
    stats
}

fn count_lines(path: &Path) -> u64 {
    let f = match fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return 0,
    };
    BufReader::new(f).lines().filter(|l| l.is_ok()).count() as u64
}

fn fmt_unix_ns(ts: u128) -> String {
    let secs = (ts / 1_000_000_000) as i64;
    let nanos_part = (ts % 1_000_000_000) as u32;
    format!("{}.{:03}", secs, nanos_part / 1_000_000)
}

fn report_stream(name: &str, s: &StreamStats, min_lines: u64, findings: &mut Findings) {
    let line = match (s.first_ts_ns, s.last_ts_ns) {
        (Some(a), Some(b)) => format!(
            "{name}: {} lines across {} files, span {:.0}s [{} → {}]",
            s.total_lines,
            s.files,
            s.span_secs().unwrap_or(0.0),
            fmt_unix_ns(a),
            fmt_unix_ns(b),
        ),
        _ => format!(
            "{name}: {} lines across {} files (no timestamps parsed)",
            s.total_lines, s.files
        ),
    };
    if s.files == 0 {
        findings.miss(format!("{name}: no files found"));
    } else if s.total_lines < min_lines {
        findings.warn(format!("{line}  ⚠ below threshold {min_lines}"));
    } else if s.parse_errors > 0 {
        findings.warn(format!("{line}  ⚠ {} parse errors", s.parse_errors));
    } else {
        findings.ok(line);
    }
}

fn report_sidecar(session_dir: &Path, name: &str, min_lines: u64, findings: &mut Findings) {
    let path = session_dir.join(name);
    if !path.exists() {
        findings.miss(format!("{name}: missing"));
        return;
    }
    let lines = count_lines(&path);
    if lines < min_lines {
        findings.warn(format!("{name}: only {lines} lines (expected ≥ {min_lines})"));
    } else {
        findings.ok(format!("{name}: {lines} lines"));
    }
}

fn report_meta(session_dir: &Path, findings: &mut Findings) {
    let path = session_dir.join("_session_meta.json");
    if !path.exists() {
        findings.warn("_session_meta.json: missing");
        return;
    }
    let s = match fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) => {
            findings.warn(format!("_session_meta.json: unreadable: {e}"));
            return;
        }
    };
    let v: serde_json::Value = match serde_json::from_str(&s) {
        Ok(v) => v,
        Err(e) => {
            findings.warn(format!("_session_meta.json: invalid JSON: {e}"));
            return;
        }
    };
    let version = v
        .get("recorder_version")
        .and_then(|x| x.as_str())
        .unwrap_or("?");
    findings.ok(format!("_session_meta.json: recorder v{version}"));
}

fn report_polymarket_markets(session_dir: &Path, findings: &mut Findings) {
    let dir = session_dir.join("polymarket");
    if !dir.exists() {
        findings.miss("polymarket/: no directory");
        return;
    }
    let mut per_market: BTreeMap<String, u64> = BTreeMap::new();
    let mut snapshot_tokens = 0u64;
    let entries = match fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) => {
            findings.warn(format!("polymarket/: read_dir failed: {e}"));
            return;
        }
    };
    for entry in entries.flatten() {
        let p = entry.path();
        let name = match p.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };
        if !name.ends_with(".ndjson") {
            continue;
        }
        // Strip optional `.<bucket>.ndjson` suffix.
        let stem = name.trim_end_matches(".ndjson");
        let key = stem.rsplit_once('.').map(|(a, _)| a).unwrap_or(stem);
        if key.contains("@book_snapshot") {
            snapshot_tokens += 1;
            continue;
        }
        *per_market.entry(key.to_string()).or_insert(0) += count_lines(&p);
    }
    if per_market.is_empty() {
        findings.miss("polymarket/: no per-market files");
    } else {
        let total_events: u64 = per_market.values().sum();
        findings.ok(format!(
            "polymarket/: {} markets, {} total events",
            per_market.len(),
            total_events
        ));
    }
    if snapshot_tokens == 0 {
        findings.warn(
            "polymarket/: no @book_snapshot files — the new REST baseline fix may not be active",
        );
    } else {
        findings.ok(format!(
            "polymarket/: {snapshot_tokens} REST book snapshot files"
        ));
    }
}

fn print_section(label: &str, lines: &[String], indent: &str) {
    if lines.is_empty() {
        return;
    }
    println!("{label}");
    for l in lines {
        println!("{indent}{l}");
    }
    println!();
}

fn main() -> ExitCode {
    let session_dir = match std::env::args().nth(1) {
        Some(s) => PathBuf::from(s),
        None => {
            eprintln!(
                "usage: recorder_verify <session_dir>\n  e.g. recorder_verify data/session_20260521T180000Z"
            );
            return ExitCode::from(2);
        }
    };
    if !session_dir.exists() {
        eprintln!("session dir does not exist: {}", session_dir.display());
        return ExitCode::from(3);
    }

    println!("== Recorder session verify ==");
    println!("Session: {}", session_dir.display());
    println!();

    let mut findings = Findings::default();

    // Session meta.
    report_meta(&session_dir, &mut findings);

    // Binance streams.
    let trade = collect_stream(&session_dir, "binance", "btcusdt@trade");
    let book = collect_stream(&session_dir, "binance", "btcusdt@bookTicker");
    let depth = collect_stream(&session_dir, "binance", "btcusdt@depth@100ms");
    let depth_snap = collect_stream(&session_dir, "binance", "btcusdt@depth_snapshot");

    // Expected volumes for a 1-hour session (rough lower bounds):
    //   trade:        ~5 trades/s  → 18k lines
    //   bookTicker:   ~5-20 ticks/s → 18k+
    //   depth@100ms:  ~10/s         → 36k
    //   depth_snapshot: ≥ 1 (one per WS connect)
    // We use generous thresholds since a short test run may not hit them.
    report_stream("binance/btcusdt@trade", &trade, 100, &mut findings);
    report_stream("binance/btcusdt@bookTicker", &book, 100, &mut findings);
    report_stream("binance/btcusdt@depth@100ms", &depth, 100, &mut findings);
    report_stream(
        "binance/btcusdt@depth_snapshot",
        &depth_snap,
        1,
        &mut findings,
    );

    // Coinbase trades.
    let cb_trades = collect_stream(&session_dir, "coinbase", "");
    report_stream("coinbase/", &cb_trades, 10, &mut findings);

    // Polymarket.
    report_polymarket_markets(&session_dir, &mut findings);

    // Sidecar logs.
    report_sidecar(&session_dir, "_resolutions.ndjson", 0, &mut findings);
    report_sidecar(&session_dir, "_health.ndjson", 1, &mut findings);
    report_sidecar(&session_dir, "_latency_probes.ndjson", 1, &mut findings);

    // Output.
    print_section("OK:", &findings.ok, "  ✓ ");
    print_section("Warnings:", &findings.warnings, "  ⚠ ");
    print_section("Missing (fatal):", &findings.fatal_missing, "  ✗ ");

    println!("== Summary ==");
    println!("  ok:       {}", findings.ok.len());
    println!("  warning:  {}", findings.warnings.len());
    println!("  missing:  {}", findings.fatal_missing.len());

    if !findings.fatal_missing.is_empty() {
        println!("\nFV-model training will NOT work until the missing items are captured.");
        return ExitCode::from(1);
    }
    if !findings.warnings.is_empty() {
        println!("\nFV-model training MAY work but data quality is below ideal.");
        return ExitCode::from(0);
    }
    println!("\nSession looks complete. FV-model training should work.");
    ExitCode::from(0)
}
