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
