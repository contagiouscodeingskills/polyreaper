//! Replayer — research interface for recorder NDJSON output.
//!
//! Turns "session directory on disk" into a sorted, filterable stream of
//! [`RawEvent`]s. The Rust API is the primary surface; a CLI binary lives
//! at `apps/replayer` and a Parquet exporter is feature-gated under
//! `--features parquet`.
//!
//! # Quick start
//!
//! ```ignore
//! use replayer::{open_session, ReplayFilter};
//! let filter = ReplayFilter::default();
//! for event in open_session("./data/session_20260425T053013Z", filter)? {
//!     let event = event?;
//!     // ... feed into book builder, signal computer, etc.
//! }
//! # Ok::<(), replayer::ReplayError>(())
//! ```
//!
//! # Module layout
//!
//! * [`discovery`] — walk the disk, produce [`SessionDir`] / [`FileBucket`].
//! * [`reader`] — one-file-at-a-time NDJSON iterator yielding `(line_no, RawEvent)`.
//! * [`filter`] — venue / stream / time-range filtering primitive.
//! * [`merge`] — k-way merge with stable tiebreaking.
//! * [`decode`] — `RawEvent` → typed [`DecodedEvent`] per venue.
//! * [`book`] — book reconstruction (Binance L2, Polymarket Yes/No).
//! * [`pacer`] — wall-clock pacing + per-venue latency offsets.
//! * `parquet` (feature-gated) — columnar export for pandas / duckdb.
//! * [`error`] — single error enum.

pub mod book;
pub mod decode;
pub mod discovery;
pub mod error;
pub mod filter;
pub mod integrity;
pub mod merge;
pub mod pacer;
#[cfg(feature = "parquet")]
pub mod parquet;
pub mod reader;

pub use book::{ApplyOutcome, BinanceBook, PolymarketMarketBook, PolymarketSideBook};
pub use common::{LocalTimestamp, RawEvent, Venue};
pub use decode::{decode, DecodedEvent};
pub use discovery::{FileBucket, SessionDir};
pub use error::ReplayError;
pub use filter::ReplayFilter;
pub use merge::MergedReader;
pub use pacer::{PaceMode, Pacer};

use std::path::Path;

pub const NAME: &str = "replayer";

/// Open a single session directory and return a time-merged event stream.
///
/// `session_dir` must be a directory whose name fits `session_<UTC>`.
/// Returns [`ReplayError::NotASessionDir`] otherwise. Use
/// [`open_base_dir`] when you want to merge across many sessions.
pub fn open_session(
    session_dir: impl AsRef<Path>,
    filter: ReplayFilter,
) -> Result<MergedReader, ReplayError> {
    let sd = SessionDir::from_path(session_dir.as_ref())?;
    let files = sd.list_files()?;
    let paths: Vec<_> = files
        .into_iter()
        .filter(|f| filter.matches_file(f))
        .map(|f| f.path)
        .collect();
    MergedReader::from_files(paths, filter)
}

/// Open every `session_*` subdir under `base_dir` and merge them together.
///
/// Use this when you want to replay across recorder restarts. Sessions
/// are sorted chronologically by name suffix; events are then merged
/// by `local_ts_ns`, so cross-session ordering is preserved as long as
/// the recorder's clock was monotonic across restarts.
pub fn open_base_dir(
    base_dir: impl AsRef<Path>,
    filter: ReplayFilter,
) -> Result<MergedReader, ReplayError> {
    let sessions = SessionDir::discover(base_dir.as_ref())?;
    let mut paths = Vec::new();
    for sd in sessions {
        let files = sd.list_files()?;
        for f in files {
            if filter.matches_file(&f) {
                paths.push(f.path);
            }
        }
    }
    MergedReader::from_files(paths, filter)
}
