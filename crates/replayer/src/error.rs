//! Replayer error types.
//!
//! Single error enum covers everything the replayer can fail at: filesystem
//! walks, single-line parses, malformed session/file names, and (when the
//! `parquet` feature is on) Arrow/Parquet writer errors.

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error("io error at {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("not a session directory: {}", path.display())]
    NotASessionDir { path: PathBuf },

    #[error("ndjson parse error in {} line {line}: {source}", path.display())]
    ParseLine {
        path: PathBuf,
        line: usize,
        #[source]
        source: serde_json::Error,
    },

    /// Payload-level decode failed (malformed inner JSON, missing required
    /// field, bad decimal). Stream is the venue stream the event came
    /// from, e.g. `"btcusdt@trade"` — useful for spotting which file or
    /// venue is the culprit.
    #[error("decode error on stream {stream:?}: {reason}")]
    Decode { stream: String, reason: String },

    #[cfg(feature = "parquet")]
    #[error("parquet error: {0}")]
    Parquet(String),
}
