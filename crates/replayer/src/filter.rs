//! Replay-time filter primitive.
//!
//! Applied at two levels:
//! * `matches_file`: file-level pre-prune so we never open a file whose
//!   `(venue, stream)` is excluded by the filter.
//! * `matches_event`: per-event check including time-range bounds.
//!
//! An empty `ReplayFilter::default()` accepts everything.

use std::collections::HashSet;

use common::{RawEvent, Venue};

use crate::discovery::FileBucket;

#[derive(Default, Clone, Debug)]
pub struct ReplayFilter {
    /// If set, only events from these venues pass.
    pub venues: Option<HashSet<Venue>>,
    /// If set, only events with a stream in this set pass.
    pub streams: Option<HashSet<String>>,
    /// Stream prefixes (e.g. `"btcusdt@"`) that pass alongside `streams`.
    pub stream_prefixes: Vec<String>,
    /// Inclusive lower bound on `local_ts_ns`.
    pub from_ts_ns: Option<u128>,
    /// Exclusive upper bound on `local_ts_ns`.
    pub to_ts_ns: Option<u128>,
}

impl ReplayFilter {
    /// File-level pre-prune. Time bounds aren't evaluated here because
    /// they require reading inside the file.
    ///
    /// The recorder writes filenames with non-`[A-Za-z0-9_-]` chars
    /// replaced by `_`, so a user-supplied prefix like `btcusdt@` is
    /// transparently re-mapped to `btcusdt_` before comparing. That
    /// keeps the same prefix usable at both file and event level.
    pub fn matches_file(&self, f: &FileBucket) -> bool {
        if let Some(vs) = &self.venues {
            if !vs.contains(&f.venue) {
                return false;
            }
        }
        if self.streams.is_none() && self.stream_prefixes.is_empty() {
            return true;
        }
        if let Some(ss) = &self.streams {
            for s in ss {
                if sanitize_stream(s) == f.stream {
                    return true;
                }
            }
        }
        for prefix in &self.stream_prefixes {
            if f.stream.starts_with(&sanitize_stream(prefix)) {
                return true;
            }
        }
        false
    }

    /// Per-event check. Run after the file passes `matches_file`.
    pub fn matches_event(&self, e: &RawEvent) -> bool {
        if let Some(vs) = &self.venues {
            if !vs.contains(&e.venue) {
                return false;
            }
        }
        if !self.matches_stream_name(&e.stream) {
            return false;
        }
        let ts = e.local_ts_ns.as_nanos();
        if let Some(lo) = self.from_ts_ns {
            if ts < lo {
                return false;
            }
        }
        if let Some(hi) = self.to_ts_ns {
            if ts >= hi {
                return false;
            }
        }
        true
    }

    fn matches_stream_name(&self, stream: &str) -> bool {
        if self.streams.is_none() && self.stream_prefixes.is_empty() {
            return true;
        }
        if let Some(ss) = &self.streams {
            if ss.contains(stream) {
                return true;
            }
        }
        for prefix in &self.stream_prefixes {
            if stream.starts_with(prefix) {
                return true;
            }
        }
        false
    }
}

/// Mirrors `storage::sanitize_stream_name` — kept inline to avoid a
/// cross-crate dep just for one function. Replaces every char that
/// isn't `[A-Za-z0-9_-]` with `_`.
fn sanitize_stream(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::LocalTimestamp;
    use std::path::PathBuf;

    fn fb(venue: Venue, stream: &str) -> FileBucket {
        FileBucket {
            venue,
            stream: stream.into(),
            bucket: 0,
            path: PathBuf::from("/dev/null"),
            compressed: false,
        }
    }

    fn ev(venue: Venue, stream: &str, ts: u128) -> RawEvent {
        RawEvent {
            venue,
            stream: stream.into(),
            local_ts_ns: LocalTimestamp::from_nanos(ts),
            venue_ts_ms: None,
            payload: String::new(),
        ..Default::default()
        }
    }

    #[test]
    fn empty_filter_accepts_everything() {
        let f = ReplayFilter::default();
        assert!(f.matches_file(&fb(Venue::Binance, "any")));
        assert!(f.matches_event(&ev(Venue::Coinbase, "x", 100)));
    }

    #[test]
    fn venue_filter_works_at_both_levels() {
        let mut vs = HashSet::new();
        vs.insert(Venue::Binance);
        let f = ReplayFilter {
            venues: Some(vs),
            ..Default::default()
        };
        assert!(f.matches_file(&fb(Venue::Binance, "x")));
        assert!(!f.matches_file(&fb(Venue::Coinbase, "x")));
        assert!(!f.matches_event(&ev(Venue::Coinbase, "x", 0)));
    }

    #[test]
    fn stream_prefix_matches_family_at_file_level() {
        // FileBuckets carry the SANITIZED stream name (filename stem).
        // User-supplied prefix is in the original `@` form; the filter
        // sanitizes it transparently for comparison.
        let f = ReplayFilter {
            stream_prefixes: vec!["btcusdt@".into()],
            ..Default::default()
        };
        assert!(f.matches_file(&fb(Venue::Binance, "btcusdt_trade")));
        assert!(f.matches_file(&fb(Venue::Binance, "btcusdt_depth_100ms")));
        assert!(!f.matches_file(&fb(Venue::Binance, "ethusdt_trade")));
    }

    #[test]
    fn stream_prefix_matches_family_at_event_level() {
        // RawEvents carry the ORIGINAL stream name with `@` chars.
        // matches_event compares without sanitization.
        let f = ReplayFilter {
            stream_prefixes: vec!["btcusdt@".into()],
            ..Default::default()
        };
        assert!(f.matches_event(&ev(Venue::Binance, "btcusdt@trade", 0)));
        assert!(f.matches_event(&ev(Venue::Binance, "btcusdt@depth@100ms", 0)));
        assert!(!f.matches_event(&ev(Venue::Binance, "ethusdt@trade", 0)));
    }

    #[test]
    fn time_range_bounds_applied() {
        let f = ReplayFilter {
            from_ts_ns: Some(100),
            to_ts_ns: Some(200),
            ..Default::default()
        };
        assert!(!f.matches_event(&ev(Venue::Binance, "x", 99)));
        assert!(f.matches_event(&ev(Venue::Binance, "x", 100)));
        assert!(f.matches_event(&ev(Venue::Binance, "x", 199)));
        assert!(!f.matches_event(&ev(Venue::Binance, "x", 200)));
    }
}
