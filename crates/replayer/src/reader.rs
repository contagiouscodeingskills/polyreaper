//! Single-file NDJSON reader.
//!
//! Wraps `BufReader<File>` (or `BufReader<GzDecoder<File>>` for `.ndjson.gz`)
//! and yields `(line_no, RawEvent)` pairs. Line numbers are 1-based and used
//! by [`crate::merge::MergedReader`] as a stable tiebreaker when many events
//! share the same `local_ts_ns` (the Polymarket array-demux case).
//!
//! Compression support: the reader transparently decompresses files whose
//! name ends `.ndjson.gz`. Sessions compressed in place by the operations
//! `disk_guard` script remain analysable without an explicit decompress
//! step.

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use flate2::read::MultiGzDecoder;

use common::RawEvent;

use crate::error::ReplayError;

pub struct FileReader {
    path: PathBuf,
    inner: Box<dyn BufRead + Send>,
    line_no: usize,
    eof: bool,
}

impl FileReader {
    /// Open a `.ndjson` or `.ndjson.gz` file. Compression is auto-detected
    /// from the filename suffix.
    pub fn open(path: &Path) -> Result<Self, ReplayError> {
        let file = File::open(path).map_err(|source| ReplayError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let compressed = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.ends_with(".gz"))
            .unwrap_or(false);
        // MultiGzDecoder handles gzip files that contain multiple
        // concatenated members — a robustness win if anyone ever gzips
        // per-rotation and concatenates.
        let raw: Box<dyn Read + Send> = if compressed {
            Box::new(MultiGzDecoder::new(file))
        } else {
            Box::new(file)
        };
        let inner: Box<dyn BufRead + Send> = Box::new(BufReader::new(raw));
        Ok(Self {
            path: path.to_path_buf(),
            inner,
            line_no: 0,
            eof: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

// Trait-object adapter so the BufReader<...> can wrap either File or
// GzDecoder<File>. The cast above goes through `dyn Read + Send` first.
impl Iterator for FileReader {
    type Item = Result<(usize, RawEvent), ReplayError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.eof {
            return None;
        }
        let mut buf = String::new();
        loop {
            buf.clear();
            match self.inner.read_line(&mut buf) {
                Ok(0) => {
                    self.eof = true;
                    return None;
                }
                Ok(_) => {
                    self.line_no += 1;
                    let trimmed = buf.trim_end_matches(&['\n', '\r'][..]);
                    if trimmed.is_empty() {
                        continue; // skip blank lines defensively
                    }
                    let parsed: Result<RawEvent, _> = serde_json::from_str(trimmed);
                    match parsed {
                        Ok(ev) => return Some(Ok((self.line_no, ev))),
                        Err(source) => {
                            return Some(Err(ReplayError::ParseLine {
                                path: self.path.clone(),
                                line: self.line_no,
                                source,
                            }));
                        }
                    }
                }
                Err(source) => {
                    self.eof = true;
                    return Some(Err(ReplayError::Io {
                        path: self.path.clone(),
                        source,
                    }));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{LocalTimestamp, Venue};
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestDir(PathBuf);
    impl TestDir {
        fn new() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let ptr = &nanos as *const _ as usize;
            let dir = std::env::temp_dir().join(format!("polybot_reader_test_{nanos}_{ptr:x}"));
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

    fn ev(stream: &str, ns: u128, payload: &str) -> RawEvent {
        RawEvent {
            venue: Venue::Binance,
            stream: stream.into(),
            local_ts_ns: LocalTimestamp::from_nanos(ns),
            venue_ts_ms: None,
            payload: payload.into(),
        ..Default::default()
        }
    }

    #[test]
    fn reads_plain_ndjson() {
        let tmp = TestDir::new();
        let p = tmp.path().join("plain.ndjson");
        let mut f = File::create(&p).unwrap();
        for i in 1u128..=3 {
            let line = serde_json::to_string(&ev("s", i, "{}")).unwrap();
            writeln!(f, "{}", line).unwrap();
        }
        let r = FileReader::open(&p).unwrap();
        let collected: Vec<_> = r.collect::<Result<_, _>>().unwrap();
        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0].0, 1);
        assert_eq!(collected[2].0, 3);
        assert_eq!(collected[0].1.local_ts_ns.as_nanos(), 1);
    }

    #[test]
    fn reads_gzipped_ndjson() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        let tmp = TestDir::new();
        let p = tmp.path().join("compressed.ndjson.gz");
        let f = File::create(&p).unwrap();
        let mut enc = GzEncoder::new(f, Compression::default());
        for i in 1u128..=5 {
            let line = serde_json::to_string(&ev("s", i, "{}")).unwrap();
            writeln!(enc, "{}", line).unwrap();
        }
        enc.finish().unwrap();

        let r = FileReader::open(&p).unwrap();
        let collected: Vec<_> = r.collect::<Result<_, _>>().unwrap();
        assert_eq!(collected.len(), 5);
        assert_eq!(collected[0].0, 1);
        assert_eq!(collected[4].0, 5);
        for (i, (_line_no, e)) in collected.iter().enumerate() {
            assert_eq!(e.local_ts_ns.as_nanos() as usize, i + 1);
        }
    }

    #[test]
    fn reads_empty_gzipped_ndjson() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        let tmp = TestDir::new();
        let p = tmp.path().join("empty.ndjson.gz");
        let f = File::create(&p).unwrap();
        let enc = GzEncoder::new(f, Compression::default());
        enc.finish().unwrap();
        let r = FileReader::open(&p).unwrap();
        assert!(r.collect::<Result<Vec<_>, _>>().unwrap().is_empty());
    }
}
