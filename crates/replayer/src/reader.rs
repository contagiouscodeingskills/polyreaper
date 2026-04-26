//! Single-file NDJSON reader.
//!
//! Wraps `BufReader<File>` and yields `(line_no, RawEvent)` pairs. Line
//! numbers are 1-based and used by [`crate::merge::MergedReader`] as a
//! stable tiebreaker when many events share the same `local_ts_ns` (the
//! Polymarket array-demux case).

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use common::RawEvent;

use crate::error::ReplayError;

pub struct FileReader {
    path: PathBuf,
    inner: BufReader<File>,
    line_no: usize,
    eof: bool,
}

impl FileReader {
    pub fn open(path: &Path) -> Result<Self, ReplayError> {
        let file = File::open(path).map_err(|source| ReplayError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Ok(Self {
            path: path.to_path_buf(),
            inner: BufReader::new(file),
            line_no: 0,
            eof: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

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
