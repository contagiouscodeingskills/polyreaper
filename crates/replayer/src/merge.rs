//! K-way merge across many [`FileReader`]s.
//!
//! # Invariant the merge depends on
//!
//! **Each input file's events are assumed to be monotonically
//! non-decreasing in `local_ts_ns`.** The recorder guarantees this because
//! every `Store::write` stamps the event with `LocalTimestamp::now()` at
//! call time. The merge only sorts *across* files; sorting within a file
//! would require buffering the whole file, defeating the streaming goal.
//!
//! Polymarket array-demux events break ties (multiple records sharing one
//! `local_ts_ns`) by appearing in the order the demux pass wrote them,
//! which is the order they appeared in the wire frame.
//!
//! # Tiebreaker
//!
//! `(ts asc, file_idx asc, line_no asc)`. `file_idx` is assigned at merge
//! construction; `line_no` is the 1-based per-file line counter from
//! [`crate::reader::FileReader`].
//!
//! Filter is applied per-event at advance time so the heap only ever
//! contains events the consumer wants.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::PathBuf;

use common::RawEvent;

use crate::error::ReplayError;
use crate::filter::ReplayFilter;
use crate::reader::FileReader;

pub struct MergedReader {
    /// `None` once a reader has reached EOF (or errored).
    readers: Vec<Option<FileReader>>,
    /// Min-heap by (ts, file_idx, line_no). `Reverse` flips the
    /// `BinaryHeap`'s default max-heap behaviour.
    heap: BinaryHeap<Reverse<HeapEntry>>,
    filter: ReplayFilter,
}

#[derive(Debug)]
struct HeapEntry {
    ts: u128,
    file_idx: usize,
    line_no: usize,
    event: RawEvent,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.ts == other.ts && self.file_idx == other.file_idx && self.line_no == other.line_no
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ts
            .cmp(&other.ts)
            .then_with(|| self.file_idx.cmp(&other.file_idx))
            .then_with(|| self.line_no.cmp(&other.line_no))
    }
}

impl MergedReader {
    pub fn from_files(paths: Vec<PathBuf>, filter: ReplayFilter) -> Result<Self, ReplayError> {
        let mut readers: Vec<Option<FileReader>> = Vec::with_capacity(paths.len());
        for p in &paths {
            readers.push(Some(FileReader::open(p)?));
        }
        let heap = BinaryHeap::new();
        let mut me = MergedReader {
            readers,
            heap,
            filter,
        };
        for idx in 0..me.readers.len() {
            me.advance(idx)?;
        }
        Ok(me)
    }

    /// Pull the next event from `readers[idx]` that passes the filter,
    /// push it onto the heap. EOF or filter-rejection-of-all loops to
    /// the next file's read attempt; eventually we either find a match
    /// or mark the reader done.
    fn advance(&mut self, idx: usize) -> Result<(), ReplayError> {
        loop {
            let next = match &mut self.readers[idx] {
                Some(r) => r.next(),
                None => return Ok(()),
            };
            match next {
                None => {
                    self.readers[idx] = None;
                    return Ok(());
                }
                Some(Err(e)) => return Err(e),
                Some(Ok((line_no, event))) => {
                    if !self.filter.matches_event(&event) {
                        continue;
                    }
                    let ts = event.local_ts_ns.as_nanos();
                    self.heap.push(Reverse(HeapEntry {
                        ts,
                        file_idx: idx,
                        line_no,
                        event,
                    }));
                    return Ok(());
                }
            }
        }
    }
}

impl Iterator for MergedReader {
    type Item = Result<RawEvent, ReplayError>;

    fn next(&mut self) -> Option<Self::Item> {
        let Reverse(entry) = self.heap.pop()?;
        let idx = entry.file_idx;
        if let Err(e) = self.advance(idx) {
            return Some(Err(e));
        }
        Some(Ok(entry.event))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{LocalTimestamp, Venue};
    use std::cmp::Ordering;

    fn entry(ts: u128, file_idx: usize, line_no: usize) -> HeapEntry {
        HeapEntry {
            ts,
            file_idx,
            line_no,
            event: RawEvent {
                venue: Venue::Binance,
                stream: "x".into(),
                local_ts_ns: LocalTimestamp::from_nanos(ts),
                venue_ts_ms: None,
                payload: String::new(),
            },
        }
    }

    #[test]
    fn ts_is_primary_sort_key() {
        assert_eq!(entry(1, 0, 0).cmp(&entry(2, 0, 0)), Ordering::Less);
        assert_eq!(entry(2, 0, 0).cmp(&entry(1, 0, 0)), Ordering::Greater);
    }

    #[test]
    fn file_idx_breaks_ts_tie() {
        assert_eq!(entry(5, 1, 0).cmp(&entry(5, 2, 0)), Ordering::Less);
    }

    #[test]
    fn line_no_breaks_file_idx_tie() {
        // Same ts, same file: line_no decides.
        assert_eq!(entry(5, 0, 1).cmp(&entry(5, 0, 2)), Ordering::Less);
    }

    #[test]
    fn binheap_with_reverse_acts_as_min_heap() {
        let mut h: BinaryHeap<Reverse<HeapEntry>> = BinaryHeap::new();
        h.push(Reverse(entry(3, 0, 0)));
        h.push(Reverse(entry(1, 0, 0)));
        h.push(Reverse(entry(2, 0, 0)));
        assert_eq!(h.pop().unwrap().0.ts, 1);
        assert_eq!(h.pop().unwrap().0.ts, 2);
        assert_eq!(h.pop().unwrap().0.ts, 3);
    }
}
