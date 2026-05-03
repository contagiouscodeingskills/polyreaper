//! Parquet export — `RawEvent` stream → columnar `.parquet` file.
//!
//! Feature-gated under `parquet`. Pulls in `arrow` + `parquet` crates.
//!
//! ## Schema (v1)
//!
//! Single unified schema for all venues. Researchers downstream pick
//! out the venue-specific JSON parsing themselves (pandas `apply`,
//! polars `str.json_decode`, etc):
//!
//! | column        | type             | nullable | notes                                   |
//! |---------------|------------------|----------|-----------------------------------------|
//! | `venue`       | `Utf8`           | no       | `"binance"` / `"polymarket"` / …        |
//! | `stream`      | `Utf8`           | no       | venue stream id, e.g. `"btcusdt@trade"` |
//! | `local_ts_ns` | `Decimal128(38, 0)` | no    | u128 nanos since epoch                  |
//! | `venue_ts_ms` | `Int64`          | yes      | venue-reported ms epoch when present    |
//! | `payload`     | `LargeUtf8`      | no       | raw JSON line                           |
//!
//! `Utf8` columns get dictionary-encoded automatically by Parquet's
//! default encoder when cardinality is low — no extra effort needed.
//!
//! ## Schema lock
//!
//! Changing this schema is a breaking change for every consumer
//! parquet file we've ever written. The doc-test
//! [`schema_string_is_locked`] asserts the schema's printable form
//! verbatim, so any accidental field rename / type tweak fails CI.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Builder, Int64Builder, LargeStringBuilder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use common::RawEvent;

use crate::ReplayError;

const PRECISION: u8 = 38;
const SCALE: i8 = 0;

/// Default rows-per-RecordBatch when streaming events into a writer.
/// Keeps memory bounded; row groups get sized by the parquet writer
/// based on its own internal threshold.
pub const DEFAULT_BATCH_ROWS: usize = 10_000;

/// The single arrow [`Schema`] this exporter writes. Locked — see
/// the test at the bottom of this file.
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("venue", DataType::Utf8, false),
        Field::new("stream", DataType::Utf8, false),
        Field::new(
            "local_ts_ns",
            DataType::Decimal128(PRECISION, SCALE),
            false,
        ),
        Field::new("venue_ts_ms", DataType::Int64, true),
        Field::new("payload", DataType::LargeUtf8, false),
    ]))
}

/// Streaming writer: feed [`RawEvent`]s in, get a Parquet file out.
///
/// Buffers up to `batch_rows` events, then flushes one RecordBatch
/// to the writer. Caller MUST call [`ParquetSink::close`] (or drop —
/// `Drop` finalises). `close` returns the row count for sanity checks.
pub struct ParquetSink {
    writer: ArrowWriter<File>,
    schema: SchemaRef,
    batch_rows: usize,
    rows_in_batch: usize,
    rows_total: usize,
    venues: StringBuilder,
    streams: StringBuilder,
    ts_ns: Decimal128Builder,
    venue_ts_ms: Int64Builder,
    payloads: LargeStringBuilder,
}

impl ParquetSink {
    /// Open a Parquet file at `path` and prepare a streaming writer.
    /// Snappy compression — fast and consistently smaller than no-compression.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, ReplayError> {
        Self::create_with_batch_rows(path, DEFAULT_BATCH_ROWS)
    }

    pub fn create_with_batch_rows(
        path: impl AsRef<Path>,
        batch_rows: usize,
    ) -> Result<Self, ReplayError> {
        let file = File::create(path.as_ref()).map_err(|source| ReplayError::Io {
            path: path.as_ref().to_path_buf(),
            source,
        })?;
        let schema = schema();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| ReplayError::Parquet(e.to_string()))?;

        Ok(Self {
            writer,
            schema,
            batch_rows: batch_rows.max(1),
            rows_in_batch: 0,
            rows_total: 0,
            venues: StringBuilder::new(),
            streams: StringBuilder::new(),
            ts_ns: Decimal128Builder::new()
                .with_precision_and_scale(PRECISION, SCALE)
                .expect("precision/scale within Decimal128 limits"),
            venue_ts_ms: Int64Builder::new(),
            payloads: LargeStringBuilder::new(),
        })
    }

    /// Append one event. Flushes a RecordBatch when the buffer fills.
    pub fn append(&mut self, event: &RawEvent) -> Result<(), ReplayError> {
        self.venues.append_value(event.venue.as_str());
        self.streams.append_value(&event.stream);
        // u128 → i128 cast: timestamps will not overflow i128 until
        // year 2300+ (i128::MAX ≈ 1.7e38; current ts ≈ 1.78e18).
        self.ts_ns.append_value(event.local_ts_ns.as_nanos() as i128);
        match event.venue_ts_ms {
            Some(v) => self.venue_ts_ms.append_value(v),
            None => self.venue_ts_ms.append_null(),
        }
        self.payloads.append_value(&event.payload);
        self.rows_in_batch += 1;
        self.rows_total += 1;
        if self.rows_in_batch >= self.batch_rows {
            self.flush_batch()?;
        }
        Ok(())
    }

    /// Stream every event from `iter`, appending each. Returns the
    /// row count after closing the writer.
    pub fn write_all<I>(mut self, iter: I) -> Result<usize, ReplayError>
    where
        I: IntoIterator<Item = Result<RawEvent, ReplayError>>,
    {
        for ev in iter {
            self.append(&ev?)?;
        }
        self.close()
    }

    /// Flush the in-memory buffer as one RecordBatch + finalise the
    /// Parquet writer. Returns total rows written.
    pub fn close(mut self) -> Result<usize, ReplayError> {
        if self.rows_in_batch > 0 {
            self.flush_batch()?;
        }
        self.writer
            .close()
            .map_err(|e| ReplayError::Parquet(e.to_string()))?;
        Ok(self.rows_total)
    }

    fn flush_batch(&mut self) -> Result<(), ReplayError> {
        let venues: ArrayRef = Arc::new(self.venues.finish());
        let streams: ArrayRef = Arc::new(self.streams.finish());
        let ts: ArrayRef = Arc::new(self.ts_ns.finish());
        let vts: ArrayRef = Arc::new(self.venue_ts_ms.finish());
        let payloads: ArrayRef = Arc::new(self.payloads.finish());
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![venues, streams, ts, vts, payloads],
        )
        .map_err(|e| ReplayError::Parquet(e.to_string()))?;
        self.writer
            .write(&batch)
            .map_err(|e| ReplayError::Parquet(e.to_string()))?;
        // Re-prime builders for the next batch. We have to rebuild the
        // Decimal128Builder because finish() resets but loses precision.
        self.venues = StringBuilder::new();
        self.streams = StringBuilder::new();
        self.ts_ns = Decimal128Builder::new()
            .with_precision_and_scale(PRECISION, SCALE)
            .expect("precision/scale within Decimal128 limits");
        self.venue_ts_ms = Int64Builder::new();
        self.payloads = LargeStringBuilder::new();
        self.rows_in_batch = 0;
        Ok(())
    }
}

/// Convenience: open a Parquet file, write every event from `iter`,
/// close. Returns row count.
pub fn dump<P: AsRef<Path>, I: IntoIterator<Item = Result<RawEvent, ReplayError>>>(
    out: P,
    iter: I,
) -> Result<usize, ReplayError> {
    ParquetSink::create(out)?.write_all(iter)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use arrow::array::{Array, Decimal128Array, Int64Array, LargeStringArray, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use common::{LocalTimestamp, Venue};

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let ptr = &nanos as *const _ as usize;
            let dir = std::env::temp_dir().join(format!("polybot_parquet_{nanos}_{ptr:x}"));
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

    fn ev(venue: Venue, stream: &str, ts_ns: u128, vts_ms: Option<i64>, payload: &str) -> RawEvent {
        RawEvent {
            venue,
            stream: stream.into(),
            local_ts_ns: LocalTimestamp::from_nanos(ts_ns),
            venue_ts_ms: vts_ms,
            payload: payload.into(),
        ..Default::default()
        }
    }

    /// Locks the on-disk Parquet schema. Touching this breaks every
    /// downstream pandas / polars / duckdb consumer of older files.
    /// Treat the assertion change like a Parquet format-version bump:
    /// document the migration plan first.
    #[test]
    fn schema_string_is_locked() {
        let s = schema();
        let want = "Field { name: \"venue\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, \
                    Field { name: \"stream\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, \
                    Field { name: \"local_ts_ns\", data_type: Decimal128(38, 0), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, \
                    Field { name: \"venue_ts_ms\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, \
                    Field { name: \"payload\", data_type: LargeUtf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }";
        let got = s
            .fields()
            .iter()
            .map(|f| format!("{:?}", f))
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(got, want);
    }

    #[test]
    fn round_trip_through_parquet_preserves_every_field() {
        let tmp = TestDir::new();
        let out = tmp.path().join("out.parquet");

        let inputs = vec![
            ev(Venue::Binance, "btcusdt@trade", 1_776_000_000_000_000_001, Some(1_776_000), r#"{"e":"trade"}"#),
            ev(Venue::Polymarket, "btc-updown-5m-1", 1_776_000_000_000_000_002, None, r#"{"event_type":"book"}"#),
            ev(Venue::Coinbase, "btc-usd@market_trades", 1_776_000_000_000_000_003, Some(1_776_001), "x"),
        ];
        let mut sink = ParquetSink::create(&out).unwrap();
        for e in &inputs {
            sink.append(e).unwrap();
        }
        let n = sink.close().unwrap();
        assert_eq!(n, 3);

        // Read back with arrow's ParquetRecordBatchReader.
        let f = File::open(&out).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f).unwrap().build().unwrap();
        let mut got = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let venues = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let streams = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            let ts = batch.column(2).as_any().downcast_ref::<Decimal128Array>().unwrap();
            let vts = batch.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
            let payload = batch.column(4).as_any().downcast_ref::<LargeStringArray>().unwrap();
            for i in 0..batch.num_rows() {
                got.push((
                    venues.value(i).to_string(),
                    streams.value(i).to_string(),
                    ts.value(i),
                    if vts.is_null(i) { None } else { Some(vts.value(i)) },
                    payload.value(i).to_string(),
                ));
            }
        }
        assert_eq!(got.len(), 3);
        for (g, expected) in got.iter().zip(inputs.iter()) {
            assert_eq!(g.0, expected.venue.as_str());
            assert_eq!(g.1, expected.stream);
            assert_eq!(g.2 as u128, expected.local_ts_ns.as_nanos());
            assert_eq!(g.3, expected.venue_ts_ms);
            assert_eq!(g.4, expected.payload);
        }
    }

    #[test]
    fn write_all_returns_row_count_and_creates_file() {
        let tmp = TestDir::new();
        let out = tmp.path().join("out.parquet");
        let inputs = (0..5).map(|i| {
            Ok(ev(Venue::Binance, "btcusdt@trade", i as u128, None, "p"))
        });
        let n = ParquetSink::create(&out).unwrap().write_all(inputs).unwrap();
        assert_eq!(n, 5);
        assert!(out.exists());
        let size = std::fs::metadata(&out).unwrap().len();
        assert!(size > 0, "parquet file is empty");
    }

    #[test]
    fn batch_flushes_at_threshold_and_finalises_on_close() {
        let tmp = TestDir::new();
        let out = tmp.path().join("out.parquet");
        // batch_rows = 2 → 5 events produce flushes at 2, 4, and on close.
        let mut sink = ParquetSink::create_with_batch_rows(&out, 2).unwrap();
        for i in 0..5 {
            sink.append(&ev(Venue::Binance, "x", i as u128, None, "p")).unwrap();
        }
        let n = sink.close().unwrap();
        assert_eq!(n, 5);

        // Reader should still see all 5 rows across all batches.
        let f = File::open(&out).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f).unwrap().build().unwrap();
        let total: usize = reader.into_iter().map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(total, 5);
    }

    #[test]
    fn null_venue_ts_ms_round_trips_as_null() {
        let tmp = TestDir::new();
        let out = tmp.path().join("out.parquet");
        let mut sink = ParquetSink::create(&out).unwrap();
        sink.append(&ev(Venue::Binance, "x", 1, None, "p")).unwrap();
        sink.close().unwrap();

        let f = File::open(&out).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f).unwrap().build().unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();
        let vts = batch.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(vts.is_null(0));
    }
}
