//! Storage → Replayer round-trip tests.
//!
//! Validates the full pipeline:
//!   build RawEvents in code
//!   → write via `storage::Store` (real file rotation, real NDJSON)
//!   → read via `replayer::open_session` / `open_base_dir`
//!   → assert popped events match input sorted by (local_ts_ns, demux_order)
//!
//! Uses an inline `TestDir` mirroring `crates/storage/src/lib.rs:389-410`
//! to avoid the `tempfile` dep.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use common::{LocalTimestamp, RawEvent, Venue};
use replayer::{
    decode, open_base_dir, open_session, ApplyOutcome, BinanceBook, DecodedEvent, ReplayFilter,
};
use rust_decimal::Decimal;
use storage::Store;

// ---------------------------------------------------------------------------
// TestDir — inline copy from storage::tests
// ---------------------------------------------------------------------------

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let ptr = &nanos as *const _ as usize;
        let dir = std::env::temp_dir().join(format!("polybot_replayer_rt_{nanos}_{ptr:x}"));
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

fn cfg(base: PathBuf) -> config::StorageConfig {
    config::StorageConfig {
        base_dir: base,
        rotate_minutes: 0, // single file per stream — keeps fixtures predictable
        fsync_on_write: false,
    }
}

fn ev(venue: Venue, stream: &str, ts_ns: u128, body: &str) -> RawEvent {
    RawEvent {
        venue,
        stream: stream.into(),
        local_ts_ns: LocalTimestamp::from_nanos(ts_ns),
        venue_ts_ms: None,
        payload: body.into(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn round_trip_single_venue_single_file() {
    let tmp = TestDir::new();
    let mut store = Store::open(&cfg(tmp.path().to_path_buf())).unwrap();

    let inputs = [
        ev(Venue::Binance, "btcusdt@trade", 100, r#"{"i":1}"#),
        ev(Venue::Binance, "btcusdt@trade", 200, r#"{"i":2}"#),
        ev(Venue::Binance, "btcusdt@trade", 300, r#"{"i":3}"#),
    ];
    for e in &inputs {
        store.write(e).unwrap();
    }
    store.flush_all().unwrap();

    let session_dir = store.session_dir().to_path_buf();
    let out: Vec<RawEvent> = open_session(&session_dir, ReplayFilter::default())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(out.len(), 3);
    for (got, exp) in out.iter().zip(inputs.iter()) {
        assert_eq!(got, exp);
    }
}

#[test]
fn round_trip_interleaved_venues_sorted_by_ts() {
    // The merge assumes each FILE is monotonic non-decreasing in ts (which
    // the recorder guarantees via LocalTimestamp::now() per write). Within
    // a file, write order = ts order. The merge interleaves ACROSS files.
    //
    // So this test writes per-file events in ts order, but interleaves
    // venues to exercise the cross-file k-way merge.
    let tmp = TestDir::new();
    let mut store = Store::open(&cfg(tmp.path().to_path_buf())).unwrap();

    // Per-file ts-order (each file becomes monotonic):
    //   binance:   B1(150) B2(200)
    //   coinbase:  C1(100) C2(250)
    //   polymarket: P3(300)
    store.write(&ev(Venue::Coinbase, "btc-usd@market_trades", 100, "C1")).unwrap();
    store.write(&ev(Venue::Binance, "btcusdt@trade", 150, "B1")).unwrap();
    store.write(&ev(Venue::Binance, "btcusdt@trade", 200, "B2")).unwrap();
    store.write(&ev(Venue::Coinbase, "btc-usd@market_trades", 250, "C2")).unwrap();
    store.write(&ev(Venue::Polymarket, "btc-updown-5m-1", 300, "P3")).unwrap();
    store.flush_all().unwrap();

    let session_dir = store.session_dir().to_path_buf();
    let out: Vec<RawEvent> = open_session(&session_dir, ReplayFilter::default())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    let payloads: Vec<&str> = out.iter().map(|e| e.payload.as_str()).collect();
    assert_eq!(payloads, vec!["C1", "B1", "B2", "C2", "P3"]);
}

#[test]
fn ties_in_local_ts_ns_preserve_write_order() {
    // Polymarket array-demux pattern: multiple events share one ts.
    let tmp = TestDir::new();
    let mut store = Store::open(&cfg(tmp.path().to_path_buf())).unwrap();

    let same_ts = 12345;
    store.write(&ev(Venue::Polymarket, "btc-updown-5m-A", same_ts, "first")).unwrap();
    store.write(&ev(Venue::Polymarket, "btc-updown-5m-A", same_ts, "second")).unwrap();
    store.write(&ev(Venue::Polymarket, "btc-updown-5m-A", same_ts, "third")).unwrap();
    store.flush_all().unwrap();

    let session_dir = store.session_dir().to_path_buf();
    let out: Vec<RawEvent> = open_session(&session_dir, ReplayFilter::default())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    let payloads: Vec<&str> = out.iter().map(|e| e.payload.as_str()).collect();
    assert_eq!(payloads, vec!["first", "second", "third"]);
}

#[test]
fn filter_by_venue_drops_other_venues() {
    let tmp = TestDir::new();
    let mut store = Store::open(&cfg(tmp.path().to_path_buf())).unwrap();
    store.write(&ev(Venue::Binance, "btcusdt@trade", 1, "B")).unwrap();
    store.write(&ev(Venue::Coinbase, "btc-usd@market_trades", 2, "C")).unwrap();
    store.flush_all().unwrap();

    let mut venues = std::collections::HashSet::new();
    venues.insert(Venue::Binance);
    let filter = ReplayFilter {
        venues: Some(venues),
        ..Default::default()
    };
    let out: Vec<RawEvent> = open_session(store.session_dir(), filter)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(out.len(), 1);
    assert_eq!(out[0].venue, Venue::Binance);
}

#[test]
fn filter_by_time_range_clips_correctly() {
    let tmp = TestDir::new();
    let mut store = Store::open(&cfg(tmp.path().to_path_buf())).unwrap();
    for ts in [50u128, 100, 150, 200, 250] {
        store.write(&ev(Venue::Binance, "btcusdt@trade", ts, "x")).unwrap();
    }
    store.flush_all().unwrap();

    let filter = ReplayFilter {
        from_ts_ns: Some(100),
        to_ts_ns: Some(200),
        ..Default::default()
    };
    let out: Vec<RawEvent> = open_session(store.session_dir(), filter)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    let tss: Vec<u128> = out.iter().map(|e| e.local_ts_ns.as_nanos()).collect();
    assert_eq!(tss, vec![100, 150]); // 200 is exclusive
}

#[test]
fn open_base_dir_merges_across_sessions() {
    let tmp = TestDir::new();

    // First session — write, drop store to flush + release.
    {
        let mut store = Store::open(&cfg(tmp.path().to_path_buf())).unwrap();
        store.write(&ev(Venue::Binance, "btcusdt@trade", 100, "S1-A")).unwrap();
        store.write(&ev(Venue::Binance, "btcusdt@trade", 300, "S1-B")).unwrap();
        store.flush_all().unwrap();
    }

    // Sleep one second so the second session_<utc> dir has a different name.
    std::thread::sleep(std::time::Duration::from_secs(1));

    // Second session, with timestamps that interleave session 1's range.
    {
        let mut store = Store::open(&cfg(tmp.path().to_path_buf())).unwrap();
        store.write(&ev(Venue::Binance, "btcusdt@trade", 200, "S2-A")).unwrap();
        store.write(&ev(Venue::Binance, "btcusdt@trade", 400, "S2-B")).unwrap();
        store.flush_all().unwrap();
    }

    let out: Vec<RawEvent> = open_base_dir(tmp.path(), ReplayFilter::default())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    let payloads: Vec<&str> = out.iter().map(|e| e.payload.as_str()).collect();
    assert_eq!(payloads, vec!["S1-A", "S2-A", "S1-B", "S2-B"]);
}

// ---------------------------------------------------------------------------
// Book reconstruction E2E
// ---------------------------------------------------------------------------

/// Helper for the book test — builds a real Binance depth-snapshot
/// payload (REST `/api/v3/depth` shape).
fn snapshot_payload(last_update_id: u64, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> String {
    let bids_json: Vec<String> = bids
        .iter()
        .map(|(p, q)| format!("[\"{p}\",\"{q}\"]"))
        .collect();
    let asks_json: Vec<String> = asks
        .iter()
        .map(|(p, q)| format!("[\"{p}\",\"{q}\"]"))
        .collect();
    format!(
        "{{\"lastUpdateId\":{last_update_id},\"bids\":[{}],\"asks\":[{}]}}",
        bids_json.join(","),
        asks_json.join(","),
    )
}

/// Helper — builds a Binance `@depth` diff payload.
fn diff_payload(
    event_time_ms: i64,
    first_update_id: u64,
    final_update_id: u64,
    bids: &[(&str, &str)],
    asks: &[(&str, &str)],
) -> String {
    let bids_json: Vec<String> = bids
        .iter()
        .map(|(p, q)| format!("[\"{p}\",\"{q}\"]"))
        .collect();
    let asks_json: Vec<String> = asks
        .iter()
        .map(|(p, q)| format!("[\"{p}\",\"{q}\"]"))
        .collect();
    format!(
        "{{\"e\":\"depthUpdate\",\"E\":{event_time_ms},\"s\":\"BTCUSDT\",\
        \"U\":{first_update_id},\"u\":{final_update_id},\
        \"b\":[{}],\"a\":[{}]}}",
        bids_json.join(","),
        asks_json.join(","),
    )
}

#[test]
fn book_reconstructs_through_storage_replayer_decode_pipeline() {
    // E2E: write snapshot + 5 diffs to storage, read via MergedReader,
    // decode each, apply to BinanceBook, assert top-N matches the
    // expected post-replay state.
    //
    // Snapshot at ts=100 with lastUpdateId=100:
    //   bids: 50000@1, 49999@2
    //   asks: 50001@3, 50002@4
    //
    // Diff 1 (ts=110, U=101, u=110): add 49998@5 bid; bump 50001 ask to qty 5.
    // Diff 2 (ts=120, U=111, u=120): remove 49999 bid; add 50003@7 ask.
    // Diff 3 (ts=130, U=121, u=130): bump 50000 bid to qty 10.
    // Diff 4 (ts=140, U=131, u=140): remove 50002 ask.
    // Diff 5 (ts=150, U=141, u=150): no-op (empty arrays).
    let tmp = TestDir::new();
    let mut store = Store::open(&cfg(tmp.path().to_path_buf())).unwrap();

    // Snapshot first — recorder stamps these on connect.
    store
        .write(&RawEvent {
            venue: Venue::Binance,
            stream: "btcusdt@depth_snapshot".into(),
            local_ts_ns: LocalTimestamp::from_nanos(100),
            venue_ts_ms: None,
            payload: snapshot_payload(
                100,
                &[("50000", "1"), ("49999", "2")],
                &[("50001", "3"), ("50002", "4")],
            ),
        })
        .unwrap();

    // Diffs in order.
    let diffs = [
        (110u128, 101u64, 110u64, vec![("49998", "5")], vec![("50001", "5")]),
        (120, 111, 120, vec![("49999", "0")], vec![("50003", "7")]),
        (130, 121, 130, vec![("50000", "10")], vec![]),
        (140, 131, 140, vec![], vec![("50002", "0")]),
        (150, 141, 150, vec![], vec![]),
    ];
    for (ts, u_first, u_final, bids, asks) in &diffs {
        store
            .write(&RawEvent {
                venue: Venue::Binance,
                stream: "btcusdt@depth@100ms".into(),
                local_ts_ns: LocalTimestamp::from_nanos(*ts),
                venue_ts_ms: Some(*ts as i64 / 1_000_000),
                payload: diff_payload(*ts as i64 / 1_000_000, *u_first, *u_final, bids, asks),
            })
            .unwrap();
    }
    store.flush_all().unwrap();

    let session = store.session_dir().to_path_buf();
    drop(store);

    // Now replay. Filter to depth-related streams only — keeps the
    // book builder's input clean.
    let filter = ReplayFilter {
        stream_prefixes: vec!["btcusdt@depth".to_string()],
        ..Default::default()
    };

    let mut book = BinanceBook::new();
    let mut applied = 0usize;
    let mut snapshot_count = 0usize;
    let mut gap_count = 0usize;

    for ev_res in open_session(&session, filter).expect("open") {
        let raw = ev_res.expect("read raw");
        let dec = decode(&raw).expect("decode");
        match book.apply(&dec).expect("apply") {
            ApplyOutcome::Applied => {
                applied += 1;
                if matches!(dec, DecodedEvent::BinanceDepthSnapshot(_)) {
                    snapshot_count += 1;
                }
            }
            ApplyOutcome::Skipped => {}
            ApplyOutcome::Gap { .. } => gap_count += 1,
        }
    }

    // Sanity counters.
    assert_eq!(applied, 6, "1 snapshot + 5 diffs should all apply");
    assert_eq!(snapshot_count, 1);
    assert_eq!(gap_count, 0);
    assert_eq!(book.last_update_id(), Some(150));
    assert_eq!(book.symbol(), Some("BTCUSDT"));

    // Final book state per the diff trace above.
    let d = |s: &str| Decimal::from_str(s).unwrap();
    assert_eq!(book.best_bid(), Some((d("50000"), d("10"))));
    assert_eq!(book.best_ask(), Some((d("50001"), d("5"))));
    assert_eq!(
        book.bids_top_n(3),
        vec![(d("50000"), d("10")), (d("49998"), d("5"))]
    );
    assert_eq!(
        book.asks_top_n(3),
        vec![(d("50001"), d("5")), (d("50003"), d("7"))]
    );
    assert_eq!(book.mid(), Some(d("50000.5")));
    assert_eq!(book.spread(), Some(d("1")));
}

#[test]
fn book_e2e_surfaces_gap_when_diff_chain_breaks() {
    // Same E2E plumbing, but skip an update id range to confirm Gap
    // is surfaced through the full pipeline (not just the unit tests).
    let tmp = TestDir::new();
    let mut store = Store::open(&cfg(tmp.path().to_path_buf())).unwrap();

    store
        .write(&RawEvent {
            venue: Venue::Binance,
            stream: "btcusdt@depth_snapshot".into(),
            local_ts_ns: LocalTimestamp::from_nanos(100),
            venue_ts_ms: None,
            payload: snapshot_payload(100, &[("100", "1")], &[("101", "1")]),
        })
        .unwrap();
    // First diff bridges the snapshot cleanly.
    store
        .write(&RawEvent {
            venue: Venue::Binance,
            stream: "btcusdt@depth@100ms".into(),
            local_ts_ns: LocalTimestamp::from_nanos(110),
            venue_ts_ms: None,
            payload: diff_payload(0, 101, 110, &[], &[]),
        })
        .unwrap();
    // Second diff jumps from u=110 expecting U=111, but we send U=200.
    store
        .write(&RawEvent {
            venue: Venue::Binance,
            stream: "btcusdt@depth@100ms".into(),
            local_ts_ns: LocalTimestamp::from_nanos(120),
            venue_ts_ms: None,
            payload: diff_payload(0, 200, 210, &[], &[]),
        })
        .unwrap();
    store.flush_all().unwrap();

    let session = store.session_dir().to_path_buf();
    drop(store);

    let mut book = BinanceBook::new();
    let mut outcomes = Vec::new();
    for ev in open_session(&session, ReplayFilter::default()).unwrap() {
        let raw = ev.unwrap();
        let dec = decode(&raw).unwrap();
        outcomes.push(book.apply(&dec).unwrap());
    }
    // Order: snapshot Applied, diff_1 Applied, diff_2 Gap.
    assert_eq!(outcomes.len(), 3);
    assert_eq!(outcomes[0], ApplyOutcome::Applied);
    assert_eq!(outcomes[1], ApplyOutcome::Applied);
    assert_eq!(
        outcomes[2],
        ApplyOutcome::Gap {
            expected: 111,
            got: 200
        }
    );
    // last_update_id pinned at the pre-gap value.
    assert_eq!(book.last_update_id(), Some(110));
}
