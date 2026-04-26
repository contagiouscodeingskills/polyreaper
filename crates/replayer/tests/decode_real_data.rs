//! Smoke tests against real captured session data.
//!
//! Skip silently if no local capture exists — keeps these useful in
//! CI (where there's no captured data) without flagging them as
//! broken. When data is present, they catch unexpected payload shapes
//! from live venues that synthetic fixtures wouldn't cover.

use std::collections::HashSet;
use std::path::PathBuf;

use common::Venue;
use replayer::decode::{decode, DecodedEvent};
use replayer::{open_base_dir, ApplyOutcome, BinanceBook, ReplayFilter};

fn data_dir() -> Option<PathBuf> {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("data");
    if p.is_dir() && std::fs::read_dir(&p).ok()?.next().is_some() {
        Some(p)
    } else {
        None
    }
}

#[test]
fn decode_succeeds_on_every_event_in_local_captures() {
    let Some(root) = data_dir() else {
        eprintln!("[skip] no local data/ directory — capture some sessions first");
        return;
    };

    let mut total = 0usize;
    let mut by_variant: std::collections::HashMap<&'static str, usize> = Default::default();
    let mut decode_errors: Vec<String> = Vec::new();

    for ev_res in open_base_dir(&root, ReplayFilter::default()).expect("open base dir") {
        let ev = ev_res.expect("read raw event");
        total += 1;
        match decode(&ev) {
            Ok(d) => {
                let key = match d {
                    DecodedEvent::BinanceTrade(_) => "BinanceTrade",
                    DecodedEvent::BinanceDepthDiff(_) => "BinanceDepthDiff",
                    DecodedEvent::BinanceDepthSnapshot(_) => "BinanceDepthSnapshot",
                    DecodedEvent::BinanceBookTicker(_) => "BinanceBookTicker",
                    DecodedEvent::PolymarketBook(_) => "PolymarketBook",
                    DecodedEvent::PolymarketPriceChange(_) => "PolymarketPriceChange",
                    DecodedEvent::PolymarketLastTradePrice(_) => "PolymarketLastTradePrice",
                    DecodedEvent::PolymarketTickSizeChange(_) => "PolymarketTickSizeChange",
                    DecodedEvent::PolymarketResolution(_) => "PolymarketResolution",
                    DecodedEvent::CoinbaseMarketTrades(_) => "CoinbaseMarketTrades",
                    DecodedEvent::ChainlinkLog(_) => "ChainlinkLog",
                    DecodedEvent::Unknown { .. } => "Unknown",
                };
                *by_variant.entry(key).or_default() += 1;
            }
            Err(e) => {
                if decode_errors.len() < 5 {
                    decode_errors.push(format!(
                        "venue={:?} stream={} payload={}: {e}",
                        ev.venue,
                        ev.stream,
                        if ev.payload.len() > 120 {
                            format!("{}…", &ev.payload[..120])
                        } else {
                            ev.payload.clone()
                        }
                    ));
                }
            }
        }
    }

    eprintln!("decoded {total} events:");
    let mut variants: Vec<_> = by_variant.iter().collect();
    variants.sort_by_key(|(_, v)| std::cmp::Reverse(**v));
    for (variant, count) in variants {
        eprintln!("  {variant:30} {count:>8}");
    }

    if !decode_errors.is_empty() {
        for err in &decode_errors {
            eprintln!("decode error: {err}");
        }
        panic!(
            "{} decode errors out of {total} events (showing up to 5)",
            decode_errors.len()
        );
    }

    assert!(total > 0, "data/ directory empty?");
}

#[test]
fn binance_book_reconstructs_from_real_snapshot_and_diffs() {
    let Some(root) = data_dir() else {
        eprintln!("[skip] no local data/ directory");
        return;
    };

    // Filter to Binance + the depth streams (snapshot + diff). If a
    // session has neither, skip — the assertion at the bottom keeps us
    // honest about whether we actually saw events.
    let mut venues = HashSet::new();
    venues.insert(Venue::Binance);
    let filter = ReplayFilter {
        venues: Some(venues),
        stream_prefixes: vec!["btcusdt@depth".to_string()],
        ..Default::default()
    };

    let mut book = BinanceBook::new();
    let mut applied = 0usize;
    let mut skipped = 0usize;
    let mut gaps: Vec<(u64, u64)> = Vec::new();

    for ev in open_base_dir(&root, filter).expect("open base") {
        let ev = ev.expect("read");
        let dec = decode(&ev).expect("decode");
        match book.apply(&dec).expect("apply") {
            ApplyOutcome::Applied => applied += 1,
            ApplyOutcome::Skipped => skipped += 1,
            ApplyOutcome::Gap { expected, got } => {
                gaps.push((expected, got));
                // Treat the next snapshot as recovery — common in real data
                // because the recorder fetches a fresh snapshot every reconnect.
            }
        }
    }

    if applied == 0 {
        if skipped > 0 {
            eprintln!(
                "[skip] {skipped} depth events seen but never went live — \
                 likely captured by an older recorder that didn't fetch \
                 snapshots. Re-record with the current recorder to exercise \
                 book reconstruction here."
            );
        } else {
            eprintln!("[skip] no Binance depth events in local captures");
        }
        return;
    }

    eprintln!(
        "binance book: applied={applied} skipped={skipped} gaps={gap_count} \
         last_update_id={lui:?} symbol={sym:?}",
        gap_count = gaps.len(),
        lui = book.last_update_id(),
        sym = book.symbol(),
    );

    // Top-of-book sanity. We assume captured data is honest — bid < ask,
    // mid roughly in the BTC range. Failing here means either book
    // reconstruction is broken OR the recorder captured rubbish.
    let bid = book.best_bid().expect("non-empty bids after replay");
    let ask = book.best_ask().expect("non-empty asks after replay");
    assert!(
        bid.0 < ask.0,
        "best bid {} must be below best ask {}",
        bid.0,
        ask.0
    );
    let mid = book.mid().expect("mid");
    let one_k = rust_decimal::Decimal::from(1_000);
    let one_m = rust_decimal::Decimal::from(1_000_000);
    assert!(
        mid > one_k && mid < one_m,
        "BTC mid {mid} outside [1k, 1m] — book reconstruction looks wrong"
    );
}
