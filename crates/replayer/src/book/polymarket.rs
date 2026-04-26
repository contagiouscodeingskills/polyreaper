//! Polymarket per-market book reconstruction.
//!
//! A Polymarket binary market has two outcomes (Yes / No), each with
//! its own `asset_id` and its own order book. The market channel sends:
//!
//! * `book` events — full snapshot for one `asset_id` (one outcome).
//! * `price_change` events — diff for one `asset_id`.
//!
//! [`PolymarketMarketBook`] composes both sides into a single market
//! view:
//!
//! ```text
//!     yes_book: PolymarketSideBook
//!     no_book:  PolymarketSideBook
//! ```
//!
//! Both sides are tracked independently because their snapshots arrive
//! independently — `apply_book` for the Yes asset only resets the Yes
//! side.
//!
//! ## Pre-snapshot buffering
//!
//! In production the recorder gets a `book` snapshot first (Polymarket
//! sends one per asset on connect). But during replay, file ordering
//! interleaves events by `local_ts_ns` — there's no guarantee a side's
//! snapshot precedes its first `price_change` in the merged stream.
//!
//! When a `price_change` arrives before that side's snapshot, we
//! buffer it (cap `MAX_BUFFER`, drop oldest with a `tracing::warn!`)
//! and replay buffered changes after the snapshot lands. This matches
//! the design plan: "While awaiting: buffer up to N=100 price_changes,
//! drop oldest if exceeded."

use std::collections::{BTreeMap, VecDeque};

use rust_decimal::Decimal;

use crate::book::ApplyOutcome;
use crate::decode::{
    PolymarketBook, PolymarketLevel, PolymarketPriceChange, PolymarketPriceChangeItem,
    PolymarketSide,
};
use crate::DecodedEvent;
use crate::ReplayError;

/// Maximum number of pre-snapshot price-changes to hold per side.
/// Above this we warn and drop the oldest. Polymarket's snapshot
/// cadence is 1/connection so this cap is mostly insurance against
/// pathological recordings; live captures rarely exceed single digits.
const MAX_BUFFER: usize = 100;

// ---------------------------------------------------------------------------
// One-sided book (Yes side or No side)
// ---------------------------------------------------------------------------

/// One outcome's book — `(price, size)` BTreeMaps for bids and asks.
///
/// Polymarket prices are 0.0–1.0 so `bids_top_n` returning highest
/// price first is the same convention as Binance.
#[derive(Debug, Clone, Default)]
pub struct PolymarketSideBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    /// Polymarket sends `hash` on each book snapshot — keep the last
    /// one for sequence-debugging. Optional because some payloads omit.
    last_hash: Option<String>,
    /// `timestamp_ms` from the most recent `book` snapshot, if any.
    /// Used to decide which buffered price_changes to replay.
    last_snapshot_ts_ms: Option<i64>,
    /// True after at least one `book` event has been applied.
    has_snapshot: bool,
}

impl PolymarketSideBook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn has_snapshot(&self) -> bool {
        self.has_snapshot
    }

    pub fn last_hash(&self) -> Option<&str> {
        self.last_hash.as_deref()
    }

    /// Replace the entire side from a snapshot.
    pub fn reset_with_snapshot(&mut self, b: &PolymarketBook) {
        self.bids.clear();
        self.asks.clear();
        for lv in &b.bids {
            insert_level(&mut self.bids, lv);
        }
        for lv in &b.asks {
            insert_level(&mut self.asks, lv);
        }
        self.last_hash = b.hash.clone();
        self.last_snapshot_ts_ms = b.timestamp_ms;
        self.has_snapshot = true;
    }

    /// Apply one `price_change` item: set or remove a level.
    /// `side == BUY` → bid side; `SELL` → ask side.
    pub fn apply_price_change_item(&mut self, item: &PolymarketPriceChangeItem) {
        let side = match item.side {
            PolymarketSide::Buy => &mut self.bids,
            PolymarketSide::Sell => &mut self.asks,
        };
        if item.size.is_zero() {
            side.remove(&item.price);
        } else {
            side.insert(item.price, item.size);
        }
    }

    pub fn bids_top_n(&self, n: usize) -> Vec<(Decimal, Decimal)> {
        self.bids
            .iter()
            .rev()
            .take(n)
            .map(|(p, q)| (*p, *q))
            .collect()
    }

    pub fn asks_top_n(&self, n: usize) -> Vec<(Decimal, Decimal)> {
        self.asks.iter().take(n).map(|(p, q)| (*p, *q)).collect()
    }

    pub fn best_bid(&self) -> Option<(Decimal, Decimal)> {
        self.bids.iter().next_back().map(|(p, q)| (*p, *q))
    }

    pub fn best_ask(&self) -> Option<(Decimal, Decimal)> {
        self.asks.iter().next().map(|(p, q)| (*p, *q))
    }

    pub fn mid(&self) -> Option<Decimal> {
        let (b, _) = self.best_bid()?;
        let (a, _) = self.best_ask()?;
        Some((b + a) / Decimal::from(2))
    }

    pub fn spread(&self) -> Option<Decimal> {
        let (b, _) = self.best_bid()?;
        let (a, _) = self.best_ask()?;
        Some(a - b)
    }
}

// ---------------------------------------------------------------------------
// Two-sided market book
// ---------------------------------------------------------------------------

/// Yes/No book for one Polymarket market.
///
/// Construct with the market's `condition_id` and the two `asset_id`s
/// for Yes and No outcomes. (Get these from the `market_registry`
/// crate at the call site — the replayer crate doesn't depend on the
/// registry to keep its dep graph small.)
#[derive(Debug, Clone)]
pub struct PolymarketMarketBook {
    market: String,
    yes_asset_id: String,
    no_asset_id: String,
    yes_book: PolymarketSideBook,
    no_book: PolymarketSideBook,
    /// Per-side buffer of price_changes that arrived before that
    /// side's snapshot. Replayed after the snapshot lands; trimmed to
    /// `MAX_BUFFER` (drop oldest) so a runaway recording can't OOM us.
    yes_pending: VecDeque<PolymarketPriceChange>,
    no_pending: VecDeque<PolymarketPriceChange>,
}

impl PolymarketMarketBook {
    pub fn new(market: &str, yes_asset_id: &str, no_asset_id: &str) -> Self {
        Self {
            market: market.to_string(),
            yes_asset_id: yes_asset_id.to_string(),
            no_asset_id: no_asset_id.to_string(),
            yes_book: PolymarketSideBook::new(),
            no_book: PolymarketSideBook::new(),
            yes_pending: VecDeque::new(),
            no_pending: VecDeque::new(),
        }
    }

    pub fn market(&self) -> &str {
        &self.market
    }

    pub fn yes_book(&self) -> &PolymarketSideBook {
        &self.yes_book
    }

    pub fn no_book(&self) -> &PolymarketSideBook {
        &self.no_book
    }

    /// Apply one decoded event. Events for other markets / asset_ids
    /// return `Skipped`; events that progress this market's state
    /// return `Applied`. There's no `Gap` outcome for Polymarket —
    /// the wire protocol doesn't expose a sequence number we can chain
    /// on, so we trust the order-by-`local_ts_ns` ordering and the
    /// hash field for hindsight checks.
    pub fn apply(&mut self, e: &DecodedEvent) -> Result<ApplyOutcome, ReplayError> {
        match e {
            DecodedEvent::PolymarketBook(b) if b.market == self.market => {
                self.apply_book(b);
                Ok(ApplyOutcome::Applied)
            }
            DecodedEvent::PolymarketPriceChange(p) if p.market == self.market => {
                Ok(self.apply_price_change(p))
            }
            _ => Ok(ApplyOutcome::Skipped),
        }
    }

    /// Apply a `book` snapshot to whichever side's `asset_id` matches.
    /// After the snapshot lands, replays any buffered price_changes
    /// for that side whose `timestamp_ms` is `> snapshot.timestamp_ms`
    /// (or all of them if the snapshot has no timestamp).
    pub fn apply_book(&mut self, b: &PolymarketBook) {
        if b.asset_id == self.yes_asset_id {
            self.yes_book.reset_with_snapshot(b);
            replay_pending(&mut self.yes_pending, &mut self.yes_book);
        } else if b.asset_id == self.no_asset_id {
            self.no_book.reset_with_snapshot(b);
            replay_pending(&mut self.no_pending, &mut self.no_book);
        }
        // Else: snapshot for an asset_id we don't track — silently ignore.
    }

    /// Apply (or buffer) a `price_change` for whichever side it
    /// targets. Returns `Skipped` if the change is for an unrelated
    /// `asset_id`, `Applied` if it touched our state (whether by
    /// updating the live book or being buffered).
    pub fn apply_price_change(&mut self, p: &PolymarketPriceChange) -> ApplyOutcome {
        if p.asset_id == self.yes_asset_id {
            apply_or_buffer(&mut self.yes_book, &mut self.yes_pending, p);
            ApplyOutcome::Applied
        } else if p.asset_id == self.no_asset_id {
            apply_or_buffer(&mut self.no_book, &mut self.no_pending, p);
            ApplyOutcome::Applied
        } else {
            ApplyOutcome::Skipped
        }
    }

    /// Pending-buffer length per side (Yes, No). Useful for tests and
    /// for diagnosing recorder-snapshot-missing situations.
    pub fn pending_lengths(&self) -> (usize, usize) {
        (self.yes_pending.len(), self.no_pending.len())
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn apply_or_buffer(
    side: &mut PolymarketSideBook,
    pending: &mut VecDeque<PolymarketPriceChange>,
    p: &PolymarketPriceChange,
) {
    if side.has_snapshot {
        for item in &p.price_changes {
            side.apply_price_change_item(item);
        }
    } else {
        if pending.len() >= MAX_BUFFER {
            tracing::warn!(
                component = "replayer",
                market = %p.market,
                asset_id = %p.asset_id,
                buffered = pending.len(),
                cap = MAX_BUFFER,
                "polymarket pre-snapshot buffer full; dropping oldest"
            );
            pending.pop_front();
        }
        pending.push_back(p.clone());
    }
}

fn replay_pending(pending: &mut VecDeque<PolymarketPriceChange>, side: &mut PolymarketSideBook) {
    let snapshot_ts = side.last_snapshot_ts_ms;
    while let Some(pc) = pending.pop_front() {
        let pc_ts = pc.timestamp_ms;
        let strictly_after = match (snapshot_ts, pc_ts) {
            (Some(snap_ts), Some(t)) => t > snap_ts,
            // If either is missing we can't compare — replay anyway.
            // Safer to over-apply (the change is at most a no-op since
            // the snapshot already reflects it) than to lose updates.
            _ => true,
        };
        if strictly_after {
            for item in &pc.price_changes {
                side.apply_price_change_item(item);
            }
        }
    }
}

fn insert_level(side: &mut BTreeMap<Decimal, Decimal>, lv: &PolymarketLevel) {
    if lv.size.is_zero() {
        side.remove(&lv.price);
    } else {
        side.insert(lv.price, lv.size);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn book(asset_id: &str, market: &str, ts: i64, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> PolymarketBook {
        PolymarketBook {
            local_ts_ns: 0,
            asset_id: asset_id.into(),
            market: market.into(),
            timestamp_ms: Some(ts),
            hash: Some("h".into()),
            bids: bids
                .iter()
                .map(|(p, q)| PolymarketLevel { price: d(p), size: d(q) })
                .collect(),
            asks: asks
                .iter()
                .map(|(p, q)| PolymarketLevel { price: d(p), size: d(q) })
                .collect(),
        }
    }

    fn pc(
        asset_id: &str,
        market: &str,
        ts: i64,
        items: &[(PolymarketSide, &str, &str)],
    ) -> PolymarketPriceChange {
        PolymarketPriceChange {
            local_ts_ns: 0,
            asset_id: asset_id.into(),
            market: market.into(),
            timestamp_ms: Some(ts),
            hash: None,
            price_changes: items
                .iter()
                .map(|(side, price, size)| PolymarketPriceChangeItem {
                    price: d(price),
                    size: d(size),
                    side: *side,
                    best_bid: None,
                    best_ask: None,
                    hash: None,
                })
                .collect(),
        }
    }

    fn fresh() -> PolymarketMarketBook {
        PolymarketMarketBook::new("0xMKT", "YES", "NO")
    }

    #[test]
    fn snapshot_initialises_correct_side() {
        let mut m = fresh();
        m.apply_book(&book(
            "YES",
            "0xMKT",
            100,
            &[("0.50", "10")],
            &[("0.51", "5")],
        ));
        assert!(m.yes_book().has_snapshot());
        assert!(!m.no_book().has_snapshot());
        assert_eq!(m.yes_book().best_bid(), Some((d("0.50"), d("10"))));
    }

    #[test]
    fn snapshot_for_unrelated_asset_is_ignored() {
        let mut m = fresh();
        m.apply_book(&book("OTHER", "0xMKT", 100, &[("0.5", "1")], &[]));
        assert!(!m.yes_book().has_snapshot());
        assert!(!m.no_book().has_snapshot());
    }

    #[test]
    fn price_change_applies_after_snapshot() {
        let mut m = fresh();
        m.apply_book(&book(
            "YES",
            "0xMKT",
            100,
            &[("0.50", "10")],
            &[("0.51", "5")],
        ));
        m.apply_price_change(&pc(
            "YES",
            "0xMKT",
            200,
            &[(PolymarketSide::Buy, "0.50", "20"), (PolymarketSide::Sell, "0.51", "0")],
        ));
        assert_eq!(m.yes_book().best_bid(), Some((d("0.50"), d("20"))));
        // 0.51 ask removed.
        assert_eq!(m.yes_book().best_ask(), None);
    }

    #[test]
    fn price_change_buffered_before_snapshot_replays_after() {
        let mut m = fresh();
        // Pre-snapshot price changes for YES.
        m.apply_price_change(&pc(
            "YES",
            "0xMKT",
            150,
            &[(PolymarketSide::Buy, "0.55", "10")],
        ));
        m.apply_price_change(&pc(
            "YES",
            "0xMKT",
            200,
            &[(PolymarketSide::Sell, "0.56", "5")],
        ));
        assert_eq!(m.pending_lengths(), (2, 0));

        // Snapshot lands at ts=100. Both pending changes are after, so
        // both replay.
        m.apply_book(&book(
            "YES",
            "0xMKT",
            100,
            &[("0.50", "100")],
            &[("0.60", "100")],
        ));
        assert_eq!(m.pending_lengths(), (0, 0));
        assert_eq!(m.yes_book().best_bid(), Some((d("0.55"), d("10"))));
        assert_eq!(m.yes_book().best_ask(), Some((d("0.56"), d("5"))));
    }

    #[test]
    fn buffered_changes_strictly_older_than_snapshot_are_dropped() {
        let mut m = fresh();
        // Two price changes at ts=50 and ts=200.
        m.apply_price_change(&pc("YES", "0xMKT", 50, &[(PolymarketSide::Buy, "0.40", "1")]));
        m.apply_price_change(&pc("YES", "0xMKT", 200, &[(PolymarketSide::Buy, "0.41", "2")]));
        // Snapshot at ts=100 — only the 200ts change replays.
        m.apply_book(&book("YES", "0xMKT", 100, &[("0.50", "10")], &[]));
        assert_eq!(m.yes_book().best_bid(), Some((d("0.50"), d("10"))));
        // After replay, the 200-ts price_change inserts 0.41 with size 2.
        // 0.50 is still top because 0.41 < 0.50.
        let bids: Vec<_> = m.yes_book().bids_top_n(5).into_iter().collect();
        assert!(bids.contains(&(d("0.41"), d("2"))));
        // The 50-ts change at 0.40 was dropped.
        assert!(!bids.iter().any(|(p, _)| *p == d("0.40")));
    }

    #[test]
    fn yes_and_no_sides_are_independent() {
        let mut m = fresh();
        m.apply_book(&book("YES", "0xMKT", 100, &[("0.50", "1")], &[]));
        m.apply_book(&book("NO", "0xMKT", 100, &[("0.49", "1")], &[]));
        assert_eq!(m.yes_book().best_bid(), Some((d("0.50"), d("1"))));
        assert_eq!(m.no_book().best_bid(), Some((d("0.49"), d("1"))));
    }

    #[test]
    fn unrelated_market_returns_skipped() {
        let mut m = fresh();
        let other = book("YES", "0xOTHER", 100, &[("0.5", "1")], &[]);
        let r = m
            .apply(&DecodedEvent::PolymarketBook(other))
            .unwrap();
        assert_eq!(r, ApplyOutcome::Skipped);
        assert!(!m.yes_book().has_snapshot());
    }

    #[test]
    fn buffer_cap_drops_oldest_when_overrun() {
        let mut m = fresh();
        for i in 0..(MAX_BUFFER + 5) {
            // Each price change has a unique ts; the first 5 should be dropped.
            let c = pc(
                "YES",
                "0xMKT",
                i as i64 + 1,
                &[(PolymarketSide::Buy, "0.50", "1")],
            );
            m.apply_price_change(&c);
        }
        assert_eq!(m.pending_lengths().0, MAX_BUFFER);
    }

    #[test]
    fn unrelated_decoded_event_is_skipped() {
        let mut m = fresh();
        let unknown = DecodedEvent::Unknown {
            local_ts_ns: 0,
            venue: common::Venue::Polymarket,
            stream: "x".into(),
            value: serde_json::json!({"foo":"bar"}),
        };
        assert_eq!(m.apply(&unknown).unwrap(), ApplyOutcome::Skipped);
    }
}
