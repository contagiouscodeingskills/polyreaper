//! Binance Spot L2 book reconstruction.
//!
//! ## Splice rules (per Binance docs)
//!
//! 1. Open the WebSocket and start buffering `@depth@100ms` diffs.
//! 2. Fetch a `/api/v3/depth?limit=1000` snapshot. It carries
//!    `lastUpdateId = X`.
//! 3. **Drop** every buffered diff whose final-update-id `u <= X`.
//! 4. The first surviving diff must satisfy `U <= X+1 <= u`, otherwise
//!    abort and re-fetch the snapshot.
//! 5. Subsequent diffs must form a chain: `diff_i.U == diff_{i-1}.u + 1`.
//!
//! The recorder writes the snapshot under `<symbol>@depth_snapshot` once
//! per WebSocket connect. The replayer reads them in order, calling
//! [`BinanceBook::apply`] with whatever variant of [`DecodedEvent`]
//! shows up next:
//!
//! * `BinanceDepthSnapshot` → reset and re-anchor to that snapshot.
//! * `BinanceDepthDiff` → splice on top, returning [`ApplyOutcome::Gap`]
//!   if the chain broke. The book stays in its post-gap state until
//!   the caller resets it with the next snapshot.

use std::collections::BTreeMap;

use rust_decimal::Decimal;

use crate::book::ApplyOutcome;
use crate::decode::{BinanceDepthDiff, BinanceDepthSnapshot, PriceLevel};
use crate::DecodedEvent;
use crate::ReplayError;

/// L2 book for one Binance symbol.
///
/// Internal storage: `BTreeMap<Decimal, Decimal>` per side, with
/// `qty == 0` levels deleted on apply (Binance's diff convention).
/// `bids` is read in *reverse* order (highest price first); `asks` in
/// natural order (lowest price first).
#[derive(Debug, Clone, Default)]
pub struct BinanceBook {
    /// Highest-price-first when iterated in reverse. Key = price, value = qty.
    bids: BTreeMap<Decimal, Decimal>,
    /// Lowest-price-first. Key = price, value = qty.
    asks: BTreeMap<Decimal, Decimal>,
    /// `None` until the first snapshot. Once set, the book is "live"
    /// and subsequent diffs are spliced against it.
    last_update_id: Option<u64>,
    /// Symbol this book tracks (uppercase like Binance returns).
    /// `None` before first snapshot/diff.
    symbol: Option<String>,
}

impl BinanceBook {
    /// Empty book — won't apply diffs until a snapshot arrives.
    pub fn new() -> Self {
        Self::default()
    }

    /// Fresh book from a REST snapshot.
    pub fn from_snapshot(s: &BinanceDepthSnapshot) -> Self {
        let mut b = Self::new();
        b.reset_with_snapshot(s);
        b
    }

    /// True once we've ingested at least one snapshot.
    pub fn is_live(&self) -> bool {
        self.last_update_id.is_some()
    }

    /// Snapshot's `lastUpdateId`, advanced by each applied diff's `u`.
    pub fn last_update_id(&self) -> Option<u64> {
        self.last_update_id
    }

    pub fn symbol(&self) -> Option<&str> {
        self.symbol.as_deref()
    }

    /// Apply one decoded event. Returns:
    ///
    /// * `Applied` — book updated (snapshot or in-sequence diff).
    /// * `Skipped` — pre-snapshot diff (or unrelated decoded variant).
    /// * `Gap{..}` — sequence break; book is stale until next snapshot.
    pub fn apply(&mut self, e: &DecodedEvent) -> Result<ApplyOutcome, ReplayError> {
        match e {
            DecodedEvent::BinanceDepthSnapshot(s) => {
                self.reset_with_snapshot(s);
                Ok(ApplyOutcome::Applied)
            }
            DecodedEvent::BinanceDepthDiff(d) => self.apply_diff(d),
            _ => Ok(ApplyOutcome::Skipped),
        }
    }

    /// Reset state to match `s`, replacing any prior book entirely.
    pub fn reset_with_snapshot(&mut self, s: &BinanceDepthSnapshot) {
        self.bids.clear();
        self.asks.clear();
        for lv in &s.bids {
            insert_level(&mut self.bids, lv);
        }
        for lv in &s.asks {
            insert_level(&mut self.asks, lv);
        }
        self.last_update_id = Some(s.last_update_id);
    }

    /// Apply one diff, returning `Skipped` if it's stale (u ≤ baseline),
    /// `Gap` if the chain broke, `Applied` otherwise.
    pub fn apply_diff(&mut self, d: &BinanceDepthDiff) -> Result<ApplyOutcome, ReplayError> {
        // Track symbol — useful for sanity-checks if researchers feed
        // diffs from multiple symbols by accident.
        if self.symbol.is_none() {
            self.symbol = Some(d.symbol.clone());
        }

        let last = match self.last_update_id {
            Some(x) => x,
            // No snapshot yet — diffs are unusable. Skip cleanly.
            None => return Ok(ApplyOutcome::Skipped),
        };

        // Stale: this diff was already absorbed in the snapshot.
        if d.final_update_id <= last {
            return Ok(ApplyOutcome::Skipped);
        }

        // First diff after a (re)snapshot must straddle `last+1`. After
        // that, the chain is U == last+1.
        if d.first_update_id > last + 1 {
            return Ok(ApplyOutcome::Gap {
                expected: last + 1,
                got: d.first_update_id,
            });
        }

        // Splice.
        for lv in &d.bids {
            insert_level(&mut self.bids, lv);
        }
        for lv in &d.asks {
            insert_level(&mut self.asks, lv);
        }
        self.last_update_id = Some(d.final_update_id);
        Ok(ApplyOutcome::Applied)
    }

    // ----- accessors -----

    /// Top `n` bid levels, highest price first. Each entry is
    /// `(price, qty)`. May be shorter than `n` if the book is thin.
    pub fn bids_top_n(&self, n: usize) -> Vec<(Decimal, Decimal)> {
        self.bids
            .iter()
            .rev()
            .take(n)
            .map(|(p, q)| (*p, *q))
            .collect()
    }

    /// Top `n` ask levels, lowest price first.
    pub fn asks_top_n(&self, n: usize) -> Vec<(Decimal, Decimal)> {
        self.asks.iter().take(n).map(|(p, q)| (*p, *q)).collect()
    }

    pub fn best_bid(&self) -> Option<(Decimal, Decimal)> {
        self.bids.iter().next_back().map(|(p, q)| (*p, *q))
    }

    pub fn best_ask(&self) -> Option<(Decimal, Decimal)> {
        self.asks.iter().next().map(|(p, q)| (*p, *q))
    }

    /// `(best_bid + best_ask) / 2`, or `None` if either side is empty.
    pub fn mid(&self) -> Option<Decimal> {
        let (b, _) = self.best_bid()?;
        let (a, _) = self.best_ask()?;
        Some((b + a) / Decimal::from(2))
    }

    /// `best_ask - best_bid`, or `None` if either side is empty.
    pub fn spread(&self) -> Option<Decimal> {
        let (b, _) = self.best_bid()?;
        let (a, _) = self.best_ask()?;
        Some(a - b)
    }
}

fn insert_level(side: &mut BTreeMap<Decimal, Decimal>, lv: &PriceLevel) {
    if lv.qty.is_zero() {
        side.remove(&lv.price);
    } else {
        side.insert(lv.price, lv.qty);
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

    fn snap(last_id: u64, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> BinanceDepthSnapshot {
        BinanceDepthSnapshot {
            local_ts_ns: 0,
            last_update_id: last_id,
            bids: bids.iter().map(|(p, q)| PriceLevel::new(d(p), d(q))).collect(),
            asks: asks.iter().map(|(p, q)| PriceLevel::new(d(p), d(q))).collect(),
        }
    }

    fn diff(
        u_first: u64,
        u_final: u64,
        bids: &[(&str, &str)],
        asks: &[(&str, &str)],
    ) -> BinanceDepthDiff {
        BinanceDepthDiff {
            local_ts_ns: 0,
            event_time_ms: 0,
            symbol: "BTCUSDT".into(),
            first_update_id: u_first,
            final_update_id: u_final,
            bids: bids.iter().map(|(p, q)| PriceLevel::new(d(p), d(q))).collect(),
            asks: asks.iter().map(|(p, q)| PriceLevel::new(d(p), d(q))).collect(),
        }
    }

    #[test]
    fn snapshot_initialises_book_and_marks_live() {
        let s = snap(10, &[("100", "1"), ("99", "2")], &[("101", "3")]);
        let b = BinanceBook::from_snapshot(&s);
        assert!(b.is_live());
        assert_eq!(b.last_update_id(), Some(10));
        assert_eq!(b.best_bid(), Some((d("100"), d("1"))));
        assert_eq!(b.best_ask(), Some((d("101"), d("3"))));
        assert_eq!(b.mid(), Some(d("100.5")));
        assert_eq!(b.spread(), Some(d("1")));
    }

    #[test]
    fn diffs_before_snapshot_are_skipped() {
        let mut b = BinanceBook::new();
        let r = b.apply_diff(&diff(1, 2, &[], &[])).unwrap();
        assert_eq!(r, ApplyOutcome::Skipped);
        assert!(!b.is_live());
    }

    #[test]
    fn stale_diff_with_u_le_baseline_is_skipped() {
        let s = snap(10, &[("100", "1")], &[("101", "1")]);
        let mut b = BinanceBook::from_snapshot(&s);
        // u = 8 < 10 → stale.
        let r = b.apply_diff(&diff(8, 9, &[], &[])).unwrap();
        assert_eq!(r, ApplyOutcome::Skipped);
        assert_eq!(b.last_update_id(), Some(10));
    }

    #[test]
    fn first_diff_straddling_baseline_applies() {
        let s = snap(10, &[("100", "1")], &[("101", "1")]);
        let mut b = BinanceBook::from_snapshot(&s);
        // (8, 11) covers 11 = 10+1 → applies, advances to 11.
        let r = b
            .apply_diff(&diff(8, 11, &[("99", "5")], &[("102", "2")]))
            .unwrap();
        assert_eq!(r, ApplyOutcome::Applied);
        assert_eq!(b.last_update_id(), Some(11));
        assert_eq!(b.bids.get(&d("99")), Some(&d("5")));
        assert_eq!(b.asks.get(&d("102")), Some(&d("2")));
    }

    #[test]
    fn chained_diffs_continue_in_sequence() {
        let s = snap(10, &[("100", "1")], &[("101", "1")]);
        let mut b = BinanceBook::from_snapshot(&s);
        b.apply_diff(&diff(11, 14, &[], &[])).unwrap(); // bridges 10→14
        let r = b.apply_diff(&diff(15, 17, &[("100", "5")], &[])).unwrap();
        assert_eq!(r, ApplyOutcome::Applied);
        assert_eq!(b.last_update_id(), Some(17));
        assert_eq!(b.bids.get(&d("100")), Some(&d("5")));
    }

    #[test]
    fn gap_returns_gap_outcome_with_expected_and_got() {
        let s = snap(10, &[], &[]);
        let mut b = BinanceBook::from_snapshot(&s);
        b.apply_diff(&diff(11, 14, &[], &[])).unwrap();
        // Skip 15..19 — next diff starts at 20.
        let r = b.apply_diff(&diff(20, 22, &[], &[])).unwrap();
        assert_eq!(r, ApplyOutcome::Gap { expected: 15, got: 20 });
        // After Gap, last_update_id remains at the pre-gap value.
        assert_eq!(b.last_update_id(), Some(14));
    }

    #[test]
    fn qty_zero_removes_level() {
        let s = snap(1, &[("100", "1"), ("99", "2")], &[("101", "1")]);
        let mut b = BinanceBook::from_snapshot(&s);
        b.apply_diff(&diff(2, 3, &[("99", "0")], &[])).unwrap();
        assert!(b.bids.get(&d("99")).is_none());
        assert_eq!(b.bids.len(), 1);
    }

    #[test]
    fn snapshot_in_apply_resets_book() {
        let s1 = snap(10, &[("100", "1")], &[("101", "1")]);
        let mut b = BinanceBook::from_snapshot(&s1);
        b.apply_diff(&diff(11, 12, &[("100", "5")], &[])).unwrap();
        assert_eq!(b.bids.get(&d("100")), Some(&d("5")));

        // New snapshot via apply() — book is reset, new baseline.
        let s2 = snap(50, &[("200", "10")], &[("201", "10")]);
        let r = b.apply(&DecodedEvent::BinanceDepthSnapshot(s2)).unwrap();
        assert_eq!(r, ApplyOutcome::Applied);
        assert_eq!(b.last_update_id(), Some(50));
        assert_eq!(b.best_bid(), Some((d("200"), d("10"))));
        assert!(b.bids.get(&d("100")).is_none());
    }

    #[test]
    fn unrelated_decoded_event_is_skipped() {
        let mut b = BinanceBook::new();
        let unknown = DecodedEvent::Unknown {
            local_ts_ns: 0,
            venue: common::Venue::Binance,
            stream: "x".into(),
            value: serde_json::json!({"foo": "bar"}),
        };
        assert_eq!(b.apply(&unknown).unwrap(), ApplyOutcome::Skipped);
    }

    #[test]
    fn bids_top_n_is_descending_asks_ascending() {
        let s = snap(
            1,
            &[("100", "1"), ("99", "1"), ("98", "1"), ("97", "1")],
            &[("101", "1"), ("102", "1"), ("103", "1"), ("104", "1")],
        );
        let b = BinanceBook::from_snapshot(&s);
        let bids: Vec<_> = b.bids_top_n(3).into_iter().map(|(p, _)| p).collect();
        let asks: Vec<_> = b.asks_top_n(3).into_iter().map(|(p, _)| p).collect();
        assert_eq!(bids, vec![d("100"), d("99"), d("98")]);
        assert_eq!(asks, vec![d("101"), d("102"), d("103")]);
    }
}
