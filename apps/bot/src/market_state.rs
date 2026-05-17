//! State for the single currently-active market.
//!
//! Tracks the chosen market, the BTC-implied strike (snapped from our
//! rolling history at the market's start time), the latest Polymarket
//! YES-mid, and time-to-resolution. The bot loop reads from this when
//! deciding whether to fire.

use std::collections::VecDeque;

use market_registry::Market;
use serde::{Deserialize, Serialize};

/// Top-of-book snapshot for one Polymarket market — both YES and NO
/// sides. Each side is independent; we don't assume `yes + no = 1`
/// because Polymarket's two tokens have separate order books and small
/// arbitrage gaps are routine.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PolyBookSnapshot {
    pub yes_bid: Option<f64>,
    pub yes_ask: Option<f64>,
    pub no_bid: Option<f64>,
    pub no_ask: Option<f64>,
    /// Local-clock ns at which the snapshot was assembled.
    pub captured_local_ts_ns: u128,
}

impl PolyBookSnapshot {
    pub fn yes_mid(&self) -> Option<f64> {
        match (self.yes_bid, self.yes_ask) {
            (Some(b), Some(a)) if a > b => Some(0.5 * (a + b)),
            _ => None,
        }
    }
    pub fn no_mid(&self) -> Option<f64> {
        match (self.no_bid, self.no_ask) {
            (Some(b), Some(a)) if a > b => Some(0.5 * (a + b)),
            _ => None,
        }
    }
}

/// Rolling buffer of `(t_secs_since_unix_epoch, btc_mid_usd)` samples.
/// Used to look up the BTC price at a past timestamp (e.g. when a new
/// market opens, snap its strike to BTC mid at the market's start time).
#[derive(Debug, Clone)]
pub struct BtcHistory {
    samples: VecDeque<(f64, f64)>,
    capacity_secs: f64,
}

impl BtcHistory {
    pub fn new(capacity_secs: f64) -> Self {
        Self {
            samples: VecDeque::new(),
            capacity_secs,
        }
    }

    pub fn observe(&mut self, t_secs: f64, mid_usd: f64) {
        if !(t_secs.is_finite() && mid_usd.is_finite()) || mid_usd <= 0.0 {
            return;
        }
        self.samples.push_back((t_secs, mid_usd));
        let cutoff = t_secs - self.capacity_secs;
        while let Some(&(t, _)) = self.samples.front() {
            if t < cutoff {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn latest(&self) -> Option<(f64, f64)> {
        self.samples.back().copied()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Nearest BTC mid at `target_t`. Returns `None` if `target_t` is
    /// outside the retained buffer.
    pub fn at_time(&self, target_t: f64) -> Option<f64> {
        if self.samples.is_empty() {
            return None;
        }
        let (t_oldest, _) = *self.samples.front().unwrap();
        let (t_newest, _) = *self.samples.back().unwrap();
        if target_t < t_oldest || target_t > t_newest {
            return None;
        }
        self.samples
            .iter()
            .min_by(|a, b| {
                (a.0 - target_t)
                    .abs()
                    .partial_cmp(&(b.0 - target_t).abs())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|&(_, mid)| mid)
    }
}

/// State for the single market we're currently trading. The bot loop
/// owns one of these at a time; a new one is built on `MarketChanged`.
#[derive(Debug, Clone)]
pub struct ActiveMarket {
    pub market: Market,
    /// BTC mid at the moment the market opened. `None` if the market
    /// started before our BTC history buffer's oldest sample — in that
    /// case we refuse to trade this market (can't compute fair value
    /// without a strike).
    pub strike: Option<f64>,
    /// Most recent Polymarket top-of-book snapshot (YES + NO) for this
    /// market. Drives the strategy and powers the decision-log
    /// diagnostics (implied-strike, gap to Binance-snapped strike).
    pub last_poly_snapshot: Option<PolyBookSnapshot>,
}

impl ActiveMarket {
    /// Duration of one BTC up/down 5-minute market. Polymarket's gamma
    /// `start_date` is unreliable for these markets — sometimes the
    /// series-creation timestamp — so we derive the actual market start
    /// as `end_time_epoch - MARKET_DURATION_SECS`.
    pub const MARKET_DURATION_SECS: i64 = 300;

    pub fn new(market: Market, btc_history: &BtcHistory) -> Self {
        let effective_start_epoch = market.end_time_epoch - Self::MARKET_DURATION_SECS;
        let strike = btc_history.at_time(effective_start_epoch as f64);
        Self {
            market,
            strike,
            last_poly_snapshot: None,
        }
    }

    /// The effective trading-window start (end - 5 min for BTC 5m markets).
    pub fn effective_start_epoch(&self) -> i64 {
        self.market.end_time_epoch - Self::MARKET_DURATION_SECS
    }

    /// Seconds until market resolution given the wall clock. Returns 0
    /// if already past `end_time_epoch`.
    pub fn ttr_secs(&self, now_secs: f64) -> f64 {
        (self.market.end_time_epoch as f64 - now_secs).max(0.0)
    }

    pub fn is_tradeable(&self) -> bool {
        self.strike.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_registry::{MarketId, TokenId};

    fn market(start: Option<i64>, end: i64) -> Market {
        Market {
            id: MarketId::new("M"),
            title: "T".into(),
            slug: "s".into(),
            yes_token: TokenId::new("Y"),
            no_token: TokenId::new("N"),
            start_time_epoch: start,
            end_time_epoch: end,
            resolved_outcome: None,
        }
    }

    #[test]
    fn history_prunes_old_samples() {
        let mut h = BtcHistory::new(10.0);
        for i in 0..20 {
            h.observe(i as f64, 100_000.0 + i as f64);
        }
        assert!(h.len() <= 11);
    }

    #[test]
    fn history_returns_none_outside_window() {
        let mut h = BtcHistory::new(10.0);
        h.observe(100.0, 100_000.0);
        h.observe(110.0, 100_001.0);
        assert!(h.at_time(50.0).is_none()); // before oldest
        assert!(h.at_time(200.0).is_none()); // after newest
    }

    #[test]
    fn history_returns_nearest_sample() {
        let mut h = BtcHistory::new(100.0);
        h.observe(100.0, 50_000.0);
        h.observe(105.0, 55_000.0);
        h.observe(110.0, 60_000.0);
        assert_eq!(h.at_time(100.0), Some(50_000.0));
        assert_eq!(h.at_time(101.0), Some(50_000.0)); // closer to 100 than 105
        assert_eq!(h.at_time(108.0), Some(60_000.0)); // closer to 110 than 105
    }

    #[test]
    fn active_market_with_strike_in_buffer_is_tradeable() {
        let mut h = BtcHistory::new(600.0);
        // Effective start is end - 300 = 1000. Observe BTC there.
        h.observe(1000.0, 100_000.0);
        h.observe(1100.0, 100_500.0);
        let m = ActiveMarket::new(market(None, 1300), &h);
        assert_eq!(m.effective_start_epoch(), 1000);
        assert_eq!(m.strike, Some(100_000.0));
        assert!(m.is_tradeable());
    }

    #[test]
    fn active_market_with_strike_before_buffer_is_not_tradeable() {
        let mut h = BtcHistory::new(60.0);
        // Buffer only covers t=940..1000; market window is 800..1100,
        // so effective_start=800 is outside the buffer.
        h.observe(1000.0, 100_000.0);
        let m = ActiveMarket::new(market(None, 1100), &h);
        assert_eq!(m.strike, None);
        assert!(!m.is_tradeable());
    }

    #[test]
    fn active_market_ignores_polymarket_garbage_start_date() {
        // gamma sometimes gives a start_date 24h before the actual 5m
        // trading window. Strike should be derived from end - 300, not
        // from the bogus start_date.
        let mut h = BtcHistory::new(600.0);
        h.observe(1000.0, 100_000.0);
        let m = ActiveMarket::new(market(Some(1000 - 86_400), 1300), &h);
        assert_eq!(m.effective_start_epoch(), 1000);
        assert_eq!(m.strike, Some(100_000.0));
    }

    #[test]
    fn ttr_clamps_at_zero() {
        let h = BtcHistory::new(60.0);
        let m = ActiveMarket::new(market(None, 100), &h);
        assert_eq!(m.ttr_secs(50.0), 50.0);
        assert_eq!(m.ttr_secs(100.0), 0.0);
        assert_eq!(m.ttr_secs(200.0), 0.0);
    }

    #[test]
    fn poly_snapshot_mid_requires_both_sides() {
        let mut s = PolyBookSnapshot::default();
        assert!(s.yes_mid().is_none());
        s.yes_bid = Some(0.34);
        assert!(s.yes_mid().is_none()); // missing ask
        s.yes_ask = Some(0.36);
        assert!((s.yes_mid().unwrap() - 0.35).abs() < 1e-9);
    }

    #[test]
    fn poly_snapshot_rejects_crossed_or_zero_spread() {
        let mut s = PolyBookSnapshot::default();
        s.yes_bid = Some(0.40);
        s.yes_ask = Some(0.40); // zero spread → not a useful mid
        assert!(s.yes_mid().is_none());
        s.yes_ask = Some(0.35); // crossed
        assert!(s.yes_mid().is_none());
    }
}
