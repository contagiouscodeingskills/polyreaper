//! Position tracking. One open Position per market max in v0.
//!
//! Realised P&L is recorded when a position is closed (either explicitly
//! by us or implicitly on market resolution). Unrealised P&L is the
//! mark-to-market against the latest observed mid.

use std::collections::HashMap;

use market_registry::{MarketId, Outcome};
use serde::{Deserialize, Serialize};

/// One open position on a single market. We hold a single side at a time
/// — averaging is allowed if the strategy fires again same-side.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub market_id: MarketId,
    /// Side we're long.
    pub side: Outcome,
    /// Total USD spent acquiring shares (cost basis).
    pub cost_usd: f64,
    /// Number of shares held. Each share pays $1 if `side` resolves.
    pub shares: f64,
    /// Volume-weighted average entry price in probability units.
    pub avg_price: f64,
}

impl Position {
    pub fn average_in(&mut self, fill_usd: f64, fill_shares: f64, fill_price: f64) {
        self.cost_usd += fill_usd;
        self.shares += fill_shares;
        // Recompute VWAP.
        if self.shares > 0.0 {
            self.avg_price = self.cost_usd / self.shares;
        } else {
            self.avg_price = fill_price;
        }
    }

    /// Mark-to-market unrealised P&L given current side-price (price of the
    /// outcome we're long, in probability units).
    pub fn unrealised_pnl_usd(&self, current_side_mid: f64) -> f64 {
        let current_value = self.shares * current_side_mid;
        current_value - self.cost_usd
    }

    /// Realise the full position at `exit_price` (probability units).
    /// Returns realised P&L.
    pub fn close_at(&mut self, exit_price: f64) -> f64 {
        let proceeds = self.shares * exit_price;
        let pnl = proceeds - self.cost_usd;
        self.cost_usd = 0.0;
        self.shares = 0.0;
        pnl
    }

    /// Realise at resolution: $1 per share if we were on the winning side,
    /// $0 otherwise.
    pub fn settle_at_resolution(&mut self, winner: Outcome) -> f64 {
        let exit_price = if winner == self.side { 1.0 } else { 0.0 };
        self.close_at(exit_price)
    }
}

#[derive(Debug, Clone, Default)]
pub struct PositionStore {
    open: HashMap<MarketId, Position>,
    /// Cumulative realised P&L per market over the bot's run.
    realised_per_market: HashMap<MarketId, f64>,
}

impl PositionStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open_count(&self) -> usize {
        self.open.len()
    }

    /// Snapshot all open positions for persistence / audit.
    pub fn open_positions(&self) -> Vec<Position> {
        self.open.values().cloned().collect()
    }

    /// Borrow the per-market realised P&L map (for persistence / audit).
    pub fn realised_pnl_map(&self) -> &HashMap<MarketId, f64> {
        &self.realised_per_market
    }

    pub fn get(&self, id: &MarketId) -> Option<&Position> {
        self.open.get(id)
    }

    pub fn realised(&self, id: &MarketId) -> f64 {
        self.realised_per_market.get(id).copied().unwrap_or(0.0)
    }

    pub fn total_realised(&self) -> f64 {
        self.realised_per_market.values().sum()
    }

    /// Add a fill. If a position exists on the same side, average in;
    /// if opposite side, partial-close it first then any remainder opens.
    /// In v0 we only ever buy one side per market (strategy doesn't flip
    /// inside a single market) so the same-side branch is the common path.
    pub fn apply_fill(
        &mut self,
        market_id: &MarketId,
        side: Outcome,
        fill_usd: f64,
        fill_price: f64,
    ) {
        if fill_usd <= 0.0 || !fill_price.is_finite() || fill_price <= 0.0 || fill_price > 1.0 {
            return;
        }
        let shares = fill_usd / fill_price;
        match self.open.get_mut(market_id) {
            Some(pos) if pos.side == side => {
                pos.average_in(fill_usd, shares, fill_price);
            }
            Some(pos) => {
                // Opposite side — close existing (at the current opposite-side
                // price = 1 - fill_price) and open a fresh one for the remainder.
                // This branch should not fire in v0; included for safety.
                let close_price = 1.0 - fill_price;
                let realised = pos.close_at(close_price);
                *self
                    .realised_per_market
                    .entry(market_id.clone())
                    .or_default() += realised;
                self.open.remove(market_id);
                self.open.insert(
                    market_id.clone(),
                    Position {
                        market_id: market_id.clone(),
                        side,
                        cost_usd: fill_usd,
                        shares,
                        avg_price: fill_price,
                    },
                );
            }
            None => {
                self.open.insert(
                    market_id.clone(),
                    Position {
                        market_id: market_id.clone(),
                        side,
                        cost_usd: fill_usd,
                        shares,
                        avg_price: fill_price,
                    },
                );
            }
        }
    }

    /// Close a market at a known exit price (paper sim) or settle on
    /// resolution. Returns the realised P&L from this close.
    pub fn close_market(&mut self, market_id: &MarketId, exit_price: f64) -> Option<f64> {
        let mut pos = self.open.remove(market_id)?;
        let pnl = pos.close_at(exit_price);
        *self
            .realised_per_market
            .entry(market_id.clone())
            .or_default() += pnl;
        Some(pnl)
    }

    pub fn settle_resolution(&mut self, market_id: &MarketId, winner: Outcome) -> Option<f64> {
        let mut pos = self.open.remove(market_id)?;
        let pnl = pos.settle_at_resolution(winner);
        *self
            .realised_per_market
            .entry(market_id.clone())
            .or_default() += pnl;
        Some(pnl)
    }

    /// Unrealised across all open positions, given a function that maps a
    /// market → current price of the side that market's position holds.
    pub fn total_unrealised(
        &self,
        mut price_lookup: impl FnMut(&MarketId, Outcome) -> Option<f64>,
    ) -> f64 {
        let mut total = 0.0;
        for (id, pos) in &self.open {
            if let Some(mid) = price_lookup(id, pos.side) {
                total += pos.unrealised_pnl_usd(mid);
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_position_records_shares_and_cost() {
        let mut store = PositionStore::new();
        let m = MarketId::new("M");
        store.apply_fill(&m, Outcome::Yes, 1.0, 0.50);
        let pos = store.get(&m).unwrap();
        assert!((pos.shares - 2.0).abs() < 1e-9);
        assert!((pos.cost_usd - 1.0).abs() < 1e-9);
        assert!((pos.avg_price - 0.50).abs() < 1e-9);
    }

    #[test]
    fn averaging_in_same_side_recomputes_vwap() {
        let mut store = PositionStore::new();
        let m = MarketId::new("M");
        store.apply_fill(&m, Outcome::Yes, 1.0, 0.50); // 2 shares
        store.apply_fill(&m, Outcome::Yes, 0.6, 0.30); // 2 shares
        let pos = store.get(&m).unwrap();
        assert!((pos.shares - 4.0).abs() < 1e-9);
        assert!((pos.cost_usd - 1.6).abs() < 1e-9);
        // VWAP = 1.6 / 4 = 0.40
        assert!((pos.avg_price - 0.40).abs() < 1e-9);
    }

    #[test]
    fn settle_winner_pays_one_per_share() {
        let mut store = PositionStore::new();
        let m = MarketId::new("M");
        store.apply_fill(&m, Outcome::Yes, 1.0, 0.50);
        let pnl = store.settle_resolution(&m, Outcome::Yes).unwrap();
        // 2 shares × $1 - $1 cost = +$1
        assert!((pnl - 1.0).abs() < 1e-9);
        assert!(store.get(&m).is_none());
        assert!((store.realised(&m) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn settle_loser_pays_zero_per_share() {
        let mut store = PositionStore::new();
        let m = MarketId::new("M");
        store.apply_fill(&m, Outcome::Yes, 1.0, 0.50);
        let pnl = store.settle_resolution(&m, Outcome::No).unwrap();
        // 0 - $1 = -$1
        assert!((pnl - -1.0).abs() < 1e-9);
        assert!((store.realised(&m) - -1.0).abs() < 1e-9);
    }

    #[test]
    fn unrealised_marks_to_current_mid() {
        let mut store = PositionStore::new();
        let m = MarketId::new("M");
        store.apply_fill(&m, Outcome::Yes, 1.0, 0.50);
        let pos = store.get(&m).unwrap();
        assert!((pos.unrealised_pnl_usd(0.60) - 0.20).abs() < 1e-9); // 2sh × 0.60 - 1.0
        assert!((pos.unrealised_pnl_usd(0.40) - -0.20).abs() < 1e-9);
    }
}
