//! Executor: turns an approved Signal into a (paper for now) fill.
//!
//! Paper-mode fill model is intentionally simple in v0: we assume we get
//! filled at the observed Polymarket mid for the signal's side, with no
//! latency, no slippage, no partial fills. This is optimistic — when we
//! move to live, real fills will be at the best ask (worse than mid by
//! half the spread) and may be partial.

use serde::{Deserialize, Serialize};

use crate::position::PositionStore;
use crate::strategy::Signal;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperFill {
    pub signal: Signal,
    pub fill_price: f64,
    pub fill_size_usd: f64,
    pub fill_shares: f64,
}

#[derive(Debug, Default)]
pub struct PaperExecutor {
    pub fills: Vec<PaperFill>,
}

impl PaperExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Submit an approved signal. Updates `positions` with the resulting
    /// fill and records the fill internally. Returns the fill.
    pub fn submit(&mut self, signal: Signal, positions: &mut PositionStore) -> PaperFill {
        let fill_price = signal.price;
        let fill_size_usd = signal.size_usd;
        let fill_shares = if fill_price > 0.0 {
            fill_size_usd / fill_price
        } else {
            0.0
        };
        positions.apply_fill(&signal.market_id, signal.side, fill_size_usd, fill_price);
        let fill = PaperFill {
            signal,
            fill_price,
            fill_size_usd,
            fill_shares,
        };
        self.fills.push(fill.clone());
        fill
    }

    pub fn fill_count(&self) -> usize {
        self.fills.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_registry::{MarketId, Outcome};

    fn sig() -> Signal {
        Signal {
            market_id: MarketId::new("M"),
            side: Outcome::Yes,
            size_usd: 1.0,
            price: 0.50,
            fv_for_side: 0.60,
            mid_for_side: 0.50,
            edge: 0.10,
            ttr_secs: 120.0,
        }
    }

    #[test]
    fn submit_records_a_fill_and_opens_position() {
        let mut exec = PaperExecutor::new();
        let mut store = PositionStore::new();
        let fill = exec.submit(sig(), &mut store);
        assert_eq!(exec.fill_count(), 1);
        assert!((fill.fill_shares - 2.0).abs() < 1e-9);
        let pos = store.get(&MarketId::new("M")).unwrap();
        assert!((pos.shares - 2.0).abs() < 1e-9);
    }
}
