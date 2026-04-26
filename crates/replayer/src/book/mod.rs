//! Order-book reconstruction from decoded venue events.
//!
//! Two builders:
//!
//! * [`BinanceBook`] — full L2 depth book from `<symbol>@depth_snapshot`
//!   plus `<symbol>@depth@100ms` diffs, with U/u sequence-number
//!   splicing per the Binance spec.
//! * [`PolymarketMarketBook`] — Yes/No book for one Polymarket market,
//!   composed of two `PolymarketBook` snapshots (one per outcome) plus
//!   `price_change` diffs.
//!
//! No shared `Book` trait — the two builders agree on accessors but
//! diverge on `apply`: Binance has sequence-number gap detection,
//! Polymarket has Yes/No asset routing and pre-snapshot buffering.
//! Forcing them under one trait would require GATs or owned-event
//! enums, neither of which buys research code anything practical.

pub mod binance;
pub mod polymarket;

pub use binance::BinanceBook;
pub use polymarket::{PolymarketMarketBook, PolymarketSideBook};

/// What [`BinanceBook::apply`] / [`PolymarketMarketBook::apply`] did
/// with the event we just handed them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// State updated.
    Applied,
    /// Event was skipped — usually a stale diff (id ≤ snapshot baseline)
    /// or an event for a different market/symbol than this book tracks.
    Skipped,
    /// Sequence break: expected the next id to be `expected` but got
    /// `got`. Caller decides whether to invalidate the book and re-fetch
    /// a snapshot. Book state after a Gap is **stale** until the next
    /// snapshot arrives.
    Gap { expected: u64, got: u64 },
}
