//! PolyBot trading bot — paper-mode v0.
//!
//! Pure strategy + risk core lives in `fv`, `strategy`, `risk`, `position`,
//! `execution`. The async glue — feed clients (Binance WS, Polymarket
//! CLOB REST + Gamma discovery), market lifecycle tracking, and the
//! orchestration loop — lives in `feeds`, `market_state`, and `bot`.
//!
//! ## Strategy
//!
//! Fair value comes from a hand-coded multi-factor scoring model
//! (`signals::scoring`) with per-regime weights. Default weights recover
//! GBM-around-strike (Φ of the Z-score) as a special case; non-zero
//! weights on book imbalance, drift, spread, etc. add corrections.
//!
//! The strategy fires as a taker only when `(scoring.p_yes - poly_yes_ask)`
//! (or the NO equivalent) exceeds a fee-aware gate:
//! `taker_fee_rate × p² × (1 − p) + safety_margin`. At the Polymarket
//! peak (p=0.5, peak fee 1.80%) the gate is ~1.4¢; at the tails it
//! drops to ~0.5–1¢. Most ticks therefore do not fire — by design.
//!
//! ## Modes
//!
//! - **Paper** (default): every "fill" is logged, P&L is tracked from the
//!   observed Polymarket mid at the moment we would have submitted. No
//!   network calls to the CLOB order endpoint.
//! - **Live** (gated, not in v0): submits real signed orders to the
//!   Polymarket CLOB. Requires wallet creds. Hard-gated behind a flag.

pub mod bot;
pub mod config;
pub mod decision_log;
pub mod execution;
pub mod feeds;
pub mod fv;
pub mod market_state;
pub mod position;
pub mod risk;
pub mod signals;
pub mod strategy;

// Re-export the demo entry so the `demo` binary can call it without
// duplicating the synthetic-feed scenario.
pub use bot::run_synthetic_demo;
