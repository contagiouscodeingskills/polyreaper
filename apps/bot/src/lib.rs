//! PolyBot trading bot — paper-mode v0.
//!
//! Pure strategy + risk core lives in `fv`, `strategy`, `risk`, `position`,
//! `execution`. The async glue — feed clients (Binance WS, Polymarket
//! CLOB REST + Gamma discovery), market lifecycle tracking, and the
//! orchestration loop — lives in `feeds`, `market_state`, and `bot`.
//!
//! ## Strategy
//!
//! One signal: `edge = fair_value(P_YES) − polymarket_mid`. Fair value
//! comes from a Black-Scholes-like P(BTC_T > strike) with rolling realised
//! σ from Binance bookTicker mids. Sizing scales with `|edge|`, gated by
//! `min_edge` and `min_time_to_resolution`. There is no separate "lag"
//! trigger — Polymarket's 2-4s repricing window naturally drives the edge
//! to zero, so the signal fires inside that window and self-suppresses
//! outside it.
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
pub mod strategy;

// Re-export the demo entry so the `demo` binary can call it without
// duplicating the synthetic-feed scenario.
pub use bot::run_synthetic_demo;
