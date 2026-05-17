//! Strategy signal sources.
//!
//! Currently houses [`scoring`] — a hand-coded multi-factor "true
//! P(YES)" estimator with per-regime weights. Future signals (e.g. a
//! pure lag detector, a microstructure-only signal) would each be their
//! own submodule here; the bot composes their outputs in `strategy.rs`.

pub mod scoring;
