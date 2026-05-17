//! Live feed clients consumed by the bot.
//!
//! - [`binance`] — bookTicker WS, emits BTC mid prices.
//! - [`polymarket`] — Gamma market discovery + CLOB book REST poller for
//!   the active BTC up/down 5m market.
//!
//! Each module spawns a long-running tokio task with its own reconnect /
//! retry loop. They communicate with the bot via mpsc channels of
//! [`crate::bot::BotEvent`].

pub mod binance;
pub mod polymarket;
