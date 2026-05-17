//! `bot` binary — live paper-mode entry.
//!
//! Connects to Binance bookTicker WS for BTC mid, polls Polymarket Gamma
//! for the active BTC up/down 5m market, polls the CLOB book for that
//! market's YES side, and runs the strategy / risk / paper-exec loop.
//!
//! Use `bot --bin demo` (i.e. the `demo` binary at `src/bin/demo.rs`)
//! for the offline synthetic-feed scenario.

use std::path::PathBuf;

use tracing::{info, warn};

use bot::bot::run_paper;
use bot::config::BotConfig;

#[tokio::main]
async fn main() {
    init_tracing();

    let cfg = load_config();

    if cfg.mode != bot::config::Mode::Paper {
        warn!(
            "live mode is not yet wired (no wallet/signing). Forcing paper mode for this run."
        );
    }

    info!(
        binance_url = %cfg.feeds.binance.ws_url,
        gamma_url = %cfg.feeds.gamma.url,
        clob_url = %cfg.feeds.polymarket.clob_url,
        series = %cfg.feeds.gamma.series_slug,
        "configured endpoints"
    );

    run_paper(cfg).await;
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();
}

/// Load config from `configs/bot.toml` if present, otherwise fall back to
/// the in-crate defaults. Bot config is small enough that we don't need a
/// CLI flag for the path in v0; if the user wants a non-default path
/// they set `BOT_CONFIG_PATH`.
fn load_config() -> BotConfig {
    let path = std::env::var("BOT_CONFIG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("configs/bot.toml"));
    match std::fs::read_to_string(&path) {
        Ok(text) => match BotConfig::from_toml_str(&text) {
            Ok(cfg) => {
                info!(path = %path.display(), "loaded config");
                cfg
            }
            Err(e) => {
                warn!(path = %path.display(), error = %e, "config parse failed; using defaults");
                BotConfig::default()
            }
        },
        Err(e) => {
            warn!(path = %path.display(), error = %e, "config not found; using defaults");
            BotConfig::default()
        }
    }
}
