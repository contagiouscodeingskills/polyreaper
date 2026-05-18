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

use bot::bot::{run_live, run_paper};
use bot::config::BotConfig;
use bot::live::LiveExecError;

#[tokio::main]
async fn main() {
    init_tracing();

    let cfg = load_config();

    info!(
        binance_url = %cfg.feeds.binance.ws_url,
        gamma_url = %cfg.feeds.gamma.url,
        clob_url = %cfg.feeds.polymarket.clob_url,
        series = %cfg.feeds.gamma.series_slug,
        mode = ?cfg.mode,
        "configured endpoints"
    );

    match cfg.mode {
        bot::config::Mode::Paper => {
            run_paper(cfg).await;
        }
        bot::config::Mode::Live => {
            // Live mode requires BOTH the EOA + proxy creds AND the
            // L2 HMAC API creds (POLY_API_KEY/SECRET/PASSPHRASE).
            // Failure is loud — silent paper fallback would be unsafe.
            match run_live(cfg).await {
                Ok(()) => {}
                Err(LiveExecError::CredentialsMissing) => {
                    warn!(
                        "Mode::Live but POLYMARKET_EOA_PRIVATE_KEY / \
                         POLYMARKET_PROXY_WALLET_ADDRESS are not set. \
                         Refusing to start (silent paper fallback would be unsafe)."
                    );
                    std::process::exit(2);
                }
                Err(LiveExecError::ApiCredentialsMissing) => {
                    warn!(
                        "Mode::Live but POLYMARKET_API_KEY / POLYMARKET_API_SECRET / \
                         POLYMARKET_API_PASSPHRASE are not set. Run \
                         `py-clob-client` (or equivalent) once with your EOA key \
                         to mint these, then set them in the environment."
                    );
                    std::process::exit(2);
                }
                Err(e) => {
                    warn!(error = %e, "live executor failed to initialise");
                    std::process::exit(2);
                }
            }
        }
    }
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
