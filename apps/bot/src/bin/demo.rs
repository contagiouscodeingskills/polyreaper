//! `demo` binary — offline synthetic-feed smoke test.
//!
//! Runs the strategy / risk / paper-exec pipeline against a deterministic
//! synthetic market (BTC drifts up, Polymarket mid lags). Useful for
//! verifying the core works without live feed dependencies.

use bot::config::BotConfig;
use bot::run_synthetic_demo;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let cfg = BotConfig::default();
    tracing::info!(?cfg, "starting bot — synthetic demo");
    run_synthetic_demo(cfg).await;
}
