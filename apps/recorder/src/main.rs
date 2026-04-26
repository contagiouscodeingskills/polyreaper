//! Recorder binary entry point.
//!
//! Loads config, installs telemetry, opens the storage session, instantiates
//! the market registry, and spawns the Binance feed. Runs until SIGINT
//! (Ctrl-C), then aborts the feed task and flushes storage with a bounded
//! grace window.

mod health;
mod latency;
mod sweep;

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Wait for the first "please stop" signal from the OS.
///
/// Unix: either `SIGTERM` (systemd `stop`, `kill`) or `SIGINT` (Ctrl-C).
/// Windows: Ctrl-C only (tokio's Windows signal API doesn't expose SIGTERM;
/// Windows service stop uses a different mechanism we don't run under).
///
/// Returns the name of the signal so the caller can log which one fired.
#[cfg(unix)]
async fn wait_for_shutdown() -> Result<&'static str, std::io::Error> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    Ok(tokio::select! {
        _ = term.recv() => "SIGTERM",
        _ = int.recv() => "SIGINT",
    })
}

#[cfg(windows)]
async fn wait_for_shutdown() -> Result<&'static str, std::io::Error> {
    tokio::signal::ctrl_c().await?;
    Ok("ctrl_c")
}

#[tokio::main]
async fn main() -> ExitCode {
    println!(
        "polybot recorder v{} starting",
        env!("CARGO_PKG_VERSION")
    );

    // 1. Config first — nothing else can run against a broken config.
    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("configs/recorder.toml"));

    let cfg = match config::Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("config load failed ({}): {e}", config_path.display());
            return ExitCode::from(2);
        }
    };

    // 2. Telemetry — everything below uses tracing.
    let _guard = match telemetry::init(&cfg.telemetry) {
        Ok(g) => g,
        Err(e) => {
            eprintln!("telemetry init failed: {e}");
            return ExitCode::from(3);
        }
    };

    tracing::info!(
        component = "recorder",
        event = "startup",
        version = env!("CARGO_PKG_VERSION"),
        config_path = %config_path.display(),
        environment = %cfg.app.environment,
        "recorder starting"
    );

    // 3. Storage — owned by the recorder, shared with feeds via Arc<Mutex>.
    let store = match storage::Store::open(&cfg.storage) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(
                component = "recorder",
                event = "storage_open_failed",
                error = %e,
                "aborting"
            );
            return ExitCode::from(4);
        }
    };
    tracing::info!(
        component = "recorder",
        event = "storage_ready",
        session_dir = %store.session_dir().display(),
        "storage session opened"
    );
    let store = Arc::new(Mutex::new(store));

    // 4. Market registry + live gamma discovery.
    let registry = Arc::new(Mutex::new(market_registry::Registry::new()));
    let discoverer = match market_registry::GammaAdapter::new(&cfg.market_discovery) {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(
                component = "recorder",
                event = "discovery_init_failed",
                error = %e,
                "aborting"
            );
            return ExitCode::from(5);
        }
    };

    // Initial (blocking) discovery so the registry is populated before any
    // downstream consumer (polymarket_feed later) looks at it. A failure
    // here is not fatal — we log and continue with an empty registry; the
    // background loop will keep trying.
    match market_registry::MarketDiscoverer::discover(&discoverer).await {
        Ok(markets) => {
            let (added, total) = {
                let mut r = registry.lock().unwrap_or_else(|p| p.into_inner());
                let stats = r.upsert_all(markets);
                (stats.added, r.len())
            };
            tracing::info!(
                component = "recorder",
                event = "registry_populated",
                added = added,
                total = total,
                "initial gamma discovery complete"
            );
        }
        Err(e) => tracing::warn!(
            component = "recorder",
            event = "initial_discovery_failed",
            reason = %e,
            "starting with empty registry; background loop will retry"
        ),
    }

    // Background discovery loop — re-polls gamma every poll_interval_secs.
    let discovery_interval = Duration::from_secs(cfg.market_discovery.poll_interval_secs);
    let discovery_registry = Arc::clone(&registry);
    let discovery_handle = tokio::spawn(async move {
        market_registry::run_discovery_loop(discoverer, discovery_registry, discovery_interval)
            .await
    });
    tracing::info!(
        component = "recorder",
        event = "discovery_loop_spawned",
        interval_secs = cfg.market_discovery.poll_interval_secs,
        "gamma discovery loop spawned"
    );

    // Resolution sweeper — captures Up/Down outcomes for closed markets
    // by polling /events?closed=true. Independent from active discovery
    // because the cadence + filters differ (~60s sweep, closed-only).
    let resolution_adapter = match market_registry::GammaAdapter::new(&cfg.market_discovery) {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(
                component = "recorder",
                event = "resolution_init_failed",
                error = %e,
                "aborting"
            );
            return ExitCode::from(6);
        }
    };
    let resolution_store = Arc::clone(&store);
    let resolution_handle = tokio::spawn(async move {
        sweep::run_resolution_sweep_loop(
            resolution_adapter,
            resolution_store,
            Duration::from_secs(60),
        )
        .await
    });
    tracing::info!(
        component = "recorder",
        event = "resolution_sweep_spawned",
        interval_secs = 60,
        "resolution sweep spawned"
    );

    // Latency probe — periodic TCP connect time to each venue endpoint.
    // Logs to journald only; useful for cross-venue clock alignment and
    // detecting routing changes.
    let latency_handle = tokio::spawn(async {
        latency::run_latency_probe_loop(Duration::from_secs(300)).await
    });
    tracing::info!(
        component = "recorder",
        event = "latency_probe_spawned",
        interval_secs = 300,
        "latency probe spawned"
    );

    // 5. Feeds — one tokio task per venue.
    //
    // Each FeedStats is `Clone` (Arc-backed counters). We clone once
    // for the feed task and keep the original to hand to the health
    // writer below — both views share the same atomic counters.
    let binance_stats = binance_feed::FeedStats::new();
    let binance_cfg = cfg.binance_feed.clone();
    let binance_store = Arc::clone(&store);
    let binance_stats_for_feed = binance_stats.clone();
    let binance_handle = tokio::spawn(async move {
        binance_feed::run(&binance_cfg, binance_store, binance_stats_for_feed).await
    });
    tracing::info!(
        component = "recorder",
        event = "feed_spawned",
        venue = "binance",
        "binance feed task spawned"
    );

    let polymarket_stats = polymarket_feed::FeedStats::new();
    let polymarket_cfg = cfg.polymarket_feed.clone();
    let polymarket_store = Arc::clone(&store);
    let polymarket_registry = Arc::clone(&registry);
    let polymarket_stats_for_feed = polymarket_stats.clone();
    let polymarket_handle = tokio::spawn(async move {
        polymarket_feed::run(
            &polymarket_cfg,
            polymarket_registry,
            polymarket_store,
            polymarket_stats_for_feed,
        )
        .await
    });
    tracing::info!(
        component = "recorder",
        event = "feed_spawned",
        venue = "polymarket",
        "polymarket feed task spawned"
    );

    let coinbase_stats = coinbase_feed::FeedStats::new();
    let coinbase_cfg = cfg.coinbase_feed.clone();
    let coinbase_store = Arc::clone(&store);
    let coinbase_stats_for_feed = coinbase_stats.clone();
    let coinbase_handle = tokio::spawn(async move {
        coinbase_feed::run(&coinbase_cfg, coinbase_store, coinbase_stats_for_feed).await
    });
    tracing::info!(
        component = "recorder",
        event = "feed_spawned",
        venue = "coinbase",
        "coinbase feed task spawned"
    );

    let chainlink_stats = chainlink_feed::FeedStats::new();
    let chainlink_cfg = cfg.chainlink_feed.clone();
    let chainlink_store = Arc::clone(&store);
    let chainlink_stats_for_feed = chainlink_stats.clone();
    let chainlink_handle = tokio::spawn(async move {
        chainlink_feed::run(&chainlink_cfg, chainlink_store, chainlink_stats_for_feed).await
    });
    tracing::info!(
        component = "recorder",
        event = "feed_spawned",
        venue = "chainlink",
        "chainlink feed task spawned"
    );

    // Health snapshot writer — appends to <session>/_health.ndjson every
    // 30 s. Used by research notebooks to filter analysis windows by
    // data quality + clock state. Independent from each feed's own
    // journald `health` log line (which goes to telemetry, not disk).
    let health_inputs = {
        let session_dir = store
            .lock()
            .map(|g| g.session_dir().to_path_buf())
            .unwrap_or_else(|p| p.into_inner().session_dir().to_path_buf());
        health::HealthInputs {
            session_dir,
            binance: binance_stats,
            polymarket: polymarket_stats,
            coinbase: coinbase_stats,
            chainlink: chainlink_stats,
        }
    };
    let health_handle = tokio::spawn(async move {
        health::run_health_writer_loop(health_inputs, Duration::from_secs(30)).await
    });
    tracing::info!(
        component = "recorder",
        event = "health_writer_spawned",
        interval_secs = 30,
        "health snapshot writer spawned"
    );

    // 6. Wait for a shutdown signal (SIGTERM/SIGINT on unix, Ctrl-C on win).
    match wait_for_shutdown().await {
        Ok(sig) => tracing::info!(
            component = "recorder",
            event = "shutdown_signal",
            signal = sig,
            "shutdown signal received"
        ),
        Err(e) => tracing::warn!(
            component = "recorder",
            event = "signal_install_failed",
            error = %e,
            "signal handler failed to install; shutting down anyway"
        ),
    }

    // 7. Abort background tasks. Phase 1: abrupt abort, not graceful cancel
    //    (see docs/TECH_DEBT.md §4).
    binance_handle.abort();
    polymarket_handle.abort();
    coinbase_handle.abort();
    chainlink_handle.abort();
    discovery_handle.abort();
    resolution_handle.abort();
    latency_handle.abort();
    health_handle.abort();
    let _ = binance_handle.await;
    let _ = polymarket_handle.await;
    let _ = coinbase_handle.await;
    let _ = chainlink_handle.await;
    let _ = discovery_handle.await;
    let _ = resolution_handle.await;
    let _ = latency_handle.await;
    let _ = health_handle.await;

    // 8. Best-effort flush with a bounded grace window.
    let grace = Duration::from_secs(cfg.app.shutdown_grace_secs.max(1));
    let flush_store = Arc::clone(&store);
    let flush_result = tokio::time::timeout(grace, async move {
        if let Ok(mut s) = flush_store.lock() {
            s.flush_all()
        } else {
            Ok(())
        }
    })
    .await;
    match flush_result {
        Ok(Ok(())) => tracing::info!(
            component = "recorder",
            event = "flush_ok",
            "final flush complete"
        ),
        Ok(Err(e)) => tracing::error!(
            component = "recorder",
            event = "flush_err",
            error = %e,
            "final flush reported an error"
        ),
        Err(_) => tracing::warn!(
            component = "recorder",
            event = "flush_timeout",
            grace_secs = cfg.app.shutdown_grace_secs,
            "flush did not finish within grace window"
        ),
    }

    tracing::info!(component = "recorder", event = "shutdown", "bye");
    ExitCode::SUCCESS
}
