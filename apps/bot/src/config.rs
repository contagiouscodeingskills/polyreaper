//! Bot configuration. TOML-loaded; pinned per session into the run log.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BotConfig {
    pub mode: Mode,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
    pub feeds: FeedsConfig,
    pub scoring: crate::signals::scoring::ScoringConfig,
    pub metrics: MetricsConfig,
}

/// Prometheus-style metrics endpoint. Empty `listen_addr` disables the
/// server entirely. Default exposes on localhost:9898 — Prometheus
/// scraper can be pointed at it.
///
/// ## Security
///
/// The endpoint is unauthenticated. The default bind (`127.0.0.1:9898`)
/// is loopback-only — safe for a Prometheus scraper running on the same
/// host. **Do not** bind to `0.0.0.0` or a public IP without putting an
/// auth proxy in front: bankroll, P&L, decision counts, market IDs, and
/// strike values are all exposed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    pub listen_addr: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:9898".to_string(),
        }
    }
}

/// Endpoints + polling cadences for the read-side feeds the bot consumes.
/// These are public knowledge — no secrets here. Live-mode order placement
/// will need a separate auth/signing section, added later.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FeedsConfig {
    pub binance: BinanceFeedSettings,
    pub polymarket: PolymarketFeedSettings,
    pub gamma: GammaSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BinanceFeedSettings {
    /// Single-stream WS endpoint for bookTicker (BTC mid).
    pub ws_url: String,
    /// Single-stream WS endpoint for trades (volume + flow).
    pub trade_ws_url: String,
    /// Reconnect if no inbound text frame for this many seconds.
    pub read_idle_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PolymarketFeedSettings {
    /// CLOB REST base, e.g. `https://clob.polymarket.com`.
    pub clob_url: String,
    /// How often we re-poll `/book?token_id=...` for the active market.
    pub book_poll_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GammaSettings {
    /// Gamma `/events` endpoint.
    pub url: String,
    /// Cadence for rediscovering the active market.
    pub poll_interval_secs: u64,
    /// Polymarket series slug — `"btc-up-or-down-5m"` for the BTC 5m series.
    pub series_slug: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Paper,
    Live,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    /// Linear ramp scale for size: a trade with edge equal to
    /// `(required_edge + edge_scale)` fires at full `max_per_trade_usd`.
    /// Probability units. Larger = harder to reach full size.
    pub edge_scale: f64,
    /// Refuse to trade if time-to-resolution is below this. Avoids the
    /// freeze window and the seconds where settlement is effectively
    /// determined.
    pub min_ttr_secs: f64,
    /// Rolling window for realised-vol estimation, in seconds.
    pub vol_window_secs: f64,
    /// Fallback σ-per-second if the vol window hasn't filled yet. BTC
    /// annualised vol ~50% → daily ~3% → per-second ~3.5e-5. We use a
    /// slightly conservative default.
    pub fallback_sigma_per_sec: f64,

    /// Bankroll-fraction sizing: `size_usd = bankroll × edge × bankroll_pct_per_edge`.
    /// Probability units in, fraction out. Default `0.02` → 0.1% of
    /// bankroll at 5% edge, 0.2% at 10% edge (user's spec May 2026).
    /// `max_per_trade_usd` still applies as a hard ceiling.
    pub bankroll_pct_per_edge: f64,

    /// Polymarket crypto-market taker fee rate. Fee as fraction of
    /// notional is `taker_fee_rate × p × (1 − p)`, where `p` is the
    /// share price. Default 0.072 → peak 1.80% at `p = 0.5`, dropping
    /// to ~0.65% at `p = 0.1` or `p = 0.9` (verified against Polymarket
    /// docs + third-party sources, May 2026).
    pub taker_fee_rate: f64,
    /// Additive safety margin on top of the fee-driven break-even edge.
    /// Covers model uncertainty, slippage on fill, and adverse-selection
    /// risk. Probability units. Default 0.005 (≈ half a cent).
    pub taker_safety_margin: f64,

    /// FV-engine timer cadence — strategy re-evaluates at this rate
    /// independently of feed events. Smaller = more responsive +
    /// more log volume.
    pub fv_tick_ms: u64,

    /// Spread baseline used to normalise the `yes_spread_normalized`
    /// feature: `(observed_spread - baseline) / baseline`. Default 0.01
    /// (1¢, the typical Polymarket tick).
    pub spread_baseline: f64,

    /// Refuse to fire if the latest Binance bookTicker frame is older
    /// than this many seconds. Guards against stale-BTC trades on a
    /// disconnected feed.
    pub max_btc_tick_age_secs: f64,
    /// Refuse to fire if the latest Polymarket book snapshot is older
    /// than this many seconds. Guards against trading off a stale book.
    pub max_poly_book_age_secs: f64,
    /// Minimum plausible σ per second. Estimates below this snap to
    /// the fallback (used to detect "flat-price" regimes where σ→0
    /// would otherwise cause degenerate FV).
    pub min_sigma_per_sec: f64,
    /// Maximum plausible σ per second. Estimates above this trigger
    /// `IncompleteReason::SigmaOutOfRange` — refuse to fire, since
    /// the vol estimator is probably picking up a venue glitch.
    pub max_sigma_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RiskConfig {
    /// Initial USDC bankroll for paper-mode simulation. Live mode
    /// would query the actual proxy-wallet balance instead. Drives
    /// edge-scaled sizing via `StrategyConfig::bankroll_pct_per_edge`.
    pub bankroll_initial_usd: f64,
    /// Max USD size per single trade — hard ceiling on bankroll-fraction sizing.
    pub max_per_trade_usd: f64,
    /// Max cumulative USD cost basis on a single market. Caps total
    /// exposure even when many small fills would otherwise stack up.
    /// Clips the order size when partial headroom remains.
    pub max_notional_per_market_usd: f64,
    /// If realised + unrealised loss on a single market reaches this, the
    /// kill switch trips for that market — no new orders, flatten if
    /// possible.
    pub max_loss_per_market_usd: f64,
    /// Portfolio-level kill switch. If aggregate session loss (realised
    /// + unrealised on the active market) reaches this, the engine
    /// halts ALL new trading across every market. Sticky once tripped
    /// — only an operator-driven `reset_kill_switch()` clears it. Set
    /// to a non-positive value or NaN to disable.
    pub max_session_loss_usd: f64,
    /// Minimum seconds between consecutive fills on the same market.
    /// Defends against firing every tick when Polymarket hasn't yet
    /// repriced; without this a sticky edge could spam orders.
    pub min_secs_between_fires_per_market: f64,
    /// Cap on concurrently-open positions (across markets).
    pub max_concurrent_positions: usize,
}

impl Default for BotConfig {
    fn default() -> Self {
        Self {
            mode: Mode::Paper,
            strategy: StrategyConfig::default(),
            risk: RiskConfig::default(),
            feeds: FeedsConfig::default(),
            scoring: crate::signals::scoring::ScoringConfig::default(),
            metrics: MetricsConfig::default(),
        }
    }
}

impl Default for FeedsConfig {
    fn default() -> Self {
        Self {
            binance: BinanceFeedSettings::default(),
            polymarket: PolymarketFeedSettings::default(),
            gamma: GammaSettings::default(),
        }
    }
}

impl Default for BinanceFeedSettings {
    fn default() -> Self {
        Self {
            ws_url: "wss://stream.binance.com:9443/ws/btcusdt@bookTicker".to_string(),
            trade_ws_url: "wss://stream.binance.com:9443/ws/btcusdt@trade".to_string(),
            read_idle_secs: 30,
        }
    }
}

impl Default for PolymarketFeedSettings {
    fn default() -> Self {
        Self {
            clob_url: "https://clob.polymarket.com".to_string(),
            book_poll_ms: 500,
        }
    }
}

impl Default for GammaSettings {
    fn default() -> Self {
        Self {
            url: "https://gamma-api.polymarket.com/events".to_string(),
            poll_interval_secs: 15,
            series_slug: "btc-up-or-down-5m".to_string(),
        }
    }
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            edge_scale: 0.04,
            min_ttr_secs: 15.0,
            vol_window_secs: 60.0,
            fallback_sigma_per_sec: 5.0e-5,
            bankroll_pct_per_edge: 0.02,
            taker_fee_rate: 0.072,
            taker_safety_margin: 0.005,
            fv_tick_ms: 100,
            spread_baseline: 0.01,
            max_btc_tick_age_secs: 5.0,
            max_poly_book_age_secs: 3.0,
            min_sigma_per_sec: 1.0e-7,
            max_sigma_per_sec: 1.0e-2,
        }
    }
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            bankroll_initial_usd: 1000.0,
            max_per_trade_usd: 5.0,
            max_notional_per_market_usd: 25.0,
            max_loss_per_market_usd: 25.0,
            // Default 3% of starting bankroll. Generous in normal play,
            // tight enough to catch a sustained losing streak.
            max_session_loss_usd: 30.0,
            min_secs_between_fires_per_market: 2.0,
            max_concurrent_positions: 1,
        }
    }
}

impl BotConfig {
    pub fn from_toml_str(s: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_round_trips_through_toml() {
        let original = BotConfig::default();
        let serialised = toml::to_string(&original).unwrap();
        let parsed = BotConfig::from_toml_str(&serialised).unwrap();
        assert_eq!(parsed.mode, original.mode);
        assert_eq!(parsed.strategy.edge_scale, original.strategy.edge_scale);
        assert_eq!(
            parsed.strategy.taker_fee_rate,
            original.strategy.taker_fee_rate
        );
        assert_eq!(
            parsed.risk.max_per_trade_usd,
            original.risk.max_per_trade_usd
        );
    }

    #[test]
    fn mode_serialises_lowercase() {
        let m = Mode::Paper;
        let s = serde_json::to_string(&m).unwrap();
        assert_eq!(s, r#""paper""#);
    }
}
