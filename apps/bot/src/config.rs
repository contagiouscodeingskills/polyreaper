//! Bot configuration. TOML-loaded; pinned per session into the run log.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BotConfig {
    pub mode: Mode,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
    pub feeds: FeedsConfig,
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
    /// Single-stream WS endpoint. `/ws/<stream>` doesn't need a SUBSCRIBE.
    pub ws_url: String,
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
    /// Minimum |FV - poly_mid| to fire a signal at all. Must cover fees
    /// + spread + edge_threshold. Probability units (0..1).
    pub min_edge: f64,
    /// Edge magnitude that yields max-size sizing. Linear ramp from
    /// `min_edge` (size ~ 0) to `edge_scale` (size = max). Probability units.
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RiskConfig {
    /// Max USD size per single trade.
    pub max_per_trade_usd: f64,
    /// Max cumulative USD cost basis on a single market. Caps total
    /// exposure even when many small fills would otherwise stack up.
    /// Clips the order size when partial headroom remains.
    pub max_notional_per_market_usd: f64,
    /// If realised + unrealised loss on a single market reaches this, the
    /// kill switch trips for that market — no new orders, flatten if
    /// possible.
    pub max_loss_per_market_usd: f64,
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
            min_edge: 0.02,
            edge_scale: 0.08,
            min_ttr_secs: 15.0,
            vol_window_secs: 60.0,
            fallback_sigma_per_sec: 5.0e-5,
        }
    }
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_per_trade_usd: 1.0,
            max_notional_per_market_usd: 5.0,
            max_loss_per_market_usd: 5.0,
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
        assert_eq!(parsed.strategy.min_edge, original.strategy.min_edge);
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
