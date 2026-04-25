//! In-memory registry of Polymarket BTC 5-minute up/down markets.
//!
//! This crate owns the domain model (`Market`, `MarketId`, `TokenId`,
//! `Outcome`, `MarketLifecycle`) and the `Registry` that tracks them.
//! External-API uncertainty is isolated behind the [`MarketDiscoverer`]
//! trait — [`StaticDiscoverer`] backs tests/replay, [`GammaAdapter`]
//! (in [`gamma`]) talks to `gamma-api.polymarket.com`.
//!
//! Lifecycle is derived, not stored: given `now`, `start_time_epoch`,
//! `end_time_epoch`, and an operator-chosen `closing_soon_window_secs`,
//! each `Market` answers `lifecycle()` on demand.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const NAME: &str = "market_registry";

pub mod gamma;
pub use gamma::GammaAdapter;

// ---------------------------------------------------------------------------
// Newtypes
// ---------------------------------------------------------------------------

/// Opaque Polymarket market identifier. Expected to hold `condition_id` in
/// practice, but we do not commit to that naming in this crate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketId(String);

impl MarketId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for MarketId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Opaque CLOB token identifier (one per Yes/No side per market).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TokenId(String);

impl TokenId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TokenId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// ---------------------------------------------------------------------------
// Market + lifecycle
// ---------------------------------------------------------------------------

/// Which side of a binary market won at resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Yes,
    No,
}

/// A single BTC 5-minute up/down market.
#[derive(Debug, Clone)]
pub struct Market {
    pub id: MarketId,
    pub title: String,
    pub slug: String,
    pub yes_token: TokenId,
    pub no_token: TokenId,
    /// Market open time (epoch seconds). `None` means "already open / no
    /// upcoming gate" — treat as `Active` immediately.
    pub start_time_epoch: Option<i64>,
    /// Market close time (epoch seconds). Required.
    pub end_time_epoch: i64,
    /// Set once the market has resolved. `None` means outcome still unknown.
    pub resolved_outcome: Option<Outcome>,
}

impl Market {
    /// Return the token id for a given outcome side.
    pub fn token_for(&self, outcome: Outcome) -> &TokenId {
        match outcome {
            Outcome::Yes => &self.yes_token,
            Outcome::No => &self.no_token,
        }
    }

    /// Return `Some(outcome)` if the given token is one of this market's
    /// Yes/No tokens, else `None`.
    pub fn outcome_of(&self, token: &TokenId) -> Option<Outcome> {
        if token == &self.yes_token {
            Some(Outcome::Yes)
        } else if token == &self.no_token {
            Some(Outcome::No)
        } else {
            None
        }
    }

    /// Compute lifecycle from raw timestamps and the caller's "closing soon"
    /// window. Resolved beats everything; otherwise fall through by time.
    pub fn lifecycle(
        &self,
        now_epoch_secs: i64,
        closing_soon_window_secs: u64,
    ) -> MarketLifecycle {
        if self.resolved_outcome.is_some() {
            return MarketLifecycle::Resolved;
        }
        if let Some(start) = self.start_time_epoch {
            if now_epoch_secs < start {
                return MarketLifecycle::Upcoming;
            }
        }
        if now_epoch_secs >= self.end_time_epoch {
            return MarketLifecycle::Closed;
        }
        let closing_threshold = self
            .end_time_epoch
            .saturating_sub(closing_soon_window_secs as i64);
        if now_epoch_secs >= closing_threshold {
            MarketLifecycle::ClosingSoon
        } else {
            MarketLifecycle::Active
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketLifecycle {
    Upcoming,
    Active,
    ClosingSoon,
    Closed,
    Resolved,
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// In-memory set of markets, keyed by [`MarketId`].
///
/// The Registry does **not** perform discovery itself. A caller (the
/// recorder's discovery loop) calls [`upsert_all`](Self::upsert_all) with
/// the output of a [`MarketDiscoverer`], and optionally [`prune`](Self::prune)
/// when it wants to drop markets (e.g. long-resolved).
#[derive(Debug, Default, Clone)]
pub struct Registry {
    by_id: HashMap<MarketId, Market>,
}

/// Counts returned by [`Registry::upsert_all`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UpsertStats {
    pub added: usize,
    pub updated: usize,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    pub fn get(&self, id: &MarketId) -> Option<&Market> {
        self.by_id.get(id)
    }

    pub fn contains(&self, id: &MarketId) -> bool {
        self.by_id.contains_key(id)
    }

    /// Iterate over every tracked market.
    pub fn iter(&self) -> impl Iterator<Item = &Market> {
        self.by_id.values()
    }

    /// Find the market that owns the given token id (Yes-side or No-side).
    /// O(N) scan — fine for the BTC 5-min series (~200 active markets at
    /// peak); revisit if we ever scale to thousands.
    pub fn market_by_token(&self, token: &TokenId) -> Option<&Market> {
        self.by_id
            .values()
            .find(|m| &m.yes_token == token || &m.no_token == token)
    }

    /// Upsert a batch. Returns a count of genuinely new markets vs. updates
    /// to already-known ids. Does **not** remove markets absent from the
    /// batch — use [`prune`](Self::prune) for that.
    pub fn upsert_all<I: IntoIterator<Item = Market>>(&mut self, markets: I) -> UpsertStats {
        let mut stats = UpsertStats::default();
        for m in markets {
            if self.by_id.contains_key(&m.id) {
                stats.updated += 1;
            } else {
                stats.added += 1;
            }
            self.by_id.insert(m.id.clone(), m);
        }
        stats
    }

    /// Drop any market for which `predicate` returns true. Returns the
    /// number of markets removed.
    pub fn prune(&mut self, predicate: impl Fn(&Market) -> bool) -> usize {
        let before = self.by_id.len();
        self.by_id.retain(|_, m| !predicate(m));
        before - self.by_id.len()
    }

    /// Iterator of `(market, lifecycle)` pairs using the caller's clock /
    /// window. Borrow-friendly so the recorder can log/filter without
    /// cloning.
    pub fn iter_with_lifecycle(
        &self,
        now_epoch_secs: i64,
        closing_soon_window_secs: u64,
    ) -> impl Iterator<Item = (&Market, MarketLifecycle)> {
        self.by_id
            .values()
            .map(move |m| (m, m.lifecycle(now_epoch_secs, closing_soon_window_secs)))
    }

    /// Convenience: markets currently in a given lifecycle state.
    pub fn in_lifecycle(
        &self,
        target: MarketLifecycle,
        now_epoch_secs: i64,
        closing_soon_window_secs: u64,
    ) -> impl Iterator<Item = &Market> {
        self.iter_with_lifecycle(now_epoch_secs, closing_soon_window_secs)
            .filter_map(move |(m, lc)| (lc == target).then_some(m))
    }
}

// ---------------------------------------------------------------------------
// Discovery seam
// ---------------------------------------------------------------------------

/// The single seam where "what markets exist" crosses into this crate.
///
/// Implementations:
/// * [`StaticDiscoverer`] — deterministic set, for tests and replay.
/// * [`GammaAdapter`] — live HTTP discovery against gamma-api.polymarket.com.
///
/// The returned future is spelled out as `impl Future + Send` (rather than
/// `async fn`) so that implementations work under `tokio::spawn` on a
/// multi-thread runtime. The runtime crosses thread boundaries, which
/// requires a `Send` future — `async fn` in traits does not imply `Send`.
pub trait MarketDiscoverer {
    fn discover(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<Market>, DiscoveryError>> + Send;
}

/// A `MarketDiscoverer` that hands back a pre-baked list. The list can be
/// updated at runtime via [`set`](Self::set) for test scenarios that need
/// to simulate market churn.
#[derive(Debug, Clone, Default)]
pub struct StaticDiscoverer {
    markets: Vec<Market>,
}

impl StaticDiscoverer {
    pub fn new(markets: Vec<Market>) -> Self {
        Self { markets }
    }
    pub fn set(&mut self, markets: Vec<Market>) {
        self.markets = markets;
    }
}

impl MarketDiscoverer for StaticDiscoverer {
    fn discover(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<Market>, DiscoveryError>> + Send {
        let markets = self.markets.clone();
        async move { Ok(markets) }
    }
}

/// Run a discoverer in a background loop, upserting into `registry` on
/// every successful pass. Errors log and retry at the next interval — the
/// loop never exits on its own.
///
/// Intended for `tokio::spawn`: the caller keeps the `JoinHandle` and
/// aborts it at shutdown.
pub async fn run_discovery_loop<D: MarketDiscoverer>(
    adapter: D,
    registry: Arc<Mutex<Registry>>,
    interval: Duration,
) {
    loop {
        match adapter.discover().await {
            Ok(markets) => {
                // Scope the lock so we don't hold it across the sleep below.
                let (added, updated, total) = {
                    let mut r = registry.lock().unwrap_or_else(|p| p.into_inner());
                    let stats = r.upsert_all(markets);
                    (stats.added, stats.updated, r.len())
                };
                tracing::info!(
                    component = "market_registry",
                    venue = "polymarket",
                    event = "discovery_tick",
                    added = added,
                    updated = updated,
                    total = total,
                    "registry updated"
                );
            }
            Err(e) => {
                tracing::warn!(
                    component = "market_registry",
                    venue = "polymarket",
                    event = "discovery_tick_failed",
                    reason = %e,
                    "discovery failed; will retry after interval"
                );
            }
        }
        tokio::time::sleep(interval).await;
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("transport error: {0}")]
    Transport(String),

    #[error("parse error: {0}")]
    Parse(String),

    #[error("adapter error: {0}")]
    Adapter(String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(start: Option<i64>, end: i64, resolved: Option<Outcome>, id: &str) -> Market {
        Market {
            id: MarketId::new(id),
            title: "Bitcoin Up or Down 5min".into(),
            slug: "btc-5min".into(),
            yes_token: TokenId::new(format!("{id}-YES")),
            no_token: TokenId::new(format!("{id}-NO")),
            start_time_epoch: start,
            end_time_epoch: end,
            resolved_outcome: resolved,
        }
    }

    #[test]
    fn lifecycle_walks_through_expected_states() {
        let m = sample(Some(50), 100, None, "M");
        let w = 10; // closing window starts at end - 10 = 90

        assert_eq!(m.lifecycle(40, w), MarketLifecycle::Upcoming);
        assert_eq!(m.lifecycle(50, w), MarketLifecycle::Active);
        assert_eq!(m.lifecycle(89, w), MarketLifecycle::Active);
        assert_eq!(m.lifecycle(90, w), MarketLifecycle::ClosingSoon);
        assert_eq!(m.lifecycle(99, w), MarketLifecycle::ClosingSoon);
        assert_eq!(m.lifecycle(100, w), MarketLifecycle::Closed);
        assert_eq!(m.lifecycle(200, w), MarketLifecycle::Closed);
    }

    #[test]
    fn resolved_beats_time_based_states() {
        let m = sample(Some(50), 100, Some(Outcome::Yes), "M");
        assert_eq!(m.lifecycle(40, 10), MarketLifecycle::Resolved);
        assert_eq!(m.lifecycle(95, 10), MarketLifecycle::Resolved);
        assert_eq!(m.lifecycle(999, 10), MarketLifecycle::Resolved);
    }

    #[test]
    fn missing_start_time_behaves_as_already_active() {
        let m = sample(None, 100, None, "M");
        assert_eq!(m.lifecycle(-1_000, 10), MarketLifecycle::Active);
        assert_eq!(m.lifecycle(89, 10), MarketLifecycle::Active);
        assert_eq!(m.lifecycle(90, 10), MarketLifecycle::ClosingSoon);
        assert_eq!(m.lifecycle(100, 10), MarketLifecycle::Closed);
    }

    #[test]
    fn token_mapping_round_trips() {
        let m = sample(None, 100, None, "M");
        assert_eq!(m.token_for(Outcome::Yes), &TokenId::new("M-YES"));
        assert_eq!(m.token_for(Outcome::No), &TokenId::new("M-NO"));
        assert_eq!(m.outcome_of(&TokenId::new("M-YES")), Some(Outcome::Yes));
        assert_eq!(m.outcome_of(&TokenId::new("M-NO")), Some(Outcome::No));
        assert_eq!(m.outcome_of(&TokenId::new("OTHER")), None);
    }

    #[test]
    fn upsert_all_counts_added_then_updated() {
        let mut r = Registry::new();
        let m1 = sample(Some(0), 100, None, "M1");
        let m2 = sample(Some(0), 100, None, "M2");

        let stats = r.upsert_all([m1.clone(), m2.clone()]);
        assert_eq!(stats.added, 2);
        assert_eq!(stats.updated, 0);
        assert_eq!(r.len(), 2);

        let stats = r.upsert_all([m1.clone()]);
        assert_eq!(stats.added, 0);
        assert_eq!(stats.updated, 1);
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn prune_removes_matching_markets() {
        let mut r = Registry::new();
        r.upsert_all([
            sample(Some(0), 100, Some(Outcome::Yes), "resolved"),
            sample(Some(0), 100, None, "open"),
        ]);
        let removed = r.prune(|m| m.resolved_outcome.is_some());
        assert_eq!(removed, 1);
        assert_eq!(r.len(), 1);
        assert!(r.contains(&MarketId::new("open")));
    }

    #[test]
    fn in_lifecycle_filters_correctly() {
        let mut r = Registry::new();
        r.upsert_all([
            sample(Some(0), 100, None, "active"),
            sample(Some(200), 300, None, "upcoming"),
            sample(Some(0), 100, Some(Outcome::No), "resolved"),
        ]);

        let now = 50i64;
        let w = 10u64;

        let active: Vec<_> = r
            .in_lifecycle(MarketLifecycle::Active, now, w)
            .map(|m| m.id.clone())
            .collect();
        assert_eq!(active, vec![MarketId::new("active")]);

        let upcoming: Vec<_> = r
            .in_lifecycle(MarketLifecycle::Upcoming, now, w)
            .map(|m| m.id.clone())
            .collect();
        assert_eq!(upcoming, vec![MarketId::new("upcoming")]);

        let resolved: Vec<_> = r
            .in_lifecycle(MarketLifecycle::Resolved, now, w)
            .map(|m| m.id.clone())
            .collect();
        assert_eq!(resolved, vec![MarketId::new("resolved")]);
    }

    #[tokio::test]
    async fn static_discoverer_returns_given_markets() {
        let m = sample(Some(0), 100, None, "X");
        let d = StaticDiscoverer::new(vec![m.clone()]);
        let out = d.discover().await.expect("static never fails");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, m.id);
    }
}
