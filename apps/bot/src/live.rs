//! Live-execution scaffold.
//!
//! This module is the integration seam between the paper-mode core and
//! a real Polymarket order placer. **It is intentionally not wired up**:
//! every method that would talk to the CLOB returns
//! `LiveExecError::CredentialsMissing` or `LiveExecError::NotImplemented`.
//!
//! ## What's here
//!
//! - [`LiveCredentials`] — typed holder for the EOA private key + proxy
//!   wallet address. Constructed from environment variables so we never
//!   put keys in TOML or on disk.
//! - [`LiveOrderId`] / [`OrderState`] — the state machine the reconciler
//!   will read.
//! - [`LiveExecutor`] — same shape as `PaperExecutor` so the orchestrator
//!   loop only has to swap one type. All trade-side methods are stubs.
//! - [`reconcile`] — pure function that diffs local + venue order state
//!   and emits the corrections that should be applied. Tested against
//!   synthetic states, no network.
//!
//! ## What's missing (needs the user's wallet keys)
//!
//! 1. EIP-712 order signing — Polymarket CLOB v2 schema. See
//!    `https://docs.polymarket.com/api/orders/sign-an-order/`.
//! 2. Order placement — `POST /order` with the signed payload + API auth
//!    headers.
//! 3. Cancellation — `POST /cancel` with the order ID + signed nonce.
//! 4. Open-orders polling — `GET /openOrders?owner=...` to drive the
//!    reconciler.
//! 5. WebSocket order-event subscription (preferred over polling once
//!    base case is working).
//!
//! ## Failure mode when called without creds
//!
//! `LiveExecutor::new(None)` returns `Err(LiveExecError::CredentialsMissing)`.
//! `bot::run_live` (when added) calls `LiveExecutor::new` at startup; this
//! will panic-bail before any feed is opened. The intent is **loud
//! failure**: no silent fallback to paper.

pub mod signing;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use market_registry::{MarketId, Outcome};

use crate::strategy::Signal;

/// Polymarket-side identifier for a placed order. Opaque string from the
/// venue; we don't parse it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LiveOrderId(pub String);

impl LiveOrderId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// State machine for a live order. Once a state is reached, the prior
/// states are no longer applicable. Terminal states are `Filled`,
/// `Cancelled`, `Rejected`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderState {
    /// We've decided to send this order but haven't called the API yet.
    Pending,
    /// The API call returned 200 and gave us a venue order ID. The order
    /// is now resting on the book (or being matched).
    Acked,
    /// Some shares filled; remainder still resting.
    PartiallyFilled,
    /// All shares filled. Terminal.
    Filled,
    /// We sent a cancel and the venue acknowledged. Terminal.
    Cancelled,
    /// Venue rejected the order (insufficient balance, invalid price,
    /// etc.). Terminal.
    Rejected,
}

impl OrderState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            OrderState::Filled | OrderState::Cancelled | OrderState::Rejected
        )
    }
}

/// One live order in our internal book. Mirrors the venue's view at the
/// last reconciliation. `filled_size_usd` accumulates partial fills.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveOrder {
    pub id: LiveOrderId,
    pub market_id: MarketId,
    pub side: Outcome,
    pub limit_price: f64,
    pub size_usd: f64,
    pub filled_size_usd: f64,
    pub state: OrderState,
}

/// Polymarket EOA private key + proxy wallet address. **Never log or
/// serialise this struct.** Constructed from environment variables only.
///
/// `POLYMARKET_EOA_PRIVATE_KEY` is the hex-encoded private key (with or
/// without `0x` prefix) used to sign EIP-712 order payloads.
/// `POLYMARKET_PROXY_WALLET_ADDRESS` is the smart-contract proxy that
/// actually holds funds and matches in the CLOB.
#[derive(Clone)]
pub struct LiveCredentials {
    // The private key is held here for the eventual signing path
    // (currently stubbed). Not exposed via any public getter.
    #[allow(dead_code)]
    eoa_private_key: String,
    proxy_wallet_address: String,
}

impl LiveCredentials {
    /// Read both vars from the env. Returns `None` if either is missing
    /// or empty — `LiveExecutor::new` then bails loudly so we never
    /// silently fall back to paper.
    pub fn from_env() -> Option<Self> {
        let key = std::env::var("POLYMARKET_EOA_PRIVATE_KEY").ok()?;
        let addr = std::env::var("POLYMARKET_PROXY_WALLET_ADDRESS").ok()?;
        if key.trim().is_empty() || addr.trim().is_empty() {
            return None;
        }
        Some(Self {
            eoa_private_key: key,
            proxy_wallet_address: addr,
        })
    }

    /// For tests that want to exercise the constructor without setting env.
    #[cfg(test)]
    pub fn for_test(key: &str, addr: &str) -> Self {
        Self {
            eoa_private_key: key.into(),
            proxy_wallet_address: addr.into(),
        }
    }

    /// Proxy wallet address — safe to log (public on-chain identifier).
    pub fn proxy_wallet_address(&self) -> &str {
        &self.proxy_wallet_address
    }
}

impl std::fmt::Debug for LiveCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveCredentials")
            .field("proxy_wallet_address", &self.proxy_wallet_address)
            .field("eoa_private_key", &"<redacted>")
            .finish()
    }
}

/// Errors from the live executor. Every failure mode is explicit — no
/// generic `String`s — so the orchestrator can switch on cause.
#[derive(Debug, thiserror::Error)]
pub enum LiveExecError {
    #[error("live mode requires POLYMARKET_EOA_PRIVATE_KEY and POLYMARKET_PROXY_WALLET_ADDRESS env vars")]
    CredentialsMissing,
    #[error("live executor method `{0}` is not yet implemented")]
    NotImplemented(&'static str),
    #[error("venue rejected order: {0}")]
    VenueReject(String),
    #[error("network error: {0}")]
    Network(String),
}

/// Live executor — same shape as `PaperExecutor`. All trade-side methods
/// are stubs returning `NotImplemented` until the EIP-712 signing path
/// is wired.
pub struct LiveExecutor {
    creds: LiveCredentials,
    open: HashMap<LiveOrderId, LiveOrder>,
}

impl LiveExecutor {
    /// Construct from credentials. Returns `CredentialsMissing` if `None`.
    pub fn new(creds: Option<LiveCredentials>) -> Result<Self, LiveExecError> {
        let creds = creds.ok_or(LiveExecError::CredentialsMissing)?;
        Ok(Self {
            creds,
            open: HashMap::new(),
        })
    }

    pub fn proxy_wallet_address(&self) -> &str {
        self.creds.proxy_wallet_address()
    }

    pub fn open_count(&self) -> usize {
        self.open.len()
    }

    pub fn open_orders(&self) -> Vec<LiveOrder> {
        self.open.values().cloned().collect()
    }

    /// Submit a signal as a real order. **Stub** — will sign + POST when
    /// implemented. Today: always errors `NotImplemented`.
    pub async fn submit(&mut self, _signal: Signal) -> Result<LiveOrderId, LiveExecError> {
        Err(LiveExecError::NotImplemented("submit"))
    }

    /// Cancel an order by venue ID. **Stub**.
    pub async fn cancel(&mut self, _id: &LiveOrderId) -> Result<(), LiveExecError> {
        Err(LiveExecError::NotImplemented("cancel"))
    }

    /// Pull our open orders from the venue. **Stub** — returns
    /// `NotImplemented` until the REST/WS client is wired.
    pub async fn fetch_open_orders_from_venue(&self) -> Result<Vec<LiveOrder>, LiveExecError> {
        Err(LiveExecError::NotImplemented(
            "fetch_open_orders_from_venue",
        ))
    }

    /// Apply a reconciler diff to our internal book. Pure — no network.
    pub fn apply_diff(&mut self, diff: ReconcileDiff) {
        for upd in diff.updates {
            if let Some(o) = self.open.get_mut(&upd.id) {
                o.state = upd.new_state;
                o.filled_size_usd = upd.new_filled_size_usd.unwrap_or(o.filled_size_usd);
            }
        }
        // Remove terminal-state orders from the open book.
        self.open.retain(|_, o| !o.state.is_terminal());
    }
}

/// One correction the reconciler wants to apply to our local view.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderUpdate {
    pub id: LiveOrderId,
    pub new_state: OrderState,
    pub new_filled_size_usd: Option<f64>,
}

/// Output of [`reconcile`]: the corrections to apply.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReconcileDiff {
    pub updates: Vec<OrderUpdate>,
}

/// Reconcile our local view of open orders against the venue's view.
/// Pure function — no IO. Tested standalone.
///
/// Rules:
/// - Order present in both, state changed → emit update with new state.
/// - Order present in both, fill size changed → emit update.
/// - Order present locally as non-terminal but missing on venue → mark
///   `Rejected` (venue evicted it; we don't know exactly why).
/// - Order present on venue but not locally → leave alone (not ours? Or
///   we missed an ack — operator will catch this in metrics). Phase 7
///   doesn't try to adopt orphan orders automatically.
pub fn reconcile(local: &[LiveOrder], venue: &[LiveOrder]) -> ReconcileDiff {
    let mut updates = Vec::new();
    let venue_by_id: HashMap<&LiveOrderId, &LiveOrder> = venue.iter().map(|o| (&o.id, o)).collect();
    for local_order in local {
        if local_order.state.is_terminal() {
            continue;
        }
        match venue_by_id.get(&local_order.id) {
            Some(venue_order) => {
                if venue_order.state != local_order.state
                    || (venue_order.filled_size_usd - local_order.filled_size_usd).abs() > 1e-9
                {
                    updates.push(OrderUpdate {
                        id: local_order.id.clone(),
                        new_state: venue_order.state,
                        new_filled_size_usd: Some(venue_order.filled_size_usd),
                    });
                }
            }
            None => {
                // Missing on venue — assume rejected/cancelled out-of-band.
                updates.push(OrderUpdate {
                    id: local_order.id.clone(),
                    new_state: OrderState::Rejected,
                    new_filled_size_usd: None,
                });
            }
        }
    }
    ReconcileDiff { updates }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_order(id: &str, state: OrderState, filled: f64) -> LiveOrder {
        LiveOrder {
            id: LiveOrderId::new(id),
            market_id: MarketId::new("M"),
            side: Outcome::Yes,
            limit_price: 0.50,
            size_usd: 5.0,
            filled_size_usd: filled,
            state,
        }
    }

    #[test]
    fn new_without_creds_errors_loudly() {
        match LiveExecutor::new(None) {
            Err(LiveExecError::CredentialsMissing) => {}
            _ => panic!("expected CredentialsMissing"),
        }
    }

    #[test]
    fn new_with_creds_succeeds() {
        let creds = LiveCredentials::for_test("0xdeadbeef", "0xproxy");
        let exec = LiveExecutor::new(Some(creds)).expect("should construct");
        assert_eq!(exec.open_count(), 0);
        assert_eq!(exec.proxy_wallet_address(), "0xproxy");
    }

    #[test]
    fn submit_is_stubbed() {
        let creds = LiveCredentials::for_test("0xdeadbeef", "0xproxy");
        let mut exec = LiveExecutor::new(Some(creds)).unwrap();
        let sig = Signal {
            market_id: MarketId::new("M"),
            side: Outcome::Yes,
            size_usd: 1.0,
            price: 0.50,
            fv_for_side: 0.60,
            mid_for_side: 0.50,
            edge: 0.10,
            ttr_secs: 120.0,
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let res = rt.block_on(exec.submit(sig));
        assert!(matches!(res, Err(LiveExecError::NotImplemented("submit"))));
    }

    #[test]
    fn credentials_debug_redacts_private_key() {
        let creds = LiveCredentials::for_test("0xsecret", "0xproxy");
        let s = format!("{:?}", creds);
        assert!(
            !s.contains("0xsecret"),
            "private key must not appear in Debug output"
        );
        assert!(s.contains("0xproxy"), "proxy address should appear");
        assert!(s.contains("redacted"));
    }

    #[test]
    fn order_state_terminal_set() {
        assert!(OrderState::Filled.is_terminal());
        assert!(OrderState::Cancelled.is_terminal());
        assert!(OrderState::Rejected.is_terminal());
        assert!(!OrderState::Pending.is_terminal());
        assert!(!OrderState::Acked.is_terminal());
        assert!(!OrderState::PartiallyFilled.is_terminal());
    }

    #[test]
    fn reconcile_no_changes_emits_empty_diff() {
        let local = vec![mk_order("A", OrderState::Acked, 0.0)];
        let venue = vec![mk_order("A", OrderState::Acked, 0.0)];
        let diff = reconcile(&local, &venue);
        assert!(diff.updates.is_empty());
    }

    #[test]
    fn reconcile_promotes_acked_to_filled() {
        let local = vec![mk_order("A", OrderState::Acked, 0.0)];
        let venue = vec![mk_order("A", OrderState::Filled, 5.0)];
        let diff = reconcile(&local, &venue);
        assert_eq!(diff.updates.len(), 1);
        assert_eq!(diff.updates[0].id.as_str(), "A");
        assert_eq!(diff.updates[0].new_state, OrderState::Filled);
        assert_eq!(diff.updates[0].new_filled_size_usd, Some(5.0));
    }

    #[test]
    fn reconcile_records_partial_fill_progress() {
        let local = vec![mk_order("A", OrderState::Acked, 0.0)];
        let venue = vec![mk_order("A", OrderState::PartiallyFilled, 2.0)];
        let diff = reconcile(&local, &venue);
        assert_eq!(diff.updates.len(), 1);
        assert_eq!(diff.updates[0].new_state, OrderState::PartiallyFilled);
        assert_eq!(diff.updates[0].new_filled_size_usd, Some(2.0));
    }

    #[test]
    fn reconcile_missing_on_venue_marks_rejected() {
        let local = vec![mk_order("A", OrderState::Acked, 0.0)];
        let venue: Vec<LiveOrder> = vec![];
        let diff = reconcile(&local, &venue);
        assert_eq!(diff.updates.len(), 1);
        assert_eq!(diff.updates[0].new_state, OrderState::Rejected);
    }

    #[test]
    fn reconcile_ignores_terminal_local_orders() {
        let local = vec![mk_order("A", OrderState::Filled, 5.0)];
        // Venue doesn't even know about it any more — still no diff.
        let venue: Vec<LiveOrder> = vec![];
        let diff = reconcile(&local, &venue);
        assert!(diff.updates.is_empty());
    }

    #[test]
    fn reconcile_ignores_orphan_venue_orders() {
        // Order on venue but not locally — Phase 7 chooses not to adopt.
        let local: Vec<LiveOrder> = vec![];
        let venue = vec![mk_order("ORPHAN", OrderState::Acked, 0.0)];
        let diff = reconcile(&local, &venue);
        assert!(diff.updates.is_empty());
    }

    #[test]
    fn apply_diff_drops_terminal_orders_from_open_book() {
        let creds = LiveCredentials::for_test("k", "a");
        let mut exec = LiveExecutor::new(Some(creds)).unwrap();
        exec.open
            .insert(LiveOrderId::new("A"), mk_order("A", OrderState::Acked, 0.0));
        exec.open
            .insert(LiveOrderId::new("B"), mk_order("B", OrderState::Acked, 0.0));
        let diff = ReconcileDiff {
            updates: vec![
                OrderUpdate {
                    id: LiveOrderId::new("A"),
                    new_state: OrderState::Filled,
                    new_filled_size_usd: Some(5.0),
                },
                OrderUpdate {
                    id: LiveOrderId::new("B"),
                    new_state: OrderState::PartiallyFilled,
                    new_filled_size_usd: Some(1.0),
                },
            ],
        };
        exec.apply_diff(diff);
        // A is terminal → removed; B is still open with updated fill.
        assert!(!exec.open.contains_key(&LiveOrderId::new("A")));
        let b = exec.open.get(&LiveOrderId::new("B")).unwrap();
        assert_eq!(b.state, OrderState::PartiallyFilled);
        assert!((b.filled_size_usd - 1.0).abs() < 1e-9);
    }
}
