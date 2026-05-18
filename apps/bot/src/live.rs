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

pub mod client;
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
    // Private; only `eoa_private_key_for_signing()` exposes it as a
    // `&str` to the signing module (which immediately hashes it into a
    // `Wallet` so the key bytes don't live in plain memory longer
    // than necessary).
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

    /// Expose the EOA private key for the signing module. Crate-internal
    /// only; never serialised, never logged. Callers must immediately
    /// hash it into a [`signing::Wallet`].
    pub(crate) fn eoa_private_key_for_signing(&self) -> &str {
        &self.eoa_private_key
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
    #[error("live mode requires POLYMARKET_API_KEY, POLYMARKET_API_SECRET and POLYMARKET_API_PASSPHRASE env vars")]
    ApiCredentialsMissing,
    #[error("live executor method `{0}` is not yet implemented")]
    NotImplemented(&'static str),
    #[error("venue rejected order: {0}")]
    VenueReject(String),
    #[error("network error: {0}")]
    Network(String),
    #[error("invalid configuration: {0}")]
    BadConfig(String),
}

/// Per-market metadata the executor needs to convert a `Signal` into an
/// EIP-712 order. Held by the orchestrator; passed in on each submit
/// since one executor instance can place orders on many markets.
#[derive(Debug, Clone)]
pub struct MarketContext {
    /// CTF token id for the YES outcome (decimal-string uint256).
    pub yes_token_id: String,
    /// CTF token id for the NO outcome.
    pub no_token_id: String,
    /// Whether this is a neg-risk market (changes the EIP-712 verifying
    /// contract). Fetch from `GET /neg-risk?token_id=...` and cache.
    pub neg_risk: bool,
    /// Fee rate in basis points for this market — from `GET /fee-rate-bps`.
    pub fee_rate_bps: u64,
    /// Polygon chain id. `137` mainnet.
    pub chain_id: u64,
}

/// Live executor — wraps a [`client::PolyClient`] with order-book
/// bookkeeping. Submits real, signed orders to Polymarket's CLOB.
///
/// The executor is constructed at boot from `LiveCredentials` (EOA +
/// proxy) and `ApiCredentials` (HMAC). All three values are required —
/// `new()` returns `CredentialsMissing` or `ApiCredentialsMissing`
/// otherwise.
pub struct LiveExecutor {
    creds: LiveCredentials,
    client: client::PolyClient,
    open: HashMap<LiveOrderId, LiveOrder>,
}

impl LiveExecutor {
    /// Construct from credentials + base URL. `clob_base_url` should be
    /// `https://clob.polymarket.com` in production; tests can point at
    /// a mock server.
    pub fn new(
        creds: Option<LiveCredentials>,
        api_creds: Option<client::ApiCredentials>,
        clob_base_url: impl Into<String>,
    ) -> Result<Self, LiveExecError> {
        let creds = creds.ok_or(LiveExecError::CredentialsMissing)?;
        let api_creds = api_creds.ok_or(LiveExecError::ApiCredentialsMissing)?;
        let wallet = signing::Wallet::from_private_key_hex(creds.eoa_private_key_for_signing())
            .map_err(|e| LiveExecError::BadConfig(format!("EOA private key: {e}")))?;
        let funder = signing::parse_address(creds.proxy_wallet_address())
            .map_err(|e| LiveExecError::BadConfig(format!("proxy address: {e}")))?;
        let client = client::PolyClient::new(wallet, funder, api_creds, clob_base_url);
        Ok(Self {
            creds,
            client,
            open: HashMap::new(),
        })
    }

    pub fn proxy_wallet_address(&self) -> &str {
        self.creds.proxy_wallet_address()
    }

    pub fn signer_address_hex(&self) -> String {
        self.client.signer_address_hex()
    }

    pub fn open_count(&self) -> usize {
        self.open.len()
    }

    pub fn open_orders(&self) -> Vec<LiveOrder> {
        self.open.values().cloned().collect()
    }

    /// Submit a strategy signal as a signed CLOB order. The market
    /// metadata (token IDs, neg-risk flag, fee rate, chain) comes from
    /// the orchestrator's per-market context. The signal's `side`
    /// chooses YES vs NO token; the order itself is always a BUY
    /// (we're long the side we believe in; selling happens at
    /// resolution or via explicit close).
    pub async fn submit(
        &mut self,
        signal: Signal,
        ctx: &MarketContext,
    ) -> Result<LiveOrderId, LiveExecError> {
        let token_id = match signal.side {
            Outcome::Yes => &ctx.yes_token_id,
            Outcome::No => &ctx.no_token_id,
        };
        let size_shares = if signal.price > 0.0 {
            signal.size_usd / signal.price
        } else {
            return Err(LiveExecError::BadConfig(format!(
                "signal price must be > 0; got {}",
                signal.price
            )));
        };
        let req = client::OrderRequest {
            side: signing::OrderSide::Buy,
            token_id: token_id.clone(),
            price: signal.price,
            size_shares,
            expiration_secs: 0,
            fee_rate_bps: ctx.fee_rate_bps,
            market_info: if ctx.neg_risk {
                client::MarketInfo {
                    neg_risk: true,
                    chain_id: ctx.chain_id,
                }
            } else {
                client::MarketInfo {
                    neg_risk: false,
                    chain_id: ctx.chain_id,
                }
            },
        };
        let salt: u128 = rand::random::<u64>() as u128;
        let order_id = self
            .client
            .submit_order(&req, salt)
            .await
            .map_err(map_client_err)?;
        let id = LiveOrderId::new(order_id);
        let live = LiveOrder {
            id: id.clone(),
            market_id: signal.market_id.clone(),
            side: signal.side,
            limit_price: signal.price,
            size_usd: signal.size_usd,
            filled_size_usd: 0.0,
            state: OrderState::Acked,
        };
        self.open.insert(id.clone(), live);
        Ok(id)
    }

    /// Cancel an order by venue ID. Sends a signed `DELETE /order` and
    /// removes the order from local open-book on success.
    pub async fn cancel(&mut self, id: &LiveOrderId) -> Result<(), LiveExecError> {
        self.client
            .cancel_order(id.as_str())
            .await
            .map_err(map_client_err)?;
        if let Some(o) = self.open.get_mut(id) {
            o.state = OrderState::Cancelled;
        }
        // Cancelled is terminal — drop from open book.
        self.open.remove(id);
        Ok(())
    }

    /// Pull our open orders from the venue. Used by the reconciliation
    /// loop to drive `reconcile(local, venue)`.
    pub async fn fetch_open_orders_from_venue(
        &self,
    ) -> Result<Vec<LiveOrder>, LiveExecError> {
        let rows = self.client.fetch_open_orders().await.map_err(map_client_err)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            // Best-effort parse. Anything we can't map cleanly is
            // skipped — the reconciler treats missing-on-venue as
            // Rejected (terminal) which is safer than fabricating
            // open state.
            let id = LiveOrderId::new(row.id);
            let side = match row.side.as_str() {
                "BUY" => Outcome::Yes,
                "SELL" => Outcome::No,
                _ => continue,
            };
            let state = match row.status.as_str() {
                "LIVE" => OrderState::Acked,
                "MATCHED" | "FILLED" => OrderState::Filled,
                "CANCELED" | "CANCELLED" => OrderState::Cancelled,
                _ => OrderState::Acked,
            };
            let limit_price = row.price.parse::<f64>().unwrap_or(0.0);
            let size_shares = row.original_size.parse::<f64>().unwrap_or(0.0);
            let size_matched = row.size_matched.parse::<f64>().unwrap_or(0.0);
            let market_id = market_registry::MarketId::new(row.market);
            out.push(LiveOrder {
                id,
                market_id,
                side,
                limit_price,
                size_usd: size_shares * limit_price,
                filled_size_usd: size_matched * limit_price,
                state,
            });
        }
        Ok(out)
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

fn map_client_err(e: client::ClientError) -> LiveExecError {
    match e {
        client::ClientError::Signing(s) => LiveExecError::BadConfig(s.to_string()),
        client::ClientError::Http(s) => LiveExecError::Network(s),
        client::ClientError::VenueReject { status, body } => {
            LiveExecError::VenueReject(format!("HTTP {status}: {body}"))
        }
        client::ClientError::Parse(s) => LiveExecError::Network(format!("parse: {s}")),
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

    const TEST_EOA_KEY: &str =
        "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const TEST_PROXY_ADDR: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";

    fn test_api_creds() -> client::ApiCredentials {
        use base64::engine::general_purpose::URL_SAFE as B64URL;
        use base64::Engine;
        client::ApiCredentials {
            api_key: "key".into(),
            secret_b64url: B64URL.encode(b"secret-bytes-32-aaaaaaaaaaaaaaaa"),
            passphrase: "pass".into(),
        }
    }

    #[test]
    fn new_without_eoa_creds_errors_loudly() {
        let res = LiveExecutor::new(None, Some(test_api_creds()), "http://x");
        assert!(matches!(res, Err(LiveExecError::CredentialsMissing)));
    }

    #[test]
    fn new_without_api_creds_errors_loudly() {
        let creds = LiveCredentials::for_test(TEST_EOA_KEY, TEST_PROXY_ADDR);
        let res = LiveExecutor::new(Some(creds), None, "http://x");
        assert!(matches!(res, Err(LiveExecError::ApiCredentialsMissing)));
    }

    #[test]
    fn new_with_creds_succeeds_and_exposes_addresses() {
        let creds = LiveCredentials::for_test(TEST_EOA_KEY, TEST_PROXY_ADDR);
        let exec = LiveExecutor::new(Some(creds), Some(test_api_creds()), "http://x")
            .expect("should construct");
        assert_eq!(exec.open_count(), 0);
        assert_eq!(exec.proxy_wallet_address(), TEST_PROXY_ADDR);
        // EOA derived from the Anvil test key.
        assert_eq!(
            exec.signer_address_hex().to_ascii_lowercase(),
            "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
        );
    }

    #[test]
    fn new_with_invalid_eoa_key_rejects_with_bad_config() {
        let creds = LiveCredentials::for_test("not-hex", TEST_PROXY_ADDR);
        let res = LiveExecutor::new(Some(creds), Some(test_api_creds()), "http://x");
        match res {
            Err(LiveExecError::BadConfig(msg)) => {
                assert!(msg.to_lowercase().contains("private key"), "got: {msg}");
            }
            Err(other) => panic!("expected BadConfig, got {other:?}"),
            Ok(_) => panic!("expected BadConfig, got Ok"),
        }
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
        let creds = LiveCredentials::for_test(TEST_EOA_KEY, TEST_PROXY_ADDR);
        let mut exec =
            LiveExecutor::new(Some(creds), Some(test_api_creds()), "http://x").unwrap();
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

    // -----------------------------------------------------------------
    // End-to-end: a real Signal goes through LiveExecutor::submit and
    // produces a signed POST against a one-shot mock server. Verifies
    // the YES/NO token selection logic and that the on-success path
    // updates the local open book.
    // -----------------------------------------------------------------

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn submit_signal_selects_yes_token_and_records_open_order() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let mut all = Vec::new();
            loop {
                let n = sock.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                all.extend_from_slice(&buf[..n]);
                // Found end of headers? Parse content-length and stop.
                if let Some(pos) = all.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&all[..pos]).to_string();
                    let cl: usize = head
                        .lines()
                        .find_map(|l| {
                            let mut s = l.splitn(2, ':');
                            let (n, v) = (s.next()?.trim(), s.next()?.trim());
                            if n.eq_ignore_ascii_case("content-length") {
                                v.parse().ok()
                            } else {
                                None
                            }
                        })
                        .unwrap_or(0);
                    while all.len() < pos + 4 + cl {
                        let m = sock.read(&mut buf).await.unwrap();
                        if m == 0 {
                            break;
                        }
                        all.extend_from_slice(&buf[..m]);
                    }
                    let body = String::from_utf8_lossy(&all[pos + 4..]).to_string();
                    // Verify the YES token was selected
                    let parsed: client::SignedOrderRequest =
                        serde_json::from_str(&body).expect("valid JSON");
                    assert_eq!(parsed.order.token_id, "100200300", "YES token id");
                    assert_eq!(parsed.order.side, "BUY");
                    break;
                }
            }
            let body = r#"{"success":true,"errorMsg":"","orderID":"0xabc123"}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.shutdown().await.ok();
        });

        let creds = LiveCredentials::for_test(TEST_EOA_KEY, TEST_PROXY_ADDR);
        let mut exec =
            LiveExecutor::new(Some(creds), Some(test_api_creds()), format!("http://{addr}"))
                .unwrap();

        let signal = Signal {
            market_id: MarketId::new("0xmkt"),
            side: Outcome::Yes,
            size_usd: 5.0,
            price: 0.50,
            fv_for_side: 0.60,
            mid_for_side: 0.50,
            edge: 0.10,
            ttr_secs: 120.0,
        };
        let ctx = MarketContext {
            yes_token_id: "100200300".into(),
            no_token_id: "400500600".into(),
            neg_risk: false,
            fee_rate_bps: 0,
            chain_id: 137,
        };

        let id = exec.submit(signal, &ctx).await.expect("submit ok");
        assert_eq!(id.as_str(), "0xabc123");
        // Internal open-book records the order.
        assert_eq!(exec.open_count(), 1);
        let order = exec.open_orders().pop().unwrap();
        assert_eq!(order.id, id);
        assert_eq!(order.side, Outcome::Yes);
        assert_eq!(order.state, OrderState::Acked);
        assert!((order.size_usd - 5.0).abs() < 1e-9);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn submit_signal_selects_no_token_when_side_is_no() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let mut all = Vec::new();
            loop {
                let n = sock.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                all.extend_from_slice(&buf[..n]);
                if let Some(pos) = all.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&all[..pos]).to_string();
                    let cl: usize = head
                        .lines()
                        .find_map(|l| {
                            let mut s = l.splitn(2, ':');
                            let (n, v) = (s.next()?.trim(), s.next()?.trim());
                            if n.eq_ignore_ascii_case("content-length") {
                                v.parse().ok()
                            } else {
                                None
                            }
                        })
                        .unwrap_or(0);
                    while all.len() < pos + 4 + cl {
                        let m = sock.read(&mut buf).await.unwrap();
                        if m == 0 {
                            break;
                        }
                        all.extend_from_slice(&buf[..m]);
                    }
                    let body = String::from_utf8_lossy(&all[pos + 4..]).to_string();
                    let parsed: client::SignedOrderRequest =
                        serde_json::from_str(&body).expect("valid JSON");
                    // Critical: signal.side = No selects the NO token id.
                    assert_eq!(parsed.order.token_id, "400500600", "NO token id");
                    break;
                }
            }
            let body = r#"{"success":true,"errorMsg":"","orderID":"0xdef456"}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.shutdown().await.ok();
        });

        let creds = LiveCredentials::for_test(TEST_EOA_KEY, TEST_PROXY_ADDR);
        let mut exec =
            LiveExecutor::new(Some(creds), Some(test_api_creds()), format!("http://{addr}"))
                .unwrap();
        let signal = Signal {
            market_id: MarketId::new("0xmkt"),
            side: Outcome::No,
            size_usd: 5.0,
            price: 0.40,
            fv_for_side: 0.50,
            mid_for_side: 0.40,
            edge: 0.10,
            ttr_secs: 120.0,
        };
        let ctx = MarketContext {
            yes_token_id: "100200300".into(),
            no_token_id: "400500600".into(),
            neg_risk: false,
            fee_rate_bps: 0,
            chain_id: 137,
        };
        let id = exec.submit(signal, &ctx).await.expect("submit ok");
        assert_eq!(id.as_str(), "0xdef456");
        server.await.unwrap();
    }
}
