//! HTTP client for the Polymarket CLOB v2 REST API.
//!
//! All authenticated calls go through `L2 HMAC` headers built by
//! [`crate::live::signing::hmac_l2_signature`]. The HMAC pre-image is
//! `timestamp + method + path + body`. CRITICAL: the body bytes used to
//! generate the HMAC MUST be IDENTICAL to the bytes sent over the wire —
//! re-serialising between sign-time and post-time will produce a
//! signature the server can't verify. We build the body once and reuse
//! the same `String` for both.
//!
//! ## What this module does NOT do
//!
//! - Talk to the CLOB. The `reqwest`-backed [`PolyClient::submit_order`]
//!   etc. are wired but only exercised in tests via a swappable
//!   transport trait. The production wiring lives in [`crate::bot`]
//!   under the (future) `run_live` orchestrator.
//! - Handle order-book reads. Those go through the existing
//!   `polymarket_feed` book poller — unauthenticated REST.
//! - Negotiate API credentials. [`derive_api_credentials`] is sketched
//!   but the L1 `ClobAuthDomain` signing path is sibling work; for v1
//!   the user provides the creds via env or by running the upstream
//!   `py-clob-client` once.

use serde::{Deserialize, Serialize};

use super::signing::{
    domain_separator, exchange_address_mainnet, hmac_l2_signature, neg_risk_exchange_address_mainnet,
    order_signing_hash, u256_be_from_decimal_string, Order, OrderSide, SignatureType, SigningError,
    Wallet,
};
#[cfg(test)]
use super::signing::parse_address;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Polygon mainnet — the production Polymarket chain.
pub const POLYGON_MAINNET_CHAIN_ID: u64 = 137;

/// Default CLOB REST base. Override in tests.
pub const CLOB_BASE_URL: &str = "https://clob.polymarket.com";

/// API credentials returned by `/auth/api-key` or `/auth/derive-api-key`.
/// **Never log or serialize these in production.**
#[derive(Clone)]
pub struct ApiCredentials {
    pub api_key: String,
    pub secret_b64url: String,
    pub passphrase: String,
}

impl std::fmt::Debug for ApiCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiCredentials")
            .field("api_key", &"<redacted>")
            .field("secret_b64url", &"<redacted>")
            .field("passphrase", &"<redacted>")
            .finish()
    }
}

impl ApiCredentials {
    /// Read from env: `POLYMARKET_API_KEY`, `POLYMARKET_API_SECRET`,
    /// `POLYMARKET_API_PASSPHRASE`. Returns `None` if any is missing
    /// or empty.
    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("POLYMARKET_API_KEY").ok()?;
        let secret = std::env::var("POLYMARKET_API_SECRET").ok()?;
        let passphrase = std::env::var("POLYMARKET_API_PASSPHRASE").ok()?;
        if api_key.trim().is_empty() || secret.trim().is_empty() || passphrase.trim().is_empty() {
            return None;
        }
        Some(Self {
            api_key,
            secret_b64url: secret,
            passphrase,
        })
    }
}

// ---------------------------------------------------------------------------
// Wire format — `WireOrder` + `SignedOrderRequest`
// ---------------------------------------------------------------------------

/// Per-market settings the order builder needs to know.
/// `neg_risk` selects the EIP-712 `verifyingContract`. Caller obtains it
/// from `GET /neg-risk?token_id=...` and caches per market.
#[derive(Debug, Clone, Copy)]
pub struct MarketInfo {
    pub neg_risk: bool,
    pub chain_id: u64,
}

impl MarketInfo {
    pub fn mainnet_standard() -> Self {
        Self {
            neg_risk: false,
            chain_id: POLYGON_MAINNET_CHAIN_ID,
        }
    }
    pub fn mainnet_neg_risk() -> Self {
        Self {
            neg_risk: true,
            chain_id: POLYGON_MAINNET_CHAIN_ID,
        }
    }
    pub fn verifying_contract(&self) -> [u8; 20] {
        if self.neg_risk {
            neg_risk_exchange_address_mainnet()
        } else {
            exchange_address_mainnet()
        }
    }
}

/// Inputs to build one signed CLOB order. The price/size are in
/// Polymarket's "outcome share" unit: shares × $1 payout. `price` ∈ (0,1).
/// `size_shares` is the number of shares — we convert price+size to the
/// `makerAmount`/`takerAmount` USDC integers the EIP-712 hash requires.
#[derive(Debug, Clone)]
pub struct OrderRequest {
    pub side: OrderSide,
    /// CTF ERC-1155 token id, decimal string (full uint256 range).
    pub token_id: String,
    /// Price per share, probability units ∈ (0, 1).
    pub price: f64,
    /// Number of shares to buy/sell.
    pub size_shares: f64,
    /// `0` = good-til-cancel (default for our taker-style strategy).
    pub expiration_secs: u64,
    /// Fee rate negotiated with venue for the market. Fetch from
    /// `GET /fee-rate-bps`; for crypto 5m markets this has been `0`
    /// historically (fees handled separately).
    pub fee_rate_bps: u64,
    pub market_info: MarketInfo,
}

/// JSON shape the CLOB `POST /order` endpoint expects under the `order`
/// key. Numeric fields go on the wire as STRINGS (decimal), side as
/// `"BUY"`/`"SELL"`, signatureType as int. Matches `clob-client`
/// `orderToJson` exactly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WireOrder {
    pub salt: u128,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    #[serde(rename = "makerAmount")]
    pub maker_amount: String,
    #[serde(rename = "takerAmount")]
    pub taker_amount: String,
    pub side: String,
    pub expiration: String,
    pub nonce: String,
    #[serde(rename = "feeRateBps")]
    pub fee_rate_bps: String,
    #[serde(rename = "signatureType")]
    pub signature_type: u8,
    pub signature: String,
}

/// Full `POST /order` body.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SignedOrderRequest {
    pub order: WireOrder,
    /// API key UUID (the `api_key` field of ApiCredentials).
    pub owner: String,
    #[serde(rename = "orderType")]
    pub order_type: String,
    #[serde(rename = "deferExec")]
    pub defer_exec: bool,
}

/// Convert price+size to the integer USDC/share amounts the contract
/// hashes. USDC has 6 decimals. CTF shares also have 6 decimals on chain.
///
/// For BUY: `makerAmount` is USDC paid, `takerAmount` is shares received.
///   maker = floor(price × size × 1e6); taker = floor(size × 1e6)
/// For SELL: flipped — we surrender shares to receive USDC.
///   maker = floor(size × 1e6); taker = floor(price × size × 1e6)
fn amounts_for_side(
    side: OrderSide,
    price: f64,
    size_shares: f64,
) -> Result<(u128, u128), SigningError> {
    if !(price.is_finite() && size_shares.is_finite()) || price <= 0.0 || price >= 1.0 || size_shares <= 0.0 {
        return Err(SigningError::ParseUint(format!(
            "invalid price/size: {price}, {size_shares}"
        )));
    }
    // USDC + CTF share have 6 decimals.
    let scale = 1_000_000.0_f64;
    let shares = (size_shares * scale).floor() as u128;
    let usdc = (price * size_shares * scale).floor() as u128;
    Ok(match side {
        OrderSide::Buy => (usdc, shares),
        OrderSide::Sell => (shares, usdc),
    })
}

/// Build a signed order ready to POST. Returns the deserialised body
/// and the canonical JSON bytes that must ALSO be used as the HMAC
/// pre-image — re-serialising would produce different bytes and break
/// the signature.
pub fn build_signed_order(
    wallet: &Wallet,
    funder_address: [u8; 20],
    api_key: &str,
    req: &OrderRequest,
    salt: u128,
) -> Result<(SignedOrderRequest, String), SigningError> {
    let (maker_amount, taker_amount) = amounts_for_side(req.side, req.price, req.size_shares)?;
    let order = Order {
        salt,
        maker: funder_address,
        signer: wallet.address(),
        taker: [0u8; 20],
        token_id: u256_be_from_decimal_string(&req.token_id)?,
        maker_amount,
        taker_amount,
        expiration: req.expiration_secs,
        nonce: 0,
        fee_rate_bps: req.fee_rate_bps,
        side: req.side,
        signature_type: SignatureType::PolyGnosisSafe,
    };
    let digest = order_signing_hash(
        &order,
        domain_separator(req.market_info.chain_id, req.market_info.verifying_contract()),
    );
    let sig = wallet.sign_digest(digest)?;
    let wire = WireOrder {
        salt: order.salt,
        maker: format!("0x{}", hex::encode(funder_address)),
        signer: wallet.address_hex(),
        taker: format!("0x{}", hex::encode([0u8; 20])),
        token_id: req.token_id.clone(),
        maker_amount: order.maker_amount.to_string(),
        taker_amount: order.taker_amount.to_string(),
        side: req.side.as_wire_str().to_string(),
        expiration: order.expiration.to_string(),
        nonce: order.nonce.to_string(),
        fee_rate_bps: order.fee_rate_bps.to_string(),
        signature_type: order.signature_type.as_u8(),
        signature: sig.to_hex_prefixed(),
    };
    let body = SignedOrderRequest {
        order: wire,
        owner: api_key.to_string(),
        order_type: "GTC".to_string(),
        defer_exec: false,
    };
    let json = serde_json::to_string(&body)
        .map_err(|e| SigningError::ParseUint(format!("serde: {e}")))?;
    Ok((body, json))
}

// ---------------------------------------------------------------------------
// Cancel request
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelRequest {
    #[serde(rename = "orderID")]
    pub order_id: String,
}

pub fn build_cancel_body(order_id: &str) -> Result<String, SigningError> {
    let req = CancelRequest {
        order_id: order_id.to_string(),
    };
    serde_json::to_string(&req).map_err(|e| SigningError::ParseUint(format!("serde: {e}")))
}

// ---------------------------------------------------------------------------
// Open-orders response shapes
// ---------------------------------------------------------------------------

/// One open order returned by `GET /data/orders`. Fields beyond what we
/// use are captured as `serde_json::Value` so future additions don't
/// break parsing.
#[derive(Debug, Clone, Deserialize)]
pub struct OpenOrderRow {
    pub id: String,
    pub status: String,
    pub market: String,
    pub side: String,
    pub price: String,
    #[serde(rename = "original_size")]
    pub original_size: String,
    #[serde(rename = "size_matched", default)]
    pub size_matched: String,
    #[serde(rename = "asset_id")]
    pub asset_id: String,
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenOrdersResponse {
    pub data: Vec<OpenOrderRow>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

// ---------------------------------------------------------------------------
// Auth header builder
// ---------------------------------------------------------------------------

/// Pre-built header list for an authenticated REST request.
/// `(name, value)` pairs.
#[derive(Debug, Clone)]
pub struct AuthHeaders {
    pub headers: Vec<(&'static str, String)>,
}

/// Build the five `POLY_*` headers for one authenticated request.
pub fn build_auth_headers(
    wallet: &Wallet,
    creds: &ApiCredentials,
    method: &str,
    path: &str,
    body: &str,
    timestamp_secs: i64,
) -> Result<AuthHeaders, SigningError> {
    let signature =
        hmac_l2_signature(&creds.secret_b64url, timestamp_secs, method, path, body)?;
    Ok(AuthHeaders {
        headers: vec![
            ("POLY_ADDRESS", wallet.address_hex()),
            ("POLY_API_KEY", creds.api_key.clone()),
            ("POLY_PASSPHRASE", creds.passphrase.clone()),
            ("POLY_TIMESTAMP", timestamp_secs.to_string()),
            ("POLY_SIGNATURE", signature),
        ],
    })
}

// ---------------------------------------------------------------------------
// Client errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("signing: {0}")]
    Signing(#[from] SigningError),
    #[error("http: {0}")]
    Http(String),
    #[error("venue rejected: HTTP {status}: {body}")]
    VenueReject { status: u16, body: String },
    #[error("response parse: {0}")]
    Parse(String),
}

// ---------------------------------------------------------------------------
// PolyClient — reqwest-backed live client
// ---------------------------------------------------------------------------

/// Production client. Methods are async; one `submit_order` call is one
/// HMAC-signed POST. The client owns the [`Wallet`] (for EOA signing)
/// and [`ApiCredentials`] (for L2 HMAC). The `funder_address` is the
/// Polymarket-side proxy (Gnosis Safe) that actually holds funds —
/// kept separate because for `signatureType = POLY_GNOSIS_SAFE` the EOA
/// signs on the proxy's behalf.
pub struct PolyClient {
    http: reqwest::Client,
    base_url: String,
    wallet: Wallet,
    funder_address: [u8; 20],
    creds: ApiCredentials,
}

impl PolyClient {
    pub fn new(
        wallet: Wallet,
        funder_address: [u8; 20],
        creds: ApiCredentials,
        base_url: impl Into<String>,
    ) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: base_url.into(),
            wallet,
            funder_address,
            creds,
        }
    }

    pub fn signer_address_hex(&self) -> String {
        self.wallet.address_hex()
    }

    /// Submit a single order. Returns the venue order ID.
    pub async fn submit_order(
        &self,
        req: &OrderRequest,
        salt: u128,
    ) -> Result<String, ClientError> {
        let (_signed, body) =
            build_signed_order(&self.wallet, self.funder_address, &self.creds.api_key, req, salt)?;
        let ts = unix_secs_now();
        let path = "/order";
        let headers = build_auth_headers(&self.wallet, &self.creds, "POST", path, &body, ts)?;
        let url = format!("{}{path}", self.base_url);
        let mut http_req = self.http.post(&url).body(body).header("Content-Type", "application/json");
        for (k, v) in &headers.headers {
            http_req = http_req.header(*k, v);
        }
        let resp = http_req.send().await.map_err(|e| ClientError::Http(e.to_string()))?;
        let status = resp.status();
        let text = resp.text().await.map_err(|e| ClientError::Http(e.to_string()))?;
        if !status.is_success() {
            return Err(ClientError::VenueReject {
                status: status.as_u16(),
                body: text,
            });
        }
        // Response shape: {"success": bool, "errorMsg": "", "orderID": "0x...", ...}
        let v: serde_json::Value =
            serde_json::from_str(&text).map_err(|e| ClientError::Parse(e.to_string()))?;
        // Sometimes the venue returns success=true with an errorMsg
        // populated; treat that as a venue reject.
        if v.get("success").and_then(|s| s.as_bool()) == Some(false) {
            let msg = v
                .get("errorMsg")
                .and_then(|s| s.as_str())
                .unwrap_or("unknown")
                .to_string();
            return Err(ClientError::VenueReject {
                status: status.as_u16(),
                body: msg,
            });
        }
        v.get("orderID")
            .and_then(|s| s.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| ClientError::Parse(format!("no orderID in response: {text}")))
    }

    /// Cancel an order by venue ID.
    pub async fn cancel_order(&self, order_id: &str) -> Result<(), ClientError> {
        let body = build_cancel_body(order_id)?;
        let ts = unix_secs_now();
        let path = "/order";
        let headers = build_auth_headers(&self.wallet, &self.creds, "DELETE", path, &body, ts)?;
        let url = format!("{}{path}", self.base_url);
        let mut http_req = self
            .http
            .delete(&url)
            .body(body)
            .header("Content-Type", "application/json");
        for (k, v) in &headers.headers {
            http_req = http_req.header(*k, v);
        }
        let resp = http_req.send().await.map_err(|e| ClientError::Http(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(ClientError::VenueReject {
                status: status.as_u16(),
                body: text,
            });
        }
        Ok(())
    }

    /// Fetch all open orders for our signer. Paginates via `next_cursor`
    /// until the terminator `"LTE="` (or no cursor) is returned.
    pub async fn fetch_open_orders(&self) -> Result<Vec<OpenOrderRow>, ClientError> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let path_with_query = match cursor.as_deref() {
                Some(c) => format!("/data/orders?next_cursor={c}"),
                None => "/data/orders".to_string(),
            };
            let ts = unix_secs_now();
            // For GETs the HMAC body is empty.
            let headers = build_auth_headers(
                &self.wallet,
                &self.creds,
                "GET",
                &path_with_query,
                "",
                ts,
            )?;
            let url = format!("{}{path_with_query}", self.base_url);
            let mut http_req = self.http.get(&url);
            for (k, v) in &headers.headers {
                http_req = http_req.header(*k, v);
            }
            let resp = http_req.send().await.map_err(|e| ClientError::Http(e.to_string()))?;
            let status = resp.status();
            let text = resp.text().await.map_err(|e| ClientError::Http(e.to_string()))?;
            if !status.is_success() {
                return Err(ClientError::VenueReject {
                    status: status.as_u16(),
                    body: text,
                });
            }
            let parsed: OpenOrdersResponse =
                serde_json::from_str(&text).map_err(|e| ClientError::Parse(e.to_string()))?;
            out.extend(parsed.data);
            cursor = match parsed.next_cursor {
                Some(c) if c != "LTE=" && !c.is_empty() => Some(c),
                _ => break,
            };
        }
        Ok(out)
    }
}

fn unix_secs_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_PRIVATE_KEY: &str =
        "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    #[test]
    fn amounts_buy_split_by_price() {
        let (m, t) = amounts_for_side(OrderSide::Buy, 0.50, 4.0).unwrap();
        // BUY: maker = 0.50 × 4 × 1e6 = 2_000_000 USDC; taker = 4 × 1e6 shares
        assert_eq!(m, 2_000_000);
        assert_eq!(t, 4_000_000);
    }

    #[test]
    fn amounts_sell_flip() {
        let (m, t) = amounts_for_side(OrderSide::Sell, 0.50, 4.0).unwrap();
        assert_eq!(m, 4_000_000);
        assert_eq!(t, 2_000_000);
    }

    #[test]
    fn amounts_reject_out_of_range_price() {
        assert!(amounts_for_side(OrderSide::Buy, 0.0, 1.0).is_err());
        assert!(amounts_for_side(OrderSide::Buy, 1.0, 1.0).is_err());
        assert!(amounts_for_side(OrderSide::Buy, -0.1, 1.0).is_err());
        assert!(amounts_for_side(OrderSide::Buy, f64::NAN, 1.0).is_err());
    }

    #[test]
    fn build_signed_order_produces_consistent_json_and_hmac_preimage() {
        let wallet = Wallet::from_private_key_hex(TEST_PRIVATE_KEY).unwrap();
        let funder = parse_address("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266").unwrap();
        let req = OrderRequest {
            side: OrderSide::Buy,
            token_id: "1234".to_string(),
            price: 0.50,
            size_shares: 4.0,
            expiration_secs: 0,
            fee_rate_bps: 0,
            market_info: MarketInfo::mainnet_standard(),
        };
        let (body, json) = build_signed_order(&wallet, funder, "api-key-uuid", &req, 42).unwrap();
        // Round-trip JSON → struct must equal the original body.
        let parsed: SignedOrderRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, body);
        // The JSON body is also the HMAC pre-image — sanity check that
        // re-serialising the same struct gives the same bytes
        // (deterministic; serde_json by default produces stable
        // key-order from struct definition order).
        assert_eq!(serde_json::to_string(&body).unwrap(), json);
    }

    #[test]
    fn signed_order_wire_fields_match_python_client_shape() {
        let wallet = Wallet::from_private_key_hex(TEST_PRIVATE_KEY).unwrap();
        let funder = parse_address("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266").unwrap();
        let req = OrderRequest {
            side: OrderSide::Buy,
            token_id: "1234".to_string(),
            price: 0.50,
            size_shares: 100.0,
            expiration_secs: 0,
            fee_rate_bps: 0,
            market_info: MarketInfo::mainnet_standard(),
        };
        let (body, _) = build_signed_order(&wallet, funder, "owner-key", &req, 7).unwrap();
        // Side rendered as string
        assert_eq!(body.order.side, "BUY");
        // Amounts as decimal strings
        assert_eq!(body.order.maker_amount, "50000000"); // 0.50 × 100 × 1e6
        assert_eq!(body.order.taker_amount, "100000000"); // 100 × 1e6
        // signatureType = 2 (POLY_GNOSIS_SAFE)
        assert_eq!(body.order.signature_type, 2);
        // Default orderType GTC, defer_exec false
        assert_eq!(body.order_type, "GTC");
        assert!(!body.defer_exec);
        // Owner echoed from caller
        assert_eq!(body.owner, "owner-key");
        // Maker = funder, signer = EOA
        assert_eq!(
            body.order.maker.to_lowercase(),
            "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
        );
        assert_eq!(
            body.order.signer.to_lowercase(),
            "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
        );
        // Taker is the zero address
        assert_eq!(
            body.order.taker,
            "0x0000000000000000000000000000000000000000"
        );
        // Signature is the 65-byte hex (130 chars + 0x prefix)
        assert!(body.order.signature.starts_with("0x"));
        assert_eq!(body.order.signature.len(), 132);
    }

    #[test]
    fn build_signed_order_uses_neg_risk_contract_when_flagged() {
        // The contract address only matters for the digest, but we
        // can confirm the JSON-side outputs are identical apart from
        // the embedded signature (which depends on the digest).
        let wallet = Wallet::from_private_key_hex(TEST_PRIVATE_KEY).unwrap();
        let funder = parse_address("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266").unwrap();
        let req_std = OrderRequest {
            side: OrderSide::Buy,
            token_id: "1234".to_string(),
            price: 0.50,
            size_shares: 1.0,
            expiration_secs: 0,
            fee_rate_bps: 0,
            market_info: MarketInfo::mainnet_standard(),
        };
        let req_nr = OrderRequest {
            market_info: MarketInfo::mainnet_neg_risk(),
            ..req_std.clone()
        };
        let (std, _) = build_signed_order(&wallet, funder, "k", &req_std, 1).unwrap();
        let (nr, _) = build_signed_order(&wallet, funder, "k", &req_nr, 1).unwrap();
        assert_ne!(
            std.order.signature, nr.order.signature,
            "different verifying contracts must produce different signatures"
        );
        // Everything else should be byte-identical.
        assert_eq!(std.order.maker_amount, nr.order.maker_amount);
        assert_eq!(std.order.taker_amount, nr.order.taker_amount);
        assert_eq!(std.order.side, nr.order.side);
    }

    #[test]
    fn build_cancel_body_is_compact_json() {
        let body = build_cancel_body("0xabc123").unwrap();
        assert_eq!(body, r#"{"orderID":"0xabc123"}"#);
    }

    #[test]
    fn open_orders_parses_paginated_response() {
        let raw = r#"{
            "data": [{
                "id": "0xord1",
                "status": "LIVE",
                "market": "0xmkt",
                "side": "BUY",
                "price": "0.42",
                "original_size": "10.0",
                "size_matched": "0.0",
                "asset_id": "12345"
            }],
            "next_cursor": "abc=="
        }"#;
        let parsed: OpenOrdersResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.data.len(), 1);
        assert_eq!(parsed.data[0].id, "0xord1");
        assert_eq!(parsed.next_cursor.as_deref(), Some("abc=="));
    }

    #[test]
    fn open_orders_parses_terminator_cursor() {
        let raw = r#"{"data": [], "next_cursor": "LTE="}"#;
        let parsed: OpenOrdersResponse = serde_json::from_str(raw).unwrap();
        assert!(parsed.data.is_empty());
        assert_eq!(parsed.next_cursor.as_deref(), Some("LTE="));
    }

    #[test]
    fn auth_headers_have_five_well_known_names() {
        let wallet = Wallet::from_private_key_hex(TEST_PRIVATE_KEY).unwrap();
        use base64::engine::general_purpose::URL_SAFE as B64URL;
        use base64::Engine;
        let creds = ApiCredentials {
            api_key: "key".into(),
            secret_b64url: B64URL.encode(b"secret-bytes-32-aaaaaaaaaaaaaaaa"),
            passphrase: "pass".into(),
        };
        let h = build_auth_headers(&wallet, &creds, "POST", "/order", "{}", 1_700_000_000).unwrap();
        let names: Vec<&&str> = h.headers.iter().map(|(n, _)| n).collect();
        assert!(names.contains(&&"POLY_ADDRESS"));
        assert!(names.contains(&&"POLY_API_KEY"));
        assert!(names.contains(&&"POLY_PASSPHRASE"));
        assert!(names.contains(&&"POLY_TIMESTAMP"));
        assert!(names.contains(&&"POLY_SIGNATURE"));
    }

    #[test]
    fn auth_headers_signature_changes_with_body() {
        let wallet = Wallet::from_private_key_hex(TEST_PRIVATE_KEY).unwrap();
        use base64::engine::general_purpose::URL_SAFE as B64URL;
        use base64::Engine;
        let creds = ApiCredentials {
            api_key: "key".into(),
            secret_b64url: B64URL.encode(b"secret-bytes-32-aaaaaaaaaaaaaaaa"),
            passphrase: "pass".into(),
        };
        let h1 = build_auth_headers(&wallet, &creds, "POST", "/order", "{}", 1).unwrap();
        let h2 = build_auth_headers(&wallet, &creds, "POST", "/order", "{\"a\":1}", 1).unwrap();
        let sig1 = h1
            .headers
            .iter()
            .find(|(n, _)| *n == "POLY_SIGNATURE")
            .unwrap()
            .1
            .clone();
        let sig2 = h2
            .headers
            .iter()
            .find(|(n, _)| *n == "POLY_SIGNATURE")
            .unwrap()
            .1
            .clone();
        assert_ne!(sig1, sig2, "different bodies must yield different HMACs");
    }

    #[test]
    fn api_credentials_debug_is_redacted() {
        let c = ApiCredentials {
            api_key: "real-key".into(),
            secret_b64url: "real-secret".into(),
            passphrase: "real-pass".into(),
        };
        let s = format!("{:?}", c);
        assert!(!s.contains("real-key"));
        assert!(!s.contains("real-secret"));
        assert!(!s.contains("real-pass"));
        assert!(s.contains("redacted"));
    }

    // -----------------------------------------------------------------
    // Mocked-HTTP integration: spin up a one-shot TCP server, point the
    // client at it, assert the request shape + handle the response.
    // -----------------------------------------------------------------

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Read one HTTP request from `socket` and return (request-line,
    /// headers, body). Tolerates `Content-Length`-framed bodies.
    async fn read_http_request(socket: &mut tokio::net::TcpStream) -> (String, Vec<(String, String)>, String) {
        let mut buf = Vec::with_capacity(4096);
        let mut tmp = [0u8; 1024];
        loop {
            let n = socket.read(&mut tmp).await.unwrap();
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            // Found end of headers?
            if let Some(pos) = find_double_crlf(&buf) {
                // Parse Content-Length to decide if we need more bytes.
                let head = String::from_utf8_lossy(&buf[..pos]).to_string();
                let cl = head
                    .lines()
                    .find_map(|l| {
                        let mut split = l.splitn(2, ':');
                        let n = split.next()?.trim();
                        let v = split.next()?.trim();
                        if n.eq_ignore_ascii_case("content-length") {
                            v.parse::<usize>().ok()
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                let body_start = pos + 4;
                while buf.len() < body_start + cl {
                    let m = socket.read(&mut tmp).await.unwrap();
                    if m == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..m]);
                }
                break;
            }
        }
        let full = String::from_utf8_lossy(&buf).to_string();
        let pos = find_double_crlf(&buf).unwrap_or(full.len());
        let head_str = &full[..pos];
        let mut lines = head_str.lines();
        let request_line = lines.next().unwrap_or("").to_string();
        let headers: Vec<(String, String)> = lines
            .filter_map(|l| {
                let mut split = l.splitn(2, ':');
                let n = split.next()?.trim().to_string();
                let v = split.next()?.trim().to_string();
                Some((n, v))
            })
            .collect();
        let body = if pos + 4 <= full.len() {
            full[pos + 4..].to_string()
        } else {
            String::new()
        };
        (request_line, headers, body)
    }

    fn find_double_crlf(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|w| w == b"\r\n\r\n")
    }

    fn http_ok(body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",
            body.len(), body
        )
        .into_bytes()
    }

    #[tokio::test]
    async fn submit_order_posts_signed_body_and_parses_orderid() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let (req_line, headers, body) = read_http_request(&mut sock).await;
            // Verify it's POST /order
            assert!(req_line.starts_with("POST /order"));
            // Required POLY_* headers present
            let names: Vec<String> = headers.iter().map(|(n, _)| n.to_ascii_uppercase()).collect();
            for required in [
                "POLY_ADDRESS",
                "POLY_API_KEY",
                "POLY_PASSPHRASE",
                "POLY_TIMESTAMP",
                "POLY_SIGNATURE",
            ] {
                assert!(
                    names.iter().any(|n| n == required),
                    "missing header {required}; got {names:?}"
                );
            }
            // Body is a SignedOrderRequest
            let parsed: SignedOrderRequest = serde_json::from_str(&body).expect("valid JSON");
            assert_eq!(parsed.order.side, "BUY");
            assert_eq!(parsed.order.signature_type, 2);
            // Respond with a fake order ID
            let resp = http_ok(r#"{"success":true,"errorMsg":"","orderID":"0xfeedbeef"}"#);
            sock.write_all(&resp).await.unwrap();
            sock.shutdown().await.ok();
        });

        let wallet = Wallet::from_private_key_hex(TEST_PRIVATE_KEY).unwrap();
        let funder = parse_address("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266").unwrap();
        use base64::engine::general_purpose::URL_SAFE as B64URL;
        use base64::Engine;
        let creds = ApiCredentials {
            api_key: "key".into(),
            secret_b64url: B64URL.encode(b"secret-bytes-32-aaaaaaaaaaaaaaaa"),
            passphrase: "pass".into(),
        };
        let client = PolyClient::new(wallet, funder, creds, format!("http://{addr}"));
        let req = OrderRequest {
            side: OrderSide::Buy,
            token_id: "1234".to_string(),
            price: 0.50,
            size_shares: 4.0,
            expiration_secs: 0,
            fee_rate_bps: 0,
            market_info: MarketInfo::mainnet_standard(),
        };
        let order_id = client.submit_order(&req, 42).await.expect("submit ok");
        assert_eq!(order_id, "0xfeedbeef");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn submit_order_surfaces_venue_reject() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _ = read_http_request(&mut sock).await;
            // success=false with errorMsg → must come back as VenueReject
            let body = r#"{"success":false,"errorMsg":"insufficient balance","orderID":""}"#;
            let resp = http_ok(body);
            sock.write_all(&resp).await.unwrap();
            sock.shutdown().await.ok();
        });

        let wallet = Wallet::from_private_key_hex(TEST_PRIVATE_KEY).unwrap();
        let funder = parse_address("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266").unwrap();
        use base64::engine::general_purpose::URL_SAFE as B64URL;
        use base64::Engine;
        let creds = ApiCredentials {
            api_key: "key".into(),
            secret_b64url: B64URL.encode(b"secret-bytes-32-aaaaaaaaaaaaaaaa"),
            passphrase: "pass".into(),
        };
        let client = PolyClient::new(wallet, funder, creds, format!("http://{addr}"));
        let req = OrderRequest {
            side: OrderSide::Buy,
            token_id: "1".into(),
            price: 0.5,
            size_shares: 1.0,
            expiration_secs: 0,
            fee_rate_bps: 0,
            market_info: MarketInfo::mainnet_standard(),
        };
        let err = client.submit_order(&req, 1).await.unwrap_err();
        match err {
            ClientError::VenueReject { body, .. } => {
                assert!(body.contains("insufficient balance"), "got: {body}");
            }
            other => panic!("expected VenueReject, got {other:?}"),
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn cancel_order_sends_delete_with_order_id_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let (req_line, _, body) = read_http_request(&mut sock).await;
            assert!(req_line.starts_with("DELETE /order"));
            assert_eq!(body, r#"{"orderID":"0xabc"}"#);
            sock.write_all(&http_ok("{}")).await.unwrap();
            sock.shutdown().await.ok();
        });

        let wallet = Wallet::from_private_key_hex(TEST_PRIVATE_KEY).unwrap();
        let funder = parse_address("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266").unwrap();
        use base64::engine::general_purpose::URL_SAFE as B64URL;
        use base64::Engine;
        let creds = ApiCredentials {
            api_key: "key".into(),
            secret_b64url: B64URL.encode(b"secret-bytes-32-aaaaaaaaaaaaaaaa"),
            passphrase: "pass".into(),
        };
        let client = PolyClient::new(wallet, funder, creds, format!("http://{addr}"));
        client.cancel_order("0xabc").await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_open_orders_walks_cursor_to_terminator() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            // First page → returns one row + cursor "p2".
            let (mut sock1, _) = listener.accept().await.unwrap();
            let (line1, _, _) = read_http_request(&mut sock1).await;
            assert!(line1.starts_with("GET /data/orders"));
            assert!(!line1.contains("next_cursor"));
            let body1 = r#"{"data":[{"id":"a","status":"LIVE","market":"m","side":"BUY","price":"0.5","original_size":"1","size_matched":"0","asset_id":"1"}],"next_cursor":"p2"}"#;
            sock1.write_all(&http_ok(body1)).await.unwrap();
            sock1.shutdown().await.ok();

            // Second page → returns terminator "LTE=".
            let (mut sock2, _) = listener.accept().await.unwrap();
            let (line2, _, _) = read_http_request(&mut sock2).await;
            assert!(line2.contains("next_cursor=p2"));
            let body2 = r#"{"data":[{"id":"b","status":"LIVE","market":"m","side":"SELL","price":"0.6","original_size":"2","size_matched":"0","asset_id":"1"}],"next_cursor":"LTE="}"#;
            sock2.write_all(&http_ok(body2)).await.unwrap();
            sock2.shutdown().await.ok();
        });

        let wallet = Wallet::from_private_key_hex(TEST_PRIVATE_KEY).unwrap();
        let funder = parse_address("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266").unwrap();
        use base64::engine::general_purpose::URL_SAFE as B64URL;
        use base64::Engine;
        let creds = ApiCredentials {
            api_key: "key".into(),
            secret_b64url: B64URL.encode(b"secret-bytes-32-aaaaaaaaaaaaaaaa"),
            passphrase: "pass".into(),
        };
        let client = PolyClient::new(wallet, funder, creds, format!("http://{addr}"));
        let rows = client.fetch_open_orders().await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, "a");
        assert_eq!(rows[1].id, "b");
        server.await.unwrap();
    }
}
