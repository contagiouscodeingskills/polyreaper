//! State persistence — bankroll + position snapshots written as NDJSON.
//!
//! Goal: survive a bot crash or restart without losing accounting state.
//! In paper mode v1 we persist for audit and bankroll continuity:
//!   - `bankroll_usd` (critical — restored on boot)
//!   - open positions snapshot (audit-only in paper v1; not restored
//!     because mid-flight positions are hard to reconcile against poly
//!     market state on restart — see Phase 7 for the live-mode answer)
//!   - cumulative notional per market (audit-only, resets on boot)
//!
//! Format: one NDJSON line per mutation. Latest valid line on reload
//! wins. Crash-safe (append-only; partial last line is ignored on parse
//! failure).
//!
//! File location: stable path `data/bot_state.ndjson` (NOT per-session),
//! so a fresh session inherits the bankroll across restarts.
//!
//! ## Restart accounting caveat
//!
//! Because positions are NOT restored, the bankroll read off the
//! snapshot is the "cash post-fill, pre-settle" value: cost has
//! already been deducted, but the proceeds from open positions have
//! NOT yet been credited (settle would have done that). On restart,
//! `PositionStore` boots empty, so the bot will never credit those
//! proceeds back — the open positions just vanish from the bot's
//! view. Concretely:
//!
//! - If the bot crashes mid-position, the restored bankroll
//!   under-reports true equity by `Σ shares × current_mid` of open
//!   positions.
//! - Edge-scaled sizing (`bankroll_pct_per_edge`) then sizes new
//!   trades against this depressed bankroll — strictly conservative,
//!   not dangerous, but the bot's books will look "leaky" until the
//!   crashed-on positions resolve and credit the venue (in live mode)
//!   or are written off (paper).
//!
//! In paper mode the 5-minute market window makes this a narrow gap;
//! the bot is unlikely to crash *during* a position. Live mode will
//! fix this by reconciling open orders + positions from the CLOB on
//! boot (Phase 7).

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use market_registry::MarketId;
use serde::{Deserialize, Serialize};

use crate::position::Position;

pub const SCHEMA_VERSION: u32 = 1;

/// A single state snapshot written on every mutation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateSnapshot {
    pub schema_version: u32,
    pub saved_at_local_ts_ns: String,
    pub session_id: String,
    pub bot_version: String,

    /// USDC bankroll. Restored on boot.
    pub bankroll_usd: f64,
    /// Total realised P&L this session (sum across all settled markets).
    pub total_realised_pnl_usd: f64,

    /// Open positions snapshot. Audit-only in paper v1.
    pub open_positions: Vec<Position>,
    /// Per-market cumulative notional fired (for the cap). Audit-only.
    pub cumulative_notional_per_market: Vec<MarketNotional>,
    /// Per-market realised P&L. Audit-only.
    pub realised_pnl_per_market: Vec<MarketPnl>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketNotional {
    pub market_id: String,
    pub notional_usd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketPnl {
    pub market_id: String,
    pub pnl_usd: f64,
}

/// Writes state snapshots — flush per write so crashes don't lose
/// data. Volume is low (one per fill + one per settle, ~10-100/min).
pub struct StatePersister {
    writer: BufWriter<File>,
    path: PathBuf,
}

impl StatePersister {
    pub fn open(path: PathBuf) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn write(&mut self, snap: &StateSnapshot) -> std::io::Result<()> {
        let line = serde_json::to_string(snap)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        self.writer.write_all(line.as_bytes())?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()
    }
}

/// Restore the latest valid snapshot from a state file. Returns `None`
/// if the file doesn't exist; returns an error only for I/O problems.
/// Partial/corrupt last lines are silently skipped — we walk backward
/// from EOF and return the latest parseable line.
pub fn load_latest(path: &Path) -> std::io::Result<Option<StateSnapshot>> {
    if !path.exists() {
        return Ok(None);
    }
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut latest: Option<StateSnapshot> = None;
    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue, // skip partial reads
        };
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<StateSnapshot>(&line) {
            Ok(snap) if snap.schema_version == SCHEMA_VERSION => {
                latest = Some(snap);
            }
            _ => {
                // Skip malformed or older-schema lines.
            }
        }
    }
    Ok(latest)
}

/// Build a snapshot from current bot state. Tuple `(market_id, value)`
/// vectors are sorted for determinism.
pub fn snapshot(
    session_id: &str,
    bot_version: &str,
    bankroll_usd: f64,
    total_realised_pnl_usd: f64,
    open_positions: Vec<Position>,
    cumulative_notional: &HashMap<MarketId, f64>,
    realised_pnl_per_market: &HashMap<MarketId, f64>,
) -> StateSnapshot {
    let mut notional: Vec<MarketNotional> = cumulative_notional
        .iter()
        .map(|(k, &v)| MarketNotional {
            market_id: k.as_str().to_string(),
            notional_usd: v,
        })
        .collect();
    notional.sort_by(|a, b| a.market_id.cmp(&b.market_id));

    let mut pnl: Vec<MarketPnl> = realised_pnl_per_market
        .iter()
        .map(|(k, &v)| MarketPnl {
            market_id: k.as_str().to_string(),
            pnl_usd: v,
        })
        .collect();
    pnl.sort_by(|a, b| a.market_id.cmp(&b.market_id));

    StateSnapshot {
        schema_version: SCHEMA_VERSION,
        saved_at_local_ts_ns: common::LocalTimestamp::now().as_nanos().to_string(),
        session_id: session_id.to_string(),
        bot_version: bot_version.to_string(),
        bankroll_usd,
        total_realised_pnl_usd,
        open_positions,
        cumulative_notional_per_market: notional,
        realised_pnl_per_market: pnl,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_registry::Outcome;

    fn tmp_path() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "polybot_state_test_{}_{}",
            std::process::id(),
            common::LocalTimestamp::now().as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("state.ndjson")
    }

    fn fake_snap(bankroll: f64) -> StateSnapshot {
        StateSnapshot {
            schema_version: SCHEMA_VERSION,
            saved_at_local_ts_ns: "1".into(),
            session_id: "s".into(),
            bot_version: "0.1.0".into(),
            bankroll_usd: bankroll,
            total_realised_pnl_usd: 0.0,
            open_positions: vec![],
            cumulative_notional_per_market: vec![],
            realised_pnl_per_market: vec![],
        }
    }

    #[test]
    fn load_missing_file_returns_none() {
        let p = tmp_path();
        let _ = std::fs::remove_file(&p);
        assert!(load_latest(&p).unwrap().is_none());
    }

    #[test]
    fn write_then_load_round_trips() {
        let p = tmp_path();
        let _ = std::fs::remove_file(&p);
        let mut sp = StatePersister::open(p.clone()).unwrap();
        sp.write(&fake_snap(1000.0)).unwrap();
        sp.write(&fake_snap(1234.56)).unwrap();
        drop(sp);
        let loaded = load_latest(&p).unwrap().expect("present");
        assert!((loaded.bankroll_usd - 1234.56).abs() < 1e-9); // latest wins
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn load_skips_malformed_lines() {
        let p = tmp_path();
        let _ = std::fs::remove_file(&p);
        // Write a good line, a malformed one, and another good one.
        let mut f = File::create(&p).unwrap();
        let good = serde_json::to_string(&fake_snap(100.0)).unwrap();
        writeln!(f, "{}", good).unwrap();
        writeln!(f, "{{this isn't json").unwrap();
        let newest = serde_json::to_string(&fake_snap(200.0)).unwrap();
        writeln!(f, "{}", newest).unwrap();
        drop(f);
        let loaded = load_latest(&p).unwrap().expect("present");
        assert!((loaded.bankroll_usd - 200.0).abs() < 1e-9);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn load_skips_old_schema() {
        let p = tmp_path();
        let _ = std::fs::remove_file(&p);
        let mut f = File::create(&p).unwrap();
        // Write a snapshot with schema_version=99 (future). Should be
        // skipped because we only restore matching schema.
        let mut future = fake_snap(999.0);
        future.schema_version = 99;
        let bad_line = serde_json::to_string(&future).unwrap();
        writeln!(f, "{}", bad_line).unwrap();
        // Write a good current-schema snapshot below.
        let good = serde_json::to_string(&fake_snap(42.0)).unwrap();
        writeln!(f, "{}", good).unwrap();
        drop(f);
        let loaded = load_latest(&p).unwrap().expect("present");
        assert!((loaded.bankroll_usd - 42.0).abs() < 1e-9);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn snapshot_serialises_with_open_positions_and_notional_maps() {
        let mut cn = HashMap::new();
        cn.insert(MarketId::new("M1"), 1.5);
        cn.insert(MarketId::new("M2"), 2.5);
        let mut pn = HashMap::new();
        pn.insert(MarketId::new("M1"), 0.5);
        let pos = Position {
            market_id: MarketId::new("M1"),
            side: Outcome::Yes,
            cost_usd: 1.0,
            shares: 2.0,
            avg_price: 0.5,
        };
        let snap = snapshot("s", "0.1.0", 1000.0, 0.5, vec![pos], &cn, &pn);
        let s = serde_json::to_string(&snap).unwrap();
        let parsed: StateSnapshot = serde_json::from_str(&s).unwrap();
        assert_eq!(parsed.cumulative_notional_per_market.len(), 2);
        // Sorted by market_id for determinism.
        assert_eq!(parsed.cumulative_notional_per_market[0].market_id, "M1");
        assert_eq!(parsed.open_positions.len(), 1);
        assert_eq!(parsed.realised_pnl_per_market.len(), 1);
    }
}
