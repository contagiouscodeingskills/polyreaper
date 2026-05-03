# PolyBot — Unsolved Problems & Risks

> **This is the single source of truth for unresolved problems, risks, TODOs,
> and known issues. New issues should be added here instead of creating
> scattered TODO/risk files.**

Last consolidated: 2026-05-03.

## Severity & status definitions

**Severity**
- `BLOCKER` — cannot go live with this open.
- `HIGH` — serious; must be addressed before scale or before a phase gate.
- `MEDIUM` — should fix before live; not currently causing damage.
- `LOW` — nice to have; informational.

**Status**
- `OPEN` — known and not yet addressed.
- `IN PROGRESS` — being worked on now.
- `FIXED` — resolved; kept here briefly for audit before archival.
- `ACCEPTED` — known and explicitly accepted at current phase.
- `NEEDS VERIFICATION` — code may be correct but lacks end-to-end / live verification.

**Blocks live trading** is YES if going live without addressing the item exposes real money to risk we haven't characterised, OR if the item is a missing component required by the live path.

---

## 1. Recorder / Data Capture

### REC-001 · Recorder restart splits in-flight 5-minute Polymarket market across sessions
- **Area**: Recorder / data continuity
- **Severity**: HIGH
- **Status**: OPEN
- **Description**: When the recorder restarts mid-market, the active 5-minute Polymarket market's data is split across two session directories. Cross-session reconstruction by the replayer's `open_base_dir` exists in code but has not been exercised end-to-end on a real boundary case.
- **Evidence / source**: User-listed; `crates/replayer/src/discovery.rs` does multi-session walk; no test.
- **How to detect**: Identify a recorder-restart boundary; replay the merged stream and verify a market spanning the boundary reads cleanly.
- **Proposed fix**: Add a smoke test in the Phase 2 analysis pipeline that walks at least one session boundary.
- **Blocks live trading?** NO (live bot consumes the live stream; this is an offline-only concern).
- **Next action**: Smoke test added to Phase 2 deliverable.

### REC-002 · 7-day soak test not yet run on finalised recorder
- **Area**: Recorder
- **Severity**: HIGH
- **Status**: OPEN
- **Description**: Post-finalisation recorder (commits up to `a364ab8`) has not run uninterrupted for 7 days. Unknowns: long-tail memory, chrony drift over multi-day windows, cumulative reconnect counters, gradual disk growth shape.
- **Evidence / source**: User-listed; current live session `session_20260503T000248Z` is at ~9.7h (per `manifests/sessions.toml`).
- **How to detect**: Watch `_health.ndjson` over 7 days for stable msg-rates, no monotonic memory growth (RSS via `ps`), no gaps > 60s.
- **Proposed fix**: Continuous capture for 7 days; daily review of last health snapshot.
- **Blocks live trading?** YES (production must prove it can run unattended).
- **Next action**: Wait. Tracked alongside the holdout-promotion gate (ANL-007).

### REC-003 · Feed-stall warning log never observed in a real stall
- **Area**: Recorder / health
- **Severity**: MEDIUM
- **Status**: NEEDS VERIFICATION
- **Description**: Stall detection (>60s without messages flips `stalled=true` in health) was added in phase 4 but has not been observed firing because no real stall has happened. Bug in this code path would only surface during a real outage.
- **Evidence / source**: User-listed; phase 4 commit `c999681`.
- **How to detect**: Inject a controlled stall on a non-production feed; verify health flips `stalled=true` and a warning log fires.
- **Proposed fix**: One-off controlled stall test in dev.
- **Blocks live trading?** NO.
- **Next action**: 5-minute test on a dev branch / dev VPS.

### REC-004 · Binance subscribe-failure handling is silent
- **Area**: Recorder / Binance feed
- **Severity**: MEDIUM
- **Status**: OPEN
- **Description**: Phase 6 dropped the subscribe-ack from health output. The recorder should still loudly WARN if a Binance subscribe ack indicates failure (currently silent).
- **Evidence / source**: User-listed; commit `76ec105` "phase 6 follow-up: drop binance subscribe ack".
- **How to detect**: Read `crates/binance_feed/src/conn.rs` and `frame.rs`; trace the subscribe-ack path.
- **Proposed fix**: `tracing::warn!` on a failing subscribe ack; keep it out of per-snapshot health, in the log stream.
- **Blocks live trading?** NO.
- **Next action**: Add WARN path in Binance feed crate.

### REC-005 · Polymarket WS endpoint not officially confirmed
- **Area**: Recorder / Polymarket feed
- **Severity**: LOW
- **Status**: NEEDS VERIFICATION
- **Description**: `wss://ws-subscriptions-clob.polymarket.com/ws/market` is a best guess. Active capture works, but no official documentation citation exists.
- **Evidence / source**: `configs/recorder.toml:32` and `crates/config/src/lib.rs:91` both have `// TODO verify endpoint` comments.
- **How to detect**: Active capture is live — endpoint is functioning. Risk is silent change.
- **Proposed fix**: Find official documentation; remove TODOs or replace with a citation.
- **Blocks live trading?** NO.
- **Next action**: Polymarket docs / forum search.

### REC-006 · Polymarket subscription resync after reconnect not verified
- **Area**: Recorder / Polymarket feed
- **Severity**: MEDIUM
- **Status**: NEEDS VERIFICATION
- **Description**: On WS reconnect, Polymarket feed re-subscribes with the current registry. We have not verified that no events are dropped or duplicated across the reconnect window, especially when the registry is mid-update.
- **Evidence / source**: Architecture doc §1.
- **How to detect**: Sequence check on `wire_batch_id` across a known reconnect boundary.
- **Proposed fix**: One reconnect test in dev; check for `wire_batch_id` gap across the boundary.
- **Blocks live trading?** NO directly (signal can tolerate occasional missed events; data-quality risk gates trading).
- **Next action**: Bundle into 7-day soak test analysis (REC-002).

### REC-007 · WS reconnect backoff cap not verified to bound under storm
- **Area**: Recorder
- **Severity**: LOW
- **Status**: NEEDS VERIFICATION
- **Description**: Architecture doc flags "verify the cap doesn't grow unbounded". Per-feed `max_ms` is 30000; backoff should saturate. Untested in storm conditions.
- **Evidence / source**: `docs/BOT_ARCHITECTURE_AND_BUILD_PLAN.md:72`.
- **How to detect**: Block a feed's network for ~5 min; verify reconnect interval saturates.
- **Proposed fix**: One-shot test; record outcome.
- **Blocks live trading?** NO.
- **Next action**: Bundle into REC-003 controlled stall test.

### REC-008 · chrony loss degrades `local_ts_ns` precision silently at recorder
- **Area**: Recorder / time
- **Severity**: MEDIUM
- **Status**: ACCEPTED at recorder; must be enforced in Risk Engine
- **Description**: If chrony goes unavailable, the recorder still writes events but `local_ts_ns` precision degrades. Health output reports `chrony.available = false` but the recorder doesn't refuse to write.
- **Evidence / source**: phase 4 health enrichment.
- **How to detect**: Health output `chrony.available = false`.
- **Proposed fix**: Recorder behaviour is correct (don't lose data). Risk Engine in live mode treats `chrony.available = false` as halt-trading.
- **Blocks live trading?** YES if Risk Engine doesn't gate on it (RSK-005).
- **Next action**: Encode rule in Risk Engine when built.

---

## 2. Storage / Disk / Compression

### STO-001 · disk_guard COMPRESS action not tested under real disk pressure
- **Area**: Storage / ops
- **Severity**: HIGH
- **Status**: OPEN
- **Description**: `scripts/disk_guard.sh` COMPRESS action has not fired under actual disk-pressure conditions.
- **Evidence / source**: User-listed.
- **How to detect**: Fill the data partition to threshold in dev; observe behaviour.
- **Proposed fix**: Controlled disk-pressure test before treating disk_guard as production-grade.
- **Blocks live trading?** NO (recorder concern).
- **Next action**: Schedule controlled fill test.

### STO-002 · disk_guard STOP action not tested under real disk pressure
- **Area**: Storage / ops
- **Severity**: HIGH
- **Status**: OPEN
- **Description**: STOP action (abort writes when disk truly full) has not been exercised under real ENOSPC.
- **Evidence / source**: User-listed.
- **How to detect**: Fill disk in dev; verify recorder stops without data corruption.
- **Proposed fix**: Bundle with STO-001.
- **Blocks live trading?** NO (recorder concern).
- **Next action**: Bundle with STO-001.

### STO-003 · disk_guard COMPRESS_AGE_HOURS boundary not tested at 23h59m vs 24h01m
- **Area**: Storage
- **Severity**: MEDIUM
- **Status**: OPEN
- **Description**: Boundary condition for "compress files older than N hours" untested.
- **Evidence / source**: User-listed.
- **How to detect**: Test files at +/- 1 minute around the boundary.
- **Proposed fix**: Quick shell test against `find -mmin` in `disk_guard.sh`.
- **Blocks live trading?** NO.
- **Next action**: Add to STO-001 test plan.

### STO-004 · Per-session gzip rotation at session-size threshold not implemented
- **Area**: Storage
- **Severity**: MEDIUM
- **Status**: OPEN (deferred)
- **Description**: Discussed in `docs/NEXT_SESSION.md:87`: "Trigger when a single session starts pushing past, say, 50 GB or `/` approaches half full." `disk_guard.sh` handles disk-level pressure; per-session rotation-into-gz at a threshold is not a thing.
- **Evidence / source**: `docs/NEXT_SESSION.md:87`.
- **How to detect**: Watch session size during long captures.
- **Proposed fix**: Either rely on disk_guard, or add explicit per-session compression at a threshold.
- **Blocks live trading?** NO.
- **Next action**: Re-evaluate when sessions exceed ~30 GB.

### STO-005 · Storage rotation aligned to session start, not wall clock
- **Area**: Storage
- **Severity**: LOW
- **Status**: ACCEPTED
- **Description**: Rotation buckets measured from session start, not wall clock. Deterministic and replay-friendly; acceptable.
- **Evidence / source**: `docs/TECH_DEBT.md` §1.
- **Proposed fix**: Re-evaluate when operationally painful.
- **Blocks live trading?** NO.
- **Next action**: Maintain.

### STO-006 · Storage only supports text payloads
- **Area**: Storage
- **Severity**: LOW
- **Status**: ACCEPTED
- **Description**: `&str` only; no binary frame path.
- **Evidence / source**: `docs/TECH_DEBT.md` §2.
- **Proposed fix**: Add binary write path on first real binary frame observed.
- **Blocks live trading?** NO.
- **Next action**: Maintain.

### STO-007 · Shared `Arc<Mutex<Store>>` may contend at scale
- **Area**: Storage
- **Severity**: LOW
- **Status**: ACCEPTED
- **Description**: Single mutex; per-feed writers / channels / sharding deferred. `store_p99_us` is well under 1 ms target per phase 5 instrumentation.
- **Evidence / source**: `docs/TECH_DEBT.md` §3; `docs/NEXT_SESSION.md`.
- **Proposed fix**: Re-evaluate if `store_p99_us` approaches alarm threshold.
- **Blocks live trading?** NO.
- **Next action**: Maintain.

### STO-008 · Recorder uses `JoinHandle::abort()` for shutdown, not cooperative cancellation
- **Area**: Storage / shutdown
- **Severity**: LOW
- **Status**: ACCEPTED
- **Description**: Currently safe because `process_text` has no `.await` points. Cooperative shutdown is ergonomic polish, not a correctness gap.
- **Evidence / source**: `docs/TECH_DEBT.md` §4.
- **Proposed fix**: Replace with `CancellationToken` / `tokio::sync::watch` if `process_text` ever grows `.await` points.
- **Blocks live trading?** NO.
- **Next action**: Maintain.

---

## 3. Replayer / Reader

### REP-001 · Replayer integrity over a fully-gzipped session not live-verified
- **Area**: Replayer
- **Severity**: MEDIUM
- **Status**: OPEN
- **Description**: `integrity` subcommand transparently handles `.ndjson.gz`, but a full integrity pass on a real gzipped session has not been demonstrated end-to-end.
- **Evidence / source**: User-listed.
- **How to detect**: Run `replayer integrity` on a session compressed by disk_guard or manually.
- **Proposed fix**: One-shot run; record verdict here.
- **Blocks live trading?** NO.
- **Next action**: After STO-001/002 leaves a gzipped session.

### REP-002 · Mixed gz + plain session not live-tested
- **Area**: Replayer
- **Severity**: MEDIUM
- **Status**: OPEN
- **Description**: A session can have some files gzipped and others plain. The replayer's behaviour on this hybrid is documented in design but unverified end-to-end.
- **Evidence / source**: User-listed.
- **How to detect**: Run `replayer dump` / `integrity` on a deliberately-mixed session.
- **Proposed fix**: One-shot test.
- **Blocks live trading?** NO.
- **Next action**: Bundle with REP-001.

### REP-003 · Multi-session integrity walk not live-verified
- **Area**: Replayer
- **Severity**: LOW
- **Status**: OPEN
- **Description**: `replayer integrity --root <base_dir>` walking many sessions hasn't been demonstrated.
- **Evidence / source**: User-listed.
- **How to detect**: Run multi-session integrity on the VPS data dir (read-only).
- **Proposed fix**: After per-session integrity verdicts populate the manifest (INT-001).
- **Blocks live trading?** NO.
- **Next action**: After INT-001 batch.

### REP-004 · Pacer latency offsets can produce local out-of-order
- **Area**: Replayer / Pacer
- **Severity**: LOW
- **Status**: ACCEPTED
- **Description**: When the Pacer applies per-venue latency offsets after merge, events can locally cross. Documented in architecture doc.
- **Evidence / source**: `docs/BOT_ARCHITECTURE_AND_BUILD_PLAN.md`.
- **Proposed fix**: Run with offsets = 0 unless explicitly testing latency. Encode rule in `lag_oos.py` when built.
- **Blocks live trading?** NO (replay-only concern).
- **Next action**: Add comment in `lag_oos.py`.

---

## 4. Data Integrity

### INT-001 · `integrity_status = "unknown"` on every catalogued session
- **Area**: Integrity / OOS
- **Severity**: HIGH
- **Status**: OPEN
- **Description**: All 15 sessions in `manifests/sessions.toml` have `integrity_status = "unknown"`. No formal `replayer integrity` verdict has been recorded against this manifest.
- **Evidence / source**: `manifests/sessions.toml`.
- **How to detect**: Read manifest.
- **Proposed fix**: Run `replayer integrity --root <session>` on each train session and the holdout candidate; update the manifest with PASS/WARN/FAIL.
- **Blocks live trading?** NO directly; blocks OOS publication.
- **Next action**: Run integrity on train sessions before any OOS run.

### INT-002 · Old recovered disk-full session needs v2 integrity confirmation
- **Area**: Integrity
- **Severity**: MEDIUM
- **Status**: OPEN
- **Description**: `session_20260427T100216Z` has a truncated last `_health.ndjson` line. `docs/NEXT_SESSION.md` mentions an early integrity run was clean, but per project policy that is informational only — not a formal verdict against this manifest.
- **Evidence / source**: `manifests/sessions.toml`; `docs/NEXT_SESSION.md`.
- **How to detect**: `replayer integrity --root .../session_20260427T100216Z` (read-only).
- **Proposed fix**: Folded into INT-001 batch.
- **Blocks live trading?** NO.
- **Next action**: Bundle with INT-001.

### INT-003 · Polymarket per-asset `timestamp_ms` non-monotonic check needs focused tests
- **Area**: Integrity
- **Severity**: MEDIUM
- **Status**: NEEDS VERIFICATION (check is implemented; tests are thin)
- **Description**: Implementation exists at `crates/replayer/src/integrity.rs:715` (`check_polymarket_per_asset_ts`). What's missing is targeted unit tests with synthetic regressions.
- **Evidence / source**: User-listed; `integrity.rs:143–157, 715–745`.
- **How to detect**: Audit existing tests; add cases.
- **Proposed fix**: Synthetic test data with a known timestamp regression; assert the counter increments.
- **Blocks live trading?** NO directly.
- **Next action**: Add focused tests in `integrity.rs`.

### INT-004 · Polymarket consecutive-hash duplicate detection needs focused tests
- **Area**: Integrity
- **Severity**: MEDIUM
- **Status**: NEEDS VERIFICATION (check is implemented; tests are thin)
- **Description**: Implementation exists at `crates/replayer/src/integrity.rs:743–748`. Tests for synthetic duplicate emissions are thin.
- **Evidence / source**: User-listed; `integrity.rs:150–157, 743`.
- **How to detect**: Inspect existing tests; add coverage.
- **Proposed fix**: Add unit test with two identical consecutive hashes for the same `asset_id`; assert the counter increments.
- **Blocks live trading?** NO directly.
- **Next action**: Bundle with INT-003.

### INT-005 · Safe replay cutoff behaviour on damaged sessions not verified end-to-end
- **Area**: Integrity
- **Severity**: MEDIUM
- **Status**: NEEDS VERIFICATION (logic is implemented; live test pending)
- **Description**: `safe_replay_cutoff_ns` is implemented (`integrity.rs:75, 663, 1288`). End-to-end behaviour on a real damaged session (e.g., `session_20260427T100216Z` with truncated last line) hasn't been demonstrated.
- **Evidence / source**: `integrity.rs`; user-listed.
- **How to detect**: Run integrity on the truncated session; inspect the cutoff field.
- **Proposed fix**: Bundle with INT-002.
- **Blocks live trading?** NO directly.
- **Next action**: Bundle with INT-002.

### INT-006 · Book-level invariants not asserted in tests
- **Area**: Integrity / book reconstruction
- **Severity**: HIGH (when signal lands)
- **Status**: OPEN
- **Description**: Architecture doc flags: "book reconstruction bugs (e.g., wrong tick size) aren't caught by integrity — book-level invariant tests are needed". No invariant tests on `BinanceBook` / `PolymarketMarketBook`.
- **Evidence / source**: `docs/BOT_ARCHITECTURE_AND_BUILD_PLAN.md`; `crates/replayer/src/book/`.
- **How to detect**: Audit `crates/replayer/src/book/`.
- **Proposed fix**: Property-based / unit invariants: bids monotonically decreasing in price, asks increasing, no crossed book post-snapshot, depth aggregation matches diffs.
- **Blocks live trading?** YES if signal depends on book state (it does, for both microstructure and PM book-mid).
- **Next action**: Defer until Phase 4.5 (signal engine consumes the book).

---

## 5. Resolution / Market Outcomes

### RES-001 · Resolution sweeper behaviour under ENOSPC not empirically tested
- **Area**: Resolution sweeper / storage
- **Severity**: MEDIUM
- **Status**: OPEN
- **Description**: `_resolutions.ndjson` write path under disk-full has not been tested. Could lose resolution records that downstream P&L depends on.
- **Evidence / source**: User-listed.
- **How to detect**: Controlled ENOSPC during sweeper write.
- **Proposed fix**: Bundle with STO-001/002.
- **Blocks live trading?** NO directly; affects post-trade settlement accuracy.
- **Next action**: Bundle with STO-001/002.

### RES-002 · Resolution-source mismatch (Chainlink, not Binance) is research input
- **Area**: Resolution
- **Severity**: LOW
- **Status**: ACCEPTED
- **Description**: Polymarket BTC 5m markets resolve via Chainlink Data Streams (Mercury, paid), not Binance Spot. Recorder captures both as-is and does NOT align resolution sources. Cross-source delta is itself a candidate signal.
- **Evidence / source**: `docs/decisions.md` §8.
- **Proposed fix**: None.
- **Blocks live trading?** NO.
- **Next action**: Maintain.

### RES-003 · Chainlink Data Streams (Mercury) integration not built
- **Area**: Resolution / Chainlink
- **Severity**: LOW
- **Status**: OPEN (deferred)
- **Description**: On-chain `AggregatorV3` was disabled in priority 2 (wrong oracle for 5m markets). Mercury is the right path; needs paid API key.
- **Evidence / source**: `docs/NEXT_SESSION.md:84`.
- **Proposed fix**: Subscribe to Mercury when justified.
- **Blocks live trading?** NO (lag signal doesn't depend on Chainlink as input).
- **Next action**: Deferred.

### RES-004 · Gamma API marked deprecated by Polymarket
- **Area**: Market discovery / Resolution
- **Severity**: LOW
- **Status**: ACCEPTED, monitor
- **Description**: Gamma `/events` and `/markets` responses include `deprecation: true` HTTP header. Endpoints alive and serving real data as of 2026-04-25.
- **Evidence / source**: `docs/TECH_DEBT.md` §5.
- **Proposed fix**: Retarget when Polymarket publishes a migration target.
- **Triggers to revisit**: header flips to 410/4xx, OR Polymarket publishes a replacement, OR discovery returns empty/stale.
- **Blocks live trading?** NO directly; would break discovery and resolution sweeper.
- **Next action**: Monitor.

---

## 6. Analysis / OOS Validation

### ANL-001 · Analysis scripts still need organisation
- **Area**: Analysis
- **Severity**: MEDIUM
- **Status**: OPEN
- **Description**: 10 untracked Python/shell tools in `scripts/` (lag_analysis × 3 versions, lag_run_confirm, casestudy_analysis, integrity_supplementary, scan_polymarket_swings, vps_health_check, vps_polymarket_*, migrate_rawevent). Duplication, hard-coded `SESSION_DIR`, no versioning.
- **Evidence / source**: `git status`; user-listed.
- **Proposed fix**: Phase 2 deliverable — versioned analysis pipeline; decide which to keep / delete / merge.
- **Blocks live trading?** NO directly.
- **Next action**: After Phase 3 OOS verdict; defer cleanup until then.

### ANL-002 · OOS validation not yet run
- **Area**: Analysis / Phase 3 gate
- **Severity**: BLOCKER (for Phase 4+)
- **Status**: OPEN
- **Description**: Phase 3 OOS validation is defined (`manifests/sessions.toml`, `configs/lag_oos_baseline.toml`) but no script exists yet. `scripts/lag_oos.py` is the next deliverable.
- **Evidence / source**: manifest commit `c6bfe1e`; no `scripts/lag_oos.py`.
- **Proposed fix**: Build OOS harness once holdout matures.
- **Blocks live trading?** YES.
- **Next action**: Wait for holdout to reach 48h clean (ANL-007); then build harness.

### ANL-003 · Holdout split must remain untouched
- **Area**: Analysis / discipline
- **Severity**: HIGH
- **Status**: ACCEPTED (rule)
- **Description**: Holdout sessions must not be inspected for signal results before frozen-params evaluation. Currently `holdout = []`; once promoted, only `lag_oos.py` with frozen baseline params may run against them.
- **Evidence / source**: `manifests/sessions.toml`; `configs/lag_oos_baseline.toml`.
- **Proposed fix**: Maintain. When `lag_oos.py` lands, audit any new analysis script for `SESSION_DIR` matching the holdout session.
- **Blocks live trading?** NO directly; protects OOS validity.
- **Next action**: Maintain.

### ANL-004 · No parameter tuning permitted on holdout
- **Area**: Analysis / discipline
- **Severity**: HIGH
- **Status**: ACCEPTED (rule)
- **Description**: `configs/lag_oos_baseline.toml` encodes `sweep_permitted = false` and `holdout_tuning_permitted = false`. Holdout is run once with frozen baseline; result is accepted.
- **Evidence / source**: `configs/lag_oos_baseline.toml`.
- **Proposed fix**: Encode rule check in `lag_oos.py` when built (refuse to run if either flag is true).
- **Blocks live trading?** NO directly.
- **Next action**: Encode in `lag_oos.py`.

### ANL-005 · Baseline params are baseline, not optimal
- **Area**: Analysis
- **Severity**: LOW
- **Status**: ACCEPTED (informational)
- **Description**: Per `configs/lag_oos_baseline.toml`: values inherited from prior discovery, not claimed-optimal.
- **Evidence / source**: `configs/lag_oos_baseline.toml`.
- **Proposed fix**: None.
- **Blocks live trading?** NO.
- **Next action**: None.

### ANL-006 · No proven tradable PnL/EV yet
- **Area**: Analysis / strategy
- **Severity**: BLOCKER (for live)
- **Status**: OPEN
- **Description**: The lag signal has not been validated through (a) OOS, then (b) execution simulator, then (c) shadow trading. Until all three pass, no economic edge is proven.
- **Evidence / source**: Phases 3–5 not done.
- **Proposed fix**: Execute Phases 3–5 in order.
- **Blocks live trading?** YES.
- **Next action**: Phase 3.

### ANL-007 · Holdout session not yet promoted or OOS-verified
- **Area**: Analysis / Phase 3 gate
- **Severity**: HIGH
- **Status**: OPEN
- **Description**: `session_20260503T000248Z` is the holdout candidate; ~9.7h captured at last inventory. Promotion gates: ≥48h clean, integrity PASS, write_failures = 0, no ENOSPC, no stalls, chrony OK.
- **Evidence / source**: `manifests/sessions.toml`.
- **Proposed fix**: Re-poll VPS metadata at >48h mark; if gates pass, promote in manifest.
- **Blocks live trading?** YES (no holdout = no OOS = no live).
- **Next action**: Re-check at ≥2026-05-05 00:02 UTC.

---

## 7. Signal Engine / Bot Brain

### SIG-001 · Signal Engine not built
- **Area**: Signal engine
- **Severity**: BLOCKER (live)
- **Status**: OPEN
- **Description**: Architecture §4 — features/brain/fair-value/signal-detector/lag-reaction-model/signal-logger.
- **Proposed fix**: Phase 4.5 deliverable.
- **Blocks live trading?** YES.
- **Next action**: After Phase 3 OOS verdict.

### SIG-002 · Replay-determinism rule unenforced
- **Area**: Signal engine / discipline
- **Severity**: HIGH
- **Status**: OPEN
- **Description**: Architecture design rule 1: no wall-clock dependencies in signal logic. No lint or runtime check enforces this. When signal engine lands, an accidental `Instant::now()` would silently break replay determinism.
- **Evidence / source**: `docs/BOT_ARCHITECTURE_AND_BUILD_PLAN.md`.
- **Proposed fix**: Clippy lint or pre-commit grep against `Instant::now`, `SystemTime::now`, `chrono::Utc::now` inside signal-engine modules.
- **Blocks live trading?** NO directly; corrupts shadow-vs-live comparison.
- **Next action**: Add the lint at the start of SIG-001.

### SIG-003 · Market lifecycle tracker not built
- **Area**: Signal engine
- **Severity**: HIGH
- **Status**: OPEN
- **Description**: Architecture proposed addition: explicit Polymarket state machine (open → trading → freeze → resolution). Strategy needs `t_to_resolution` and `tradeable` boolean.
- **Evidence / source**: `docs/BOT_ARCHITECTURE_AND_BUILD_PLAN.md` §4.
- **Proposed fix**: Build alongside SIG-001.
- **Blocks live trading?** YES (without it, strategy can't honour market lifecycle).
- **Next action**: Phase 4.5.

### SIG-004 · Feature store not built
- **Area**: Signal engine
- **Severity**: MEDIUM
- **Status**: OPEN
- **Description**: Architecture proposed addition: in-memory snapshot of derived features (mid, spread, imbalance, smoothed BTC direction).
- **Proposed fix**: Build alongside SIG-001.
- **Blocks live trading?** NO directly.
- **Next action**: Phase 4.5.

---

## 8. Execution Simulator

### SIM-001 · Execution Simulator not built
- **Area**: Simulator
- **Severity**: BLOCKER (live)
- **Status**: OPEN
- **Description**: Architecture §5 — fill model, P&L simulator, fee schedule, size/liquidity model, delay simulation.
- **Proposed fix**: Phase 4 deliverable.
- **Blocks live trading?** YES.
- **Next action**: After Phase 3 OOS verdict.

### SIM-002 · Fill probability model not built or tested
- **Area**: Simulator
- **Severity**: BLOCKER (live)
- **Status**: OPEN
- **Description**: Part of SIM-001. Without a fill-probability model, signals can't be turned into trades.
- **Proposed fix**: Phase 4.
- **Blocks live trading?** YES.
- **Next action**: Folded into SIM-001.

### SIM-003 · Spread / slippage / liquidity not modelled
- **Area**: Simulator
- **Severity**: BLOCKER (live)
- **Status**: OPEN
- **Description**: Part of SIM-001.
- **Proposed fix**: Phase 4.
- **Blocks live trading?** YES.
- **Next action**: Folded into SIM-001.

### SIM-004 · Adverse-selection model not built
- **Area**: Simulator
- **Severity**: HIGH (live)
- **Status**: OPEN
- **Description**: Architecture proposed addition. Important specifically because we target 2–4s lag on a thin Polymarket book.
- **Evidence / source**: `docs/BOT_ARCHITECTURE_AND_BUILD_PLAN.md`.
- **Proposed fix**: Add to Phase 4 simulator.
- **Blocks live trading?** YES (sim P&L will mislead without it).
- **Next action**: Phase 4.

### SIM-005 · Polymarket fee schedule not encoded or verified current
- **Area**: Simulator
- **Severity**: HIGH (live)
- **Status**: OPEN
- **Description**: Phase 4 needs an accurate Polymarket fee schedule (taker/maker, USDC decimals, gas if applicable).
- **Proposed fix**: Document fees from Polymarket docs in `configs/polymarket_fees.toml`; cite source; encode in simulator.
- **Blocks live trading?** YES.
- **Next action**: Phase 4.

---

## 9. Shadow Trading

### SHD-001 · Shadow Trading System not built
- **Area**: Shadow
- **Severity**: BLOCKER (live)
- **Status**: OPEN
- **Description**: Phase 5 deliverable.
- **Blocks live trading?** YES (≥2 weeks of clean shadow required before live).
- **Next action**: After Phase 4.

### SHD-002 · Live-vs-sim divergence monitor not built
- **Area**: Shadow
- **Severity**: HIGH
- **Status**: OPEN
- **Description**: Architecture proposed addition. Re-runs bot on the day's recording; any signal divergence is a determinism bug.
- **Proposed fix**: Build alongside SHD-001.
- **Blocks live trading?** NO directly; protects determinism.
- **Next action**: Phase 5.

---

## 10. Live Execution

### LIV-001 · Polymarket order router not built
- **Severity**: BLOCKER (live)
- **Status**: OPEN
- **Description**: Phase 7 deliverable.
- **Blocks live trading?** YES.
- **Next action**: After Phase 6.

### LIV-002 · Wallet / signing module not built
- **Severity**: BLOCKER (live)
- **Status**: OPEN
- **Description**: EOA / proxy wallet, signs order payloads.
- **Blocks live trading?** YES.
- **Next action**: Phase 7.

### LIV-003 · Position store (durable source-of-truth) not built
- **Severity**: BLOCKER (live)
- **Status**: OPEN
- **Description**: Architecture proposed addition. Durable record of position, fills, realised P&L; reconciled with Polymarket's view.
- **Blocks live trading?** YES.
- **Next action**: Phase 7.

### LIV-004 · Order manager (lifecycle + idempotency) not built
- **Severity**: BLOCKER (live)
- **Status**: OPEN
- **Description**: Tracks orders intended → submitted → acked → (filled / rejected / canceled); idempotency keys per signal; recoverable on restart.
- **Blocks live trading?** YES.
- **Next action**: Phase 7.

### LIV-005 · User-data WS handler (real-time fills) not built
- **Severity**: HIGH
- **Status**: OPEN
- **Description**: Polymarket user-data stream gives real-time fill / cancel events; without it, order manager polls REST.
- **Blocks live trading?** NO; poll-only is operable, slower.
- **Next action**: Phase 7.

### LIV-006 · Latency-critical hot path not designed
- **Severity**: HIGH
- **Status**: OPEN
- **Description**: Hot path "signal fires → order on the wire" — no allocations, no blocking I/O, possibly separate runtime.
- **Blocks live trading?** NO directly; affects performance.
- **Next action**: Phase 7 design.

---

## 11. Risk Engine / Safety

### RSK-001 · Risk Engine not built
- **Severity**: BLOCKER (live)
- **Status**: OPEN
- **Description**: All four tiers — trade / market / account / data-quality.
- **Blocks live trading?** YES.
- **Next action**: Phase 6.

### RSK-002 · Trading kill switch not built
- **Severity**: BLOCKER (live)
- **Status**: OPEN
- **Description**: Halts new orders + (optionally) cancels all open. Triggered by account-level breach, manual SIGINT, data-quality red, integrity FAIL, P&L floor breach.
- **Blocks live trading?** YES.
- **Next action**: Phase 6.

### RSK-003 · Pre-trade simulation not built
- **Severity**: HIGH
- **Status**: OPEN
- **Description**: Architecture proposed addition. Run signal through fill model before submission; reject if expected slippage/cost makes it unprofitable.
- **Blocks live trading?** NO directly.
- **Next action**: Phase 6.

### RSK-004 · Risk Engine fail-open mode not designed
- **Severity**: HIGH
- **Status**: OPEN
- **Description**: Architecture rule: must default-deny on internal errors. Currently no impl exists; this is a design rule to encode.
- **Blocks live trading?** YES (must implement properly).
- **Next action**: Phase 6 design.

### RSK-005 · Data-quality halt rules not encoded
- **Severity**: HIGH
- **Status**: OPEN
- **Description**: Risk Engine should halt trading on: feed staleness, reconnect-rate spike, parse-failure spike, `chrony.available = false`, integrity FAIL on last session, Polymarket book depth < minimum.
- **Evidence / source**: Architecture §8; cross-references REC-008.
- **Proposed fix**: Encode halt thresholds when Risk Engine lands.
- **Blocks live trading?** YES.
- **Next action**: Phase 6.

---

## 12. Monitoring / Operations

### MON-001 · Trading dashboard not built
- **Severity**: BLOCKER (live)
- **Status**: OPEN
- **Description**: P&L, position, open orders, signal rate, fill rate, slippage vs sim, latency.
- **Blocks live trading?** YES.
- **Next action**: Phase 5/8.

### MON-002 · Trading alerts not built
- **Severity**: BLOCKER (live)
- **Status**: OPEN
- **Description**: Recorder-down, feed stalled > X, integrity FAIL, P&L floor near, kill-switch fired, wallet balance low, Polymarket API error rate spike.
- **Blocks live trading?** YES.
- **Next action**: Phase 5/8.

### MON-003 · Off-host alerting path not designed
- **Severity**: HIGH (live)
- **Status**: OPEN
- **Description**: All current monitoring is on the same VPS as the recorder/bot. Need an off-host alerter for "the VPS itself is down".
- **Blocks live trading?** YES at scale; live can start single-host with manual oversight.
- **Next action**: Phase 5/8.

### MON-004 · Audit log of trading decisions not built
- **Severity**: HIGH (live)
- **Status**: OPEN
- **Description**: Architecture proposed addition. Append-only log of every trade decision and config change.
- **Blocks live trading?** NO directly; required for "what was the bot doing at 03:14 UTC?" investigations.
- **Next action**: Phase 5/7.

### MON-005 · 78 files of pre-existing rustfmt drift
- **Severity**: LOW
- **Status**: OPEN
- **Description**: One `cargo fmt --workspace` commit clears it.
- **Evidence / source**: `docs/NEXT_SESSION.md:99`.
- **Blocks live trading?** NO.
- **Next action**: One-shot when convenient.

### MON-006 · 0-byte locked file `docs/sample_polymarket_census.txt`
- **Severity**: LOW
- **Status**: OPEN
- **Description**: Locked by something on the local Windows box; flagged as untracked.
- **Evidence / source**: `git status`; `docs/NEXT_SESSION.md:101`.
- **Proposed fix**: `rm docs/sample_polymarket_census.txt` once the lock releases.
- **Blocks live trading?** NO.
- **Next action**: When lock releases.

### MON-007 · Recorder git rev not in `_session_meta.json`
- **Severity**: MEDIUM
- **Status**: OPEN
- **Description**: Schema has static `recorder_version: "0.1.0"` but no `git_rev` field. Future sessions can't self-identify their build.
- **Evidence / source**: `manifests/sessions.toml` open-gaps; `_session_meta.json` content.
- **Proposed fix**: Add `git_rev` (and `git_dirty`) to `apps/recorder/src/meta.rs` build-time. Use `vergen` or similar.
- **Blocks live trading?** NO directly; affects audit.
- **Next action**: Add to next recorder commit.

### MON-008 · Coinbase L2 channel not captured
- **Severity**: LOW
- **Status**: OPEN (deferred)
- **Description**: Currently only `market_trades`. L2 would enable cross-venue book/BBO comparison.
- **Evidence / source**: `docs/NEXT_SESSION.md:80`.
- **Proposed fix**: New stream subscription + decoder + book module.
- **Blocks live trading?** NO (current strategy doesn't depend on it).
- **Next action**: Deferred until cross-venue analysis is on the roadmap.

---

## 13. Open Questions

(Originally `docs/BOT_ARCHITECTURE_AND_BUILD_PLAN.md` §"Open questions". Consolidated here.)

### OPN-001 · Polymarket API rate limits & end-to-end ack latency
- **Status**: OPEN
- Measure during shadow.

### OPN-002 · Wallet posture: hot on VPS / hardware-remote-sign / cloud KMS
- **Status**: OPEN
- Affects latency-critical path.

### OPN-003 · Polymarket order types in use: limit / FoK / IoC
- **Status**: OPEN
- Affects simulator accuracy.

### OPN-004 · Market-discovery cadence: 15s Gamma poll fast enough or push channel?
- **Status**: OPEN

### OPN-005 · Position concentration: one market at a time or queue across N
- **Status**: OPEN

### OPN-006 · Strategy regime override during BTC large moves
- **Status**: OPEN
- Calibration windows.

### OPN-007 · Funding / capital amount
- **Status**: OPEN
- Drives risk thresholds and minimum-edge requirements.

### OPN-008 · Regulatory considerations
- **Status**: OPEN
- Not technical; gates Phase 7.

### OPN-009 · Fail-over architecture
- **Status**: OPEN
- Single-VPS acceptable for early live; upgrade path TBD.

### OPN-010 · NDJSON-only vs relational store for positions / orders
- **Status**: OPEN

---

## 14. Archived / Accepted Risks

(Items that are real, tracked, but explicitly accepted at the current phase. Mostly carried in from `docs/TECH_DEBT.md` and `docs/decisions.md`. Re-evaluate triggers are listed where relevant.)

| ID | Item | Source | Re-evaluate when |
|----|------|--------|------------------|
| STO-005 | Storage rotation aligned to session start | TECH_DEBT §1 | operational pain |
| STO-006 | Storage text-only payloads | TECH_DEBT §2 | first binary frame observed |
| STO-007 | `Arc<Mutex<Store>>` shared writer | TECH_DEBT §3 | `store_p99_us` near alarm |
| STO-008 | `JoinHandle::abort()` shutdown | TECH_DEBT §4 | `process_text` grows `.await` |
| RES-002 | Resolution-source mismatch | decisions.md §8 | never (research input) |
| RES-004 | Gamma `deprecation: true` header | TECH_DEBT §5 | header flips, replacement published, or discovery returns empty |
| REC-008 | chrony loss silent at recorder | here | when Risk Engine lands |
| REP-004 | Pacer offsets can locally re-order | architecture doc | only if testing latency |
| ANL-003 | Holdout untouched (rule) | manifest | never breaks |
| ANL-004 | No holdout tuning (rule) | baseline params | never breaks |
| ANL-005 | Baseline ≠ optimal | baseline params | informational |

---

## Source files consolidated

This file pulls from:
- `docs/TECH_DEBT.md` — 5 deferred items (now ARC items above)
- `docs/NEXT_SESSION.md` — 5 open tasks + 2 carry-overs (now in §1, §2, §5, §11, §12)
- `docs/BOT_ARCHITECTURE_AND_BUILD_PLAN.md` — Open Questions (§13) + per-system "what could go wrong" notes
- `docs/decisions.md` — accepted-design items (RES-002, REC-008 framing)
- `configs/recorder.toml` line 32 — Polymarket WS TODO (REC-005)
- `crates/config/src/lib.rs` line 91 — same TODO (REC-005)
- `manifests/sessions.toml` — integrity_status="unknown" gap (INT-001) + git_rev gap (MON-007) + holdout pending (ANL-007)

When a new issue surfaces:
1. Add it here with a fresh ID in the appropriate section.
2. Do NOT create a new TODO/RISK/KNOWN_ISSUES file.
3. If the issue lives best as a per-system note in `BOT_ARCHITECTURE_AND_BUILD_PLAN.md` ("what could go wrong"), keep that note brief and cross-reference the canonical entry here.
