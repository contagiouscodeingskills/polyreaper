# Promotion Gates

> **Purpose.** Define measurable, checklist-style gates for moving between bot phases. No phase begins until the previous phase's gate is signed off with evidence. No vibes, no qualitative confidence alone.

Last updated: 2026-05-03.

## Universal rules

1. **Do not skip gates.** Skipping a gate voids the project's safety story.
2. **Gates are AND-conditions.** Every requirement is mandatory unless explicitly marked "(advisory)". A failing gate stops the phase.
3. **Gate failure stops and documents.** When a gate fails, the failing requirements are recorded in `docs/UNSOLVED_PROBLEMS_AND_RISKS.md` (or an existing entry is updated), and the phase does not begin.
4. **Evidence is required.** Each gate item names what counts as evidence — a test exit code, an integrity verdict, an artefact path. "Looks fine" is not evidence.
5. **Sign-off is explicit.** When a gate passes, the user records the sign-off in the commit message of the artefact that passes the gate (e.g., the manifest update, the simulator commit, the shadow report). Self-sign-off by Claude is forbidden.
6. **Numeric thresholds in this file are version 0.** They reflect the project's current intent. Each can be tightened (never silently loosened) by editing this file in a separate, reviewed commit.

## Cross-references

- `docs/TASK_BOARD.md` — every gated task names its gate.
- `docs/REPRODUCIBILITY_AND_EXPERIMENT_STANDARD.md` — every gate-passing run produces an `experiments/<run_id>/` record.
- `docs/UNSOLVED_PROBLEMS_AND_RISKS.md` — risk IDs cross-referenced per gate.
- `docs/CLAUDE_UNATTENDED_WORK_RULES.md` — what may / may not be done while a gate is pending.

---

## Gate 1 — Data layer ready

**Goal.** The recorder + replayer + storage stack is trustworthy enough to be the foundation of every downstream phase.

**Requirements.**

| # | Requirement | Evidence | Risk ID |
|---|-------------|----------|---------|
| 1.1 | Recorder running with all enabled feeds healthy | `_health.ndjson` last snapshot shows `messages > 0` and `stalled = false` for binance, polymarket, coinbase | REC-002, REC-003 |
| 1.2 | No write failures on the active session | last health snapshot: `write_failures = 0` per feed | REC-002 |
| 1.3 | No ENOSPC events during the session | journalctl grep for ENOSPC over the session window: empty | STO-001, STO-002 |
| 1.4 | Replayer integrity command exists and runs | `cargo build --release -p replayer-cli` green; `replayer integrity --help` works | INT-001 |
| 1.5 | Resolution sweeper writing records | `_resolutions.ndjson` line count > 0 over a session | RES-001 |
| 1.6 | Latency probes writing records | `_latency_probes.ndjson` line count > 0 over a session | — |
| 1.7 | Disk guard active (or accepted-deferred) | `disk_guard.sh` cron / systemd active OR explicit acceptance recorded | STO-001..004 |
| 1.8 | Recorder service auto-restarts | `systemctl show polybot-recorder | grep Restart=` shows `always` | — |

**Status (2026-05-03).** PASS for the most part:
- 1.1, 1.2, 1.5, 1.6, 1.8 are operational on the live session.
- 1.3 needs an explicit grep for ENOSPC over the active session window; not formally recorded.
- 1.4 is operational; the verdict for any individual session is still `unknown` (Gate 2 #2.4 covers this).
- 1.7 is operational but COMPRESS / STOP not tested under real disk pressure (STO-001, STO-002) — accepted as advisory for Gate 1, mandatory for any later "scale" gate.

**Sign-off.** Already presumed for the current data work; record the sign-off explicitly when promoting the holdout (Gate 2 commit).

---

## Gate 2 — Holdout ready

**Goal.** The holdout-candidate session has captured enough clean data and passes integrity, so it can serve as the OOS evaluation set without the OOS run hitting data quality issues.

**Pre-conditions.**

- Gate 1 passed.
- Candidate session named in `manifests/sessions.toml [splits.holdout_candidate]` (currently `session_20260503T000248Z`).

**Requirements.**

| # | Requirement | Evidence | Risk ID |
|---|-------------|----------|---------|
| 2.1 | ≥48h of continuous capture, no recorder restart | `_health.ndjson` first ts vs last ts ≥ 172800 seconds; no gap >60s in the snapshot stream | REC-002 |
| 2.2 | `replayer integrity --root <session>` returns PASS | exit code 0; verdict `PASS`; copy of report committed under `experiments/<id>/` | INT-001, INT-002 |
| 2.3 | `replayer validate-resolutions --root <session>` returns PASS | exit code 0 | RES-001 |
| 2.4 | `write_failures = 0` across the entire session | last health snapshot per-feed `write_failures = 0`, AND no intermediate snapshot showed >0 then dropped (sticky check via aggregation) | REC-002 |
| 2.5 | No ENOSPC events | journalctl over the session window: 0 lines matching ENOSPC | STO-001, STO-002 |
| 2.6 | No feed stalls | no health snapshot in the window has `stalled = true` for any feed | REC-003 |
| 2.7 | chrony OK throughout | every health snapshot has `chrony.available = true`; `Last offset` from `chronyc tracking` < 1 ms when sampled | REC-008 |
| 2.8 | Session is provably untouched by analysis | provenance audit: no `/tmp/lag_*` summary or `experiments/<id>/` record references this session_id; no `scripts/*.py` has it as `SESSION_DIR`; bash history grep is empty for the session_id | ANL-003, ANL-007 |
| 2.9 | Manifest update prepared | proposed diff to `manifests/sessions.toml` moving the candidate from `splits.holdout_candidate` into `splits.holdout`; `splits.holdout_status` flips from `pending` to `active` | ANL-007 |

**Sign-off.** User reviews the gate-2 evidence file (committed under `experiments/<id>-gate-2-promotion/`) and the manifest diff, and approves the manifest commit.

**Failure handling.**
- If 2.1 fails: wait, the session is still capturing.
- If 2.2 / 2.4 / 2.5 / 2.6 fails: do NOT promote; the session stays as candidate. Investigate, fix, possibly wait for the next clean session.
- If 2.8 fails (provenance audit shows the session was touched): move it to TRAIN permanently, never holdout. Pick a fresh future session as the new candidate.

---

## Gate 3 — OOS signal validation

**Goal.** The lag signal survives evaluation on holdout at the frozen baseline, with statistically valid reporting.

**Pre-conditions.**

- Gate 2 passed; `splits.holdout` is non-empty.
- `configs/lag_oos_baseline.toml` is committed and `parameter_status = "frozen-baseline-not-optimal"`, `sweep_permitted = false`, `holdout_tuning_permitted = false`.
- `scripts/lag_oos.py` is built and reviewed (T-B02 done).

**Requirements.**

| # | Requirement | Evidence |
|---|-------------|----------|
| 3.1 | Frozen baseline params used unchanged | `experiment.json:params_sha256` matches the committed `configs/lag_oos_baseline.toml` |
| 3.2 | No holdout tuning attempted | `experiment.json:params.tuned_during_run = false`; `holdout_tuning_attempted = false` |
| 3.3 | Same-direction / opposite / no-response counts reported | `metrics.json` includes these per slice |
| 3.4 | Median lag and p90 lag reported | `metrics.json` includes both per slice |
| 3.5 | Sample sizes reported per slice | `metrics.json:per_slice.<slice>.n` populated for every slice in the family |
| 3.6 | Confidence intervals (or explicit no-CI rationale) | per Reproducibility Standard §4 |
| 3.7 | Minimum sample-size rule stated and met | every reported slice meets `min_sample_size_required`; underpowered slices are explicitly labelled "no conclusion" |
| 3.8 | Multiple-testing correction named | `metrics.json:multiple_testing_correction` populated; family pre-registered in `configs/lag_oos_baseline.toml` |
| 3.9 | Train comparison without retuning | a parallel run with the same baseline against `splits.train` is included in the report; train numbers are not retuned to match holdout |
| 3.10 | No PnL claim | `report.md` does not state PnL or EV; explicitly says "no PnL — Phase 4 deliverable" |

**Pass condition (numeric, version 0).**

The OOS verdict is "PASS" if **all** of:
- For at least one Binance bps threshold (5 / 10 / 20), the same-direction rate on holdout has 95% CI lower bound > 50% on a slice that meets `min_sample_size_required`.
- The median lag on the corresponding slice is positive (Polymarket lags Binance) and within a documented range (initially: 500 ms ≤ median ≤ 6 s).
- Train-vs-holdout same-direction rate difference is within the 95% CI for both — i.e., we cannot reject "the holdout looks like the train" at the family-corrected level.

**Pass condition is advisory — the user has the final decision.** If numbers are technically passing but the user judges the result spurious or underpowered, the gate does not pass.

**Failure handling.**
- If the lag signal does not survive: the strategy is killed at this gate. Do not proceed to Phase 4. Do not "iterate" on holdout. A new strategy hypothesis would need a new pre-registration cycle and a fresh holdout.

---

## Gate 4 — Execution simulator

**Goal.** A fill model + economic-metrics module that produces simulated trades and PnL with documented sensitivity.

**Pre-conditions.** Gate 3 PASS.

**Requirements.**

| # | Requirement | Evidence |
|---|-------------|----------|
| 4.1 | Fill probability model implemented | unit tests on synthetic books cover: full fill, partial fill, no fill |
| 4.2 | Visible-size / depth-walked entry price | walking the book at signal time produces fill price and remaining size; tested on synthetic and real-book replay |
| 4.3 | Spread / slippage modelled | per-fill slippage = entry price − (mid at signal time); reported per trade |
| 4.4 | Polymarket fee schedule encoded | `configs/polymarket_fees.toml` (or equivalent) committed; cited source documented |
| 4.5 | Delay scenarios swept | runs at 0 ms, 250 ms, 500 ms, 1 s, 2 s; PnL reported per scenario |
| 4.6 | Missed fills counted | "signal fired but liquidity unavailable" tracked per slice |
| 4.7 | Bad fills counted | "fill at price worse than threshold from mid" tracked |
| 4.8 | Gross PnL / EV per signal and aggregate | reported with sample size and CI |
| 4.9 | Sensitivity to latency | a delta in expected PnL between 250 ms and 1 s scenarios is explicitly reported |
| 4.10 | Sensitivity to size | runs at multiple intended sizes (small / medium / sized to top-of-book / sized larger); PnL reported per |
| 4.11 | Adverse-selection model present | even a simple version (e.g., probability our taken quote was about-to-update); documented in the model spec |
| 4.12 | Simulator's interface matches what the live router will expose | one trait / signature reused; the simulator and the (future) router are swappable behind the same interface |

**Pass condition (numeric, version 0).**

- At a documented delay scenario (initially 500 ms — to be calibrated to shadow), the simulated EV per signal is positive on a sample size meeting `min_sample_size_required`.
- The 95% CI lower bound on EV is greater than the per-trade Polymarket fee.
- Sensitivity to latency is monotonic (higher latency → lower EV) within the swept range; an inversion triggers a model audit.

**Failure handling.**
- If EV is not positive after fees at any delay scenario: the strategy fails Gate 4. Do not proceed to shadow. Either revisit the signal hypothesis or kill.

---

## Gate 5 — Shadow trading

**Goal.** End-to-end live data → bot → paper orders, with no real orders submitted, demonstrating production readiness.

**Pre-conditions.** Gate 4 PASS.

**Requirements.**

| # | Requirement | Evidence |
|---|-------------|----------|
| 5.1 | Live signal runner exists | binary tapping live feeds and emitting signals; `paper = true` enforced by config |
| 5.2 | No real orders submitted | order router is in paper mode; assertion at startup; no entries in any order-history endpoint of Polymarket attributable to this run |
| 5.3 | Paper order logger writes NDJSON | per-decision and per-paper-fill records under `experiments/<run_id>/orders.ndjson` |
| 5.4 | Daily reports emitted | per-day `report.md` summarising paper PnL, fills, missed fills, latency, data-quality breaches |
| 5.5 | Live-vs-sim divergence monitor runs nightly | re-runs the bot on the day's recording; signal stream from live and from replay must be bit-identical |
| 5.6 | No data-quality alerts during the run window | feed-stall, integrity FAIL, chrony unavailable, write_failures all clean over the run window |
| 5.7 | ≥ 14 calendar days of clean shadow operation | aggregate daily reports cover the window |
| 5.8 | Sim-to-live latency calibration delta logged | observed end-to-end paper-decision-to-paper-fill latency vs simulator-modelled latency; delta documented |

**Pass condition (numeric, version 0).**

- Signal-stream divergence between live and replay = 0 over the run window.
- Paper PnL CI lower bound > simulator's CI lower bound minus a documented tolerance (initially: shadow ≥ 75% of simulator PnL CI lower bound, accounting for queue position and adverse selection).
- 0 days in the window where data-quality halt rules would have fired.

**Failure handling.**
- Determinism divergence: stop, fix, do not promote.
- Sub-tolerance shadow PnL: recalibrate the simulator (likely adverse-selection model); do not promote until simulator + shadow agree.

---

## Gate 6 — Live trading

**Goal.** Real orders, real money, with every safety control in place and tested.

**Pre-conditions.** Gates 1–5 PASS.

**Requirements.**

### 6.1 Risk Engine — fully built and tested

| # | Requirement | Evidence |
|---|-------------|----------|
| 6.1.1 | Trade-level risk: max order size, max price slippage, sane price, market is tradeable, t_to_resolution > minimum | unit tests for each |
| 6.1.2 | Market-level risk: per-market position cap, daily P&L floor, max consecutive losses | unit tests + simulator-replay tests |
| 6.1.3 | Account-level risk: daily P&L floor, max drawdown, max # open positions, max notional, max loss per minute | unit tests + simulator-replay tests |
| 6.1.4 | Data-quality risk: feed staleness, reconnect-rate spike, parse-failure spike, chrony unavailable, integrity FAIL on last session, Polymarket book depth < minimum | unit tests + drill |
| 6.1.5 | Risk Engine fails closed on internal error | tested via fault injection (raise an exception in a check; verify the engine denies) |
| 6.1.6 | Pre-trade simulation runs before submit | every signal goes through fill-model → reject if uneconomic |

### 6.2 Kill switch — built and tested

| # | Requirement | Evidence |
|---|-------------|----------|
| 6.2.1 | Manual kill switch (operator-triggered) cancels all open orders and halts new orders | drill: triggered manually, verified all orders cancelled within the documented timeout |
| 6.2.2 | Automatic kill switch on account-level breach | drill: synthetic breach injected, kill fires, no further orders submitted |
| 6.2.3 | Kill switch is idempotent under race | drill: signal fires the same instant as kill — kill wins |
| 6.2.4 | Kill switch state survives a process restart | drill: restart bot mid-kill — state recovered, kill remains in effect until cleared by operator |

### 6.3 Hard live-risk controls — all in place

| # | Control | Evidence |
|---|---------|----------|
| 6.3.1 | Max order size (per signal, per market) | enforced + tested |
| 6.3.2 | Max message rate to Polymarket | enforced + tested |
| 6.3.3 | Max inventory / exposure (per market and account-wide) | enforced + tested |
| 6.3.4 | Max daily loss (account-wide) | enforced + tested |
| 6.3.5 | Max slippage (per fill) | enforced + tested |
| 6.3.6 | Stale-data block | enforced + tested |
| 6.3.7 | Duplicate-order prevention (idempotency keys per signal) | enforced + tested |
| 6.3.8 | Human kill switch | always available; tested per 6.2.1 |
| 6.3.9 | Automatic kill switch | tested per 6.2.2 |

### 6.4 Wallet / signing / order router

| # | Requirement | Evidence |
|---|-------------|----------|
| 6.4.1 | Wallet posture documented | `docs/RUNNERS_AND_SECRETS.md` exists and names the chosen approach (hot / hardware / KMS) |
| 6.4.2 | Signing module reviewed | code review with a second pair of eyes (user + Claude or external); test vectors confirmed |
| 6.4.3 | Order router tested | dry-run against Polymarket testnet (or equivalent) with synthetic order; ack received; cancel received |
| 6.4.4 | User-data WS handler producing fills | live test: a known fill on testnet shows up in our user-data stream and updates the position store |
| 6.4.5 | Position store reconciles to Polymarket's view | startup reconcile passes; periodic reconcile logged |
| 6.4.6 | Order manager handles every transition | unit tests cover intended → submitted → acked → filled / rejected / canceled / timeout |

### 6.5 Monitoring and alerts active

| # | Requirement | Evidence |
|---|-------------|----------|
| 6.5.1 | Trading dashboard shows P&L, position, open orders, signal rate, fill rate, slippage, latency | dashboard URL committed to docs |
| 6.5.2 | Alerts active for all categories listed in `CLAUDE_UNATTENDED_WORK_RULES.md` §13 | tested by triggering each one in dev |
| 6.5.3 | Off-host alerting path exists | independent channel that fires when the VPS itself is down |
| 6.5.4 | Audit log of trade decisions live | append-only log location committed; each decision recorded with full context |

### 6.6 Approval

| # | Requirement | Evidence |
|---|-------------|----------|
| 6.6.1 | Explicit human approval per first live order | approval recorded in commit message of the config flag flip from `paper = true` to `paper = false`; approval is for a specific size cap |
| 6.6.2 | No self-approval | Claude does not approve its own changes to live-adjacent code; user signs off explicitly |
| 6.6.3 | Smallest-possible-size first | first live order at the minimum Polymarket order size; documented |

**Pass condition.**

- All sub-items 6.1.x, 6.2.x, 6.3.x, 6.4.x, 6.5.x, 6.6.x check off with evidence.
- A dry-run "go-live drill" passes: from clean state, run the bot in live mode against a test wallet for 1 hour, confirm all controls fire as expected on synthetic events, then halt cleanly.

**Failure handling.**

- Any sub-item failing means the gate fails. No partial passes.
- Failure stops the live promotion; the bot stays in shadow.

---

## After Gate 6: scaling

(Out of scope for this document at version 0. After Gate 6 passes and a sustained live operation period, a future "Gate 7 — Scale" can address: multi-market, multi-account, hot standby, fail-over, etc. None of this is on the roadmap until live trades are running cleanly.)
