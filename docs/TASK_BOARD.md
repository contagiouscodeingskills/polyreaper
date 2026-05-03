# PolyBot — Task Board

> **This is the single source of truth for what to do next, what is blocked, what needs review, and what must not be started yet.** New tasks live here, not scattered across other docs.

Last updated: 2026-05-03.

## Rules

1. **Work tasks in priority order within a status column** unless the user approves a change.
2. **Do not skip blocked tasks** — gating is intentional.
3. **Do not touch** the live recorder, raw market data, wallet/signing/API key code, live execution, holdout sessions, or holdout-tuning unless a task explicitly allows it.
4. **Do not tune parameters on holdout data** — ever. Holdout is run once with frozen baseline (`configs/lag_oos_baseline.toml`) and the result is accepted as-is.
5. **Stop** if tests fail, build fails, TOML/JSON validation fails, or an action requires approval. See `docs/CLAUDE_UNATTENDED_WORK_RULES.md` §"Stop conditions".
6. **Every task must have a clear definition of done.** Tasks without one stay in Backlog until they're refined.
7. **No task is complete until evidence is recorded.** A test pass, a file diff, an artifact path, or a command output. "Looks fine" is not done.
8. **No edge claims without simulator output.** Signal-validation tasks (Phase 3 OOS) report lag/response stats only; PnL/EV is Phase 4+.

## Status columns

| # | Column | Meaning |
|---|--------|---------|
| 1 | Ready | Definition of done is clear; preconditions met; can start now |
| 2 | In Progress | Currently being worked on |
| 3 | Needs Review | Work done; awaiting user review / approval / commit |
| 4 | Blocked | Preconditions not met; gate condition spelled out |
| 5 | Backlog | Real but not next — keep visible, refine before promoting |
| 6 | Do Not Start Yet | Explicit hard hold — do NOT begin under any circumstances |
| 7 | Done | Complete; commit hash recorded |

## Task schema

Every task uses this schema. Empty fields use `—`.

```
- ID:                       <T-XNN>
- Title:                    <imperative phrase>
- Phase:                    <recorder | replayer | analysis | sim | shadow | live | ops | docs>
- Status:                   <Ready | In Progress | Needs Review | Blocked | Backlog | Do Not Start Yet | Done>
- Priority:                 <LOW | MEDIUM | HIGH | BLOCKER>
- Purpose:                  <one-sentence why>
- Allowed actions:          <e.g., DOCS_ONLY, READ_ONLY, TEST_ONLY — see CLAUDE_UNATTENDED_WORK_RULES.md>
- Not allowed:              <hard prohibitions — raw data, holdout inspect, etc.>
- Inputs:                   <files, sessions, prior commits>
- Outputs:                  <files, commits, reports>
- Definition of done:       <objective, checkable>
- Tests/checks required:    <toml parse, integrity verdict, unit test, etc.>
- Blocks live trading?      <YES | NO>
- Related risks:            <IDs from UNSOLVED_PROBLEMS_AND_RISKS.md>
- Notes:                    <any caveats>
```

---

## 1. Ready

### T-R01 · Organise analysis-script inventory
- **Phase**: analysis / docs
- **Status**: Ready
- **Priority**: MEDIUM
- **Purpose**: Resolve ANL-001. There are 10 untracked Python/shell tools in `scripts/`; this task produces an inventory only — keep / merge / delete decisions are flagged for user approval.
- **Allowed actions**: DOCS_ONLY, READ_ONLY (no code changes)
- **Not allowed**: deleting, moving, or rewriting any script; running any script.
- **Inputs**: `scripts/*.py`, `scripts/*.sh`, `/tmp/lag_*` artifacts on VPS (read-only inspection)
- **Outputs**: New `docs/ANALYSIS_SCRIPTS_INVENTORY.md` with: per-script purpose, hardcoded SESSION_DIR, last-modified, dependencies, recommendation (keep / merge / delete) — recommendation only, no action.
- **Definition of done**: One row per script, with a clear recommendation. User signs off before any deletion or merge.
- **Tests/checks required**: lint the new doc parses as Markdown.
- **Blocks live trading?** NO
- **Related risks**: ANL-001
- **Notes**: Do not run the scripts. Do not edit them. Do not delete `/tmp` files on the VPS.

### T-R02 · Run replayer integrity on train sessions
- **Phase**: replayer / data quality
- **Status**: Ready
- **Priority**: HIGH
- **Purpose**: Resolve INT-001 and INT-002 for the train sessions. Required input for OOS work; required input for promotion gates.
- **Allowed actions**: VPS_READ_ONLY, ANALYSIS_TRAIN_ONLY
- **Not allowed**: holdout session, modify data, restart recorder.
- **Inputs**: `manifests/sessions.toml` train list = `[session_20260426T122347Z, session_20260427T100216Z]`
- **Outputs**: Integrity verdicts (PASS / WARN / FAIL + per-tier counters) recorded in `manifests/sessions.toml` per-session entries.
- **Definition of done**: Both train sessions have `integrity_status` updated from `"unknown"` to a real verdict. Manifest still parses as TOML.
- **Tests/checks required**: TOML parse; verdict source recorded.
- **Blocks live trading?** NO directly; blocks OOS publication.
- **Related risks**: INT-001, INT-002, INT-005 (truncated session boundary case)
- **Notes**: Read-only. The replayer integrity command does not mutate session data.

### T-R03 · Verify finalised live session readiness when ≥48h
- **Phase**: recorder / promotion gate
- **Status**: Ready (re-check task; runs on schedule)
- **Priority**: HIGH
- **Purpose**: Decide whether to promote the holdout candidate (`session_20260503T000248Z`) into `splits.holdout`. Required by ANL-007 and Promotion Gate 2 (see `docs/PROMOTION_GATES.md`).
- **Allowed actions**: VPS_READ_ONLY
- **Not allowed**: any analysis on the candidate session; any modification to the session; restart recorder.
- **Inputs**: VPS metadata (`_health.ndjson`, `_session_meta.json`, `_resolutions.ndjson`, `_latency_probes.ndjson`)
- **Outputs**: A short readiness report (gate-by-gate checklist) and, if all gates pass, a proposed manifest update for user approval.
- **Definition of done**: Each gate from Promotion Gate 2 is checked and either ✓ or ✗ with evidence. Manifest update is proposed but not committed without approval.
- **Tests/checks required**: see Gate 2 in `docs/PROMOTION_GATES.md`.
- **Blocks live trading?** YES (no holdout = no OOS = no live).
- **Related risks**: ANL-007, REC-002 (7-day soak)
- **Notes**: Earliest re-check time is 2026-05-05 00:02 UTC.

### T-R04 · Add focused tests for INT-003 and INT-004
- **Phase**: replayer
- **Status**: Ready
- **Priority**: MEDIUM
- **Purpose**: Synthetic test cases for the existing Polymarket per-asset `timestamp_ms` non-monotonic check (INT-003) and consecutive-hash duplicate detection (INT-004).
- **Allowed actions**: CODE_SAFE, TEST_ONLY (test code only, in `crates/replayer/src/integrity.rs`)
- **Not allowed**: changing the integrity logic itself; running on real data.
- **Inputs**: existing implementation at `crates/replayer/src/integrity.rs:715` and `:743`.
- **Outputs**: New unit tests + commit.
- **Definition of done**: Tests pass. `cargo test -p replayer` clean. Coverage for the synthetic regression and duplicate cases.
- **Tests/checks required**: `cargo test -p replayer` green.
- **Blocks live trading?** NO directly.
- **Related risks**: INT-003, INT-004
- **Notes**: Auto-commit allowed if tests pass and changes touch only test files.

### T-R05 · Add WARN log for Binance subscribe-failure
- **Phase**: recorder
- **Status**: Ready
- **Priority**: MEDIUM
- **Purpose**: Resolve REC-004. Phase 6 dropped subscribe-ack from health output; we still want to loudly WARN on failed acks.
- **Allowed actions**: CODE_SAFE
- **Not allowed**: changing subscribe semantics; changing health output schema.
- **Inputs**: `crates/binance_feed/src/conn.rs`, `frame.rs`.
- **Outputs**: `tracing::warn!` on failed subscribe ack; small unit test if practical.
- **Definition of done**: Code compiles, existing tests pass, new test (or manual reasoning if no test path) covers the WARN branch.
- **Tests/checks required**: `cargo test -p binance_feed` green; `cargo build --release -p recorder` green.
- **Blocks live trading?** NO.
- **Related risks**: REC-004
- **Notes**: Requires user approval before commit (touches recorder).

---

## 2. In Progress

### T-P01 · Workflow docs operating system (this round)
- **Phase**: docs
- **Status**: In Progress
- **Priority**: HIGH
- **Purpose**: Stand up the project's operating system — task board, unattended-work rules, reproducibility standard, promotion gates, workflow index.
- **Allowed actions**: DOCS_ONLY
- **Not allowed**: any code change, any analysis, any data touch.
- **Inputs**: User spec.
- **Outputs**: `docs/TASK_BOARD.md`, `docs/CLAUDE_UNATTENDED_WORK_RULES.md`, `docs/REPRODUCIBILITY_AND_EXPERIMENT_STANDARD.md`, `docs/PROMOTION_GATES.md`, `docs/PROJECT_WORKFLOW_INDEX.md`.
- **Definition of done**: All 5 files written, all parse, summary delivered, awaiting commit approval.
- **Tests/checks required**: Markdown sanity; user review.
- **Blocks live trading?** NO directly; the rules guard against unsafe live work.
- **Related risks**: WRK-001, WRK-002, WRK-003, PRV-001, PRV-004, PRV-005
- **Notes**: Drafts complete in this turn; awaiting user approval before commit.

---

## 3. Needs Review

*(Empty)*

---

## 4. Blocked

### T-B01 · Promote holdout session
- **Phase**: data / OOS
- **Status**: Blocked
- **Priority**: HIGH
- **Purpose**: Move `session_20260503T000248Z` from `holdout_candidate` into `splits.holdout`.
- **Blocked by**: All Gate 2 conditions in `docs/PROMOTION_GATES.md` — ≥48h clean capture, integrity PASS, validate-resolutions PASS, `write_failures = 0`, no ENOSPC, no stalls, chrony OK, never analysed.
- **Definition of done**: Manifest entry for the candidate moved to `splits.holdout`; gate-passing evidence recorded in commit message.
- **Blocks live trading?** YES (no holdout → no OOS → no live).
- **Related risks**: ANL-007, INT-001, REC-002
- **Notes**: User approval required to commit the manifest update.

### T-B02 · Build OOS validation harness (`scripts/lag_oos.py`)
- **Phase**: analysis
- **Status**: Blocked
- **Priority**: BLOCKER
- **Purpose**: Phase 3 deliverable — runs the frozen baseline against the holdout split and reports same-direction / opposite / no-response counts, median/p90 lag, sample sizes per slice, odds-band and threshold breakdowns. **No PnL.**
- **Blocked by**: T-B01 (holdout promoted) AND user approval of the script design before commit.
- **Definition of done**: Script written; produces the report against train (sanity); refuses to run if `sweep_permitted` or `holdout_tuning_permitted` is true; refuses to run if `splits.holdout = []`.
- **Blocks live trading?** YES.
- **Related risks**: ANL-002, ANL-003, ANL-004, PRV-002, PRV-003

### T-B03 · Run OOS validation
- **Phase**: analysis
- **Status**: Blocked
- **Priority**: BLOCKER
- **Purpose**: Execute the harness once against the promoted holdout. Single run with frozen baseline; result accepted as-is.
- **Blocked by**: T-B02 done and approved.
- **Definition of done**: Report committed under `experiments/<run_id>/` with `experiment.json` provenance (per `docs/REPRODUCIBILITY_AND_EXPERIMENT_STANDARD.md`).
- **Blocks live trading?** YES.
- **Related risks**: ANL-006, PRV-001..PRV-004

### T-B04 · Build execution simulator (Phase 4)
- **Phase**: sim
- **Status**: Blocked
- **Priority**: HIGH
- **Purpose**: Fill model + size/liquidity + slippage + delay + Polymarket fee schedule. First place economic metrics (PnL/EV) appear.
- **Blocked by**: T-B03 done and reviewed; OOS verdict is "proceed".
- **Definition of done**: Simulator binary runs against a `Signal` stream from a Phase-3-validated parameter set; passes Promotion Gate 4.
- **Blocks live trading?** YES.
- **Related risks**: SIM-001..SIM-005, INT-006

### T-B05 · Build shadow trading (Phase 5)
- **Phase**: shadow
- **Status**: Blocked
- **Priority**: HIGH
- **Purpose**: Live signal runner + paper order logger + live-vs-sim divergence monitor.
- **Blocked by**: T-B04 reviewed; simulator passes Gate 4.
- **Definition of done**: Shadow runs ≥2 weeks across varied conditions; passes Gate 5.
- **Blocks live trading?** YES.
- **Related risks**: SHD-001, SHD-002, SIG-002 (replay determinism)

### T-B06 · Build live execution (Phase 7)
- **Phase**: live
- **Status**: Blocked
- **Priority**: BLOCKER
- **Purpose**: Order router + wallet + order manager + position store + user-data WS handler + risk engine + kill switch.
- **Blocked by**: T-B05 done and approved; risk engine + monitoring complete; Gate 6 passed; explicit human approval.
- **Definition of done**: Smallest-possible-size first live trade with manual kill switch primary.
- **Blocks live trading?** YES (this IS live trading).
- **Related risks**: LIV-001..LIV-006, RSK-001..RSK-005, MON-001..MON-004, OPN-002 (wallet), OPN-008 (regulatory)

---

## 5. Backlog

Real but not next. Refine before promoting to Ready.

- **T-G01** · Replace `docs/TECH_DEBT.md` with a pointer to `docs/UNSOLVED_PROBLEMS_AND_RISKS.md`. Requires user approval.
- **T-G02** · Archive `docs/NEXT_SESSION.md` (rename `NEXT_SESSION_2026-04-27.md`) or replace with pointer. Requires user approval.
- **T-G03** · Add `git_rev` + `git_dirty` to `_session_meta.json` (`apps/recorder/src/meta.rs`). Resolves MON-007.
- **T-G04** · `cargo fmt --workspace` one-shot commit. Resolves MON-005 (78 files of drift).
- **T-G05** · Remove `docs/sample_polymarket_census.txt` once the local Windows lock releases. Resolves MON-006.
- **T-G06** · Fix stale `TECH_DEBT.md §4` cross-reference in `docs/SETUP_WINDOWS.md`. Trivial.
- **T-G07** · Define and start running failure-injection drills (ENOSPC, feed stall, reconnect, chrony loss, stale data, duplicate event, order reject, partial fill, kill switch). Resolves WRK-002.
- **T-G08** · Define runner / secrets / approval boundary doc (`docs/RUNNERS_AND_SECRETS.md`). Resolves WRK-001.
- **T-G09** · Implement book-level invariant tests (`crates/replayer/src/book/`). Resolves INT-006. Defer until Phase 4.5 needs the book.
- **T-G10** · Coinbase L2 channel (MON-008). Deferred — current strategy doesn't depend on it.
- **T-G11** · Chainlink Data Streams / Mercury integration (RES-003). Deferred — needs paid API key.

---

## 6. Do Not Start Yet

**Explicit hard hold.** Do NOT begin these under any circumstances without the user's direct, in-message approval. These are not just "blocked" — they are forbidden until prior phases land.

- **T-X01** · Live execution engine
- **T-X02** · Polymarket order router
- **T-X03** · Wallet / signing module
- **T-X04** · Real-money trading
- **T-X05** · Trading risk engine for live orders (data-quality risk gate is OK to design earlier as part of Phase 6)
- **T-X06** · Shadow trading before the simulator exists
- **T-X07** · Any holdout tuning / inspection of holdout signal results
- **T-X08** · Any change to `manifests/sessions.toml` holdout split after promotion
- **T-X09** · Any change to `configs/lag_oos_baseline.toml` after the holdout has been run

---

## 7. Done

- **T-D01** · Recorder + replayer foundation. Multiple commits up to `b7dfab6` (recorder: add session metadata sidecar + persist latency probes).
- **T-D02** · Recorder finalisation phases 1–6 + follow-up. Commit `a364ab8`.
- **T-D03** · Bot architecture blueprint `docs/BOT_ARCHITECTURE_AND_BUILD_PLAN.md`. Commit `4e9810f`.
- **T-D04** · Metadata-only session split + frozen lag baseline. Commit `c6bfe1e` (`manifests/sessions.toml`, `configs/lag_oos_baseline.toml`).
- **T-D05** · Initial consolidation of `docs/UNSOLVED_PROBLEMS_AND_RISKS.md`. Commit `f87f166`.

---

## How to use this board

1. Pick the highest-priority **Ready** task.
2. If you can't satisfy the "Allowed actions" constraint or the "Tests/checks required" constraint without help, escalate. Do NOT relax constraints.
3. Move the task to **In Progress** as you start; record the commit hash on completion and move to **Done**.
4. New issues that surface mid-task: file them in `docs/UNSOLVED_PROBLEMS_AND_RISKS.md`, then optionally create a Backlog task here that references them. **Do not** create a TODO file or scattered note.
5. **Do not** invent new Ready tasks during unattended work; only the user promotes Backlog → Ready (or creates new tasks).
