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

### T-R02 · Run replayer integrity on train sessions ✅ DONE
- **Phase**: replayer / data quality
- **Status**: Done (commit `e18eb5d`, 2026-05-10 verdicts recorded 2026-05-12)
- **Priority**: HIGH
- **Purpose**: Resolve INT-001 and INT-002 for the train sessions. Required input for OOS work; required input for promotion gates.
- **Outputs delivered**: `docs/research/integrity/2026-05-10-train-integrity.md`; both train entries in `manifests/sessions.toml` updated to `integrity_status = "WARN"` with source / reasons / safe-cutoff fields.
- **Result**: both WARN, both safe for TRAIN use. WARN driven by venue-side informational counters, not recorder corruption. See report for details.
- **Related risks**: INT-001 (partially resolved — train only), INT-002 (resolved).

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

### Production-readiness track (T-PR01..T-PR07)

A tightly-scoped, sequential track to move the recorder from "dev recorder" to "production-grade research recorder" per the operator's direction (2026-05-12). Each task ends with proof the previous step worked. **No bot work, no signal engine, no live execution until this track is complete and the 7-day soak passes.** Bash commands for any VPS mutation are drafted and approved per-task.

### T-PR01 · Operational reset — clean disk safely, restart recorder
- **Phase**: recorder / ops
- **Status**: Ready
- **Priority**: BLOCKER (prerequisite for everything else)
- **Purpose**: Free disk on the VPS so the recorder can run again. Without this, every other PR-track task is blocked by disk pressure.
- **Allowed actions**: VPS_MUTATION_APPROVAL_REQUIRED (commands drafted and approved per-step)
- **Not allowed**: deleting any session data; touching active recorder files or any session less than 7 days old; modifying anything in `splits.train` or `holdout_candidate`; touching `manifests/sessions.toml [splits.holdout]`.
- **Disk-cleanup policy (operator decision 2026-05-12)**: gzip all `*.ndjson` files in sessions older than 7 days. **No deletions** unless the operator approves specific paths. Live / current session is never touched.
- **Inputs**: VPS at `159.69.0.78`, current disk state (98% full, 1.9 GB free), recorder inactive since 2026-05-04 21:14 UTC.
- **Outputs**: gzipped older sessions; documented before/after free-disk-GB; recorder restarted; new session_id captured in a follow-up read-only check.
- **Definition of done**: (a) operator-approved bash executed; (b) at least 20 GB free on `/dev/sda1`; (c) recorder service `active` per `systemctl is-active polybot-recorder`; (d) new `_health.ndjson` writing per 30s; (e) all pre-existing sessions present and unchanged (verified by file count + checksum); (f) summary report appended to `docs/research/ops/2026-05-12-operational-reset.md`.
- **Tests/checks required**: post-step disk-free measurement; `systemctl is-active`; first 3 `_health.ndjson` snapshots parse cleanly; pre-existing session count matches.
- **Blocks live trading?** Indirectly YES (unblocks the whole PR track).
- **Related risks**: STO-001, STO-002, STO-004, REC-002.
- **Notes on holdout candidate**: `session_20260503T000248Z` (45.2h, captured cleanly before SIGTERM shutdown) will never grow — the recorder restart creates a new session_id. The 45.2h session is preserved as-is (5 days old, will not be touched by 7-day-gzip policy until 2026-05-10... wait, today is 2026-05-12; session start was 2026-05-03; the session is 9 days old. Under 7-day policy it would be compressed. Confirm preservation explicitly).

### T-PR02 · Routine compression timer (systemd)
- **Phase**: recorder / ops
- **Status**: Blocked (by T-PR01)
- **Priority**: HIGH
- **Purpose**: Make compression a routine action, not an emergency. Prevents the disk-fill root cause from recurring.
- **Allowed actions**: CODE_SAFE for the script; DEPLOY_APPROVAL_REQUIRED to install the systemd unit on the VPS.
- **Not allowed**: touching active recorder files; touching `splits.holdout` or `holdout_candidate`; deleting data.
- **Inputs**: existing `scripts/disk_guard.sh` as a reference pattern. Discipline: gzip files where `mtime > 24h` AND file is NOT in the currently-active session.
- **Outputs**: new `scripts/recorder_compress.sh` + `scripts/recorder_compress.service` + `scripts/recorder_compress.timer`; daily timer.
- **Definition of done**: timer fires daily; `cargo`-free shell script; runs in dry-run mode on first deploy; output verified before enabling apply-mode; one full week of operation observed with no false positives.
- **Tests/checks required**: dry-run output reviewed; "would compress" set must exclude active session and any file <24h old.
- **Blocks live trading?** NO.
- **Related risks**: STO-001, STO-003, STO-004.

### T-PR03 · Off-host encrypted backup (B2/S3-compatible)
- **Phase**: ops
- **Status**: Blocked (by T-PR01)
- **Priority**: HIGH
- **Purpose**: Single point of failure (VPS) currently holds all recorded data. Backup is the prerequisite for trusting the data long-term. **Operator decision (2026-05-12): encrypted B2/S3-style object storage as the proper long-term target; geographically separate from Frankfurt/Nuremberg. Hetzner Storage Box acceptable only as a temporary cheaper bridge.**
- **Allowed actions**: CODE_SAFE for the script; DEPLOY_APPROVAL_REQUIRED for the systemd timer; secrets management for bucket credentials (env vars, never logged).
- **Not allowed**: backing up wallet keys or anything containing secrets; running unencrypted at rest or in transit.
- **Inputs**: choice of provider (Backblaze B2 / AWS S3 / Cloudflare R2 / Wasabi — operator to pick); bucket created by operator with credentials in env vars; encryption key generated and stored separately from credentials.
- **Outputs**: new `scripts/recorder_backup.sh` (rclone or restic); systemd unit + timer; encrypted upload; documented restore procedure.
- **Definition of done**: (a) one successful end-to-end backup of a small completed session; (b) restore test on a separate host produces bit-identical files (sha256 match per file); (c) backup runs daily on a timer; (d) backup completes without error for 7 consecutive days.
- **Tests/checks required**: restore-and-verify test; alert path on backup failure.
- **Blocks live trading?** YES (long-term data trustworthiness gate).
- **Related risks**: new — propose `OPS-001 No off-host backup` for the risk register.
- **Notes**: operator decision still pending: which provider, which region, encryption-at-rest model, who holds the master key.

### T-PR04 · Provenance fields in `_session_meta.json`
- **Phase**: recorder / code
- **Status**: Blocked (by T-PR01)
- **Priority**: HIGH
- **Purpose**: Resolve MON-007 and lock down replay reproducibility. Without these fields, a session cannot be pinned to a specific build.
- **Allowed actions**: CODE_SAFE in `apps/recorder/src/meta.rs`; DEPLOY_APPROVAL_REQUIRED to put a new recorder build in production.
- **Not allowed**: logging any value of any env var that might contain secrets (only names + set/unset status); changing the existing `RawEvent` schema; touching the recorder's wire-faithful invariant.
- **Inputs**: existing `apps/recorder/src/meta.rs`; `vergen` or a small build.rs to embed git info.
- **Outputs**: `_session_meta.json` gains: `git_rev` (full SHA), `git_dirty` (bool), `cargo_lock_sha256`, `rustc_version`, `os_release` (`/etc/os-release` parsed), `kernel_release` (`uname -r`), `build_timestamp_utc`, `recorder_started_at_iso`, `full_config_toml` (the loaded TOML, verbatim).
- **Definition of done**: new recorder build deployed; first new session's `_session_meta.json` contains all new fields; `git_rev` matches `git rev-parse HEAD` of the deployed commit; sample parsed by a small script.
- **Tests/checks required**: unit test on the meta-writer; integration check that the file contains all fields after a fresh start.
- **Blocks live trading?** YES (provenance is a Phase J precondition).
- **Related risks**: MON-007 (resolves), PRV-001 (partially).

### T-PR05 · Lifecycle / reconnect / subscribe-ack / Gamma sidecars
- **Phase**: recorder / code
- **Status**: Blocked (by T-PR01)
- **Priority**: MEDIUM-HIGH
- **Purpose**: Move forensic data out of `journalctl` and into durable, parseable NDJSON sidecars. Without these, post-mortem on any incident depends on whether journald rotation has flushed the relevant lines.
- **Allowed actions**: CODE_SAFE in `apps/recorder` and the feed crates; DEPLOY_APPROVAL_REQUIRED to deploy.
- **Not allowed**: changing wire-faithful payload preservation; changing existing `_health.ndjson` schema; logging secrets.
- **Inputs**: existing feed crates (where reconnect / subscribe-ack happen) and `apps/recorder/src/sweep.rs` (Gamma calls).
- **Outputs**: per session — `_recorder_lifecycle.ndjson` (boot / shutdown / restart events; each with reason and stack-of-causes), `_reconnects.ndjson` (per feed: connection_id, opened_at, closed_at, close reason, duration_up_ms), `_subscribe_acks.ndjson` (per subscribe attempt: feed, target, result, response sample), `_gamma_calls.ndjson` (per call: ts, endpoint, status, latency_ms, response sample for failures).
- **Definition of done**: each sidecar appears in a fresh session; each record schema-validated by a sample script; existing recorder behavior unchanged otherwise.
- **Tests/checks required**: unit tests per writer; integration on a 5-minute test session.
- **Blocks live trading?** NO directly; closes specific forensic gaps (REC-003, REC-004, REC-007).
- **Related risks**: REC-003, REC-004, REC-007.

### T-PR06 · Nightly raw replay integrity check
- **Phase**: ops / replayer
- **Status**: Blocked (by T-PR01)
- **Priority**: HIGH
- **Purpose**: Catch silent data-quality regressions within 24h instead of when an analysis fails weeks later. Note: **raw integrity only — NOT signal-stream determinism (no signal engine yet).**
- **Allowed actions**: CODE_SAFE for the shell script; DEPLOY_APPROVAL_REQUIRED for the systemd timer on the VPS.
- **Not allowed**: touching the live recorder service; running against `splits.holdout` (only against the last completed non-holdout session); writing back into the manifest without explicit approval.
- **Inputs**: existing `replayer integrity` subcommand; the previous day's completed session.
- **Outputs**: new `scripts/nightly_raw_integrity.sh` + systemd unit + timer; daily report written under `data/_nightly_integrity/<date>.txt`; alert on FAIL.
- **Scope (per operator direction 2026-05-12)**: (a) replay yesterday's session, confirm all files parse; (b) count events per stream; (c) check timestamp monotonicity per file; (d) check sequence gaps where defined (Binance depth_chain, bookTicker update_id, Coinbase trade_id, Polymarket per-asset ts); (e) check reconnect boundaries; (f) hash the normalised decoded-event stream for each venue; (g) report failures.
- **Definition of done**: nightly run completes within 30 min; output committed to a per-date file; alert fires on WARN/FAIL; first 7 consecutive nightly runs pass.
- **Tests/checks required**: dry-run on the most recent completed session; verify the hash is deterministic across two runs.
- **Blocks live trading?** YES (nightly check is a Phase G prerequisite and a Phase I expectation).
- **Related risks**: INT-005 (partially); new "no nightly integrity check" risk.
- **Notes**: when a signal engine eventually exists, T-PR06's scope expands to include signal-stream hashing (Blueprint Rule 1 / SIG-002).

### T-PR07 · 7-day recorder soak test
- **Phase**: validation
- **Status**: Blocked (by T-PR01..T-PR06)
- **Priority**: HIGH
- **Purpose**: Empirical proof that the production-ready recorder runs unattended for a week without manual intervention. Resolves REC-002.
- **Allowed actions**: VPS_READ_ONLY (observational only).
- **Not allowed**: any code or config change during the soak window; any restart of the recorder during the window (unless an actual incident requires it, in which case the soak fails and restarts).
- **Inputs**: fresh recorder running with all PR-track changes deployed (T-PR04, T-PR05); compression + backup timers active (T-PR02, T-PR03); nightly integrity check active (T-PR06).
- **Outputs**: 7 daily check-in reports under `docs/research/ops/soak/2026-MM-DD.md`; final pass/fail report.
- **Definition of done**: 168 consecutive hours of: (a) `systemctl is-active polybot-recorder` = active; (b) no `write_failures > 0` in any `_health.ndjson` snapshot; (c) no `stalled = true` for any feed in any snapshot; (d) `disk_free_bytes` stays above the threshold from `disk_guard`; (e) `chrony.available = true` throughout; (f) nightly integrity checks all pass; (g) backup completes daily without error. Any single failure during the window means restart the window from zero.
- **Tests/checks required**: daily check-in script (`scripts/daily_soak_checkin.sh`); per-day report committed.
- **Blocks live trading?** YES (REC-002).
- **Related risks**: REC-002 (resolves), REC-003, REC-007, STO-001/002/003, WRK-002.

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
