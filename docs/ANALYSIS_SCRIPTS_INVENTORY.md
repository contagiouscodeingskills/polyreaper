# Analysis Scripts Inventory

> **Read-only triage** of `scripts/`. No script was executed, modified, or deleted in producing this document. Recommendations below are proposals — they require user approval before any keep / refactor / archive / delete / move action is taken (per [docs/CLAUDE_UNATTENDED_WORK_RULES.md](CLAUDE_UNATTENDED_WORK_RULES.md) §2 and §7).

Last surveyed: 2026-05-03.

## At-a-glance

| File | Tracked? | Lines | Last modified | Kind | Recommendation |
|------|----------|-------|---------------|------|----------------|
| [`scripts/lag_analysis.py`](../scripts/lag_analysis.py) | untracked | 27 KB | 2026-04-28 | obsolete | **Archive** — superseded by v2/v3 |
| [`scripts/lag_analysis_v2.py`](../scripts/lag_analysis_v2.py) | untracked | 31 KB | 2026-05-02 | obsolete | **Archive** — superseded by v3 |
| [`scripts/lag_analysis_v3.py`](../scripts/lag_analysis_v3.py) | untracked | 29 KB | 2026-05-02 | current discovery | **Refactor → base for `scripts/lag_oos.py`** |
| [`scripts/lag_run_confirm.py`](../scripts/lag_run_confirm.py) | untracked | 5 KB | 2026-05-02 | diagnostic | **Refactor** — generalise SESSION arg |
| [`scripts/casestudy_analysis.py`](../scripts/casestudy_analysis.py) | untracked | 34 KB | 2026-04-29 | one-off | **Archive** — methodology useful, run is done |
| [`scripts/scan_polymarket_swings.py`](../scripts/scan_polymarket_swings.py) | untracked | 7 KB | 2026-04-28 | diagnostic | **Refactor** — generalise SESSION arg |
| [`scripts/integrity_supplementary.py`](../scripts/integrity_supplementary.py) | untracked | 9 KB | 2026-05-02 | diagnostic | **Refactor** — generalise SESSION arg |
| [`scripts/migrate_rawevent.py`](../scripts/migrate_rawevent.py) | untracked | 3 KB | 2026-05-02 | one-off | **Archive** — codegen job done; idempotent |
| [`scripts/vps_health_check.sh`](../scripts/vps_health_check.sh) | untracked | 1 KB | 2026-04-26 | ops diagnostic | **Refactor** — point at latest session, not hardcoded |
| [`scripts/vps_polymarket_census.sh`](../scripts/vps_polymarket_census.sh) | tracked | 3 KB | 2026-04-27 | one-off | **Archive** — job done; output committed |
| [`scripts/vps_polymarket_inner_keys.sh`](../scripts/vps_polymarket_inner_keys.sh) | tracked | 2 KB | 2026-04-27 | one-off | **Archive** — job done; informed decoder |
| [`scripts/disk_guard.sh`](../scripts/disk_guard.sh) | tracked | 9 KB | 2026-05-03 | ops infrastructure | **Keep** — production guard |
| [`scripts/disk_guard.service`](../scripts/disk_guard.service) | tracked | 0.4 KB | 2026-05-03 | ops infrastructure | **Keep** — systemd unit |
| [`scripts/disk_guard.timer`](../scripts/disk_guard.timer) | tracked | 0.4 KB | 2026-05-03 | ops infrastructure | **Keep** — systemd timer |
| [`scripts/deploy.sh`](../scripts/deploy.sh) | tracked | 1 KB | 2026-04-25 | ops tool | **Keep** — but DEPLOY_APPROVAL_REQUIRED |

15 files total — 9 untracked, 6 tracked.

---

## Lag-analysis chain

The analysis line of work has three generations of `lag_analysis*.py` and supporting helpers. The most recent (v3) is the basis for the OOS harness.

### `scripts/lag_analysis.py` (v1)

- **Path**: `scripts/lag_analysis.py`
- **Purpose**: First-pass Binance↔Polymarket directional-response analysis on the 13h gold session. Detects non-overlapping moves on both sides, computes lag, classifies same-direction / opposite / no-response.
- **Kind**: **Obsolete** — superseded by v2 and v3.
- **Hardcoded session paths**: `SESSION_DIR = /home/polybot/polybot/data/session_20260426T122347Z` (TRAIN per `manifests/sessions.toml`).
- **Hardcoded parameters**:
  - `POLY_THRESHOLDS = [0.10, 0.20, 0.30]`
  - `BIN_THRESHOLDS_BPS = [5.0, 10.0, 20.0]`
  - `MOVE_WINDOW_NS = 20s`, `LOOKBACK_NS = 30s`, `LOOKAHEAD_NS = 30s`
  - `EXCLUDE_LAST_NS = 10s`, `GAMMA_LAST_NS = 10s` (loose; v2 tightens to 30s + 10s settlement)
  - `RANDOM_ANCHORS_PER_MARKET = 5`
  - Yes/No direction **inferred** from BTC start vs market end (v2 corrects this with gamma metadata)
- **Dependencies**: stdlib only (`json`, `bisect`, `random`, `statistics`, `multiprocessing`). POSIX-fork-based pool — won't run on Windows.
- **Outputs**: `/tmp/lag_summary.json`, `/tmp/lag_summary.txt`. Both ephemeral on VPS reboot.
- **Risks if used incorrectly**:
  - Yes/No inference is biased — v2's commit notes call this out as "look-ahead bias risk".
  - Settlement-bucket exclusion (10s) is too narrow.
  - No PM quality filters (spread, book size, freshness).
  - No OOS split.
  - Hardcoded TRAIN session — risk of accidentally repointing at HOLDOUT in future.
- **Recommendation**: **Archive.** Move to `experiments/historical/lag-v1/` (or similar). Do not use for any future OOS or sim work.

### `scripts/lag_analysis_v2.py`

- **Path**: `scripts/lag_analysis_v2.py`
- **Purpose**: v2 of the lag analysis on the recovered 17 GB session. Fixes v1's biases: gamma-metadata Yes-token (no inference), tighter settlement buckets (30s + 10s), PM quality filters at trigger time, three-way OOS period split (discovery / test / validation).
- **Kind**: **Obsolete** — superseded by v3 on the same data path.
- **Hardcoded session paths**: `SESSION_DIR = /home/polybot/polybot/data/session_20260427T100216Z` (TRAIN per manifest, the disk-full-era recovered session).
- **Hardcoded parameters**:
  - Same thresholds, windows, lookback/lookahead as v1.
  - `EXCLUDE_LAST_MAIN_NS = 30s`, `EDGE_LO_NS = 30s`, `EDGE_HI_NS = 10s`, `SETTLEMENT_NS = 10s`.
  - `QUAL_MAX_SPREAD = 0.05`, `QUAL_MIN_BOOK_USD = 50.0`, `QUAL_MAX_QUOTE_AGE_NS = 60s`, `QUAL_MAX_TRADE_AGE_NS = 60s`.
  - `LATENCY_DIFFERENTIAL_MS = 118.5` (Binance ~121.5ms vs Polymarket ~3ms one-way from Hetzner).
  - `SESSION_START_NS = 1777284136_000_000_000` (2026-04-27T10:02:16Z).
  - `CUTOFF_NS = 1777523971_487807791` (last clean Binance bookTicker ts before the disk-full event).
- **Dependencies**: stdlib + `urllib.request` (one live HTTP call to Gamma `/events?series=btc-up-or-down-5m&closed=true` to build the up_token map).
- **Outputs**: `/tmp/lag_v2_summary.{json,txt}`.
- **Risks if used incorrectly**:
  - Hardcoded TRAIN session.
  - Embedded cutoff timestamp tied to a specific corruption event — useless for any other session.
  - One live network call to gamma at startup — depends on internet from the host.
  - POSIX-fork-only multiprocessing.
- **Recommendation**: **Archive.** Same destination as v1. Methodology is preserved in v3.

### `scripts/lag_analysis_v3.py`

- **Path**: `scripts/lag_analysis_v3.py`
- **Purpose**: v3 of the lag analysis. Same data path as v2; adds filter ablation (10 combinations of the four PM quality filters) + odds-band slicing (5 overlapping bands). Stores filter checks as individual booleans per trigger so post-pass aggregation is cheap and consistent.
- **Kind**: **Current discovery script.** This is the script whose parameters are frozen in `configs/lag_oos_baseline.toml`.
- **Hardcoded session paths**: `SESSION_DIR = /home/polybot/polybot/data/session_20260427T100216Z` (TRAIN).
- **Hardcoded parameters**: same as v2 except no edge-bucket constants (collapsed). Adds 10 filter combinations and 5 odds bands enumerated explicitly in the docstring.
- **Dependencies**: same as v2 (stdlib + urllib for gamma).
- **Outputs**: `/tmp/lag_v3_summary.{json,txt}`.
- **Risks if used incorrectly**:
  - Same as v2 (hardcoded train session, embedded cutoff, gamma dependency, POSIX fork).
  - Filter-ablation × odds-band × thresholds is a large family; v3 produces all of them but does NOT correct for multiple testing internally — interpretation discipline is needed (PRV-003 in the risk register).
- **Recommendation**: **Refactor — base for `scripts/lag_oos.py` (T-B02).** Specifically:
  1. Replace hardcoded `SESSION_DIR` and `SESSION_START_NS`/`CUTOFF_NS` with manifest-driven inputs (`--manifest manifests/sessions.toml --split train|holdout`).
  2. Replace inline parameter constants with a load from `configs/lag_oos_baseline.toml`; refuse to run if `sweep_permitted = true` or `holdout_tuning_permitted = true`.
  3. Replace `/tmp/...` outputs with `experiments/<run_id>/{report.md,metrics.json,experiment.json,logs/}` per `docs/REPRODUCIBILITY_AND_EXPERIMENT_STANDARD.md`.
  4. Refuse to start if `--split holdout` is passed and `manifests/sessions.toml [splits.holdout]` is empty.
  5. Strip "edge"/"alpha"/PnL language from any output text — Phase 3 reports lag/response stats only.
  6. Add a `--family` lock that pre-registers the slice/threshold combinations; reject any new combination at runtime.
  7. Replace gamma-at-startup with reading `_resolutions.ndjson` (already present from the resolution sweeper) so the run is offline-deterministic.

### `scripts/lag_run_confirm.py`

- **Path**: `scripts/lag_run_confirm.py`
- **Purpose**: Pre-run sanity check before launching the full v2/v3 lag analysis on the recovered session. Counts markets at each filter stage (in window, parseable epoch, fully-in-window, has data, has resolution).
- **Kind**: Diagnostic / pre-flight checker.
- **Hardcoded session paths**: `SESSION = /home/polybot/polybot/data/session_20260427T100216Z` (TRAIN).
- **Hardcoded parameters**: `SESSION_START_NS = 1777284136_000_000_000`, `CUTOFF_NS = 1777523971_487807791` — same disk-full-era constants as v2/v3.
- **Dependencies**: stdlib only.
- **Outputs**: stdout only (text summary). No file writes. Read-only on filesystem.
- **Risks if used incorrectly**: Hardcoded session + cutoff. Will print misleading numbers if pointed at any other session without code changes.
- **Recommendation**: **Refactor.** Make `SESSION` and `CUTOFF_NS` arguments. Useful pattern as a generic "what's analysable in this session?" tool — should live alongside the future OOS harness.

---

## Other discovery / case-study scripts

### `scripts/casestudy_analysis.py`

- **Path**: `scripts/casestudy_analysis.py`
- **Purpose**: 10-market deep-dive validation. Phases: no-arb sweep over 50 random samples; manual walkthroughs; stratified market selection (3 active / 3 quiet / 2 mid / 2 purposive); per-market case study; cross-market summary.
- **Kind**: One-off analysis (already run; informed the v2/v3 design).
- **Hardcoded session paths**: `SESSION_DIR = session_20260426T122347Z` (TRAIN, 13h gold).
- **Hardcoded parameters**:
  - `BIN_ONEWAY_MS = 121.5`, `POLY_ONEWAY_MS = 3.0` (latency floor measurements).
  - `KEY_BIN_BPS = 5.0`, `KEY_BIN_ISOLATION_NS = 30s`.
  - `PM_REACTION_THRESHOLD_PP = 0.05`, `PM_REACTION_NULL_PP = 0.02`.
  - `RNG_SEED = 20260429`.
  - Stratification counts: 3/3/2/2.
- **Dependencies**: stdlib only.
- **Outputs**: `/tmp/casestudy_summary.{json,txt}`.
- **Risks if used incorrectly**: Hardcoded TRAIN session; the random seed makes the sample reproducible but bias-locked.
- **Recommendation**: **Archive.** The case study has already informed the v2/v3 work. The pattern (stratified sampling + per-market trace) is worth keeping — should be reborn as part of a Phase 4-era post-trade audit tool, not as a TRAIN analysis script.

### `scripts/scan_polymarket_swings.py`

- **Path**: `scripts/scan_polymarket_swings.py`
- **Purpose**: Find Polymarket markets where mid moved ≥50pp within 20s — used to find candidate windows for deeper analysis.
- **Kind**: Diagnostic (case-finding).
- **Hardcoded session paths**: `SESSION = session_20260426T122347Z` (TRAIN, 13h gold).
- **Hardcoded parameters**: `THRESHOLD_MOVE = 0.50` (50pp), `WINDOW_NS = 20s`. 4-process pool.
- **Dependencies**: stdlib only. POSIX-fork-only multiprocessing.
- **Outputs**: `/tmp/odds_swings_top5.{json,txt}`.
- **Risks if used incorrectly**: Hardcoded TRAIN session. The 50pp threshold is arbitrary; lowering it on holdout would constitute parameter exploration.
- **Recommendation**: **Refactor.** Make `SESSION` and `THRESHOLD_MOVE` arguments. Useful for picking windows to inspect; keep it explicitly out of the OOS verdict.

---

## Diagnostic / data-quality scripts

### `scripts/integrity_supplementary.py`

- **Path**: `scripts/integrity_supplementary.py`
- **Purpose**: Per-file partial-write corruption scan — line-by-line JSON parse, classify file as clean / tail-truncated / interspersed-corruption. Complements the Rust `replayer integrity` subcommand. This is the script that originally surfaced the `session_20260427T100216Z` truncation.
- **Kind**: Diagnostic (still useful in 2026 for any session under suspicion).
- **Hardcoded session paths**: `SESSION_DIR = session_20260427T100216Z` (the original target).
- **Hardcoded parameters**: 4-process pool; family classifier (binance bookTicker / depth / trade; polymarket admin / resolved / market; coinbase market_trades).
- **Dependencies**: stdlib only. POSIX-fork-only multiprocessing.
- **Outputs**: `/tmp/integrity_supplementary.{json,txt}`.
- **Risks if used incorrectly**: Hardcoded session. Output to `/tmp` is ephemeral.
- **Recommendation**: **Refactor.** Make `SESSION_DIR` a CLI arg. This is a generic diagnostic — should be runnable against the holdout-candidate session as one of the Gate 2 (holdout-ready) checks. Cross-references INT-005 in the risk register.

---

## One-shot codegen / utility

### `scripts/migrate_rawevent.py`

- **Path**: `scripts/migrate_rawevent.py`
- **Purpose**: One-shot codegen that inserts `..Default::default()` into every `RawEvent { ... }` literal in a fixed list of Rust source files, so new optional fields (e.g., `wire_batch_id` from phase 5) stay backward-compatible.
- **Kind**: One-off (idempotent — re-running is a no-op).
- **Hardcoded paths**: 10 Rust source files in `crates/common`, `crates/storage`, `crates/replayer`, `apps/replayer/tests`.
- **Hardcoded parameters**: none (just the file list).
- **Dependencies**: stdlib only. **MODIFIES Rust source** in `crates/`.
- **Outputs**: rewrites the files in-place; prints a count per file.
- **Risks if used incorrectly**:
  - Modifies source code. Already run; running again is a no-op, but the source-file list is hardcoded — if it drifts from reality, no error.
  - Should never run unattended without explicit task allowance — falls under `CODE_SAFE` at minimum.
- **Recommendation**: **Archive.** The job is done. Keep for reference if a new optional field gets added to `RawEvent` in the future. Move to `scripts/historical/` (or similar) so it's not in the working-script ambit.

---

## VPS / ops scripts

### `scripts/vps_health_check.sh`

- **Path**: `scripts/vps_health_check.sh`
- **Purpose**: Remote VPS health check — service status, file growth (line counts), latest health snapshot, chrony, disk free, recent reconnect count.
- **Kind**: Ops diagnostic.
- **Hardcoded session paths**: `SESSION = session_20260426T122347Z`. **STALE** — the recorder has restarted multiple times since this session ended; this script will report on a session that ended weeks ago.
- **Hardcoded parameters**: `journalctl --since "1 hour ago"` reconnect count.
- **Dependencies**: bash + `wc`, `tail`, `chronyc`, `df`, `journalctl`, `xargs`.
- **Outputs**: stdout only.
- **Risks if used incorrectly**: Stale session path → report shows nothing useful or last-known stale numbers. Operator could miss a real outage.
- **Recommendation**: **Refactor.** Replace the hardcoded `SESSION=` line with `SESSION=$(ls -1dt /home/polybot/polybot/data/session_*/ | head -1)` so it always points at the live session. Cross-reference: this is the pattern already used in `docs/NEXT_SESSION.md` for re-running integrity.

### `scripts/vps_polymarket_census.sh`

- **Path**: `scripts/vps_polymarket_census.sh`
- **Purpose**: One-off Polymarket payload schema discovery — event-type distribution and per-type top-level keys, sampled from the largest market files. Output was committed to `docs/polymarket_census.txt` (per the script header).
- **Kind**: One-off (already run; result committed).
- **Hardcoded session paths**: `SESSION = session_20260426T122347Z`.
- **Outputs**: stdout (intended to be redirected to `docs/polymarket_census.txt`).
- **Risks if used incorrectly**: Will run against an old session if not updated; output structure assumes the v3 (working) form of the script.
- **Recommendation**: **Archive.** The job is done. The output committed in `docs/polymarket_census.txt` is the durable artifact.

### `scripts/vps_polymarket_inner_keys.sh`

- **Path**: `scripts/vps_polymarket_inner_keys.sh`
- **Purpose**: Polymarket inner-payload field-shape drill. Sampled `bids[0]` / `asks[0]` shape from `book` events and `price_changes[0..1]` shape from `price_change` events. Used to nail down the decoder field shape before edits to `crates/replayer/src/decode/polymarket.rs`.
- **Kind**: One-off (already run; informed the decoder).
- **Hardcoded session paths**: `SESSION = session_20260426T122347Z`.
- **Outputs**: stdout (intended to be redirected to `docs/polymarket_inner_keys.txt`).
- **Risks if used incorrectly**: Same as census — runs on stale session if rerun without updating.
- **Recommendation**: **Archive.** The decoder has been updated based on its output; the data discovery is durable.

---

## Production ops infrastructure (keep as-is)

### `scripts/disk_guard.sh`

- **Path**: `scripts/disk_guard.sh`
- **Purpose**: Out-of-process disk-pressure protection for the recorder. Tiered actions: WARN at <20 GB, ALERT at <10 GB, COMPRESS oldest non-live session at <5 GB, STOP recorder service at <2 GB.
- **Kind**: Production ops infrastructure.
- **Hardcoded paths**: `DATA_ROOT=/home/polybot/polybot/data`, `STATE_DIR=/var/lib/polybot`, `LOG_DIR=/var/log/polybot`, `RECORDER_SERVICE=polybot-recorder.service`. All env-overridable.
- **Hardcoded parameters**: GB thresholds (20/10/5/2), `LIVE_THRESHOLD_SECS=120`, `COMPRESS_AGE_HOURS=24`. All env-overridable.
- **Dependencies**: bash, `df`, `find`, `gzip`, `systemctl`. POSIX.
- **Outputs**: log lines to stdout and `/var/log/polybot/disk_guard.log`; state file `/var/lib/polybot/disk_guard.state`. Exit codes 0/2/3/4/10 documented in header.
- **Risks if used incorrectly**:
  - **STO-001**: COMPRESS action not tested under real disk pressure.
  - **STO-002**: STOP action not tested under real disk pressure.
  - **STO-003**: COMPRESS_AGE_HOURS boundary not tested at 23h59m vs 24h01m.
  - **CAN STOP THE RECORDER** on its own — under unattended rules, manual invocation requires VPS_MUTATION_APPROVAL_REQUIRED, but the systemd timer auto-fires every 5 min as designed.
- **Recommendation**: **Keep.** Production guard. Tracked risks STO-001/002/003 stay open until a controlled fill test exercises the COMPRESS / STOP paths.

### `scripts/disk_guard.service`

- **Path**: `scripts/disk_guard.service`
- **Purpose**: systemd unit (oneshot) that runs `disk_guard.sh`. SuccessExitStatus=2,3,4 so the unit doesn't fail when disk_guard takes its designed actions.
- **Recommendation**: **Keep.**

### `scripts/disk_guard.timer`

- **Path**: `scripts/disk_guard.timer`
- **Purpose**: systemd timer that fires the disk_guard service every 5 min (`OnUnitActiveSec`, not `OnCalendar`, so a slow run can't overlap).
- **Recommendation**: **Keep.**

### `scripts/deploy.sh`

- **Path**: `scripts/deploy.sh`
- **Purpose**: One-command deploy to the recorder VPS — `git pull --ff-only`, `cargo build --release`, `systemctl restart polybot-recorder`, then `journalctl -fu polybot-recorder`.
- **Kind**: Operational tool.
- **Hardcoded paths**: `VPS_HOST=159.69.0.78` (env-overridable). Path on VPS: `/home/polybot/polybot`.
- **Risks if used incorrectly**:
  - **DEPLOY_APPROVAL_REQUIRED**: this script restarts the live recorder. Forbidden under unattended-work rules without explicit user approval per invocation.
  - Includes a `git pull` on the VPS — if the local branch isn't fast-forward, the VPS pull fails and the deploy halts cleanly, but the cargo build still happens against the previous commit.
- **Recommendation**: **Keep.** The script is correct; the policy boundary (no unattended invocation) is documented in `docs/CLAUDE_UNATTENDED_WORK_RULES.md` §2 and §11.

---

## VPS-side `/tmp/lag_*` artifacts (informational — not in the repo)

Per the earlier inventory of `159.69.0.78:/tmp` (2026-05-03 read-only check), the VPS has copies of `lag_analysis*.py` and their summary outputs in `/tmp`:

| Path | Mirror of | Notes |
|------|-----------|-------|
| `/tmp/lag_analysis.py` | `scripts/lag_analysis.py` | mtime 2026-04-28 |
| `/tmp/lag_analysis_v2.py` | `scripts/lag_analysis_v2.py` | mtime 2026-05-02 |
| `/tmp/lag_analysis_v3.py` | `scripts/lag_analysis_v3.py` | mtime 2026-05-02 |
| `/tmp/lag_run_confirm.py` | `scripts/lag_run_confirm.py` | mtime 2026-05-02 |
| `/tmp/lag_summary.{json,txt}` | output of v1 | references `session_20260426T122347Z` |
| `/tmp/lag_v2_summary.{json,txt}` | output of v2 | references `session_20260427T100216Z` |
| `/tmp/lag_v3_summary.{json,txt}` | output of v3 | references `session_20260427T100216Z` |
| `/tmp/lag.log`, `/tmp/lag_v2.log`, `/tmp/lag_v3.log` | run logs | no session_id matches |

These were the basis for the provenance audit recorded in `manifests/sessions.toml` for `session_20260502T071957Z` — none of them target it, supporting the "untouched as far as we can verify" claim.

**Recommendation**: leave as-is on the VPS for now. They are not repository artefacts. When `experiments/<run_id>/` becomes the canonical location (per `docs/REPRODUCIBILITY_AND_EXPERIMENT_STANDARD.md`), `/tmp/lag_*` becomes legacy exploration and can be deleted in a future ops sweep — under VPS_MUTATION_APPROVAL_REQUIRED.

---

## Cross-references to risk register

- **ANL-001** "Analysis scripts still need organisation" — this inventory is the first half of the resolution. Action items derived from it become Backlog tasks.
- **ANL-002** "OOS validation not yet run" — `lag_analysis_v3.py` is the base for the Phase 3 OOS harness (T-B02).
- **INT-005** "safe replay cutoff behaviour should be verified on damaged sessions" — `integrity_supplementary.py` already exercises this against the disk-full-era session; refactoring it (above) would help generalise.
- **PRV-001..PRV-005** Reproducibility & provenance gaps — every refactor in this document moves a script closer to the `experiment.json` standard.
- **STO-001/002/003** disk_guard COMPRESS/STOP/AGE-boundary not tested — flagged on `disk_guard.sh` above; no change here, just visibility.
- **WRK-001** Runner / secrets / approval boundary — `deploy.sh` is the canonical example of DEPLOY_APPROVAL_REQUIRED.

---

## Summary recommendation

| Disposition | Count | Files |
|-------------|-------|-------|
| **Keep as-is** | 4 | `disk_guard.sh`, `disk_guard.service`, `disk_guard.timer`, `deploy.sh` |
| **Refactor** | 4 | `lag_analysis_v3.py` (→ basis for `lag_oos.py`), `lag_run_confirm.py`, `scan_polymarket_swings.py`, `integrity_supplementary.py`, `vps_health_check.sh` (5 if counting both — see below) |
| **Archive** | 6 | `lag_analysis.py` (v1), `lag_analysis_v2.py`, `casestudy_analysis.py`, `migrate_rawevent.py`, `vps_polymarket_census.sh`, `vps_polymarket_inner_keys.sh` |
| **Delete** | 0 | — |
| **Move** | (overlaps Archive) | All "Archive" candidates → `scripts/historical/` (proposal). The directory does not exist yet; creating it is part of the archival action |

(The Refactor count is 5 — `vps_health_check.sh` was double-counted into "4" above. The corrected count is 5 Refactor + 6 Archive + 4 Keep = 15.)

**No script should be deleted outright** — all of them either inform current work or document a one-off methodology worth preserving.

---

## Proposed next moves (require user approval)

These are suggestions, not actions:

1. Create `scripts/historical/` and move the 6 Archive items there in a single commit. **Approval needed.**
2. Refactor `lag_analysis_v3.py` into `scripts/lag_oos.py` per task **T-B02** in `docs/TASK_BOARD.md`. Blocked behind holdout promotion (T-B01); design needs review before commit.
3. Refactor `lag_run_confirm.py`, `integrity_supplementary.py`, `scan_polymarket_swings.py` to take `SESSION` as a CLI argument. Each is a small, focused change — could be one PR or three. **Approval needed.**
4. Refactor `vps_health_check.sh` to use `ls -1dt …/session_*/ | head -1` so it always points at the live session. Trivial. **Approval needed.**

None of these are started in this round.
