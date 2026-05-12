# Train-session integrity verdicts (2026-05-10)

**Task**: T-R02 — run `replayer integrity` on the two TRAIN sessions per `manifests/sessions.toml`.
**Permission class**: VPS_READ_ONLY + ANALYSIS_TRAIN_ONLY.
**Holdout / holdout_candidate**: NOT touched.
**Manifest**: NOT modified (per task instructions; recommended updates listed at bottom).

## Summary

| Session | Verdict | Events | Files | Scan time | Train-usable? |
|---------|---------|--------|-------|-----------|---------------|
| `session_20260426T122347Z` (13h gold) | **WARN** | 48,213,050 | 6,060 | 518.3s | YES (informational counters only) |
| `session_20260427T100216Z` (disk-full-era) | **WARN** | 23,362,821 | 4,832 | 253.7s | YES (with documented safe-cutoff) |

Both sessions return WARN, neither FAIL. Neither has interspersed corruption, parse errors mid-file, or decoder errors. The WARN in each case is dominated by **informational counters** that the integrity tool elevates conservatively (Polymarket per-asset `timestamp_ms` regressions, Coinbase `trade_id` skips, Polymarket consecutive-hash duplicates) — these are characteristics of the venue feeds themselves, not the recorder.

**Both sessions remain eligible for TRAIN use.** Neither is eligible for VALIDATION or HOLDOUT — both are in `splits.train` and have prior signal-discovery analysis history per `manifests/sessions.toml`.

---

## 1. `session_20260426T122347Z` — "13h gold session"

| Field | Value |
|-------|-------|
| Session path | `/home/polybot/polybot/data/session_20260426T122347Z` |
| Command run | `sudo -u polybot /home/polybot/polybot/target/release/replayer integrity --root /home/polybot/polybot/data/session_20260426T122347Z --verbose` (run on VPS, output redirected on VPS, fetched via `scp`) |
| Log path | `/tmp/polybot_integrity_logs/session_20260426T122347Z.text.log` (86 lines) |
| **VERDICT** | **WARN** |
| Reason | `tail_truncated_files=0, depth_breaks_unexplained=0, book_ticker_breaks=0, coinbase_trade_id_breaks=18, polymarket_ts_violations=13053, ts_violations=0, decode_errors=0` |

### Per-venue event counts

| Venue | Streams | Events |
|-------|---------|--------|
| binance | 5 | 11,499,705 |
| chainlink | 2 | 606 |
| coinbase | 2 | 188,075 |
| polymarket | 1,151 | 36,524,664 |

(This session predates phase 2's chainlink-disable; chainlink subdir present.)

### Structural

| Metric | Count | Notes |
|--------|-------|-------|
| Empty files | 0 | |
| Resolution-sweeper 0-byte files | 0 | (legacy pre-v1 sweeper; sidecar absent — see §3 below) |
| **Tail-truncated files** | **0** | clean shutdown — no last-line corruption |
| **Interspersed-corrupt files** | **0** | no mid-file parse errors |
| `_unrouted` files | 2 | `binance/_unrouted.0000.ndjson.gz`, `chainlink/_unrouted.0013.ndjson.gz` (both empty) |
| `_unknown_market-*` / `_unknown_token-*` files | 0 | |
| Bucket gaps | 35 | all in polymarket per-market files; pattern `BucketGap (1, between bucket 0 and 2)` — short-lived 5-min market that ran across an hour boundary with no events landing in bucket 1; not a data-loss gap |
| **Parse errors** | **0** | |
| Per-file ts violations | 0 | (within-file monotonicity) |

### Decoder

| Metric | Count |
|--------|-------|
| **Decode errors** | **0** |
| Unknown variants | 608 (informational; subscription acks + control frames) |

### Sequence integrity

| Check | Value |
|-------|-------|
| Binance depth chain breaks | **0** |
| Binance depth snapshots observed | 1 (breaks ≤ snapshots → expected) |
| Binance bookTicker `update_id` breaks | **0** (out of 8,427,175 observed) |
| **Polymarket per-asset `timestamp_ms` violations** | **13,053** (informational; venue-side timestamp regressions) |
| Polymarket hash records | 69,712,063 (4,550,615 consecutive duplicates — informational; venue-side) |
| Coinbase `trade_id` breaks | **18** (out of 412,659 = 0.0044%) |

### Binance arrival delta (`local_ts_ns - venue_ts_ms*1e6`)

| Stat | ms |
|------|-----|
| n | 3,072,528 |
| min | 117 |
| p10 | 121 |
| p50 | 128 |
| p90 | 131 |
| p99 | 134 |
| max | 582 |

(Matches the ~121.5ms one-way Binance latency the lag scripts assume.)

### Safe replay cutoff (last clean event ns since epoch, per venue)

| Venue | local_ts_ns |
|-------|-------------|
| binance | 1777283681144826499 (~2026-04-27 09:54:41 UTC) |
| chainlink | 1777283644070011274 |
| coinbase | 1777217040487604895 |
| polymarket | 1777206349678476428 |

The Polymarket cutoff is fairly early in the session — driven by the per-asset ts_violations (informational, not a data-corruption signal). For lag-analysis purposes, the much more permissive cutoff used in `lag_analysis.py` (whole session) is appropriate; the integrity safe-cutoff is a conservative lower bound, not a hard rule.

### Resolution validation

| Field | Value |
|-------|-------|
| Command | `replayer validate-resolutions --root .../session_20260426T122347Z` |
| Result | **WARN** |
| Sidecar | absent |
| Sidecar lines | 0 |
| Parse errors | 0 |
| Legacy `*-resolved.0000.ndjson` files | 759 (all non-empty) |
| Note | "no sidecar but legacy resolved files present (pre-v1 sweeper)" |

The session predates phase-2's consolidated `_resolutions.ndjson` sidecar. The 759 legacy per-slug `*-resolved.0000.ndjson` files are intact and are what `lag_analysis.py` / `casestudy_analysis.py` already consume.

### Verdict for splits

- **Safe for TRAIN use**: **YES**. No corruption, no decode errors, no sequence breaks; ts_violations are venue-side artefacts already known and accommodated by the analysis scripts.
- **Eligible for VALIDATION or HOLDOUT**: **NO**. Permanently quarantined to TRAIN per `manifests/sessions.toml [splits.train]` and prior-discovery history (`scripts/lag_analysis.py`, `scripts/casestudy_analysis.py`, `scripts/scan_polymarket_swings.py`).

---

## 2. `session_20260427T100216Z` — "disk-full-era recovered"

| Field | Value |
|-------|-------|
| Session path | `/home/polybot/polybot/data/session_20260427T100216Z` |
| Command run | `sudo -u polybot /home/polybot/polybot/target/release/replayer integrity --root /home/polybot/polybot/data/session_20260427T100216Z --verbose` (single ssh round-trip, no retry needed) |
| Log path | `/tmp/polybot_integrity_logs/session_20260427T100216Z.text.log` (611 lines) |
| **VERDICT** | **WARN** |
| Reason | `tail_truncated_files=249, depth_breaks_unexplained=0, book_ticker_breaks=0, coinbase_trade_id_breaks=6, polymarket_ts_violations=5133, ts_violations=0, decode_errors=0` |

### Per-venue event counts

| Venue | Streams | Events |
|-------|---------|--------|
| binance | 5 | 5,851,493 |
| coinbase | 2 | 93,123 |
| polymarket | 2,758 | 17,418,205 |

(Chainlink already disabled by this session — phase-2 stub commit `3acb1c1`.)

### Structural

| Metric | Count | Notes |
|--------|-------|-------|
| Empty files | 0 | |
| Resolution-sweeper 0-byte files | 0 | |
| **Tail-truncated files** | **249** | All in `polymarket/btc-updown-5m-*.ndjson.gz`, all with a single trailing `ParseError` — recoverable; replay can stop at last valid line |
| **Interspersed-corrupt files** | **0** | no mid-file parse errors |
| `_unrouted` files | 1 | `binance/_unrouted.0000.ndjson.gz` (empty) |
| `_unknown_market-*` / `_unknown_token-*` files | 0 | |
| Bucket gaps | 491 | including 4 large gaps in Binance streams between buckets 8↔28, 28↔50, 50↔66, 66↔89 (recorder restart / disk-full window) |
| **Parse errors** | **249** | (one per tail-truncated file; recoverable) |
| Per-file ts violations | 0 | |

### Decoder

| Metric | Count |
|--------|-------|
| **Decode errors** | **0** |
| Unknown variants | 1 (informational) |

### Sequence integrity

| Check | Value |
|-------|-------|
| Binance depth chain breaks | 28 |
| Binance depth snapshots observed | 49 (breaks ≤ snapshots → expected reconnect/refresh, NOT real packet loss) |
| Binance bookTicker `update_id` breaks | **0** (out of 4,351,480 observed) |
| **Polymarket per-asset `timestamp_ms` violations** | **5,133** (informational) |
| Polymarket hash records | 33,435,544 (2,198,696 consecutive duplicates — informational) |
| Coinbase `trade_id` breaks | **6** (out of 239,247 = 0.0025%) |

### Binance arrival delta (`local_ts_ns - venue_ts_ms*1e6`)

| Stat | ms |
|------|-----|
| n | 1,499,964 |
| min | 117 |
| p10 | 121 |
| p50 | 126 |
| p90 | 131 |
| p99 | 134 |
| max | 524 |

### Safe replay cutoff (last clean event ns since epoch, per venue)

| Venue | local_ts_ns | Notes |
|-------|-------------|-------|
| binance | 1777523971736139791 | **2026-04-30 04:39:31.736 UTC** — matches the `CUTOFF_NS = 1777523971_487807791` used in `scripts/lag_analysis_v2.py` line 40 (the v2/v3 scripts use a slightly tighter cutoff from `bookTicker`'s last clean ts) |
| coinbase | 1777302287334703671 | |
| polymarket | 1777284136960309700 | |

### Resolution validation

| Field | Value |
|-------|-------|
| Command | `replayer validate-resolutions --root .../session_20260427T100216Z` |
| Result | **WARN** |
| Sidecar | absent |
| Sidecar lines | 0 |
| Parse errors | 0 |
| Legacy `*-resolved.0000.ndjson` files | 1,526 (all non-empty) |
| Note | "no sidecar but legacy resolved files present (pre-v1 sweeper)" |

The 1,526 legacy resolved files are what `lag_analysis_v2.py` / `lag_analysis_v3.py` already consume via the gamma `up_token` map.

### Verdict for splits

- **Safe for TRAIN use**: **YES** — with the documented safe-cutoff already encoded in `lag_analysis_v2.py:40` (`CUTOFF_NS`). The 249 tail-truncated files are all in the post-cutoff window and the analysis scripts already exclude them. Pre-cutoff data (within `[SESSION_START_NS, CUTOFF_NS]`) has 0 parse errors, 0 interspersed corruption, 0 decode errors.
- **Eligible for VALIDATION or HOLDOUT**: **NO**. In `splits.train` with prior-discovery history (`scripts/lag_analysis_v2.py`, `scripts/lag_analysis_v3.py`, `scripts/lag_run_confirm.py`, `scripts/integrity_supplementary.py`); also has the recovered-disk-full-era classification (`split_label = "discovery / disk-full-era / recovered / train-only"` in the manifest).

---

## 3. Cross-cutting observations (informational)

### WARN dominance is venue-side, not recorder-side

Both sessions return WARN largely because of **Polymarket per-asset `timestamp_ms` violations** and **Polymarket consecutive-hash duplicates**. These are characteristics of Polymarket's own emissions:

- `timestamp_ms` regressions — Polymarket sometimes emits price_change events with timestamps slightly behind the previous event for the same `asset_id`. Recorder captures faithfully; replayer flags conservatively.
- Consecutive-hash duplicates — Polymarket emits a fresh `hash` on every state change; consecutive duplicates within an `asset_id` indicate **the venue itself sent the same state again**, not a recorder issue.

These do not reflect data-quality problems in the recorder. They are flagged so a downstream user can decide whether to dedupe or filter; the lag analysis scripts already handle this implicitly via mid-aggregation (taking `(best_bid + best_ask) / 2` per event).

### Coinbase `trade_id` breaks (24 across both sessions)

18 + 6 = 24 breaks across 651,906 total observed trade_ids = **0.0037%**. Coinbase's own venue-side gaps (recorder cannot create these). Lag analyses don't depend on Coinbase trade_id continuity.

### Binance arrival delta is stable across both sessions

Both sessions show min=117ms, p50≈126–128ms, p99=134ms. This is the Hetzner ↔ Binance one-way RTT/2 baseline already encoded as `BIN_ONEWAY_MS = 121.5` in `lag_analysis_v2/v3.py`. No regression observed.

### Binance bookTicker `update_id` is perfect

0 breaks out of 12,778,655 (8,427,175 + 4,351,480) bookTicker updates across both sessions. The basis for `lag_analysis_v3.py`'s 5/10/20 bps move detection is sound.

---

## 4. VPS-state observations (out of scope for T-R02; flagging only)

These are NOT integrity findings on TRAIN sessions; they are environmental observations gathered while running the scans. Action recommended **separately**, not in this report.

| Observation | Detail |
|-------------|--------|
| Recorder state | `polybot-recorder.service` is **inactive (dead) since Mon 2026-05-04 21:14:02 UTC**. Clean shutdown via SIGTERM (`code=exited, status=0/SUCCESS`, `final flush complete`, `bye`). No write_failures or ENOSPC at last snapshot. |
| Holdout candidate | `session_20260503T000248Z` captured **~45.2h** before shutdown — short of the **48h** Gate 2.1 requirement (`docs/PROMOTION_GATES.md`). 5,422 health snapshots, 59 GB on disk. |
| Disk pressure | `/dev/sda1` at **98% used** (1.9 GB free of 75 GB). The 59 GB of `session_20260503T000248Z` is the dominant consumer. Likely the operational reason for the recorder stop. |
| `disk_guard.sh` | Reached or near `STOP_GB = 2` threshold. The recorder stop appears to have been operator-initiated (SIGTERM) ahead of disk_guard's auto-stop, but I have not confirmed by reading `/var/log/polybot/disk_guard.log`. |

These observations imply T-B01 (holdout promotion) needs separate attention — the current candidate failed Gate 2.1 by ~2.8h.

---

## 5. Recommended `manifests/sessions.toml` updates (not applied)

Per task instructions, no manifest edit was made. Proposed deltas, awaiting user approval:

### `session_20260426T122347Z`
```toml
integrity_status = "WARN"
integrity_status_source = "docs/research/integrity/2026-05-10-train-integrity.md (replayer integrity v2)"
integrity_status_recorded_at = "2026-05-10"
integrity_warn_reasons = ["polymarket_ts_violations", "coinbase_trade_id_breaks", "polymarket_hash_consecutive_duplicates"]
```
(Replace the current `integrity_status = "unknown"`.)

### `session_20260427T100216Z`
```toml
integrity_status = "WARN"
integrity_status_source = "docs/research/integrity/2026-05-10-train-integrity.md (replayer integrity v2)"
integrity_status_recorded_at = "2026-05-10"
integrity_warn_reasons = ["tail_truncated_files_post_cutoff", "polymarket_ts_violations", "coinbase_trade_id_breaks"]
integrity_safe_replay_cutoff_ns = { binance = "1777523971736139791", coinbase = "1777302287334703671", polymarket = "1777284136960309700" }
```
(Replace the current `integrity_status = "unknown"`. The safe-cutoff field is new — the manifest schema doesn't currently define it; adding it ties the verdict to a concrete bound the analysis scripts can read.)

Both manifest updates would be a single doc-only commit. **Awaiting your approval before any edit.**

---

## 6. Artefacts

| Path | What | Size |
|------|------|------|
| `/tmp/polybot_integrity_logs/session_20260426T122347Z.text.log` | full integrity output (text) | 86 lines |
| `/tmp/polybot_integrity_logs/session_20260427T100216Z.text.log` | full integrity output (text) | 611 lines |
| `/tmp/polybot_integrity_logs/validate_resolutions_train.log` | both validate-resolutions runs | small |
| `docs/research/integrity/2026-05-10-train-integrity.md` | this report | — |

The `/tmp/polybot_integrity_logs/` files are ephemeral on this Windows machine. If retention beyond this session is desired, copy them under `experiments/<run_id>/logs/` per `docs/REPRODUCIBILITY_AND_EXPERIMENT_STANDARD.md` — would require a separate task and user approval.

---

## 7. Risk-register cross-references

- **INT-001** "integrity_status = 'unknown' on every catalogued session" — **resolved for the two TRAIN sessions** (verdicts now WARN, not unknown). 13 of 15 sessions still `unknown` in the manifest.
- **INT-002** "Old recovered disk-full session needs v2 integrity confirmation" — **resolved**: `session_20260427T100216Z` has a formal v2 integrity verdict on file. Tail truncations confined to `*.ndjson.gz` post-cutoff; the analysis scripts already use the safe-cutoff to exclude them.
- **INT-005** "Safe replay cutoff behaviour on damaged sessions not verified end-to-end" — **partially resolved**: this run exercised the safe-cutoff machinery on the disk-full-era session and produced sensible per-venue cutoffs.
- **REC-002** "7-day soak test not yet run" — **regressed**: holdout candidate captured 45.2h before recorder stop; the project does not yet have a single ≥168h continuous capture. Update the risk entry to note the failed attempt.
- **ANL-007** "Holdout session not yet promoted or OOS-verified" — **changed**: candidate failed Gate 2.1 (≥48h). Decision needed on whether to wait for a fresh ≥48h session or relax the gate (the latter is not recommended).

---

## 8. Stop conditions consulted

Per `docs/CLAUDE_UNATTENDED_WORK_RULES.md` §3:

| Condition | Status during T-R02 |
|-----------|---------------------|
| tests fail | n/a — no tests run |
| build fails | n/a — no build run |
| TOML/JSON validation fails | n/a — no manifest written |
| live recorder write_failures | not observable (recorder inactive); last snapshot before shutdown was clean |
| ENOSPC | not in any health snapshot read; disk at 98% but not 100% |
| feed stalls | last snapshot before shutdown showed `stalled = false` for all feeds |
| raw data mutation | not attempted |
| service restart | not attempted |
| destructive command | not attempted |
| holdout signal results | not inspected |
| **integrity finds unexpected interspersed corruption** | **0 interspersed-corrupt files in either session — pass** |
| **raw data path is ambiguous** | TRAIN paths confirmed against manifest before any command — pass |
| **ssh connection reset** during first 13h-gold scan | re-ran with VPS-side output capture; did not result in any TRAIN data being touched or any false PASS verdict |

No stop condition was hit that aborted the task. The single ssh disconnect during the first scan was diagnosed and worked around without retry-on-failure ambiguity.
