# Claude Unattended Work Rules

> **Purpose.** Define exactly how Claude may work on this project for multiple hours while the user is away. These rules are binding. When in doubt, stop and report — do not improvise.

Last updated: 2026-05-03.

## Reading order (start here)

When starting an unattended session, read these in order before doing anything else:

1. This file.
2. `docs/TASK_BOARD.md` — pick the highest-priority Ready task.
3. `docs/UNSOLVED_PROBLEMS_AND_RISKS.md` — for context on the task's related risks.
4. `docs/REPRODUCIBILITY_AND_EXPERIMENT_STANDARD.md` — if the task produces a report or runs an experiment.
5. `docs/PROMOTION_GATES.md` — if the task is gate-adjacent (holdout promotion, OOS, sim, shadow, live).

If any of these are missing or contradict each other, **stop and ask**.

---

## 1. Allowed unattended work

These are OK to do without per-task user approval, provided the task is on the Ready list and the action stays inside the listed permission class.

- Documentation writes / edits in `docs/` (DOCS_ONLY).
- Repo organisation: moving non-code files, fixing internal cross-references (DOCS_ONLY).
- Read-only metadata checks against the local repo or VPS (READ_ONLY, VPS_READ_ONLY).
- Adding / running unit and integration tests (TEST_ONLY, CODE_SAFE).
- Building non-live tools (offline analysis, replay tooling, dev binaries) (CODE_SAFE).
- Producing reports from already-permitted data (ANALYSIS_TRAIN_ONLY).
- Non-destructive refactors that have test coverage (CODE_SAFE).
- Running approved analysis scripts on **train data only** (ANALYSIS_TRAIN_ONLY) — sessions in `manifests/sessions.toml [splits.train]` only.

A task in **Ready** with permission class `DOCS_ONLY`, `READ_ONLY`, `CODE_SAFE`, `TEST_ONLY`, or `ANALYSIS_TRAIN_ONLY` may be started without further approval — but auto-commit rules below still apply.

---

## 2. Forbidden without explicit approval

Never do any of the following during unattended work, regardless of how reasonable it seems:

**Data**
- Delete raw market data.
- Move raw market data.
- Gzip / re-compress raw market data.
- Modify raw market data in any way.

**Recorder / VPS**
- Stop or restart the live recorder service.
- Deploy to the VPS (build, push, restart, anything that mutates `/home/polybot/`).
- Run `apt`, `systemctl`, or any system-level mutating command on the VPS.
- Modify the recorder config in production.

**Git**
- Merge to `main` (or any default branch).
- Force-push.
- `git reset --hard`, `git push --force`, `git checkout --` over uncommitted work.
- Use `--no-verify`, `--no-gpg-sign`, or otherwise bypass hooks.

**Trading**
- Place trades.
- Build or modify any code under a "live execution" path.
- Touch any wallet, signing, or API-key-handling code.
- Read or write secrets to disk.

**Holdout**
- Inspect any holdout signal result.
- Run any analysis whose `SESSION_DIR` matches a session in `manifests/sessions.toml [splits.holdout]`.
- Tune parameters using holdout data.
- Edit `manifests/sessions.toml` to add, remove, or move sessions in `[splits.holdout]`.
- Edit `configs/lag_oos_baseline.toml` after a holdout run has been recorded.

If the user has explicitly asked for one of the above in the current session message, that is approval — but only for the exact thing asked, not for adjacent things.

---

## 3. Stop conditions

Stop work **immediately** and report when any of the following is true. Do not try to "fix" the issue first; surface it.

- Tests fail (`cargo test`, `pytest`, etc.).
- Build fails (`cargo build`, `cargo check`).
- TOML / JSON / YAML validation fails on a file you wrote.
- The live recorder reports `write_failures > 0` in any health snapshot.
- Any health snapshot reports `disk_free_bytes` near zero or an `ENOSPC`-shaped error in logs.
- Any feed reports `stalled = true` for more than one consecutive snapshot.
- A required raw-data mutation appears (compress, move, delete) — refuse and escalate.
- A required service restart appears (recorder, systemd unit) — refuse and escalate.
- A result contradicts a previous assumption recorded in `docs/UNSOLVED_PROBLEMS_AND_RISKS.md` or in a prior commit's reasoning.
- You are unsure what the user would want.
- A task requires permission you don't have under the current permission class.
- A command would be destructive (deletes data, force-pushes, drops a database, etc.).

The right behaviour at a stop condition is: **do not commit, do not retry, write a clear report, end the unattended run.**

---

## 4. Permission classes

Every task carries one or more permission classes. They compose; "TEST_ONLY + CODE_SAFE" means you can write test code and run tests but not change non-test code. The classes:

| Class | What's allowed |
|-------|----------------|
| `READ_ONLY` | Read files in the local repo. No writes, no commits, no remote calls beyond plain HTTP GETs to public docs |
| `DOCS_ONLY` | Read + write under `docs/`. No code changes. Commits allowed per auto-commit rules |
| `CODE_SAFE` | Code changes that are demonstrably non-destructive (small surface, full test coverage, no behaviour change in unrelated paths). No commit without test pass |
| `TEST_ONLY` | Add or modify only test code and test fixtures. Run the full test suite |
| `ANALYSIS_TRAIN_ONLY` | Run approved analysis scripts against sessions in `splits.train`. Never against `holdout`, never against `holdout_candidate` |
| `ANALYSIS_HOLDOUT_APPROVAL_REQUIRED` | Any task involving a holdout session — even reading metadata — requires per-task user approval |
| `VPS_READ_ONLY` | SSH to the VPS as `root@` for read-only commands (`ls`, `cat`, `wc`, `tail`, `du`, `ps`, `journalctl --no-pager`, `systemctl is-active`). No writes, no `kill`, no service mutations |
| `VPS_MUTATION_APPROVAL_REQUIRED` | Anything on the VPS that writes or restarts. Always requires per-task user approval, even if "obviously needed" |
| `DEPLOY_APPROVAL_REQUIRED` | Build + push + restart anywhere. Always requires per-task user approval, never default-allow |
| `LIVE_TRADING_FORBIDDEN` | Live execution code, wallet/signing code, real orders, anything in `Do Not Start Yet` of the task board. Forbidden until explicitly unlocked by the user, after promotion gates 1–6 are signed off |

A task without an explicit permission class is implicitly `READ_ONLY`. If you find yourself wanting to do something the class forbids, that's a stop condition.

---

## 5. Required report after every task

Every task — even a "trivial" doc fix — produces this report. The report is the contract that the work happened the way you say it did.

```
Task:                       T-XNN — <title>
Permission class used:      <READ_ONLY | DOCS_ONLY | ... >

Files changed (paths):
  - <path/to/file.md>     (created | modified | deleted)
  - <path/to/file2.rs>    (modified)

Commands run:
  - <full command>        # exit code <N>
  - <full command>        # exit code <N>

Raw outputs / artefact paths:
  - <where the test output went>
  - <where the report went>
  - <experiment.json path if applicable>

Tests run:
  - <suite>               (pass | fail | skipped)
  - <suite>               (pass | fail | skipped)

What was NOT verified:
  - <thing that was not checked, with reason>
  - <thing that requires VPS mutation, deferred>

Risks introduced:
  - <new entry filed in UNSOLVED_PROBLEMS_AND_RISKS.md, ID>
  - <or: none>

Next task:
  - <T-XNN if a clear next, or: stopping for review>

Stop conditions hit:
  - <none | list>
```

If any field is missing or "tbd", the task is not done.

---

## 6. Final report at end of unattended run

When the unattended run ends (out of tasks, hit a stop condition, or end-of-time), produce one consolidated final report:

```
Unattended run summary
======================

Started:                    <UTC timestamp>
Ended:                      <UTC timestamp>
Reason for ending:          <out of Ready tasks | stop condition | scheduled end>

Completed tasks:
  - T-XNN — <title>            (commit <sha>)
  - T-XNN — <title>            (uncommitted, awaiting review)

Skipped tasks (with reason):
  - T-XNN — <title> — <reason — usually a stop condition or missing permission>

Commits created:
  - <sha>  <title>           <files-changed-count>
  - <sha>  <title>           <files-changed-count>

Files changed (aggregate):
  - <path>     <created | modified | deleted>

Tests:
  passed:  <count>
  failed:  <count> (must be 0 — otherwise stop condition)
  skipped: <count>

Open risks introduced this run:
  - <UNSOLVED-ID> — <one-line>

Recommended next action:
  - <pick from Ready, Blocked-becoming-unblocked, or "wait for user review">
```

This report goes to the user. The unattended run does not "succeed" without it.

---

## 7. Auto-commit rules

Auto-commit means: stage and commit changes without per-task user approval, after the task's tests/checks pass. The user still reviews the commit afterwards via `git log`.

| Task type | Auto-commit allowed? | Conditions |
|-----------|----------------------|------------|
| `DOCS_ONLY` task with explicit "auto-commit allowed" in the task definition | YES | Markdown parses; cross-references resolve |
| `DOCS_ONLY` task without that explicit allowance | NO — show diff, wait for approval | — |
| `TEST_ONLY` task | YES if all tests pass and only test files changed | `cargo test` green; only paths under `tests/` or `*_test.rs` modified |
| `CODE_SAFE` task with test coverage for the change | YES if tests pass AND task definition allows commit | Otherwise: show diff, wait |
| `CODE_SAFE` task without explicit commit allowance | NO | Show diff |
| Any task touching the recorder, replayer, configs/, manifests/ | NO | Always show diff and wait, even if tests pass |
| Any task that would merge to `main` | NEVER without user approval | — |
| Any task that would push to a remote | NEVER without user approval | — |
| Any task that would deploy or touch the VPS in a mutating way | NEVER without user approval | — |

Default to "show diff and wait". When in doubt, do not auto-commit. A pause is cheap; a bad auto-commit is expensive.

---

## 8. Anti-hallucination / over-reach control

These are non-negotiable.

- **Separate facts from assumptions.** A fact has a citation: a file path with line number, a command with its output, a commit hash, an artefact path. An assumption is anything else and must be labelled "assumption" or "unverified".
- **Claims require evidence.** "The recorder is healthy" → cite the last `_health.ndjson` snapshot. "The signal works" → cite the OOS report's `experiment.json`. "Tests pass" → cite the `cargo test` exit code and summary.
- **If evidence is missing, label as unknown.** Don't fill in plausible numbers. Don't extrapolate from one session to the population.
- **Do not turn metadata into proof of integrity.** A clean `_health.ndjson` is not an integrity verdict. Only `replayer integrity` PASS counts as integrity PASS, and only against the manifest entry being claimed.
- **Do not claim tradable edge from signal-only results.** Phase 3 OOS reports lag/response statistics; PnL/EV is Phase 4 (simulator) and beyond. Until then, signal results are evidence of structure, not of profitability.
- **Do not claim P&L without a fill model.** No paper-PnL on book-mid alone.
- **Do not extend fixes to unrelated files** unless the task says so.
- **If you find yourself reasoning "the user probably meant X"** when X exceeds the literal task scope: stop and ask.

---

## 9. Statistical-validity safeguards

Cross-reference: `docs/REPRODUCIBILITY_AND_EXPERIMENT_STANDARD.md` and `docs/PROMOTION_GATES.md`.

- Every metric report includes a sample size (per slice).
- Confidence intervals (or bootstrap intervals) are reported where applicable; methodology cited.
- Multiple-testing correction is named when reporting on >1 slice / threshold / window.
- No parameter search or threshold selection on holdout data.
- Baseline parameters are pre-registered (frozen in `configs/lag_oos_baseline.toml`) before holdout evaluation.
- A surprising result on holdout does NOT trigger re-tuning; it triggers an honest report.

---

## 10. Reconciliation expectations (forward-looking)

For phases 4–7 (when they exist):

- Replay vs simulator: signals computed from the same recording must be bit-identical. Divergence is a determinism bug.
- Simulator vs shadow: paper fills produced from a recording must match shadow paper fills produced from the live tap, given the same parameters. Divergence triggers a calibration review.
- Shadow vs live: real fills must be no worse than (pessimistic) shadow paper fills by more than a documented threshold; large divergence stops trading and reopens calibration.
- Expected vs actual fills, slippage, latency: all logged; divergence beyond threshold triggers review.

---

## 11. Runner / secrets / approval boundary (initial cut)

Cross-reference: WRK-001 in `docs/UNSOLVED_PROBLEMS_AND_RISKS.md`. A fuller `docs/RUNNERS_AND_SECRETS.md` will land later (T-G08).

For now:

- **Recorder runs on the VPS as `polybot`** under systemd unit `polybot-recorder.service`. Read-only inspection of the recorder's data is via `ssh root@159.69.0.78`.
- **Local development runs as the user on Windows** at `C:\Users\61429\Desktop\PolyBot`. No production credentials live here.
- **There are no project secrets today.** When wallet keys / API keys arrive (Phase 7 / Mercury / etc.) they will need a documented home; until then, any task that asks for a key is forbidden under unattended rules.
- **Live / wallet / signing tasks always require human approval** — they are in `Do Not Start Yet` on the task board. Even after they're unlocked, no self-approval is allowed: the user must explicitly approve each commit.
- **No self-approval for live-adjacent changes.** A task whose blast radius reaches live execution, the wallet, or the kill switch always shows the diff and waits, regardless of permission class.

---

## 12. Failure-injection drill expectations

Cross-reference: WRK-002. Full checklist lives in (forthcoming) drills doc; for now:

When the failure-injection drills doc is built, every drill produces a pass/fail row recorded against:

- ENOSPC drill (recorder behaviour)
- Feed-stall drill (Binance / Polymarket / Coinbase)
- Disconnect/reconnect drill
- Clock drift / chrony failure drill
- Stale-data drill (gates trading off)
- Duplicate-event drill
- Order reject drill (Phase 7)
- Partial-fill drill (Phase 7)
- Kill switch drill (Phase 6/7)

Until those drills exist, any related capability is `NEEDS VERIFICATION`, not "works".

---

## 13. Monitoring and artefact retention (initial)

- **Actionable alerts** (must be loud): recorder-down, feed-stalled > 60s, integrity FAIL on the last completed session, P&L floor near (live), kill-switch fired, wallet balance low (live), Polymarket API error-rate spike. Cross-reference: MON-002.
- **Dashboard-only info** (silent unless asked): per-feed msg-rate, reconnect counters, store latency p50/p99, disk-free trend.
- **Required artefacts for unattended runs**: every commit; every `experiment.json` per `docs/REPRODUCIBILITY_AND_EXPERIMENT_STANDARD.md`; every test output for any task that ran tests; the final report from §6.
- **Where reports/logs are saved**: under `experiments/<run_id>/` (committed) for analysis runs; otherwise in the chat transcript.
- **Retention (initial)**: commit everything that's small and reproducible-evidence; gzip large `_health.ndjson` etc. only by user instruction; raw market data stays out of git forever.

---

## 14. Hard live-risk controls (forward-looking)

These are tracked in `docs/PROMOTION_GATES.md` Gate 6 and in `docs/UNSOLVED_PROBLEMS_AND_RISKS.md` §11. Listed here so unattended-mode work knows the boundary it must not cross until they're built and approved.

- Max order size (per signal, per market).
- Max message rate to Polymarket (avoid bans).
- Max inventory / exposure (per market and account-wide).
- Max daily loss (account-wide).
- Max slippage (per fill).
- Stale-data block (no orders if data-quality red).
- Duplicate-order prevention (idempotency keys).
- Human kill switch (always available).
- Automatic kill switch (account-level breaches).

Until all of these are in place and tested, **any unattended work that touches live execution is forbidden**, regardless of how minor the change looks.

---

## 15. When in doubt

If you don't know whether a task is allowed, whether a result is good enough, whether a commit is safe, whether a stop condition has triggered, or whether the user would want X: **stop and ask**. The cost of asking is a small delay. The cost of getting it wrong is potentially data corruption, lost work, or — worst case — money.

Default behaviour at any boundary: do nothing, report, wait.
