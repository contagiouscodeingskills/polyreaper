# Reproducibility & Experiment Standard

> **Purpose.** Every analysis, OOS test, backtest, simulator run, shadow run, and report must be **reproducible**. A future reader (you in 6 weeks, the user, or a reviewer) should be able to pick up the artefacts and reproduce the result bit-for-bit. This file defines the floor.

Last updated: 2026-05-03.

## What this standard applies to

Anything that produces a number, a chart, a verdict, a P&L, a calibration constant, or a "yes/no, the signal works" claim. Concretely:

- OOS validation runs (Phase 3).
- Execution-simulator runs (Phase 4).
- Shadow-trading daily / weekly reports (Phase 5).
- Live-execution reconciliations (Phase 7).
- Ad-hoc analyses — including informal ones. There is no "informal" exception.

Anything that does NOT touch a metric — pure code refactors, doc edits, schema migrations — is exempt.

## Where artefacts live

- Each experiment lives under `experiments/<run_id>/` in the repo, where `<run_id>` is `YYYYMMDDTHHMMSSZ-<short-purpose>` (e.g. `20260505T010000Z-oos-lag-baseline`).
- Inside that directory:
  - `experiment.json` — the provenance record (see §1).
  - `report.md` — human-readable summary.
  - `metrics.json` — machine-readable metrics.
  - `inputs/` — symlinks or hashes pointing at the manifest, params, dataset slices used.
  - `outputs/` — figures, tables, derived datasets.
  - `logs/` — stdout / stderr captures from the run.
- Raw market data NEVER lives in `experiments/`. Only references and hashes.

If `experiments/` doesn't exist yet, the first experiment creates it.

---

## 1. Required `experiment.json` fields

Every run writes an `experiment.json` with **all** of these fields. A run that cannot fill all of them is not a real experiment — it's exploration, and its outputs do not enter the record.

```json
{
  "experiment_id": "20260505T010000Z-oos-lag-baseline",
  "purpose": "Phase 3 OOS validation of frozen lag baseline against promoted holdout",
  "status": "succeeded | failed | aborted",

  "data": {
    "session_ids": ["session_20260503T000248Z"],
    "split_used": "holdout",
    "manifest_path": "manifests/sessions.toml",
    "manifest_sha256": "<hex>",
    "raw_data_status": "plain | gzipped | mixed",
    "session_dir_paths_resolved": ["/home/polybot/polybot/data/session_20260503T000248Z"],
    "time_range_start_ns": "1777766568779821774",
    "time_range_end_ns": "<...>",
    "cutoff_timestamps": {
      "endgame_exclude_last_seconds": 30,
      "safe_replay_cutoff_ns_per_venue": {"binance": "<...>", "polymarket": "<...>"}
    }
  },

  "code": {
    "git_commit_sha": "<full sha>",
    "git_dirty": false,
    "git_remote": "<url>",
    "recorder_version": "0.1.0",
    "replayer_version": "0.1.0",
    "schema_version": 1,
    "rust_version": "1.XX.X",
    "python_version": "3.XX.X",
    "tool_versions": {"polars": "...", "pyarrow": "..."}
  },

  "params": {
    "params_path": "configs/lag_oos_baseline.toml",
    "params_sha256": "<hex>",
    "params_frozen": true,
    "tuned_during_run": false,
    "random_seed": null
  },

  "execution": {
    "command_line": "scripts/lag_oos.py --manifest manifests/sessions.toml --params configs/lag_oos_baseline.toml --split holdout",
    "host": "polybot-8gb-nbg1-1",
    "user": "polybot",
    "started_at_iso": "2026-05-05T01:00:00Z",
    "ended_at_iso": "2026-05-05T01:14:32Z",
    "elapsed_seconds": 872,
    "exit_code": 0
  },

  "outputs": {
    "report_path": "experiments/20260505T010000Z-oos-lag-baseline/report.md",
    "metrics_path": "experiments/20260505T010000Z-oos-lag-baseline/metrics.json",
    "figures": ["experiments/20260505T010000Z-oos-lag-baseline/outputs/<...>"],
    "log_paths": ["experiments/20260505T010000Z-oos-lag-baseline/logs/<...>"]
  },

  "metrics_summary": {
    "sample_sizes_per_slice": {"5bps_0.30-0.70": 0, "...": 0},
    "min_sample_size_required": 0,
    "min_sample_size_met": true,
    "primary_metric_name": "same_direction_rate_5bps_0.30-0.70",
    "primary_metric_value": 0.0,
    "primary_metric_ci_method": "wilson | bootstrap | none",
    "primary_metric_ci_lower": 0.0,
    "primary_metric_ci_upper": 0.0,
    "multiple_testing_correction": "none | bonferroni | holm | fdr",
    "family_size": 0
  },

  "limitations": [
    "<known caveat 1>",
    "<known caveat 2>"
  ],

  "holdout_touched": false,
  "holdout_tuning_attempted": false,

  "verdict": {
    "claim": "<one-sentence claim, e.g. 'lag signal survives holdout at baseline'>",
    "supports_promotion_to_next_gate": true,
    "promotion_gate_referenced": "Gate 3 — OOS signal validation"
  }
}
```

The fields are required. "`null`" is fine where a field genuinely doesn't apply (e.g., `random_seed` for a deterministic script). "Not implemented yet" is not fine.

---

## 2. Hashing

- All file hashes are SHA-256, hex-encoded, lowercase.
- Hash the file content, not the path. Whitespace counts.
- Compute hashes inside the run and embed them in `experiment.json`. A separate "verify hashes" pass at read time confirms reproducibility.
- For datasets that are too large to read twice, hash the manifest entry (which itself names the session and a per-file checksum if available). This is a known limitation tracked in PRV-001 — when the recorder grows per-file checksums, hash those.

---

## 3. Hard rules

These are non-negotiable. Violating any of them voids the experiment.

### Holdout discipline

- **Never tune on holdout.** Parameter search, threshold scans, slice selection — none of these may run with `--split holdout` or any session in `[splits.holdout]`.
- **Never change baseline after a holdout run without a new `experiment_id`.** If the baseline changes, the previous holdout result no longer applies; a new run with a new ID is required.
- **`scripts/lag_oos.py` must refuse to run** if `configs/lag_oos_baseline.toml` has `sweep_permitted = true` or `holdout_tuning_permitted = true`, OR if `manifests/sessions.toml [splits.holdout]` is empty when `--split holdout` is requested.

### Edge claims

- **No report can claim "edge", "alpha", "the signal works", or any tradable conclusion without:**
  - artefact paths to the metrics file and the run log,
  - reproducible config (the params file and its hash),
  - a sample-size column per metric,
  - a confidence interval (or an explicit "uncertainty not estimated" note explaining why).
- **PnL or EV may not be claimed until the execution simulator (Phase 4) exists.** Phase 3 reports lag/response statistics only — same-direction rate, opposite rate, no-response rate, median lag, p90 lag, sample sizes — and explicitly states "no PnL claim".
- **Signal-validation reports must not claim tradable edge.** Even if same-direction rate looks great, the report's conclusion is "the lag signal has structure on holdout" — never "the strategy is profitable".
- **Numbers presented without a sample size are invalid.** A "78% same-direction" rate with N=23 is not the same number as with N=2,300 and must be reported as such.

### Provenance integrity

- A report whose `experiment.json` contains `git_dirty: true` is provisional and labelled "uncommitted-code experiment, not for promotion". It cannot pass any promotion gate.
- A report whose `manifest_sha256` doesn't match the live manifest at read-time is stale; reproduce or re-run before citing.
- A report whose `params_sha256` doesn't match `configs/lag_oos_baseline.toml` is from a different baseline; either it gets a new experiment_id or it's voided.

---

## 4. Statistical-validity floor

Cross-reference: PRV-002, PRV-003 in `docs/UNSOLVED_PROBLEMS_AND_RISKS.md`.

### Sample size

- Every metric reports the sample size that produced it, per slice.
- A `min_sample_size_required` value is set per experiment type (initial proposal: N ≥ 200 events per slice for response-rate metrics; revisable upward if signal is weak).
- A slice that doesn't meet the minimum is reported as "underpowered, no conclusion" — not as a low-confidence result.

### Confidence intervals

- Rate metrics (e.g., same-direction rate) report 95% Wilson or Clopper-Pearson intervals.
- Continuous metrics (e.g., median lag) report 95% bootstrap intervals (≥1000 resamples) unless an analytic CI is named and justified.
- The CI method is recorded in `experiment.json:metrics_summary.primary_metric_ci_method`.
- A claim that hinges on a single point estimate without a CI is not a valid claim.

### Multiple testing

- When >1 slice / threshold / window is tested in the same family, a correction is applied: Bonferroni (default, conservative), Holm, or FDR (Benjamini-Hochberg) — named in `experiment.json:metrics_summary.multiple_testing_correction`.
- The family is pre-registered: the slices, thresholds, and windows considered are listed in `configs/lag_oos_baseline.toml` BEFORE the holdout run.
- Adding a slice / threshold / window post-hoc requires a new experiment_id; it does not retroactively join the family.

### Pre-registration

- Baseline parameters are frozen (in `configs/lag_oos_baseline.toml`) **before** the holdout is touched.
- Any change to the family of metrics, slices, or thresholds AFTER seeing holdout numbers invalidates the pre-registration. The result becomes exploratory; a fresh holdout (a future session) is required for a confirmatory run.

---

## 5. Reconciliation

(Forward-looking — applies once Phases 4–7 exist. Cross-reference: §10 of `docs/CLAUDE_UNATTENDED_WORK_RULES.md`.)

Every level of the system must reconcile against the level below it:

| Comparison | Rule | Action on divergence |
|------------|------|----------------------|
| **Replay vs simulator** | The Signal Engine fed a replay of session X must produce the SAME signals as the Signal Engine fed live data on the day session X was recorded. | Any signal-level divergence is a determinism bug. Stop, fix, do not promote. |
| **Simulator vs shadow** | Paper fills produced from a recording must match shadow paper fills produced from the live tap, with the same params. | Divergence → calibration review. Document delta in an experiment record. |
| **Shadow vs live** | Real fills must be no worse than (pessimistic) shadow paper fills by more than a documented threshold (TBD in `docs/PROMOTION_GATES.md` Gate 6). | Large divergence stops trading and reopens calibration. |
| **Expected vs actual fills** | Fill probability, fill price, fill size from the simulator must match observed live within tolerance. | Divergence beyond tolerance → simulator recalibration. |
| **Expected vs actual slippage** | Per-trade slippage observed live must be within bounded distribution of simulator estimate. | Divergence triggers review of the fill model. |
| **Expected vs actual latency** | End-to-end ack latency must match the calibrated `_latency_probes.ndjson`-based estimate. | Divergence triggers review of the delay model. |

Reconciliation runs are themselves experiments and follow this standard.

---

## 6. Anti-hallucination guardrails for reports

- A report's claims must be backed by entries in `metrics.json`. If you can't point at a number, you can't claim it.
- A report cannot summarise more confidently than the underlying CIs allow.
- "Looks good", "promising", "suggests" — these phrases are red flags. Replace with the actual number + CI + sample size.
- A report that contradicts an earlier committed report needs an explicit reconciliation section explaining why this run differs (data difference? code change? bug fix?). It does not silently supersede.
- Negative results are first-class. A holdout run that says "no edge at baseline, lower CI bound on same-direction rate is 48%, below the 50% null" is a successful experiment.

---

## 7. What a "good" experiment record looks like

Concrete checklist when reviewing whether an experiment passes this standard:

- [ ] `experiments/<run_id>/` directory exists and is committed (or proposed for commit).
- [ ] `experiment.json` has every field in §1 populated (no "tbd", no "later").
- [ ] `git_dirty` is `false` (or the experiment is explicitly labelled provisional).
- [ ] `manifest_sha256` matches the manifest used.
- [ ] `params_sha256` matches the params file used.
- [ ] `holdout_touched` is `false`, OR the experiment was an explicit holdout run with `params_frozen = true` and `tuned_during_run = false`.
- [ ] `metrics.json` exists; every metric has a sample size and CI (or "underpowered" / "uncertainty not estimated" with reason).
- [ ] `report.md` exists; every claim cites a number from `metrics.json`.
- [ ] `logs/` contains the full stdout + stderr.
- [ ] No raw market data is committed inside the experiment directory.
- [ ] The verdict.claim is a single sentence consistent with the metrics.

If any box is unticked, the experiment doesn't graduate from "exploration" to "record".

---

## 8. Migration from the current state

Cross-reference: PRV-001..PRV-005 in `docs/UNSOLVED_PROBLEMS_AND_RISKS.md`.

Today, analysis outputs live in `/tmp/lag_*` on the VPS, in `scripts/` locally, and as untracked summary JSONs. Those are **exploration, not records**. They were valid for parameter discovery on train, and the parameters they produced live in `configs/lag_oos_baseline.toml`.

When `scripts/lag_oos.py` (T-B02) is built, its first valid run produces the first record under `experiments/`. Going forward:

- Only artefacts under `experiments/<run_id>/` count as records.
- `/tmp/lag_*` on the VPS is exploration. It can be used to develop the harness; it does not enter the OOS verdict.
- Existing scripts in `scripts/` may be reorganised (T-R01) into a runnable form that emits the standard `experiment.json`.
