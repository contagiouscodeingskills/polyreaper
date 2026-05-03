# Project Workflow Index

> **One page explaining where everything lives.** Read this first when picking up the project after a break, or when onboarding a new collaborator.

Last updated: 2026-05-03.

## TL;DR — what each piece is for

| Question | File | One-liner |
|----------|------|-----------|
| What are we building? | [`docs/BOT_ARCHITECTURE_AND_BUILD_PLAN.md`](BOT_ARCHITECTURE_AND_BUILD_PLAN.md) | Architecture blueprint — 10 systems + design rules + build order |
| What is wrong / uncertain? | [`docs/UNSOLVED_PROBLEMS_AND_RISKS.md`](UNSOLVED_PROBLEMS_AND_RISKS.md) | Single-source risk register (77+ items, severity-graded) |
| What do I do next? | [`docs/TASK_BOARD.md`](TASK_BOARD.md) | Ready / Blocked / Backlog / Done — pick the top Ready task |
| How can Claude work safely while I'm away? | [`docs/CLAUDE_UNATTENDED_WORK_RULES.md`](CLAUDE_UNATTENDED_WORK_RULES.md) | Allowed / forbidden actions, stop conditions, permission classes, auto-commit rules |
| How are results proven? | [`docs/REPRODUCIBILITY_AND_EXPERIMENT_STANDARD.md`](REPRODUCIBILITY_AND_EXPERIMENT_STANDARD.md) | Every analysis emits an `experiment.json` with provenance + sample sizes + CIs |
| When can we move to the next phase? | [`docs/PROMOTION_GATES.md`](PROMOTION_GATES.md) | 6 gates with measurable, evidence-backed checklists |
| Which data is train vs validation vs holdout? | [`manifests/sessions.toml`](../manifests/sessions.toml) | Authoritative split + per-session metadata + holdout candidate |
| What are the lag-signal parameters? | [`configs/lag_oos_baseline.toml`](../configs/lag_oos_baseline.toml) | Frozen baseline (NOT optimal); no sweeps; no holdout tuning |
| What are the project's design decisions? | [`docs/decisions.md`](decisions.md) | Active decisions with rationale |
| How is the VPS set up? | [`docs/SETUP_VPS.md`](SETUP_VPS.md) | Operational runbook |
| How is Windows dev set up? | [`docs/SETUP_WINDOWS.md`](SETUP_WINDOWS.md) | Toolchain notes |

## How the docs interact

```
              ┌────────────────────────────────────────┐
              │  BOT_ARCHITECTURE_AND_BUILD_PLAN.md    │  the "what"
              │      (10 systems + build order)        │
              └─────────────────┬──────────────────────┘
                                │
                ┌───────────────┴───────────────┐
                ▼                               ▼
      ┌─────────────────┐           ┌──────────────────────────┐
      │   TASK_BOARD    │◀──────────│  UNSOLVED_PROBLEMS_AND_  │  the "what's wrong"
      │   (the "next")  │  risks    │      RISKS               │
      └────────┬────────┘  drive    └──────────────────────────┘
               │           tasks
               ▼
      ┌──────────────────┐         ┌──────────────────────────┐
      │  CLAUDE_UNATTEN- │         │  REPRODUCIBILITY_AND_    │
      │  DED_WORK_RULES  │         │  EXPERIMENT_STANDARD     │
      │   (the "how")    │         │   (the "how proven")     │
      └────────┬─────────┘         └─────────────┬────────────┘
               │                                 │
               └──────────────┬──────────────────┘
                              ▼
                  ┌──────────────────────────┐
                  │     PROMOTION_GATES      │  the "when next phase"
                  └────────────┬─────────────┘
                               │
                               ▼
                ┌──────────────────────────────┐
                │  manifests/sessions.toml     │  data discipline
                │  configs/lag_oos_baseline    │  param discipline
                └──────────────────────────────┘
```

Workflow:

1. **Architecture doc** says what we're building.
2. **Risk register** lists what's wrong / uncertain.
3. **Task board** turns risks + architecture gaps into work, in priority order.
4. **Unattended-work rules** define how Claude may execute that work.
5. **Reproducibility standard** defines how each experiment proves it ran the way it claims.
6. **Promotion gates** define when a phase is "done" and the next can begin.
7. **Manifest + baseline config** are the data and param contracts the rest of the system honours.

## Where to start in different scenarios

### "I'm picking up after a break"

1. Read this file.
2. Check `git log --oneline -10` for recent commits.
3. Read `docs/TASK_BOARD.md` — what's in `In Progress`?
4. Skim `docs/UNSOLVED_PROBLEMS_AND_RISKS.md` — anything moved to FIXED that I should verify?
5. Pick a Ready task or wait for direction.

### "I'm starting an unattended Claude session"

1. Read `docs/CLAUDE_UNATTENDED_WORK_RULES.md` end-to-end.
2. Read `docs/TASK_BOARD.md`.
3. Pick the highest-priority Ready task whose permission class fits unattended work.
4. Stop on any of the §3 stop conditions.
5. Produce the per-task report (§5) and final report (§6) from the rules doc.

### "I'm running an experiment"

1. Read `docs/REPRODUCIBILITY_AND_EXPERIMENT_STANDARD.md`.
2. Pre-register parameters and slice family BEFORE touching holdout.
3. Emit a complete `experiment.json` per §1 of that standard.
4. Commit under `experiments/<run_id>/`.
5. The verdict.claim must be a single sentence supported by `metrics.json` numbers.

### "I'm proposing a phase transition"

1. Read `docs/PROMOTION_GATES.md` for the relevant gate.
2. Tick every requirement with evidence.
3. Commit the gate-passing artefact (manifest update / simulator commit / shadow report) with the user's explicit sign-off in the commit message.
4. Move the relevant Blocked task to Done on the task board.

### "I want to add a new TODO / risk / known issue"

1. Add it to `docs/UNSOLVED_PROBLEMS_AND_RISKS.md` in the right section, with a fresh ID.
2. Optionally add a Backlog task to `docs/TASK_BOARD.md` referencing the risk ID.
3. Do NOT create a new TODO / RISK / KNOWN_ISSUES file. The risk register is the only such file.

## Files NOT in the operating system (kept for context)

These are useful but not part of the active workflow:

- `docs/decisions.md` — historical record of design decisions; consulted for the "why".
- `docs/TECH_DEBT.md` — superseded; items are mirrored in `docs/UNSOLVED_PROBLEMS_AND_RISKS.md` §16. Slated for replacement with a pointer (T-G01 in task board, awaiting user approval).
- `docs/NEXT_SESSION.md` — stale 2026-04-27 hand-off; open tasks from it are mirrored into the operating system. Slated for archival (T-G02).
- `docs/SETUP_VPS.md`, `docs/SETUP_WINDOWS.md` — operational runbooks; consulted on setup, not on day-to-day work.

## Source of truth at a glance

| Topic | Single source of truth |
|-------|------------------------|
| Architecture | `docs/BOT_ARCHITECTURE_AND_BUILD_PLAN.md` |
| Open problems / risks / TODOs | `docs/UNSOLVED_PROBLEMS_AND_RISKS.md` |
| Active work | `docs/TASK_BOARD.md` |
| Unattended-work rules | `docs/CLAUDE_UNATTENDED_WORK_RULES.md` |
| Experiment provenance | `docs/REPRODUCIBILITY_AND_EXPERIMENT_STANDARD.md` |
| Phase promotion gates | `docs/PROMOTION_GATES.md` |
| Train / validation / holdout split | `manifests/sessions.toml` |
| Frozen lag baseline | `configs/lag_oos_baseline.toml` |
| VPS deployment | `docs/SETUP_VPS.md` |
| Design decisions (why) | `docs/decisions.md` |

If two docs contradict each other, the source-of-truth doc above wins. Other docs should be edited to align (or, if that's a non-trivial change, an entry filed in `docs/UNSOLVED_PROBLEMS_AND_RISKS.md`).
