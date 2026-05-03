# PolyBot — Architecture & Build Plan

## Document purpose

This document is the master blueprint for the PolyBot Polymarket BTC 5-minute trading bot. It separates the system into modular layers so live trading can be added on top of the existing recording / replay / research stack without rebuilding the data layer. Re-read it at the start of each phase. Update it whenever an architectural decision changes.

This document is **not**:
- A spec for any specific strategy parameters.
- A research report.
- An ops runbook.

## Current state (2026-05)

A recorder and replayer are operational on a VPS:
- `apps/recorder` — Rust binary; Binance, Polymarket, Coinbase feeds; Chainlink feed exists but is disabled.
- `crates/replayer` + `apps/replayer` — library API (`open_session`, `open_base_dir`) and CLI (`sessions`, `count`, `head`, `tail`, `dump`, `integrity`, `validate-resolutions`, `schema`).
- `scripts/` — untracked Python/shell tools for lag analysis, integrity supplementary, swing scan, VPS health, RawEvent migration codegen.
- Recorder phases 1–6 + finalisation merged (`a364ab8`); replayer integrity v2 merged (`af28191`).

Active research: Binance → Polymarket lag signal. Early evidence: Polymarket lags Binance by ~2–4s under some conditions on the `btc-up-or-down-5m` series.

---

## Live-ready design rules

These rules apply to every system. They are how we keep the option of going live without a rewrite.

1. **Replay determinism.** The Signal Engine MUST produce bit-identical signals when fed live data on the day, AND the same data replayed from disk afterwards. No wall-clock dependencies in signal logic. No `now()`. Time enters via event timestamps only.
2. **One code path for replay and live.** The Signal Engine, Risk Engine, and fill model accept either a live event source or a replay source through the same trait. If we end up with two implementations of "compute fair value" we have failed.
3. **Time is `local_ts_ns`, always nanoseconds, always recorder-stamped.** Venue timestamps are hints, not truth. Latency offsets are applied at one boundary only (replay), never inside strategy logic.
4. **Data-quality gates in front of every trade decision.** Before any signal can fire in live mode, the Risk Engine's data-quality check gates it. The same check runs in shadow.
5. **Position and orders are owned by Live Execution.** No other system mutates them. Every other system reads.
6. **Configuration is versioned and pinned.** Every backtest, shadow run, and live session writes its full config (commit hash + param values) into session metadata. We never run "the strategy" — we run a specific commit + param set.
7. **Polymarket market discovery is a separate concern.** "Which market am I trading right now?" is answered by a market-tracker, not by the strategy. The strategy receives `(market_id, market_state, t_to_resolution)` as inputs.
8. **Fail closed.** Any uncaught error, missing field, stale feed, or risk breach in live flattens or holds. Default to "do nothing".
9. **Event-time, not arrival-time.** "What was the state X seconds ago?" uses event-time on `local_ts_ns`. Arrival-time exists only for monitoring (lag-to-now).
10. **No hidden state across processes.** A live restart must be safe at any instant. Position, open orders, last-decided action all reload from durable storage on startup.

---

## Cross-cutting concerns

Not their own systems, but they touch everything:

- **Time / clock**: chrony on the VPS, `local_ts_ns` everywhere, latency offsets applied only at the replay boundary.
- **Configuration**: versioned TOML; recorder uses `configs/recorder.toml`; bot will use `configs/bot.toml`. Every session pins its config (commit hash + values) into `_session_meta.json`.
- **Identifiers**: `session_id` (UTC dir), `market_id` (Polymarket condition_id), `instrument_id` (Binance/Coinbase symbol). Used everywhere.
- **Schema versioning**: `RawEvent` is wire-stable; new optional fields use serde defaults. Migration policy: `migrate_rawevent.py`-style codegen for source; lenient deserializers for old data.
- **Logging**: structured (tracing/JSON in Rust, structlog in Python). Bot decisions log enough context to replay the decision offline.

---

## 1. Recorder System

**Purpose.** Capture every byte of relevant market data with high-precision local timestamps and write to disk in a format that supports faithful replay.

**Inputs.** WebSocket streams (Binance `stream.binance.com`, Polymarket CLOB, Coinbase Advanced Trade); Gamma REST API for market discovery and resolutions; chrony-disciplined system clock.

**Outputs.** `session_<UTC>/<venue>/<stream>.ndjson[.gz]` plus `_health.ndjson`, `_resolutions.ndjson`, `_latency_probes.ndjson`, `_session_meta.json` sidecars.

**Sub-components.**
- **Binance feed recorder** — `crates/binance_feed`; trade, depth@100ms, bookTicker.
- **Polymarket feed recorder** — `crates/polymarket_feed`; registry-driven subscriptions; demuxes array frames with `wire_batch_id` + `event_index_in_batch`.
- **Coinbase feed recorder** — `crates/coinbase_feed`; market_trades only.
- **Storage writer** — `crates/storage`; per-stream NDJSON, optional minute-bucket rotation, `fsync_on_write` toggle, write-latency histograms.
- **Health monitor** — `apps/recorder/src/health.rs`; per-feed counters, msg-rate, stall flag, chrony state, `disk_free_bytes`; every 30s.
- **Latency probes** — `apps/recorder/src/latency.rs`; TCP connect-time per venue every 300s.
- **Resolution sweeper** — `apps/recorder/src/sweep.rs`; Gamma `/events?closed=true` every 60s.
- **Disk guard** — `scripts/disk_guard.sh`; ENOSPC abort companion.
- **Market discovery (proposed promotion)** — currently inside `polymarket_feed`; once the bot needs the same logic at runtime, lift to its own crate so recorder and bot share it.

**What could go wrong.** WS reconnect storms (handled by exponential backoff, but verify the cap doesn't grow unbounded); Polymarket idle 120s tolerance can leave partially-written lines on real outage (integrity v2 tail/interspersed split mitigates); chrony loss degrades `local_ts_ns` precision; disk-full loses unflushed events; Gamma rate limits stall market discovery so the Polymarket feed misses new markets.

**How it supports live trading.** The same feed crates can run in "tee mode": one socket, two consumers (recorder + bot) — either keep recorder + bot as separate processes consuming the same WS, or split each feed crate so `Feed → RawEvent stream` is reused by both. Health and latency-probe sidecars become live-monitoring inputs. Resolution sweeper is the source of truth for settlement P&L.

---

## 2. Reader / Replayer System

**Purpose.** Read recorded sessions back deterministically, in time order, decoded to typed events, with optional book reconstruction and integrity validation.

**Inputs.** Session directories produced by the Recorder.

**Outputs.** Library: `MergedReader` iterator of `RawEvent` / `DecodedEvent`. CLI: NDJSON to stdout, Parquet to file, integrity reports (text or JSON). Reconstructed orderbooks (`BinanceBook`, `PolymarketMarketBook`).

**Sub-components.**
- **Session discovery** — `crates/replayer/src/discovery.rs`; walks `session_<UTC>` dirs into `FileBucket` records.
- **Raw event reader** — `crates/replayer/src/reader.rs`; gzip-transparent; yields `(line_no, RawEvent)`.
- **Event merger** — `crates/replayer/src/merge.rs`; k-way min-heap on `(local_ts_ns, file_idx, line_no)`.
- **Decoder** — `crates/replayer/src/decode/`; per-venue payload → `DecodedEvent`.
- **Book rebuilder** — `crates/replayer/src/book/`; Binance L2, Polymarket Yes/No.
- **Integrity checker** — `crates/replayer/src/integrity.rs`; T0 structural / T1 decoder / T2 sequence; PASS/WARN/FAIL verdict.
- **Resolution validator** — CLI `validate-resolutions`.
- **Pacer** — `crates/replayer/src/pacer.rs`; max-speed or realtime + per-venue latency offsets.
- **Parquet exporter** — `crates/replayer/src/parquet.rs`; 5-col Arrow.
- **Filter** — `crates/replayer/src/filter.rs`.

**What could go wrong.** Tail-truncated NDJSON (handled by integrity v2 classification, but the Pacer must skip cleanly); latency offsets in the Pacer can produce local out-of-order if offset deltas exceed inter-event gaps (document the limitation; bot replay should run with offsets = 0 unless explicitly testing latency); decoder regressions surface as `DecodedEvent::Unknown`; book reconstruction bugs (e.g., wrong tick size) aren't caught by integrity — book-level invariant tests are needed.

**How it supports live trading.** A "live source" implementation that wraps the live feeds presents the same `MergedReader` API to the bot. Integrity verdicts become data-quality risk inputs. The book rebuilder is identical in replay and live (design rule 2).

---

## 3. Research / Analysis System

**Purpose.** Iterate on hypotheses (e.g., Binance → Polymarket lag) using historical data; produce evidence good enough to justify building a signal.

**Inputs.** Replayed sessions (via reader API or direct NDJSON read for Python scripts); resolution sidecars.

**Outputs.** Reports (Markdown, plots, CSV summaries); calibrated parameter ranges (lag thresholds, signal cutoffs) handed to the Signal Engine.

**Sub-components.**
- **Lag analysis** — `scripts/lag_analysis_v{1,2,3}.py`; directional-move detection on Binance bookTicker mid vs Polymarket price; v3 adds filter ablation + odds-band slicing.
- **Odds-band analysis** — slice signal performance by Polymarket implied odds (e.g., 0.40–0.60 vs 0.60–0.80 vs >0.80). Often subsumed inside `lag_analysis_v3`.
- **Out-of-sample validation** — explicit holdout: train on dates A, evaluate on dates B. Currently informal — **recommend a `scripts/oos_validation.py` harness with fixed train/test/holdout date splits and a manifest of which sessions belong to which set**.
- **Reporting** — Markdown reports under `docs/research/<topic>.md`. Each must include: hypothesis, data slice, method, headline number, confidence interval, failure modes.
- **Case-study analysis** — `scripts/casestudy_analysis.py`; event-level traces for cherry-picked moments; useful for intuition and signal auditing.
- **Swing scanner** — `scripts/scan_polymarket_swings.py`; market-level price moves above threshold; feeds candidate windows to deeper analysis.

**What could go wrong.** Overfitting (defense: OOS discipline). Survivorship bias (only analyse markets we have full data for — flag and document). Data leakage / look-ahead in joins (off-by-one-tick errors look like real edge). Conflating "the model says X" with "we could have traded X" — slippage and fill probability are not in pure analysis.

**How it supports live trading.** Defines the parameter set the Signal Engine ships with. Establishes a reproducible benchmark — every change to the Signal Engine reruns these reports for regression detection.

---

## 4. Signal Engine / Bot Brain

**Purpose.** Convert market events into trade decisions. Pure function from `(event stream, internal state) → decision stream`.

**Inputs.** Decoded event stream (Binance trades / depth / bookTicker, Polymarket book / price_change / trade, Coinbase trades). Current market context: which Polymarket market, time-to-resolution, current outcome odds.

**Outputs.** Stream of `Signal` records (intent: buy YES at price ≤ X, size ≤ Y, or hold, or flatten). NOT orders — orders are produced by Execution. Logged features and intermediate values for auditing.

**Sub-components.**
- **Binance microstructure brain** — short-horizon (100–500ms) BTC direction estimate from L2 imbalance, trade flow, queue dynamics. Stateful.
- **Polymarket state brain** — current top-of-book on YES/NO across the active 5m market; tick size; recent trade history; implied probability.
- **Fair-value calculator** — combines BTC direction estimate, time-to-resolution, and current BTC price level into a "true" P(YES wins). The model output.
- **Signal detector** — compares fair value to Polymarket mid; emits a candidate when edge > threshold AND confidence sufficient AND feed-health OK.
- **Lag / reaction model** — explicitly models the 2–4s reaction window: "Binance moved at t; Polymarket has not yet repriced; expected repricing magnitude M; trade now if M > slippage + fees + threshold."
- **Signal logger** — writes every signal (and every "looked, didn't fire" decision) to NDJSON next to the session for replay diagnostics.
- **Market lifecycle tracker (proposed addition)** — Polymarket 5m markets have a `(open → trading → freeze → resolution)` state machine. The brain needs `t_to_resolution` and a `tradeable` boolean. Currently latent — make it explicit.
- **Feature store (proposed addition)** — single in-memory snapshot holding the latest derived features (mid, spread, imbalance, smoothed BTC direction). Both signal detector and lag model read from it. Avoids recomputation; serializable for logging.

**What could go wrong.** Wall-clock leakage — a feature accidentally uses `Instant::now()` and breaks replay determinism (lint for it). State corruption across markets — when a 5m market closes and a new one opens, internal state from the old market poisons the new one (explicit reset rule needed). Stale features when a feed pauses (data-quality gate prevents firing). Look-ahead in book reconstruction — if the brain reads the book BEFORE the triggering event is applied, off-by-one bugs flatter backtests. Feature drift — yesterday's parameters don't work today (calibration cadence).

**How it supports live trading.** This *is* the live trading brain. Same code path, different event source. Decisions log identically in backtest, shadow, and live, so we can diff them.

---

## 5. Execution Simulator

**Purpose.** Convert a stream of `Signal`s into a stream of simulated fills using historical orderbook state, producing P&L and execution stats.

**Inputs.** `Signal` stream from a backtest of the Signal Engine; historical Polymarket book state (from book rebuilder); a model of Polymarket fees, tick size, minimum order size, latency, and fill probability.

**Outputs.** Simulated fills, slippage estimates, P&L curve, win-rate, drawdown. Per-trade audit log: `(signal, intended_price, intended_size, fill_price, fill_size, slippage, fees, P&L)`.

**Sub-components.**
- **Historical fill simulator** — given an intended order at time t, walks the book and matches against resting liquidity at `t + assumed_latency`.
- **PnL simulator** — marks-to-market on each event; accounts for fees; resolves at market close using `_resolutions.ndjson`.
- **Size / liquidity simulator** — caps fill size at available depth at price; models partial fills and queue position.
- **Delay simulation** — explicit `(decision_t → submission_t → ack_t → match_t)` timeline. Default to a calibrated latency drawn from `_latency_probes.ndjson`.
- **Adverse-selection model (proposed addition)** — when we are first to take a quote, the quote may have been about to update. Model a probability that our fill was the last fill at that price before a move against us. Important for short-lag signals on a thin book.

**What could go wrong.** Self-trade illusion — simulator assumes our orders don't move the book (fine for small size on Polymarket; document the size cap). Latency too optimistic — connect-time probes ≠ end-to-end order ack latency (must measure end-to-end during shadow). Wrong / outdated fee schedule. Resolution payouts wrong (USDC decimals). Partial-fill semantics on Polymarket (limit vs FoK vs IoC differ — must match what the live router will do).

**How it supports live trading.** The fill simulator's interface matches the live order router's. If we keep the trait surface identical, switching from sim to live is a config flag. Calibration of the latency model against shadow-measured latency closes the sim-to-live gap.

---

## 6. Shadow Trading System

**Purpose.** Run the bot end-to-end against live data with no real orders submitted. Produce a stream of "what would have happened" trades against the live order book.

**Inputs.** Live event stream (Binance, Polymarket, Coinbase via the live tap); same Signal Engine + Risk Engine as live mode.

**Outputs.** Paper-order log: every decision, every paper fill (matched against the observed live book), every paper P&L. Shadow performance report — daily / weekly / per-market. Sim-to-live calibration deltas.

**Sub-components.**
- **Live signal runner** — long-running process tapping live feeds, running Signal Engine, emitting `Signal`s.
- **Paper order logger** — receives signals, simulates fills against the live book in real time (same fill model as Phase 5), writes NDJSON.
- **Shadow performance report** — daily roll-up: # signals, # fills, hypothetical P&L, latency stats, data-quality breaches, divergence vs backtest expectation.
- **Live-vs-sim divergence monitor (proposed addition)** — during shadow, the same data exists twice (live as it arrives + recorded for replay). Re-running the bot on the recording should produce identical signals. Any divergence is a determinism bug. Run nightly.

**What could go wrong.** Shadow fills are aspirational — real fills will be worse (compare shadow to a pessimistic version, e.g. assume queue position = last). Live tap stalls but bot keeps running (same data-quality gate as live applies). Configuration drift — the version we shadow today is not the version we replayed last week (pin commit + params per shadow run).

**How it supports live trading.** This is the dress rehearsal. Going live should be flipping a flag from `paper=true` to `paper=false`. Shadow establishes the calibrated latency model and surfaces production failure modes (reconnects mid-signal, partial frames during a trade decision) before real money is at risk.

---

## 7. Live Execution System

**Purpose.** Translate `Signal`s into real Polymarket orders, track them through the order lifecycle, and maintain the canonical position / orders state.

**Inputs.** `Signal` stream from the Signal Engine (gated by Risk Engine); Polymarket order API + user-data WS stream; wallet (signing key).

**Outputs.** Real orders submitted to Polymarket; authoritative position and open-order state, persisted; fill events fed back to Risk Engine and Signal Engine logger.

**Sub-components.**
- **Polymarket order router** — converts `Signal` → CLOB order; routes via REST or WS depending on order type.
- **Wallet / signing module** — manages the EOA / proxy wallet, signs order payloads. Hardware wallet or KMS for live; encrypted-on-disk keystore acceptable for early live.
- **Order manager** — tracks every order from `intended → submitted → acked → (filled | rejected | canceled)`; handles cancels, re-quotes, timeouts. Persists state durably; recoverable on restart.
- **Latency-critical path** — the hot path from "signal fires" → "order on the wire": no blocking I/O, no allocations beyond the order payload, separate runtime from the rest of the bot if needed.
- **Position store (proposed addition)** — durable record of current position, fills, realised P&L. Source of truth. Consulted on startup. Reconciled with Polymarket's own view periodically.
- **User-data WS handler (proposed addition)** — Polymarket user-data stream gives real-time fill / cancel events. Order manager listens here, not just to REST polling.

**What could go wrong.** Double-submit: signal fires twice, two orders go out, position doubled (idempotency keys per signal). Lost ack: order placed but unknown to us (reconcile on startup against Polymarket's view). Signing latency: hardware wallet adds 100ms+, may break the strategy (measure first). Wallet key compromise: catastrophic (use minimal-permission proxy where possible; rate-limit at the wallet level). Network partition: WS still connected, REST broken (retries with backoff, circuit-break eventually). Polymarket rejects our order (bad price, market frozen, insufficient balance) — clean error mapping into the Risk Engine.

**How it supports live trading.** This *is* live trading. Built last and built carefully.

---

## 8. Risk Engine

**Purpose.** Veto bad trades and stop everything when something is wrong. Sits between the Signal Engine and Live Execution.

**Inputs.** Pending `Signal`s; current position, open orders, today's P&L (from Live Execution); feed health (from Recorder health output and live monitor); account balance.

**Outputs.** Approved / rejected signals; kill events (halt trading, optionally flatten).

**Sub-components.**
- **Trade-level risk** — per-signal: max order size, max price slippage, sane price (within X of mid), market is tradeable, t_to_resolution > minimum.
- **Market-level risk** — per-market position cap, per-market daily P&L floor, max consecutive losses on a single market.
- **Account-level risk** — daily P&L floor, max drawdown, max # open positions, max notional, max loss per minute.
- **Data-quality risk** — gates trading on: feed staleness (`last_msg` age), reconnect rate, parse failure rate, chrony status, integrity verdict (if computable in real time), Polymarket book depth above minimum.
- **Kill switch** — halts new orders and (optionally) cancels all open orders. Triggered by: any account-level breach, manual SIGINT, data-quality red, integrity FAIL, P&L floor breach.
- **Pre-trade simulation (proposed addition)** — for each signal, run it through the Execution Simulator's fill model first; reject if expected slippage/cost makes it unprofitable. Cheap; catches a class of bad fills.

**What could go wrong.** Risk Engine fails open — bug in a check causes silent approval (must default-deny on any internal error). Kill-switch race — kill fires while orders are mid-flight (idempotently cancel and reconcile). Stale account state — position cap evaluated against an old position (fresh fill events are critical). Risk thresholds tuned against backtest, not live (calibrate during shadow).

**How it supports live trading.** It is the live trading safety net. Every signal goes through it.

---

## 9. Monitoring / Operations

**Purpose.** Visibility into the running systems and notification when something needs human attention.

**Inputs.** Recorder health sidecars and logs; bot logs, position store, P&L; VPS-level metrics (CPU, RAM, disk, network).

**Outputs.** Dashboards (web); alerts (push / email / webhook); daily / weekly performance summaries.

**Sub-components.**
- **Recorder monitoring** — feed-up, msg-rate, reconnect rate, disk free, chrony state. Already partially covered by `_health.ndjson` + `scripts/vps_health_check.sh`.
- **Trading monitoring** — P&L, position, open orders, signal rate, fill rate, slippage vs sim, latency.
- **Alerts** — recorder-down, feed stalled > X, integrity FAIL on last session, P&L floor near, kill-switch fired, wallet balance low, Polymarket API error rate spike.
- **Dashboards** — recorder dashboard (per-feed health), trading dashboard (P&L, positions, recent decisions), research dashboard (latest reports). Local Grafana or hosted.
- **Deployment / infra (proposed addition)** — systemd units, log rotation, auto-restart policy, deploy script, config file management, secrets management. Glues everything else.
- **Audit log (proposed addition)** — append-only log of every trade decision and every config change. Required for "what was the bot doing at 03:14 UTC?" investigations.

**What could go wrong.** Alert fatigue (tune thresholds against shadow data first). Single point of failure — monitoring on the same VPS as the bot (off-host alert path needed for "the VPS itself is down"). Slow / silent failure — bot stops trading but doesn't error (heartbeat / liveness alert).

**How it supports live trading.** We can't go live without it. Build it before going live, alongside Risk Engine.

---

## 10. Build Order

**Phase 1 — Recorder / replayer / data layer.** Done. Recorder phases 1–6 + replayer integrity v2 + scripts.

**Phase 2 — Clean analysis pipeline.** In progress. Replace the `lag_analysis_v{1,2,3}.py` sprawl with a versioned, parameterised pipeline. Output: a stable benchmark report we re-run on every signal change. Includes a session manifest (which sessions go to train, validation, holdout).

**Phase 3 — OOS signal validation.** Define the lag-signal hypothesis precisely; lock a holdout set; characterise the signal on the holdout *without retuning parameters and without computing P&L* (P&L requires the execution simulator). Phase 3 reports:
- same-direction / opposite / no-response counts
- median lag / p90 lag
- sample sizes (per slice)
- odds-band and threshold results
- **no parameter retuning** on the holdout — parameters are frozen from train

**Gate to Phase 4:** if the holdout response rates and lag distribution don't match what we saw in train (or look indistinguishable from noise), kill the strategy before building the simulator and brain.

**Phase 4 — Execution simulator.** Build the fill model + Polymarket fee schedule + size/liquidity model + delay model. This is where economic metrics first appear. Phase 4 adds:
- fill probability (given intended price + size + observed book)
- entry price (after walking the book)
- available size (depth at price)
- slippage / spread cost
- missed fills (signal fired but no liquidity available)
- P&L / EV per signal and aggregate

End-state: feed in a `Signal` stream from a Phase 3-validated parameter set, get back simulated trades and a P&L curve we trust within ±X% of what shadow will show.

**Phase 4.5 — Signal Engine (in parallel with the second half of Phase 4).** Build features + brain + signal detector. End-state: a binary that consumes a replay, outputs a `Signal` stream, runs through the simulator, and produces a backtest report.

**Phase 5 — Shadow trading.** Live tap → bot → paper orders → shadow report. Run for at least 2 weeks across varied market conditions before any thought of Phase 7. Build the live-vs-sim divergence monitor here too.

**Phase 6 — Risk engine.** All four tiers (trade / market / account / data-quality) + kill switch. Built before going live but tested under shadow first. Phase 6 and Phase 5 overlap heavily — risk should be active during shadow.

**Phase 7 — Live execution.** Order router + wallet + order manager + position store + user-data WS handler. Smallest possible size first. Manual kill switch primary; automated risk secondary.

**Phase 8 — Monitoring & scaling.** Most of the monitoring should already exist by now (recorder monitoring is done; shadow brings in trading monitoring). Phase 8 is hardening: alerts, deployment polish, operational runbook, scaling to additional markets / strategies.

---

## Recommended additions / removals / changes (vs. user's draft)

### Added
- **Cross-cutting concerns** section (time, config, schema, identifiers, logging) — these touch every system; calling them out prevents per-system divergence.
- **Live-ready design rules** — the ten invariants we don't violate. The single biggest leverage point for not having to rewrite later.
- **Market lifecycle tracker** in the Signal Engine — Polymarket 5m markets have a state machine the brain needs as an explicit input.
- **Feature store** in the Signal Engine — single in-memory snapshot to make features replayable and loggable.
- **Position store** in Live Execution — explicit source of truth for position / orders, durable, recovered on restart.
- **User-data WS handler** in Live Execution — real-time fill / cancel events, not just REST polling.
- **Pre-trade simulation** in Risk Engine — run through fill model first.
- **Live-vs-sim divergence monitor** in Shadow Trading — catches replay-determinism bugs.
- **Adverse-selection model** in Execution Simulator — important for short-lag signals on a thin book.
- **Audit log** + **Deployment / infra** sub-components in Monitoring / Operations.

### Renamed / reframed
- "Out-of-sample validation" promoted from a research sub-component to its own phase-level gate (Phase 3) — it's the kill / proceed decision for the whole bot.
- "Market discovery" — currently inside `polymarket_feed`. Documented there as a sub-component, but flagged for promotion to its own crate when the bot needs it.

### Flagged but not removed
- Chainlink feed in the recorder — a no-op today, intentionally left in tree. Don't invest in it for the 5m strategy.

### Reordered
- No change to the user-proposed top-level ordering — it's logical and proceeds in dependency order.
- Phase 4 split into Phase 4 (simulator) and 4.5 (signal engine), running in parallel. The signal engine without a simulator gives unmeasurable backtests; the simulator without a signal engine has nothing to consume. They co-evolve.

---

## Non-goals (for now)

- Live trading on any market.
- Strategies other than the lag signal and the BTC microstructure signal.
- Cross-market arbitrage.
- Multi-account / multi-wallet operation.
- Multi-asset (only BTC 5m markets).
- Ultra-low-latency colocation (sub-1ms). Polymarket reaction window is ~2–4s; we don't need it.
- A general-purpose strategy framework. We are building one specific bot.
- A web UI for the bot. CLI + dashboards only.
- Tax / accounting tooling.

---

## Open questions (for later)

1. **Polymarket API specifics**: rate limits on order submission? Realistic end-to-end latency from signal to ack? *(Measure during shadow.)*
2. **Wallet posture**: hot wallet on the VPS, hardware-wallet remote-signing, or cloud KMS? Affects latency-critical path.
3. **Polymarket order types**: limit, FoK, IoC — which do we actually use? Affects simulator accuracy.
4. **Market-discovery cadence**: how often do new 5m markets open? Currently 15s Gamma poll — fast enough, or do we need a push channel?
5. **Position concentration**: one market at a time, or queue across the next N 5m markets?
6. **Strategy override**: when BTC is in a regime change (large move), does the lag model still hold? Calibration windows.
7. **Funding**: how much capital? Drives risk thresholds and minimum-edge requirements.
8. **Regulatory**: jurisdictional considerations for an automated Polymarket bot. Not technical, but it gates Phase 7.
9. **Fail-over**: a single VPS is a single point of failure. Acceptable for early live; what's the upgrade path — multi-region, hot standby, or "stop trading on outage"?
10. **Logs as source of truth vs. DB**: we currently NDJSON-everything. Does the bot also need a relational store for positions / orders, or does NDJSON + an in-memory index suffice through Phase 7?
