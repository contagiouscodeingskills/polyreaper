# Hand-off prompt — paste into a new Claude Code session

I'm picking up the PolyBot recorder. Everything from the previous
hand-off is done and verified; below is the current state and the
actual next pile of work.

## What's running

A Hetzner 8 GB Frankfurt VPS at `159.69.0.78`, user `polybot`, project
at `/home/polybot/polybot`, systemd service `polybot-recorder.service`
is `active (running)`. **SSH access:** `ssh root@159.69.0.78` works
key-less (this is the only key-auth user). `ssh polybot@159.69.0.78`
prompts for a password — `polybot` was created with
`--disabled-password` and has no key in `~/.ssh/authorized_keys`.
**For all read-only operations on the captured data, use `root@`** —
root can read polybot's data dir without any extra setup. Repo is at
`C:\Users\61429\Desktop\PolyBot`, GitHub remote
`git@github.com:contagiouscodeingskills/polyreaper.git`, branch `main`.

Live session: `session_20260427T100216Z` (recorder restarted
2026-04-27 10:02:16 UTC to pick up priorities 1–5). Three live feeds
active:

- **Binance** — `@trade`, `@depth@100ms`, `@bookTicker`, plus
  `@depth_snapshot` (initial REST baseline on connect + a periodic
  refresh every 10 min — priority 1, confirmed firing in production).
- **Polymarket** CLOB market channel for ~800 BTC up/down 5-minute
  markets, ~480 msg/s.
- **Coinbase** `market_trades` for `BTC-USD`, ~1.4 msg/s (sparse by
  design — BTC trades on Coinbase are infrequent).
- **Chainlink** — intentionally disabled (priority 2 stub). On-chain
  `AggregatorV3` is the wrong oracle for 5-min markets; re-enable
  via Chainlink Data Streams (Mercury, paid). See open task 3 below.

Health snapshots every 30 s to `<session>/_health.ndjson` with
per-feed counters, last-message wall-clock (`last_msg_local_ts_ns`),
storage critical-section quantiles (`store_p50/p99/p999_us`), and
chrony state. Clock is sub-200 µs accurate.

A 13-hour capture from 2026-04-26T12:23 UTC also lives on the VPS at
`session_20260426T122347Z` — 20 GB, mostly Polymarket. Verified sane
during the priority-3 census. Not rsynced to local.

## What's been verified

The previous session shipped six priorities; current status of each:

| # | Commit  | Title                                               | Status                                                                                                                                  |
|---|---------|-----------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| 1 | `0b8b873` | Periodic Binance depth snapshot every 10 min      | Deployed in recorder. Integrity check observed 11 snapshots in 95 min on the live session — cadence confirmed.                           |
| 2 | `3acb1c1` | Disable chainlink on-chain feed (no-op stub)      | Deployed. `chainlink.messages: 0` and all health fields correctly omitted.                                                              |
| 3 | `b4eb98e` | Polymarket `price_change` per-item routing        | Replayer-only fix; 0 decode errors on 3.66 M live events at last check.                                                                  |
| 4 | `1aed81f` | `last_msg_local_ts_ns` per feed in health output  | Deployed. Sub-50 ms staleness across all live feeds.                                                                                     |
| 5 | `e929439` | Storage write latency quantiles in health output  | Deployed. `store_p99_us` 100 µs (Binance), 200 µs (Polymarket/Coinbase) — well under the 1 ms target. **Per-feed writers NOT needed.** |
| 6 | `35349dd` | Replayer `integrity` subcommand                   | Built on VPS (recorder process unaffected by the rebuild). Clean run on the live session: 0 parse / decode / ts / depth-chain failures. |

**The current live session is research-usable.** Tech-debt §3 (per-feed
writers) stays deferred — the priority-5 instrumentation shows the
storage critical section is comfortably below the alarm threshold.

To re-run the integrity check after deploys:

```bash
ssh root@159.69.0.78 'sudo -u polybot -H bash -c "
  cd ~/polybot && source ~/.cargo/env && \
  git pull --ff-only && cargo build --release -p replayer-cli &&
  LATEST=\$(ls -1dt ~/polybot/data/session_*/ | head -1) &&
  ~/polybot/target/release/replayer integrity --root \"\$LATEST\"
"'
```

## Open next tasks (rough priority order, no hard sequencing)

1. **Fix resolution-sweeper 0-byte files.** The integrity checker
   found 521 zero-byte `*-resolved.0000.ndjson` files in the live
   session — the sweeper opens the file before it has anything to
   write. Doesn't corrupt market-channel capture; just a recorder
   bookkeeping bug. Likely a small guard in
   `apps/recorder/src/sweep.rs`.
2. **Coinbase Level 2 channel.** Currently only `market_trades`. L2
   would let researchers reconstruct Coinbase BBO/depth for
   cross-venue lead/lag analysis. New stream subscription + decoder
   + book module.
3. **Chainlink Data Streams integration.** Re-enables the disabled
   chainlink feed. Needs paid API key. Different protocol from the
   on-chain `AggregatorV3` we removed in priority 2.
4. **Gzip rotation under disk pressure.** Not urgent today. Trigger
   when a single session starts pushing past, say, 50 GB or `/`
   approaches half full.
5. **Integrity checker v2: extra sequence checks.** Add Binance
   `trade_id` / `bookTicker.update_id` monotonicity, Coinbase
   `trade_id`, Polymarket per-asset `timestamp_ms` non-decreasing.
   Each is ~30 lines, parallel in shape to the existing
   `binance_depth_chain_breaks` check in
   `crates/replayer/src/integrity.rs`.

## Smaller carry-overs

- **78 files of pre-existing `rustfmt` drift.** One `cargo fmt`
  workspace-wide commit clears it. Not introduced by recent work.
- **`docs/sample_polymarket_census.txt`** — 0-byte file locked by
  something on the local Windows box; just `rm` it when whatever has
  it open releases.
- **Tech-debt items 1, 2, 4, 5 in `docs/TECH_DEBT.md`** — all
  appropriately deferred, none triggering revisits.

## Non-negotiable rules

- No raw NDJSON / Parquet files into Claude context. Every analysis
  produces a small summary file; that's what I read.
- The recorder's "store payload byte-identical to wire" invariant
  must not break.
- Don't restart the running recorder unless explicitly told.
- Don't apt-upgrade or reboot the VPS.
- SSH to VPS as `root@` only.

## First action

There's no fixed first action — pick a task whose scope matches the
session. Smallest first if you want a low-risk win:

- Open task 1 (resolution-sweeper 0-byte fix) — probably under 30 lines.
- Open task 5 (integrity v2 sequence checks) — ~150 lines if all four
  at once; ships well one at a time too.

Biggest are Coinbase L2 (task 2) and Chainlink Data Streams (task 3),
each spanning a feed crate + replayer decoder + book module + tests.
