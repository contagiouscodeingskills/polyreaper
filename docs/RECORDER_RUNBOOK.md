# Recorder runbook (pre-FV-model data collection)

Operational guide for re-enabling the data recorder ahead of FV-model
training. The recorder captures raw WebSocket feeds from Binance,
Polymarket, and Coinbase, plus Polymarket market discovery + resolution
metadata. Output is one NDJSON file per `(venue, stream)` pair under a
single session directory.

## What changed since the last recorder run

Two fixes landed in this branch — both required for FV-model-grade data:

1. **Dynamic Polymarket subscription.** Previously the WS subscribed to
   the registry's tokens *once* at connect time. New 5-minute BTC
   markets discovered by the Gamma loop after connect were not picked
   up until the next full reconnect (~5-15 min), so the first portion
   of every new market — including the moment the strike was set — was
   often missing. Now the connection refreshes the subscription against
   the registry every `subscription_refresh_secs` (default 10s),
   sending incremental MARKET subscribe messages for the diff.

2. **REST `/book` snapshot baseline.** Mirroring the Binance
   `/depth?limit=1000` pattern, the recorder now fetches
   `GET /book?token_id=<id>` for every subscribed token on each WS
   connect and on each incremental subscribe. Stored as
   `<token>@book_snapshot` events. Replay can rebuild the book at any
   tick by combining the snapshot with subsequent diffs.

## Pre-flight on the VPS

```bash
# Verify code builds (≈40s first time, cached after)
cargo build -p recorder --release --bin recorder

# Confirm the systemd unit points at the right binary and config.
systemctl cat polybot-recorder.service  # if it exists; else create it
```

If `polybot-recorder.service` doesn't exist, drop this at
`/etc/systemd/system/polybot-recorder.service`:

```ini
[Unit]
Description=PolyBot recorder
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=polybot
WorkingDirectory=/opt/polybot
Environment=RUST_LOG=info,recorder=info
ExecStart=/opt/polybot/target/release/recorder /opt/polybot/configs/recorder.toml
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Then `systemctl daemon-reload && systemctl enable --now polybot-recorder`.

## Config — what each setting means

`configs/recorder.toml` (key fields):

| Field | Default | Why |
|---|---|---|
| `storage.base_dir` | `./data` | Where session dirs go. Move to a fast SSD path with ≥ 20 GB free for a 3-day run. |
| `storage.rotate_minutes` | 60 | Per-stream file rotates every 60 min. Lets you compress / archive without stopping the recorder. |
| `polymarket_feed.subscription_refresh_secs` | 10 | New fix. ≤ 30s recommended so new markets don't miss data. |
| `polymarket_feed.clob_url` | `https://clob.polymarket.com` | New REST snapshot endpoint. Keep default. |
| `polymarket_feed.read_idle_secs` | 120 | Idle timeout for the Polymarket WS. Don't lower below 60s or you'll churn reconnects during low-trade lulls. |
| `binance_feed.streams` | `[@trade, @depth@100ms, @bookTicker]` | All three required for the FV feature set. |
| `market_discovery.poll_interval_secs` | 15 | Cadence for Gamma re-poll. Keep low — drives the subscription refresh effectiveness. |

## Expected output structure

After ~1 hour of running you should see:

```
data/session_20260521T180000Z/
├── _session_meta.json             # config snapshot + recorder version
├── _health.ndjson                 # 30s snapshots of feed counters
├── _latency_probes.ndjson         # 5-min TCP RTT probes per venue
├── _resolutions.ndjson            # resolved markets from gamma sweep
├── binance/
│   ├── btcusdt@trade.0000.ndjson
│   ├── btcusdt@bookTicker.0000.ndjson
│   ├── btcusdt@depth@100ms.0000.ndjson
│   └── btcusdt@depth_snapshot.0000.ndjson   # ≥ 1 per WS connect
├── coinbase/
│   └── BTC-USD.0000.ndjson
└── polymarket/
    ├── btc-updown-5m-<epoch>.0000.ndjson     # per-market events
    ├── …
    ├── <token_id>@book_snapshot.0000.ndjson  # REST snapshots
    └── _unknown_market-<id>.ndjson           # frames we couldn't route
```

## Verification

After the recorder has been running long enough to roll over at least
one market (say 10 min), check coverage:

```bash
cargo run -p recorder --release --bin recorder_verify -- \
  data/session_20260521T180000Z
```

The tool reports per-stream event counts, time spans, and flags missing
files. Exit code 0 = session is complete enough for training; 1 =
missing critical files.

What you want to see:

- `binance/btcusdt@trade`: thousands of events per minute
- `binance/btcusdt@bookTicker`: thousands of events per minute (sub-ms
  cadence on real BBO changes)
- `binance/btcusdt@depth_snapshot`: at least 1 (one per WS connect; a
  stable connection produces exactly 1)
- `polymarket/`: one file per market the bot was active in, plus
  `<token>@book_snapshot` files (new — confirms REST snapshot fix is
  active)
- `_resolutions.ndjson`: at least 1 line per resolved BTC up/down
  market window during the session

If `polymarket/*@book_snapshot.ndjson` is missing or empty, the fix is
not active — check `clob_url` in config and recorder logs for
`snapshot_fetch_complete` events.

## Disk budget

Rough sizing from earlier soak data (pre-fix; with the snapshot files
add ~5% on top):

| Window | Approx size |
|---|---|
| 1 hour | ~150 MB |
| 24 hours | ~3.6 GB |
| 3 days | ~11 GB |
| 7 days | ~25 GB |

For FV-model v1 (logistic regression with ~12 features), 200-500
labelled markets is the typical minimum for a stable fit. At ~12 BTC
up/down markets per hour, **3 days ≈ 850 markets**. That's the floor.
Plan for ~12-15 GB free.

## What we'll do next (after data is collected)

1. Build the dataset builder: read raw NDJSON + resolution sidecar,
   replay book state at fixed timestamps, emit one row per
   (market, tick) with the feature set and the label.
2. Logistic regression (sklearn) with walk-forward split.
3. Evaluate: Brier, log loss, calibration table, by-TTR-bucket accuracy,
   side-by-side with Polymarket mid.
4. Recommendation: model usable / not usable / needs more data.

None of step 1-4 happens until the recorder has been running for at
least 24-72 hours with the new fixes verified.
