# VPS deployment — recorder

One-time setup for running `recorder` as a systemd service on Ubuntu 24.04.
Written ahead of the first real deploy; adjust against what actually worked
after the box is up.

Target shape: Hetzner CX32 in Nuremberg or Falkenstein (8 GB RAM / 80 GB
NVMe / 20 TB traffic, ~€10/mo with backups). Commands apply to any Ubuntu
24.04 host; only §1 cares about the region.

---

## 0. Prerequisites

- VPS provisioned with Ubuntu 24.04 LTS.
- SSH key added at provision time (not password).
- You can `ssh root@<ip>` from your laptop.
- Git repo URL in hand (or plan to rsync from laptop).

Commands below alternate between `root` (system setup) and `polybot`
(service user). Prompts make it explicit: `#` = root, `$` = polybot.

---

## 1. Connectivity sanity checks — do FIRST

Run these as root **before any install/build work**. If either fails, the
region is blocked — destroy the VPS and reprovision elsewhere.

```bash
# Polymarket gamma: expect HTTP/2 200 + JSON content-type.
curl -sI https://gamma-api.polymarket.com/markets?limit=1 | head -5

# Binance WSS endpoint: expect *any* real HTTP response (400/426 is fine —
# it's the WS upgrade path rejecting plain HTTP). A connection reset or
# NXDOMAIN is not fine.
curl -sI --connect-timeout 5 https://stream.binance.com:443 | head -5
```

Red flags:
- A 200 with `server: azurefd` and HTML body on gamma → you hit a DNS-block
  page (this is what happened on the AU residential ISP). Different region.
- Connection refused / timeout on Binance → network path broken.
- Any cert warning from curl → MITM or bad DNS.

---

## 2. System packages

```bash
# as root
apt update
apt install -y build-essential pkg-config libssl-dev ca-certificates \
               git rsync zstd curl
```

`build-essential` gives `gcc`/`ld` for the rustc linker. `libssl-dev` is
for `native-tls` linkage. The rest are for operations.

---

## 3. Service user

```bash
# as root
adduser --disabled-password --gecos "" polybot
```

No sudo. The recorder needs zero privileges and holds no secrets.

---

## 4. Install Rust (as polybot)

```bash
# as root: drop into polybot's shell
sudo -iu polybot

# now $ prompt — as polybot
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain none
source ~/.cargo/env
```

`--default-toolchain none` is deliberate. `rust-toolchain.toml` in the repo
pins `channel = "stable"`, which on Linux resolves to
`stable-x86_64-unknown-linux-gnu`. Cargo installs it on first build.

---

## 5. Clone + build

```bash
# as polybot
cd ~
git clone <repo-url> polybot
cd polybot
cargo build --release
```

First build pulls ~200 crates and takes 3–6 minutes on CX32. RAM peak
~2 GB during link — no swap needed on an 8 GB box.

If `cargo build` fails with `linker 'cc' not found`, `build-essential`
wasn't installed — redo §2.

---

## 6. Config review

```bash
# as polybot
nano configs/recorder.toml
```

Minimum to check:
- `storage.base_dir` — set to `"/home/polybot/polybot/data"` (absolute
  path; the service won't share your shell's cwd).
- `telemetry.log_format` — `"json"` plays nicely with `journalctl -o json`
  + `jq`. `"pretty"` is fine for reading directly.
- All WS / HTTP URLs — unchanged from the sample unless you have a reason.

---

## 7. First run, by hand

Before wrapping in systemd, confirm the stack works interactively:

```bash
# as polybot, in ~/polybot
./target/release/recorder configs/recorder.toml
```

Expected log sequence (order matters):
1. `recorder starting`
2. `storage session opened`
3. `gamma discovery pass kept=N` — `N > 0` means the regex is matching
4. `gamma discovery loop spawned`
5. `binance feed task spawned`
6. `connected` then `subscribed`
7. `opened stream file` for trade and depth

Let it run ~30 s, then `Ctrl-C`. You should see
`shutdown_signal signal="SIGINT"` followed by `flush_ok`. Inspect:

```bash
ls -lh data/session_*/binance/
head -c 400 data/session_*/binance/btcusdt_trade.*.ndjson
```

Each line should be a full NDJSON record with `"venue":"binance"` and a
`payload` field.

---

## 8. systemd service

```bash
# as root
tee /etc/systemd/system/polybot-recorder.service >/dev/null <<'EOF'
[Unit]
Description=Polybot recorder
Documentation=file:///home/polybot/polybot/docs/SETUP_VPS.md
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=polybot
Group=polybot
WorkingDirectory=/home/polybot/polybot
ExecStart=/home/polybot/polybot/target/release/recorder /home/polybot/polybot/configs/recorder.toml
Restart=always
RestartSec=5s
# Must be >= cfg.app.shutdown_grace_secs (currently 10s) + a margin.
TimeoutStopSec=15s
StandardOutput=journal
StandardError=journal

LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now polybot-recorder
```

Watch it boot:

```bash
journalctl -fu polybot-recorder
```

Same sequence as §7. When you run `systemctl stop polybot-recorder`, the
log should show `shutdown_signal signal="SIGTERM"` + a clean flush within
~15 s (not a hard kill).

---

## 9. Data directory layout

```
/home/polybot/polybot/data/
├── session_20260424T120000Z/
│   ├── binance/
│   │   ├── btcusdt_trade.0000.ndjson
│   │   ├── btcusdt_depth_100ms.0000.ndjson
│   │   ├── btcusdt_trade.0001.ndjson      # rotated at hour boundary
│   │   └── …
│   └── polymarket/                        # once polymarket_feed lands
└── session_…                              # new one each recorder restart
```

One session directory per recorder start; one file per `(venue, stream,
bucket)` triple. `rotate_minutes = 60` in config controls bucket cadence.

---

## 10. Disk alert timer

Hourly check, warns if the data partition is >80% full.

```bash
# as root
tee /usr/local/bin/polybot-diskcheck >/dev/null <<'EOF'
#!/bin/bash
set -euo pipefail
DATA_DIR=/home/polybot/polybot/data
THRESHOLD=80
USAGE=$(df --output=pcent "$DATA_DIR" | tail -1 | tr -d ' %')
if [ "$USAGE" -gt "$THRESHOLD" ]; then
    logger -t polybot-diskcheck -p user.warning \
        "Disk usage ${USAGE}% > ${THRESHOLD}% on ${DATA_DIR}"
    exit 1
fi
exit 0
EOF
chmod +x /usr/local/bin/polybot-diskcheck

tee /etc/systemd/system/polybot-diskcheck.service >/dev/null <<'EOF'
[Unit]
Description=Polybot disk-usage check

[Service]
Type=oneshot
ExecStart=/usr/local/bin/polybot-diskcheck
EOF

tee /etc/systemd/system/polybot-diskcheck.timer >/dev/null <<'EOF'
[Unit]
Description=Polybot disk-usage check (hourly)

[Timer]
OnBootSec=5min
OnUnitActiveSec=1h
Persistent=true

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now polybot-diskcheck.timer
```

Triggers show up in `journalctl -t polybot-diskcheck`. Add a webhook /
email emitter inside the script later if you want off-box alerting.

---

## 11. Cap journald size

Prevents the journal silently eating disk over weeks:

```bash
# as root
mkdir -p /etc/systemd/journald.conf.d
tee /etc/systemd/journald.conf.d/polybot.conf >/dev/null <<'EOF'
[Journal]
SystemMaxUse=500M
EOF
systemctl restart systemd-journald
```

500 MB ≈ weeks of recorder `info`-level logs.

---

## 12. Data offload (run from your laptop)

Simple pattern — pulls rotated NDJSON but not the currently-written bucket:

```bash
# from laptop; ssh key must be set up to polybot@vps
rsync -av --update --progress \
    --include='*/' --include='*.ndjson' --include='*.ndjson.zst' \
    --exclude='*' \
    polybot@vps:~/polybot/data/ ./data/
```

`--update` skips anything the laptop already has with a newer mtime, so
repeat runs are idempotent.

Compressing on the VPS first (huge win — ~85% size drop on JSON) keeps the
disk small and speeds up transfers:

```bash
# on the VPS, as polybot. Only compresses files not modified in 2h (the
# live bucket is still being written, leave it alone).
find ~/polybot/data -name '*.ndjson' -mmin +120 -exec zstd --rm {} \;
```

Run by hand for the first week. Wrap in another systemd timer once you've
seen it work.

---

## 13. Things likely to need tuning on the real box

In rough order of likelihood:

1. **Gamma title regex.** `cfg.market_discovery.title_pattern` may not
   match live BTC 5-minute market titles. If every
   `gamma discovery pass` shows `kept=0`, inspect current titles:
   ```bash
   curl -s 'https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=20' \
       | jq -r '.[].question'
   ```
   Tune the regex, reload the service.
2. **Shutdown grace.** `cfg.app.shutdown_grace_secs=10` handles the normal
   buffered-write case. If buffers grow (higher message rates, more
   streams), bump it *and* `TimeoutStopSec=` in the unit together.
3. **Rotation cadence.** `rotate_minutes=60` ≈ 1 GB/h files at Binance
   peak. Drop to 15 if files become unwieldy to inspect.
4. **Log level.** `info` is right day-to-day. `recorder=info,binance_feed=debug`
   when debugging a feed; keep that brief — DEBUG can be hundreds of
   lines/sec.

---

## 14. Common failures

- `cargo build`: `error: linker 'cc' not found` → `apt install build-essential` not run.
- Recorder exit code 2 → config load failed. `eprintln` shows the path / TOML parse error.
- Recorder exit code 5 → `title_pattern` regex didn't compile. Escape `(`/`)` or double quotes properly in TOML.
- `gamma discovery pass kept=0` every tick → regex mismatch, see §13.
- Repeated `connection_error reason="websocket connect: ..."` → either VPS lost network (check `journalctl -b -1 | grep -i network`) or the venue changed something (rare).
- Disk alert firing constantly → offload + compress (§12) or size up.

---

## 15. Scope of this doc

Deliberately not covered:
- HTTPS proxies / VPN — not needed from a clean region.
- Secrets management — the recorder has none.
- CI/CD pipelines — build on the box.
- Prometheus / Grafana / log shipping — `journalctl` is enough for Phase 1.
- Multi-VPS / HA — one box. If it dies, reprovision + redeploy (<1 hour).

Trading-bot deployment is a separate doc and a separate host per the
architecture direction.
