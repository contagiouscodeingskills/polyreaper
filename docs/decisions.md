# Project decisions

## 1. Recorder-first architecture
Decision:
- Build recorder before book builder, features, signals, or execution.

Reason:
- Raw data quality and replayability must be proven first.

Status:
- active


## 2. NDJSON as the recorder storage format
Decision:
- Use append-only NDJSON as the recorder storage format in phase 1.

Reason:
- It preserves raw payloads, is human-readable, replay-friendly, and simple to append safely.

Status:
- active


## 3. Session-directory layout
Decision:
- Store recorder output under session-based directories grouped by venue and stream.

Reason:
- This keeps capture sessions easy to inspect, replay, and debug.

Status:
- active


## 4. BTCUSDT-only in v1
Decision:
- Limit Binance capture scope to BTCUSDT in v1.

Reason:
- Keep the research scope narrow and focused on the target market hypothesis.

Status:
- active


## 5. Polymarket BTC 5-minute scope in v1
Decision:
- Limit Polymarket capture scope to BTC 5-minute up/down markets in v1.

Reason:
- Keep the recorder aligned to the specific edge hypothesis being tested.

Status:
- active


## 6. Windows Rust toolchain target
Decision:
- On Windows this repo targets the MSVC Rust toolchain (`stable-x86_64-pc-windows-msvc`).
- `rust-toolchain.toml` uses `channel = "stable"` (host-triple-agnostic) so the same file works on the Linux VPS too; MSVC selection is enforced per-host by `rustup set default-host x86_64-pc-windows-msvc`.
- Linker + LIB paths set in `.cargo/config.toml` under `[target.x86_64-pc-windows-msvc]` — inert on Linux.
- GNU (`stable-x86_64-pc-windows-gnu`) is not supported on Windows.

Reason:
- MSVC is Microsoft's first-class Windows Rust target — `tokio`, `reqwest`, `tokio-tungstenite`, and friends are tested primarily there.
- GNU needs a full MinGW-w64 bundle; rustup's self-contained tools are incomplete (no `as`, no full binutils) and fail on any crate that builds import libraries via `dlltool`.
- Setup is a one-time VS Installer step (Windows 10 SDK component); see `docs/SETUP_WINDOWS.md` if the setup ever needs to be redone on a fresh machine.

Status:
- active
