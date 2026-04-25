# Windows Rust toolchain setup

This repo targets the **MSVC** Rust toolchain (`stable-x86_64-pc-windows-msvc`).
See `docs/decisions.md` §6 for why.

The toolchain is not yet active because this machine is missing the Windows 10
SDK. The steps below move the repo from "GNU with ansi-feature off" (works for
Phase 1 skeleton) to "MSVC fully configured" (ready for tokio / reqwest /
tungstenite, i.e. `binance_feed` and anything async).

Do this once.

---

## 1. Install the Windows 10 SDK (user action)

Pick one of the following. All three install the same thing.

### Option A — Visual Studio Installer (recommended; GUI)
1. Open **Visual Studio Installer** from the Start menu.
2. Find **Visual Studio Build Tools 2019**, click **Modify**.
3. On the *Individual components* tab, check a recent **Windows 10 SDK**
   (10.0.19041 or newer is fine; any 10.0.x works).
4. Install. Closing and reopening the shell afterwards is not required —
   cargo picks up new files on the next build.

### Option B — Chocolatey (needs admin)
```powershell
choco install windows-sdk-10-version-2004-all -y
```

### Option C — Standalone installer from Microsoft
<https://developer.microsoft.com/en-us/windows/downloads/windows-sdk/>

---

## 2. Sanity-check the install

In Git Bash:

```bash
ls "/c/Program Files (x86)/Windows Kits/10/Lib/" | head -5
```

You should see at least one SDK version directory, e.g. `10.0.19041.0`. Note
that version — you'll reference it in step 4.

---

## 3. Apply the repo-side toolchain changes

Once the SDK is installed, three small edits land in the repo:

### 3a. Set the host default toolchain to MSVC

The committed `rust-toolchain.toml` uses `channel = "stable"` (no host triple)
so it works on both Windows and Linux. On Windows it resolves to whatever
`rustup` has set as its **default-host**. Confirm, and fix if needed:

```bash
rustup show                     # "Default host: x86_64-pc-windows-msvc" ✓
# If default-host is gnu:
rustup set default-host x86_64-pc-windows-msvc
```

Also make sure the MSVC toolchain is installed (it probably already is):

```bash
rustup toolchain install stable-x86_64-pc-windows-msvc
```

### 3b. Configure the linker + LIB env

Create `.cargo/config.toml` at the repo root. **Replace `{VERSION}` with the
SDK version from step 2, and `{MSVC}` with the folder name under
`VC\Tools\MSVC\`** (currently `14.29.30133` on this machine):

```toml
[target.x86_64-pc-windows-msvc]
# Explicit path so cargo doesn't pick up Git Bash's /usr/bin/link (coreutils).
linker = "C:\\Program Files (x86)\\Microsoft Visual Studio\\2019\\BuildTools\\VC\\Tools\\MSVC\\{MSVC}\\bin\\Hostx64\\x64\\link.exe"

[env]
# Needed for link.exe to find kernel32.lib, msvcrt.lib, ucrt, etc.
LIB = "C:\\Program Files (x86)\\Microsoft Visual Studio\\2019\\BuildTools\\VC\\Tools\\MSVC\\{MSVC}\\lib\\x64;C:\\Program Files (x86)\\Windows Kits\\10\\Lib\\{VERSION}\\um\\x64;C:\\Program Files (x86)\\Windows Kits\\10\\Lib\\{VERSION}\\ucrt\\x64"
```

These paths are machine-specific (single-developer repo — documented as such
in `docs/TECH_DEBT.md` §4).

### 3c. Re-enable the `ansi` feature on `tracing-subscriber`

In workspace `Cargo.toml`, replace:

```toml
tracing-subscriber = { version = "0.3", default-features = false, features = ["fmt", "env-filter", "json", "smallvec", "std", "tracing-log"] }
```

with the default features back on:

```toml
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
```

Also drop the explanatory comment above it.

---

## 4. Verify

```bash
cargo clean
cargo build --workspace
cargo test --workspace
cargo run -p recorder
```

Everything should build, all tests should pass, and the recorder logs
should come out with ANSI colour (if you're in a colour-capable terminal).

When this is confirmed:
- Close out `docs/TECH_DEBT.md` §4 (delete the section; git history keeps
  the audit trail).
- No other doc changes needed — `docs/decisions.md` §7 is already written.
