# Tech debt / later fixes

## 1. Storage rotation alignment
Current:
- rotation buckets are measured from session start

Later:
- consider wall-clock aligned rotation if operationally useful

Why deferred:
- not a correctness issue
- current behavior is deterministic and replay-friendly

Trigger to revisit:
- when logs are being managed operationally across long-running sessions


## 2. Binary payload support
Current:
- storage only supports text payloads (`&str`)

Later:
- add raw byte / binary frame write path if any venue emits binary frames

Why deferred:
- Binance + Polymarket expected to be text for current recorder phase
- adding binary support now would be speculative

Trigger to revisit:
- first real binary frame observed
- or first feed requires non-UTF8 payload preservation


## 3. Store contention / writer architecture
Current:
- shared Store via `Arc<Mutex<Store>>`

Later:
- consider per-stream writers, channels, or sharded write path if contention appears

Why deferred:
- current priority is correctness, simplicity, replayability

Trigger to revisit:
- measured lock contention
- dropped throughput
- writer becoming bottleneck


## 4. Graceful feed shutdown
Current:
- Recorder uses `tokio::JoinHandle::abort()` to stop the feed on Ctrl-C.
- Works because `frame::process_text` has no `.await` points, so abort cannot land mid-write.
- Final `flush_all()` runs within a bounded grace window afterwards.

Later:
- Replace abort with a cooperative shutdown (e.g. `CancellationToken` or `tokio::sync::watch`) so feeds can drain in-flight writes, log final counters, and exit on their own.

Why deferred:
- Current approach is correct for BufWriter-backed storage; zero data loss observed in live testing.
- Cooperative shutdown is ergonomic polish, not a correctness gap.

Trigger to revisit:
- When multiple feeds are running and ordered shutdown matters.
- Or if `process_text` ever grows `.await` points (e.g. async storage), making abort unsafe.
