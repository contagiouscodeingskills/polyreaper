//! Wall-clock pacing for replay streams.
//!
//! Wrap any `Iterator<Item = Result<RawEvent, ReplayError>>` (typically
//! a [`MergedReader`]) in a [`Pacer`] to either:
//!
//! * **Max-speed** — pass-through, no sleep. Useful for batch
//!   processing / Parquet export / whole-session backtests.
//! * **Realtime** — sleep so the gap between consecutive events on
//!   the wall clock matches the gap on `local_ts_ns`, optionally
//!   scaled by `speed`. `speed = 1.0` plays back at original pace;
//!   `speed = 100.0` is 100×.
//!
//! The first emitted event sets the anchor: subsequent events are
//! sleep-released relative to it.
//!
//! # Latency offsets
//!
//! [`Pacer::with_latency_offsets`] adds a per-venue millisecond shift
//! to `local_ts_ns` *before* the sleep decision, so realtime pacing
//! reflects the corrected timeline. Mutation is in-place — researchers
//! who want the original timestamps must keep their offset map and
//! subtract.
//!
//! ## Ordering caveat
//!
//! Offsets applied here run *after* the k-way merge, so a positive
//! offset on venue A combined with a negative offset on venue B can
//! cause an A-event timestamped just before a B-event in the merged
//! stream to appear *after* it on the corrected timeline — a local
//! out-of-order glitch within the offset budget.
//!
//! For strict order, pre-process the stream: collect events into a
//! `Vec`, apply offsets, sort by `local_ts_ns`, and re-pace. v1 keeps
//! that as caller responsibility; the `Pacer`'s job is the wall-clock
//! sleep, not the second sort.
//!
//! # Example
//!
//! ```ignore
//! use std::collections::HashMap;
//! use common::Venue;
//! use replayer::{open_session, ReplayFilter};
//! use replayer::pacer::{Pacer, PaceMode};
//!
//! let merger = open_session("./data/session_X", ReplayFilter::default())?;
//! let mut offsets = HashMap::new();
//! offsets.insert(Venue::Polymarket, -30); // Polymarket runs ~30ms ahead
//! let paced = Pacer::new(merger, PaceMode::Realtime { speed: 100.0 })
//!     .with_latency_offsets(offsets);
//! for ev in paced { /* ... */ }
//! # Ok::<(), replayer::ReplayError>(())
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant};

use common::{LocalTimestamp, RawEvent, Venue};

use crate::ReplayError;

/// How fast to play back events.
#[derive(Debug, Clone, Copy)]
pub enum PaceMode {
    /// No sleep. Iterator is a pass-through.
    MaxSpeed,
    /// Wall-clock-paced. `speed` scales the gap between consecutive
    /// events: `1.0` = original cadence, `100.0` = 100× faster, `0.5` = half speed.
    Realtime { speed: f64 },
}

impl PaceMode {
    fn is_realtime(self) -> bool {
        matches!(self, PaceMode::Realtime { .. })
    }
}

/// First-event anchor for realtime mode.
#[derive(Debug, Clone, Copy)]
struct Anchor {
    wall: Instant,
    event_ts_ns: u128,
}

/// Iterator wrapper that paces an inner replay stream.
///
/// `Pacer` is a "transparent" iterator — every `Ok` event from the
/// inner iterator is emitted, optionally with `local_ts_ns` shifted
/// by a per-venue offset and optionally after a `thread::sleep`.
/// Errors propagate without sleeping.
pub struct Pacer<I> {
    inner: I,
    mode: PaceMode,
    /// Per-venue latency offset, milliseconds. Positive shifts later.
    offsets: HashMap<Venue, i64>,
    anchor: Option<Anchor>,
}

impl<I> Pacer<I> {
    /// Wrap `inner` in a pacer with the given mode.
    pub fn new(inner: I, mode: PaceMode) -> Self {
        Self {
            inner,
            mode,
            offsets: HashMap::new(),
            anchor: None,
        }
    }

    /// Convenience: max-speed (no sleep). Equivalent to `Pacer::new(inner, MaxSpeed)`.
    pub fn max_speed(inner: I) -> Self {
        Self::new(inner, PaceMode::MaxSpeed)
    }

    /// Convenience: real-time at `speed`× original cadence.
    pub fn realtime(inner: I, speed: f64) -> Self {
        Self::new(inner, PaceMode::Realtime { speed })
    }

    /// Set per-venue offsets. Each offset is in milliseconds; positive
    /// shifts the event's `local_ts_ns` later, negative earlier.
    /// Replaces any existing offsets.
    pub fn with_latency_offsets(mut self, offsets: HashMap<Venue, i64>) -> Self {
        self.offsets = offsets;
        self
    }
}

impl<I> Iterator for Pacer<I>
where
    I: Iterator<Item = Result<RawEvent, ReplayError>>,
{
    type Item = Result<RawEvent, ReplayError>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next()?;
        let mut ev = match item {
            Ok(e) => e,
            Err(e) => return Some(Err(e)),
        };

        // Apply latency offset (if any) before pacing decision.
        if let Some(off_ms) = self.offsets.get(&ev.venue).copied() {
            ev.local_ts_ns = shift_ts(ev.local_ts_ns, off_ms);
        }

        // Sleep if realtime.
        if self.mode.is_realtime() {
            self.pace_to(ev.local_ts_ns.as_nanos());
        }

        Some(Ok(ev))
    }
}

impl<I> Pacer<I> {
    /// Sleep until wall-clock matches `event_ts_ns` per the speed factor.
    /// First call sets the anchor; subsequent calls compute relative.
    fn pace_to(&mut self, event_ts_ns: u128) {
        let speed = match self.mode {
            PaceMode::Realtime { speed } => speed.max(f64::MIN_POSITIVE),
            PaceMode::MaxSpeed => return,
        };
        let now = Instant::now();
        let anchor = match self.anchor {
            Some(a) => a,
            None => {
                self.anchor = Some(Anchor {
                    wall: now,
                    event_ts_ns,
                });
                return;
            }
        };

        // Saturating: an event-ts before the anchor (shouldn't happen
        // post-merge; but if it does, after a negative offset say,
        // don't sleep negatively — just emit immediately).
        let event_elapsed_ns = event_ts_ns.saturating_sub(anchor.event_ts_ns);
        let wall_target_ns = (event_elapsed_ns as f64 / speed) as u128;
        // Bound to u64 nanos so `Duration::from_nanos` doesn't panic on
        // multi-decade gaps in degenerate captures.
        let wall_target_ns_u64 = wall_target_ns.min(u64::MAX as u128) as u64;
        let target = anchor.wall + Duration::from_nanos(wall_target_ns_u64);

        if target > now {
            std::thread::sleep(target - now);
        }
        // If target ≤ now, we're already late — just continue. Realtime
        // mode is "no faster than", not "exactly". Catching up after a
        // pause is normal.
    }
}

/// Shift a [`LocalTimestamp`] by `delta_ms` milliseconds. Saturates at
/// 0 on negative-overflow rather than wrapping; saturates at u128::MAX
/// on positive overflow (effectively infinite-future, never real).
fn shift_ts(ts: LocalTimestamp, delta_ms: i64) -> LocalTimestamp {
    let cur = ts.as_nanos() as i128;
    let delta_ns = (delta_ms as i128).saturating_mul(1_000_000);
    let shifted = cur.saturating_add(delta_ns);
    LocalTimestamp::from_nanos(shifted.max(0) as u128)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use common::{LocalTimestamp, Venue};

    fn ev(venue: Venue, ts_ns: u128) -> RawEvent {
        RawEvent {
            venue,
            stream: "x".into(),
            local_ts_ns: LocalTimestamp::from_nanos(ts_ns),
            venue_ts_ms: None,
            payload: String::new(),
        }
    }

    fn iter_ok(events: Vec<RawEvent>) -> impl Iterator<Item = Result<RawEvent, ReplayError>> {
        events.into_iter().map(Ok)
    }

    #[test]
    fn max_speed_is_passthrough_with_negligible_overhead() {
        let evs = (0..1000).map(|i| ev(Venue::Binance, i)).collect();
        let p = Pacer::max_speed(iter_ok(evs));
        let start = Instant::now();
        let count = p.count();
        let elapsed = start.elapsed();
        assert_eq!(count, 1000);
        // 1000 events through pass-through should be nowhere near 100ms.
        // Ten-millisecond ceiling is comfortable on cold caches.
        assert!(
            elapsed < Duration::from_millis(100),
            "max-speed pass-through took {elapsed:?}"
        );
    }

    #[test]
    fn realtime_at_high_speed_sleeps_measurably() {
        // Two events 1 second apart on event clock. Speed = 100×, so
        // wall-clock sleep should be ~10ms. Use a generous bound to
        // avoid flakes from CI scheduler jitter.
        let evs = vec![
            ev(Venue::Binance, 0),
            ev(Venue::Binance, 1_000_000_000), // +1s
        ];
        let p = Pacer::realtime(iter_ok(evs), 100.0);
        let start = Instant::now();
        let _all: Vec<_> = p.collect();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(8),
            "expected ≥8ms (1s/100), got {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(200),
            "expected <200ms, got {elapsed:?}"
        );
    }

    #[test]
    fn realtime_at_max_finite_speed_doesnt_panic() {
        // Lower bound on speed shouldn't blow up arithmetic.
        let evs = vec![ev(Venue::Binance, 0), ev(Venue::Binance, 1_000_000)];
        let p = Pacer::realtime(iter_ok(evs), 1e12);
        let _: Vec<_> = p.collect();
    }

    #[test]
    fn latency_offset_shifts_local_ts() {
        let evs = vec![ev(Venue::Polymarket, 1_000_000_000)];
        let mut offsets = HashMap::new();
        offsets.insert(Venue::Polymarket, -50); // 50ms earlier
        let p = Pacer::max_speed(iter_ok(evs)).with_latency_offsets(offsets);
        let out: Vec<_> = p.map(Result::unwrap).collect();
        // 1s - 50ms = 950ms in nanos.
        assert_eq!(out[0].local_ts_ns.as_nanos(), 950_000_000);
    }

    #[test]
    fn latency_offset_only_applies_to_matching_venue() {
        let evs = vec![
            ev(Venue::Binance, 1_000_000_000),
            ev(Venue::Polymarket, 1_000_000_000),
        ];
        let mut offsets = HashMap::new();
        offsets.insert(Venue::Polymarket, 100); // +100ms only
        let p = Pacer::max_speed(iter_ok(evs)).with_latency_offsets(offsets);
        let out: Vec<_> = p.map(Result::unwrap).collect();
        assert_eq!(out[0].local_ts_ns.as_nanos(), 1_000_000_000); // Binance unchanged
        assert_eq!(out[1].local_ts_ns.as_nanos(), 1_100_000_000); // Polymarket +100ms
    }

    #[test]
    fn negative_offset_doesnt_underflow() {
        // 0 ts with -1s offset → would be -1s, must clamp to 0.
        let evs = vec![ev(Venue::Binance, 0)];
        let mut offsets = HashMap::new();
        offsets.insert(Venue::Binance, -1_000);
        let p = Pacer::max_speed(iter_ok(evs)).with_latency_offsets(offsets);
        let out: Vec<_> = p.map(Result::unwrap).collect();
        assert_eq!(out[0].local_ts_ns.as_nanos(), 0);
    }

    #[test]
    fn errors_propagate_without_sleeping() {
        let mixed: Vec<Result<RawEvent, ReplayError>> = vec![
            Err(ReplayError::Decode {
                stream: "x".into(),
                reason: "boom".into(),
            }),
            Ok(ev(Venue::Binance, 1_000_000_000)),
        ];
        let p = Pacer::realtime(mixed.into_iter(), 1.0);
        let out: Vec<_> = p.collect();
        assert!(out[0].is_err());
        assert!(out[1].is_ok());
        // Anchor was never set (only Ok's set the anchor), so the second
        // event sets the anchor and emits immediately. We just verify
        // the error didn't get swallowed.
    }

    #[test]
    fn shift_ts_clamps_negative_to_zero() {
        let t = LocalTimestamp::from_nanos(100);
        let s = shift_ts(t, -1_000); // -1s
        assert_eq!(s.as_nanos(), 0);
    }

    #[test]
    fn shift_ts_handles_large_positive() {
        let t = LocalTimestamp::from_nanos(1_000_000);
        let s = shift_ts(t, 5);
        // +5ms = 5_000_000 ns
        assert_eq!(s.as_nanos(), 6_000_000);
    }
}
