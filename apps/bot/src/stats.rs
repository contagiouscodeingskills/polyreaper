//! Shared rolling-window statistics utilities.
//!
//! Used by the bot's feature extractor to compute z-scores of feature
//! values vs their own rolling distribution — so the model self-adapts
//! to changing regimes (vol, volume, momentum) without manual re-tuning
//! of any feature's scale.

use std::collections::VecDeque;

/// Rolling-window stats (mean, variance, stdev, median) over the most
/// recent `(t, value)` samples within `window_secs`.
///
/// Implementation: keeps samples in a VecDeque, prunes by time on each
/// `observe`, and recomputes mean+variance from running sums. Sums are
/// updated incrementally as samples are added/removed, so per-observe
/// cost is O(1) amortised (plus the prune work which is bounded by
/// sample rate × window size).
#[derive(Debug, Clone)]
pub struct RollingStats {
    samples: VecDeque<(f64, f64)>, // (t_secs, value)
    window_secs: f64,
    sum: f64,
    sum_sq: f64,
    min_samples: usize,
}

impl RollingStats {
    pub fn new(window_secs: f64) -> Self {
        Self {
            samples: VecDeque::new(),
            window_secs,
            sum: 0.0,
            sum_sq: 0.0,
            min_samples: 5,
        }
    }

    pub fn with_min_samples(window_secs: f64, min_samples: usize) -> Self {
        Self {
            samples: VecDeque::new(),
            window_secs,
            sum: 0.0,
            sum_sq: 0.0,
            min_samples,
        }
    }

    pub fn observe(&mut self, t_secs: f64, value: f64) {
        if !(t_secs.is_finite() && value.is_finite()) {
            return;
        }
        self.samples.push_back((t_secs, value));
        self.sum += value;
        self.sum_sq += value * value;
        let cutoff = t_secs - self.window_secs;
        while let Some(&(t, v)) = self.samples.front() {
            if t < cutoff {
                self.samples.pop_front();
                self.sum -= v;
                self.sum_sq -= v * v;
            } else {
                break;
            }
        }
    }

    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    pub fn mean(&self) -> Option<f64> {
        if self.samples.len() < self.min_samples {
            return None;
        }
        Some(self.sum / self.samples.len() as f64)
    }

    pub fn variance(&self) -> Option<f64> {
        if self.samples.len() < self.min_samples {
            return None;
        }
        let n = self.samples.len() as f64;
        let mean = self.sum / n;
        let var = (self.sum_sq / n) - mean * mean;
        // Numerical noise can yield tiny negative variance; clamp.
        Some(var.max(0.0))
    }

    pub fn stdev(&self) -> Option<f64> {
        self.variance().map(f64::sqrt)
    }

    /// Z-score of `value` against the rolling mean+stdev. Returns
    /// `None` when there aren't enough samples or stdev is degenerate
    /// (zero or NaN).
    pub fn z_score(&self, value: f64) -> Option<f64> {
        let mean = self.mean()?;
        let stdev = self.stdev()?;
        if !(stdev.is_finite() && stdev > 1e-12) {
            return None;
        }
        Some((value - mean) / stdev)
    }

    /// Median of retained samples (O(n log n) via sort — not hot-path).
    /// Useful for adaptive baselines (e.g. spread baseline = rolling median).
    pub fn median(&self) -> Option<f64> {
        if self.samples.len() < self.min_samples {
            return None;
        }
        let mut vals: Vec<f64> = self.samples.iter().map(|&(_, v)| v).collect();
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = vals.len();
        Some(if n % 2 == 1 {
            vals[n / 2]
        } else {
            0.5 * (vals[n / 2 - 1] + vals[n / 2])
        })
    }

    /// Sum of values in the window. Useful for "total volume over last N seconds".
    pub fn sum(&self) -> f64 {
        self.sum
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64, eps: f64) -> bool {
        (a - b).abs() < eps
    }

    #[test]
    fn empty_returns_none() {
        let s = RollingStats::new(60.0);
        assert!(s.mean().is_none());
        assert!(s.stdev().is_none());
        assert!(s.z_score(1.0).is_none());
    }

    #[test]
    fn min_samples_gates_output() {
        let mut s = RollingStats::with_min_samples(60.0, 5);
        for i in 0..4 {
            s.observe(i as f64, i as f64);
        }
        assert!(s.mean().is_none());
        s.observe(4.0, 4.0);
        assert!(s.mean().is_some());
    }

    #[test]
    fn mean_and_stdev_correct_for_uniform_sequence() {
        let mut s = RollingStats::with_min_samples(120.0, 1);
        for i in 0..10 {
            s.observe(i as f64, i as f64);
        }
        // Values: 0..9, mean = 4.5, var = 8.25, stdev = 2.872
        assert!(approx(s.mean().unwrap(), 4.5, 1e-9));
        assert!(approx(s.variance().unwrap(), 8.25, 1e-9));
        assert!(approx(s.stdev().unwrap(), 8.25_f64.sqrt(), 1e-9));
    }

    #[test]
    fn z_score_is_zero_at_mean() {
        let mut s = RollingStats::with_min_samples(60.0, 1);
        for i in 0..10 {
            s.observe(i as f64, (i % 2) as f64);
        }
        // values alternate 0,1 → mean = 0.5
        let z = s.z_score(0.5).unwrap();
        assert!(approx(z, 0.0, 1e-9));
    }

    #[test]
    fn z_score_one_at_one_sigma() {
        let mut s = RollingStats::with_min_samples(60.0, 1);
        // Values with known stdev. Use {-1, 1} alternating.
        for i in 0..10 {
            s.observe(i as f64, if i % 2 == 0 { -1.0 } else { 1.0 });
        }
        // mean = 0, stdev = 1
        let z = s.z_score(1.0).unwrap();
        assert!(approx(z, 1.0, 1e-9));
    }

    #[test]
    fn prunes_old_samples() {
        let mut s = RollingStats::with_min_samples(10.0, 1);
        for i in 0..20 {
            s.observe(i as f64, i as f64);
        }
        // window 10s, last sample t=19, cutoff = 9. Survivors: t ∈ [9, 19] → 11 samples
        assert!(s.len() <= 11);
        // mean of 9..=19 = 154/11 = 14
        assert!((s.mean().unwrap() - 14.0).abs() < 1e-6);
    }

    #[test]
    fn median_correct() {
        let mut s = RollingStats::with_min_samples(60.0, 1);
        for v in [1.0, 3.0, 5.0, 7.0, 9.0] {
            s.observe(v, v);
        }
        assert!(approx(s.median().unwrap(), 5.0, 1e-9));
        s.observe(11.0, 11.0);
        // 6 values: 1,3,5,7,9,11 → median (5+7)/2 = 6
        assert!(approx(s.median().unwrap(), 6.0, 1e-9));
    }

    #[test]
    fn ignores_nonfinite_input() {
        let mut s = RollingStats::with_min_samples(60.0, 1);
        s.observe(0.0, f64::NAN);
        s.observe(1.0, f64::INFINITY);
        s.observe(2.0, 1.0);
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn z_score_none_when_stdev_zero() {
        let mut s = RollingStats::with_min_samples(60.0, 1);
        for i in 0..5 {
            s.observe(i as f64, 7.0); // constant
        }
        // stdev = 0 → degenerate
        assert!(s.z_score(7.0).is_none());
    }
}
