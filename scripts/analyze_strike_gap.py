#!/usr/bin/env python3
"""Quantify cross-venue strike divergence from a bot decision log.

Reads `decisions.ndjson` from a `data/bot_session_*/` directory and
reports the gap between the Polymarket-implied strike and the
Binance-snapped strike.

Outputs:
- record counts by decision_kind / no_signal_reason / incomplete_reason
- strike_gap_usd and strike_gap_bps: median, p10/p25/p75/p90, mean
- per-market: mean and trend (linear fit over time) so we can tell
  systematic-bias from random-walk
- a "verdict" line: stable, drift, or random based on simple thresholds

Usage:
    python scripts/analyze_strike_gap.py data/bot_session_<id>

This is intentionally a small, self-contained script — no pandas, only
stdlib + statistics, so it runs on a fresh checkout.
"""

import json
import math
import os
import statistics
import sys
from collections import Counter, defaultdict


def percentile(values, p):
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * (p / 100.0)
    f = int(math.floor(k))
    c = int(math.ceil(k))
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def load_records(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"warn: skipping line {ln}: {e}", file=sys.stderr)
    return rows


def linear_fit(xs, ys):
    """Return slope of OLS y = a + b*x. None if fewer than 2 points."""
    if len(xs) < 2:
        return None
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return None
    return num / den


def summarize_distribution(label, values):
    print(f"\n{label} (n={len(values)}):")
    if not values:
        print("  (no data)")
        return
    print(f"  median: {statistics.median(values):+.4f}")
    print(f"  mean:   {statistics.fmean(values):+.4f}")
    print(f"  stdev:  {statistics.pstdev(values):.4f}")
    print(f"  p10:    {percentile(values, 10):+.4f}")
    print(f"  p25:    {percentile(values, 25):+.4f}")
    print(f"  p75:    {percentile(values, 75):+.4f}")
    print(f"  p90:    {percentile(values, 90):+.4f}")
    print(f"  min:    {min(values):+.4f}")
    print(f"  max:    {max(values):+.4f}")


def main():
    if len(sys.argv) != 2:
        print("usage: analyze_strike_gap.py <bot_session_dir>", file=sys.stderr)
        sys.exit(2)
    session_dir = sys.argv[1]
    decisions_path = os.path.join(session_dir, "decisions.ndjson")
    if not os.path.isfile(decisions_path):
        print(f"error: {decisions_path} not found", file=sys.stderr)
        sys.exit(2)

    rows = load_records(decisions_path)
    print(f"loaded {len(rows)} decision records from {decisions_path}")

    # Counts by decision_kind.
    kinds = Counter(r.get("decision_kind") for r in rows)
    print("\ndecision_kind counts:")
    for k, v in kinds.most_common():
        print(f"  {k:20s} {v}")

    # No-signal / incomplete reasons.
    ns = Counter(r.get("no_signal_reason") for r in rows if r.get("no_signal_reason"))
    if ns:
        print("\nno_signal_reason counts:")
        for k, v in ns.most_common():
            print(f"  {k:25s} {v}")
    inc = Counter(r.get("incomplete_reason") for r in rows if r.get("incomplete_reason"))
    if inc:
        print("\nincomplete_reason counts:")
        for k, v in inc.most_common():
            print(f"  {k:25s} {v}")
    rej = Counter(r.get("reject_reason") for r in rows if r.get("reject_reason"))
    if rej:
        print("\nreject_reason counts:")
        for k, v in rej.most_common():
            print(f"  {k:25s} {v}")

    # Cross-venue strike gap. Only count rows where both strikes are present.
    gaps_usd = [
        r["strike_gap_usd"]
        for r in rows
        if r.get("strike_gap_usd") is not None and r.get("binance_strike_usd") is not None
    ]
    gaps_bps = [
        r["strike_gap_bps"]
        for r in rows
        if r.get("strike_gap_bps") is not None
    ]
    summarize_distribution("strike_gap_usd (implied - Binance)", gaps_usd)
    summarize_distribution("strike_gap_bps", gaps_bps)

    # Per-market analysis: mean gap and slope over time (seconds) per market.
    by_market = defaultdict(list)
    for r in rows:
        if r.get("strike_gap_usd") is None:
            continue
        m = r.get("market_slug")
        if not m:
            continue
        try:
            ts_ns = int(r["local_ts_ns"])
        except (KeyError, ValueError):
            continue
        by_market[m].append((ts_ns / 1.0e9, r["strike_gap_usd"]))

    if by_market:
        print(f"\nper-market gap (n_markets={len(by_market)}):")
        per_market_means = []
        slopes = []
        for slug, samples in sorted(by_market.items()):
            samples.sort()
            ts = [t for t, _ in samples]
            gs = [g for _, g in samples]
            mean = statistics.fmean(gs)
            slope_per_sec = linear_fit(ts, gs)
            per_market_means.append(mean)
            if slope_per_sec is not None:
                slopes.append(slope_per_sec)
            slope_str = (
                f"slope={slope_per_sec:+.3f}usd/s"
                if slope_per_sec is not None
                else "slope=n/a"
            )
            print(
                f"  {slug:35s} n={len(samples):4d}  "
                f"mean={mean:+8.2f}usd  "
                f"min={min(gs):+8.2f}  max={max(gs):+8.2f}  "
                f"{slope_str}"
            )

        # Stable vs random vs biased verdict.
        # Stable: median across markets is small AND per-market means cluster tight.
        # Biased: median is far from zero (consistently signed).
        # Random: large stdev across markets AND median close to zero.
        cross_market_median = statistics.median(per_market_means)
        cross_market_std = statistics.pstdev(per_market_means) if len(per_market_means) >= 2 else 0.0
        print(
            f"\ncross-market: median_of_means={cross_market_median:+.2f}usd, "
            f"stdev_of_means={cross_market_std:.2f}usd"
        )
        verdict = classify(cross_market_median, cross_market_std)
        print(f"\nVERDICT: {verdict}")


def classify(median_of_means_usd, stdev_of_means_usd):
    """Heuristic verdict. Thresholds are first-pass — refine after seeing
    real data."""
    abs_med = abs(median_of_means_usd)
    if abs_med < 5.0 and stdev_of_means_usd < 10.0:
        return (
            "STABLE — median cross-market gap is small and tight across markets. "
            "Binance and Polymarket's oracle agree closely on average."
        )
    if abs_med >= 5.0 and stdev_of_means_usd < abs_med:
        sign = "high" if median_of_means_usd > 0 else "low"
        return (
            f"SYSTEMATICALLY BIASED — Polymarket implies a {sign}er strike than Binance "
            f"by ~{median_of_means_usd:+.1f}usd on average across markets, with low cross-market "
            f"variation. Likely a real cross-venue oracle divergence (e.g. Chainlink-vs-Binance). "
            f"The current bot would have systematic-bias signals; do NOT enable live until this is addressed."
        )
    return (
        f"RANDOM/NOISY — median {median_of_means_usd:+.2f}usd, cross-market stdev "
        f"{stdev_of_means_usd:.2f}usd. Gap is large but unsigned — likely a mix of Polymarket spread, "
        f"thin-book noise, and possibly transient oracle lag. Worth deeper case-study analysis."
    )


if __name__ == "__main__":
    main()
