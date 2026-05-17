#!/usr/bin/env python3
"""Sigma sensitivity sweep on a bot decision log.

For each candidate sigma, re-run the implied-strike inversion AND the
fair-value computation against every record in the log. Report whether
any sigma produces a tight gap distribution and shrinks the
FV-vs-Polymarket mismatch meaningfully.

Pure stdlib. Uses statistics.NormalDist for the normal CDF + inverse.
"""

import json
import math
import statistics
import sys
import collections
from statistics import NormalDist

NORM = NormalDist()


def compute_fv_yes(btc, strike, ttr, sigma):
    """P(BTC_T > strike) under zero-drift GBM. Mirrors apps/bot/src/fv.rs."""
    if btc <= 0 or strike <= 0 or ttr <= 0 or sigma <= 0:
        if btc > strike:
            return 1.0
        if btc < strike:
            return 0.0
        return 0.5
    sigma_t = sigma * math.sqrt(ttr)
    d = (math.log(btc / strike) - 0.5 * sigma_t * sigma_t) / sigma_t
    return NORM.cdf(d)


def implied_strike(btc, ttr, sigma, p_yes):
    """Invert compute_fv_yes on K. Mirrors apps/bot/src/fv.rs."""
    if btc <= 0 or ttr <= 0 or sigma <= 0:
        return None
    if not (0.001 <= p_yes <= 0.999):
        return None
    sigma_t = sigma * math.sqrt(ttr)
    d = NORM.inv_cdf(p_yes)
    log_ratio = sigma_t * d + 0.5 * sigma_t * sigma_t
    return btc * math.exp(-log_ratio)


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


def evaluate_sigma(rows, sigma):
    gaps_usd = []
    gaps_bps = []
    abs_edges = []
    per_market_gaps = collections.defaultdict(list)

    for r in rows:
        btc = r.get("binance_btc_mid_usd")
        strike = r.get("binance_strike_usd")
        ttr = r.get("ttr_secs")
        py = r.get("poly_yes_mid")
        slug = r.get("market_slug")
        if btc is None or strike is None or ttr is None or py is None or ttr <= 0:
            continue
        if not (0.001 <= py <= 0.999):
            continue
        impl_k = implied_strike(btc, ttr, sigma, py)
        if impl_k is None:
            continue
        gap_usd = impl_k - strike
        gap_bps = gap_usd / strike * 10_000.0 if strike > 0 else 0.0
        gaps_usd.append(gap_usd)
        gaps_bps.append(gap_bps)
        per_market_gaps[slug].append(gap_usd)
        fv = compute_fv_yes(btc, strike, ttr, sigma)
        abs_edges.append(abs(fv - py))

    return gaps_usd, gaps_bps, abs_edges, per_market_gaps


def stat_line(values, fmt="+.4f"):
    if not values:
        return "(no data)"
    return (
        f"median={statistics.median(values):{fmt}} "
        f"mean={statistics.fmean(values):{fmt}} "
        f"stdev={statistics.pstdev(values):.4f}"
    )


def percentiles_line(values):
    return (
        f"p10={percentile(values, 10):+.2f} "
        f"p25={percentile(values, 25):+.2f} "
        f"p75={percentile(values, 75):+.2f} "
        f"p90={percentile(values, 90):+.2f}"
    )


def report_sigma(sigma, rows):
    print(f"\n{'=' * 78}")
    sig_t_pct = sigma * math.sqrt(300.0) * 100.0
    print(
        f"sigma={sigma:.0e}/sec   "
        f"(sigma*sqrt(300) = {sig_t_pct:.3f}% per 5min)"
    )
    print("=" * 78)
    gaps_usd, gaps_bps, abs_edges, per_market_gaps = evaluate_sigma(rows, sigma)
    if not gaps_usd:
        print("  (no usable records)")
        return None

    print(f"  gap_usd  n={len(gaps_usd):4d}   {stat_line(gaps_usd, '+.2f')}")
    print(f"           {percentiles_line(gaps_usd)} usd")
    print(f"  gap_bps  n={len(gaps_bps):4d}   {stat_line(gaps_bps, '+.2f')}")
    print(f"           {percentiles_line(gaps_bps)} bps")
    print(f"  |edge|   n={len(abs_edges):4d}   {stat_line(abs_edges, '+.4f')}")
    print(f"           {percentiles_line(abs_edges)}")

    print(f"\n  per-market mean gap (n_markets={len(per_market_gaps)}):")
    means = []
    for slug in sorted(per_market_gaps):
        gs = per_market_gaps[slug]
        m = statistics.fmean(gs)
        means.append(m)
        print(
            f"    {slug[-13:]:14s} n={len(gs):4d}  mean={m:+9.2f}usd  "
            f"range=[{min(gs):+8.1f}, {max(gs):+8.1f}]"
        )
    pos = sum(1 for m in means if m > 0)
    neg = sum(1 for m in means if m < 0)
    sign_str = "SIGN_FLIPS" if pos * neg > 0 else f"all_{'POSITIVE' if pos > 0 else 'NEGATIVE'}"
    print(f"  cross-market: {pos} positive-mean, {neg} negative-mean -> {sign_str}")
    if len(means) >= 2:
        cm_med = statistics.median(means)
        cm_std = statistics.pstdev(means)
        print(f"  cross-market median_of_means={cm_med:+.2f}usd, stdev_of_means={cm_std:.2f}usd")

    return {
        "sigma": sigma,
        "median_abs_edge": statistics.median(abs_edges),
        "median_gap_usd": statistics.median(gaps_usd),
        "gap_stdev_usd": statistics.pstdev(gaps_usd),
        "per_market_means": means,
        "sign_flips": pos > 0 and neg > 0,
        "n": len(gaps_usd),
    }


def main():
    path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "data/bot_session_20260517T115549Z/decisions.ndjson"
    )
    rows = [json.loads(l) for l in open(path, "r", encoding="utf-8")]
    print(f"loaded {len(rows)} records from {path}")

    sigmas = [1e-4, 2e-4, 5e-4, 1e-3]
    results = []
    for sigma in sigmas:
        r = report_sigma(sigma, rows)
        if r is not None:
            results.append(r)

    # Comparison table.
    print(f"\n{'=' * 78}")
    print("COMPARISON")
    print("=" * 78)
    print(
        f"{'sigma':>10}  {'med_|edge|':>10}  {'med_gap_usd':>12}  {'gap_stdev':>10}  "
        f"{'cm_med':>10}  {'cm_stdev':>10}  {'flips':>6}"
    )
    for r in results:
        cm_med = statistics.median(r["per_market_means"])
        cm_std = (
            statistics.pstdev(r["per_market_means"]) if len(r["per_market_means"]) >= 2 else 0.0
        )
        print(
            f"  {r['sigma']:.0e}  {r['median_abs_edge']:10.4f}  "
            f"{r['median_gap_usd']:+12.2f}  {r['gap_stdev_usd']:10.2f}  "
            f"{cm_med:+10.2f}  {cm_std:10.2f}  "
            f"{'YES' if r['sign_flips'] else 'no':>6}"
        )

    print()
    print("Read:")
    print("  med_|edge|   — median absolute mismatch between FV and poly_yes_mid (prob units)")
    print("  med_gap_usd  — median of (implied_strike - binance_strike) across all records")
    print("  gap_stdev    — within-cohort spread of the gap distribution (USD)")
    print("  cm_med       — median across the 6 per-market means (cross-market central)")
    print("  cm_stdev     — spread across the 6 per-market means (cross-market noise)")
    print("  flips        — does the per-market mean change sign across markets?")
    print()
    print("Verdict heuristic: a sigma 'fixes' the gap if median |edge| << 0.20,")
    print("gap_stdev shrinks substantially, AND cm_stdev shrinks (markets agree).")


if __name__ == "__main__":
    main()
