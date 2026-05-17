#!/usr/bin/env python3
"""Z-score regression — does Polymarket price P(BTC > strike) under GBM?

If Polymarket bidders price the canonical GBM probability with SOME
volatility, then poly_yes_mid = Phi(d) where
    d = (ln(S/K) - 0.5*sigma^2*T) / (sigma*sqrt(T))
and Phi is the standard-normal CDF.

That implies probit(poly_yes_mid) ~ d is *linear with slope 1*. If we
regress probit(poly_yes) on d for many different sigma values and find
ANY sigma that produces R^2 high and slope ~1, the bot's model class
(GBM probability around a strike) is correct — we just need to use
Polymarket's effective sigma. If no sigma fits, the model class itself
is wrong and the bot can't price these markets the way it's trying to.

Three sub-tests:
  A. Aggregate: poly_yes_mid against bot's recorded fv_yes (sanity).
  B. Aggregate: probit(poly_yes) against Z using the bot's per-record sigma.
  C. Sweep: probit(poly_yes) against Z(sigma) for fixed sigma in a grid,
     report which sigma maximises R^2 and what the slope is at that point.
  D. Per-market: repeat Test C's best-sigma regression for each market
     individually to see whether markets agree or vary.

Stdlib only.
"""

import collections
import json
import math
import statistics
import sys
from statistics import NormalDist

NORM = NormalDist()


def z_score(btc, strike, sigma_per_sec, ttr_secs):
    """The d in compute_fv: (ln(S/K) - 0.5 sigma^2 T) / (sigma sqrt T)."""
    if sigma_per_sec is None or sigma_per_sec <= 0 or ttr_secs <= 0:
        return None
    sigma_t = sigma_per_sec * math.sqrt(ttr_secs)
    return (math.log(btc / strike) - 0.5 * sigma_t * sigma_t) / sigma_t


def ols(xs, ys):
    n = len(xs)
    if n < 2:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    if sxx == 0 or syy == 0:
        return None
    slope = sxy / sxx
    intercept = my - slope * mx
    r2 = (sxy * sxy) / (sxx * syy)
    return slope, intercept, r2


def main():
    path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "data/bot_session_20260517T115549Z/decisions.ndjson"
    )
    rows = [json.loads(l) for l in open(path, "r", encoding="utf-8")]
    print(f"loaded {len(rows)} records from {path}")

    # Filter to records with all the fields we need for the regression.
    base = []
    for r in rows:
        btc = r.get("binance_btc_mid_usd")
        strike = r.get("binance_strike_usd")
        ttr = r.get("ttr_secs")
        py = r.get("poly_yes_mid")
        if btc is None or strike is None or ttr is None or py is None:
            continue
        if ttr <= 0 or btc <= 0 or strike <= 0:
            continue
        # Restrict to [0.005, 0.995] so probit is well-defined and not
        # dominated by near-saturation noise.
        if not (0.005 <= py <= 0.995):
            continue
        base.append(r)
    print(f"records usable for regression: {len(base)}")

    # --- Test A. poly_yes_mid vs bot's recorded fv_yes ----------------------
    print("\n" + "=" * 78)
    print("A. poly_yes_mid vs bot's recorded fv_yes  (sanity — using whatever σ the bot used)")
    print("=" * 78)
    pairs = [(r["fv_yes"], r["poly_yes_mid"]) for r in base if r.get("fv_yes") is not None]
    if pairs:
        xs, ys = zip(*pairs)
        res = ols(list(xs), list(ys))
        if res:
            s, i, r2 = res
            print(f"  poly_yes = {i:+.4f} + {s:+.4f} * fv_yes   R^2 = {r2:.4f}   n = {len(xs)}")
            print(f"  median fv_yes = {statistics.median(xs):.4f}")
            print(f"  median poly_yes_mid = {statistics.median(ys):.4f}")

    # --- Test B. probit(poly_yes) vs Z using bot's recorded σ ---------------
    print("\n" + "=" * 78)
    print("B. probit(poly_yes) vs Z(bot's per-record σ)")
    print("=" * 78)
    zs, ps = [], []
    for r in base:
        sigma = r.get("sigma_per_sec_used")
        z = z_score(r["binance_btc_mid_usd"], r["binance_strike_usd"], sigma, r["ttr_secs"])
        if z is None:
            continue
        zs.append(z)
        ps.append(NORM.inv_cdf(r["poly_yes_mid"]))
    if zs:
        res = ols(zs, ps)
        if res:
            s, i, r2 = res
            print(f"  probit(poly_yes) = {i:+.4f} + {s:+.4f} * Z   R^2 = {r2:.4f}   n = {len(zs)}")
            print("  (slope ~ 1 means GBM model class is right at the σ the bot used)")
            print("  (slope < 1 means effective σ is LARGER than bot's σ)")
            print("  (slope > 1 means effective σ is SMALLER than bot's σ)")

    # --- Test C. Sweep fixed σ; find R^2-maximising σ -----------------------
    print("\n" + "=" * 78)
    print("C. probit(poly_yes) vs Z(fixed σ) — sweep")
    print("=" * 78)
    grid = [
        5e-6,
        1e-5,
        2e-5,
        5e-5,
        1e-4,
        2e-4,
        5e-4,
        1e-3,
        2e-3,
        5e-3,
    ]
    best = None
    print(f"  {'σ':>10}  {'slope':>9}  {'intercept':>11}  {'R^2':>7}  {'n':>5}")
    for sigma in grid:
        zs, ps = [], []
        for r in base:
            z = z_score(
                r["binance_btc_mid_usd"], r["binance_strike_usd"], sigma, r["ttr_secs"]
            )
            if z is None:
                continue
            zs.append(z)
            ps.append(NORM.inv_cdf(r["poly_yes_mid"]))
        if not zs:
            continue
        res = ols(zs, ps)
        if not res:
            continue
        s, i, r2 = res
        print(f"  {sigma:>10.0e}  {s:>+9.4f}  {i:>+11.4f}  {r2:>7.4f}  {len(zs):>5}")
        if best is None or r2 > best[3]:
            best = (sigma, s, i, r2)
    if best is not None:
        sigma_b, s_b, i_b, r2_b = best
        print(f"\n  best σ in grid: {sigma_b:.0e}  (slope={s_b:+.4f}, intercept={i_b:+.4f}, R^2={r2_b:.4f})")
        # If slope is far from 1, we can back out an "implied effective σ":
        # if probit(poly) = slope * Z(sigma_used), then probit(poly) = 1 * Z(sigma_used / slope).
        # So effective σ = σ_used / slope.
        if s_b > 0:
            implied_sigma = sigma_b / s_b
            print(
                f"  implied effective σ (if model class is right): "
                f"σ_used / slope = {sigma_b:.0e} / {s_b:.3f} = {implied_sigma:.2e}/sec"
            )
            print(
                f"  → σ*sqrt(300) = {implied_sigma * math.sqrt(300) * 100:.3f}% per 5min"
            )

    # --- Test D. Per-market regression at the best σ -----------------------
    if best is not None:
        sigma_b = best[0]
        print("\n" + "=" * 78)
        print(f"D. Per-market probit(poly_yes) vs Z(σ={sigma_b:.0e})")
        print("=" * 78)
        by_market = collections.defaultdict(list)
        for r in base:
            z = z_score(
                r["binance_btc_mid_usd"], r["binance_strike_usd"], sigma_b, r["ttr_secs"]
            )
            if z is None:
                continue
            by_market[r["market_slug"]].append(
                (z, NORM.inv_cdf(r["poly_yes_mid"]))
            )
        print(f"  {'slug':<14}  {'slope':>9}  {'intercept':>11}  {'R^2':>7}  {'n':>5}")
        slopes = []
        intercepts = []
        r2s = []
        for slug in sorted(by_market):
            pairs = by_market[slug]
            if len(pairs) < 5:
                continue
            xs, ys = zip(*pairs)
            res = ols(list(xs), list(ys))
            if not res:
                continue
            s, i, r2 = res
            slopes.append(s)
            intercepts.append(i)
            r2s.append(r2)
            print(
                f"  {slug[-13:]:<14}  {s:>+9.4f}  {i:>+11.4f}  {r2:>7.4f}  {len(pairs):>5}"
            )
        if slopes:
            print()
            print(
                f"  slope     median={statistics.median(slopes):+.4f}, "
                f"mean={statistics.fmean(slopes):+.4f}, "
                f"stdev={statistics.pstdev(slopes):.4f}"
            )
            print(
                f"  intercept median={statistics.median(intercepts):+.4f}, "
                f"mean={statistics.fmean(intercepts):+.4f}, "
                f"stdev={statistics.pstdev(intercepts):.4f}"
            )
            print(
                f"  R^2       median={statistics.median(r2s):.4f}, "
                f"mean={statistics.fmean(r2s):.4f}, "
                f"min={min(r2s):.4f}, max={max(r2s):.4f}"
            )

    print("\n" + "=" * 78)
    print("INTERPRETATION GUIDE")
    print("=" * 78)
    print("""
  R^2 > 0.7 (aggregate or per-market): Polymarket DOES price approximately
    Phi(Z) under some σ. Bot's model class is right; just need to use the
    effective σ (recovered from the slope). Calibrate and try again.

  R^2 in 0.3-0.7: weak relationship. GBM explains some variance but a
    large component is something else (sentiment, order-book effects,
    Chainlink strike drift). Bot's framework is partially correct.

  R^2 < 0.3: GBM-around-strike is the wrong model class entirely. Bot's
    fair value is not picking up what Polymarket actually prices.
    Strategy needs to be rebuilt around a different signal class —
    probably treating Polymarket mid as the true probability and only
    firing on transient Binance/Polymarket disagreements.
""")


if __name__ == "__main__":
    main()
