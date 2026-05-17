#!/usr/bin/env python3
"""Momentum hypothesis test on the decision log.

Question: does poly_yes_mid at the OPEN of each BTC 5m market correlate
with Binance BTC's log-return over the 60 seconds BEFORE market open?

For each market with a captured strike:
  - t_open      = effective_start_epoch (seconds)
  - btc_open    = binance_strike_usd (this IS the BTC mid at t_open)
  - poly_yes_at_open = mean poly_yes_mid over the first OPEN_WINDOW_S
                       seconds of the market's records (averaging over
                       multiple poll ticks smooths spread/quote noise)
  - btc_60s_before = nearest BTC sample whose ts is in
                     [t_open - 90, t_open - 30]
  - log_return_60s = ln(btc_open / btc_60s_before)

Then regress poly_yes_at_open on log_return_60s. R^2 > 0.6 supports the
momentum hypothesis; R^2 < 0.3 rejects it; in between is ambiguous.

Stdlib only.
"""

import collections
import json
import math
import statistics
import sys


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
    r = sxy / math.sqrt(sxx * syy)
    return slope, intercept, r * r


def main():
    path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "data/bot_session_20260517T115549Z/decisions.ndjson"
    )
    rows = [json.loads(l) for l in open(path, "r", encoding="utf-8")]
    print(f"loaded {len(rows)} records from {path}")

    # Sort all rows by local_ts_ns.
    rows.sort(key=lambda r: int(r["local_ts_ns"]))

    # Group by market.
    by_market = collections.defaultdict(list)
    for r in rows:
        by_market[r["market_slug"]].append(r)

    # Order markets by effective_start_epoch.
    market_opens = sorted(
        (
            (recs[0]["effective_start_epoch"], slug, recs)
            for slug, recs in by_market.items()
            if recs[0].get("effective_start_epoch") is not None
        )
    )

    # All records with a BTC sample, in ts order.
    btc_rows = [r for r in rows if r.get("binance_btc_mid_usd") is not None]

    OPEN_WINDOW_S = 15  # mean poly_yes over first 15s of market
    BEFORE_LO_S = 30  # 60s-before-open lookup window: [t_open-90, t_open-30]
    BEFORE_HI_S = 90

    print(
        f"\nconfig: open_window={OPEN_WINDOW_S}s, "
        f"before_window=[{BEFORE_LO_S},{BEFORE_HI_S}]s before open"
    )
    print(f"markets with effective_start_epoch: {len(market_opens)}\n")

    data_points = []
    for est, slug, recs in market_opens:
        t_open_ns = est * 1_000_000_000

        # poly_yes at open: average poly_yes_mid over the first OPEN_WINDOW_S
        # seconds of this market's records that have a mid.
        open_window = [
            r
            for r in recs
            if r.get("poly_yes_mid") is not None
            and int(r["local_ts_ns"]) <= t_open_ns + OPEN_WINDOW_S * 1_000_000_000
        ]
        if not open_window:
            continue
        poly_yes_at_open = statistics.fmean(
            r["poly_yes_mid"] for r in open_window
        )
        # Also record min/max within that early window so we can see book volatility.
        py_min = min(r["poly_yes_mid"] for r in open_window)
        py_max = max(r["poly_yes_mid"] for r in open_window)

        # Use strike as the canonical BTC-at-open. (This is what the bot
        # captured from BtcHistory.at_time(t_open) — i.e. the nearest sample.)
        btc_open = next(
            (
                r.get("binance_strike_usd")
                for r in recs
                if r.get("binance_strike_usd") is not None
            ),
            None,
        )

        # BTC 60s before open: nearest sample with ts in the window.
        before_pool = [
            r
            for r in btc_rows
            if (t_open_ns - BEFORE_HI_S * 1_000_000_000)
            <= int(r["local_ts_ns"])
            <= (t_open_ns - BEFORE_LO_S * 1_000_000_000)
        ]
        if not before_pool or btc_open is None:
            data_points.append(
                {
                    "slug": slug,
                    "t_open_epoch": est,
                    "btc_open": btc_open,
                    "btc_before": None,
                    "log_return_60s": None,
                    "actual_gap_s": None,
                    "poly_yes_at_open": poly_yes_at_open,
                    "py_min": py_min,
                    "py_max": py_max,
                    "n_recs": len(recs),
                    "usable": False,
                    "reject_reason": "no BTC sample in [-90,-30]s before open",
                }
            )
            continue
        # Pick the one closest to t_open - 60s.
        target = t_open_ns - 60 * 1_000_000_000
        before = min(before_pool, key=lambda r: abs(int(r["local_ts_ns"]) - target))
        btc_before = before["binance_btc_mid_usd"]
        actual_gap_s = (t_open_ns - int(before["local_ts_ns"])) / 1.0e9
        log_ret = math.log(btc_open / btc_before)

        data_points.append(
            {
                "slug": slug,
                "t_open_epoch": est,
                "btc_open": btc_open,
                "btc_before": btc_before,
                "log_return_60s": log_ret,
                "actual_gap_s": actual_gap_s,
                "poly_yes_at_open": poly_yes_at_open,
                "py_min": py_min,
                "py_max": py_max,
                "n_recs": len(recs),
                "usable": True,
                "reject_reason": None,
            }
        )

    print(
        f"{'slug':<14}  {'t_open':<10}  {'btc_open':>9}  {'btc_-60s':>9}  "
        f"{'ret_bps':>8}  {'gap_s':>6}  {'poly_yes':>10}  {'[min,max]':>15}  "
        f"{'usable':>6}"
    )
    for d in data_points:
        if d["usable"]:
            print(
                f"{d['slug'][-13:]:<14}  "
                f"{d['t_open_epoch']:<10}  "
                f"{d['btc_open']:>9.2f}  "
                f"{d['btc_before']:>9.2f}  "
                f"{d['log_return_60s']*10000:>+8.2f}  "
                f"{d['actual_gap_s']:>6.1f}  "
                f"{d['poly_yes_at_open']:>10.4f}  "
                f"[{d['py_min']:.3f},{d['py_max']:.3f}]   "
                f"{'YES':>6}"
            )
        else:
            print(
                f"{d['slug'][-13:]:<14}  "
                f"{d['t_open_epoch']:<10}  "
                f"{(d['btc_open'] if d['btc_open'] else 0):>9.2f}  "
                f"{'n/a':>9}  "
                f"{'n/a':>8}  "
                f"{'n/a':>6}  "
                f"{d['poly_yes_at_open']:>10.4f}  "
                f"[{d['py_min']:.3f},{d['py_max']:.3f}]   "
                f"{'no':>6}  ({d['reject_reason']})"
            )

    usable = [d for d in data_points if d["usable"]]
    print(f"\nusable points: {len(usable)} / {len(data_points)}")

    if len(usable) >= 2:
        xs = [d["log_return_60s"] for d in usable]
        ys = [d["poly_yes_at_open"] for d in usable]
        res = ols(xs, ys)
        if res:
            slope, intercept, r2 = res
            print(f"\nOLS  poly_yes_at_open = {intercept:+.4f} + {slope:+.2f} * log_return_60s")
            print(f"     R^2 = {r2:.3f}  (n={len(usable)})")
            slope_per_bp = slope / 10_000
            print(
                f"     each +1bp of pre-open BTC return shifts poly_yes_at_open by "
                f"{slope_per_bp:+.4f} prob units ({slope_per_bp*100:+.2f}pp)"
            )

        # Sign agreement.
        agree = sum(
            1
            for d in usable
            if (d["poly_yes_at_open"] > 0.5) == (d["log_return_60s"] > 0)
        )
        print(
            f"\n  sign-agreement (poly_yes>0.5 vs ret>0): "
            f"agree={agree}/{len(usable)}"
        )

        # Quick verdict heuristic.
        if res and r2 > 0.6:
            print("\n  VERDICT: STRONG support for momentum hypothesis.")
            print("    Polymarket appears to price recent BTC direction at open, not GBM.")
            print("    Bot's option-theoretic FV is the wrong model class for these markets.")
        elif res and r2 > 0.3:
            print("\n  VERDICT: WEAK/MIXED support for momentum hypothesis.")
            print("    Momentum explains some variance but other factors dominate. Worth")
            print("    a deeper case-study and a larger sample.")
        elif res:
            print("\n  VERDICT: NO support for momentum-at-open hypothesis.")
            print("    Polymarket's open price isn't a function of the pre-open 60s return.")
            print("    Look elsewhere — Chainlink strike timing, liquidity, or order-book")
            print("    structure on the YES vs NO sides.")
    else:
        print("\n  Not enough usable points to fit a regression. Need a longer run or")
        print("  a denser BTC sample stream around market opens.")


if __name__ == "__main__":
    main()
