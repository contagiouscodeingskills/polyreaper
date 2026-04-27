#!/usr/bin/env bash
# Inner-payload drill — samples bids[0] / asks[0] from `book` events and
# price_changes[0..1] from `price_change` events, so we can nail down the
# decoder field shape before touching
# crates/replayer/src/decode/polymarket.rs.
#
# Local invocation (read-only on dormant .0000 buckets; recorder is at
# ~.0012 by now and is not reading from these):
#
#   ssh root@159.69.0.78 'bash -s' \
#     < scripts/vps_polymarket_inner_keys.sh \
#     > docs/polymarket_inner_keys.txt

set -u
SESSION=/home/polybot/polybot/data/session_20260426T122347Z
DIR=$SESSION/polymarket

for f in $(ls -S "$DIR"/*.0000.ndjson 2>/dev/null | head -2); do
  base=$(basename "$f")
  echo "=== $base ==="
  head -2000 "$f" | python3 -c '
import sys, json

samples = {"book": [], "price_change": []}
for line in sys.stdin:
    if all(len(v) >= 3 for v in samples.values()):
        break
    try:
        rec = json.loads(line)
        p = rec.get("payload")
        if not isinstance(p, str):
            continue
        inner = json.loads(p)
        et = inner.get("event_type")
        if et in samples and len(samples[et]) < 3:
            samples[et].append(inner)
    except Exception:
        continue

print("-- book bids[0] / asks[0] shape --")
for i, b in enumerate(samples["book"]):
    bids = b.get("bids", [])
    asks = b.get("asks", [])
    print(f"  sample {i}: bids={len(bids)} asks={len(asks)}")
    if bids:
        first = bids[0]
        print(f"    bids[0] keys+types: {[(k, type(v).__name__) for k,v in sorted(first.items())]}")
    if asks:
        first = asks[0]
        print(f"    asks[0] keys+types: {[(k, type(v).__name__) for k,v in sorted(first.items())]}")

print("-- price_change top-level + price_changes[0..1] shape --")
sides_seen = set()
for i, p in enumerate(samples["price_change"]):
    print(f"  sample {i}: top keys: {sorted(p.keys())}")
    pcs = p.get("price_changes", [])
    print(f"    price_changes count: {len(pcs)}")
    for j, item in enumerate(pcs[:2]):
        print(f"    price_changes[{j}] keys+types: {[(k, type(v).__name__) for k,v in sorted(item.items())]}")
        side_val = item.get("side", "<missing>")
        sides_seen.add(side_val)
        print(f"    price_changes[{j}].side value: {side_val!r}")
print(f"  distinct side values seen: {sorted(sides_seen)}")
'
done
