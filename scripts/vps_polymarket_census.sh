#!/usr/bin/env bash
# Remote polymarket census — v3 (python3 -c form, no heredoc clobbering stdin).
#
# Local invocation (read-only on dormant .0000 buckets; recorder is at
# ~.0012 by now and not touching what we read here):
#
#   ssh root@159.69.0.78 'bash -s' \
#     < scripts/vps_polymarket_census.sh \
#     > docs/polymarket_census.txt
#
# History:
#   v1 (grep-based)       — wrong: payload quotes are JSON-escaped on disk.
#   v2 (python3 - <<'PY') — wrong: heredoc redirected python's stdin, so
#                           the head|python pipe was discarded.
#   v3 (python3 -c '...') — pipe stdin reaches python; correct.

set -u
SESSION=/home/polybot/polybot/data/session_20260426T122347Z
DIR=$SESSION/polymarket

echo "=== polymarket dir summary ==="
du -sh "$DIR"
ls -1 "$DIR" 2>/dev/null | wc -l | xargs echo "files:"
echo

echo "=== top 5 .0000.ndjson files by size ==="
ls -lS "$DIR"/*.0000.ndjson 2>/dev/null | head -5 | awk '{printf "%10s  %s\n", $5, $NF}'
echo

echo "=== event-type distribution + payload keys (first 1000 lines per file) ==="
for f in $(ls -S "$DIR"/*.0000.ndjson 2>/dev/null | head -5); do
  base=$(basename "$f")
  size=$(du -h "$f" | cut -f1)
  echo "-- $base ($size) --"
  head -1000 "$f" | python3 -c '
import sys, json
from collections import Counter
count = Counter()
keys_per_type = {}
for line in sys.stdin:
    try:
        rec = json.loads(line)
        payload = rec.get("payload")
        if not isinstance(payload, str):
            count["<no_payload>"] += 1; continue
        try:
            inner = json.loads(payload)
        except Exception:
            count["<payload_not_json>"] += 1; continue
        et = inner.get("event_type", "<missing>")
        count[et] += 1
        keys_per_type.setdefault(et, sorted(inner.keys()))
    except Exception:
        count["<line_parse_err>"] += 1
for et, n in count.most_common():
    print(f"   {et:<22} {n}")
for et, keys in keys_per_type.items():
    print(f"   keys[{et}]: {keys}")
'
done
echo

echo "=== resolution-sweeper sample (one file, first 100 lines) ==="
RESF=$(ls -S "$DIR"/*-resolved*.ndjson 2>/dev/null | head -1)
if [ -n "$RESF" ]; then
  echo "-- $(basename "$RESF") ($(du -h "$RESF" | cut -f1)) --"
  head -100 "$RESF" | python3 -c '
import sys, json
n = 0; keys = set()
for line in sys.stdin:
    try:
        rec = json.loads(line)
        p = rec.get("payload", "")
        if isinstance(p, str):
            inner = json.loads(p)
            keys |= set(inner.keys())
            n += 1
    except Exception:
        pass
print(f"   records parsed: {n}")
print(f"   union of payload keys: {sorted(keys)}")
'
fi
echo

echo "=== _unrouted / _unknown_* files ==="
ls -l "$DIR"/_*.ndjson 2>/dev/null | awk '{print $5, $NF}' || echo "(none)"
