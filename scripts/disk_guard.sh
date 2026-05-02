#!/usr/bin/env bash
# disk_guard.sh — out-of-process disk-pressure protection for polybot-recorder.
#
# Behaviour:
#   < WARN_GB free       → log a warning line
#   < ALERT_GB free      → log a louder alert line
#   < COMPRESS_GB free   → gzip the oldest non-live session whose newest
#                          file mtime is older than COMPRESS_AGE_HOURS
#   < STOP_GB free       → gracefully stop the recorder service
#
# Never:
#   - touches the live session
#   - deletes raw data
#   - operates on sessions whose newest file mtime is recent
#
# Modes:
#   apply (default)  — take action
#   --dry-run        — log what would be done; do nothing
#   --check          — print current free + classification, exit
#   --list-candidates — print sessions eligible for compression
#
# Exit codes:
#   0  ok / no action required
#   2  dry-run: would take destructive action (informational, not failure)
#   3  acted: gzip was performed
#   4  acted: recorder stopped
#   10 misuse / config error
#
# Idempotent. Safe to run from cron / systemd timer every 5 min.

set -euo pipefail

DATA_ROOT="${DATA_ROOT:-/home/polybot/polybot/data}"
STATE_DIR="${STATE_DIR:-/var/lib/polybot}"
STATE_FILE="${STATE_FILE:-$STATE_DIR/disk_guard.state}"
LOG_DIR="${LOG_DIR:-/var/log/polybot}"
LOG_FILE="${LOG_FILE:-$LOG_DIR/disk_guard.log}"
RECORDER_SERVICE="${RECORDER_SERVICE:-polybot-recorder.service}"

WARN_GB="${WARN_GB:-20}"
ALERT_GB="${ALERT_GB:-10}"
COMPRESS_GB="${COMPRESS_GB:-5}"
STOP_GB="${STOP_GB:-2}"

# A session is considered "live" if any file inside it was modified
# within this many seconds of now.
LIVE_THRESHOLD_SECS="${LIVE_THRESHOLD_SECS:-120}"
# A session is eligible for compression only if its newest file is at
# least this many hours old.
COMPRESS_AGE_HOURS="${COMPRESS_AGE_HOURS:-24}"

DRY_RUN=0
ACTION="apply"

# ---------- argument parsing ----------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)         DRY_RUN=1; shift ;;
        --check)           ACTION="check"; shift ;;
        --list-candidates) ACTION="list_candidates"; shift ;;
        --apply)           ACTION="apply"; shift ;;
        -h|--help)
            grep '^#' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "unknown arg: $1" >&2; exit 10 ;;
    esac
done

# ---------- setup ----------
mkdir -p "$STATE_DIR" "$LOG_DIR" 2>/dev/null || true

log() {
    local lvl="$1"; shift
    local msg="$*"
    local ts
    ts="$(date -u +'%Y-%m-%dT%H:%M:%S.%3NZ')"
    local line="$ts $lvl $msg"
    echo "$line"
    if [[ -w "$LOG_DIR" ]] || mkdir -p "$LOG_DIR" 2>/dev/null; then
        echo "$line" >> "$LOG_FILE" 2>/dev/null || true
    fi
}

# ---------- helpers ----------
disk_free_gb() {
    # Available bytes for non-root, divided by 1024^3, integer-truncated.
    df -B1 --output=avail "$DATA_ROOT" 2>/dev/null | tail -n 1 \
        | awk '{ printf "%d\n", $1 / (1024*1024*1024) }'
}

# Returns the full path to the live session, or empty string if none.
live_session_path() {
    local now_secs newest_secs newest_path
    now_secs=$(date +%s)
    # For each session_* dir, find newest file mtime; pick globally newest.
    while IFS= read -r d; do
        [[ -z "$d" ]] && continue
        local s
        s=$(find "$d" -type f -printf '%T@\n' 2>/dev/null | sort -nr | head -1)
        s=${s%.*}
        [[ -z "$s" ]] && continue
        if [[ -z "${newest_secs:-}" || "$s" -gt "$newest_secs" ]]; then
            newest_secs="$s"
            newest_path="$d"
        fi
    done < <(find "$DATA_ROOT" -maxdepth 1 -mindepth 1 -type d -name 'session_*' 2>/dev/null)
    if [[ -n "${newest_secs:-}" ]]; then
        local age=$(( now_secs - newest_secs ))
        if (( age <= LIVE_THRESHOLD_SECS )); then
            echo "$newest_path"
        fi
    fi
}

# Print "<mtime_epoch> <path>" lines for sessions eligible for compression.
# Newest first. Excludes the live session.
list_compressible_sessions() {
    local live="$1"
    local now_secs cutoff_secs
    now_secs=$(date +%s)
    cutoff_secs=$(( now_secs - COMPRESS_AGE_HOURS * 3600 ))
    while IFS= read -r d; do
        [[ -z "$d" ]] && continue
        [[ "$d" == "$live" ]] && continue
        # newest .ndjson file age — only consider .ndjson, not already-gzipped.
        local newest_secs
        newest_secs=$(find "$d" -type f -name '*.ndjson' -printf '%T@\n' 2>/dev/null \
            | sort -nr | head -1)
        newest_secs=${newest_secs%.*}
        [[ -z "$newest_secs" ]] && continue  # already fully gzipped or empty
        if (( newest_secs <= cutoff_secs )); then
            echo "$newest_secs $d"
        fi
    done < <(find "$DATA_ROOT" -maxdepth 1 -mindepth 1 -type d -name 'session_*' 2>/dev/null) \
        | sort -n
}

# Pick the OLDEST compressible session (lowest mtime).
oldest_compressible_session() {
    list_compressible_sessions "$1" | head -1 | awk '{print $2}'
}

gzip_session() {
    local sess="$1"
    [[ -z "$sess" ]] && return 1
    [[ ! -d "$sess" ]] && return 1
    local count=0
    # Gzip every .ndjson file. Skip _session_meta.json (it's diagnostic).
    while IFS= read -r f; do
        [[ -z "$f" ]] && continue
        if (( DRY_RUN == 1 )); then
            log INFO "DRY: would gzip $f"
        else
            gzip -9 -- "$f" 2>/dev/null && count=$((count + 1))
        fi
    done < <(find "$sess" -type f -name '*.ndjson' 2>/dev/null)
    log INFO "compressed_files=$count session=$sess"
}

stop_recorder() {
    if (( DRY_RUN == 1 )); then
        log WARN "DRY: would stop recorder service ($RECORDER_SERVICE)"
        return 0
    fi
    if command -v systemctl >/dev/null 2>&1; then
        systemctl stop "$RECORDER_SERVICE" 2>/dev/null \
            && log WARN "stopped recorder service" \
            || log ERROR "failed to stop recorder service"
    else
        log ERROR "systemctl not available; cannot stop recorder"
        return 1
    fi
}

read_state_level() {
    [[ -r "$STATE_FILE" ]] || { echo "ok"; return; }
    awk -F= '$1=="level"{print $2; exit}' "$STATE_FILE" 2>/dev/null || echo "ok"
}
write_state_level() {
    local lvl="$1"
    if (( DRY_RUN == 1 )); then
        return 0
    fi
    mkdir -p "$STATE_DIR" 2>/dev/null || true
    {
        echo "level=$lvl"
        echo "ts=$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
    } > "$STATE_FILE" 2>/dev/null || true
}

# ---------- main ----------
free_gb=$(disk_free_gb)
live="$(live_session_path || true)"

if [[ "$ACTION" == "check" ]]; then
    echo "free_gb=$free_gb"
    echo "live_session=${live:-<none-detected>}"
    if   (( free_gb < STOP_GB ));     then echo "level=stop"
    elif (( free_gb < COMPRESS_GB )); then echo "level=compress"
    elif (( free_gb < ALERT_GB ));    then echo "level=alert"
    elif (( free_gb < WARN_GB ));     then echo "level=warn"
    else                                   echo "level=ok"
    fi
    exit 0
fi

if [[ "$ACTION" == "list_candidates" ]]; then
    echo "live=${live:-<none-detected>}"
    echo "compressible_sessions (oldest first):"
    list_compressible_sessions "$live" | while read -r ts path; do
        local_iso=$(date -u -d "@$ts" +'%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || echo "$ts")
        size=$(du -sh "$path" 2>/dev/null | awk '{print $1}')
        echo "  $local_iso $size $path"
    done
    exit 0
fi

# ACTION=apply
prev_level=$(read_state_level)
new_level="ok"
exit_code=0

log INFO "free_gb=$free_gb prev_level=$prev_level live=${live:-<none>} dry_run=$DRY_RUN"

if (( free_gb < STOP_GB )); then
    new_level="stop"
    log ERROR "STOP threshold: free=${free_gb}GB < ${STOP_GB}GB; stopping recorder"
    stop_recorder
    exit_code=4
elif (( free_gb < COMPRESS_GB )); then
    new_level="compress"
    log WARN "COMPRESS threshold: free=${free_gb}GB < ${COMPRESS_GB}GB"
    target=$(oldest_compressible_session "$live")
    if [[ -n "$target" ]]; then
        log WARN "compressing $target"
        gzip_session "$target"
        if (( DRY_RUN == 1 )); then
            exit_code=2
        else
            exit_code=3
        fi
    else
        log WARN "no compressible session found (none older than ${COMPRESS_AGE_HOURS}h)"
    fi
elif (( free_gb < ALERT_GB )); then
    new_level="alert"
    log ERROR "ALERT: free=${free_gb}GB < ${ALERT_GB}GB"
elif (( free_gb < WARN_GB )); then
    new_level="warn"
    log WARN "WARN: free=${free_gb}GB < ${WARN_GB}GB"
fi

write_state_level "$new_level"
exit "$exit_code"
