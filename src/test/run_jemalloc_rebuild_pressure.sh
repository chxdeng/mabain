#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
BIN="$SCRIPT_DIR/jemalloc_restart_rebuild_test"
READER_COUNT=${1:-4}
mkdir -p /var/tmp/mabain_test
created_tmp_root=0
if [[ -z "${MABAIN_JEMALLOC_REBUILD_DIR:-}" ]]; then
    MABAIN_JEMALLOC_REBUILD_DIR=$(mktemp -d /var/tmp/mabain_test/jemalloc_rebuild.XXXXXX)
    export MABAIN_JEMALLOC_REBUILD_DIR
    created_tmp_root=1
fi
STOP_FILE="$MABAIN_JEMALLOC_REBUILD_DIR/full_cycle.stop"

if [[ ! -x "$BIN" ]]; then
    echo "missing test binary: $BIN" >&2
    exit 1
fi

reader_pids=()
overall_rc=0

print_reader_metrics() {
    shopt -s nullglob
    local files=("$MABAIN_JEMALLOC_REBUILD_DIR"/reader_metrics.*.txt)
    if [[ ${#files[@]} -eq 0 ]]; then
        echo "== reader metrics files =="
        echo "(none)"
        return
    fi

    echo "== reader metrics files =="
    cat "${files[@]}"
    echo "== reader metrics aggregate =="
    awk '
    {
        for (i = 1; i <= NF; ++i) {
            split($i, kv, "=")
            if (kv[1] == "overall_lookup_count") overall_count += kv[2]
            else if (kv[1] == "overall_total_ns") overall_ns += kv[2]
            else if (kv[1] == "rebuild_lookup_count") rebuild_count += kv[2]
            else if (kv[1] == "rebuild_total_ns") rebuild_ns += kv[2]
            else if (kv[1] == "fast_slot_guard_count") fast_slot_guard_count += kv[2]
            else if (kv[1] == "barrier_fallback_guard_count") barrier_fallback_guard_count += kv[2]
        }
    }
    END {
        overall_avg = (overall_count > 0) ? overall_ns / overall_count : 0
        rebuild_avg = (rebuild_count > 0) ? rebuild_ns / rebuild_count : 0
        printf "overall_lookup_count=%.0f overall_avg_ns=%.2f rebuild_lookup_count=%.0f rebuild_avg_ns=%.2f fast_slot_guard_count=%.0f barrier_fallback_guard_count=%.0f\n", overall_count, overall_avg, rebuild_count, rebuild_avg, fast_slot_guard_count, barrier_fallback_guard_count
    }' "${files[@]}"
}

cleanup() {
    : > "$STOP_FILE" 2>/dev/null || true
    for pid in "${reader_pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    if [[ $created_tmp_root -eq 1 ]]; then
        rm -rf "$MABAIN_JEMALLOC_REBUILD_DIR" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

echo "== full_cycle_prepare =="
"$BIN" full_cycle_prepare || exit 1

for i in $(seq 1 "$READER_COUNT"); do
    connect_id=$((0x7000 + i))
    echo "== reader_loop $i connect_id=$connect_id =="
    MABAIN_READER_CONNECT_ID=$connect_id "$BIN" reader_loop &
    reader_pids+=("$!")
done

sleep 1

echo "== full_cycle writer =="
if ! "$BIN" full_cycle; then
    overall_rc=1
fi

echo "== reader post-rebuild validation window =="
sleep 2

: > "$STOP_FILE"
for pid in "${reader_pids[@]}"; do
    if ! wait "$pid"; then
        overall_rc=1
    fi
done
reader_pids=()

print_reader_metrics

echo "== full_cycle insert verify =="
if ! "$BIN" full_cycle_insert_verify; then
    overall_rc=1
fi

echo "== full_cycle verify reuse =="
if ! "$BIN" full_cycle_verify_reuse; then
    overall_rc=1
fi

if [[ $overall_rc -eq 0 ]]; then
    echo "run_jemalloc_rebuild_pressure.sh: passed"
fi
exit $overall_rc
