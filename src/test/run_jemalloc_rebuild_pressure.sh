#!/bin/bash
set -u

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
BIN="$SCRIPT_DIR/jemalloc_restart_rebuild_test"
STOP_FILE="/var/tmp/mabain_test/jemalloc_rebuild/full_cycle.stop"
READER_COUNT=${1:-4}

if [[ ! -x "$BIN" ]]; then
    echo "missing test binary: $BIN" >&2
    exit 1
fi

reader_pids=()
overall_rc=0

cleanup() {
    : > "$STOP_FILE" 2>/dev/null || true
    for pid in "${reader_pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
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

echo "== full_cycle verify reuse =="
if ! "$BIN" full_cycle_verify_reuse; then
    overall_rc=1
fi

if [[ $overall_rc -eq 0 ]]; then
    echo "run_jemalloc_rebuild_pressure.sh: passed"
fi
exit $overall_rc
