#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="${SCRIPT_DIR}"
SRC_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Comma or space separated list can be overridden by user:
#   FS_CANDIDATES="minix,bfs,vfat" src/test/repro_errno95_real.sh
FS_CANDIDATES_DEFAULT="vfat msdos minix bfs ntfs"
FS_CANDIDATES_RAW="${FS_CANDIDATES:-${FS_CANDIDATES_DEFAULT}}"
IFS=', ' read -r -a FS_CANDIDATES_ARR <<< "${FS_CANDIDATES_RAW}"
# High-volume defaults. Can still be overridden via env vars.
INSERT_LOOKUP_COUNT="${INSERT_LOOKUP_COUNT:-1000000}"
MEMCAP_INDEX_MB="${MEMCAP_INDEX_MB:-256}"
MEMCAP_DATA_MB="${MEMCAP_DATA_MB:-256}"
IMAGE_SIZE_MB="${IMAGE_SIZE_MB:-2048}"
# Keep final mount/image/artifacts for inspection by default.
# Set KEEP_ARTIFACTS=0 for auto cleanup.
KEEP_ARTIFACTS="${KEEP_ARTIFACTS:-1}"

if ! command -v fallocate >/dev/null 2>&1; then
    echo "[repro-real-errno95] ERROR: fallocate command not found." >&2
    exit 1
fi

SUDO=""
if [[ "$(id -u)" -ne 0 ]]; then
    if ! command -v sudo >/dev/null 2>&1; then
        echo "[repro-real-errno95] ERROR: need root or sudo for mount/umount." >&2
        exit 1
    fi
    SUDO="sudo"
fi

IMG=""
MNT=""
DBDIR=""
PROBE=""
ERR_LOG=""
SELECTED_FS=""
LOG_ARCHIVE=""

cleanup_current_mount() {
    local force="${1:-0}"
    if [[ "${KEEP_ARTIFACTS}" == "1" && "${force}" != "1" && -n "${SELECTED_FS}" ]]; then
        echo "[repro-real-errno95] KEEP_ARTIFACTS=1: preserving mount and image for inspection."
        return
    fi

    set +e
    if [[ -n "${MNT}" ]] && mountpoint -q "${MNT}" 2>/dev/null; then
        ${SUDO} umount "${MNT}" >/dev/null 2>&1 || true
    fi
    if [[ -n "${MNT}" ]]; then
        rm -rf "${MNT}" >/dev/null 2>&1 || true
    fi
    if [[ -n "${IMG}" ]]; then
        rm -f "${IMG}" >/dev/null 2>&1 || true
    fi
    if [[ -n "${ERR_LOG}" ]]; then
        rm -f "${ERR_LOG}" >/dev/null 2>&1 || true
    fi
}

cleanup() {
    cleanup_current_mount
}
trap cleanup EXIT

mkfs_bin_for_fs() {
    local fs="$1"
    local bin="mkfs.${fs}"
    if command -v "${bin}" >/dev/null 2>&1; then
        command -v "${bin}"
        return 0
    fi
    return 1
}

try_fs() {
    local fs="$1"
    local mkfs_bin
    mkfs_bin="$(mkfs_bin_for_fs "${fs}")" || return 1

    IMG="/tmp/mabain_errno95_${fs}_$$.img"
    MNT="/tmp/mabain_errno95_mnt_${fs}_$$"
    DBDIR="${MNT}/mabain"
    PROBE="${MNT}/probe_fallocate"
    ERR_LOG="/tmp/mabain_errno95_fallocate_${fs}_$$.err"

    echo "[repro-real-errno95] Trying filesystem type: ${fs}"
    dd if=/dev/zero of="${IMG}" bs=1M count="${IMAGE_SIZE_MB}" status=none
    local -a mkfs_args=()
    if [[ "${fs}" == "minix" ]]; then
        mkfs_args=(-3)
    fi
    if ! "${mkfs_bin}" "${mkfs_args[@]}" "${IMG}" >/dev/null 2>&1; then
        echo "[repro-real-errno95]   mkfs failed for ${fs}, skipping."
        cleanup_current_mount
        return 1
    fi

    mkdir -p "${MNT}"
    local mount_opts="loop"
    case "${fs}" in
        vfat|msdos|ntfs|fat)
            mount_opts="${mount_opts},uid=$(id -u),gid=$(id -g),umask=022"
            ;;
    esac

    if ! ${SUDO} mount -t "${fs}" -o "${mount_opts}" "${IMG}" "${MNT}" >/dev/null 2>&1; then
        echo "[repro-real-errno95]   mount failed for ${fs}, skipping."
        cleanup_current_mount
        return 1
    fi

    # Some filesystems mount root-owned with restrictive mode; loosen for non-root runs.
    ${SUDO} chmod 0777 "${MNT}" >/dev/null 2>&1 || true
    mkdir -p "${DBDIR}" >/dev/null 2>&1 || true
    ${SUDO} mkdir -p "${DBDIR}" >/dev/null 2>&1 || true
    ${SUDO} chown "$(id -u):$(id -g)" "${MNT}" "${DBDIR}" >/dev/null 2>&1 || true
    ${SUDO} chmod 0777 "${DBDIR}" >/dev/null 2>&1 || true

    set +e
    LC_ALL=C fallocate -l 1048576 "${PROBE}" 2>"${ERR_LOG}"
    local rc=$?
    set -e
    if [[ ${rc} -ne 0 ]] && grep -Eq "Permission denied|Operation not permitted" "${ERR_LOG}"; then
        set +e
        LC_ALL=C ${SUDO} fallocate -l 1048576 "${PROBE}" 2>"${ERR_LOG}"
        rc=$?
        set -e
    fi

    if [[ ${rc} -ne 0 ]] && grep -Eq "Operation not supported|EOPNOTSUPP" "${ERR_LOG}"; then
        SELECTED_FS="${fs}"
        echo "[repro-real-errno95]   selected ${fs} (kernel returned EOPNOTSUPP)."
        return 0
    fi

    echo "[repro-real-errno95]   ${fs} does not reproduce EOPNOTSUPP on this host."
    cleanup_current_mount
    return 1
}

echo "[repro-real-errno95] Probing filesystems for real EOPNOTSUPP..."
for fs in "${FS_CANDIDATES_ARR[@]}"; do
    [[ -z "${fs}" ]] && continue
    if try_fs "${fs}"; then
        break
    fi
done

if [[ -z "${SELECTED_FS}" ]]; then
    echo "[repro-real-errno95] ERROR: none of the candidate filesystems produced EOPNOTSUPP." >&2
    echo "[repro-real-errno95] Tried: ${FS_CANDIDATES_RAW}" >&2
    echo "[repro-real-errno95] Tip: rerun with FS_CANDIDATES override, e.g. FS_CANDIDATES=\"minix,bfs,ntfs\"." >&2
    exit 1
fi

echo "[repro-real-errno95] Building Mabain library and test binary..."
make -C "${SRC_DIR}" libmabain.so >/dev/null
make -C "${TEST_DIR}" errno95_db_writer_test >/dev/null

echo "[repro-real-errno95] Config: entries=${INSERT_LOOKUP_COUNT}, memcap_index_mb=${MEMCAP_INDEX_MB}, memcap_data_mb=${MEMCAP_DATA_MB}, image_size_mb=${IMAGE_SIZE_MB}"
echo "[repro-real-errno95] Mabain db directory: ${DBDIR}"
echo "[repro-real-errno95] Running insert/lookup test on ${SELECTED_FS} mount..."
set +e
(
    cd "${TEST_DIR}"
    LD_LIBRARY_PATH=../ ./errno95_db_writer_test "${DBDIR}" "${INSERT_LOOKUP_COUNT}" "${MEMCAP_INDEX_MB}" "${MEMCAP_DATA_MB}"
)
writer_rc=$?
set -e
if [[ ${writer_rc} -ne 0 ]]; then
    echo "[repro-real-errno95] Writer failed as user; retrying with sudo..."
    ${SUDO} env LD_LIBRARY_PATH="${SRC_DIR}" "${TEST_DIR}/errno95_db_writer_test" "${DBDIR}" "${INSERT_LOOKUP_COUNT}" "${MEMCAP_INDEX_MB}" "${MEMCAP_DATA_MB}"
fi

LOG_FILE="${DBDIR}/mabain.log"
if [[ ! -f "${LOG_FILE}" ]]; then
    echo "[repro-real-errno95] ERROR: expected log file not found: ${LOG_FILE}" >&2
    exit 1
fi

if ! grep -E "fallocate failed for file .* errno=95" "${LOG_FILE}" >/dev/null; then
    echo "[repro-real-errno95] ERROR: Mabain log did not show errno=95." >&2
    echo "[repro-real-errno95] Log tail:" >&2
    tail -n 80 "${LOG_FILE}" >&2
    exit 1
fi

if ! grep -F "errno95_db_writer_test db_dir=${DBDIR}" "${LOG_FILE}" >/dev/null; then
    echo "[repro-real-errno95] ERROR: log does not contain db directory marker." >&2
    echo "[repro-real-errno95] Log tail:" >&2
    tail -n 80 "${LOG_FILE}" >&2
    exit 1
fi

LOG_ARCHIVE="/tmp/mabain_errno95_log_${SELECTED_FS}_$$.txt"
cp -f "${LOG_FILE}" "${LOG_ARCHIVE}"

echo "[repro-real-errno95] SUCCESS: real filesystem reproduction of errno=95 confirmed."
echo "[repro-real-errno95] Filesystem: ${SELECTED_FS}"
echo "[repro-real-errno95] Mabain db directory: ${DBDIR}"
echo "[repro-real-errno95] Insert/lookup count: ${INSERT_LOOKUP_COUNT}"
echo "[repro-real-errno95] Saved Mabain log copy: ${LOG_ARCHIVE}"
if [[ "${KEEP_ARTIFACTS}" == "1" ]]; then
    echo "[repro-real-errno95] Preserved mount path: ${MNT}"
    echo "[repro-real-errno95] Preserved image file: ${IMG}"
    if [[ -n "${SUDO}" ]]; then
        echo "[repro-real-errno95] Cleanup: ${SUDO} umount '${MNT}' && rm -rf '${MNT}' '${IMG}' '${ERR_LOG}'"
    else
        echo "[repro-real-errno95] Cleanup: umount '${MNT}' && rm -rf '${MNT}' '${IMG}' '${ERR_LOG}'"
    fi
fi
