# Mabain Test Runbook

This runbook covers every tracked test program and test driver under
`src/test`, plus the GoogleTest suite under `src/unittest`. Run the tests on a
Linux validation host from a clean Mabain checkout. Do not run tests that share
`/var/tmp/mabain_test` concurrently.

## 1. Safety and isolation

- Confirm that no production Mabain process uses `/var/tmp/mabain_test` or any
  other path selected below. Several tests delete or recreate their database.
- `shared_prefix_cache_concurrency_test` removes every entry under
  `/var/tmp/mabain_test` at startup, not only Mabain files.
- Run `sigbus_disk_pressure_test` only on a disposable host or an isolated
  `/tmp` filesystem. It intentionally fills `/tmp` to 100%.
- Run `repro_errno95_real.sh` only on a Linux validation host with root or
  passwordless sudo access and enough free space under `/tmp`. It creates a 2 GB
  filesystem image and loop-mounts it. Use `KEEP_ARTIFACTS=0` for normal
  validation so the mount, loop device, image, and probe error file are cleaned
  up on exit; the script default is to preserve them for debugging.
- The tracked `run_test` script is a historical soak launcher, not a complete or
  reliable all-tests driver. It omits newer tests, runs for hours, starts the
  final multi-process workload in the background, and does not wait for it.
  Use the explicit commands in this runbook instead.
- Backup validation is intentionally excluded because Mabain backup is not a
  supported feature. The backup row in `test_list` is already commented out.

## 2. Dependencies and build

Required build dependencies are a C++17 compiler, CMake, GNU Make, jemalloc
development files, OpenSSL development files, pthreads, GoogleTest, and the
normal Mabain dependencies. `gcovr` is needed only to generate coverage reports.

From the repository root:

```bash
cd ~/mabain
cmake -S . -B build -DMB_WERROR=ON
cmake --build build --parallel "$(nproc)"
make -C src/unittest clean build
make -C src/test clean all

MABAIN_ROOT="$(pwd)"
export LD_LIBRARY_PATH="$MABAIN_ROOT/build/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
mkdir -p /var/tmp/mabain_test

reset_mabain_test_db() {
  find /var/tmp/mabain_test -mindepth 1 -maxdepth 1 \
    \( -name '_mabain_*' -o -name '_success' -o -name 'mabain.log' \
       -o -name 'key_id' \) \
    -delete
}
```

The reset helper is used by later sections. Keep the same shell session or
define it again before running those sections.

The `src/unittest/Makefile` prefixes the test and coverage commands with `-`, so
`make unit-test` can return success after a failed test. Run the binary directly
and check its exit status as shown below.

## 3. GoogleTest unit suite

```bash
cd ~/mabain/src/unittest
LD_LIBRARY_PATH=../../build/lib ./test_mabain --gtest_brief=1
```

Pass criteria: exit status 0 and the final summary reports all tests passed.

## 4. Standalone correctness and concurrency tests

Run these sequentially from `src/test`:

```bash
cd ~/mabain/src/test

./errno95_db_writer_test
./mb_header_test
./shmq_reservation_timeout_test
./shmq_queue_full_stress_test
./prefix_cache_snapshot_concurrency_test 1000000 4
./shared_prefix_cache_concurrency_test 200000 4 95
./find_lower_bound_concurrency_test \
  /var/tmp/mabain_find_lower_bound_concurrency 24 200000 5000000
```

Pass criteria:

- Every command exits with status 0.
- `errno95_db_writer_test` reports successful insert/lookup verification. The
  separate privileged script in section 8 is what tests a real errno 95
  filesystem.
- `shmq_reservation_timeout_test` reports that the stale slot was reclaimed in
  its expected one-second test window.
- `shmq_queue_full_stress_test` reports at least one full-queue retry and verifies
  every queued request.
- `prefix_cache_snapshot_concurrency_test` reports nonzero hits and no torn or
  invalid stable snapshot.
- `shared_prefix_cache_concurrency_test` reports `Post-remove verification OK`
  and no value mismatch. The fourth argument is best omitted: the current test
  parses it both as cache capacity and as prefix depth.
- `find_lower_bound_concurrency_test` reports success for the writer and all
  reader processes. `TRY_AGAIN` is an accepted transient result.

The direct errno 95 test accepts optional arguments:

```text
./errno95_db_writer_test [db_dir] [entries] [index_memcap_mb] [data_memcap_mb]
```

The lower-bound concurrency test accepts:

```text
./find_lower_bound_concurrency_test [db_dir] [readers] [keys] [writer_operations]
```

The prefix-cache snapshot test accepts:

```text
./prefix_cache_snapshot_concurrency_test [writer_iterations] [reader_threads]
```

## 5. Jemalloc restart and rebuild tests

The first eight modes are independent. Give each one a unique directory:

```bash
cd ~/mabain/src/test

for mode in \
  header_metadata arena_cursor startup_gate async_reject \
  shrink_only evacuate_only recover_shrink recover_evacuate
do
  MABAIN_JEMALLOC_REBUILD_DIR="/var/tmp/mabain_test/jemalloc_$mode" \
    ./jemalloc_restart_rebuild_test "$mode" || exit 1
done
```

The remaining five modes form one ordered multi-process scenario:

```text
full_cycle_prepare -> reader_loop -> full_cycle ->
full_cycle_insert_verify -> full_cycle_verify_reuse
```

`run_jemalloc_rebuild_pressure.sh` is intended to orchestrate that scenario,
but the current script exports `MABAIN_READER_CONNECT_ID` while the test binary
reads `MABAIN_CONNECT_ID`. Until those names are made consistent, use this
manual form so every reader receives a distinct connection ID:

```bash
cd ~/mabain/src/test

MABAIN_JEMALLOC_REBUILD_DIR="$(mktemp -d /var/tmp/mabain_test/jemalloc_cycle.XXXXXX)"
export MABAIN_JEMALLOC_REBUILD_DIR

./jemalloc_restart_rebuild_test full_cycle_prepare

MABAIN_CONNECT_ID=28673 ./jemalloc_restart_rebuild_test reader_loop & reader1=$!
MABAIN_CONNECT_ID=28674 ./jemalloc_restart_rebuild_test reader_loop & reader2=$!
MABAIN_CONNECT_ID=28675 ./jemalloc_restart_rebuild_test reader_loop & reader3=$!
MABAIN_CONNECT_ID=28676 ./jemalloc_restart_rebuild_test reader_loop & reader4=$!

sleep 1
./jemalloc_restart_rebuild_test full_cycle
sleep 2
touch "$MABAIN_JEMALLOC_REBUILD_DIR/full_cycle.stop"

wait "$reader1" || exit 1
wait "$reader2" || exit 1
wait "$reader3" || exit 1
wait "$reader4" || exit 1

./jemalloc_restart_rebuild_test full_cycle_insert_verify
./jemalloc_restart_rebuild_test full_cycle_verify_reuse
```

Pass criteria: every mode exits with status 0, the writer and verification modes
print `passed`, and all four reader processes exit successfully.

## 6. Memory-management, lower-bound, and writer stress tests

These tests use the shared `/var/tmp/mabain_test` path and may be CPU-, memory-,
or time-intensive. Start with a clean dedicated directory and run sequentially.

```bash
cd ~/mabain/src/test

reset_mabain_test_db
./mb_mm_test -iter 50 -k int -n 500000
./mb_mm_test -iter 50 -k sha1 -n 500000

reset_mabain_test_db
./mb_mm_prune_test -iter 50 -k int -n 500000
./mb_mm_prune_test -iter 50 -k sha2 -n 500000

reset_mabain_test_db
./multi_writer_bug_test

reset_mabain_test_db
./mb_bound_test
```

Purpose and pass criteria:

- `mb_mm_test` repeatedly adds/removes random keys under jemalloc and asserts
  that pending data and index buffers return to zero.
- `mb_mm_prune_test` exercises offset-linked pruning and purge under jemalloc.
- `multi_writer_bug_test` starts 32 producer/reader threads against one async
  writer, then verifies overwrite behavior. It performs a large number of
  operations even though it has no arguments.
- `mb_bound_test` validates and benchmarks `FindLowerBound` against `std::map`
  for text integers, 4-byte binary integers, SHA-1 keys, and SHA-256 keys. It
  inserts one million candidates for each key type.

All four programs use assertions for correctness; an abort or nonzero exit is a
failure.

## 7. Legacy data-driven and soak tests

These are part of the tracked test inventory but are not short pre-commit tests.
They require the large `key_list_*` input files present in `src/test`. Only
`key_list_3` is tracked by Git, so verify the local fixture set first:

```bash
cd ~/mabain/src/test
ls -lh key_list_1 key_list_2 key_list_3 key_list_4 key_list_5 key_list_6
```

Run the jemalloc data-driven suite:

```bash
reset_mabain_test_db
./jemalloc_test /var/tmp/mabain_test/ ./jemalloc_test_list
```

Run the general data-driven suite. `test_list` includes very large memory caps
and workloads, so use a host with sufficient RAM, disk, and run time:

```bash
reset_mabain_test_db
./mb_test /var/tmp/mabain_test/ ./test_list
```

Both programs create `/var/tmp/mabain_test/_success` only after their complete
input list finishes. A nonzero exit, assertion, or missing success marker is a
failure.

Run the two fixed-duration resource-collection soaks with an explicitly chosen
duration in seconds. The historical durations are 1800 and 3600 seconds:

```bash
reset_mabain_test_db
./mb_test1 /var/tmp/mabain_test 1800

reset_mabain_test_db
./mb_test2 /var/tmp/mabain_test 3600
```

`mb_test1` persists its key range in `/var/tmp/mabain_test/key_id`; the reset
helper removes it for a fresh run. Each soak creates `_success` only after
normal completion.

Run the historical multi-process writer/producer scenario as one coordinated
group and wait for every process:

```bash
reset_mabain_test_db
./mb_test_mp -d /var/tmp/mabain_test -w -t 860 & master_writer=$!
sleep 1
./mb_test_mp -d /var/tmp/mabain_test -n 1800000 & producer1=$!
./mb_test_mp -d /var/tmp/mabain_test -n 1800000 -k sha1 & producer2=$!
./mb_test_mp -d /var/tmp/mabain_test -n 18000000 -k sha2 & producer3=$!

wait "$producer1" || exit 1
wait "$producer2" || exit 1
wait "$producer3" || exit 1
wait "$master_writer" || exit 1
```

`mb_test_mp` options are `-d <db_dir>`, `-w` for the master async writer,
`-n <count>`, `-n0 <starting_id>`, `-t <seconds>`, and
`-k <int|sha1|sha2>`.

## 8. Privileged and destructive filesystem tests

### Real errno 95 reproduction

Run only on a Linux validation host with root or passwordless sudo access. The
default test probes vfat, msdos, minix, bfs, and ntfs. For each available
filesystem it creates a 2 GB image under `/tmp`, formats and loop-mounts it, and
uses a 1 MB `fallocate` probe. Minix is formatted as Minix v3. The first mounted
filesystem whose probe returns real kernel `EOPNOTSUPP`/errno 95 is used for the
Mabain test. Which candidate is selected can vary with the host kernel and
filesystem tools; on the current validation host, vfat and msdos supported the
probe and Minix produced errno 95.

The recommended invocation automatically cleans up the mount, loop device,
image, and probe error file even after a normal test failure:

```bash
cd ~/mabain
KEEP_ARTIFACTS=0 src/test/repro_errno95_real.sh
```

Useful overrides are `FS_CANDIDATES`, `INSERT_LOOKUP_COUNT`,
`MEMCAP_INDEX_MB`, `MEMCAP_DATA_MB`, `IMAGE_SIZE_MB`, and `KEEP_ARTIFACTS`.

Pass criteria:

- The probe selects a filesystem after receiving real kernel errno 95.
- The Mabain log reports `errno=95, falling back to ftruncate` for its files.
- `errno95_db_writer_test` completes all configured inserts and verifies every
  lookup; the default is one million of each.
- The script exits with status 0 and reports `SUCCESS: real filesystem
  reproduction of errno=95 confirmed`.

A pass means errno 95 was reproduced and Mabain handled it correctly. It does
not mean the filesystem stopped returning errno 95. With `KEEP_ARTIFACTS=0`,
the script retains only its small Mabain log copy under
`/tmp/mabain_errno95_log_<fs>_<pid>.txt`.

After an interrupted run, verify that no test mount or loop device remains:

```bash
findmnt -rn -o SOURCE,TARGET,FSTYPE | grep mabain_errno95 || true
sudo losetup -a | grep mabain_errno95 || true
find /tmp -maxdepth 1 \
  \( -name 'mabain_errno95_*.img' -o -name 'mabain_errno95_mnt_*' \) -print
```

### SIGBUS disk-pressure test

Do not run this on a shared host. Its child process consumes all available
space on the filesystem backing `/tmp` and then the parent performs 100,000
database insertions:

```bash
cd ~/mabain/src/test
./sigbus_disk_pressure_test
```

Pass criteria for the hardened allocation path: exit status 0 and no SIGBUS.
The program exits 1 if it reproduces SIGBUS, even though its diagnostic text
calls that a successful reproduction.

## 9. Performance-only benchmark

`hashmap_lookup_bench` measures the internal HashMap and is not a correctness
gate beyond successful insertion and lookup hits:

```bash
cd ~/mabain/src/test
./hashmap_lookup_bench \
  1000000 5000000 /var/tmp/mabain_hashmap_bench 256 0.5 1
```

Arguments are:

```text
<entries> [lookups] [backing_path] [memory_mb] [load_factor] [compact64]
```

Record the hit percentage and average nanoseconds per lookup. Compare
performance only on an otherwise idle host using the same binary, fixture,
CPU affinity, and run count.

## 10. Final validation record

For a commit or pull request, record:

```bash
cd ~/mabain
git rev-parse HEAD
git status --short
git diff --check
```

Also record the build commands, each test command and exit status, the GoogleTest
pass count, any intentionally skipped privileged/destructive test, and the
reason for the skip. Do not treat benchmark throughput as a correctness result.
