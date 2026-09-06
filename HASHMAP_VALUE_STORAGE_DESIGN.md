# HashMap-Owned Value Storage Design

- Status: Draft for review
- Scope: `mabain::HashMap` only
- Compatibility target: one writer process, multiple reader processes and threads

## 1. Decision summary

Extend `HashMap` with an explicit value-storage mode in which it owns binary,
variable-length keys and values. The fixed index remains an open-addressed
table. Each occupied bucket holds a 64-bit hash and a shared-memory offset to
one immutable key/value record allocated from a separate jemalloc-backed
`RollableFile`.

The existing reference-only mode and its public methods remain available and
unchanged. Value mode adds new methods rather than changing the meaning of the
existing `size_t ref_offset` API.

Removed and replaced records are not freed immediately. Each reader thread
claims one persistent shared epoch slot on first use. A lookup publishes and
clears only that slot's active value generation and epoch while copying a
record; it does not claim and release slot ownership for every lookup. The
writer retires old offsets and returns them to jemalloc only after all readers
that could have observed those offsets have quiesced.

The first value-storage implementation deliberately retains the existing
16-byte compact bucket. Index-table redesign, online resizing, and Robin Hood
probing are deferred until value storage and reclamation are validated.

## 2. Goals

- Store binary keys and binary values with variable lengths.
- Guarantee exact key matching, including two different keys with the same
  64-bit hash.
- Support full process-level writer/reader concurrency: exactly one writer
  process may update the HashMap while multiple reader processes perform
  lookups concurrently.
- Return only a complete old value, a complete new value, or not found during
  concurrent update/removal; never return torn or unrelated bytes.
- Reuse Mabain's existing jemalloc-backed `RollableFile` allocation model.
- Keep lookup synchronization lock-free. Growing a caller-owned output buffer
  may allocate, but no lookup or lookup slow path may take a process-local
  mutex or file lock.
- Reclaim removed and replaced records without use-after-free.
- Keep the existing reference-only HashMap API source-compatible.
- Keep the radix-tree DB implementation and `db.h` unchanged.
- Treat HashMap files as version-specific caches: incompatible layouts are
  rejected and must be recreated; in-place upgrade is not supported.

## 3. Non-goals for the first implementation

- Multiple concurrent writers.
- Zero-copy values whose pointers remain valid after `GetValue()` returns.
- Online table resizing or rehashing.
- Robin Hood or backward-shift deletion.
- Persistent recovery of cache contents after the writer restarts.
- Encryption, authentication, or integrity protection for stored values.
- Values larger than Mabain's existing `CONSTS::MAX_DATA_SIZE` limit.

## 4. Current behavior

The current HashMap is an index, not a key/value store:

```text
input key -> hash/probe -> fixed-size caller-provided ref_offset
```

Compact buckets contain a 64-bit hash and a `size_t` reference. Full buckets
also contain the key length and up to 24 leading key bytes. Neither format owns
the complete key or value.

Index collisions between different hashes are handled by linear probing. In
compact mode, a full 64-bit hash collision is treated as key equality. Full
mode adds partial collision screening but still cannot compare keys longer than
its inline prefix exactly.

## 5. Proposed storage layout

Use two independent `RollableFile` families:

```text
<base>_hashmap0                 fixed header and bucket table
<base>_hashmap_values_g7_0      generation 7 jemalloc metadata/records
<base>_hashmap_values_g7_1      additional generation 7 value block
...
```

Keeping the index and value allocator separate prevents jemalloc extent
metadata and variable allocations from fragmenting the contiguous bucket
table.

### 5.1 Index header

The versioned HashMap header gains value-mode configuration and reader epoch
state:

- layout version and mode (`REFERENCE` or `VALUE`)
- bucket capacity and mask
- production hash-algorithm identifier
- value block size, maximum blocks, and mapped-memory cap
- atomic current value-file generation
- atomic global map generation used during writer reset
- atomic global reader epoch
- fixed array of reader epoch slots
- atomic live and retired value-byte statistics

Value-file generation and reader epoch start at one; zero is reserved for an
inactive/uninitialized slot or view.

All readers must open the map using configuration compatible with the stored
header. Mismatch returns an error rather than remapping or resizing a live map.
The writer initializes every immutable field before release-publishing the
header control/version word; readers acquire-load that word before validating
or using the remaining header.

Each cache-line-aligned reader slot contains a nonzero owner ID, process ID,
process start time, active epoch, and active value-file generation. All fields
used concurrently are atomic. An inactive owned slot has both active fields set
to zero. The process identity and start time are cached when the reader opens
the HashMap; they are not queried on every lookup.

A thread claims a slot with compare/exchange only on its first lookup for that
HashMap and keeps the slot index in thread-local state. Normal thread teardown
first clears the active fields and then releases ownership. Process death is
handled by the stale-owner protocol in Section 8.1. A default of at least 64
slots supports the stated 32-reader-thread deployment while leaving headroom.
Slot count is configurable at creation and immutable afterward. If no slot is
available on first use, the lookup returns a resource/retry error without
reading a value record.

As with the existing C++ object lifetime contract, callers must stop/join local
operations before destroying a `HashMap`. Destruction invalidates the
process-local reader connection, clears all slots owned by that connection, and
then unmaps generation views. Thread-local slot caches carry a connection
generation and never dereference a destroyed/reused `HashMap` address.

### 5.2 Bucket

Value mode initially uses the existing compact-sized bucket:

```cpp
struct ValueBucket {
    std::atomic<uint64_t> hash;          // 0 empty, 1 tombstone, >=2 occupied
    std::atomic<size_t> record_offset;   // offset in the values RollableFile
};
```

Size remains 16 bytes on the supported 64-bit platform. Four buckets fit in a
64-byte cache line.

The hash is the publication field. A writer completely initializes a record,
stores its offset, and publishes the occupied hash with release ordering.
Readers acquire-load the hash before using the offset.

### 5.3 Immutable key/value record

Each live entry uses one jemalloc allocation:

```cpp
struct ValueRecordHeader {
    uint32_t value_length;
    uint16_t key_length;
    uint16_t flags;
};

// Followed by:
// uint8_t key[key_length];
// uint8_t value[value_length];
```

The allocation size is the checked sum of header, key, and value lengths,
rounded according to jemalloc's size class. Storing key and value together
requires one offset, one allocation, and one reclamation operation.

Records are immutable after publication. Updating a value allocates a new
record and atomically replaces the bucket offset. Immutability avoids data
races while readers copy bytes.

The reader validates every record before accessing its payload:

- offset is inside the configured value-file range
- header fits within its mapped block
- key and value lengths are within configured limits
- flags contain only defined bits
- checked total size does not overflow and stays within the mapped block
- complete stored key equals the lookup key

The complete-key comparison makes linear probing correct even when two keys
have the same 64-bit hash.

## 6. Public API

The HashMap API is declared in the separately installed `hash_map_api.h`
header. It is not added to or included by `db.h`; existing radix-tree clients
therefore do not acquire a HashMap dependency.

Preserve the existing constructor and reference-only methods:

```cpp
int Put(const uint8_t* key, int key_len, size_t ref_offset,
    bool overwrite = true);
bool Get(const uint8_t* key, int key_len, size_t& ref_offset) const;
int Erase(const uint8_t* key, int key_len);
```

Add an explicit value-mode constructor/configuration and methods such as:

```cpp
int PutValue(const uint8_t* key, int key_len,
    const uint8_t* value, int value_len, bool overwrite = true);
int GetValue(const uint8_t* key, int key_len, MBData& value) const;
```

`GetValue()` sets `value.data_len` to zero on entry and keeps it zero while it
copies into caller-owned `MBData` storage. It publishes the copied length only
after the final map/value-generation checks succeed. Every retry and non-success
return leaves `value.data_len == 0`, so bytes copied by a rejected attempt are
not exposed as a valid result. The reader epoch remains active through the copy
and final validation. `GetValue()` never exposes a mapped pointer whose lifetime
extends beyond the call.

If the existing `MBData` buffer is too small, `GetValue()` may call
`MBData::Resize()` while the record is protected. Allocation failure returns
`MBError::NO_MEMORY` with `data_len == 0`. Callers should reuse a suitably sized
`MBData` when lookup latency is important.

A single map file cannot mix reference entries and value entries. Calling a
method incompatible with the map's stored mode returns `MBError::NOT_ALLOWED`.
`Erase()` works in both modes; in value mode it also retires the old record.

Input limits initially match the existing Mabain DB contract:

- `0 < key_len <= CONSTS::MAX_KEY_LENGHTH` (currently 256)
- `0 < value_len <= CONSTS::MAX_DATA_SIZE` (currently 32767)

Supporting empty values can be added later if a concrete client requires it.

## 7. Concurrency and publication protocol

The supported topology is one active writer process and multiple concurrent
reader processes. Writer mutations and reader lookups may overlap for their
entire execution. Correctness must not depend on pausing, restarting, reopening,
or globally locking readers while the writer inserts, replaces, or removes a
record. A process may also have multiple concurrent lookup threads. The
single-writer restriction applies only to mutation ownership; it does not
reduce writer/reader concurrency.

A file-backed value-mode writer acquires an exclusive writer-lifetime lock on a
dedicated HashMap lock file before creating, resetting, allocating, or mutating
shared state. A second writer open fails without changing the map. Reader
processes neither open nor acquire this lock, so it adds no reader file
descriptor or lookup operation. The operating system releases the writer lock
if the writer process dies.

Value mode follows a single mutation-thread contract inside the one writer
process. It does not add a process-local mutex or a disguised spin lock.
Multiple threads in the writer process may perform lookups concurrently with
that mutation thread. Supporting concurrent mutation calls from multiple
threads requires a separate lock-free multi-writer probing/publication design
and is not part of this one-writer implementation.

### 7.1 Reader contract

Each `GetValue()` performs the following steps:

1. Set `value.data_len` to zero. On first use by this thread, claim a persistent
   reader slot and cache its index in thread-local state.
2. Acquire-load an even map generation, the atomic current value-file
   generation, the process-local stable-lifetime view for that generation, and
   the sequentially consistent global reader epoch. Clear and retry if reset is
   in progress.
3. Release-store the captured value generation to
   `active_value_generation`, followed by a sequentially consistent store of
   the captured epoch to `active_epoch`. The epoch store is the single
   steady-state publication barrier for both fields.
4. Before dereferencing the view or a bucket, revalidate the global epoch, the
   shared map/value generations, and the identity of the local current view.
   Clear and retry if any captured value changed.
5. Hash the input key and linearly probe buckets.
6. For a matching bucket hash, acquire-load its record offset.
7. Resolve and validate the immutable record.
8. Compare the complete key and copy the complete value, keeping
   `value.data_len` equal to zero.
9. Recheck the map generation and value-file generation before accepting either
   success or not-found. A changed/odd generation causes a retry.
10. On success, publish the copied value length. On every exit, release-store
    zero to `active_epoch`, then relaxed-store zero to
    `active_value_generation`. A scanner may conservatively retain an
    entry/view after a stale read, but it must never reclaim one early.

Concurrent replacement may return either the complete old value or the
complete new value. Concurrent removal may return the old value or not found.
Both outcomes are linearizable. Returning unrelated or partially updated bytes
is forbidden.

The slot-claim operation is a first-use/thread-teardown slow path. After the
slot, current generation view, and target block mapping are established, a
steady-state lookup has no slot compare/exchange, process-identity query, file
open, HashMap mutex, or file lock. Each active thread owns a different
cache-line-aligned slot, avoiding reader-to-reader false sharing. Exhausting
bounded lookup retries returns `TRY_AGAIN`, not `NOT_EXIST`.

For the first implementation, the epoch and local-view hazard handshake uses
sequentially consistent operations for `active_epoch` publication, global epoch
load/advance, scanner loads of `active_epoch`, and current-view pointer
publication/revalidation. After a scanner observes a nonzero `active_epoch`, it
acquire-loads `active_value_generation`. Bucket publication retains the
release/acquire ordering described in Section 5.2. Weaker handshake ordering is
permitted only after a separate memory-model proof and performance validation;
release/acquire operations on unrelated atomics must not be assumed to provide
the required ordering.

### 7.2 Writer insert

1. Probe for an existing exact key or insertion position.
2. Allocate and fully populate the immutable record.
3. Store `record_offset` with relaxed ordering.
4. Publish the normalized hash with release ordering.
5. Update writer-only statistics.

If allocation fails, the table remains unchanged. If table insertion fails,
the unpublished allocation can be freed immediately because no reader can have
observed it.

### 7.3 Writer overwrite

1. Probe by hash and compare the complete key in each candidate record.
2. Validate the old record size. If adding it would cross the reclamation
   threshold, run a batched reclamation pass.
3. Before changing the bucket, reserve both retired-byte budget and a retired
   queue entry. If the hard retired-memory limit would still be exceeded, return
   a resource/retry error and leave the existing entry unchanged.
4. Allocate and populate a replacement record.
5. Sequentially consistently load the current global epoch as the old record's
   retirement epoch; do not advance it on each mutation.
6. Release-store the new record offset in the existing bucket.
7. Commit the current value generation, old offset, size, and captured
   retirement epoch to the pre-reserved retired queue entry.

Only the writer modifies bucket offsets. Readers see either complete immutable
record. Queue storage and retired-byte capacity are reserved before publication
so an allocation failure cannot strand an untracked retired record during
normal execution. A writer crash after publication may leak the old allocation,
but the next writer discards that complete value-file generation.

### 7.4 Writer erase

1. Probe by hash and complete key.
2. Validate the old record size, run reclamation if required, and reserve its
   retired-byte budget and queue entry. If the hard retired-memory limit cannot
   accommodate it, return a resource/retry error with the entry unchanged.
3. Sequentially consistently load the current global epoch as the retirement
   epoch, without advancing it.
4. Publish the tombstone through the bucket hash using release ordering.
5. Commit the current value generation and old offset to the pre-reserved
   retirement entry using the captured epoch.
6. Update used/tombstone statistics.

Tombstones preserve linear-probe chains. The normal `Erase()` path only enqueues
the record and remains expected O(1). A reclamation pass may scan reader slots
when a configured batch threshold is crossed, but `Erase()` never waits for a
reader to exit.

## 8. Safe jemalloc reclamation

Calling `RollableFile::Free(offset)` immediately after overwrite or erase is
unsafe: a reader may already have copied that offset from the bucket. A
reference count alone also does not close the race between loading an offset
and incrementing the record's count.

Use epoch-based deferred reclamation, following Mabain's existing reader-epoch
pattern:

```text
reader: publish {value generation, epoch} -> revalidate -> copy -> clear
writer: publish new bucket state -> retire old record at current epoch
reclaimer: advance epoch -> scan -> free only after the old epoch is quiescent
```

The ordering requirement is important: the writer publishes bucket changes and
queues their old records before a reclamation pass advances the epoch. A reader
publishes both captured fields and then revalidates them before dereferencing a
record. A reader that observes the newer epoch must also be able to observe all
bucket publications eligible for that pass. A reader that entered on the older
epoch prevents reclamation until it exits.

Reclamation is batched by the writer after retired records or bytes cross a
configurable threshold. A separate hard retired-memory limit bounds memory when
a reader is stalled. Before an overwrite or erase publishes a new bucket state,
the writer must reserve the corresponding retired bytes and queue entry. If a
reclamation pass cannot make room below the hard limit, the mutation fails
without changing the entry. Every retired entry records its value-file
generation and the current epoch captured by its mutation. A reclamation pass,
serialized with mutations, sequentially consistently advances the global epoch
once and then scans reader slots. Only entries whose retirement epoch precedes
the newly published global epoch are eligible. It frees an eligible entry only
when no slot has a nonzero sequentially consistently loaded `active_epoch` at
or before the entry's retirement epoch and, after that observation, an
acquire-loaded `active_value_generation` matching the entry. Entries retired at
the newly advanced epoch wait for a later pass. A paused reader from an
abandoned generation therefore cannot block reclamation in the current
generation. Reclamation is not performed by reader processes.

The retired queue may remain writer-process-local because opening a replacement
writer resets this cache rather than preserving its contents. If the writer
dies after retiring a record but before freeing it, the record leaks only until
the next writer reset; it cannot be returned incorrectly.

### 8.1 Reader death and stalled readers

Epoch slots include process identity and process start time, matching Mabain's
existing protection against PID reuse. Slot ownership is persistent, but only a
nonzero `active_epoch` protects records. Lookup return and exception paths clear
the active fields through the RAII guard; normal thread teardown additionally
releases slot ownership. An idle owned slot with `active_epoch == 0` does not
delay reclamation.

The writer may release a slot left by a dead reader process only after
establishing that the recorded process instance no longer exists and atomically
revalidating the owner ID, PID, start time, active epoch, and active value-file
generation. It first compare/exchanges the captured owner ID to a reserved
cleanup marker, revalidates the remaining captured fields, clears the metadata,
and release-stores owner ID zero last. A new reader therefore cannot claim a
partially cleared slot. Process start time is necessary because a PID alone may
have been reused.

If a thread is indefinitely suspended during a lookup while its process remains
alive, its record remains protected and reclamation can stall. The writer must
never bypass that protection. If the retired-memory limit is reached, affected
overwrite/erase operations return a resource error or retry status without
changing the map; reads continue safely.

Writer restart does not reclaim a slot owned by a still-live paused process.
Such a slot no longer blocks new-generation record reclamation, but it remains
unavailable to new reader threads. If every configured slot is occupied, a new
thread returns `TRY_AGAIN`; this is why slot count is an immutable deployment
capacity with explicit headroom.

This failure mode sacrifices write availability rather than reader correctness.

### 8.2 Writer restart

Writer startup must not reset or reuse a jemalloc arena while a reader can
still use one of its records. It also must not wait indefinitely for a live but
paused reader. Use a new value-file generation on every writer restart:

1. While holding the writer-lifetime lock, select a monotonically increasing
   generation greater than every stored or orphan generation. Create its files
   with exclusive-create semantics before changing the live header.
2. Release-store an odd atomic map generation to prevent any new lookup from
   returning a result during reset. If a prior writer left the generation odd,
   the replacement writer keeps the map unavailable and performs a complete
   fresh reset.
3. Reset the bucket table with atomic stores and release-store the new atomic
   value-file generation. Do not clear live reader slots during cutover.
4. Release-store the next even map generation only after the table and value
   generation are ready.
5. Unlink older generation file names after cutover. Cleanup matches only the
   exact HashMap value-generation naming pattern.

The global reader epoch remains monotonic for the lifetime of the index header
and is not reset during writer restart. Approaching 64-bit generation or epoch
wrap requires cache recreation rather than wrapping a live synchronization
domain.

Each reader process maintains stable-lifetime process-local
`ValueGenerationView` objects. A view's identity and configuration are
immutable; its fixed-size atomic block-view table is populated only from null to
a validated mapping. A view owns the file descriptors and mappings for exactly
one value generation, and its table is sized from the immutable
`max_value_blocks` setting. A steady-state lookup sequentially
consistently loads the current view pointer, publishes the already captured
shared value generation in its persistent reader slot, and then revalidates the
current view and shared map/value generations before dereferencing the pointer.
This hazard-style sequence closes the race with a concurrent local generation
switch without a lookup mutex or per-lookup reference-count allocation.
Block mappings are shared by all lookup threads in that process, so the design
uses at most one file descriptor/mapping per opened value block per process,
not one per reader thread.

Observing a new shared value generation invokes a process-local slow path.
Competing threads construct candidate views and use compare/exchange to install
one; unsuccessful candidates are discarded. The old view remains as a small
stable-lifetime shell and its mappings are released only after a local scan
finds no slot owned by that process-local connection
with a nonzero `active_epoch` whose subsequently loaded
`active_value_generation` advertises the old generation. No process-local mutex
is used for generation installation or cleanup.

The writer creates, sizes, and maps a new value block before publishing any
record offset in that block, and the writer process release-publishes the
allocator's mapping in the same fixed block-view table before bucket
publication. This also protects concurrent `GetValue()` calls in the writer
process from allocator-side block-table mutation. A reader process that first
encounters an offset in an unmapped block uses a view-local slow path to
open/map that block and compare/exchange its pointer into the fixed block-view
table. Competing first mappers discard their redundant mappings; mappings are
never removed from an active view. The reader then revalidates the slot epoch,
bucket hash/offset, and map/value generations before dereferencing. Lookups
whose target blocks are already mapped remain lock-free and perform no file
open.

An active reader therefore keeps its old file descriptor and mapping, so POSIX
unlink does not invalidate bytes it is already copying. Its final map/value
generation checks reject a result that crossed cutover. If opening a generation
fails with `ENOENT`, the reader reloads the shared map and value generations: a
change causes `TRY_AGAIN`, while the same stable even generation produces
`READ_ERROR`. No reader treats an old-generation `ENOENT` as a permanent error
after a successful cutover.

This makes restart availability independent of reader scheduling. Old disk and
mapped pages are released when the last reader drops the old generation. If a
writer dies before cutover, the new generation is an orphan that the next
writer removes. If it dies after cutover, the next writer performs another
generation switch and cleans older files.

A reader process that remains idle after cutover may retain its one previously
current view until its next lookup or destruction. A thread paused inside a
lookup may retain that advertised generation indefinitely; this is required for
memory safety and does not block the new writer. A reader process retains at
most its current view plus views still advertised by its active local slots, so
it does not accumulate every generation skipped while idle.

## 9. Memory management policy

- Use a separate jemalloc arena owned only by the writer.
- Readers map value blocks but never call jemalloc allocation/free APIs.
- Store offsets, never process-local pointers, in shared structures.
- Disable jemalloc thread caches for this arena, matching `RollableFile`'s
  existing `MALLOCX_TCACHE_NONE` behavior.
- Reserve offset zero as invalid and reject it before record resolution or
  reclamation.
- Track live allocated bytes, retired bytes, and allocation failures.
- Batch reclamation to keep writer overhead low.
- Never reclaim based only on elapsed time.
- Never resize or unlink a mapped value file while readers are attached.

The last rule applies to the active generation. A retired generation may be
unlinked after header cutover because existing mappings remain valid and no
new reader opens that generation.

Jemalloc value mode does not use sliding or partially mapped allocation blocks.
Checked construction requires
`value_memcap == value_block_size * max_value_blocks`, with overflow checks, and
the writer maps every block from which its arena may allocate. The largest
permitted record, including allocator metadata and alignment, must fit wholly
inside one value block. The writer calls `RollableFile::Free(offset)` only for a
validated offset in a block mapped by that writer. Readers never invoke
jemalloc allocation or free APIs. Incompatible block size, memory cap, or
maximum-block settings are rejected before shared state is changed.

## 10. Hash collisions and probing

The first implementation keeps the current build-selected 64-bit hash and
linear probing (XXH3 when enabled, with the existing FNV-1a fallback). The
production hash-algorithm identifier is stored in the header and must match in
every writer and reader process; a mismatch rejects open instead of silently
returning misses. Unlike current compact reference mode, equal hashes do not
imply equal keys in value mode. Every hash candidate is verified against the
complete key stored in its record; a different key continues probing.

This provides exact collision correctness without increasing the 16-byte
bucket. The selected production hash is a non-cryptographic indexing hash; it
is not used for data integrity or authentication.

Capacity remains a power of two for masking speed. Construction must round the
requested minimum capacity up, subject to the configured index memory cap, and
must reject configurations that cannot provide the requested capacity. The
writer should reject inserts once the configured maximum occupancy is reached
rather than allowing unbounded probe growth.

Online resizing and tombstone rebuilding require a separate design because
moving buckets while readers probe them adds a second reclamation problem.

## 11. Error and crash behavior

| Condition | Required result |
|---|---|
| Invalid key/value length or arithmetic overflow | Reject before allocation |
| Value allocation failure | Existing entry remains unchanged |
| Index is full | Free unpublished record and return `NO_RESOURCE` |
| Second writer attempts to open value mode | Writer open fails; shared state remains unchanged |
| Reader cannot claim an epoch slot | Return `TRY_AGAIN`/resource error; do not read unprotected |
| Invalid or out-of-range record offset | Return `READ_ERROR`; never dereference |
| Stable current value generation or required block cannot be opened | Return `READ_ERROR`; leave `MBData::data_len` zero |
| Generation changes while opening or copying | Clear active state and return/retry as `TRY_AGAIN`; never report not-found from the rejected attempt |
| Retired-memory limit cannot accept an old record | Overwrite/erase fails before publication; existing entry remains unchanged |
| Writer dies before bucket publication | Unreachable allocation discarded with the old value generation on restart |
| Writer dies after publication | Published immutable record remains readable |
| Writer dies with retired records | Records remain allocated until the old value generation is retired on restart |
| Reader process dies inside lookup | Writer clears slot only after verified process death |
| Reader remains paused | Its active generation remains protected; current-generation reclamation may wait, while a later writer generation proceeds |
| Layout/configuration mismatch | Reject open; require map recreation |

Every non-successful `GetValue()` return leaves `MBData::data_len == 0`.

## 12. Performance expectations and measurements

The value lookup necessarily adds a record access and value copy relative to
the current reference-only benchmark. The design minimizes additional index
cost by retaining 16-byte buckets and immutable records.

Reader epoch/view publication is the largest new fixed lookup cost. Slot
ownership is acquired once per thread; a steady-state lookup performs one
sequentially consistent `active_epoch` publication plus the required atomic
epoch/view loads and release clears. It must not perform slot CAS,
`getpid()`/process-start lookup, file opening, or mutex/file-lock acquisition.
The writer advances the global epoch once per reclamation batch rather than on
every overwrite/erase, minimizing reader-entry retries during continuous
updates. This fixed cost must be measured rather than assumed negligible. The
performance suite should report:

1. Existing reference-only compact HashMap lookup.
2. Value-mode lookup with a one-byte value.
3. Value-mode lookup with representative production value sizes.
4. Value-mode lookup while the writer overwrites records.
5. Value-mode lookup while the writer removes/reinserts records.
6. Optimized Mabain radix-tree lookup using the same keys and values.

Report average and percentile latency, lookups/second, index RSS, value-arena
allocated bytes, retired bytes, and maximum reclamation backlog. Run with one,
eight, and 32 reader threads and with multiple reader processes.

Measure pre-sized `MBData` separately from buffer-growth calls. Measure the
first lookup after writer restart and the first access to a newly created value
block separately from the steady-state path. Generation/block installation
cost must not be averaged into claims about ordinary lookup synchronization.

No index-table optimization should be accepted solely from estimated bucket
sizes; benchmark it after the value-mode baseline is correct.

## 13. Validation plan

### Unit tests

- Binary keys and values, including embedded zero bytes.
- Minimum, boundary, and invalid key/value lengths.
- Checked record-size and offset arithmetic.
- Insert, duplicate insert, overwrite, erase, and reinsert.
- Allocation failure leaves the prior value intact.
- A test-only injectable hash seam creates a deterministic full 64-bit
  collision and proves exact-key probing. Production builds expose no injection
  path and continue to use the hash algorithm recorded in the header.
- Tombstones preserve probe chains.
- Reader cannot create or initialize missing storage.
- A second writer is rejected without changing shared state; readers never
  participate in the writer lock.
- Writer and reader builds selecting different production hash algorithms are
  rejected at open.
- Configuration and layout mismatches are rejected.
- Every failed/retried lookup leaves `MBData::data_len` zero.
- Retired-memory exhaustion rejects overwrite/erase before publication.
- Retired records are not freed while an epoch slot is active.
- Retired records are freed after readers advance.
- Multiple mutations share one retirement epoch and one reclamation pass
  advances the epoch only once; records retired after that advance wait for the
  next pass.
- Deterministic pause hooks cover publication before/after
  `active_value_generation`, `active_epoch`, bucket replacement, epoch advance,
  and reclamation scan.
- Slot ownership is claimed once per thread, idle owned slots do not block
  reclamation, and thread teardown releases ownership.
- HashMap destruction after local lookup threads join clears that reader
  connection's slots without leaving dangling thread-local object pointers.
- PID reuse cannot clear a replacement reader's slot.
- Writer restart switches value-file generations without waiting for a paused
  reader and rejects any lookup that crossed the cutover.
- A paused old-generation slot protects its old local view but does not block
  reclamation of records retired in the new generation.
- Concurrent threads sharing one reader object cannot unmap a generation still
  advertised by another local thread.
- Concurrent first access to a newly published value block installs exactly one
  local block mapping and revalidates before dereference.
- A stale-generation `ENOENT` retries after cutover, while a stable-current
  generation open failure returns `READ_ERROR`.
- Newly created value-mode index, value, and lock files have the required
  owner-only permissions.

### Stress tests under `src/test`

- One writer process with multiple reader processes.
- At least 32 concurrent reader threads across those processes.
- Continuous overwrite with no permitted lookup misses.
- Remove/reinsert with misses permitted but wrong/torn values forbidden.
- Random value lengths and contents validated end to end.
- Writer growth into new value blocks while reader threads continuously look up
  records in both old and newly mapped blocks.
- Concurrent `GetValue()` threads in the writer process while its serialized
  mutation path allocates and publishes a new block.
- Forced writer termination at allocation, publication, retirement, and reset
  phases, followed by writer restart.
- Repeated writer restart while multiple processes and threads continuously
  look up values, verifying generation-view lifetime and bounded old mappings.
- Reader process termination while holding an epoch slot.
- Long-running churn verifies that allocated memory becomes bounded after
  readers quiesce.

### Regression validation

- Existing HashMap reference-mode tests and benchmarks.
- Existing full Mabain GoogleTest suite.
- Existing standalone `src/test` runbook.
- Radix-tree lookup benchmark to confirm no unrelated DB regression.

## 14. Implementation sequence

1. Add value-mode configuration, atomic versioned header fields, owner-only file
   creation, and the writer-lifetime lock without changing existing
   reference-mode behavior.
2. Add the separate jemalloc-backed value `RollableFile` and immutable record
   helpers with strict block, offset, and checked-size validation.
3. Add value APIs and exact full-key collision handling, including the
   success-only `MBData` publication contract.
4. Add persistent per-thread reader slots, writer retirement preflight, and
   epoch reclamation.
5. Add crash-safe writer restart using a fresh value-file generation and the
   hazard-protected process-local generation-view switch.
6. Add unit and multiprocess/thread stress tests, including deterministic hash
   collision injection in test builds only.
7. Run correctness, crash, memory-churn, file-permission, and performance
   validation.
8. Only after this baseline is accepted, evaluate index-layout changes.

Each implementation step should be reviewed before commit. No change should be
made to the radix-tree DB lookup/update path as part of this work.

## 15. Decisions required before implementation

1. Confirm that HashMap value limits should match the existing Mabain limits:
   256-byte keys and 32767-byte values.
2. Confirm `MBData` as the copying output type for `GetValue()`.
3. Select the default reader-slot count; 64 is sufficient for the stated
   32-reader-thread case, while 128 provides more operational headroom.
4. Select the reclamation batch threshold, hard retired-memory limit, and
   whether a blocked mutation returns `TRY_AGAIN` or `NO_MEMORY` when
   reclamation cannot progress.
5. Decide whether value-arena pressure may clear the cache and switch to a new
   value-file generation instead of rejecting writes. This is safe but loses
   cached entries.
6. Establish the acceptable measured lookup overhead of steady-state
   epoch/generation-view publication before implementation is considered
   complete.

## 16. Security and operational notes

- HashMap files contain plaintext key/value data. Newly created value-mode
  index files, generation files, and the writer lock file default to owner-only
  mode `0600`. Creation must pass this mode explicitly and must not rely on the
  generic mmap-file `0644` request or on the caller's umask to remove group/world
  access. This proposal does not add encryption.
- The default multi-process topology assumes writer and reader processes run
  under the same effective user. Cross-user sharing requires a separate,
  explicit ownership/mode design and is not enabled by weakening the default.
- The production XXH3/FNV hash selection is intentionally non-cryptographic
  indexing. It must not be represented as integrity or authentication
  protection.
- No credentials, keys, tokens, or certificates are embedded in the design.
- Every mapped offset and length is validated before bounded copy operations.
- On uncertainty, reclamation fails closed by retaining memory; it never frees
  a record that may still be visible to a reader.
