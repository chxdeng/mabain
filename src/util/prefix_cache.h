/**
 * Prefix cache for accelerating Find lookups by caching intermediate edge state
 */

#ifndef MABAIN_PREFIX_CACHE_H
#define MABAIN_PREFIX_CACHE_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

#include "drm_base.h" // for EDGE_SIZE

namespace mabain {

// PrefixCache tables overview
//
// The cache is composed of three independent tables addressed by a little-endian
// prefix derived from the user key:
//
// - 2-byte table (tab2/tag2):
//   - Dense coverage of all 2-byte prefixes when `cap2 == 65536` (full2 == true).
//     In this mode there is no aliasing and lookups only check that tag2[idx] is
//     non-zero to consider the slot populated (we still store `p2+1` as the tag).
//   - When `cap2 < 65536`, `mask2` induces aliasing. We store `p2+1` in tag2 to
//     distinguish empty (0) from the real prefix value 0, and to verify hits under
//     aliasing (compare tag2[idx] == p2+1).
//   - This table provides the broadest coverage and is the most effective
//     fast-path for short keys and early traversal seeding.
//
// - 3-byte table (tab3/tag3):
//   - Capacity is a power-of-two (possibly zero) selected from the requested
//     capacity remainder. The index uses `p3 & mask3` and the tag stores `p3+1`.
//   - Because of masking there can be aliasing; the `+1` tag scheme allows us to
//     safely differentiate empty from a legitimate `p3 == 0` and validate hits.
//   - This table provides higher specificity than 2-byte, reducing false positives
//     under aliasing while remaining compact.
//
// - 4-byte sparse table (tab4/tag4/valid4):
//   - Capacity is also a power-of-two and indexing uses `p4 & mask4`.
//   - We store the exact 32-bit prefix in `tag4` and keep a separate `valid4`
//     bitmap (0 = empty, 1 = populated). Readers check `valid4[idx] != 0` and
//     then compare `tag4[idx] == p4` to validate. Writers clear `valid4` first,
//     store the entry body and tag, and finally set `valid4` — this provides a
//     simple publish protocol without relying on `p4+1` which does not fit the
//     full 32-bit space cleanly.
//
// Concurrency & publication protocol
// - All tag/valid arrays are std::atomic<uint32_t> and are accessed with
//   acquire on reads and release on writes. For 2/3-byte tables, publication is
//   done by writing `tag=0`, storing the body, then `tag=p+1` (release). For the
//   4-byte table, publication is `valid=0`, store body, store `tag=p4` (release),
//   then `valid=1` (release). Reads use acquire loads to validate and then copy
//   the body directly.
//
// Notes
// - `lf_counter` is carried over on overwrites to preserve origin bits when the
//   same slot is reused (e.g., due to aliasing).
// - All pointers reference a single shared-memory mapping; there is no
//   process-local fallback.

struct PrefixCacheEntry {
    size_t edge_offset;
    uint8_t edge_buff[EDGE_SIZE];
    // Number of bytes already consumed within the current edge label
    // when this entry is used (1..edge_len). 0 means start-of-edge.
    uint8_t edge_skip;
    uint8_t reserved_[3];
    uint32_t lf_counter;
};

class PrefixCache {
public:
    ~PrefixCache();
    // Shared cache backed by embedded region in _mabain_d.
    PrefixCache(const std::string& mbdir, const IndexHeader* hdr, size_t capacity = 65536);

    void Put(const uint8_t* key, int len, const PrefixCacheEntry& in);
    void PutAtDepth(const uint8_t* key, int depth, const PrefixCacheEntry& in);
    int GetDepth(const uint8_t* key, int len, PrefixCacheEntry& out) const;
    // Report the maximum prefix length this cache can seed from (3 bytes reported for compatibility)
    int PrefixLen() const { return 3; }
    bool IsShared() const { return true; }
    size_t Size() const; // total entries across tables
    size_t Size2() const; // entries in 2-byte table
    size_t Size3() const; // entries in 3-byte table
    size_t Size4() const; // entries in 4-byte table
    // Capacity getters (number of slots)
    size_t Cap2() const { return cap2 ? cap2 : 1; }
    size_t Cap3() const { return cap3; }
    size_t Cap4() const { return cap4; }
    // Memory footprint per table in bytes (including tag/valid arrays)
    size_t Memory2() const;
    size_t Memory3() const;
    size_t Memory4() const;
    void Clear();

    // Simple counters for diagnostics (single-threaded use)
    uint64_t HitCount() const { return 0; }
    uint64_t MissCount() const { return 0; }
    uint64_t PutCount() const { return put_count; }
    void ResetStats() { put_count = 0; }

private:
    inline bool build2(const uint8_t* key, int len, uint16_t& p2) const;
    inline bool build3(const uint8_t* key, int len, uint32_t& p3) const;
    inline bool build4(const uint8_t* key, int len, uint32_t& p4) const;

    const size_t cap2;
    const size_t cap3;
    const size_t cap4;
    size_t mask3 = 0; // cap3-1 when cap3 is power-of-two
    size_t mask2 = 0; // cap2-1
    size_t mask4 = 0; // cap4-1
    bool full2 = false; // true when cap2 == 65536 (no aliasing)

    // Direct pointers for table/tag arrays (shared-mapped only). See the
    // table overview above for design and usage semantics.
    PrefixCacheEntry* tab2 = nullptr;
    std::atomic<uint32_t>* tag2 = nullptr;
    PrefixCacheEntry* tab3 = nullptr;
    std::atomic<uint32_t>* tag3 = nullptr;
    // 4-byte sparse table: use a separate valid flag to avoid tag overflow issues
    PrefixCacheEntry* tab4 = nullptr;
    std::atomic<uint32_t>* tag4 = nullptr; // stores exact 32-bit prefix
    std::atomic<uint32_t>* valid4 = nullptr; // 0 = empty, 1 = valid

    // Hit/miss counters removed; keep only put_count for write diagnostics
    mutable uint64_t put_count = 0;

    // Shared-mapping settings
    // Underlying mapping base/size (may include a leading delta before cache)
    void* shm_base = nullptr;
    size_t shm_size = 0;
    size_t shm_delta = 0; // bytes from shm_base to the start of cache region
    int shm_fd = -1;
    std::string shm_path;
    bool map_shared(const std::string& path);
    bool map_embedded(const std::string& mbdir);
    void unmap_shared();

    const IndexHeader* hdr_ = nullptr; // header for embedded layout discovery

public:
    static std::string ShmPath(const std::string& mbdir)
    {
        // Use DB directory based path to maximize portability across systems
        return mbdir + "_pfxcache";
    }
};

} // namespace mabain

#endif // MABAIN_PREFIX_CACHE_H
