/**
 * Prefix cache for accelerating Find lookups by caching intermediate edge state
 */

#ifndef MABAIN_PREFIX_CACHE_H
#define MABAIN_PREFIX_CACHE_H

#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "drm_base.h" // for EDGE_SIZE

namespace mabain {

struct PrefixCacheEntry {
    size_t edge_offset; // Edge offset for the cached prefix
    uint8_t edge_buff[EDGE_SIZE]; // Snapshot of the edge content
    uint32_t lf_counter; // Optional: writer counter observed when cached
};

class PrefixCache {
public:
    explicit PrefixCache(int prefix_len, size_t capacity = 65536);

    bool Get(const uint8_t* key, int len, PrefixCacheEntry& out);
    void Put(const uint8_t* key, int len, const PrefixCacheEntry& in);

    int PrefixLen() const { return n; }
    size_t Size() const;
    void Clear();

    // Simple counters for diagnostics (single-threaded use)
    uint64_t HitCount() const { return hit_count; }
    uint64_t MissCount() const { return miss_count; }
    uint64_t PutCount() const { return put_count; }
    void ResetStats() { hit_count = miss_count = put_count = 0; }

private:
    inline bool BuildKey(const uint8_t* key, int len, uint64_t& out) const;
    inline bool BuildIndex4(const uint8_t* key, int len, uint32_t& idx) const;

    const int n;
    const size_t cap;
    std::unordered_map<uint64_t, PrefixCacheEntry> map;
    uint64_t hit_count = 0;
    uint64_t miss_count = 0;
    uint64_t put_count = 0;

    // Fast-path for n == 4: direct index into fixed array of 65536 slots
    bool fast4;
    std::vector<PrefixCacheEntry> slots4;
    std::vector<uint8_t> filled4;
};

} // namespace mabain

#endif // MABAIN_PREFIX_CACHE_H
