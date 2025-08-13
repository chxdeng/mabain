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
#include "util/prefix_cache_iface.h"

namespace mabain {

class PrefixCache : public PrefixCacheIface {
public:
    explicit PrefixCache(int prefix_len, size_t capacity = 65536);

    bool Get(const uint8_t* key, int len, PrefixCacheEntry& out) const override;
    void Put(const uint8_t* key, int len, const PrefixCacheEntry& in) override;

    int PrefixLen() const override { return n; }
    bool IsShared() const override { return false; }
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
    mutable uint64_t hit_count = 0;
    mutable uint64_t miss_count = 0;
    mutable uint64_t put_count = 0;

    // Fast-path for n == 4: direct index into fixed array of 65536 slots
    bool fast4;
    std::vector<PrefixCacheEntry> slots4;
    std::vector<uint8_t> filled4;
};

} // namespace mabain

#endif // MABAIN_PREFIX_CACHE_H
