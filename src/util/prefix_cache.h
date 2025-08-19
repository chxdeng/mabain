/**
 * Prefix cache for accelerating Find lookups by caching intermediate edge state
 */

#ifndef MABAIN_PREFIX_CACHE_H
#define MABAIN_PREFIX_CACHE_H

#include <cstddef>
#include <cstdint>
#include <vector>

#include "drm_base.h" // for EDGE_SIZE
#include "util/prefix_cache_iface.h"

namespace mabain {

class PrefixCache : public PrefixCacheIface {
public:
    // capacity: overall budget; must be <= total 3-byte prefix count (2^24).
    // - table2: direct table sized to floor_pow2(min(capacity, 65536)).
    // - table3: direct table sized to floor_pow2(max(capacity - 65536, 0)).
    explicit PrefixCache(size_t capacity = 65536);

    bool Get(const uint8_t* key, int len, PrefixCacheEntry& out) const override;
    void Put(const uint8_t* key, int len, const PrefixCacheEntry& in) override;
    int GetDepth(const uint8_t* key, int len, PrefixCacheEntry& out) const override;
    void SetFastNoTagCheck(bool v) { fast_no_tag_check = v; }

    // Report the maximum prefix length this cache can seed from (3 bytes)
    int PrefixLen() const override { return 3; }
    bool IsShared() const override { return false; }
    size_t Size() const; // total entries across both tables
    size_t Size2() const; // entries in 2-byte table
    size_t Size3() const; // entries in 3-byte table
    void Clear();

    // Simple counters for diagnostics (single-threaded use)
    uint64_t HitCount() const { return hit_count; }
    uint64_t MissCount() const { return miss_count; }
    uint64_t PutCount() const { return put_count; }
    void ResetStats() { hit_count = miss_count = put_count = 0; }

private:
    inline bool build2(const uint8_t* key, int len, uint16_t& p2) const;
    inline bool build3(const uint8_t* key, int len, uint32_t& p3) const;

    const size_t cap2;
    const size_t cap3;
    size_t mask3 = 0; // cap3-1 when cap3 is power-of-two

    // 2-byte table: direct index sized by capacity (power-of-two up to 65536)
    std::vector<PrefixCacheEntry> table2;
    std::vector<uint8_t> filled2;
    std::vector<uint16_t> keys2; // stored 2-byte prefixes for validation
    size_t size2 = 0;
    size_t mask2 = 0; // cap2-1 when using mask
    bool use_mask2 = true; // true when cap2 is power-of-two

    // 3-byte table: sparse hash map keyed by top 3 bytes (big endian)
    // 3-byte table: direct index (0..16,777,215)
    std::vector<PrefixCacheEntry> table3;
    std::vector<uint8_t> filled3;
    std::vector<uint32_t> keys3; // stored 3-byte prefixes for validation
    size_t size3 = 0;

    mutable uint64_t hit_count = 0;
    mutable uint64_t miss_count = 0;
    mutable uint64_t put_count = 0;

    bool fast_no_tag_check = false; // skip tag validation on table3 in read-only bench mode
};

} // namespace mabain

#endif // MABAIN_PREFIX_CACHE_H
