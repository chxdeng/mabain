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
#include "util/prefix_cache_iface.h"

namespace mabain {

class PrefixCache : public PrefixCacheIface {
public:
    ~PrefixCache();
    // Shared cache backed by shared memory file (multi-process).
    PrefixCache(const std::string& mbdir, size_t capacity = 65536);

    void Put(const uint8_t* key, int len, const PrefixCacheEntry& in) override;
    void PutAtDepth(const uint8_t* key, int depth, const PrefixCacheEntry& in) override;
    int GetDepth(const uint8_t* key, int len, PrefixCacheEntry& out) const override;
    // Report the maximum prefix length this cache can seed from (3 bytes reported for compatibility)
    int PrefixLen() const override { return 3; }
    bool IsShared() const override { return true; }
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

    // Direct pointers for table/tag arrays (shared-mapped or process-local fallback)
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
    void* shm_base = nullptr;
    size_t shm_size = 0;
    int shm_fd = -1;
    std::string shm_path;
    bool map_shared(const std::string& path);
    void unmap_shared();

public:
    static std::string ShmPath(const std::string& mbdir)
    {
        // Use DB directory based path to maximize portability across systems
        return mbdir + "_pfxcache";
    }
};

} // namespace mabain

#endif // MABAIN_PREFIX_CACHE_H
