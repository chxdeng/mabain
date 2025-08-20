/**
 * Prefix cache for accelerating Find lookups by caching intermediate edge state
 */

#ifndef MABAIN_PREFIX_CACHE_H
#define MABAIN_PREFIX_CACHE_H

#include <cstddef>
#include <cstdint>
#include <atomic>
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
    int GetDepth(const uint8_t* key, int len, PrefixCacheEntry& out) const override;
    // Report the maximum prefix length this cache can seed from (3 bytes)
    int PrefixLen() const override { return 3; }
    bool IsShared() const override { return true; }
    size_t Size() const; // total entries across both tables
    size_t Size2() const; // entries in 2-byte table
    size_t Size3() const; // entries in 3-byte table
    void Clear();

    // Simple counters for diagnostics (single-threaded use)
    uint64_t HitCount() const { return 0; }
    uint64_t MissCount() const { return 0; }
    uint64_t PutCount() const { return put_count; }
    void ResetStats() { put_count = 0; }

private:
    inline bool build2(const uint8_t* key, int len, uint16_t& p2) const;
    inline bool build3(const uint8_t* key, int len, uint32_t& p3) const;

    const size_t cap2;
    const size_t cap3;
    size_t mask3 = 0; // cap3-1 when cap3 is power-of-two
    size_t mask2 = 0; // cap2-1
    bool full2 = false; // true when cap2 == 65536 (no aliasing)

    // Direct pointers for table/tag arrays (shared-mapped or process-local fallback)
    PrefixCacheEntry* tab2 = nullptr;
    std::atomic<uint32_t>* tag2 = nullptr;
    PrefixCacheEntry* tab3 = nullptr;
    std::atomic<uint32_t>* tag3 = nullptr;

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
