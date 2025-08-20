/**
 * Prefix cache for accelerating Find lookups by caching intermediate edge state
 */

#ifndef MABAIN_PREFIX_CACHE_H
#define MABAIN_PREFIX_CACHE_H

#include <cstddef>
#include <cstdint>
#include <vector>
#include <string>

#include "drm_base.h" // for EDGE_SIZE
#include "util/prefix_cache_iface.h"

namespace mabain {

class PrefixCache : public PrefixCacheIface {
public:
    // capacity: overall budget; must be <= total 3-byte prefix count (2^24).
    // - table2: direct table sized to floor_pow2(min(capacity, 65536)).
    // - table3: direct table sized to floor_pow2(max(capacity - 65536, 0)).
    explicit PrefixCache(size_t capacity = 65536);
    ~PrefixCache();
    // Unified cache: optionally back tables by shared memory file (multi-process)
    PrefixCache(const std::string& mbdir, bool use_shared_mem, bool writer_mode, size_t capacity = 65536);

    bool Get(const uint8_t* key, int len, PrefixCacheEntry& out) const override;
    void Put(const uint8_t* key, int len, const PrefixCacheEntry& in) override;
    int GetDepth(const uint8_t* key, int len, PrefixCacheEntry& out) const override;
    void SetFastNoTagCheck(bool v) { fast_no_tag_check = v; }

    // Report the maximum prefix length this cache can seed from (3 bytes)
    int PrefixLen() const override { return 3; }
    bool IsShared() const override { return use_shared_mem; }
    size_t Size() const; // total entries across both tables
    size_t Size2() const; // entries in 2-byte table
    size_t Size3() const; // entries in 3-byte table
    void Clear();
    // Count entries by origin, using lf_counter tagging: 1=add-time, 2=read-time
    void CountOrigin(size_t& add_entries, size_t& read_entries) const;

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
    size_t mask2 = 0; // cap2-1
    bool full2 = false; // true when cap2 == 65536 (no aliasing)

    // 2-byte table storage
    std::vector<PrefixCacheEntry> table2_vec;
    std::vector<uint32_t> tag2_vec; // tag2 stores p2+1; 0 means empty
    // 3-byte table storage
    std::vector<PrefixCacheEntry> table3_vec;
    std::vector<uint32_t> tag3_vec; // tag3 stores p3+1; 0 means empty
    // Direct pointers used by both normal and shared-mapped storage
    PrefixCacheEntry* tab2 = nullptr;
    uint32_t* tag2 = nullptr;
    PrefixCacheEntry* tab3 = nullptr;
    uint32_t* tag3 = nullptr;
    size_t size2 = 0;
    size_t size3 = 0;

    mutable uint64_t hit_count = 0;
    mutable uint64_t miss_count = 0;
    mutable uint64_t put_count = 0;

    bool fast_no_tag_check = false; // skip tag validation on table3 in read-only bench mode
    // Shared-mapping settings
    bool use_shared_mem = false;
    bool writer_mode_ = false;
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
