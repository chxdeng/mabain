/**
 * Writer-managed shared prefix cache
 * - Lock-free readers using per-entry CAS seqlock
 * - Multi-process writers (each Put/Invalidate acquires a slot via CAS)
 * - Targets invalidation by edge offset (no global flush)
 */
#ifndef MABAIN_PREFIX_CACHE_SHARED_H
#define MABAIN_PREFIX_CACHE_SHARED_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <ostream>

#include "drm_base.h"          // EDGE_SIZE
#include "util/prefix_cache_iface.h"

namespace mabain {

struct PrefixCacheSharedHeader {
    uint32_t magic;
    uint16_t version;
    uint16_t n;            // prefix length (bytes)
    uint32_t assoc;        // slots per bucket
    uint64_t nbuckets;     // number of buckets
    // stats (atomic; multi-process safe)
    std::atomic<uint64_t> hit;
    std::atomic<uint64_t> miss;
    std::atomic<uint64_t> put;
    std::atomic<uint64_t> invalidated;
};

// One slot; CAS seqlock guards the body
struct PrefixCacheSharedEntry {
    std::atomic<uint32_t> seq;   // even=stable, odd=write in progress
    uint8_t  prefix_len;         // stored prefix length (<= 8), zero means empty
    uint8_t  prefix[8];          // first up to 8 bytes of key
    size_t   edge_offset;        // saved edge offset
    uint8_t  edge_buff[EDGE_SIZE]; // saved edge content
    uint32_t _pad;               // padding
};

class PrefixCacheShared : public PrefixCacheIface {
public:
    static PrefixCacheShared* CreateWriter(const std::string& mbdir,
                                           int prefix_len,
                                           size_t capacity,
                                           uint32_t associativity = 4);
    static PrefixCacheShared* OpenReader(const std::string& mbdir);
    ~PrefixCacheShared();

    // Multi-process lock-free read
    bool Get(const uint8_t* key, int len, PrefixCacheEntry& out) const override;
    // Multi-process writers (any process can call these)
    void Put(const uint8_t* key, int len, const PrefixCacheEntry& in) override;
    void InvalidateByEdgeOffset(size_t edge_offset);
    // Targeted invalidation using the key's prefix: scans only its bucket
    void InvalidateByPrefixAndEdge(const uint8_t* key, int len, size_t edge_offset);

    void DumpStats(std::ostream& os) const;
    int PrefixLen() const override { return hdr_ ? hdr_->n : 0; }
    bool IsShared() const override { return true; }
    size_t Buckets() const { return hdr_ ? hdr_->nbuckets : 0; }
    uint32_t Assoc() const { return hdr_ ? hdr_->assoc : 0; }

    static std::string ShmPath(const std::string& mbdir) {
        return mbdir + "_pfxcache";
    }

private:
    PrefixCacheShared() = default;
    static uint64_t fnv1a64(const uint8_t* p, size_t n);
    inline size_t bucket_of(const uint8_t* pfx, size_t n) const;
    inline size_t slot_of(uint64_t h) const; // deterministic preferred slot

    bool map_file(const std::string& path, bool create,
                  int n, size_t capacity, uint32_t assoc);
    void unmap();

    void*  base_ = nullptr;
    size_t size_ = 0;
    PrefixCacheSharedHeader* hdr_ = nullptr;
    PrefixCacheSharedEntry*  entries_ = nullptr;
};

} // namespace mabain

#endif // MABAIN_PREFIX_CACHE_SHARED_H
