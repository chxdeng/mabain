/**
 * Prefix cache implementation
 */

#include "util/prefix_cache.h"
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace mabain {

// Returns the largest power-of-two <= x (0 for x==0).
static inline size_t floor_pow2(size_t x)
{
    if (x == 0)
        return 0;
    // Round down to the highest power of two <= x
    size_t p = 1;
    while ((p << 1) && ((p << 1) <= x))
        p <<= 1;
    return p;
}

// Helper: compute a power-of-two size for the 3-byte table from a total capacity.
// Keeps the first 64K for the 2-byte table, caps 3-byte table at 2^24 slots.
static inline size_t cap3_from_capacity(size_t capacity)
{
    // Allocate 3-byte table only for capacity in excess of 2-byte coverage (65536)
    if (capacity <= 65536)
        return 0;
    size_t excess = capacity - 65536;
    size_t capped = std::min<size_t>(excess, (1u << 24));
    size_t p2 = floor_pow2(capped);
    return p2; // may be 0 if excess < 1
}

// Construct a prefix cache and back it with a single shared-memory mapping.
// Layout (in order):
//   header | tag2 | tab2 | tag3 | tab3 | valid4 | tag4 | tab4
// tag2/tag3 store (prefix+1) to differentiate empty(0) from real 0-value keys when aliased.
// 4-byte table uses a separate valid[] to allow full 32-bit tags without +1.
PrefixCache::PrefixCache(const std::string& mbdir, size_t capacity)
    : cap2(0)
    , cap3(0)
    , cap4(0)
{
    // Allow capacity beyond 2^24 to provision a sparse 4-byte table.
    size_t norm_cap = capacity;
    size_t base2 = std::min<size_t>(norm_cap, (size_t)65536);
    size_t c2 = floor_pow2(base2);
    if (norm_cap > 0 && c2 < 16384)
        c2 = 16384;
    const_cast<size_t&>(cap2) = c2;
    // Split remainder between 3-byte and 4-byte sparse tables
    size_t remainder = (norm_cap > 65536 ? norm_cap - 65536 : 0);
    // Allow biasing 4-byte table via env: MB_PFXCACHE_4_RATIO=[0..100]
    int ratio4 = 50; // default: split remainder evenly (reduce mem4)
    if (const char* env = std::getenv("MB_PFXCACHE_4_RATIO")) {
        int r = std::atoi(env);
        if (r < 0)
            r = 0;
        if (r > 100)
            r = 100;
        ratio4 = r;
    }
    size_t target4 = (remainder * (size_t)ratio4) / 100;
    size_t target3 = (remainder > target4 ? remainder - target4 : 0);
    size_t c3 = floor_pow2(target3);
    size_t c4 = floor_pow2(target4);
    const_cast<size_t&>(cap3) = c3;
    const_cast<size_t&>(cap4) = c4;

    mask2 = (cap2 ? cap2 - 1 : 0);
    mask3 = (cap3 ? cap3 - 1 : 0);
    mask4 = (cap4 ? cap4 - 1 : 0);
    full2 = (cap2 == 65536);
    shm_path = PrefixCache::ShmPath(mbdir);
    if (!map_shared(shm_path)) {
        throw std::runtime_error("PrefixCache: failed to map shared memory");
    }
}

struct PCShmHeader {
    uint32_t magic;
    uint16_t version;
    uint16_t reserved;
    uint32_t cap2;
    uint32_t cap3;
    uint32_t cap4;
    uint32_t mask2;
    uint32_t mask3;
    uint32_t mask4;
};

// Create or open the shared-memory file and map the cache arrays.
// Initializes tags/valids on first creation; otherwise issues MADV_WILLNEED.
bool PrefixCache::map_shared(const std::string& path)
{
    const uint32_t MAGIC = 0x50434632; // 'PCF2'
    const uint16_t VER = 2; // bump for 4-byte table layout
    size_t t2 = sizeof(PrefixCacheEntry) * (cap2 ? cap2 : 1);
    size_t g2 = sizeof(uint32_t) * (cap2 ? cap2 : 1);
    size_t t3 = sizeof(PrefixCacheEntry) * (cap3 ? cap3 : 1);
    size_t g3 = sizeof(uint32_t) * (cap3 ? cap3 : 1);
    size_t t4 = sizeof(PrefixCacheEntry) * (cap4 ? cap4 : 1);
    size_t g4 = sizeof(uint32_t) * (cap4 ? cap4 : 1); // tag4
    size_t v4 = sizeof(uint32_t) * (cap4 ? cap4 : 1); // valid4
    size_t need = sizeof(PCShmHeader) + g2 + t2 + g3 + t3 + v4 + g4 + t4;

    int fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd < 0)
        return false;
    struct stat st;
    if (fstat(fd, &st) != 0) {
        ::close(fd);
        return false;
    }
    bool init = false;
    if ((size_t)st.st_size < need) {
        if (ftruncate(fd, need) != 0) {
            ::close(fd);
            return false;
        }
        init = true;
    }

    int prot = PROT_READ | PROT_WRITE;
    void* base = ::mmap(nullptr, need, prot, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) {
        ::close(fd);
        return false;
    }

    shm_base = base;
    shm_size = need;
    shm_fd = fd;

    auto* hdr = reinterpret_cast<PCShmHeader*>(base);
    if (hdr->magic != MAGIC || hdr->version != VER) {
        hdr->magic = MAGIC;
        hdr->version = VER;
        hdr->reserved = 0;
        hdr->cap2 = static_cast<uint32_t>(cap2 ? cap2 : 1);
        hdr->cap3 = static_cast<uint32_t>(cap3 ? cap3 : 0);
        hdr->cap4 = static_cast<uint32_t>(cap4 ? cap4 : 0);
        hdr->mask2 = static_cast<uint32_t>(mask2);
        hdr->mask3 = static_cast<uint32_t>(mask3);
        hdr->mask4 = static_cast<uint32_t>(mask4);
        init = true;
    }

    uint8_t* p = reinterpret_cast<uint8_t*>(base) + sizeof(PCShmHeader);
    tag2 = reinterpret_cast<std::atomic<uint32_t>*>(p);
    p += sizeof(uint32_t) * (cap2 ? cap2 : 1);
    tab2 = reinterpret_cast<PrefixCacheEntry*>(p);
    p += sizeof(PrefixCacheEntry) * (cap2 ? cap2 : 1);
    tag3 = (cap3 ? reinterpret_cast<std::atomic<uint32_t>*>(p) : nullptr);
    p += sizeof(uint32_t) * (cap3 ? cap3 : 0);
    tab3 = (cap3 ? reinterpret_cast<PrefixCacheEntry*>(p) : nullptr);
    p += sizeof(PrefixCacheEntry) * (cap3 ? cap3 : 0);
    valid4 = (cap4 ? reinterpret_cast<std::atomic<uint32_t>*>(p) : nullptr);
    p += sizeof(uint32_t) * (cap4 ? cap4 : 0);
    tag4 = (cap4 ? reinterpret_cast<std::atomic<uint32_t>*>(p) : nullptr);
    p += sizeof(uint32_t) * (cap4 ? cap4 : 0);
    tab4 = (cap4 ? reinterpret_cast<PrefixCacheEntry*>(p) : nullptr);

    if (init) {
        size_t c2 = (cap2 ? cap2 : 1);
        for (size_t i = 0; i < c2; ++i)
            tag2[i].store(0u, std::memory_order_relaxed);
        if (cap3) {
            for (size_t i = 0; i < cap3; ++i)
                tag3[i].store(0u, std::memory_order_relaxed);
        }
        if (cap4) {
            for (size_t i = 0; i < cap4; ++i) {
                valid4[i].store(0u, std::memory_order_relaxed);
                tag4[i].store(0u, std::memory_order_relaxed);
            }
        }
    } else {
        (void)::madvise(shm_base, shm_size, MADV_WILLNEED);
    }
    return true;
}

// Unmap and close the shared-memory resources.
void PrefixCache::unmap_shared()
{
    if (shm_base) {
        ::munmap(shm_base, shm_size);
        shm_base = nullptr;
    }
    if (shm_fd >= 0) {
        ::close(shm_fd);
        shm_fd = -1;
    }
}

// Build 2-byte prefix index from key bytes in little-endian order.
inline bool PrefixCache::build2(const uint8_t* key, int len, uint16_t& p2) const
{
    if (key == nullptr || len < 2)
        return false;
    // Little endian: key[0] is least significant
    p2 = static_cast<uint16_t>(static_cast<uint16_t>(key[0]) | (static_cast<uint16_t>(key[1]) << 8));
    return true;
}

// Build 3-byte prefix index from key bytes in little-endian order.
inline bool PrefixCache::build3(const uint8_t* key, int len, uint32_t& p3) const
{
    if (key == nullptr || len < 3)
        return false;
    // Little endian: key[0] is least significant
    p3 = static_cast<uint32_t>(static_cast<uint32_t>(key[0]) | (static_cast<uint32_t>(key[1]) << 8)
        | (static_cast<uint32_t>(key[2]) << 16));
    return true;
}

// Build 4-byte prefix index from key bytes in little-endian order.
inline bool PrefixCache::build4(const uint8_t* key, int len, uint32_t& p4) const
{
    if (key == nullptr || len < 4)
        return false;
    p4 = static_cast<uint32_t>(static_cast<uint32_t>(key[0]) | (static_cast<uint32_t>(key[1]) << 8)
        | (static_cast<uint32_t>(key[2]) << 16) | (static_cast<uint32_t>(key[3]) << 24));
    return true;
}

// Lookup the deepest available cached entry for the given key prefix.
// Returns 4, 3, 2 for hits at that depth; 0 on miss. Out param receives the cached entry.
int PrefixCache::GetDepth(const uint8_t* key, int len, PrefixCacheEntry& out) const
{
    if (cap4 > 0 && key != nullptr && len >= 4) {
        uint32_t p4 = static_cast<uint32_t>(static_cast<uint32_t>(key[0]) | (static_cast<uint32_t>(key[1]) << 8)
            | (static_cast<uint32_t>(key[2]) << 16) | (static_cast<uint32_t>(key[3]) << 24));
        size_t idx4 = static_cast<size_t>(p4) & mask4;
        uint32_t v = valid4[idx4].load(std::memory_order_acquire);
        if (v) {
            uint32_t t4 = tag4[idx4].load(std::memory_order_acquire);
            if (t4 == p4) {
                out = tab4[idx4];
                return 4;
            }
        }
        // fall through to 3/2 below on miss or alias
    }
    if (cap3 > 0 && key != nullptr && len >= 3) {
        // Little-endian 3-byte build
        uint32_t p3 = static_cast<uint32_t>(static_cast<uint32_t>(key[0]) | (static_cast<uint32_t>(key[1]) << 8)
            | (static_cast<uint32_t>(key[2]) << 16));
        size_t idx3 = static_cast<size_t>(p3) & mask3;
        uint32_t t3 = tag3[idx3].load(std::memory_order_acquire);
        if (t3 == (p3 + 1)) {
            out = tab3[idx3];
            return 3;
        }
        // Fall back to 2-byte table using lower 2 bytes of p3 (LE)
        uint16_t p2 = static_cast<uint16_t>(p3 & 0xFFFFu);
        size_t idx2 = static_cast<size_t>(p2) & mask2;
        uint32_t t2 = tag2[idx2].load(std::memory_order_acquire);
        if ((full2 && t2) || (!full2 && t2 == (static_cast<uint32_t>(p2) + 1))) {
            out = tab2[idx2];
            return 2;
        }
        return 0;
    }
    if (key != nullptr && len >= 2) {
        // Little-endian 2-byte build
        uint16_t p2 = static_cast<uint16_t>(static_cast<uint16_t>(key[0]) | (static_cast<uint16_t>(key[1]) << 8));
        size_t idx2 = static_cast<size_t>(p2) & mask2;
        uint32_t t2 = tag2[idx2].load(std::memory_order_acquire);
        if ((full2 && t2) || (!full2 && t2 == (static_cast<uint32_t>(p2) + 1))) {
            out = tab2[idx2];
            return 2;
        }
    }
    return 0;
}

// Insert a cache entry for all eligible depths present in the key (4->3->2).
// Uses release-ordering on tag/valid to publish after the entry body is written.
void PrefixCache::Put(const uint8_t* key, int len, const PrefixCacheEntry& in)
{
    // Insert into 4-byte table if we have at least 4 bytes
    uint32_t p4;
    if (cap4 > 0 && build4(key, len, p4)) {
        size_t idx4 = static_cast<size_t>(p4) & mask4;
        // Clear valid first to prevent readers from observing partial update
        valid4[idx4].store(0, std::memory_order_release);
        PrefixCacheEntry e = in;
        tab4[idx4] = e;
        tag4[idx4].store(p4, std::memory_order_release);
        valid4[idx4].store(1, std::memory_order_release);
        ++put_count;
    }
    // Insert into 3-byte table if we have at least 3 bytes
    uint32_t p3;
    if (cap3 > 0 && build3(key, len, p3)) {
        size_t idx3 = static_cast<size_t>(p3) & mask3;
        uint32_t old_tag = tag3[idx3].load(std::memory_order_relaxed);
        PrefixCacheEntry e = in;
        if (old_tag != 0) {
            // Preserve prior origin bits on overwrite
            e.lf_counter |= tab3[idx3].lf_counter;
        }
        // Clear tag to prevent readers from observing partial update
        tag3[idx3].store(0, std::memory_order_release);
        tab3[idx3] = e; // write body
        tag3[idx3].store(p3 + 1, std::memory_order_release); // publish
        ++put_count;
    }
    // Also insert into 2-byte table if we have at least 2 bytes
    uint16_t p2;
    if (build2(key, len, p2)) {
        size_t idx2 = static_cast<size_t>(p2) & mask2;
        uint32_t old_tag = tag2[idx2].load(std::memory_order_relaxed);
        PrefixCacheEntry e = in;
        if (old_tag != 0) {
            e.lf_counter |= tab2[idx2].lf_counter;
        }
        tag2[idx2].store(0, std::memory_order_release);
        tab2[idx2] = e;
        tag2[idx2].store(static_cast<uint32_t>(p2) + 1, std::memory_order_release);
        ++put_count;
    }
}

// Insert a cache entry at a specific prefix depth (2/3/4).
// Used by writer to seed canonical boundaries and mid-edge seeds.
void PrefixCache::PutAtDepth(const uint8_t* key, int depth, const PrefixCacheEntry& in)
{
    if (depth == 4 && cap4 > 0) {
        uint32_t p4;
        if (build4(key, 4, p4)) {
            size_t idx4 = static_cast<size_t>(p4) & mask4;
            valid4[idx4].store(0, std::memory_order_release);
            PrefixCacheEntry e = in;
            tab4[idx4] = e;
            tag4[idx4].store(p4, std::memory_order_release);
            valid4[idx4].store(1, std::memory_order_release);
            ++put_count;
        }
        return;
    }
    if (depth == 3 && cap3 > 0) {
        uint32_t p3;
        if (build3(key, 3, p3)) {
            size_t idx3 = static_cast<size_t>(p3) & mask3;
            uint32_t old_tag = tag3[idx3].load(std::memory_order_relaxed);
            PrefixCacheEntry e = in;
            if (old_tag != 0)
                e.lf_counter |= tab3[idx3].lf_counter;
            tag3[idx3].store(0, std::memory_order_release);
            tab3[idx3] = e;
            tag3[idx3].store(p3 + 1, std::memory_order_release);
            ++put_count;
        }
        return;
    }
    if (depth == 2) {
        uint16_t p2;
        if (build2(key, 2, p2)) {
            size_t idx2 = static_cast<size_t>(p2) & mask2;
            uint32_t old_tag = tag2[idx2].load(std::memory_order_relaxed);
            PrefixCacheEntry e = in;
            if (old_tag != 0)
                e.lf_counter |= tab2[idx2].lf_counter;
            tag2[idx2].store(0, std::memory_order_release);
            tab2[idx2] = e;
            tag2[idx2].store(static_cast<uint32_t>(p2) + 1, std::memory_order_release);
            ++put_count;
        }
        return;
    }
}

// Total number of populated entries across all tables (best-effort, no locking).
size_t PrefixCache::Size() const { return Size2() + Size3() + Size4(); }

// Count populated slots in the 2-byte table.
size_t PrefixCache::Size2() const
{
    size_t cnt = 0;
    size_t cap2_eff = (cap2 ? cap2 : 1);
    for (size_t i = 0; i < cap2_eff; ++i)
        cnt += (tag2 && tag2[i] != 0);
    return cnt;
}

// Count populated slots in the 3-byte table.
size_t PrefixCache::Size3() const
{
    size_t cnt = 0;
    for (size_t i = 0; i < cap3; ++i)
        cnt += (tag3 && tag3[i] != 0);
    return cnt;
}

// Clear all tables by resetting tag/valid arrays (entries remain but are ignored).
void PrefixCache::Clear()
{
    size_t c2 = (cap2 ? cap2 : 1);
    for (size_t i = 0; i < c2; ++i)
        tag2[i].store(0u, std::memory_order_relaxed);
    if (cap3) {
        for (size_t i = 0; i < cap3; ++i)
            tag3[i].store(0u, std::memory_order_relaxed);
    }
    if (cap4) {
        for (size_t i = 0; i < cap4; ++i) {
            valid4[i].store(0u, std::memory_order_relaxed);
            tag4[i].store(0u, std::memory_order_relaxed);
        }
    }
}

// Count populated slots in the 4-byte table using the valid[] bitmap.
size_t PrefixCache::Size4() const
{
    size_t cnt = 0;
    for (size_t i = 0; i < cap4; ++i)
        cnt += (valid4 && valid4[i] != 0);
    return cnt;
}

// Report memory used by the 2-byte table (tags + entries).
size_t PrefixCache::Memory2() const
{
    size_t c2 = (cap2 ? cap2 : 1);
    return c2 * (sizeof(uint32_t) + sizeof(PrefixCacheEntry));
}

// Report memory used by the 3-byte table (tags + entries).
size_t PrefixCache::Memory3() const
{
    size_t c3 = cap3;
    return c3 * (sizeof(uint32_t) + sizeof(PrefixCacheEntry));
}

// Report memory used by the 4-byte table (valid + tags + entries).
size_t PrefixCache::Memory4() const
{
    size_t c4 = cap4;
    return c4 * (sizeof(uint32_t) /*valid*/ + sizeof(uint32_t) /*tag*/ + sizeof(PrefixCacheEntry));
}

// Destructor: unmap shared memory if mapped.
PrefixCache::~PrefixCache()
{
    if (shm_base) {
        unmap_shared();
    }
}

} // namespace mabain
