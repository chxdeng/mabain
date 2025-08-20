/**
 * Prefix cache implementation
 */

#include "util/prefix_cache.h"
#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace mabain {

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

// Shared-mapped constructor
PrefixCache::PrefixCache(const std::string& mbdir, size_t capacity)
    : cap2(0)
    , cap3(0)
{
    size_t norm_cap = std::min<size_t>(capacity, (1u << 24));
    size_t base2 = std::min<size_t>(norm_cap, (size_t)65536);
    size_t c2 = floor_pow2(base2);
    if (norm_cap > 0 && c2 < 16384)
        c2 = 16384;
    const_cast<size_t&>(cap2) = c2;
    const_cast<size_t&>(cap3) = cap3_from_capacity(norm_cap);

    mask2 = (cap2 ? cap2 - 1 : 0);
    mask3 = (cap3 ? cap3 - 1 : 0);
    full2 = (cap2 == 65536);
    shm_path = PrefixCache::ShmPath(mbdir);
    if (!map_shared(shm_path)) {
        // fallback to in-process arrays
        size_t cap2_eff = (cap2 ? cap2 : 1);
        tag2 = new std::atomic<uint32_t>[cap2_eff]();
        tab2 = new PrefixCacheEntry[cap2_eff]();
        if (cap3 > 0) {
            tag3 = new std::atomic<uint32_t>[cap3]();
            tab3 = new PrefixCacheEntry[cap3]();
        } else {
            tag3 = nullptr;
            tab3 = nullptr;
        }
    }
}

struct PCShmHeader {
    uint32_t magic;
    uint16_t version;
    uint16_t reserved;
    uint32_t cap2;
    uint32_t cap3;
    uint32_t mask2;
    uint32_t mask3;
};

bool PrefixCache::map_shared(const std::string& path)
{
    const uint32_t MAGIC = 0x50434632; // 'PCF2'
    const uint16_t VER = 1;
    size_t t2 = sizeof(PrefixCacheEntry) * (cap2 ? cap2 : 1);
    size_t g2 = sizeof(uint32_t) * (cap2 ? cap2 : 1);
    size_t t3 = sizeof(PrefixCacheEntry) * (cap3 ? cap3 : 1);
    size_t g3 = sizeof(uint32_t) * (cap3 ? cap3 : 1);
    size_t need = sizeof(PCShmHeader) + g2 + t2 + g3 + t3;

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
        hdr->mask2 = static_cast<uint32_t>(mask2);
        hdr->mask3 = static_cast<uint32_t>(mask3);
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

    if (init) {
        std::memset(tag2, 0, sizeof(uint32_t) * (cap2 ? cap2 : 1));
        if (cap3)
            std::memset(tag3, 0, sizeof(uint32_t) * cap3);
    } else {
        (void)::madvise(shm_base, shm_size, MADV_WILLNEED);
    }
    return true;
}

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

// Build 2-byte prefix index from key bytes in little-endian order
inline bool PrefixCache::build2(const uint8_t* key, int len, uint16_t& p2) const
{
    if (key == nullptr || len < 2)
        return false;
    // Little endian: key[0] is least significant
    p2 = static_cast<uint16_t>(static_cast<uint16_t>(key[0]) | (static_cast<uint16_t>(key[1]) << 8));
    return true;
}

// Build 3-byte prefix index from key bytes in little-endian order
inline bool PrefixCache::build3(const uint8_t* key, int len, uint32_t& p3) const
{
    if (key == nullptr || len < 3)
        return false;
    // Little endian: key[0] is least significant
    p3 = static_cast<uint32_t>(static_cast<uint32_t>(key[0]) | (static_cast<uint32_t>(key[1]) << 8)
        | (static_cast<uint32_t>(key[2]) << 16));
    return true;
}

int PrefixCache::GetDepth(const uint8_t* key, int len, PrefixCacheEntry& out) const
{
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

void PrefixCache::Put(const uint8_t* key, int len, const PrefixCacheEntry& in)
{
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

size_t PrefixCache::Size() const { return Size2() + Size3(); }

size_t PrefixCache::Size2() const
{
    size_t cnt = 0;
    size_t cap2_eff = (cap2 ? cap2 : 1);
    for (size_t i = 0; i < cap2_eff; ++i)
        cnt += (tag2 && tag2[i] != 0);
    return cnt;
}

size_t PrefixCache::Size3() const
{
    size_t cnt = 0;
    for (size_t i = 0; i < cap3; ++i)
        cnt += (tag3 && tag3[i] != 0);
    return cnt;
}

void PrefixCache::Clear()
{
    std::memset(tag2, 0, sizeof(uint32_t) * (cap2 ? cap2 : 1));
    if (cap3)
        std::memset(tag3, 0, sizeof(uint32_t) * cap3);
}

PrefixCache::~PrefixCache()
{
    if (shm_base) {
        unmap_shared();
    } else {
        delete[] tag2;
        tag2 = nullptr;
        delete[] tab2;
        tab2 = nullptr;
        delete[] tag3;
        tag3 = nullptr;
        delete[] tab3;
        tab3 = nullptr;
    }
}

} // namespace mabain
