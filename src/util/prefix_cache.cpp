/**
 * Prefix cache implementation
 */

#include "util/prefix_cache.h"
#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>

namespace mabain {

static inline size_t floor_pow2(size_t x) {
    if (x == 0) return 0;
    // Round down to the highest power of two <= x
    size_t p = 1;
    while ((p << 1) && ((p << 1) <= x)) p <<= 1;
    return p;
}

static inline size_t cap3_from_capacity(size_t capacity)
{
    // Allocate 3-byte table only for capacity in excess of 2-byte coverage (65536)
    if (capacity <= 65536) return 0;
    size_t excess = capacity - 65536;
    size_t capped = std::min<size_t>(excess, (1u << 24));
    size_t p2 = floor_pow2(capped);
    return p2; // may be 0 if excess < 1
}

PrefixCache::PrefixCache(size_t capacity)
    : cap2(0)
    , cap3(0)
{
    // Validation: capacity cannot exceed total number of 3-byte prefixes (2^24)
    size_t norm_cap = std::min<size_t>(capacity, (1u << 24));

    size_t base2 = std::min<size_t>(norm_cap, (size_t)65536);
    size_t c2 = floor_pow2(base2);
    if (norm_cap > 0 && c2 < 16384) c2 = 16384; // power-of-two minimum
    const_cast<size_t&>(cap2) = c2;
    const_cast<size_t&>(cap3) = cap3_from_capacity(norm_cap);

    // allocate vectors and bind pointers
    if (cap2 == 0) {
        table2_vec.resize(1);
        tag2_vec.assign(1, 0);
        mask2 = 0;
        full2 = false;
    } else {
        table2_vec.resize(cap2);
        tag2_vec.assign(cap2, 0);
        mask2 = cap2 - 1;
        full2 = (cap2 == 65536);
    }
    if (cap3 > 0) {
        table3_vec.resize(cap3);
        tag3_vec.assign(cap3, 0);
        mask3 = cap3 - 1;
    } else {
        mask3 = 0;
    }
    tab2 = table2_vec.data();
    tag2 = tag2_vec.data();
    tab3 = cap3 ? table3_vec.data() : nullptr;
    tag3 = cap3 ? tag3_vec.data() : nullptr;
}

// Shared-mapped constructor
PrefixCache::PrefixCache(const std::string& mbdir, bool use_shared, bool writer_mode, size_t capacity)
    : cap2(0)
    , cap3(0)
{
    size_t norm_cap = std::min<size_t>(capacity, (1u << 24));
    size_t base2 = std::min<size_t>(norm_cap, (size_t)65536);
    size_t c2 = floor_pow2(base2);
    if (norm_cap > 0 && c2 < 16384) c2 = 16384;
    const_cast<size_t&>(cap2) = c2;
    const_cast<size_t&>(cap3) = cap3_from_capacity(norm_cap);

    mask2 = (cap2 ? cap2 - 1 : 0);
    mask3 = (cap3 ? cap3 - 1 : 0);
    full2 = (cap2 == 65536);
    use_shared_mem = use_shared;
    writer_mode_ = writer_mode;
    if (use_shared_mem) {
        shm_path = PrefixCache::ShmPath(mbdir);
        if (!map_shared(shm_path)) {
            // Fallback to local vectors if mapping fails
            use_shared_mem = false;
        }
    }
    if (!use_shared_mem) {
        // fallback to in-process vectors
        if (cap2 == 0) {
            table2_vec.resize(1);
            tag2_vec.assign(1, 0);
        } else {
            table2_vec.resize(cap2);
            tag2_vec.assign(cap2, 0);
        }
        if (cap3 > 0) {
            table3_vec.resize(cap3);
            tag3_vec.assign(cap3, 0);
        }
        tab2 = table2_vec.data();
        tag2 = tag2_vec.data();
        tab3 = cap3 ? table3_vec.data() : nullptr;
        tag3 = cap3 ? tag3_vec.data() : nullptr;
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

    int oflags = writer_mode_ ? (O_CREAT | O_RDWR) : O_RDWR;
    int fd = ::open(path.c_str(), oflags, 0666);
    if (fd < 0) return false;

    if (writer_mode_) {
        if (ftruncate(fd, need) != 0) { ::close(fd); return false; }
    } else {
        struct stat st;
        if (fstat(fd, &st) != 0) { ::close(fd); return false; }
        if ((size_t)st.st_size < need) { ::close(fd); return false; }
    }

    // Allow readers to also write to the shared mapping so that read-time
    // prepopulation can insert entries into the shared cache.
    int prot = PROT_READ | PROT_WRITE;
    void* base = ::mmap(nullptr, need, prot, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) { ::close(fd); return false; }

    shm_base = base;
    shm_size = need;
    shm_fd = fd;

    auto* hdr = reinterpret_cast<PCShmHeader*>(base);
    if (writer_mode_) {
        hdr->magic = MAGIC;
        hdr->version = VER;
        hdr->reserved = 0;
        hdr->cap2 = static_cast<uint32_t>(cap2 ? cap2 : 1);
        hdr->cap3 = static_cast<uint32_t>(cap3 ? cap3 : 0);
        hdr->mask2 = static_cast<uint32_t>(mask2);
        hdr->mask3 = static_cast<uint32_t>(mask3);
    } else {
        if (hdr->magic != MAGIC || hdr->version != VER) {
            ::munmap(shm_base, shm_size); ::close(fd); shm_base=nullptr; return false;
        }
        // Trust reader-view of caps/masks already computed from ctor
    }

    uint8_t* p = reinterpret_cast<uint8_t*>(base) + sizeof(PCShmHeader);
    tag2 = reinterpret_cast<uint32_t*>(p); p += sizeof(uint32_t) * (cap2 ? cap2 : 1);
    tab2 = reinterpret_cast<PrefixCacheEntry*>(p); p += sizeof(PrefixCacheEntry) * (cap2 ? cap2 : 1);
    tag3 = (cap3 ? reinterpret_cast<uint32_t*>(p) : nullptr); p += sizeof(uint32_t) * (cap3 ? cap3 : 0);
    tab3 = (cap3 ? reinterpret_cast<PrefixCacheEntry*>(p) : nullptr);

    if (writer_mode_) {
        // Initialize tags to zero on first create
        // Note: simple heuristic; if file existed we still clear
        std::memset(tag2, 0, sizeof(uint32_t) * (cap2 ? cap2 : 1));
        if (cap3) std::memset(tag3, 0, sizeof(uint32_t) * cap3);
    } else {
        // Reader: proactively fault in the mapping to avoid first-lookup stalls
        // when benchmarks time the first pass. This does not change semantics
        // and only hints the kernel to prefetch/cache the mapped pages.
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

inline bool PrefixCache::build2(const uint8_t* key, int len, uint16_t& p2) const
{
    if (key == nullptr || len < 2)
        return false;
    // Big endian: key[0] is most significant
    p2 = static_cast<uint16_t>((static_cast<uint16_t>(key[0]) << 8) |
                               static_cast<uint16_t>(key[1]));
    return true;
}

inline bool PrefixCache::build3(const uint8_t* key, int len, uint32_t& p3) const
{
    if (key == nullptr || len < 3)
        return false;
    p3 = (static_cast<uint32_t>(key[0]) << 16) |
         (static_cast<uint32_t>(key[1]) << 8)  |
         (static_cast<uint32_t>(key[2]));
    return true;
}

bool PrefixCache::Get(const uint8_t* key, int len, PrefixCacheEntry& out) const
{
    uint32_t p3;
    if (cap3 > 0 && build3(key, len, p3)) {
        size_t idx3 = static_cast<size_t>(p3) & mask3;
        uint32_t t3 = tag3[idx3];
        if (t3 && ((!use_shared_mem && fast_no_tag_check) || t3 == (p3 + 1))) {
            out = tab3[idx3];
            ++hit_count;
            return true;
        }
        // fall through to try 2-byte table (derive p2)
        uint16_t p2 = static_cast<uint16_t>(p3 >> 8);
        size_t idx2 = static_cast<size_t>(p2) & mask2;
        uint32_t t2 = tag2[idx2];
        if ((full2 && t2) || (!full2 && t2 == (static_cast<uint32_t>(p2) + 1))) {
            out = tab2[idx2];
            ++hit_count;
            return true;
        }
        ++miss_count;
        return false;
    }
    uint16_t p2;
    if (build2(key, len, p2)) {
        size_t idx2 = static_cast<size_t>(p2) & mask2;
        uint32_t t2 = tag2[idx2];
        if ((full2 && t2) || (!full2 && t2 == (static_cast<uint32_t>(p2) + 1))) {
            out = tab2[idx2];
            ++hit_count;
            return true;
        }
    }
    ++miss_count;
    return false;
}

int PrefixCache::GetDepth(const uint8_t* key, int len, PrefixCacheEntry& out) const
{
    uint32_t p3;
    if (cap3 > 0 && build3(key, len, p3)) {
        size_t idx3 = static_cast<size_t>(p3) & mask3;
        uint32_t t3 = tag3[idx3];
        if (t3 && ((!use_shared_mem && fast_no_tag_check) || t3 == (p3 + 1))) {
            out = tab3[idx3];
            ++hit_count;
            return 3;
        }
        // fall through to 2-byte table
    }
    uint16_t p2;
    if (build2(key, len, p2)) {
        size_t idx2 = static_cast<size_t>(p2) & mask2;
        uint32_t t2 = tag2[idx2];
        if ((full2 && t2) || (!full2 && t2 == (static_cast<uint32_t>(p2) + 1))) {
            out = tab2[idx2];
            ++hit_count;
            return 2;
        }
    }
    ++miss_count;
    return 0;
}

void PrefixCache::Put(const uint8_t* key, int len, const PrefixCacheEntry& in)
{
    // Insert into 3-byte table if we have at least 3 bytes
    uint32_t p3;
    if (cap3 > 0 && build3(key, len, p3)) {
        size_t idx3 = static_cast<size_t>(p3) & mask3;
        uint32_t old_tag = tag3[idx3];
        PrefixCacheEntry e = in;
        if (old_tag != 0) {
            // Preserve prior origin bits on overwrite
            e.lf_counter |= tab3[idx3].lf_counter;
        }
        tab3[idx3] = e;      // write body first
        tag3[idx3] = p3 + 1;  // then publish tag
        ++put_count;
    }
    // Also insert into 2-byte table if we have at least 2 bytes
    uint16_t p2;
    if (build2(key, len, p2)) {
        size_t idx2 = static_cast<size_t>(p2) & mask2;
        uint32_t old_tag = tag2[idx2];
        PrefixCacheEntry e = in;
        if (old_tag != 0) {
            e.lf_counter |= tab2[idx2].lf_counter;
        }
        tab2[idx2] = e;
        tag2[idx2] = static_cast<uint32_t>(p2) + 1;
        ++put_count;
    }
}

size_t PrefixCache::Size() const { return Size2() + Size3(); }

size_t PrefixCache::Size2() const
{
    if (!use_shared_mem) return static_cast<size_t>(tag2_vec.size() - std::count(tag2_vec.begin(), tag2_vec.end(), 0));
    // shared: scan tags
    size_t cnt = 0;
    for (size_t i = 0; i < (cap2 ? cap2 : 1); ++i) cnt += (tag2[i] != 0);
    return cnt;
}

size_t PrefixCache::Size3() const
{
    if (!use_shared_mem) {
        if (cap3 == 0) return 0;
        return static_cast<size_t>(tag3_vec.size() - std::count(tag3_vec.begin(), tag3_vec.end(), 0));
    }
    size_t cnt = 0;
    for (size_t i = 0; i < cap3; ++i) cnt += (tag3 && tag3[i] != 0);
    return cnt;
}

void PrefixCache::Clear()
{
    if (use_shared_mem) {
        if (writer_mode_) {
            std::memset(tag2, 0, sizeof(uint32_t) * (cap2 ? cap2 : 1));
            if (cap3) std::memset(tag3, 0, sizeof(uint32_t) * cap3);
        }
    } else {
        std::fill(tag2_vec.begin(), tag2_vec.end(), 0);
        if (cap3) std::fill(tag3_vec.begin(), tag3_vec.end(), 0);
    }
    size2 = 0;
    size3 = 0;
}

PrefixCache::~PrefixCache()
{
    if (use_shared_mem) unmap_shared();
}

void PrefixCache::CountOrigin(size_t& add_entries, size_t& read_entries) const
{
    add_entries = 0;
    read_entries = 0;
    // 2-byte table
    size_t cap2_eff = (cap2 ? cap2 : 1);
    for (size_t i = 0; i < cap2_eff; ++i) {
        uint32_t t = tag2[i];
        if (t != 0) {
            uint32_t lf = tab2[i].lf_counter;
            if (lf & 1u) ++add_entries;
            if (lf & 2u) ++read_entries;
        }
    }
    // 3-byte table
    for (size_t i = 0; i < cap3; ++i) {
        if (cap3 == 0) break;
        uint32_t t = tag3 ? tag3[i] : 0;
        if (t != 0) {
            uint32_t lf = tab3[i].lf_counter;
            if (lf & 1u) ++add_entries;
            if (lf & 2u) ++read_entries;
        }
    }
}

} // namespace mabain
