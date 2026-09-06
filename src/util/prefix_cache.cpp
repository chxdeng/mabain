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

// Construct a prefix cache backed by an embedded mapping in _mabain_d.
//
// Embedded Region Layout (in block 0 of _mabain_d):
//   PCShmHeader | root_epoch | prefix2_epoch |
//   tag2 | tab2 | tag3 | tab3 | valid4 | tag4 | tab4
// where:
//   - PCShmHeader carries magic/version and fixed capacities/masks for tables.
//   - tag2/tag3 hold (prefix+1) to distinguish empty from p == 0.
//   - 4-byte table uses a separate valid[] bitmap and exact tag4 values to avoid
//     overflow; publish protocol clears valid, writes body+tag, then sets valid.
//
// Publication/reads use release/acquire on the atomic tag/valid arrays and a
// per-entry optimistic counter so readers can reject an entry changed while
// its non-atomic body was being copied.
PrefixCache::PrefixCache(const std::string& mbdir, const IndexHeader* hdr, size_t capacity)
    : cap2(0)
    , cap3(0)
    , cap4(0)
{
    hdr_ = hdr;
    // Allow capacity beyond 2^24 to provision a sparse 4-byte table.
    size_t norm_cap = capacity;
    size_t base2 = std::min<size_t>(norm_cap, (size_t)65536);
    size_t c2 = floor_pow2(base2);
    if (norm_cap > 0 && c2 < 16384)
        c2 = 16384;
    // Require embedded region; readers will not map external files anymore.
    if (hdr_ && hdr_->pfxcache_size > 0 && hdr_->pfx_cap2) {
        const_cast<size_t&>(cap2) = hdr_->pfx_cap2;
    } else {
        // No embedded cache: disable completely.
        const_cast<size_t&>(cap2) = 0;
        const_cast<size_t&>(cap3) = 0;
        const_cast<size_t&>(cap4) = 0;
        return; // leave uninitialized; ActivePrefixCache() will stay null
    }
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
    if (hdr_ && hdr_->pfxcache_size > 0) {
        if (hdr_->pfx_cap3)
            c3 = hdr_->pfx_cap3;
        else
            c3 = 0;
        if (hdr_->pfx_cap4)
            c4 = hdr_->pfx_cap4;
        else
            c4 = 0;
    }
    const_cast<size_t&>(cap3) = c3;
    const_cast<size_t&>(cap4) = c4;

    mask2 = (cap2 ? cap2 - 1 : 0);
    mask3 = (cap3 ? cap3 - 1 : 0);
    mask4 = (cap4 ? cap4 - 1 : 0);
    full2 = (cap2 == 65536);
    // Embedded mapping only
    if (hdr_ && hdr_->pfxcache_size > 0) {
        if (!map_embedded(mbdir))
            throw std::runtime_error("PrefixCache: failed to map embedded cache");
    } else {
        throw std::runtime_error("PrefixCache: no embedded cache available");
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
    uint32_t global_epoch;
    // Keep the following PrefixCacheEntry array 8-byte aligned.
    uint32_t reserved2;
};

static_assert(sizeof(PCShmHeader) == 40,
    "Unexpected shared prefix-cache header layout");
static_assert(sizeof(PCShmHeader) % alignof(PrefixCacheEntry) == 0,
    "Shared prefix-cache tables must remain naturally aligned");

// Create or open the shared-memory file and map the cache arrays.
// Initializes tags/valids on first creation; otherwise issues MADV_WILLNEED.
bool PrefixCache::map_shared(const std::string& path)
{
    const uint32_t MAGIC = 0x50434632; // 'PCF2'
    const uint16_t VER = 3; // folded indexes and hierarchical invalidation
    size_t er = sizeof(uint32_t) * ROOT_EPOCH_COUNT;
    size_t e2 = sizeof(uint32_t) * PREFIX2_EPOCH_COUNT;
    size_t t2 = sizeof(PrefixCacheEntry) * (cap2 ? cap2 : 1);
    size_t g2 = sizeof(uint32_t) * (cap2 ? cap2 : 1);
    size_t t3 = sizeof(PrefixCacheEntry) * (cap3 ? cap3 : 1);
    size_t g3 = sizeof(uint32_t) * (cap3 ? cap3 : 1);
    size_t t4 = sizeof(PrefixCacheEntry) * (cap4 ? cap4 : 1);
    size_t g4 = sizeof(uint32_t) * (cap4 ? cap4 : 1); // tag4
    size_t v4 = sizeof(uint32_t) * (cap4 ? cap4 : 1); // valid4
    size_t need = sizeof(PCShmHeader) + er + e2
        + g2 + t2 + g3 + t3 + v4 + g4 + t4;

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
        hdr->global_epoch = 0;
        hdr->reserved2 = 0;
        init = true;
    }

    uint8_t* p = reinterpret_cast<uint8_t*>(base) + sizeof(PCShmHeader);
    global_epoch = &hdr->global_epoch;
    root_epoch = reinterpret_cast<uint32_t*>(p);
    p += er;
    prefix2_epoch = reinterpret_cast<uint32_t*>(p);
    p += e2;
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
        __atomic_store_n(global_epoch, 0u, __ATOMIC_RELAXED);
        for (size_t i = 0; i < ROOT_EPOCH_COUNT; ++i)
            __atomic_store_n(&root_epoch[i], 0u, __ATOMIC_RELAXED);
        for (size_t i = 0; i < PREFIX2_EPOCH_COUNT; ++i)
            __atomic_store_n(&prefix2_epoch[i], 0u, __ATOMIC_RELAXED);
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

// Map the embedded cache region inside _mabain_d.
// We map the whole first block if necessary and then set pointers at the HDR offset.
bool PrefixCache::map_embedded(const std::string& mbdir)
{
    // Resolve data file block0 path and open.
    std::string data_path0 = mbdir + std::string("_mabain_d0");
    // Create the first data block file if it does not exist yet
    int fd = ::open(data_path0.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd < 0) return false;
    struct stat st;
    if (fstat(fd, &st) != 0) { ::close(fd); return false; }
    // Ensure file is at least large enough to cover the cache region
    size_t end_needed = hdr_->pfxcache_offset + hdr_->pfxcache_size;
    if ((size_t)st.st_size < end_needed) {
        // Prefer posix_fallocate to avoid sparse files; fall back to ftruncate if unavailable
        int rc = 0;
#ifdef _XOPEN_SOURCE
        rc = posix_fallocate(fd, 0, static_cast<off_t>(end_needed));
#endif
        if (rc != 0) {
            if (ftruncate(fd, static_cast<off_t>(end_needed)) != 0) { ::close(fd); return false; }
        }
    }
    size_t map_len = std::max<size_t>(end_needed, (size_t)st.st_size);
    void* base = ::mmap(nullptr, map_len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) { ::close(fd); return false; }

    shm_base = base;
    shm_size = map_len;
    shm_delta = hdr_->pfxcache_offset;
    shm_fd = fd;

    // Recreate the same header and tables layout offsets as map_shared, but relative to base+delta
    const uint32_t MAGIC = 0x50434632; // 'PCF2'
    const uint16_t VER = 3; // folded indexes and hierarchical invalidation
    size_t er = sizeof(uint32_t) * ROOT_EPOCH_COUNT;
    size_t e2 = sizeof(uint32_t) * PREFIX2_EPOCH_COUNT;
    size_t t2 = sizeof(PrefixCacheEntry) * (cap2 ? cap2 : 1);
    size_t g2 = sizeof(uint32_t) * (cap2 ? cap2 : 1);
    size_t t3 = sizeof(PrefixCacheEntry) * (cap3 ? cap3 : 1);
    size_t g3 = sizeof(uint32_t) * (cap3 ? cap3 : 1);
    size_t t4 = sizeof(PrefixCacheEntry) * (cap4 ? cap4 : 1);
    size_t g4 = sizeof(uint32_t) * (cap4 ? cap4 : 1); // tag4
    size_t v4 = sizeof(uint32_t) * (cap4 ? cap4 : 1); // valid4
    size_t need = sizeof(PCShmHeader) + er + e2
        + g2 + t2 + g3 + t3 + v4 + g4 + t4;
    if (need != hdr_->pfxcache_size) {
        // Capacity mismatch vs header; clear mapped region and try to proceed if space allows
        // to keep running. Readers/writers can still function without cache.
        ::munmap(shm_base, shm_size);
        ::close(fd);
        shm_base = nullptr;
        shm_fd = -1;
        return false;
    }

    auto* hdr = reinterpret_cast<PCShmHeader*>((uint8_t*)base + shm_delta);
    bool init = (hdr->magic != MAGIC || hdr->version != VER);
    if (init) {
        hdr->magic = MAGIC;
        hdr->version = VER;
        hdr->reserved = 0;
        hdr->cap2 = static_cast<uint32_t>(cap2 ? cap2 : 1);
        hdr->cap3 = static_cast<uint32_t>(cap3 ? cap3 : 0);
        hdr->cap4 = static_cast<uint32_t>(cap4 ? cap4 : 0);
        hdr->mask2 = static_cast<uint32_t>(mask2);
        hdr->mask3 = static_cast<uint32_t>(mask3);
        hdr->mask4 = static_cast<uint32_t>(mask4);
        hdr->global_epoch = 0;
        hdr->reserved2 = 0;
    }
    uint8_t* p = reinterpret_cast<uint8_t*>(hdr) + sizeof(PCShmHeader);
    global_epoch = &hdr->global_epoch;
    root_epoch = reinterpret_cast<uint32_t*>(p);
    p += er;
    prefix2_epoch = reinterpret_cast<uint32_t*>(p);
    p += e2;
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
        __atomic_store_n(global_epoch, 0u, __ATOMIC_RELAXED);
        for (size_t i = 0; i < ROOT_EPOCH_COUNT; ++i)
            __atomic_store_n(&root_epoch[i], 0u, __ATOMIC_RELAXED);
        for (size_t i = 0; i < PREFIX2_EPOCH_COUNT; ++i)
            __atomic_store_n(&prefix2_epoch[i], 0u, __ATOMIC_RELAXED);
        size_t c2 = (cap2 ? cap2 : 1);
        for (size_t i = 0; i < c2; ++i) tag2[i].store(0u, std::memory_order_relaxed);
        if (cap3) for (size_t i = 0; i < cap3; ++i) tag3[i].store(0u, std::memory_order_relaxed);
        if (cap4) {
            for (size_t i = 0; i < cap4; ++i) { valid4[i].store(0u, std::memory_order_relaxed); tag4[i].store(0u, std::memory_order_relaxed); }
        }
    } else {
        (void)::madvise((uint8_t*)base + shm_delta, need, MADV_WILLNEED);
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

namespace {

constexpr uint32_t PFX_ORIGIN_MASK = 0x3u;
constexpr uint32_t PFX_COUNTER_STEP = 0x4u;
constexpr uint32_t PFX_COUNTER_MASK = 0x7FFFFFFCu;
constexpr uint32_t PFX_UPDATE_ACTIVE = 0x80000000u;

// Fold the upper prefix bytes into the low table-index bits. Unlike a
// multiply-heavy avalanche hash, this leaves 16-bit integer keys unchanged
// while distributing fixed-leading-byte string prefixes across sparse tables.
inline uint32_t FoldPrefix(uint32_t value)
{
    return value ^ (value >> 16);
}

inline uint32_t LoadEntryCounter(const PrefixCacheEntry& entry)
{
    return __atomic_load_n(&entry.lf_counter, __ATOMIC_ACQUIRE);
}

inline bool EntryUpdateActive(uint32_t counter)
{
    return (counter & PFX_UPDATE_ACTIVE) != 0;
}

inline uint32_t BeginEntryUpdate(PrefixCacheEntry& entry, uint32_t origin_flags,
    bool preserve_origin)
{
    uint32_t old_counter = LoadEntryCounter(entry);
    if (preserve_origin)
        origin_flags |= old_counter & PFX_ORIGIN_MASK;

    uint32_t generation = ((old_counter & PFX_COUNTER_MASK) + PFX_COUNTER_STEP)
        & PFX_COUNTER_MASK;
    uint32_t stable_counter = generation | (origin_flags & PFX_ORIGIN_MASK);

    // The acquire side of the exchange keeps subsequent body stores after the
    // active marker; the final release store publishes the completed body.
    __atomic_exchange_n(&entry.lf_counter, stable_counter | PFX_UPDATE_ACTIVE,
        __ATOMIC_ACQ_REL);
    return stable_counter;
}

inline void FinishEntryUpdate(PrefixCacheEntry& entry, uint32_t stable_counter)
{
    __atomic_store_n(&entry.lf_counter, stable_counter, __ATOMIC_RELEASE);
}

inline void CopyEntryBody(PrefixCacheEntry& dst, const PrefixCacheEntry& src)
{
    // `lf_counter` is the final field. Copy the contiguous payload in one
    // operation while leaving the active generation marker untouched.
    memcpy(&dst, &src, offsetof(PrefixCacheEntry, lf_counter));
}

inline bool CopyStableEntry(const PrefixCacheEntry& src, uint32_t before,
    PrefixCacheEntry& out)
{
    // Intentional Mabain optimistic snapshot protocol, matching
    // LockFree::ReaderLockFreeStart/Stop: copy the mmap-backed payload, then
    // accept it only if the atomic generation is unchanged and inactive.
    // This relies on Mabain's validated GCC/Linux x86-64 ordering rather than
    // providing a general ISO C++ synchronization primitive. A port to another
    // compiler or architecture must validate both protocols together.
    // See prefix_cache_snapshot_concurrency_test.cpp.
    PrefixCacheEntry candidate {};
    CopyEntryBody(candidate, src);

    // Keep the body copy before the final validation load. This is Mabain's
    // existing optimistic read pattern: consume the copy only when unchanged.
    std::atomic_thread_fence(std::memory_order_acquire);
    uint32_t after = LoadEntryCounter(src);
    if (before != after || EntryUpdateActive(after))
        return false;

    candidate.lf_counter = after & PFX_ORIGIN_MASK;
    out = candidate;
    return true;
}

} // namespace

uint32_t PrefixCache::CurrentEpoch(const uint8_t* key, int len) const
{
    if (key == nullptr || len < 2 || global_epoch == nullptr
        || root_epoch == nullptr || prefix2_epoch == nullptr)
        return 0;

    uint16_t p2;
    if (!build2(key, len, p2))
        return 0;

    // Counters only increase. Their modulo-2^32 sum changes whenever any
    // applicable invalidation scope advances, without enlarging cache entries.
    return __atomic_load_n(global_epoch, __ATOMIC_ACQUIRE)
        + __atomic_load_n(&root_epoch[key[0]], __ATOMIC_ACQUIRE)
        + __atomic_load_n(&prefix2_epoch[p2], __ATOMIC_ACQUIRE);
}

void PrefixCache::InvalidatePrefix2(const uint8_t* key, int len)
{
    uint16_t p2;
    if (prefix2_epoch != nullptr && build2(key, len, p2))
        (void)__atomic_add_fetch(&prefix2_epoch[p2], 1u, __ATOMIC_ACQ_REL);
}

void PrefixCache::InvalidateRoot(uint8_t first_byte)
{
    if (root_epoch != nullptr)
        (void)__atomic_add_fetch(&root_epoch[first_byte], 1u, __ATOMIC_ACQ_REL);
}

void PrefixCache::InvalidateAll()
{
    if (global_epoch != nullptr)
        (void)__atomic_add_fetch(global_epoch, 1u, __ATOMIC_ACQ_REL);
}

uint32_t PrefixCache::LoadEntryCounterForTest(const PrefixCacheEntry& entry)
{
    return LoadEntryCounter(entry);
}

bool PrefixCache::CopyStableEntryForTest(const PrefixCacheEntry& src,
    uint32_t before, PrefixCacheEntry& out)
{
    return CopyStableEntry(src, before, out);
}

// Lookup the deepest available cached entry for the given key prefix.
// Returns 4, 3, 2 for stable hits; 0 on miss; UNSTABLE when a matching slot
// changed while copied. Out receives the entry only after validation succeeds.
int PrefixCache::GetDepth(const uint8_t* key, int len, PrefixCacheEntry& out) const
{
    if (cap4 > 0 && key != nullptr && len >= 4) {
        uint32_t p4 = static_cast<uint32_t>(static_cast<uint32_t>(key[0]) | (static_cast<uint32_t>(key[1]) << 8)
            | (static_cast<uint32_t>(key[2]) << 16) | (static_cast<uint32_t>(key[3]) << 24));
        size_t idx4 = static_cast<size_t>(FoldPrefix(p4)) & mask4;
        uint32_t valid = valid4[idx4].load(std::memory_order_acquire);
        if (valid && tag4[idx4].load(std::memory_order_acquire) == p4) {
            uint32_t before = LoadEntryCounter(tab4[idx4]);
            if (!EntryUpdateActive(before)) {
                if (!CopyStableEntry(tab4[idx4], before, out))
                    return UNSTABLE;
                if (!valid4[idx4].load(std::memory_order_acquire)
                    || tag4[idx4].load(std::memory_order_acquire) != p4)
                    return UNSTABLE;
                if (out.cache_epoch != CurrentEpoch(key, len))
                    return 0;
                return 4;
            }
        }
        // fall through to 3/2 below on miss or alias
    }
    if (cap3 > 0 && key != nullptr && len >= 3) {
        // Little-endian 3-byte build
        uint32_t p3 = static_cast<uint32_t>(static_cast<uint32_t>(key[0]) | (static_cast<uint32_t>(key[1]) << 8)
            | (static_cast<uint32_t>(key[2]) << 16));
        size_t idx3 = static_cast<size_t>(FoldPrefix(p3)) & mask3;
        if (tag3[idx3].load(std::memory_order_acquire) == (p3 + 1)) {
            uint32_t before3 = LoadEntryCounter(tab3[idx3]);
            if (!EntryUpdateActive(before3)) {
                if (!CopyStableEntry(tab3[idx3], before3, out))
                    return UNSTABLE;
                if (tag3[idx3].load(std::memory_order_acquire) != (p3 + 1))
                    return UNSTABLE;
                if (out.cache_epoch != CurrentEpoch(key, len))
                    return 0;
                return 3;
            }
        }
        // Fall back to 2-byte table using lower 2 bytes of p3 (LE)
        uint16_t p2 = static_cast<uint16_t>(p3 & 0xFFFFu);
        size_t idx2 = static_cast<size_t>(p2) & mask2;
        uint32_t tag = tag2[idx2].load(std::memory_order_acquire);
        if ((full2 && tag) || (!full2 && tag == (static_cast<uint32_t>(p2) + 1))) {
            uint32_t before2 = LoadEntryCounter(tab2[idx2]);
            if (!EntryUpdateActive(before2)) {
                if (!CopyStableEntry(tab2[idx2], before2, out))
                    return UNSTABLE;
                uint32_t after_tag = tag2[idx2].load(std::memory_order_acquire);
                if (!((full2 && after_tag)
                        || (!full2 && after_tag == (static_cast<uint32_t>(p2) + 1))))
                    return UNSTABLE;
                if (out.cache_epoch != CurrentEpoch(key, len))
                    return 0;
                return 2;
            }
        }
        return 0;
    }
    if (key != nullptr && len >= 2) {
        // Little-endian 2-byte build
        uint16_t p2 = static_cast<uint16_t>(static_cast<uint16_t>(key[0]) | (static_cast<uint16_t>(key[1]) << 8));
        size_t idx2 = static_cast<size_t>(p2) & mask2;
        uint32_t tag = tag2[idx2].load(std::memory_order_acquire);
        if ((full2 && tag) || (!full2 && tag == (static_cast<uint32_t>(p2) + 1))) {
            uint32_t before = LoadEntryCounter(tab2[idx2]);
            if (!EntryUpdateActive(before)) {
                if (!CopyStableEntry(tab2[idx2], before, out))
                    return UNSTABLE;
                uint32_t after_tag = tag2[idx2].load(std::memory_order_acquire);
                if (!((full2 && after_tag)
                        || (!full2 && after_tag == (static_cast<uint32_t>(p2) + 1))))
                    return UNSTABLE;
                if (out.cache_epoch != CurrentEpoch(key, len))
                    return 0;
                return 2;
            }
        }
    }
    return 0;
}

// Insert a cache entry for all eligible depths present in the key (4->3->2).
// Uses release-ordering on tag/valid to publish after the entry body is written.
void PrefixCache::Put(const uint8_t* key, int len, const PrefixCacheEntry& in)
{
    PrefixCacheEntry seeded = in;
    seeded.cache_epoch = CurrentEpoch(key, len);

    // Insert into 4-byte table if we have at least 4 bytes
    uint32_t p4;
    if (cap4 > 0 && build4(key, len, p4)) {
        size_t idx4 = static_cast<size_t>(FoldPrefix(p4)) & mask4;
        uint32_t stable_counter = BeginEntryUpdate(tab4[idx4], seeded.lf_counter,
            false);
        // Clear valid first to prevent readers from observing partial update
        valid4[idx4].store(0, std::memory_order_release);
        CopyEntryBody(tab4[idx4], seeded);
        tag4[idx4].store(p4, std::memory_order_release);
        valid4[idx4].store(1, std::memory_order_release);
        FinishEntryUpdate(tab4[idx4], stable_counter);
        ++put_count;
    }
    // Insert into 3-byte table if we have at least 3 bytes
    uint32_t p3;
    if (cap3 > 0 && build3(key, len, p3)) {
        size_t idx3 = static_cast<size_t>(FoldPrefix(p3)) & mask3;
        uint32_t old_tag = tag3[idx3].load(std::memory_order_relaxed);
        uint32_t stable_counter = BeginEntryUpdate(tab3[idx3], seeded.lf_counter,
            old_tag != 0);
        // Clear tag to prevent readers from observing partial update
        tag3[idx3].store(0, std::memory_order_release);
        CopyEntryBody(tab3[idx3], seeded);
        tag3[idx3].store(p3 + 1, std::memory_order_release); // publish
        FinishEntryUpdate(tab3[idx3], stable_counter);
        ++put_count;
    }
    // Also insert into 2-byte table if we have at least 2 bytes
    uint16_t p2;
    if (build2(key, len, p2)) {
        size_t idx2 = static_cast<size_t>(p2) & mask2;
        uint32_t old_tag = tag2[idx2].load(std::memory_order_relaxed);
        uint32_t stable_counter = BeginEntryUpdate(tab2[idx2], seeded.lf_counter,
            old_tag != 0);
        tag2[idx2].store(0, std::memory_order_release);
        CopyEntryBody(tab2[idx2], seeded);
        tag2[idx2].store(static_cast<uint32_t>(p2) + 1, std::memory_order_release);
        FinishEntryUpdate(tab2[idx2], stable_counter);
        ++put_count;
    }
}

// Insert a cache entry at a specific prefix depth (2/3/4).
// Used by writer to seed canonical boundaries and mid-edge seeds.
void PrefixCache::PutAtDepth(const uint8_t* key, int depth, const PrefixCacheEntry& in)
{
    PrefixCacheEntry seeded = in;
    seeded.cache_epoch = CurrentEpoch(key, depth);

    if (depth == 4 && cap4 > 0) {
        uint32_t p4;
        if (build4(key, 4, p4)) {
            size_t idx4 = static_cast<size_t>(FoldPrefix(p4)) & mask4;
            uint32_t stable_counter = BeginEntryUpdate(tab4[idx4], seeded.lf_counter,
                false);
            valid4[idx4].store(0, std::memory_order_release);
            CopyEntryBody(tab4[idx4], seeded);
            tag4[idx4].store(p4, std::memory_order_release);
            valid4[idx4].store(1, std::memory_order_release);
            FinishEntryUpdate(tab4[idx4], stable_counter);
            ++put_count;
        }
        return;
    }
    if (depth == 3 && cap3 > 0) {
        uint32_t p3;
        if (build3(key, 3, p3)) {
            size_t idx3 = static_cast<size_t>(FoldPrefix(p3)) & mask3;
            uint32_t old_tag = tag3[idx3].load(std::memory_order_relaxed);
            uint32_t stable_counter = BeginEntryUpdate(tab3[idx3], seeded.lf_counter,
                old_tag != 0);
            tag3[idx3].store(0, std::memory_order_release);
            CopyEntryBody(tab3[idx3], seeded);
            tag3[idx3].store(p3 + 1, std::memory_order_release);
            FinishEntryUpdate(tab3[idx3], stable_counter);
            ++put_count;
        }
        return;
    }
    if (depth == 2) {
        uint16_t p2;
        if (build2(key, 2, p2)) {
            size_t idx2 = static_cast<size_t>(p2) & mask2;
            uint32_t old_tag = tag2[idx2].load(std::memory_order_relaxed);
            uint32_t stable_counter = BeginEntryUpdate(tab2[idx2], seeded.lf_counter,
                old_tag != 0);
            tag2[idx2].store(0, std::memory_order_release);
            CopyEntryBody(tab2[idx2], seeded);
            tag2[idx2].store(static_cast<uint32_t>(p2) + 1, std::memory_order_release);
            FinishEntryUpdate(tab2[idx2], stable_counter);
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
    for (size_t i = 0; i < cap2_eff; ++i) {
        if (tag2 && tag2[i].load(std::memory_order_relaxed) != 0)
            ++cnt;
    }
    return cnt;
}

// Count populated slots in the 3-byte table.
size_t PrefixCache::Size3() const
{
    size_t cnt = 0;
    for (size_t i = 0; i < cap3; ++i) {
        if (tag3 && tag3[i].load(std::memory_order_relaxed) != 0)
            ++cnt;
    }
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
    for (size_t i = 0; i < cap4; ++i) {
        if (valid4 && valid4[i].load(std::memory_order_relaxed) != 0)
            ++cnt;
    }
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
