/**
 * Writer-managed shared prefix cache (implementation, multi-process safe)
 */
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600
#endif
#include "util/prefix_cache_shared.h"
//
#ifdef MB_HAVE_XXHASH
#include <xxhash.h>
#endif

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace mabain {

static constexpr uint32_t kMagic = 0x50F1CACE;
static constexpr uint16_t kVersion = 1;

// Ensure the backing file has fully allocated space to avoid SIGBUS on write
// when disk is full. Prefer posix_fallocate (portable); fall back to ftruncate
// only when allocation is unsupported.
static bool ensure_file_size(int fd, size_t length)
{
#if defined(__linux__)
    // Try Linux fallocate first for efficient, non-sparse allocation
    if (fallocate(fd, 0, 0, static_cast<off_t>(length)) == 0)
        return true;
    // fall back to posix_fallocate if fallocate is not supported by FS
#endif
    int rc = posix_fallocate(fd, 0, static_cast<off_t>(length));
    if (rc == 0)
        return true;
    // Do not fall back to ftruncate: sparse files risk SIGBUS on full disks
    // Only treat "not supported" as a hard failure (caller will bail out)
    return false;
}

PrefixCacheShared* PrefixCacheShared::CreateWriter(const std::string& mbdir,
    int prefix_len,
    size_t capacity,
    uint32_t associativity)
{
    if (prefix_len <= 0 || prefix_len > 8 || capacity == 0 || associativity == 0)
        return nullptr;
    auto* pc = new PrefixCacheShared();
    if (!pc->map_file(ShmPath(mbdir), /*create*/ true, prefix_len, capacity, associativity)) {
        delete pc;
        return nullptr;
    }
    return pc;
}

PrefixCacheShared* PrefixCacheShared::OpenReader(const std::string& mbdir)
{
    auto* pc = new PrefixCacheShared();
    if (!pc->map_file(ShmPath(mbdir), /*create*/ false, /*n*/ 0, /*cap*/ 0, /*assoc*/ 0)) {
        delete pc;
        return nullptr;
    }
    return pc;
}

PrefixCacheShared::~PrefixCacheShared()
{
    unmap();
}

uint64_t PrefixCacheShared::fnv1a64(const uint8_t* p, size_t n)
{
    uint64_t h = 1469598103934665603ULL;
    for (size_t i = 0; i < n; ++i) {
        h ^= p[i];
        h *= 1099511628211ULL;
    }
    return h;
}

inline uint64_t PrefixCacheShared::prefix_hash64(const uint8_t* p, size_t n)
{
#ifdef MB_HAVE_XXHASH
    // Prefer XXH3_64bits when available; otherwise fall back to XXH64 with seed 0
#ifdef XXH3_64BITS
    return XXH3_64bits(p, n);
#else
    return XXH64(p, n, 0);
#endif
#else
    return fnv1a64(p, n);
#endif
}

inline const char* PrefixCacheShared::hash_name()
{
#ifdef MB_HAVE_XXHASH
    return "xxhash";
#else
    return "fnv1a";
#endif
}

inline size_t PrefixCacheShared::bucket_of(const uint8_t* pfx, size_t n) const
{
    return static_cast<size_t>(prefix_hash64(pfx, n) % hdr_->nbuckets);
}

inline size_t PrefixCacheShared::slot_of(uint64_t h) const
{
    return static_cast<size_t>((h >> 48) % hdr_->assoc);
}

bool PrefixCacheShared::Get(const uint8_t* key, int len, PrefixCacheEntry& out) const
{
    if (!hdr_ || !entries_ || len < static_cast<int>(hdr_->n))
        return false;
    const uint8_t* pfx = key;
    size_t b = bucket_of(pfx, hdr_->n);
    uint64_t h = prefix_hash64(pfx, hdr_->n);
    size_t s0 = slot_of(h);
    size_t base = b * hdr_->assoc;
    for (uint32_t probe = 0; probe < hdr_->assoc; ++probe) {
        size_t s = base + ((s0 + probe) % hdr_->assoc);
        const PrefixCacheSharedEntry* e = &entries_[s];
        uint32_t seq1 = e->seq.load(std::memory_order_acquire);
        if (seq1 & 1)
            continue; // writer in progress
        uint8_t plen = e->prefix_len;
        if (plen == 0 || plen != hdr_->n)
            continue;
        uint8_t pbuf[8];
        size_t edge_off;
        uint8_t eb[EDGE_SIZE];
        memcpy(pbuf, e->prefix, 8);
        edge_off = e->edge_offset;
        memcpy(eb, e->edge_buff, EDGE_SIZE);
        uint32_t seq2 = e->seq.load(std::memory_order_acquire);
        if (seq1 != seq2 || (seq2 & 1))
            continue; // unstable read
        if (memcmp(pbuf, pfx, hdr_->n) != 0)
            continue;
        // Hit
        out.edge_offset = edge_off;
        memcpy(out.edge_buff, eb, EDGE_SIZE);
        out.lf_counter = 0;
        hdr_->hit.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    hdr_->miss.fetch_add(1, std::memory_order_relaxed);
    return false;
}

void PrefixCacheShared::Put(const uint8_t* key, int len, const PrefixCacheEntry& in)
{
    if (!hdr_ || !entries_ || len < static_cast<int>(hdr_->n))
        return;
    const uint8_t* pfx = key;
    uint64_t h = prefix_hash64(pfx, hdr_->n);
    size_t b = static_cast<size_t>(h % hdr_->nbuckets);
    size_t base = b * hdr_->assoc;
    size_t preferred = base + slot_of(h);

    // Try to acquire any slot in the bucket with CAS on seq
    for (uint32_t probe = 0; probe <= hdr_->assoc; ++probe) {
        size_t s = (probe == 0) ? preferred : (base + ((slot_of(h) + probe) % hdr_->assoc));
        PrefixCacheSharedEntry* e = &entries_[s];
        uint32_t expected = e->seq.load(std::memory_order_acquire);
        // Attempt to flip even->odd
        if ((expected & 1) == 0 && e->seq.compare_exchange_weak(expected, expected + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
            // Write body
            e->prefix_len = static_cast<uint8_t>(hdr_->n);
            memset(e->prefix, 0, sizeof(e->prefix));
            memcpy(e->prefix, pfx, hdr_->n);
            e->edge_offset = in.edge_offset;
            memcpy(e->edge_buff, in.edge_buff, EDGE_SIZE);
            // Release: odd->even
            e->seq.fetch_add(1, std::memory_order_release);
            hdr_->put.fetch_add(1, std::memory_order_relaxed);
            return;
        }
    }
    // All slots busy; give up silently to avoid contention
}

void PrefixCacheShared::InvalidateByEdgeOffset(size_t edge_offset)
{
    if (!hdr_ || !entries_)
        return;
    uint64_t inv = 0;
    size_t total = hdr_->nbuckets * hdr_->assoc;
    for (size_t i = 0; i < total; ++i) {
        PrefixCacheSharedEntry* e = &entries_[i];
        size_t off = e->edge_offset;
        if (off == edge_offset) {
            // Acquire with CAS: even->odd
            uint32_t expected = e->seq.load(std::memory_order_acquire);
            if ((expected & 1) == 0 && e->seq.compare_exchange_weak(expected, expected + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                if (e->edge_offset == edge_offset && e->prefix_len != 0) {
                    e->prefix_len = 0;
                    e->edge_offset = 0;
                    memset(e->edge_buff, 0, EDGE_SIZE);
                    memset(e->prefix, 0, sizeof(e->prefix));
                    ++inv;
                }
                // Release: odd->even
                e->seq.fetch_add(1, std::memory_order_release);
            }
        }
    }
    if (inv)
        hdr_->invalidated.fetch_add(inv, std::memory_order_relaxed);
}

void PrefixCacheShared::InvalidateByPrefixAndEdge(const uint8_t* key, int len, size_t edge_offset)
{
    if (!hdr_ || !entries_ || len < static_cast<int>(hdr_->n))
        return;
    const uint8_t* pfx = key;
    size_t b = bucket_of(pfx, hdr_->n);
    size_t base = b * hdr_->assoc;
    uint64_t inv = 0;
    for (uint32_t i = 0; i < hdr_->assoc; ++i) {
        PrefixCacheSharedEntry* e = &entries_[base + i];
        uint32_t expected = e->seq.load(std::memory_order_acquire);
        if (expected & 1)
            continue; // writer in progress, skip
        if (e->prefix_len != hdr_->n)
            continue;
        if (memcmp(e->prefix, pfx, hdr_->n) != 0)
            continue;
        if (e->edge_offset != edge_offset)
            continue;
        // Acquire slot and clear
        if (e->seq.compare_exchange_weak(expected, expected + 1,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            if (e->edge_offset == edge_offset && e->prefix_len == hdr_->n && memcmp(e->prefix, pfx, hdr_->n) == 0) {
                e->prefix_len = 0;
                e->edge_offset = 0;
                memset(e->edge_buff, 0, EDGE_SIZE);
                memset(e->prefix, 0, sizeof(e->prefix));
                ++inv;
            }
            e->seq.fetch_add(1, std::memory_order_release);
        }
    }
    if (inv)
        hdr_->invalidated.fetch_add(inv, std::memory_order_relaxed);
}

void PrefixCacheShared::DumpStats(std::ostream& os) const
{
    if (!hdr_) {
        os << "PrefixCacheShared: not mapped\n";
        return;
    }
    os << "PrefixCacheShared: n=" << hdr_->n
       << " buckets=" << hdr_->nbuckets
       << " assoc=" << hdr_->assoc
       << " hash=" << hash_name()
       << " hit=" << hdr_->hit.load()
       << " miss=" << hdr_->miss.load()
       << " put=" << hdr_->put.load()
       << " invalidated=" << hdr_->invalidated.load()
       << std::endl;
}

// (no entry dump in library; tests should validate via public APIs)

bool PrefixCacheShared::map_file(const std::string& path, bool create,
    int n, size_t capacity, uint32_t assoc)
{
    int fd = -1;
    if (create) {
        // Attempt exclusive create to avoid races, otherwise fall back to opening existing
        fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0644);
        if (fd < 0) {
            if (errno == EEXIST) {
                create = false;
                fd = ::open(path.c_str(), O_RDWR, 0644);
            }
        }
    } else {
        fd = ::open(path.c_str(), O_RDWR, 0644);
    }
    if (fd < 0)
        return false;

    if (create) {
        uint64_t nbuckets = std::max<uint64_t>(1, capacity / std::max<uint32_t>(1, assoc));
        size_ = sizeof(PrefixCacheSharedHeader) + nbuckets * assoc * sizeof(PrefixCacheSharedEntry);
        if (!ensure_file_size(fd, size_)) {
            ::close(fd);
            return false;
        }
        void* p = mmap(nullptr, size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (p == MAP_FAILED) {
            ::close(fd);
            return false;
        }
        base_ = p;
        hdr_ = reinterpret_cast<PrefixCacheSharedHeader*>(base_);
        entries_ = reinterpret_cast<PrefixCacheSharedEntry*>(
            reinterpret_cast<uint8_t*>(base_) + sizeof(PrefixCacheSharedHeader));
        memset(base_, 0, size_);
        hdr_->magic = kMagic;
        hdr_->version = kVersion;
        hdr_->n = static_cast<uint16_t>(n);
        hdr_->assoc = assoc;
        hdr_->nbuckets = nbuckets;
        hdr_->hit.store(0);
        hdr_->miss.store(0);
        hdr_->put.store(0);
        hdr_->invalidated.store(0);
    } else {
        struct stat st;
        if (fstat(fd, &st) != 0) {
            ::close(fd);
            return false;
        }
        size_ = static_cast<size_t>(st.st_size);
        void* p = mmap(nullptr, size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (p == MAP_FAILED) {
            ::close(fd);
            return false;
        }
        base_ = p;
        hdr_ = reinterpret_cast<PrefixCacheSharedHeader*>(base_);
        if (hdr_->magic != kMagic || hdr_->version != kVersion || hdr_->n == 0) {
            unmap();
            ::close(fd);
            return false;
        }
        entries_ = reinterpret_cast<PrefixCacheSharedEntry*>(
            reinterpret_cast<uint8_t*>(base_) + sizeof(PrefixCacheSharedHeader));
    }
    ::close(fd);
    return true;
}

void PrefixCacheShared::unmap()
{
    if (base_ && size_) {
        munmap(base_, size_);
    }
    base_ = nullptr;
    size_ = 0;
    hdr_ = nullptr;
    entries_ = nullptr;
}

} // namespace mabain
