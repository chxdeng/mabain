/**
 * Shared-memory hash map for exact-match caching using RollableFile.
 */

#include "hash_map.h"

#include <algorithm>
#include <cstring>
#include <new>
#include <sys/mman.h>

#include "error.h"
#include "logger.h"
#include <xxhash.h>

namespace mabain {

static inline size_t floor_pow2_sz(size_t x)
{
    if (x == 0)
        return 0;
    size_t p = 1;
    while ((p << 1) && ((p << 1) <= x))
        p <<= 1;
    return p;
}

// Fast 64-bit hash (XXH3); avoids 0 as publish value
uint64_t HashMap::fnv1a64(const uint8_t* data, int len)
{
    uint64_t h = XXH3_64bits(data, (size_t)len);
    return h ? h : 0xA5A5A5A5A5A5A5A5ULL;
}

HashMap::HashMap(const std::string& mbdir, size_t capacity, int options,
    uint32_t num_stripes, uint32_t inline_key, size_t memcap_mb, bool compact64)
    : path_(mbdir + "_hashmap")
    , options_(options)
    , file_(path_, /*block*/ (size_t)memcap_mb << 20, /*memcap*/ (size_t)memcap_mb << 20, options, /*max_block*/ 1)
    , hdr_(nullptr)
    , compact_(compact64)
{
    size_t cap = floor_pow2_sz(std::max<size_t>(capacity, 1024));
    if (cap < 1024)
        cap = 1024;
    // Lock-free design: ignore stripes parameter; use stride=1
    uint32_t stripes = 1;
    uint32_t ik = std::min<uint32_t>(inline_key, 24);

    // Layout: [HMHeader][buckets]
    size_t off = 0;
    uint8_t* p = nullptr;
    int rv = file_.Reserve(off, sizeof(HMHeader), p, true);
    if (rv != MBError::SUCCESS) {
        throw (int)rv;
    }
    hdr_ = reinterpret_cast<HMHeader*>(p);
    bool need_init = (hdr_->magic != 0x484D4150U); // 'HMAP'
    if (need_init || (options & CONSTS::ACCESS_MODE_WRITER)) {
        initialize_header(cap, stripes, ik);
    }
}

HashMap::~HashMap()
{
    // RollableFile owns mappings and flush
}

void HashMap::initialize_header(size_t capacity, uint32_t stripes, uint32_t inline_key)
{
    // Compute offsets
    size_t buckets_off = sizeof(HMHeader);
    size_t bsz = compact_ ? sizeof(BucketCompact) : sizeof(BucketFull);
    size_t buckets_bytes = bsz * capacity;
    // Zero-initialize the bucket region in block-sized chunks. If the
    // requested capacity does not fit into the configured RollableFile size,
    // we cap the capacity to what we can successfully reserve. Avoid
    // attempting to map beyond the first block to prevent overflow logs.
    const size_t kBlock = file_.GetBlockSize();
    size_t curr = buckets_off;
    size_t end = std::min(buckets_off + buckets_bytes, kBlock);
    while (curr < end) {
        size_t block_rem = kBlock - (curr % kBlock);
        size_t chunk = std::min(block_rem, end - curr);
        uint8_t* cp = nullptr;
        size_t off2 = curr;
        int rv = file_.Reserve(off2, (int)chunk, cp, true);
        if (rv != MBError::SUCCESS || cp == nullptr) {
            break; // out of space; cap capacity below
        }
        std::memset(cp, 0, chunk);
        curr += chunk;
    }

    // Fill header
    hdr_->magic = 0x484D4150U; // 'HMAP'
    hdr_->version = 1;
    hdr_->flags = 0;
    // Compute the actual capacity based on how much we zeroed.
    size_t actual_bytes = (curr > buckets_off) ? (curr - buckets_off) : 0;
    size_t actual_buckets = (actual_bytes / bsz);
    // Round down to power-of-two and enforce minimum size.
    auto floor_pow2 = [](size_t x) {
        size_t p = 1;
        while ((p << 1) && ((p << 1) <= x))
            p <<= 1;
        return p;
    };
    size_t cap_final = floor_pow2(std::max<size_t>(actual_buckets, 1024));
    hdr_->capacity = cap_final;
    hdr_->mask = cap_final - 1;
    hdr_->stripes = stripes;
    hdr_->inline_key = compact_ ? 0 : inline_key;
    hdr_->bucket_size = (uint32_t)bsz;
    hdr_->reserved = 0;
    hdr_->used.store(0, std::memory_order_relaxed);
    hdr_->stripes_off = 0; // deprecated
    hdr_->buckets_off = buckets_off;
}

HashMap::BucketFull* HashMap::bucket_full_ptr(size_t i) const
{
    size_t off = bucket_offset(i);
    return reinterpret_cast<BucketFull*>(file_.GetShmPtr(off, sizeof(BucketFull)));
}

HashMap::BucketCompact* HashMap::bucket_compact_ptr(size_t i) const
{
    size_t off = bucket_offset(i);
    return reinterpret_cast<BucketCompact*>(file_.GetShmPtr(off, sizeof(BucketCompact)));
}

bool HashMap::Get(const uint8_t* key, int len, size_t& ref_offset) const
{
    if (key == nullptr || len <= 0 || hdr_ == nullptr)
        return false;
    uint64_t h = fnv1a64(key, len);
    size_t idx = index_of(h);
    size_t cap = hdr_->capacity;

    // Linear probe with stride=1; readers are lock-free.
    // For backward compatibility with older maps that used striped probing,
    // we do not stop on first empty unless stride==1 in header.
    uint32_t stride = 1;
    bool is_compact = (hdr_->bucket_size == sizeof(BucketCompact));
    for (size_t probe = 0; probe < cap; ++probe) {
        size_t bi = (idx + probe * stride) & hdr_->mask;
        // Prefetch only on longer chains to reduce overhead
        // Prefetch only on longer chains to reduce overhead
        if (probe >= 4 && (probe + 2) < cap) {
            size_t pf_i = (idx + (probe + 2) * stride) & hdr_->mask;
            if (is_compact)
                __builtin_prefetch(bucket_compact_ptr(pf_i), 0, 1);
            else
                __builtin_prefetch(bucket_full_ptr(pf_i), 0, 1);
        }
        if (is_compact) {
            BucketCompact* b = bucket_compact_ptr(bi);
            uint64_t bh = b->hash.load(std::memory_order_relaxed);
            if (bh == 0) {
                if (hdr_->stripes == 1)
                    return false;
                continue;
            }
            if (bh == h) {
                std::atomic_thread_fence(std::memory_order_acquire);
                ref_offset = b->ref_offset;
                return true;
            }
        } else {
            BucketFull* b = bucket_full_ptr(bi);
            uint64_t bh = b->hash.load(std::memory_order_relaxed);
            if (bh == 0) {
                if (hdr_->stripes == 1)
                    return false; // new layout: early miss
                continue; // old layout: keep scanning all buckets
            }
            if (bh == h) {
                std::atomic_thread_fence(std::memory_order_acquire);
                // quick screen by length and inline
                if (b->key_len == (uint32_t)len) {
                    uint32_t ik = hdr_->inline_key;
                    if (ik > 0) {
                        uint32_t cmp = std::min<uint32_t>(ik, (uint32_t)len);
                        if (std::memcmp(b->key_inline, key, cmp) != 0)
                            continue; // collision
                    }
                    ref_offset = b->ref_offset;
                    return true;
                }
            }
        }
    }
    return false;
}

int HashMap::Put(const uint8_t* key, int len, size_t ref_offset, bool overwrite)
{
    if (!(options_ & CONSTS::ACCESS_MODE_WRITER))
        return MBError::NOT_ALLOWED;
    if (key == nullptr || len <= 0)
        return MBError::INVALID_ARG;
    uint64_t h = fnv1a64(key, len);
    size_t idx = index_of(h);
    size_t cap = hdr_->capacity;
    uint32_t stride = 1;
    size_t first_free = (size_t)-1;
    bool is_compact = (hdr_->bucket_size == sizeof(BucketCompact));
    for (size_t probe = 0; probe < cap; ++probe) {
        size_t bi = (idx + probe * stride) & hdr_->mask;
        if (is_compact) {
            BucketCompact* b = bucket_compact_ptr(bi);
            uint64_t bh = b->hash.load(std::memory_order_relaxed);
            if (bh == 0) {
                if (first_free == (size_t)-1)
                    first_free = bi;
                break;
            }
            if (bh == h) {
                if (!overwrite)
                    return MBError::SUCCESS;
                b->ref_offset = ref_offset;
                reinterpret_cast<std::atomic<uint64_t>&>(b->hash).store(h, std::memory_order_release);
                return MBError::SUCCESS;
            }
        } else {
            BucketFull* b = bucket_full_ptr(bi);
            uint64_t bh = b->hash.load(std::memory_order_relaxed);
            if (bh == 0) {
                if (first_free == (size_t)-1)
                    first_free = bi;
                break;
            }
            if (bh == h) {
                if (!overwrite)
                    return MBError::SUCCESS;
                // overwrite body then publish by re-storing hash
                b->key_len = (uint32_t)len;
                b->flags = 0;
                b->ref_offset = ref_offset;
                uint32_t ik = hdr_->inline_key;
                if (ik) {
                    uint32_t copy = std::min<uint32_t>(ik, (uint32_t)len);
                    std::memcpy(b->key_inline, key, copy);
                }
                b->hash.store(h, std::memory_order_release);
                return MBError::SUCCESS;
            }
        }
    }

    if (first_free == (size_t)-1) {
        // Table is full or heavily clustered; avoid O(cap) scans per insert.
        return MBError::NO_RESOURCE;
    }
    if (is_compact) {
        BucketCompact* b = bucket_compact_ptr(first_free);
        b->ref_offset = ref_offset;
        reinterpret_cast<std::atomic<uint64_t>&>(b->hash).store(h, std::memory_order_release);
    } else {
        BucketFull* b = bucket_full_ptr(first_free);
        b->key_len = (uint32_t)len;
        b->flags = 0;
        b->ref_offset = ref_offset;
        if (hdr_->inline_key) {
            uint32_t copy = std::min<uint32_t>(hdr_->inline_key, (uint32_t)len);
            std::memcpy(b->key_inline, key, copy);
        }
        b->hash.store(h, std::memory_order_release);
    }
    hdr_->used.fetch_add(1, std::memory_order_relaxed);
    return MBError::SUCCESS;
}

int HashMap::Erase(const uint8_t* key, int len)
{
    if (!(options_ & CONSTS::ACCESS_MODE_WRITER))
        return MBError::NOT_ALLOWED;
    if (key == nullptr || len <= 0)
        return MBError::INVALID_ARG;
    uint64_t h = fnv1a64(key, len);
    size_t idx = index_of(h);
    size_t cap = hdr_->capacity;
    uint32_t stride = 1;
    bool is_compact2 = (hdr_->bucket_size == sizeof(BucketCompact));
    for (size_t probe = 0; probe < cap; ++probe) {
        size_t bi = (idx + probe * stride) & hdr_->mask;
        if (is_compact2) {
            BucketCompact* b = bucket_compact_ptr(bi);
            uint64_t bh = b->hash.load(std::memory_order_relaxed);
            if (bh == 0) {
                if (hdr_->stripes == 1)
                    break;
                continue;
            }
            if (bh == h) {
                reinterpret_cast<std::atomic<uint64_t>&>(b->hash).store(0, std::memory_order_release);
                return MBError::SUCCESS;
            }
        } else {
            BucketFull* b = bucket_full_ptr(bi);
            uint64_t bh = b->hash.load(std::memory_order_relaxed);
            if (bh == 0) {
                if (hdr_->stripes == 1)
                    break;
                continue;
            }
            if (bh == h) {
                b->hash.store(0, std::memory_order_release);
                return MBError::SUCCESS;
            }
        }
    }
    return MBError::NOT_EXIST;
}

void HashMap::PrintStats(std::ostream& os) const
{
    os << "HashMap stats:\n"
       << "\tcapacity: " << hdr_->capacity << "\n"
       << "\tused: " << hdr_->used.load(std::memory_order_relaxed) << "\n"
       << "\tstripes: " << hdr_->stripes << "\n";
}

void HashMap::Flush() const
{
    file_.Flush();
}

} // namespace mabain
