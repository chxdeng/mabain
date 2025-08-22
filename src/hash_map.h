/**
 * Shared-memory hash map for exact-match caching using RollableFile.
 */

#ifndef MABAIN_HASH_MAP_H
#define MABAIN_HASH_MAP_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>

#include "mabain_consts.h"
#include "rollable_file.h"

namespace mabain {

class HashMap {
public:
    // Create or open a shared hash map under mbdir (file: mbdir+"_hashmap").
    // capacity: requested number of buckets (will be rounded to power-of-two).
    // options: reader/writer flags from CONSTS.
    // num_stripes: deprecated; ignored (lock-free; single-writer assumed).
    // inline_key: bytes of key to inline for quick equality screening (0..32 reasonable).
    HashMap(const std::string& mbdir, size_t capacity, int options,
        uint32_t num_stripes = 64, uint32_t inline_key = 16, size_t memcap_mb = 32,
        bool compact64 = false);
    ~HashMap();

    // Insert or update an entry. overwrite=true replaces existing ref_offset on match.
    int Put(const uint8_t* key, int len, size_t ref_offset, bool overwrite = true);
    // Lookup; returns true on hit and sets ref_offset.
    bool Get(const uint8_t* key, int len, size_t& ref_offset) const;
    // Remove an entry by key. Returns 0 on success, NOT_FOUND if missing.
    int Erase(const uint8_t* key, int len);

    void PrintStats(std::ostream& os) const;
    void Flush() const;

private:
    struct HMHeader {
        uint32_t magic;
        uint16_t version;
        uint16_t flags;
        uint64_t capacity; // power-of-two bucket count
        uint64_t mask; // capacity-1
        uint32_t stripes; // probe stride (kept for compatibility); now 1
        uint32_t inline_key; // bytes of inline key comparison
        uint32_t bucket_size; // sizeof(Bucket)
        uint32_t reserved;
        std::atomic<uint64_t> used;
        size_t stripes_off; // deprecated; 0
        size_t buckets_off; // offset to first Bucket
    };

    struct BucketFull {
        // 0 means empty; acts as publish/valid flag for the rest of the fields.
        std::atomic<uint64_t> hash; // 64-bit FNV1a/xxhash fingerprint
        uint32_t key_len;
        uint32_t flags; // reserved for future
        size_t ref_offset; // data or index offset
        uint8_t key_inline[24]; // default inline room (trimmed by header.inline_key)
    };

    struct BucketCompact {
        std::atomic<uint64_t> hash; // 0 means empty; publish flag
        size_t ref_offset;
    };

    // Hash helpers
    static uint64_t fnv1a64(const uint8_t* data, int len);

    // Bucket helpers
    inline size_t bucket_offset(size_t i) const { return hdr_->buckets_off + i * hdr_->bucket_size; }
    BucketFull* bucket_full_ptr(size_t i) const;
    BucketCompact* bucket_compact_ptr(size_t i) const;

    // Probe
    inline size_t index_of(uint64_t h) const { return static_cast<size_t>(h) & hdr_->mask; }

    // Initialization
    void initialize_header(size_t capacity, uint32_t stripes, uint32_t inline_key);

private:
    std::string path_;
    int options_;
    mutable RollableFile file_;
    HMHeader* hdr_;
    bool compact_;
};

} // namespace mabain

#endif // MABAIN_HASH_MAP_H
