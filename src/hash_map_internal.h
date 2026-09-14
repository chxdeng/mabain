/**
 * Internal HashMap implementation details. Public clients include
 * hash_map_api.h instead.
 */

#ifndef MABAIN_HASH_MAP_INTERNAL_H
#define MABAIN_HASH_MAP_INTERNAL_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>

#include "hash_map_api.h"
#include "mabain_consts.h"
#include "rollable_file.h"

namespace mabain {

class HashMapValueState;

class HashMapImpl {
public:
    // Create or open a shared hash map under mbdir (file: mbdir+"_hashmap").
    // One writer process and multiple reader processes may use the map. Opening
    // a writer resets an existing compatible map in place; readers never create
    // or initialize a map. All processes must use matching layout arguments.
    // capacity: requested number of buckets (will be rounded to power-of-two).
    // options: reader/writer flags from CONSTS.
    // num_stripes: deprecated; ignored (lock-free; single-writer assumed).
    // inline_key: bytes of key to inline for quick equality screening (0..24).
    HashMapImpl(const std::string& mbdir, size_t capacity, int options,
        uint32_t num_stripes = 64, uint32_t inline_key = 16, size_t memcap_mb = 32,
        bool compact64 = false);
    // Create or open value-storage mode. The fixed index remains in
    // mbdir+"_hashmap0" while immutable key/value records use generation-named
    // jemalloc-backed files. A value-mode map cannot be mixed with reference
    // mode at the same path.
    HashMapImpl(const std::string& mbdir, size_t capacity, int options,
        const HashMapValueConfig& config, size_t index_memcap_mb = 32);
    ~HashMapImpl();

    // Insert or update an entry. overwrite=true replaces existing ref_offset on match.
    int Put(const uint8_t* key, int len, size_t ref_offset, bool overwrite = true);
    // Lookup; returns true on hit and sets ref_offset.
    bool Get(const uint8_t* key, int len, size_t& ref_offset) const;
    // Value-mode insert/update and copying lookup. GetValue always leaves
    // value.data_len zero on non-success.
    int PutValue(const uint8_t* key, int key_len, const uint8_t* value,
        int value_len, bool overwrite = true);
    int GetValue(const uint8_t* key, int key_len, MBData& value) const;
    // Remove an entry by key. Returns 0 on success, NOT_FOUND if missing.
    int Erase(const uint8_t* key, int len);

    void PrintStats(std::ostream& os) const;
    void Flush() const;

private:
    friend class HashMapValueState;

    enum class StorageMode : uint8_t {
        REFERENCE,
        VALUE,
    };

    struct alignas(64) HMHeader {
        // Published last during first-time initialization. A zero value means
        // the layout is not ready for readers.
        std::atomic<uint64_t> control;
        uint64_t capacity; // power-of-two bucket count
        uint64_t mask; // capacity-1
        size_t buckets_off; // offset to first Bucket
        uint32_t inline_key; // bytes of inline key comparison
        uint32_t bucket_size; // sizeof(Bucket)
        uint32_t stripes; // probe stride (kept for compatibility); now 1
        uint32_t reserved;
        // Odd while a writer is resetting the map, even while readable.
        std::atomic<uint64_t> generation;
        uint8_t read_padding[8];

        // Writer-updated statistics live on a separate cache line so normal
        // inserts/removes do not invalidate the reader control cache line.
        std::atomic<uint64_t> used;
        std::atomic<uint64_t> tombstones;
        uint8_t stats_padding[48];
    };

    struct BucketFull {
        // hash is the publication field: 0=empty, 1=tombstone, >=2=occupied.
        std::atomic<uint64_t> hash;
        // Low 32 bits are key length; high 32 bits are an even stable sequence.
        std::atomic<uint64_t> key_meta;
        std::atomic<size_t> ref_offset;
        std::atomic<uint64_t> key_inline[3];
    };

    struct BucketCompact {
        std::atomic<uint64_t> hash; // 0=empty, 1=tombstone, >=2=occupied
        std::atomic<size_t> ref_offset;
    };

    static_assert(sizeof(HMHeader) == 128, "HashMap header layout changed");
    static_assert(sizeof(BucketFull) == 48, "HashMap full bucket layout changed");
    static_assert(sizeof(BucketCompact) == 16, "HashMap compact bucket layout changed");

    static constexpr uint32_t MAX_VALUE_READER_SLOTS = 128;

    struct alignas(64) ValueReaderSlot {
        std::atomic<uint64_t> owner_id;
        std::atomic<uint64_t> process_id;
        std::atomic<uint64_t> process_start_time;
        std::atomic<uint64_t> active_epoch;
        std::atomic<uint64_t> active_value_generation;
        uint8_t padding[24];
    };

    struct alignas(64) ValueHMHeader {
        std::atomic<uint64_t> control;
        uint64_t capacity;
        uint64_t mask;
        size_t buckets_off;
        uint64_t value_block_size;
        uint64_t value_memcap;
        uint32_t max_value_blocks;
        uint32_t reader_slot_count;
        uint32_t hash_algorithm;
        uint32_t max_load_percent;
        uint32_t max_key_length;
        uint32_t max_value_length;
        std::atomic<uint64_t> map_generation;
        std::atomic<uint64_t> value_generation;
        std::atomic<uint64_t> reader_epoch;
        std::atomic<uint64_t> used;
        std::atomic<uint64_t> tombstones;
        std::atomic<uint64_t> live_value_bytes;
        std::atomic<uint64_t> retired_value_bytes;
        std::atomic<uint64_t> allocation_failures;
        uint8_t padding[16];
        ValueReaderSlot reader_slots[MAX_VALUE_READER_SLOTS];
    };

    struct ValueBucket {
        // One atomic publication word: high 16 bits are a hash fingerprint and
        // low 48 bits are the immutable record offset. Values 0 and 1 are the
        // empty and tombstone sentinels respectively.
        std::atomic<uint64_t> entry;
    };

    static_assert(sizeof(ValueReaderSlot) == 64,
        "HashMap value reader slot layout changed");
    static_assert(sizeof(ValueBucket) == 8,
        "HashMap value bucket layout changed");

    // Hash helpers
    static uint64_t fnv1a64(const uint8_t* data, int len);
    static uint64_t normalize_hash(uint64_t hash);

    // Bucket helpers
    inline size_t bucket_offset(size_t i) const { return hdr_->buckets_off + i * hdr_->bucket_size; }
    BucketFull* bucket_full_ptr(size_t i) const;
    BucketCompact* bucket_compact_ptr(size_t i) const;
    bool full_key_matches(const BucketFull& bucket, const uint8_t* key,
        uint32_t len, uint64_t& stable_meta) const;
    bool write_full_body(BucketFull& bucket, const uint8_t* key,
        uint32_t len, size_t ref_offset);

    // Probe
    inline size_t index_of(uint64_t h) const { return static_cast<size_t>(h) & hdr_->mask; }

    // Initialization
    static size_t checked_memcap_bytes(size_t memcap_mb);
    size_t expected_capacity(size_t requested, size_t bucket_size) const;
    void initialize_header(size_t capacity, uint32_t inline_key);
    void validate_header(size_t capacity, uint32_t inline_key) const;
    void reset_for_writer();
    int erase_value(const uint8_t* key, int len);
    void print_value_stats(std::ostream& os) const;
    void flush_value() const;

private:
    std::string path_;
    int options_;
    size_t map_size_;
    mutable RollableFile file_;
    uint8_t* map_base_;
    HMHeader* hdr_;
    bool compact_;
    StorageMode storage_mode_;
    ValueHMHeader* value_hdr_;
    std::shared_ptr<HashMapValueState> value_state_;
};

} // namespace mabain

#endif // MABAIN_HASH_MAP_INTERNAL_H
