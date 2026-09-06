/**
 * Shared-memory hash map for exact-match caching using RollableFile.
 */

#include "hash_map.h"

#include <algorithm>
#include <cerrno>
#include <limits>
#include <new>
#include <sys/stat.h>

#include "error.h"

#ifdef MB_HAVE_XXHASH
#include <xxhash.h>
#endif

namespace mabain {

namespace {

constexpr uint32_t kHashMapMagic = 0x484D4150U; // 'HMAP'
constexpr uint16_t kHashMapVersion = 2;
constexpr uint64_t kHashMapControl
    = static_cast<uint64_t>(kHashMapMagic)
    | (static_cast<uint64_t>(kHashMapVersion) << 32);
constexpr uint64_t kEmptyHash = 0;
constexpr uint64_t kTombstoneHash = 1;
constexpr size_t kMinimumCapacity = 1024;
constexpr unsigned kReaderRetries = 8;

size_t floor_pow2_sz(size_t value)
{
    if (value == 0)
        return 0;
    size_t result = 1;
    while (result <= value / 2)
        result <<= 1;
    return result;
}

uint64_t PackKeyMeta(uint32_t key_len, uint32_t sequence)
{
    return static_cast<uint64_t>(key_len)
        | (static_cast<uint64_t>(sequence) << 32);
}

uint32_t KeyMetaLength(uint64_t meta)
{
    return static_cast<uint32_t>(meta);
}

uint32_t KeyMetaSequence(uint64_t meta)
{
    return static_cast<uint32_t>(meta >> 32);
}

void BuildInlineWords(const uint8_t* key, uint32_t len, uint32_t inline_key,
    uint64_t (&words)[3])
{
    words[0] = words[1] = words[2] = 0;
    const uint32_t count = std::min(std::min(len, inline_key), 24U);
    for (uint32_t i = 0; i < count; ++i)
        words[i / 8] |= static_cast<uint64_t>(key[i]) << ((i % 8) * 8);
}

} // namespace

// Fast 64-bit hash; uses XXH3 when available, otherwise FNV-1a.
uint64_t HashMap::fnv1a64(const uint8_t* data, int len)
{
    uint64_t hash = 0;
#ifdef MB_HAVE_XXHASH
    hash = XXH3_64bits(data, static_cast<size_t>(len));
#else
    const uint64_t kOffset = 1469598103934665603ULL;
    const uint64_t kPrime = 1099511628211ULL;
    hash = kOffset;
    for (int i = 0; i < len; ++i) {
        hash ^= static_cast<uint64_t>(data[i]);
        hash *= kPrime;
    }
#endif
    return hash;
}

uint64_t HashMap::normalize_hash(uint64_t hash)
{
    if (hash == kEmptyHash)
        return 0xA5A5A5A5A5A5A5A5ULL;
    if (hash == kTombstoneHash)
        return 0x5A5A5A5A5A5A5A5AULL;
    return hash;
}

size_t HashMap::checked_memcap_bytes(size_t memcap_mb)
{
    if (memcap_mb == 0
        || memcap_mb > (std::numeric_limits<size_t>::max() >> 20)) {
        throw static_cast<int>(MBError::INVALID_SIZE);
    }
    return memcap_mb << 20;
}

HashMap::HashMap(const std::string& mbdir, size_t capacity, int options,
    uint32_t num_stripes, uint32_t inline_key, size_t memcap_mb, bool compact64)
    : path_(mbdir + "_hashmap")
    , options_(options)
    , map_size_(checked_memcap_bytes(memcap_mb))
    , file_(path_, map_size_, map_size_, options, 1)
    , map_base_(nullptr)
    , hdr_(nullptr)
    , compact_(compact64)
    , storage_mode_(StorageMode::REFERENCE)
    , value_hdr_(nullptr)
    , value_state_(nullptr)
{
    (void)num_stripes;
    std::atomic<uint64_t> atomic64;
    std::atomic<size_t> atomic_size;
    if (!atomic64.is_lock_free() || !atomic_size.is_lock_free())
        throw static_cast<int>(MBError::NOT_ALLOWED);

    const bool writer = (options & CONSTS::ACCESS_MODE_WRITER) != 0;
    const bool memory_only = (options & CONSTS::MEMORY_ONLY_MODE) != 0;
    const std::string block_path = path_ + "0";
    bool exists = false;
    if (!memory_only) {
        struct stat info {};
        if (stat(block_path.c_str(), &info) == 0) {
            exists = true;
            if (info.st_size < 0
                || static_cast<uint64_t>(info.st_size) != map_size_) {
                throw static_cast<int>(MBError::INVALID_SIZE);
            }
        } else if (errno != ENOENT) {
            throw static_cast<int>(MBError::OPEN_FAILURE);
        }
    }

    uint8_t* base = nullptr;
    if (writer && (!exists || memory_only)) {
        size_t offset = 0;
        int rval = file_.Reserve(offset, static_cast<int>(sizeof(HMHeader)),
            base, true);
        if (rval != MBError::SUCCESS || base == nullptr)
            throw static_cast<int>(rval);
    } else {
        base = file_.GetShmPtr(0, static_cast<int>(sizeof(HMHeader)));
        if (base == nullptr)
            throw static_cast<int>(exists ? MBError::MMAP_FAILED : MBError::NO_DB);
    }

    map_base_ = base;
    hdr_ = reinterpret_cast<HMHeader*>(map_base_);
    const uint32_t effective_inline = std::min<uint32_t>(inline_key, 24);
    const size_t bucket_size
        = compact_ ? sizeof(BucketCompact) : sizeof(BucketFull);
    const size_t actual_capacity = expected_capacity(capacity, bucket_size);
    const uint64_t control = hdr_->control.load(std::memory_order_acquire);

    if (control == 0) {
        if (!writer)
            throw static_cast<int>(MBError::NOT_INITIALIZED);
        initialize_header(actual_capacity, effective_inline);
    } else {
        if (control != kHashMapControl)
            throw static_cast<int>(MBError::VERSION_MISMATCH);
        validate_header(actual_capacity, effective_inline);
        if (writer)
            reset_for_writer();
    }
}

HashMap::~HashMap()
{
    // RollableFile owns mappings and flush.
}

size_t HashMap::expected_capacity(size_t requested, size_t bucket_size) const
{
    size_t desired = floor_pow2_sz(std::max(requested, kMinimumCapacity));
    if (map_size_ < sizeof(HMHeader) + kMinimumCapacity * bucket_size)
        throw static_cast<int>(MBError::INVALID_SIZE);
    const size_t available = (map_size_ - sizeof(HMHeader)) / bucket_size;
    const size_t maximum = floor_pow2_sz(available);
    return std::min(desired, maximum);
}

void HashMap::initialize_header(size_t capacity, uint32_t inline_key)
{
    new (&hdr_->control) std::atomic<uint64_t>(0);
    hdr_->capacity = capacity;
    hdr_->mask = capacity - 1;
    hdr_->buckets_off = sizeof(HMHeader);
    hdr_->inline_key = compact_ ? 0 : inline_key;
    hdr_->bucket_size = compact_ ? sizeof(BucketCompact) : sizeof(BucketFull);
    hdr_->stripes = 1;
    hdr_->reserved = 0;
    new (&hdr_->generation) std::atomic<uint64_t>(1);
    new (&hdr_->used) std::atomic<uint64_t>(0);
    new (&hdr_->tombstones) std::atomic<uint64_t>(0);

    for (size_t i = 0; i < capacity; ++i) {
        if (compact_) {
            BucketCompact* bucket = bucket_compact_ptr(i);
            new (&bucket->hash) std::atomic<uint64_t>(kEmptyHash);
            new (&bucket->ref_offset) std::atomic<size_t>(0);
        } else {
            BucketFull* bucket = bucket_full_ptr(i);
            new (&bucket->hash) std::atomic<uint64_t>(kEmptyHash);
            new (&bucket->key_meta) std::atomic<uint64_t>(0);
            new (&bucket->ref_offset) std::atomic<size_t>(0);
            for (size_t word = 0; word < 3; ++word)
                new (&bucket->key_inline[word]) std::atomic<uint64_t>(0);
        }
    }

    hdr_->generation.store(2, std::memory_order_release);
    hdr_->control.store(kHashMapControl, std::memory_order_release);
}

void HashMap::validate_header(size_t capacity, uint32_t inline_key) const
{
    const size_t expected_bucket_size
        = compact_ ? sizeof(BucketCompact) : sizeof(BucketFull);
    const uint32_t expected_inline = compact_ ? 0 : inline_key;
    if (hdr_->capacity != capacity || hdr_->mask != capacity - 1
        || hdr_->buckets_off != sizeof(HMHeader)
        || hdr_->bucket_size != expected_bucket_size
        || hdr_->inline_key != expected_inline || hdr_->stripes != 1
        || capacity > (map_size_ - sizeof(HMHeader)) / expected_bucket_size) {
        throw static_cast<int>(MBError::INVALID_SIZE);
    }
}

void HashMap::reset_for_writer()
{
    uint64_t generation = hdr_->generation.load(std::memory_order_relaxed);
    if (generation >= std::numeric_limits<uint64_t>::max() - 1)
        throw static_cast<int>(MBError::NO_RESOURCE);
    const uint64_t resetting
        = (generation & 1U) != 0 ? generation : generation + 1;
    hdr_->generation.store(resetting, std::memory_order_release);

    for (size_t i = 0; i < hdr_->capacity; ++i) {
        if (compact_) {
            bucket_compact_ptr(i)->hash.store(kEmptyHash,
                std::memory_order_release);
        } else {
            bucket_full_ptr(i)->hash.store(kEmptyHash,
                std::memory_order_release);
        }
    }
    hdr_->used.store(0, std::memory_order_relaxed);
    hdr_->tombstones.store(0, std::memory_order_relaxed);
    hdr_->generation.store(resetting + 1, std::memory_order_release);
}

HashMap::BucketFull* HashMap::bucket_full_ptr(size_t i) const
{
    return reinterpret_cast<BucketFull*>(map_base_ + bucket_offset(i));
}

HashMap::BucketCompact* HashMap::bucket_compact_ptr(size_t i) const
{
    return reinterpret_cast<BucketCompact*>(map_base_ + bucket_offset(i));
}

bool HashMap::full_key_matches(const BucketFull& bucket, const uint8_t* key,
    uint32_t len, uint64_t& stable_meta) const
{
    uint64_t meta_before = bucket.key_meta.load(std::memory_order_acquire);
    if ((KeyMetaSequence(meta_before) & 1U) != 0)
        return false;

    uint64_t actual[3];
    for (size_t word = 0; word < 3; ++word)
        actual[word] = bucket.key_inline[word].load(std::memory_order_relaxed);
    uint64_t meta_after = bucket.key_meta.load(std::memory_order_acquire);
    if (meta_before != meta_after
        || (KeyMetaSequence(meta_after) & 1U) != 0) {
        return false;
    }
    stable_meta = meta_after;
    if (KeyMetaLength(meta_after) != len)
        return false;

    uint64_t expected[3];
    BuildInlineWords(key, len, hdr_->inline_key, expected);
    return actual[0] == expected[0] && actual[1] == expected[1]
        && actual[2] == expected[2];
}

bool HashMap::write_full_body(BucketFull& bucket, const uint8_t* key,
    uint32_t len, size_t ref_offset)
{
    const uint64_t old_meta = bucket.key_meta.load(std::memory_order_relaxed);
    const uint32_t old_sequence = KeyMetaSequence(old_meta);
    if (old_sequence >= std::numeric_limits<uint32_t>::max() - 2)
        return false;
    const uint32_t updating_sequence
        = (old_sequence & 1U) != 0 ? old_sequence + 2 : old_sequence + 1;
    bucket.key_meta.store(PackKeyMeta(0, updating_sequence),
        std::memory_order_release);

    uint64_t words[3];
    BuildInlineWords(key, len, hdr_->inline_key, words);
    for (size_t word = 0; word < 3; ++word)
        bucket.key_inline[word].store(words[word], std::memory_order_relaxed);
    bucket.ref_offset.store(ref_offset, std::memory_order_relaxed);
    bucket.key_meta.store(PackKeyMeta(len, updating_sequence + 1),
        std::memory_order_release);
    return true;
}

bool HashMap::Get(const uint8_t* key, int len, size_t& ref_offset) const
{
    if (storage_mode_ != StorageMode::REFERENCE)
        return false;
    if (key == nullptr || len <= 0 || hdr_ == nullptr)
        return false;
    const uint64_t hash = normalize_hash(fnv1a64(key, len));
    const size_t start = index_of(hash);
    const size_t capacity = hdr_->capacity;

    for (unsigned attempt = 0; attempt < kReaderRetries; ++attempt) {
        const uint64_t generation_before
            = hdr_->generation.load(std::memory_order_acquire);
        if ((generation_before & 1U) != 0)
            return false;

        bool retry = false;
        for (size_t probe = 0; probe < capacity; ++probe) {
            const size_t index = (start + probe) & hdr_->mask;
            if (probe >= 4 && probe + 2 < capacity) {
                const size_t prefetch = (start + probe + 2) & hdr_->mask;
                __builtin_prefetch(map_base_ + bucket_offset(prefetch), 0, 1);
            }

            if (compact_) {
                BucketCompact* bucket = bucket_compact_ptr(index);
                const uint64_t hash_before
                    = bucket->hash.load(std::memory_order_acquire);
                if (hash_before == kEmptyHash)
                    return false;
                if (hash_before == kTombstoneHash || hash_before != hash)
                    continue;

                const size_t candidate
                    = bucket->ref_offset.load(std::memory_order_acquire);
                const uint64_t hash_after
                    = bucket->hash.load(std::memory_order_acquire);
                const uint64_t generation_after
                    = hdr_->generation.load(std::memory_order_acquire);
                if (hash_before != hash_after
                    || generation_before != generation_after
                    || (generation_after & 1U) != 0) {
                    retry = true;
                    break;
                }
                ref_offset = candidate;
                return true;
            }

            BucketFull* bucket = bucket_full_ptr(index);
            const uint64_t hash_before
                = bucket->hash.load(std::memory_order_acquire);
            if (hash_before == kEmptyHash)
                return false;
            if (hash_before == kTombstoneHash || hash_before != hash)
                continue;

            const uint64_t meta_before
                = bucket->key_meta.load(std::memory_order_acquire);
            if ((KeyMetaSequence(meta_before) & 1U) != 0) {
                retry = true;
                break;
            }
            uint64_t actual[3];
            for (size_t word = 0; word < 3; ++word) {
                actual[word]
                    = bucket->key_inline[word].load(std::memory_order_relaxed);
            }
            const size_t candidate
                = bucket->ref_offset.load(std::memory_order_acquire);
            const uint64_t meta_after
                = bucket->key_meta.load(std::memory_order_acquire);
            const uint64_t hash_after
                = bucket->hash.load(std::memory_order_acquire);
            if (meta_before != meta_after || hash_before != hash_after
                || (KeyMetaSequence(meta_after) & 1U) != 0) {
                retry = true;
                break;
            }

            bool matches
                = KeyMetaLength(meta_after) == static_cast<uint32_t>(len);
            if (matches) {
                uint64_t expected[3];
                BuildInlineWords(key, static_cast<uint32_t>(len),
                    hdr_->inline_key, expected);
                matches = actual[0] == expected[0]
                    && actual[1] == expected[1]
                    && actual[2] == expected[2];
            }
            if (!matches)
                continue;

            const uint64_t generation_after
                = hdr_->generation.load(std::memory_order_acquire);
            if (generation_before != generation_after
                || (generation_after & 1U) != 0) {
                retry = true;
                break;
            }
            ref_offset = candidate;
            return true;
        }
        if (!retry)
            return false;
    }
    return false;
}

int HashMap::Put(const uint8_t* key, int len, size_t ref_offset, bool overwrite)
{
    if (storage_mode_ != StorageMode::REFERENCE)
        return MBError::NOT_ALLOWED;
    if (!(options_ & CONSTS::ACCESS_MODE_WRITER))
        return MBError::NOT_ALLOWED;
    if (key == nullptr || len <= 0)
        return MBError::INVALID_ARG;
    const uint64_t hash = normalize_hash(fnv1a64(key, len));
    const size_t start = index_of(hash);
    const size_t capacity = hdr_->capacity;
    size_t first_tombstone = capacity;
    size_t first_empty = capacity;

    for (size_t probe = 0; probe < capacity; ++probe) {
        const size_t index = (start + probe) & hdr_->mask;
        const uint64_t bucket_hash = compact_
            ? bucket_compact_ptr(index)->hash.load(std::memory_order_relaxed)
            : bucket_full_ptr(index)->hash.load(std::memory_order_relaxed);
        if (bucket_hash == kTombstoneHash) {
            if (first_tombstone == capacity)
                first_tombstone = index;
            continue;
        }
        if (bucket_hash == kEmptyHash) {
            first_empty = index;
            break;
        }
        if (bucket_hash != hash)
            continue;

        if (compact_) {
            if (!overwrite)
                return MBError::SUCCESS;
            bucket_compact_ptr(index)->ref_offset.store(ref_offset,
                std::memory_order_release);
            return MBError::SUCCESS;
        }

        uint64_t stable_meta = 0;
        BucketFull* bucket = bucket_full_ptr(index);
        if (!full_key_matches(*bucket, key, static_cast<uint32_t>(len),
                stable_meta)) {
            continue;
        }
        if (!overwrite)
            return MBError::SUCCESS;
        bucket->ref_offset.store(ref_offset, std::memory_order_release);
        return MBError::SUCCESS;
    }

    size_t target = first_tombstone != capacity ? first_tombstone : first_empty;
    if (target == capacity)
        return MBError::NO_RESOURCE;

    if (compact_) {
        BucketCompact* bucket = bucket_compact_ptr(target);
        bucket->ref_offset.store(ref_offset, std::memory_order_relaxed);
        bucket->hash.store(hash, std::memory_order_release);
    } else {
        BucketFull* bucket = bucket_full_ptr(target);
        if (!write_full_body(*bucket, key, static_cast<uint32_t>(len),
                ref_offset)) {
            if (target == first_tombstone && first_empty != capacity) {
                target = first_empty;
                bucket = bucket_full_ptr(target);
                if (!write_full_body(*bucket, key, static_cast<uint32_t>(len),
                        ref_offset)) {
                    return MBError::NO_RESOURCE;
                }
            } else {
                return MBError::NO_RESOURCE;
            }
        }
        bucket->hash.store(hash, std::memory_order_release);
    }

    hdr_->used.fetch_add(1, std::memory_order_relaxed);
    if (target == first_tombstone)
        hdr_->tombstones.fetch_sub(1, std::memory_order_relaxed);
    return MBError::SUCCESS;
}

int HashMap::Erase(const uint8_t* key, int len)
{
    if (storage_mode_ == StorageMode::VALUE)
        return erase_value(key, len);
    if (!(options_ & CONSTS::ACCESS_MODE_WRITER))
        return MBError::NOT_ALLOWED;
    if (key == nullptr || len <= 0)
        return MBError::INVALID_ARG;
    const uint64_t hash = normalize_hash(fnv1a64(key, len));
    const size_t start = index_of(hash);
    const size_t capacity = hdr_->capacity;

    for (size_t probe = 0; probe < capacity; ++probe) {
        const size_t index = (start + probe) & hdr_->mask;
        if (compact_) {
            BucketCompact* bucket = bucket_compact_ptr(index);
            const uint64_t bucket_hash
                = bucket->hash.load(std::memory_order_relaxed);
            if (bucket_hash == kEmptyHash)
                break;
            if (bucket_hash != hash)
                continue;
            bucket->hash.store(kTombstoneHash, std::memory_order_release);
        } else {
            BucketFull* bucket = bucket_full_ptr(index);
            const uint64_t bucket_hash
                = bucket->hash.load(std::memory_order_relaxed);
            if (bucket_hash == kEmptyHash)
                break;
            if (bucket_hash != hash)
                continue;
            uint64_t stable_meta = 0;
            if (!full_key_matches(*bucket, key, static_cast<uint32_t>(len),
                    stable_meta)) {
                continue;
            }
            bucket->hash.store(kTombstoneHash, std::memory_order_release);
        }

        hdr_->used.fetch_sub(1, std::memory_order_relaxed);
        hdr_->tombstones.fetch_add(1, std::memory_order_relaxed);
        return MBError::SUCCESS;
    }
    return MBError::NOT_EXIST;
}

void HashMap::PrintStats(std::ostream& os) const
{
    if (storage_mode_ == StorageMode::VALUE) {
        print_value_stats(os);
        return;
    }
    os << "HashMap stats:\n"
       << "\tcapacity: " << hdr_->capacity << "\n"
       << "\tused: " << hdr_->used.load(std::memory_order_relaxed) << "\n"
       << "\ttombstones: "
       << hdr_->tombstones.load(std::memory_order_relaxed) << "\n"
       << "\tstripes: " << hdr_->stripes << "\n";
}

void HashMap::Flush() const
{
    if (storage_mode_ == StorageMode::VALUE) {
        flush_value();
        return;
    }
    file_.Flush();
}

} // namespace mabain
