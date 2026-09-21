/**
 * Shared-memory hash map for exact-match caching using RollableFile.
 */

#include "hash_map_internal.h"

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
constexpr uint16_t kHashMapVersion = 3;
constexpr size_t kFastProbeCount = 4;
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
uint64_t HashMapImpl::fnv1a64(const uint8_t* data, int len)
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

uint64_t HashMapImpl::normalize_hash(uint64_t hash)
{
    if (hash == kEmptyHash)
        return 0xA5A5A5A5A5A5A5A5ULL;
    if (hash == kTombstoneHash)
        return 0x5A5A5A5A5A5A5A5AULL;
    return hash;
}

size_t HashMapImpl::checked_memcap_bytes(size_t memcap_mb)
{
    if (memcap_mb == 0
        || memcap_mb > (std::numeric_limits<size_t>::max() >> 20)) {
        throw static_cast<int>(MBError::INVALID_SIZE);
    }
    return memcap_mb << 20;
}

HashMapImpl::HashMapImpl(const std::string& mbdir, size_t capacity, int options,
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

HashMapImpl::~HashMapImpl()
{
    // RollableFile owns mappings and flush.
}

size_t HashMapImpl::expected_capacity(size_t requested, size_t bucket_size) const
{
    size_t desired = floor_pow2_sz(std::max(requested, kMinimumCapacity));
    if (map_size_ < sizeof(HMHeader) + kMinimumCapacity * bucket_size)
        throw static_cast<int>(MBError::INVALID_SIZE);
    const size_t available = (map_size_ - sizeof(HMHeader)) / bucket_size;
    const size_t maximum = floor_pow2_sz(available);
    return std::min(desired, maximum);
}

void HashMapImpl::initialize_header(size_t capacity, uint32_t inline_key)
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
    new (&hdr_->max_probe) std::atomic<uint64_t>(0);
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

void HashMapImpl::validate_header(size_t capacity, uint32_t inline_key) const
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

void HashMapImpl::reset_for_writer()
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
    hdr_->max_probe.store(0, std::memory_order_relaxed);
    hdr_->generation.store(resetting + 1, std::memory_order_release);
}

HashMapImpl::BucketFull* HashMapImpl::bucket_full_ptr(size_t i) const
{
    return reinterpret_cast<BucketFull*>(map_base_ + bucket_offset(i));
}

HashMapImpl::BucketCompact* HashMapImpl::bucket_compact_ptr(size_t i) const
{
    return reinterpret_cast<BucketCompact*>(map_base_ + bucket_offset(i));
}

bool HashMapImpl::full_key_matches(const BucketFull& bucket, const uint8_t* key,
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

bool HashMapImpl::write_full_body(BucketFull& bucket, const uint8_t* key,
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

uint64_t HashMapImpl::bucket_hash(size_t index) const
{
    return compact_
        ? bucket_compact_ptr(index)->hash.load(std::memory_order_relaxed)
        : bucket_full_ptr(index)->hash.load(std::memory_order_relaxed);
}

void HashMapImpl::publish_max_probe(uint64_t probe)
{
    const uint64_t current
        = hdr_->max_probe.load(std::memory_order_relaxed);
    if (probe > current)
        hdr_->max_probe.store(probe, std::memory_order_release);
}

void HashMapImpl::recompute_max_probe()
{
    uint64_t maximum = 0;
    for (size_t index = 0; index < hdr_->capacity; ++index) {
        const uint64_t hash = bucket_hash(index);
        if (hash == kEmptyHash || hash == kTombstoneHash)
            continue;
        const uint64_t probe
            = static_cast<uint64_t>((index - index_of(hash)) & hdr_->mask);
        maximum = std::max(maximum, probe);
    }
    hdr_->max_probe.store(maximum, std::memory_order_release);
}

void HashMapImpl::collapse_trailing_tombstones(size_t erased_index)
{
    const size_t next = (erased_index + 1) & hdr_->mask;
    if (bucket_hash(next) != kEmptyHash)
        return;

    size_t index = erased_index;
    for (size_t scanned = 0; scanned < hdr_->capacity; ++scanned) {
        if (bucket_hash(index) != kTombstoneHash)
            break;
        if (compact_) {
            bucket_compact_ptr(index)->hash.store(
                kEmptyHash, std::memory_order_release);
        } else {
            bucket_full_ptr(index)->hash.store(
                kEmptyHash, std::memory_order_release);
        }
        hdr_->tombstones.fetch_sub(1, std::memory_order_relaxed);
        index = (index - 1) & hdr_->mask;
    }
}

bool HashMapImpl::Get(const uint8_t* key, int len, size_t& ref_offset) const
{
    if (storage_mode_ == StorageMode::VALUE)
        return get_stored_reference(key, len, ref_offset);
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
            continue;

        bool retry = false;
        bool probe_limit_loaded = false;
        size_t probe = 0;
        size_t probe_limit = std::min(capacity - 1, kFastProbeCount - 1);
        for (;;) {
            for (; probe <= probe_limit; ++probe) {
                const size_t index = (start + probe) & hdr_->mask;
                if (probe >= kFastProbeCount && probe + 2 < capacity) {
                    const size_t prefetch = (start + probe + 2) & hdr_->mask;
                    __builtin_prefetch(
                        map_base_ + bucket_offset(prefetch), 0, 1);
                }

                if (compact_) {
                    BucketCompact* bucket = bucket_compact_ptr(index);
                    const uint64_t hash_before
                        = bucket->hash.load(std::memory_order_acquire);
                    if (hash_before == kEmptyHash) {
                        const uint64_t generation_after
                            = hdr_->generation.load(std::memory_order_acquire);
                        if (generation_before == generation_after
                            && (generation_after & 1U) == 0) {
                            return false;
                        }
                        retry = true;
                        break;
                    }
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
                if (hash_before == kEmptyHash) {
                    const uint64_t generation_after
                        = hdr_->generation.load(std::memory_order_acquire);
                    if (generation_before == generation_after
                        && (generation_after & 1U) == 0) {
                        return false;
                    }
                    retry = true;
                    break;
                }
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
                    actual[word] = bucket->key_inline[word].load(
                        std::memory_order_relaxed);
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
                        && actual[1] == expected[1] && actual[2] == expected[2];
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

            if (retry || probe_limit_loaded || probe_limit == capacity - 1)
                break;

            // The common lookup path stays within the first four probes and
            // does not load this writer-maintained churn bound.
            probe_limit_loaded = true;
            probe_limit = static_cast<size_t>(std::min<uint64_t>(
                hdr_->max_probe.load(std::memory_order_acquire),
                static_cast<uint64_t>(capacity - 1)));
            if (probe > probe_limit)
                break;
        }

        if (!retry) {
            const uint64_t generation_after
                = hdr_->generation.load(std::memory_order_acquire);
            if (generation_before == generation_after
                && (generation_after & 1U) == 0) {
                return false;
            }
        }
    }
    return false;
}

int HashMapImpl::Put(
    const uint8_t* key, int len, size_t ref_offset, bool overwrite)
{
    if (storage_mode_ == StorageMode::VALUE)
        return put_stored_reference(key, len, ref_offset, overwrite);
    if (storage_mode_ != StorageMode::REFERENCE)
        return MBError::NOT_ALLOWED;
    if (!(options_ & CONSTS::ACCESS_MODE_WRITER))
        return MBError::NOT_ALLOWED;
    if (key == nullptr || len <= 0)
        return MBError::INVALID_ARG;
    const uint64_t hash = normalize_hash(fnv1a64(key, len));
    const size_t start = index_of(hash);
    const size_t capacity = hdr_->capacity;
    const uint64_t max_probe = std::min<uint64_t>(
        hdr_->max_probe.load(std::memory_order_acquire),
        static_cast<uint64_t>(capacity - 1));
    size_t first_free = capacity;
    size_t first_free_probe = capacity;
    bool first_free_is_tombstone = false;

    // Existing copies cannot be beyond max_probe. This avoids a full-table
    // search when churn has replaced empty buckets with tombstones.
    for (size_t probe = 0;
         probe < capacity && static_cast<uint64_t>(probe) <= max_probe;
         ++probe) {
        const size_t index = (start + probe) & hdr_->mask;
        const uint64_t current_hash = bucket_hash(index);
        if (current_hash == kTombstoneHash) {
            if (first_free == capacity) {
                first_free = index;
                first_free_probe = probe;
                first_free_is_tombstone = true;
            }
            continue;
        }
        if (current_hash == kEmptyHash) {
            if (first_free == capacity) {
                first_free = index;
                first_free_probe = probe;
                first_free_is_tombstone = false;
            }
            break;
        }
        if (current_hash != hash)
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

    // If every slot through the live probe bound is occupied, locate the
    // first reusable slot. No matching live key can exist in this suffix.
    if (first_free == capacity) {
        const size_t first_probe = static_cast<size_t>(max_probe) + 1;
        for (size_t probe = first_probe; probe < capacity; ++probe) {
            const size_t index = (start + probe) & hdr_->mask;
            const uint64_t current_hash = bucket_hash(index);
            if (current_hash == kTombstoneHash
                || current_hash == kEmptyHash) {
                first_free = index;
                first_free_probe = probe;
                first_free_is_tombstone
                    = current_hash == kTombstoneHash;
                break;
            }
        }
    }
    if (first_free == capacity)
        return MBError::NO_RESOURCE;

    if (compact_) {
        BucketCompact* bucket = bucket_compact_ptr(first_free);
        bucket->ref_offset.store(ref_offset, std::memory_order_relaxed);
        publish_max_probe(first_free_probe);
        bucket->hash.store(hash, std::memory_order_release);
    } else {
        size_t target = first_free;
        size_t target_probe = first_free_probe;
        bool target_is_tombstone = first_free_is_tombstone;
        BucketFull* bucket = nullptr;
        while (true) {
            bucket = bucket_full_ptr(target);
            if (write_full_body(*bucket, key, static_cast<uint32_t>(len),
                    ref_offset)) {
                break;
            }

            // An exhausted per-bucket sequence cannot be reused. A tombstone
            // may be skipped, but an empty bucket ends the probe chain and
            // therefore cannot be skipped safely.
            if (!target_is_tombstone)
                return MBError::NO_RESOURCE;

            target = capacity;
            for (size_t probe = target_probe + 1; probe < capacity; ++probe) {
                const size_t index = (start + probe) & hdr_->mask;
                const uint64_t current_hash = bucket_hash(index);
                if (current_hash == kTombstoneHash
                    || current_hash == kEmptyHash) {
                    target = index;
                    target_probe = probe;
                    target_is_tombstone
                        = current_hash == kTombstoneHash;
                    break;
                }
            }
            if (target == capacity)
                return MBError::NO_RESOURCE;
        }
        first_free = target;
        first_free_probe = target_probe;
        first_free_is_tombstone = target_is_tombstone;
        publish_max_probe(first_free_probe);
        bucket->hash.store(hash, std::memory_order_release);
    }

    hdr_->used.fetch_add(1, std::memory_order_relaxed);
    if (first_free_is_tombstone)
        hdr_->tombstones.fetch_sub(1, std::memory_order_relaxed);
    return MBError::SUCCESS;
}

int HashMapImpl::Erase(const uint8_t* key, int len)
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
    const size_t probe_limit = static_cast<size_t>(std::min<uint64_t>(
        hdr_->max_probe.load(std::memory_order_acquire),
        static_cast<uint64_t>(capacity - 1)));

    for (size_t probe = 0; probe <= probe_limit; ++probe) {
        const size_t index = (start + probe) & hdr_->mask;
        if (compact_) {
            BucketCompact* bucket = bucket_compact_ptr(index);
            const uint64_t bucket_hash
                = bucket->hash.load(std::memory_order_relaxed);
            if (bucket_hash == kEmptyHash)
                break;
            if (bucket_hash != hash)
                continue;
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
        }

        if (compact_) {
            bucket_compact_ptr(index)->hash.store(
                kTombstoneHash, std::memory_order_release);
        } else {
            bucket_full_ptr(index)->hash.store(
                kTombstoneHash, std::memory_order_release);
        }
        hdr_->used.fetch_sub(1, std::memory_order_relaxed);
        hdr_->tombstones.fetch_add(1, std::memory_order_relaxed);

        const uint64_t old_max
            = hdr_->max_probe.load(std::memory_order_relaxed);
        collapse_trailing_tombstones(index);
        if (old_max != 0 && static_cast<uint64_t>(probe) == old_max)
            recompute_max_probe();
        return MBError::SUCCESS;
    }
    return MBError::NOT_EXIST;
}

void HashMapImpl::PrintStats(std::ostream& os) const
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

void HashMapImpl::Flush() const
{
    if (storage_mode_ == StorageMode::VALUE) {
        flush_value();
        return;
    }
    file_.Flush();
}

} // namespace mabain
