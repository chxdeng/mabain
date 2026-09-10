/**
 * Compare the legacy 16-byte HashMap value index bucket with the packed
 * 8-byte bucket. This is a benchmark-only model: production sources are
 * intentionally not changed.
 */

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <sched.h>
#include <string>
#include <vector>

namespace {

constexpr uint64_t kEmpty = 0;
constexpr uint64_t kTombstone = 1;
constexpr uint64_t kOffsetMask = (uint64_t { 1 } << 48) - 1;
constexpr size_t kRecordHeaderSize = 8;

struct CurrentBucket {
    std::atomic<uint64_t> hash { kEmpty };
    std::atomic<uint64_t> record_offset { 0 };
};

struct PackedBucket {
    std::atomic<uint64_t> entry { kEmpty };
};

static_assert(sizeof(CurrentBucket) == 16,
    "legacy benchmark bucket layout changed");
static_assert(sizeof(PackedBucket) == 8,
    "packed benchmark bucket must be one atomic word");

struct ProbeStats {
    uint64_t lookups = 0;
    uint64_t probes = 0;
    uint64_t record_checks = 0;
    size_t max_probes = 0;
};

struct TimedResult {
    double nanoseconds_per_lookup = 0;
    uint64_t checksum = 0;
    uint64_t errors = 0;
};

uint64_t Mix64(uint64_t value)
{
    value += 0x9E3779B97F4A7C15ULL;
    value = (value ^ (value >> 30)) * 0xBF58476D1CE4E5B9ULL;
    value = (value ^ (value >> 27)) * 0x94D049BB133111EBULL;
    return value ^ (value >> 31);
}

uint64_t HashKey(const uint8_t* data, size_t length)
{
    // This matches Mabain's default build, where MB_USE_XXHASH is OFF.
    constexpr uint64_t kOffset = 1469598103934665603ULL;
    constexpr uint64_t kPrime = 1099511628211ULL;
    uint64_t hash = kOffset;
    for (size_t index = 0; index < length; ++index) {
        hash ^= static_cast<uint64_t>(data[index]);
        hash *= kPrime;
    }
    if (hash == kEmpty)
        return 0xA5A5A5A5A5A5A5A5ULL;
    if (hash == kTombstone)
        return 0x5A5A5A5A5A5A5A5AULL;
    return hash;
}

uint16_t HashTag(uint64_t hash)
{
    return static_cast<uint16_t>(hash >> 48);
}

uint64_t PackEntry(uint64_t hash, size_t offset)
{
    return (static_cast<uint64_t>(HashTag(hash)) << 48)
        | static_cast<uint64_t>(offset);
}

void StoreU16(std::vector<uint8_t>& bytes, uint16_t value)
{
    bytes.push_back(static_cast<uint8_t>(value));
    bytes.push_back(static_cast<uint8_t>(value >> 8));
}

void StoreU32(std::vector<uint8_t>& bytes, uint32_t value)
{
    for (unsigned shift = 0; shift < 32; shift += 8)
        bytes.push_back(static_cast<uint8_t>(value >> shift));
}

uint16_t LoadU16(const uint8_t* bytes)
{
    return static_cast<uint16_t>(bytes[0])
        | static_cast<uint16_t>(static_cast<uint16_t>(bytes[1]) << 8);
}

uint32_t LoadU32(const uint8_t* bytes)
{
    uint32_t value = 0;
    for (unsigned shift = 0; shift < 32; shift += 8)
        value |= static_cast<uint32_t>(bytes[shift / 8]) << shift;
    return value;
}

void BuildKey(uint64_t identifier, size_t key_length, uint8_t* output)
{
    const size_t identity_bytes = std::min<size_t>(key_length, sizeof(identifier));
    for (size_t index = 0; index < identity_bytes; ++index)
        output[index] = static_cast<uint8_t>(identifier >> (index * 8));

    uint64_t mixed = Mix64(identifier);
    for (size_t index = identity_bytes; index < key_length; ++index) {
        if ((index & 7U) == 0)
            mixed = Mix64(mixed);
        output[index] = static_cast<uint8_t>(mixed >> ((index & 7U) * 8));
    }
}

void BuildValue(uint64_t identifier, size_t value_length, uint8_t* output)
{
    uint64_t mixed = Mix64(identifier ^ 0xC6A4A7935BD1E995ULL);
    for (size_t index = 0; index < value_length; ++index) {
        if ((index & 7U) == 0)
            mixed = Mix64(mixed);
        output[index] = static_cast<uint8_t>(mixed >> ((index & 7U) * 8));
    }
}

class RecordArena {
public:
    explicit RecordArena(size_t reserve_bytes)
    {
        bytes_.reserve(reserve_bytes);
        bytes_.resize(4096, 0);
    }

    size_t Add(const uint8_t* key, uint16_t key_length, const uint8_t* value,
        uint32_t value_length)
    {
        const size_t offset = bytes_.size();
        const size_t record_size = kRecordHeaderSize
            + static_cast<size_t>(key_length) + value_length;
        if (offset > kOffsetMask || record_size > kOffsetMask - offset)
            return 0;

        StoreU32(bytes_, value_length);
        StoreU16(bytes_, key_length);
        StoreU16(bytes_, 0);
        bytes_.insert(bytes_.end(), key, key + key_length);
        bytes_.insert(bytes_.end(), value, value + value_length);
        return offset;
    }

    bool KeyMatches(size_t offset, const uint8_t* key, size_t key_length) const
    {
        uint32_t value_length = 0;
        return Validate(offset, key_length, value_length)
            && std::equal(key, key + key_length,
                bytes_.begin() + static_cast<std::ptrdiff_t>(
                    offset + kRecordHeaderSize));
    }

    bool CopyIfMatches(size_t offset, const uint8_t* key, size_t key_length,
        uint8_t* output, size_t output_capacity, size_t& output_length) const
    {
        output_length = 0;
        uint32_t value_length = 0;
        if (!Validate(offset, key_length, value_length)
            || output_capacity < value_length) {
            return false;
        }
        const auto key_begin = bytes_.begin() + static_cast<std::ptrdiff_t>(
            offset + kRecordHeaderSize);
        if (!std::equal(key, key + key_length, key_begin))
            return false;
        const auto value_begin = key_begin + static_cast<std::ptrdiff_t>(key_length);
        std::copy_n(value_begin, value_length, output);
        output_length = value_length;
        return true;
    }

    size_t Size() const { return bytes_.size(); }

private:
    bool Validate(size_t offset, size_t key_length, uint32_t& value_length) const
    {
        if (offset < 2 || offset > bytes_.size()
            || bytes_.size() - offset < kRecordHeaderSize) {
            return false;
        }
        const uint8_t* header = bytes_.data() + offset;
        value_length = LoadU32(header);
        const uint16_t stored_key_length = LoadU16(header + 4);
        const uint16_t flags = LoadU16(header + 6);
        if (flags != 0 || stored_key_length != key_length)
            return false;
        const size_t payload_size = static_cast<size_t>(stored_key_length)
            + value_length;
        return payload_size <= bytes_.size() - offset - kRecordHeaderSize;
    }

    std::vector<uint8_t> bytes_;
};

class CurrentIndex {
public:
    CurrentIndex(size_t capacity, const RecordArena& records)
        : buckets_(new CurrentBucket[capacity])
        , capacity_(capacity)
        , mask_(capacity - 1)
        , records_(records)
    {
    }

    bool Insert(const uint8_t* key, size_t key_length, size_t offset)
    {
        const uint64_t hash = HashKey(key, key_length);
        const size_t start = static_cast<size_t>(hash) & mask_;
        for (size_t probe = 0; probe < capacity_; ++probe) {
            CurrentBucket& bucket = buckets_[(start + probe) & mask_];
            const uint64_t existing = bucket.hash.load(std::memory_order_relaxed);
            if (existing == kEmpty || existing == kTombstone) {
                bucket.record_offset.store(offset, std::memory_order_relaxed);
                bucket.hash.store(hash, std::memory_order_release);
                return true;
            }
            if (existing == hash
                && records_.KeyMatches(bucket.record_offset.load(
                        std::memory_order_acquire), key, key_length)) {
                return false;
            }
        }
        return false;
    }

    bool Lookup(const uint8_t* key, size_t key_length, uint8_t* output,
        size_t output_capacity, size_t& output_length) const
    {
        const uint64_t hash = HashKey(key, key_length);
        const size_t start = static_cast<size_t>(hash) & mask_;
        for (size_t probe = 0; probe < capacity_; ++probe) {
            const CurrentBucket& bucket = buckets_[(start + probe) & mask_];
            const uint64_t existing = bucket.hash.load(std::memory_order_acquire);
            if (existing == kEmpty)
                return false;
            if (existing != hash)
                continue;
            const size_t offset = bucket.record_offset.load(
                std::memory_order_acquire);
            if (records_.CopyIfMatches(offset, key, key_length, output,
                    output_capacity, output_length)) {
                return true;
            }
        }
        return false;
    }

    ProbeStats Analyze(const uint8_t* keys, size_t key_stride,
        const std::vector<size_t>& queries, size_t limit) const
    {
        ProbeStats stats;
        limit = std::min(limit, queries.size());
        for (size_t query = 0; query < limit; ++query) {
            const uint8_t* key = keys + queries[query] * key_stride;
            const uint64_t hash = HashKey(key, key_stride);
            const size_t start = static_cast<size_t>(hash) & mask_;
            size_t lookup_probes = 0;
            for (size_t probe = 0; probe < capacity_; ++probe) {
                ++lookup_probes;
                const CurrentBucket& bucket = buckets_[(start + probe) & mask_];
                const uint64_t existing = bucket.hash.load(
                    std::memory_order_relaxed);
                if (existing == kEmpty)
                    break;
                if (existing != hash)
                    continue;
                ++stats.record_checks;
                if (records_.KeyMatches(bucket.record_offset.load(
                        std::memory_order_relaxed), key, key_stride)) {
                    break;
                }
            }
            stats.probes += lookup_probes;
            stats.max_probes = std::max(stats.max_probes, lookup_probes);
            ++stats.lookups;
        }
        return stats;
    }

    size_t IndexBytes() const { return capacity_ * sizeof(CurrentBucket); }

private:
    std::unique_ptr<CurrentBucket[]> buckets_;
    size_t capacity_;
    size_t mask_;
    const RecordArena& records_;
};

class PackedIndex {
public:
    PackedIndex(size_t capacity, const RecordArena& records)
        : buckets_(new PackedBucket[capacity])
        , capacity_(capacity)
        , mask_(capacity - 1)
        , records_(records)
    {
    }

    bool Insert(const uint8_t* key, size_t key_length, size_t offset)
    {
        if (offset == 0 || offset > kOffsetMask)
            return false;
        const uint64_t hash = HashKey(key, key_length);
        const uint16_t tag = HashTag(hash);
        const uint64_t entry = PackEntry(hash, offset);
        const size_t start = static_cast<size_t>(hash) & mask_;
        for (size_t probe = 0; probe < capacity_; ++probe) {
            PackedBucket& bucket = buckets_[(start + probe) & mask_];
            const uint64_t existing = bucket.entry.load(std::memory_order_relaxed);
            if (existing == kEmpty || existing == kTombstone) {
                bucket.entry.store(entry, std::memory_order_release);
                return true;
            }
            if (static_cast<uint16_t>(existing >> 48) == tag
                && records_.KeyMatches(static_cast<size_t>(existing & kOffsetMask),
                    key, key_length)) {
                return false;
            }
        }
        return false;
    }

    bool Lookup(const uint8_t* key, size_t key_length, uint8_t* output,
        size_t output_capacity, size_t& output_length) const
    {
        const uint64_t hash = HashKey(key, key_length);
        const uint16_t tag = HashTag(hash);
        const size_t start = static_cast<size_t>(hash) & mask_;
        for (size_t probe = 0; probe < capacity_; ++probe) {
            const uint64_t entry = buckets_[(start + probe) & mask_].entry.load(
                std::memory_order_acquire);
            if (entry == kEmpty)
                return false;
            if (entry == kTombstone
                || static_cast<uint16_t>(entry >> 48) != tag) {
                continue;
            }
            if (records_.CopyIfMatches(static_cast<size_t>(entry & kOffsetMask),
                    key, key_length, output, output_capacity, output_length)) {
                return true;
            }
        }
        return false;
    }

    ProbeStats Analyze(const uint8_t* keys, size_t key_stride,
        const std::vector<size_t>& queries, size_t limit) const
    {
        ProbeStats stats;
        limit = std::min(limit, queries.size());
        for (size_t query = 0; query < limit; ++query) {
            const uint8_t* key = keys + queries[query] * key_stride;
            const uint64_t hash = HashKey(key, key_stride);
            const uint16_t tag = HashTag(hash);
            const size_t start = static_cast<size_t>(hash) & mask_;
            size_t lookup_probes = 0;
            for (size_t probe = 0; probe < capacity_; ++probe) {
                ++lookup_probes;
                const uint64_t entry = buckets_[(start + probe) & mask_]
                                           .entry.load(std::memory_order_relaxed);
                if (entry == kEmpty)
                    break;
                if (entry == kTombstone
                    || static_cast<uint16_t>(entry >> 48) != tag) {
                    continue;
                }
                ++stats.record_checks;
                if (records_.KeyMatches(static_cast<size_t>(entry & kOffsetMask),
                        key, key_stride)) {
                    break;
                }
            }
            stats.probes += lookup_probes;
            stats.max_probes = std::max(stats.max_probes, lookup_probes);
            ++stats.lookups;
        }
        return stats;
    }

    size_t IndexBytes() const { return capacity_ * sizeof(PackedBucket); }

private:
    std::unique_ptr<PackedBucket[]> buckets_;
    size_t capacity_;
    size_t mask_;
    const RecordArena& records_;
};

bool IsPowerOfTwo(size_t value)
{
    return value != 0 && (value & (value - 1)) == 0;
}

size_t ParseSize(const char* text, const char* name)
{
    char* end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0'
        || parsed > std::numeric_limits<size_t>::max()) {
        std::cerr << "Invalid " << name << ": " << text << "\n";
        std::exit(2);
    }
    return static_cast<size_t>(parsed);
}

int PinToFirstAllowedCpu()
{
    cpu_set_t allowed;
    CPU_ZERO(&allowed);
    if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0)
        return -1;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (!CPU_ISSET(cpu, &allowed))
            continue;
        cpu_set_t selected;
        CPU_ZERO(&selected);
        CPU_SET(cpu, &selected);
        return sched_setaffinity(0, sizeof(selected), &selected) == 0 ? cpu : -1;
    }
    return -1;
}

__attribute__((noinline)) uint64_t ConsumeValue(const uint8_t* value,
    size_t length)
{
    asm volatile("" : : "r"(value), "r"(length) : "memory");
    if (length == 0)
        return 0;
    return static_cast<uint64_t>(value[0])
        + (static_cast<uint64_t>(value[length - 1]) << 8);
}

template <typename Index>
TimedResult Measure(const Index& index, const std::vector<uint8_t>& keys,
    size_t key_length, const std::vector<size_t>& queries, size_t value_length,
    bool expect_hits)
{
    std::vector<uint8_t> output(value_length);
    uint64_t checksum = 0;
    uint64_t errors = 0;
    const auto begin = std::chrono::steady_clock::now();
    for (size_t key_index : queries) {
        size_t copied = 0;
        const bool found = index.Lookup(keys.data() + key_index * key_length,
            key_length, output.data(), output.size(), copied);
        if (found != expect_hits) {
            ++errors;
            continue;
        }
        if (found)
            checksum += ConsumeValue(output.data(), copied);
    }
    const auto end = std::chrono::steady_clock::now();
    const double elapsed = std::chrono::duration<double, std::nano>(end - begin)
                               .count();
    return { elapsed / static_cast<double>(queries.size()), checksum, errors };
}

double Median(std::vector<double> values)
{
    std::sort(values.begin(), values.end());
    return values[values.size() / 2];
}

void PrintMemory(const char* name, size_t index_bytes, size_t record_bytes,
    size_t entries)
{
    constexpr double kMiB = 1024.0 * 1024.0;
    std::cout << std::left << std::setw(18) << name << std::right
              << " index=" << std::fixed << std::setprecision(3)
              << static_cast<double>(index_bytes) / kMiB << " MiB"
              << "  index/live="
              << static_cast<double>(index_bytes) / entries << " B"
              << "  index+records="
              << static_cast<double>(index_bytes + record_bytes) / kMiB
              << " MiB\n";
}

void PrintProbeStats(const char* table, const char* workload,
    const ProbeStats& stats)
{
    std::cout << std::left << std::setw(18) << table << std::setw(7)
              << workload << std::right << " avg_probes=" << std::fixed
              << std::setprecision(4)
              << static_cast<double>(stats.probes) / stats.lookups
              << "  max_probes=" << stats.max_probes
              << "  record_checks/lookup="
              << static_cast<double>(stats.record_checks) / stats.lookups
              << "\n";
}

} // namespace

int main(int argc, char** argv)
{
    size_t entries = 838000;
    size_t lookups = 5000000;
    size_t capacity = 1048576;
    size_t key_length = 4;
    size_t value_length = 32;
    size_t rounds = 7;
    if (argc > 1)
        entries = ParseSize(argv[1], "entries");
    if (argc > 2)
        lookups = ParseSize(argv[2], "lookups");
    if (argc > 3)
        capacity = ParseSize(argv[3], "capacity");
    if (argc > 4)
        key_length = ParseSize(argv[4], "key_length");
    if (argc > 5)
        value_length = ParseSize(argv[5], "value_length");
    if (argc > 6)
        rounds = ParseSize(argv[6], "rounds");

    if (entries == 0 || entries >= capacity || !IsPowerOfTwo(capacity)
        || lookups == 0 || key_length < 4 || key_length > 256
        || value_length == 0 || value_length > 32767 || rounds < 3
        || (rounds & 1U) == 0) {
        std::cerr << "Usage: " << argv[0]
                  << " [entries] [lookups] [power_of_two_capacity]"
                     " [key_length] [value_length] [odd_rounds>=3]\n";
        return 2;
    }
    if (key_length == 4
        && entries > static_cast<size_t>(std::numeric_limits<uint32_t>::max())
            / 2) {
        std::cerr << "4-byte key mode needs room for unique miss keys\n";
        return 2;
    }
    if (entries > std::numeric_limits<size_t>::max() / key_length
        || std::min(entries, lookups)
            > std::numeric_limits<size_t>::max() / key_length) {
        std::cerr << "key storage size overflow\n";
        return 2;
    }

    const int cpu = PinToFirstAllowedCpu();
    const size_t record_size = kRecordHeaderSize + key_length + value_length;
    if (entries > (std::numeric_limits<size_t>::max() - 4096) / record_size) {
        std::cerr << "record arena size overflow\n";
        return 2;
    }
    RecordArena records(4096 + entries * record_size);
    std::vector<uint8_t> keys(entries * key_length);
    std::vector<uint8_t> values(value_length);
    std::vector<size_t> offsets(entries);

    std::cout << "HashMap bucket-layout A/B benchmark\n"
              << "  layouts: legacy 16-byte vs packed 8-byte\n"
              << "  hash: FNV-1a 64 (current default build)\n"
              << "  entries: " << entries << "\n"
              << "  capacity: " << capacity << "\n"
              << "  load: " << std::fixed << std::setprecision(3)
              << 100.0 * static_cast<double>(entries) / capacity << "%\n"
              << "  key/value bytes: " << key_length << "/" << value_length
              << "\n  timed lookups: " << lookups << " per round\n"
              << "  rounds: " << rounds << " (alternating order)\n"
              << "  pinned CPU: " << cpu << "\n" << std::flush;

    for (size_t index = 0; index < entries; ++index) {
        uint8_t* key = keys.data() + index * key_length;
        BuildKey(index, key_length, key);
        BuildValue(index, value_length, values.data());
        offsets[index] = records.Add(key, static_cast<uint16_t>(key_length),
            values.data(), static_cast<uint32_t>(value_length));
        if (offsets[index] == 0) {
            std::cerr << "record allocation failed at " << index << "\n";
            return 3;
        }
    }

    CurrentIndex current(capacity, records);
    PackedIndex packed(capacity, records);
    for (size_t index = 0; index < entries; ++index) {
        const uint8_t* key = keys.data() + index * key_length;
        if (!current.Insert(key, key_length, offsets[index])
            || !packed.Insert(key, key_length, offsets[index])) {
            std::cerr << "table insertion failed at " << index << "\n";
            return 3;
        }
    }

    const size_t miss_key_count = std::min(entries, lookups);
    std::vector<uint8_t> miss_keys(miss_key_count * key_length);
    for (size_t index = 0; index < miss_key_count; ++index) {
        BuildKey(entries + index + 1, key_length,
            miss_keys.data() + index * key_length);
    }

    std::vector<size_t> hit_queries(lookups);
    std::vector<size_t> miss_queries(lookups);
    std::mt19937_64 random(0x5EEDBACCULL);
    std::uniform_int_distribution<size_t> hit_select(0, entries - 1);
    std::uniform_int_distribution<size_t> miss_select(0, miss_key_count - 1);
    for (size_t index = 0; index < lookups; ++index) {
        hit_queries[index] = hit_select(random);
        miss_queries[index] = miss_select(random);
    }

    // Full correctness validation is outside the timed region.
    std::vector<uint8_t> current_value(value_length);
    std::vector<uint8_t> packed_value(value_length);
    std::vector<uint8_t> expected_value(value_length);
    for (size_t index = 0; index < entries; ++index) {
        size_t current_length = 0;
        size_t packed_length = 0;
        const uint8_t* key = keys.data() + index * key_length;
        BuildValue(index, value_length, expected_value.data());
        if (!current.Lookup(key, key_length, current_value.data(), value_length,
                current_length)
            || !packed.Lookup(key, key_length, packed_value.data(), value_length,
                packed_length)
            || current_length != value_length || packed_length != value_length
            || current_value != expected_value || packed_value != expected_value) {
            std::cerr << "lookup validation failed at " << index << "\n";
            return 4;
        }
    }
    for (size_t index = 0; index < std::min<size_t>(miss_key_count, 100000);
         ++index) {
        size_t ignored = 0;
        const uint8_t* key = miss_keys.data() + index * key_length;
        if (current.Lookup(key, key_length, current_value.data(), value_length,
                ignored)
            || packed.Lookup(key, key_length, packed_value.data(), value_length,
                ignored)) {
            std::cerr << "miss validation failed at " << index << "\n";
            return 4;
        }
    }
    std::cout << "Correctness validation: PASS\n\n";

    std::cout << "Memory (fixed production header excluded; identical in both):\n";
    PrintMemory("legacy-16B", current.IndexBytes(), records.Size(), entries);
    PrintMemory("packed-8B", packed.IndexBytes(), records.Size(), entries);
    std::cout << "Index reduction: " << std::setprecision(2)
              << 100.0 * (1.0 - static_cast<double>(packed.IndexBytes())
                      / current.IndexBytes())
              << "%\n\n";

    const size_t analysis_limit = std::min<size_t>(lookups, 1000000);
    const ProbeStats current_hits = current.Analyze(keys.data(), key_length,
        hit_queries, analysis_limit);
    const ProbeStats packed_hits = packed.Analyze(keys.data(), key_length,
        hit_queries, analysis_limit);
    const ProbeStats current_misses = current.Analyze(miss_keys.data(),
        key_length, miss_queries, analysis_limit);
    const ProbeStats packed_misses = packed.Analyze(miss_keys.data(), key_length,
        miss_queries, analysis_limit);
    std::cout << "Probe behavior (" << analysis_limit << " queries):\n";
    PrintProbeStats("legacy-16B", "hits", current_hits);
    PrintProbeStats("packed-8B", "hits", packed_hits);
    PrintProbeStats("legacy-16B", "misses", current_misses);
    PrintProbeStats("packed-8B", "misses", packed_misses);
    std::cout << "\n";

    // Warm both layouts before collecting alternating rounds.
    const size_t warm_count = std::min<size_t>(lookups, 1000000);
    std::vector<size_t> warm_queries(hit_queries.begin(),
        hit_queries.begin() + static_cast<std::ptrdiff_t>(warm_count));
    (void)Measure(current, keys, key_length, warm_queries, value_length, true);
    (void)Measure(packed, keys, key_length, warm_queries, value_length, true);

    std::vector<double> current_hit_ns;
    std::vector<double> packed_hit_ns;
    std::vector<double> current_miss_ns;
    std::vector<double> packed_miss_ns;
    uint64_t expected_checksum = 0;
    for (size_t round = 0; round < rounds; ++round) {
        TimedResult current_hit;
        TimedResult packed_hit;
        TimedResult current_miss;
        TimedResult packed_miss;
        if ((round & 1U) == 0) {
            current_hit = Measure(current, keys, key_length, hit_queries,
                value_length, true);
            packed_hit = Measure(packed, keys, key_length, hit_queries,
                value_length, true);
            current_miss = Measure(current, miss_keys, key_length, miss_queries,
                value_length, false);
            packed_miss = Measure(packed, miss_keys, key_length, miss_queries,
                value_length, false);
        } else {
            packed_hit = Measure(packed, keys, key_length, hit_queries,
                value_length, true);
            current_hit = Measure(current, keys, key_length, hit_queries,
                value_length, true);
            packed_miss = Measure(packed, miss_keys, key_length, miss_queries,
                value_length, false);
            current_miss = Measure(current, miss_keys, key_length, miss_queries,
                value_length, false);
        }
        if (round == 0)
            expected_checksum = current_hit.checksum;
        if (current_hit.errors != 0 || packed_hit.errors != 0
            || current_miss.errors != 0 || packed_miss.errors != 0
            || current_hit.checksum != expected_checksum
            || packed_hit.checksum != expected_checksum) {
            std::cerr << "timed correctness failure in round " << round << "\n";
            return 5;
        }
        current_hit_ns.push_back(current_hit.nanoseconds_per_lookup);
        packed_hit_ns.push_back(packed_hit.nanoseconds_per_lookup);
        current_miss_ns.push_back(current_miss.nanoseconds_per_lookup);
        packed_miss_ns.push_back(packed_miss.nanoseconds_per_lookup);
        std::cout << "round " << (round + 1)
                  << ": hit legacy/packed=" << std::setprecision(3)
                  << current_hit.nanoseconds_per_lookup << "/"
                  << packed_hit.nanoseconds_per_lookup
                  << " ns  miss legacy/packed="
                  << current_miss.nanoseconds_per_lookup << "/"
                  << packed_miss.nanoseconds_per_lookup << " ns\n"
                  << std::flush;
    }

    const double current_hit_median = Median(current_hit_ns);
    const double packed_hit_median = Median(packed_hit_ns);
    const double current_miss_median = Median(current_miss_ns);
    const double packed_miss_median = Median(packed_miss_ns);
    std::cout << "\nMedian lookup time:\n"
              << "  hits  legacy=" << std::setprecision(3)
              << current_hit_median << " ns  packed=" << packed_hit_median
              << " ns  change="
              << 100.0 * (packed_hit_median / current_hit_median - 1.0)
              << "%\n"
              << "  misses legacy=" << current_miss_median
              << " ns  packed=" << packed_miss_median << " ns  change="
              << 100.0 * (packed_miss_median / current_miss_median - 1.0)
              << "%\n";
    return 0;
}
