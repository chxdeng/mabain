/**
 * Compare HashMap::Get with Mabain's radix-tree DB::Find lookup path.
 *
 * Both implementations receive the same keys and precomputed query order.
 * The radix tree is created with OPTION_PREFIX_CACHE, its lookup fast path.
 */

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iostream>
#include <limits>
#include <numeric>
#include <random>
#include <string>
#include <system_error>
#include <utility>
#include <unistd.h>
#include <vector>

#include "../db.h"
#include "../error.h"
#include "../hash_map_api.h"
#include "../mabain_consts.h"

using namespace mabain;

namespace {

constexpr size_t kMiB = 1024ULL * 1024ULL;
constexpr size_t kHashMapValueHeaderReserve = 16ULL * 1024ULL;

struct Cleanup {
    explicit Cleanup(std::string path)
        : path(std::move(path))
    {
    }

    ~Cleanup()
    {
        DB::ClearResources(path);
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }

    std::string path;
};

struct TimedResult {
    double ns_per_lookup = 0.0;
    size_t failures = 0;
    uint64_t checksum = 0;
};

bool ParseSize(const char* text, size_t& value)
{
    if (text == nullptr || *text == '\0')
        return false;
    errno = 0;
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || parsed == 0
        || parsed > std::numeric_limits<size_t>::max()) {
        return false;
    }
    value = static_cast<size_t>(parsed);
    return true;
}

bool ParseLoadFactor(const char* text, double& value)
{
    if (text == nullptr || *text == '\0')
        return false;
    errno = 0;
    char* end = nullptr;
    double parsed = std::strtod(text, &end);
    if (errno != 0 || end == text || *end != '\0' || !std::isfinite(parsed)
        || parsed < 0.20 || parsed > 0.90) {
        return false;
    }
    value = parsed;
    return true;
}

bool ParseBool01(const char* text, bool& value)
{
    if (text == nullptr || text[0] == '\0' || text[1] != '\0'
        || (text[0] != '0' && text[0] != '1')) {
        return false;
    }
    value = text[0] == '1';
    return true;
}

bool NextPowerOfTwo(size_t value, size_t& result)
{
    if (value <= 1) {
        result = 1;
        return true;
    }
    const size_t highest = size_t { 1 }
        << (std::numeric_limits<size_t>::digits - 1);
    if (value > highest)
        return false;
    --value;
    for (size_t shift = 1; shift < std::numeric_limits<size_t>::digits;
         shift <<= 1) {
        value |= value >> shift;
    }
    result = value + 1;
    return true;
}

std::vector<std::string> BuildKeys(size_t count, const std::string& key_mode)
{
    std::vector<std::string> keys;
    keys.reserve(count);
    std::mt19937_64 rng(0xC0FFEEULL);
    std::uniform_int_distribution<uint64_t> random64;

    for (size_t i = 0; i < count; ++i) {
        if (key_mode == "int32") {
            uint32_t value = static_cast<uint32_t>(i);
            std::string key(4, '\0');
            key[0] = static_cast<char>(value & 0xffU);
            key[1] = static_cast<char>((value >> 8) & 0xffU);
            key[2] = static_cast<char>((value >> 16) & 0xffU);
            key[3] = static_cast<char>((value >> 24) & 0xffU);
            keys.emplace_back(std::move(key));
        } else {
            keys.emplace_back("key_" + std::to_string(random64(rng)) + "_"
                + std::to_string(i));
        }
    }
    return keys;
}

std::vector<std::string> BuildValues(size_t count, size_t value_size)
{
    std::vector<std::string> values;
    values.reserve(count);
    for (size_t key_id = 0; key_id < count; ++key_id) {
        std::string value(value_size, '\0');
        for (size_t index = 0; index < value_size; ++index) {
            value[index] = static_cast<char>(
                (key_id * 31 + index * 17 + (key_id >> 8)) & 0xffU);
        }
        for (size_t byte = 0; byte < sizeof(uint64_t); ++byte) {
            value[byte] = static_cast<char>(
                (static_cast<uint64_t>(key_id) >> (byte * 8)) & 0xffU);
        }
        values.emplace_back(std::move(value));
    }
    return values;
}

bool WarmHashMap(const HashMap& map, const std::vector<std::string>& keys,
    const std::vector<std::string>& values,
    const std::vector<size_t>& queries, size_t warmup)
{
    MBData data(static_cast<int>(values.front().size()), 0);
    for (size_t q = 0; q < warmup; ++q) {
        const size_t expected = queries[q];
        const std::string& key = keys[expected];
        const std::string& expected_value = values[expected];
        if (map.GetValue(reinterpret_cast<const uint8_t*>(key.data()),
                static_cast<int>(key.size()), data)
                != MBError::SUCCESS
            || data.data_len != static_cast<int>(expected_value.size())
            || data.buff == nullptr
            || std::memcmp(data.buff, expected_value.data(),
                   expected_value.size())
                != 0) {
            return false;
        }
    }
    return true;
}

bool WarmRadix(const DB& db, const std::vector<std::string>& keys,
    const std::vector<std::string>& values,
    const std::vector<size_t>& queries, size_t warmup)
{
    MBData data(static_cast<int>(values.front().size()), 0);
    for (size_t q = 0; q < warmup; ++q) {
        const size_t expected = queries[q];
        const std::string& key = keys[expected];
        const std::string& expected_value = values[expected];
        if (db.Find(key.data(), static_cast<int>(key.size()), data)
                != MBError::SUCCESS
            || data.data_len != static_cast<int>(expected_value.size())
            || data.buff == nullptr
            || std::memcmp(data.buff, expected_value.data(),
                   expected_value.size())
                != 0) {
            return false;
        }
    }
    return true;
}

TimedResult MeasureHashMap(const HashMap& map,
    const std::vector<std::string>& keys,
    const std::vector<std::string>& values,
    const std::vector<size_t>& queries)
{
    TimedResult result;
    MBData data(static_cast<int>(values.front().size()), 0);
    const int expected_length = static_cast<int>(values.front().size());
    const auto begin = std::chrono::steady_clock::now();
    for (size_t q = 0; q < queries.size(); ++q) {
        const size_t expected = queries[q];
        const std::string& key = keys[expected];
        int lookup_result = map.GetValue(
            reinterpret_cast<const uint8_t*>(key.data()),
            static_cast<int>(key.size()), data);
        if (lookup_result != MBError::SUCCESS
            || data.data_len != expected_length || data.buff == nullptr) {
            ++result.failures;
        } else {
            result.checksum += data.buff[0] + data.buff[data.data_len - 1];
        }
    }
    const auto end = std::chrono::steady_clock::now();
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
        end - begin);
    result.ns_per_lookup = static_cast<double>(elapsed.count())
        / static_cast<double>(queries.size());
    return result;
}

TimedResult MeasureRadix(const DB& db, const std::vector<std::string>& keys,
    const std::vector<std::string>& values,
    const std::vector<size_t>& queries)
{
    TimedResult result;
    MBData data(static_cast<int>(values.front().size()), 0);
    const int expected_length = static_cast<int>(values.front().size());
    const auto begin = std::chrono::steady_clock::now();
    for (size_t q = 0; q < queries.size(); ++q) {
        const size_t expected = queries[q];
        const std::string& key = keys[expected];
        int rc = db.Find(key.data(), static_cast<int>(key.size()), data);
        if (rc != MBError::SUCCESS
            || data.data_len != expected_length || data.buff == nullptr) {
            ++result.failures;
        } else {
            result.checksum += data.buff[0] + data.buff[data.data_len - 1];
        }
    }
    const auto end = std::chrono::steady_clock::now();
    const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
        end - begin);
    result.ns_per_lookup = static_cast<double>(elapsed.count())
        / static_cast<double>(queries.size());
    return result;
}

double Average(const std::vector<double>& values)
{
    return std::accumulate(values.begin(), values.end(), 0.0)
        / static_cast<double>(values.size());
}

double Median(std::vector<double> values)
{
    std::sort(values.begin(), values.end());
    const size_t middle = values.size() / 2;
    if ((values.size() & 1U) != 0)
        return values[middle];
    return (values[middle - 1] + values[middle]) / 2.0;
}

void Usage(const char* program)
{
    std::cerr << "Usage: " << program
              << " [entries] [lookups] [rounds] [memory_mb]"
                 " [load_factor] [compact64] [int32|string] [value_bytes]\n";
}

} // namespace

int main(int argc, char** argv)
{
    size_t entries = 1000000;
    size_t lookups = 5000000;
    size_t rounds = 5;
    size_t memory_mb = 256;
    double load_factor = 0.5;
    bool compact64 = true;
    std::string key_mode = "int32";
    size_t value_size = 32;

    if ((argc > 1 && !ParseSize(argv[1], entries))
        || (argc > 2 && !ParseSize(argv[2], lookups))
        || (argc > 3 && !ParseSize(argv[3], rounds))
        || (argc > 4 && !ParseSize(argv[4], memory_mb))
        || (argc > 5 && !ParseLoadFactor(argv[5], load_factor))
        || (argc > 6 && !ParseBool01(argv[6], compact64))
        || (argc > 8 && !ParseSize(argv[8], value_size)) || argc > 9) {
        Usage(argv[0]);
        return 1;
    }
    if (argc > 7)
        key_mode = argv[7];
    if (value_size < sizeof(uint64_t)
        || value_size > static_cast<size_t>(CONSTS::MAX_DATA_SIZE)) {
        std::cerr << "value_bytes must be between 8 and "
                  << CONSTS::MAX_DATA_SIZE << "\n";
        return 1;
    }
    if (key_mode != "int32" && key_mode != "string") {
        Usage(argv[0]);
        return 1;
    }
    if (key_mode == "int32" && entries > std::numeric_limits<uint32_t>::max()) {
        std::cerr << "int32 mode supports at most 2^32 entries\n";
        return 1;
    }
    if (memory_mb > (std::numeric_limits<size_t>::max() >> 20)) {
        std::cerr << "memory_mb is too large\n";
        return 1;
    }
    if (memory_mb < 16 || memory_mb % 16 != 0) {
        std::cerr << "memory_mb must be a multiple of 16 for value mode\n";
        return 1;
    }

    const double requested_buckets = std::ceil(
        static_cast<double>(entries) / load_factor);
    if (requested_buckets > static_cast<double>(
                                std::numeric_limits<size_t>::max())) {
        std::cerr << "requested capacity is too large\n";
        return 1;
    }
    size_t capacity = 0;
    if (!NextPowerOfTwo(static_cast<size_t>(requested_buckets), capacity)) {
        std::cerr << "requested capacity cannot be rounded to a power of two\n";
        return 1;
    }
    capacity = std::max<size_t>(capacity, 1024);

    (void)compact64;
    const size_t bucket_size = 16;
    if (capacity
        > (std::numeric_limits<size_t>::max() - kHashMapValueHeaderReserve)
            / bucket_size) {
        std::cerr << "HashMap size calculation overflow\n";
        return 1;
    }
    const size_t required_hash_bytes
        = kHashMapValueHeaderReserve + capacity * bucket_size;
    const size_t configured_bytes = memory_mb << 20;
    if (required_hash_bytes > configured_bytes) {
        std::cerr << "HashMap requires at least "
                  << ((required_hash_bytes + kMiB - 1) / kMiB)
                  << " MiB for this configuration\n";
        return 1;
    }

    const std::string work_dir
        = "/var/tmp/mabain_hashmap_radix_lookup_" + std::to_string(getpid());
    std::error_code ec;
    if (!std::filesystem::create_directory(work_dir, ec)) {
        std::cerr << "Failed to create benchmark directory " << work_dir
                  << ": " << ec.message() << "\n";
        return 2;
    }
    Cleanup cleanup(work_dir);
    const std::string radix_dir = work_dir + "/radix";
    if (!std::filesystem::create_directory(radix_dir, ec)) {
        std::cerr << "Failed to create radix directory: " << ec.message()
                  << "\n";
        return 2;
    }
    const std::string hash_path = work_dir + "/hash";

    std::cout << "Preparing " << entries << " " << key_mode << " keys, "
              << value_size << "-byte per-key values, and " << lookups
              << " hit queries\n";
    std::vector<std::string> keys = BuildKeys(entries, key_mode);
    std::vector<std::string> values = BuildValues(entries, value_size);
    std::vector<size_t> queries;
    queries.reserve(lookups);
    std::mt19937_64 query_rng(0x5EEDULL);
    std::uniform_int_distribution<size_t> select(0, entries - 1);
    for (size_t q = 0; q < lookups; ++q)
        queries.push_back(select(query_rng));

    try {
        HashMapValueConfig value_config;
        value_config.value_block_size = 16ULL * kMiB;
        value_config.value_memcap = configured_bytes;
        value_config.reader_slots = 64;
        value_config.reclaim_threshold_bytes = 1ULL * kMiB;
        value_config.max_retired_bytes = std::min<size_t>(
            64ULL * kMiB, configured_bytes / 2);
        HashMap hash_map(hash_path, capacity, CONSTS::ACCESS_MODE_WRITER,
            value_config, memory_mb);

        MBConfig writer_config {};
        writer_config.mbdir = radix_dir.c_str();
        writer_config.options
            = CONSTS::WriterOptions() | CONSTS::OPTION_PREFIX_CACHE
            | CONSTS::OPTION_JEMALLOC;
        writer_config.block_size_index = 128ULL * kMiB;
        writer_config.block_size_data = 128ULL * kMiB;
        writer_config.max_num_index_block = 10;
        writer_config.max_num_data_block = 10;
        writer_config.memcap_index
            = writer_config.block_size_index
            * writer_config.max_num_index_block;
        writer_config.memcap_data
            = writer_config.block_size_data
            * writer_config.max_num_data_block;
        writer_config.queue_dir = radix_dir.c_str();
        DB radix_writer(writer_config);
        if (!radix_writer.is_open()) {
            std::cerr << "Failed to open radix writer: "
                      << radix_writer.StatusStr() << "\n";
            return 3;
        }

        for (size_t i = 0; i < entries; ++i) {
            const std::string& key = keys[i];
            const std::string& value = values[i];
            int hash_rc = hash_map.PutValue(
                reinterpret_cast<const uint8_t*>(key.data()),
                static_cast<int>(key.size()),
                reinterpret_cast<const uint8_t*>(value.data()),
                static_cast<int>(value.size()), true);
            if (hash_rc != MBError::SUCCESS) {
                std::cerr << "HashMap Put failed at " << i << ": "
                          << MBError::get_error_str(hash_rc) << "\n";
                return 4;
            }
            int radix_rc = radix_writer.Add(key, value, false);
            if (radix_rc != MBError::SUCCESS) {
                std::cerr << "Radix Add failed at " << i << ": "
                          << MBError::get_error_str(radix_rc) << "\n";
                return 5;
            }
        }

        MBConfig reader_config = writer_config;
        reader_config.options
            = CONSTS::ReaderOptions() | CONSTS::OPTION_PREFIX_CACHE;
        DB radix_reader(reader_config);
        if (!radix_reader.is_open()) {
            std::cerr << "Failed to open radix reader: "
                      << radix_reader.StatusStr() << "\n";
            return 6;
        }

        const size_t warmup = std::min(entries, lookups);
        std::vector<double> hash_times;
        std::vector<double> radix_times;
        hash_times.reserve(rounds);
        radix_times.reserve(rounds);
        uint64_t combined_checksum = 0;

        auto run_hash = [&]() -> bool {
            if (!WarmHashMap(hash_map, keys, values, queries, warmup))
                return false;
            TimedResult result
                = MeasureHashMap(hash_map, keys, values, queries);
            if (result.failures != 0)
                return false;
            hash_times.push_back(result.ns_per_lookup);
            combined_checksum += result.checksum;
            return true;
        };
        auto run_radix = [&]() -> bool {
            if (!WarmRadix(radix_reader, keys, values, queries, warmup))
                return false;
            TimedResult result = MeasureRadix(radix_reader, keys, values,
                queries);
            if (result.failures != 0)
                return false;
            radix_times.push_back(result.ns_per_lookup);
            combined_checksum += result.checksum;
            return true;
        };

        for (size_t round = 0; round < rounds; ++round) {
            bool ok = ((round & 1U) == 0)
                ? (run_hash() && run_radix())
                : (run_radix() && run_hash());
            if (!ok) {
                std::cerr << "Lookup validation failed in round " << round + 1
                          << "\n";
                return 7;
            }
            std::cout << "Round " << round + 1
                      << ": HashMap+value=" << hash_times.back()
                      << " ns, radix+prefix-cache=" << radix_times.back()
                      << " ns\n";
        }

        std::vector<size_t> validation_queries(entries);
        std::iota(validation_queries.begin(), validation_queries.end(), 0);
        if (!WarmHashMap(hash_map, keys, values, validation_queries, entries)
            || !WarmRadix(radix_reader, keys, values, validation_queries,
                entries)) {
            std::cerr << "Post-measurement full-value validation failed\n";
            return 8;
        }

        const double hash_average = Average(hash_times);
        const double radix_average = Average(radix_times);
        std::cout << "\nConfiguration:\n"
                  << "  HashMap capacity: " << capacity << "\n"
                  << "  HashMap value bucket: compact (16 bytes)\n"
                  << "  Stored value size: " << value_size << " bytes\n"
                  << "  HashMap load: "
                  << static_cast<double>(entries) / static_cast<double>(capacity)
                  << "\n"
                  << "  Radix mode: prefix cache + jemalloc\n"
                  << "  Warmup lookups per measurement: " << warmup << "\n"
                  << "\nResults:\n"
                  << "  HashMap+value average: " << hash_average
                  << " ns/lookup\n"
                  << "  HashMap+value median:  " << Median(hash_times)
                  << " ns/lookup\n"
                  << "  Radix average:   " << radix_average << " ns/lookup\n"
                  << "  Radix median:    " << Median(radix_times)
                  << " ns/lookup\n"
                  << "  HashMap+value speedup: "
                  << radix_average / hash_average
                  << "x\n"
                  << "  Validation: timed lookups copied complete values; all"
                     " stored key/value pairs matched in an untimed pass;"
                     " checksum="
                  << combined_checksum << "\n";
        radix_reader.DumpPrefixCacheStats(std::cout);

        radix_reader.Close();
        radix_writer.Close();
        hash_map.Flush();
    } catch (int error) {
        std::cerr << "Mabain exception: " << MBError::get_error_str(error)
                  << "\n";
        return 8;
    } catch (const std::exception& error) {
        std::cerr << "Exception: " << error.what() << "\n";
        return 8;
    }

    return 0;
}
