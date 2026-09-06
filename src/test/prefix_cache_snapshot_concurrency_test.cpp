/**
 * Stress the prefix-cache optimistic snapshot protocol on one hot slot.
 */

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>

#include "db.h"
#include "dict.h"
#include "mabain_consts.h"
#include "util/prefix_cache.h"

using namespace mabain;

namespace {

bool SameBody(const PrefixCacheEntry& lhs, const PrefixCacheEntry& rhs)
{
    return lhs.edge_offset == rhs.edge_offset
        && memcmp(lhs.edge_buff, rhs.edge_buff, sizeof(lhs.edge_buff)) == 0
        && lhs.edge_skip == rhs.edge_skip;
}

PrefixCacheEntry MakeEntry(uint8_t value, uint32_t origin)
{
    PrefixCacheEntry entry {};
    entry.edge_offset = value == 0x11 ? 0x1111111111111111ULL
                                      : 0x2222222222222222ULL;
    memset(entry.edge_buff, value, sizeof(entry.edge_buff));
    entry.edge_skip = value;
    entry.lf_counter = origin;
    return entry;
}

} // namespace

int main(int argc, char** argv)
{
    const uint64_t iterations = argc > 1
        ? static_cast<uint64_t>(std::strtoull(argv[1], nullptr, 10))
        : 500000;
    const unsigned reader_count = argc > 2
        ? static_cast<unsigned>(std::strtoul(argv[2], nullptr, 10))
        : 4;
    const std::string db_dir = "/var/tmp/mabain_prefix_cache_snapshot_test";

    std::error_code ec;
    std::filesystem::remove_all(db_dir, ec);
    std::filesystem::create_directories(db_dir, ec);
    if (ec) {
        std::cerr << "failed to prepare test directory: " << ec.message() << '\n';
        return 1;
    }

    DB writer(db_dir.c_str(),
        CONSTS::WriterOptions() | CONSTS::OPTION_PREFIX_CACHE);
    if (!writer.is_open()) {
        std::cerr << "writer open failed: " << writer.StatusStr() << '\n';
        return 1;
    }

    PrefixCache* cache = writer.GetDictPtr()->ActivePrefixCache();
    if (cache == nullptr) {
        std::cerr << "prefix cache is not active\n";
        return 1;
    }

    const uint8_t key[] = { 'p', 'c' };
    const PrefixCacheEntry entry_a = MakeEntry(0x11, 1);
    const PrefixCacheEntry entry_b = MakeEntry(0x22, 2);
    cache->PutAtDepth(key, 2, entry_a);

    std::atomic<bool> start { false };
    std::atomic<bool> stop { false };
    std::atomic<bool> failed { false };
    std::atomic<uint64_t> hits { 0 };
    std::atomic<uint64_t> misses { 0 };
    std::atomic<uint64_t> retries { 0 };

    std::vector<std::thread> readers;
    readers.reserve(reader_count);
    for (unsigned i = 0; i < reader_count; ++i) {
        readers.emplace_back([&] {
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();

            while (!stop.load(std::memory_order_acquire)) {
                PrefixCacheEntry out {};
                int depth = cache->GetDepth(key, 2, out);
                if (depth == PrefixCache::UNSTABLE) {
                    retries.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                if (depth == 0) {
                    misses.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                if (depth != 2 || (!SameBody(out, entry_a) && !SameBody(out, entry_b))) {
                    failed.store(true, std::memory_order_release);
                    stop.store(true, std::memory_order_release);
                    break;
                }
                hits.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (uint64_t i = 0; i < iterations && !failed.load(std::memory_order_acquire); ++i)
        cache->PutAtDepth(key, 2, (i & 1) ? entry_a : entry_b);
    stop.store(true, std::memory_order_release);

    for (std::thread& reader : readers)
        reader.join();

    PrefixCacheEntry final_entry {};
    int final_depth = cache->GetDepth(key, 2, final_entry);
    bool final_valid = final_depth == 2
        && (SameBody(final_entry, entry_a) || SameBody(final_entry, entry_b));

    std::cout << "prefix cache snapshot concurrency: hits=" << hits.load()
              << " misses=" << misses.load()
              << " retries=" << retries.load() << '\n';

    if (failed.load() || !final_valid || hits.load() == 0) {
        std::cerr << "prefix cache returned a torn or invalid stable snapshot\n";
        return 2;
    }
    return 0;
}
