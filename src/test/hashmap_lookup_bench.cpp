/**
 * Benchmark HashMap lookup performance.
 * Usage: ./hashmap_lookup_bench <n> [lookups] [mbdir]
 *   n: number of entries to insert
 *   lookups: number of random Get operations to perform (default: n)
 *   mbdir: base path for the hashmap backing file (default: ./test/hashmap_bench)
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "../error.h"
#include "../hash_map.h"
#include "../mabain_consts.h"

using namespace mabain;

static inline size_t ceil_pow2(size_t x)
{
    if (x <= 1)
        return 1;
    --x;
    for (size_t i = 1; i < sizeof(size_t) * 8; i <<= 1)
        x |= x >> i;
    return x + 1;
}

int main(int argc, char** argv)
{
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <n> [lookups] [mbdir] [mem_mb] [load_factor] [compact64]\n";
        return 1;
    }
    const size_t n = static_cast<size_t>(std::strtoull(argv[1], nullptr, 10));
    size_t num_lookups = (argc >= 3) ? static_cast<size_t>(std::strtoull(argv[2], nullptr, 10)) : n;
    std::string mbdir = (argc >= 4) ? argv[3] : std::string("./test/hashmap_bench");
    size_t mem_mb = (argc >= 5) ? static_cast<size_t>(std::strtoull(argv[4], nullptr, 10)) : 64;
    double load_factor = (argc >= 6) ? std::strtod(argv[5], nullptr) : 0.5;
    if (load_factor <= 0.1 || load_factor > 0.95)
        load_factor = 0.5;
    bool compact64 = (argc >= 7) ? (std::strtoull(argv[6], nullptr, 10) != 0) : true;

    const uint32_t inline_key = 24; // inline first 16 bytes for quick screening
    const size_t desired_capacity = std::max<size_t>(1024, ceil_pow2((size_t)std::ceil(n / load_factor)));

    // Build keys and values
    std::vector<std::string> keys;
    keys.reserve(n);
    std::mt19937_64 rng(0xC0FFEEULL);
    std::uniform_int_distribution<uint64_t> dist64;
    for (size_t i = 0; i < n; ++i) {
        uint64_t r = dist64(rng);
        // Generate variable-length key with a fixed prefix
        std::string k = "key_" + std::to_string(r) + "_" + std::to_string(i);
        keys.emplace_back(std::move(k));
    }

    // Create hashmap and insert entries
    HashMap hmap(mbdir, desired_capacity, CONSTS::ACCESS_MODE_WRITER, /*num_stripes (ignored)*/ 1, inline_key, mem_mb, compact64);
    for (size_t i = 0; i < n; ++i) {
        const auto& k = keys[i];
        int rc = hmap.Put(reinterpret_cast<const uint8_t*>(k.data()), static_cast<int>(k.size()), /*ref_offset*/ i, /*overwrite*/ true);
        if (rc != MBError::SUCCESS) {
            std::cerr << "Put failed at i=" << i << " rc=" << rc << "\n";
            return 2;
        }
    }

    // Random lookups (precompute everything so timing only includes Get)
    std::uniform_int_distribution<size_t> dist_idx(0, n ? (n - 1) : 0);
    std::vector<const uint8_t*> qkeys(num_lookups);
    std::vector<int> qlens(num_lookups);
    for (size_t q = 0; q < num_lookups; ++q) {
        size_t i = dist_idx(rng);
        const auto& k = keys[i];
        qkeys[q] = reinterpret_cast<const uint8_t*>(k.data());
        qlens[q] = static_cast<int>(k.size());
    }
    // Warm-up a little to fault in pages (not timed)
    for (size_t q = 0; q < std::min<size_t>(num_lookups, 1000); ++q) {
        size_t ref = 0;
        (void)hmap.Get(qkeys[q], qlens[q], ref);
    }
    size_t hits = 0;
    auto t0 = std::chrono::steady_clock::now();
    for (size_t q = 0; q < num_lookups; ++q) {
        size_t ref = 0;
        bool ok = hmap.Get(qkeys[q], qlens[q], ref);
        hits += ok;
    }
    auto t1 = std::chrono::steady_clock::now();
    auto ns_total = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
    double avg_ns = num_lookups ? (double)ns_total / (double)num_lookups : 0.0;

    std::cout << "Inserted: " << n << "\n"
              << "Lookups: " << num_lookups << "\n"
              << "Hits:     " << hits << " (" << (num_lookups ? (100.0 * hits / num_lookups) : 0.0) << "%)\n"
              << "Avg Get:  " << avg_ns << " ns per lookup\n";

    hmap.Flush();
    return 0;
}
