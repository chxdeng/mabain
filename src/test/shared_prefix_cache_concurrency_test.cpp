/**
 * Concurrent add/remove + lookup test exercising shared prefix cache
 */

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <iomanip>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>

#include "db.h"
#include "error.h"
#include "mabain_consts.h"
#include "util/utils.h"
#include <filesystem>

using namespace mabain;

struct Timer {
    std::chrono::steady_clock::time_point t0;
    void start() { t0 = std::chrono::steady_clock::now(); }
    double sec() const { return std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count(); }
};

static std::string make_key(uint32_t i)
{
    char buf[32];
    // Hex prefix keeps first bytes diverse for prefix cache
    std::snprintf(buf, sizeof(buf), "k%08x", i);
    return std::string(buf);
}

struct TestConfig {
    std::string dbdir = "/var/tmp/mabain_test";
    int nkeys = 100000;
    int nreaders = 4;
    int pfx_n = 4;
    size_t pfx_cap = 131072;
    uint32_t pfx_assoc = 8;
    int window = 1000; // interleave add/remove distance
    int hot_span = 0;  // readers sample within last hot_span keys (0=uniform over all)
};

struct Metrics {
    std::atomic<bool> stop_read{false};
    std::atomic<bool> validation_failed{false};
    std::atomic<uint64_t> hits{0};
    std::atomic<uint64_t> misses{0};
    std::atomic<uint64_t> ops{0};
    std::atomic<uint64_t> us{0};
};

static void ensure_clean_db(const std::string& dbdir)
{
    namespace fs = std::filesystem;
    try {
        fs::create_directories(dbdir);
        for (const auto& entry : fs::directory_iterator(dbdir)) {
            std::error_code ec;
            fs::remove_all(entry.path(), ec);
        }
    } catch (const std::exception& ex) {
        std::cerr << "Failed to clean db dir '" << dbdir << "': " << ex.what() << std::endl;
    }
}

static bool initialize_db_header(const TestConfig& cfg)
{
    DB writer(cfg.dbdir.c_str(), CONSTS::WriterOptions());
    if (!writer.is_open()) {
        std::cerr << "writer init open failed: " << writer.StatusStr() << std::endl;
        return false;
    }
    // Optionally enable shared prefix cache to create its backing file early
    writer.EnableSharedPrefixCache(cfg.pfx_n, cfg.pfx_cap, cfg.pfx_assoc);
    // writer goes out of scope here and closes; header and directory are now created
    return true;
}

static std::vector<std::string> generate_keys(int n)
{
    std::vector<std::string> keys;
    keys.reserve(n);
    for (int i = 0; i < n; ++i) keys.emplace_back(make_key(i));
    return keys;
}

static void reader_thread_fn(const TestConfig& cfg, const std::vector<std::string>& keys, Metrics& m, const std::atomic<int>* last_added)
{
    DB rdb(cfg.dbdir.c_str(), CONSTS::ReaderOptions());
    if (!rdb.is_open()) {
        std::cerr << "reader open failed" << std::endl;
        return;
    }
    rdb.EnableSharedPrefixCache(cfg.pfx_n, cfg.pfx_cap, cfg.pfx_assoc);
    std::mt19937 rng(1234 ^ (unsigned)reinterpret_cast<uintptr_t>(&cfg));
    std::uniform_int_distribution<int> dist_all(0, cfg.nkeys - 1);
    MBData md;
    while (!m.stop_read.load(std::memory_order_relaxed)) {
        int i;
        if (cfg.hot_span > 0) {
            int la = last_added ? last_added->load(std::memory_order_relaxed) : -1;
            if (la > 0) {
                int lo = std::max(0, la - cfg.hot_span + 1);
                int hi = la;
                if (hi >= lo) {
                    std::uniform_int_distribution<int> dist_hot(lo, hi);
                    i = dist_hot(rng);
                } else {
                    i = dist_all(rng);
                }
            } else {
                i = dist_all(rng);
            }
        } else {
            i = dist_all(rng);
        }
        auto t0 = std::chrono::steady_clock::now();
        int rc = rdb.Find(keys[i], md);
        auto t1 = std::chrono::steady_clock::now();
        uint64_t dur_us = (uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
        m.us.fetch_add(dur_us, std::memory_order_relaxed);
        m.ops.fetch_add(1, std::memory_order_relaxed);
        if (rc == MBError::SUCCESS)
            m.hits.fetch_add(1, std::memory_order_relaxed);
        else if (rc == MBError::NOT_EXIST)
            m.misses.fetch_add(1, std::memory_order_relaxed);

        // Validation happens outside timing window
        if (rc == MBError::SUCCESS) {
            const std::string got((const char*)md.buff, md.data_len);
            if (got != keys[i]) {
                std::cerr << "Validation failed: key=" << keys[i] << " value=" << got << std::endl;
                m.validation_failed.store(true, std::memory_order_relaxed);
                m.stop_read.store(true, std::memory_order_relaxed);
                break;
            }
        }
    }
}

static void start_readers(const TestConfig& cfg, const std::vector<std::string>& keys, Metrics& m, std::vector<std::thread>& out, const std::atomic<int>* last_added)
{
    out.reserve(cfg.nreaders);
    for (int i = 0; i < cfg.nreaders; ++i) {
        out.emplace_back(reader_thread_fn, std::ref(cfg), std::cref(keys), std::ref(m), last_added);
    }
}

static void stop_readers(Metrics& m, std::vector<std::thread>& readers)
{
    m.stop_read.store(true);
    for (auto& th : readers) th.join();
}

static bool run_interleaved_workload(const TestConfig& cfg, const std::vector<std::string>& keys, double& work_sec, std::atomic<int>& last_added)
{
    DB writer(cfg.dbdir.c_str(), CONSTS::WriterOptions());
    if (!writer.is_open()) {
        std::cerr << "writer open failed: " << writer.StatusStr() << std::endl;
        return false;
    }
    writer.EnableSharedPrefixCache(cfg.pfx_n, cfg.pfx_cap, cfg.pfx_assoc);

    Timer t; t.start();
    for (int i = 0; i < cfg.nkeys; ++i) {
        // Value equals key for validation
        const std::string& value = keys[i];
        int rc = writer.Add(keys[i], value, /*overwrite*/ false);
        if (rc != MBError::SUCCESS && rc != MBError::IN_DICT) {
            std::cerr << "Add failed at i=" << i << " rc=" << MBError::get_error_str(rc) << std::endl;
            break;
        }
        last_added.store(i, std::memory_order_relaxed);
        if (i >= cfg.window) {
            int rrc = writer.Remove(keys[i - cfg.window]);
            if (rrc != MBError::SUCCESS && rrc != MBError::NOT_EXIST) {
                std::cerr << "Remove failed at i=" << (i - cfg.window) << " rc=" << MBError::get_error_str(rrc) << std::endl;
                break;
            }
        }
    }
    for (int i = std::max(0, cfg.nkeys - cfg.window); i < cfg.nkeys; ++i) {
        int rrc = writer.Remove(keys[i]);
        if (rrc != MBError::SUCCESS && rrc != MBError::NOT_EXIST) {
            std::cerr << "Final remove failed at i=" << i << " rc=" << MBError::get_error_str(rrc) << std::endl;
        }
    }
    writer.Flush();
    work_sec = t.sec();
    return true;
}

static bool verify_all_removed(DB& db, const std::vector<std::string>& keys)
{
    for (size_t i = 0; i < keys.size(); ++i) {
        MBData md; int rc = db.Find(keys[i], md);
        if (rc != MBError::NOT_EXIST)
            return false;
    }
    return true;
}

int main(int argc, char** argv)
{
    if (argc == 1) {
        std::cerr << "Usage: " << argv[0]
                  << " <nkeys> <readers> [found_rate] [prefix_len]" << std::endl
                  << "  - nkeys: total unique keys to insert/remove (e.g., 2000000)" << std::endl
                  << "  - readers: number of reader threads (e.g., 4)" << std::endl
                  << "  - found_rate (optional): target found rate, as fraction or percent" << std::endl
                  << "      examples: 0.95, 95, 99.5 (defaults to internal uniform sampling if omitted)" << std::endl
                  << "  - prefix_len (optional): shared prefix cache length (1..8), default 4" << std::endl
                  << std::endl
                  << "Examples:" << std::endl
                  << "  " << argv[0] << " 2000000 4" << std::endl
                  << "  " << argv[0] << " 2000000 4 0.95" << std::endl
                  << "  " << argv[0] << " 2000000 4 99.5 5" << std::endl;
        return 1;
    }
    TestConfig cfg;
    if (argc > 1) cfg.nkeys = std::atoi(argv[1]);
    if (argc > 2) cfg.nreaders = std::atoi(argv[2]);

    // Single-parameter mode for target found rate (3rd arg), replaces manual window/hot_span.
    double target_found = -1.0;
    if (argc > 3) {
        target_found = std::atof(argv[3]);
        if (target_found > 1.0) // allow percentage input like 90 or 99.5
            target_found /= 100.0;
        if (target_found <= 0.0)
            target_found = 0.01;
        if (target_found > 1.0)
            target_found = 1.0;
        // Derive internal window/hot_span to approximate the target found rate without exposing two knobs.
        // Strategy: keep a bounded live window for writer; bias readers to recent keys to achieve the rate.
        const int max_window = std::min(cfg.nkeys, 10000); // cap live set to keep runtime stable
        cfg.window = std::max(1, max_window);
        cfg.hot_span = std::max(1, static_cast<int>(std::lround(cfg.window / target_found)));
    }
    if (argc > 4) {
        int pn = std::atoi(argv[4]);
        if (pn < 1) pn = 1;
        if (pn > 8) pn = 8;
        cfg.pfx_n = pn;
    }

    std::cout << "shared_prefix_cache_concurrency_test: nkeys=" << cfg.nkeys
              << " readers=" << cfg.nreaders
              << " pfx_n=" << cfg.pfx_n << " cap=" << cfg.pfx_cap << " assoc=" << cfg.pfx_assoc
              << " target_found_rate=" << (target_found > 0 ? target_found : -1)
              << " window=" << cfg.window
              << " hot_span=" << cfg.hot_span
              << std::endl;
    ensure_clean_db(cfg.dbdir);
    if (!initialize_db_header(cfg)) return 1;
    auto keys = generate_keys(cfg.nkeys);
    if (!keys.empty()) {
        std::cout << "Key size: " << keys[0].size() << " bytes, Value size: " << keys[0].size()
                  << " bytes" << std::endl;
    }
    Metrics m;
    std::vector<std::thread> readers;
    std::atomic<int> last_added{-1};
    start_readers(cfg, keys, m, readers, &last_added);

    double work_sec = 0.0;
    bool ok = run_interleaved_workload(cfg, keys, work_sec, last_added);
    stop_readers(m, readers);
    if (!ok) return 1;
    if (m.validation_failed.load()) return 2;

    // Open a fresh handle to dump shared prefix cache; use reader to avoid resetting the cache
    DB verify_db(cfg.dbdir.c_str(), CONSTS::ReaderOptions());
    if (!verify_db.is_open()) {
        std::cerr << "verify writer open failed: " << verify_db.StatusStr() << std::endl;
        return 1;
    }
    verify_db.EnableSharedPrefixCache(cfg.pfx_n, cfg.pfx_cap, cfg.pfx_assoc);
    bool all_removed = verify_all_removed(verify_db, keys);
    std::cout << "Post-remove verification " << (all_removed ? "OK" : "FAILED")
              << " (" << (all_removed ? cfg.nkeys : 0) << "/" << cfg.nkeys << ")" << std::endl;

    uint64_t ops = m.ops.load();
    uint64_t us = m.us.load();
    uint64_t hits = m.hits.load();
    uint64_t misses = m.misses.load();
    double avg_us = ops ? (double)us / (double)ops : 0.0;
    double hit_pct = ops ? (100.0 * (double)hits / (double)ops) : 0.0;
    double miss_pct = ops ? (100.0 * (double)misses / (double)ops) : 0.0;

    std::cout << std::fixed << std::setprecision(6);
    std::cout << "Writer work time: " << work_sec << " s" << std::endl;
    std::cout << "Lookup summary" << std::endl;
    std::cout << "- Total lookups: " << ops << std::endl;
    std::cout << std::setprecision(2);
    std::cout << "- Found: " << hits << " (" << hit_pct << "%)" << std::endl;
    std::cout << "- Not found: " << misses << " (" << miss_pct << "%)" << std::endl;
    std::cout << std::setprecision(6);
    std::cout << "- Avg lookup time: " << avg_us << " us" << std::endl;
    std::cout << "Note: 'Not found' is expected while the writer is removing keys (sliding window)." << std::endl;

    std::cout << "Prefix cache note: counters below are cache-level events (seed hits/misses)"
              << "; they do not equal the found/not-found totals." << std::endl;
    verify_db.DumpSharedPrefixCacheStats(std::cout);
    return all_removed ? 0 : 2;
}
