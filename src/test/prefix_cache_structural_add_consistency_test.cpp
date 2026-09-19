/**
 * Reproduces a prefix-cache reader retaining a released radix node across a
 * structural Add. Build Mabain and this test with
 * MABAIN_PREFIX_CACHE_CONSISTENCY_TEST_HOOKS enabled.
 */

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>

#include "db.h"
#include "dict.h"
#include "drm_base.h"
#include "error.h"
#include "integer_4b_5b.h"
#include "mabain_consts.h"
#include "util/prefix_cache.h"

using namespace mabain;

#ifndef MABAIN_PREFIX_CACHE_CONSISTENCY_TEST_HOOKS
#error "This test requires MABAIN_PREFIX_CACHE_CONSISTENCY_TEST_HOOKS"
#endif

namespace {

constexpr auto kTimeout = std::chrono::seconds(5);
constexpr const char* kDbDir
    = "/var/tmp/mabain_prefix_cache_structural_add_consistency";

std::atomic<bool> arm_writer_hook { false };
std::atomic<bool> arm_reader_hook { false };
std::atomic<bool> writer_at_cache_gap { false };
std::atomic<bool> reader_copied_cache { false };
std::atomic<bool> release_reader { false };
std::atomic<bool> reader_completed { false };
std::atomic<bool> hook_timeout { false };

bool WaitFor(const std::atomic<bool>& condition)
{
    const auto deadline = std::chrono::steady_clock::now() + kTimeout;
    while (!condition.load(std::memory_order_acquire)) {
        if (std::chrono::steady_clock::now() >= deadline)
            return false;
        std::this_thread::yield();
    }
    return true;
}

bool WaitForReaderProgress()
{
    const auto deadline = std::chrono::steady_clock::now() + kTimeout;
    while (!reader_copied_cache.load(std::memory_order_acquire)
        && !reader_completed.load(std::memory_order_acquire)) {
        if (std::chrono::steady_clock::now() >= deadline)
            return false;
        std::this_thread::yield();
    }
    return true;
}

void BeforePrefixCacheSeed()
{
    if (!arm_writer_hook.exchange(false, std::memory_order_acq_rel))
        return;

    writer_at_cache_gap.store(true, std::memory_order_release);
    if (!WaitForReaderProgress())
        hook_timeout.store(true, std::memory_order_release);
}

void AfterPrefixCacheHit()
{
    if (!arm_reader_hook.exchange(false, std::memory_order_acq_rel))
        return;

    reader_copied_cache.store(true, std::memory_order_release);
    if (!WaitFor(release_reader))
        hook_timeout.store(true, std::memory_order_release);
}

void UnblockHooks()
{
    reader_copied_cache.store(true, std::memory_order_release);
    reader_completed.store(true, std::memory_order_release);
    release_reader.store(true, std::memory_order_release);
}

void ResetHooks()
{
    Dict::SetBeforePrefixCacheSeedHookForTest(nullptr);
    Dict::SetAfterPrefixCacheHitHookForTest(nullptr);
}

std::string ReadValue(DB& db, const std::string& key, int& result)
{
    MBData data;
    result = db.Find(key, data);
    if (result != MBError::SUCCESS)
        return {};
    return std::string(
        reinterpret_cast<const char*>(data.buff), data.data_len);
}

bool CachedNodeOffset(PrefixCache* cache, const std::string& key,
    size_t& node_offset)
{
    PrefixCacheEntry entry {};
    const int depth = cache->GetDepth(
        reinterpret_cast<const uint8_t*>(key.data()),
        static_cast<int>(key.size()), entry);
    if (depth != 4)
        return false;
    node_offset = Get6BInteger(entry.edge_buff + EDGE_NODE_LEADING_POS);
    return true;
}

} // namespace

int main()
{
    std::error_code error;
    std::filesystem::remove_all(kDbDir, error);
    std::filesystem::create_directories(kDbDir, error);
    if (error) {
        std::cerr << "database cleanup failed: " << error.message() << '\n';
        return 1;
    }

    DB writer(kDbDir,
        CONSTS::WriterOptions() | CONSTS::OPTION_PREFIX_CACHE);
    if (!writer.is_open()) {
        std::cerr << "writer open failed: " << writer.StatusStr() << '\n';
        return 1;
    }

    if (writer.Add("abcdx", "ABCDX") != MBError::SUCCESS
        || writer.Add("abcdy", "ABCDY") != MBError::SUCCESS
        || writer.Add("wxyzx", "WXYZX") != MBError::SUCCESS) {
        std::cerr << "initial data setup failed\n";
        return 1;
    }

    DB reader(kDbDir,
        CONSTS::ReaderOptions() | CONSTS::OPTION_PREFIX_CACHE);
    if (!reader.is_open()) {
        std::cerr << "reader open failed: " << reader.StatusStr() << '\n';
        return 1;
    }

    int result = MBError::SUCCESS;
    if (ReadValue(reader, "abcdx", result) != "ABCDX") {
        std::cerr << "failed to warm the abcd prefix-cache entry\n";
        return 1;
    }

    PrefixCache* cache = writer.GetDictPtr()->ActivePrefixCache();
    size_t released_node_offset = 0;
    if (cache == nullptr
        || !CachedNodeOffset(cache, "abcdx", released_node_offset)) {
        std::cerr << "failed to capture the original cached node offset\n";
        return 1;
    }

    Dict::SetBeforePrefixCacheSeedHookForTest(BeforePrefixCacheSeed);
    Dict::SetAfterPrefixCacheHitHookForTest(AfterPrefixCacheHit);
    arm_writer_hook.store(true, std::memory_order_release);

    int structural_add_result = MBError::SUCCESS;
    std::thread add_thread([&] {
        structural_add_result = writer.Add("abcdz", "ABCDZ");
    });

    if (!WaitFor(writer_at_cache_gap)) {
        std::cerr << "writer did not reach the structural-add cache gap\n";
        UnblockHooks();
        add_thread.join();
        ResetHooks();
        return 1;
    }

    arm_reader_hook.store(true, std::memory_order_release);
    int lookup_result = MBError::SUCCESS;
    std::string observed_value;
    std::thread reader_thread([&] {
        observed_value = ReadValue(reader, "abcdx", lookup_result);
        reader_completed.store(true, std::memory_order_release);
    });

    if (!WaitForReaderProgress()) {
        std::cerr << "reader neither copied the cache nor completed\n";
        UnblockHooks();
        add_thread.join();
        reader_thread.join();
        ResetHooks();
        return 1;
    }

    add_thread.join();
    if (structural_add_result != MBError::SUCCESS) {
        std::cerr << "structural add failed: "
                  << MBError::get_error_str(structural_add_result) << '\n';
        UnblockHooks();
        reader_thread.join();
        ResetHooks();
        return 1;
    }

    const int reuse_result = writer.Add("wxyzy", "WXYZY");
    size_t reused_node_offset = 0;
    const bool node_was_reused
        = reuse_result == MBError::SUCCESS
        && CachedNodeOffset(cache, "wxyzx", reused_node_offset)
        && reused_node_offset == released_node_offset;

    release_reader.store(true, std::memory_order_release);
    reader_thread.join();
    ResetHooks();

    if (hook_timeout.load(std::memory_order_acquire)) {
        std::cerr << "test synchronization timed out\n";
        return 1;
    }
    if (!node_was_reused) {
        std::cerr << "test setup did not reuse the released node: released="
                  << released_node_offset << " reused=" << reused_node_offset
                  << '\n';
        return 1;
    }

    if (lookup_result != MBError::SUCCESS || observed_value != "ABCDX") {
        std::cerr << "REPRODUCED: lookup for abcdx returned rc="
                  << MBError::get_error_str(lookup_result)
                  << " value=" << observed_value
                  << " after node " << released_node_offset
                  << " was reused by the wxyz prefix\n";
        return 2;
    }

    std::cout << "PASS: structural Add returned the correct cached value\n";
    return 0;
}
