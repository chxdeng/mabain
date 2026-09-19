/**
 * Reproduces a stale prefix-cache lookup crossing an overwrite and immediate
 * free-list reuse. Build Mabain and this test with
 * MABAIN_PREFIX_CACHE_CONSISTENCY_TEST_HOOKS enabled.
 */

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>

#include "db.h"
#include "dict.h"
#include "error.h"
#include "mabain_consts.h"

using namespace mabain;

#ifndef MABAIN_PREFIX_CACHE_CONSISTENCY_TEST_HOOKS
#error "This test requires MABAIN_PREFIX_CACHE_CONSISTENCY_TEST_HOOKS"
#endif

namespace {

constexpr auto kTimeout = std::chrono::seconds(5);

std::atomic<bool> arm_writer_hook { false };
std::atomic<bool> arm_reader_hook { false };
std::atomic<bool> writer_at_cache_gap { false };
std::atomic<bool> reader_copied_cache { false };
std::atomic<bool> reader_completed { false };
std::atomic<bool> release_reader { false };
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

void BeforePrefixCacheSeed()
{
    if (!arm_writer_hook.exchange(false, std::memory_order_acq_rel))
        return;

    writer_at_cache_gap.store(true, std::memory_order_release);
    const auto deadline = std::chrono::steady_clock::now() + kTimeout;
    while (!reader_copied_cache.load(std::memory_order_acquire)
        && !reader_completed.load(std::memory_order_acquire)) {
        if (std::chrono::steady_clock::now() >= deadline) {
            hook_timeout.store(true, std::memory_order_release);
            return;
        }
        std::this_thread::yield();
    }
}

void AfterPrefixCacheHit()
{
    if (!arm_reader_hook.exchange(false, std::memory_order_acq_rel))
        return;

    reader_copied_cache.store(true, std::memory_order_release);
    if (!WaitFor(release_reader))
        hook_timeout.store(true, std::memory_order_release);
}

int AddWithOffset(DB& db, const std::string& key, const std::string& value,
    bool overwrite, size_t& offset)
{
    MBData data;
    data.buff = reinterpret_cast<uint8_t*>(const_cast<char*>(value.data()));
    data.data_len = static_cast<int>(value.size());
    const int rc = db.Add(key.data(), static_cast<int>(key.size()), data, overwrite);
    offset = data.data_offset;
    data.buff = nullptr;
    return rc;
}

std::string ReadValue(DB& db, const std::string& key, int& rc)
{
    MBData data;
    rc = db.Find(key, data);
    if (rc != MBError::SUCCESS)
        return {};
    return std::string(reinterpret_cast<const char*>(data.buff), data.data_len);
}

} // namespace

int main()
{
    const std::string db_dir = "/var/tmp/mabain_prefix_cache_add_consistency";
    std::error_code cleanup_error;
    std::filesystem::remove_all(db_dir, cleanup_error);
    std::filesystem::create_directories(db_dir, cleanup_error);
    if (cleanup_error) {
        std::cerr << "database cleanup failed: " << cleanup_error.message() << '\n';
        return 1;
    }

    DB writer(db_dir.c_str(),
        CONSTS::WriterOptions() | CONSTS::OPTION_PREFIX_CACHE);
    if (!writer.is_open()) {
        std::cerr << "writer open failed: " << writer.StatusStr() << '\n';
        return 1;
    }

    const std::string target_key = "aa00";
    const std::string unrelated_key = "zz00";
    const std::string old_value(4096, 'A');
    const std::string new_value(4096, 'B');
    const std::string unrelated_value(4096, 'Z');

    size_t old_offset = 0;
    int rc = AddWithOffset(writer, target_key, old_value, false, old_offset);
    if (rc != MBError::SUCCESS) {
        std::cerr << "initial add failed: " << MBError::get_error_str(rc) << '\n';
        return 1;
    }

    DB reader(db_dir.c_str(),
        CONSTS::ReaderOptions() | CONSTS::OPTION_PREFIX_CACHE);
    if (!reader.is_open()) {
        std::cerr << "reader open failed: " << reader.StatusStr() << '\n';
        return 1;
    }

    int warm_rc = MBError::SUCCESS;
    if (ReadValue(reader, target_key, warm_rc) != old_value) {
        std::cerr << "failed to warm prefix cache, rc=" << warm_rc << '\n';
        return 1;
    }

    Dict::SetBeforePrefixCacheSeedHookForTest(BeforePrefixCacheSeed);
    Dict::SetAfterPrefixCacheHitHookForTest(AfterPrefixCacheHit);
    arm_writer_hook.store(true, std::memory_order_release);

    int overwrite_rc = MBError::SUCCESS;
    size_t new_offset = 0;
    std::thread overwrite_thread([&] {
        overwrite_rc = AddWithOffset(
            writer, target_key, new_value, true, new_offset);
    });

    if (!WaitFor(writer_at_cache_gap)) {
        std::cerr << "writer did not reach the post-publish cache gap\n";
        release_reader.store(true, std::memory_order_release);
        overwrite_thread.join();
        return 1;
    }

    arm_reader_hook.store(true, std::memory_order_release);
    int lookup_rc = MBError::SUCCESS;
    std::string observed_value;
    std::thread reader_thread([&] {
        observed_value = ReadValue(reader, target_key, lookup_rc);
        reader_completed.store(true, std::memory_order_release);
    });

    overwrite_thread.join();
    if (overwrite_rc != MBError::SUCCESS) {
        std::cerr << "overwrite failed: " << MBError::get_error_str(overwrite_rc) << '\n';
        release_reader.store(true, std::memory_order_release);
        reader_thread.join();
        return 1;
    }

    size_t reused_offset = 0;
    rc = AddWithOffset(writer, unrelated_key, unrelated_value, false, reused_offset);
    release_reader.store(true, std::memory_order_release);
    reader_thread.join();

    Dict::SetBeforePrefixCacheSeedHookForTest(nullptr);
    Dict::SetAfterPrefixCacheHitHookForTest(nullptr);

    if (rc != MBError::SUCCESS) {
        std::cerr << "unrelated add failed: " << MBError::get_error_str(rc) << '\n';
        return 1;
    }
    if (hook_timeout.load(std::memory_order_acquire)) {
        std::cerr << "test synchronization timed out\n";
        return 1;
    }
    if (reader_copied_cache.load(std::memory_order_acquire)
        && reused_offset != old_offset) {
        std::cerr << "free-list did not reuse the stale value offset: old="
                  << old_offset << " reused=" << reused_offset << '\n';
        return 1;
    }

    if (lookup_rc != MBError::SUCCESS
        || (observed_value != old_value && observed_value != new_value)) {
        std::cerr << "REPRODUCED: lookup for " << target_key
                  << " returned rc=" << MBError::get_error_str(lookup_rc)
                  << " value_byte="
                  << (observed_value.empty() ? '?' : observed_value.front())
                  << " old_offset=" << old_offset
                  << " new_offset=" << new_offset
                  << " reused_offset=" << reused_offset << '\n';
        return 2;
    }

    std::cout << "PASS: concurrent lookup returned a complete old or new value"
              << " (cache_hit="
              << reader_copied_cache.load(std::memory_order_acquire) << ")\n";
    return 0;
}
