/**
 * Deterministically exercises the exact-Find snapshot gap between
 * findInternal() and traverseFromEdge(). Build Mabain and this test with
 * MABAIN_LF_GUARD_TEST_HOOKS enabled.
 */

#include <atomic>
#include <filesystem>
#include <iostream>
#include <string>

#include "db.h"
#include "detail/search_engine.h"
#include "dict.h"
#include "error.h"
#include "mabain_consts.h"

using namespace mabain;

#ifndef MABAIN_LF_GUARD_TEST_HOOKS
#error "This test requires MABAIN_LF_GUARD_TEST_HOOKS"
#endif

namespace {

constexpr const char* kDbDir = "/var/tmp/mabain_lf_guard_snapshot_gap";
DB* writer_db = nullptr;
std::atomic<unsigned> reader_attempts { 0 };
std::atomic<int> structural_add_result { MBError::UNKNOWN_ERROR };
std::atomic<int> reuse_add_result { MBError::UNKNOWN_ERROR };
std::atomic<uint32_t> counter_before { 0 };
std::atomic<uint32_t> counter_after { 0 };
thread_local bool writer_add_in_progress = false;

void BeforeExactTraverse()
{
    if (writer_add_in_progress)
        return;

    const unsigned attempt =
        reader_attempts.fetch_add(1, std::memory_order_relaxed) + 1;
    if (attempt != 1)
        return;

    IndexHeader* header = writer_db->GetDictPtr()->GetHeaderPtr();
    counter_before.store(
        header->lock_free.counter.load(MEMORY_ORDER_READER),
        std::memory_order_relaxed);

    writer_add_in_progress = true;
    const int result = writer_db->Add("abe", "value-e");
    int reuse_result = MBError::UNKNOWN_ERROR;
    if (result == MBError::SUCCESS)
        reuse_result = writer_db->Add("xyq", "value-q");
    writer_add_in_progress = false;
    structural_add_result.store(result, std::memory_order_release);
    reuse_add_result.store(reuse_result, std::memory_order_release);

    counter_after.store(
        header->lock_free.counter.load(MEMORY_ORDER_READER),
        std::memory_order_relaxed);
}

} // namespace

int main()
{
    std::error_code error;
    std::filesystem::remove_all(kDbDir, error);
    std::filesystem::create_directories(kDbDir, error);
    if (error) {
        std::cerr << "database setup failed: " << error.message() << '\n';
        return 1;
    }

    DB writer(kDbDir, CONSTS::WriterOptions());
    if (!writer.is_open()) {
        std::cerr << "writer open failed: " << writer.StatusStr() << '\n';
        return 1;
    }
    if (writer.Add("abc", "value-c") != MBError::SUCCESS
        || writer.Add("abd", "value-d") != MBError::SUCCESS
        || writer.Add("xyz", "value-z") != MBError::SUCCESS) {
        std::cerr << "initial tree setup failed\n";
        return 1;
    }

    DB reader(kDbDir, CONSTS::ReaderOptions());
    if (!reader.is_open()) {
        std::cerr << "reader open failed: " << reader.StatusStr() << '\n';
        return 1;
    }

    writer_db = &writer;
    detail::SearchEngine::SetBeforeExactTraverseHookForTest(
        BeforeExactTraverse);

    MBData data;
    const int find_result = reader.Find("abc", data);

    detail::SearchEngine::SetBeforeExactTraverseHookForTest(nullptr);
    writer_db = nullptr;

    if (structural_add_result.load(std::memory_order_acquire)
        != MBError::SUCCESS) {
        std::cerr << "structural Add failed\n";
        return 1;
    }
    if (reuse_add_result.load(std::memory_order_acquire)
        != MBError::SUCCESS) {
        std::cerr << "node-reuse Add failed\n";
        return 1;
    }
    if (counter_after.load(std::memory_order_relaxed)
        == counter_before.load(std::memory_order_relaxed)) {
        std::cerr << "test setup did not publish a lock-free update\n";
        return 1;
    }

    std::string observed_value;
    if (find_result == MBError::SUCCESS && data.buff != nullptr)
        observed_value.assign(
            reinterpret_cast<const char*>(data.buff), data.data_len);

    const unsigned attempts =
        reader_attempts.load(std::memory_order_relaxed);
    if (attempts < 2) {
        std::cerr << "REPRODUCED: exact Find accepted its first attempt "
                     "after the old child node was reused; rc="
                  << MBError::get_error_str(find_result)
                  << " value=" << observed_value << '\n';
        return 2;
    }
    if (find_result != MBError::SUCCESS
        || observed_value != "value-c") {
        std::cerr << "retried lookup returned an unexpected result\n";
        return 1;
    }

    std::cout << "PASS: exact Find retried after the parent-edge update\n";
    return 0;
}
