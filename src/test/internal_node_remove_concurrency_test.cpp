/**
 * Deterministically reproduces a reader retaining an internal-node value
 * offset while Remove() releases and another Add() reuses that value buffer.
 * Build Mabain and this test with MABAIN_LF_GUARD_TEST_HOOKS enabled.
 */

#include <filesystem>
#include <iostream>
#include <string>

#include "db.h"
#include "dict.h"
#include "error.h"
#include "mabain_consts.h"

using namespace mabain;

#ifndef MABAIN_LF_GUARD_TEST_HOOKS
#error "This test requires MABAIN_LF_GUARD_TEST_HOOKS"
#endif

namespace {

constexpr const char* kDbDir =
    "/var/tmp/mabain_internal_node_remove_concurrency";
const std::string kParentKey = "abc";
const std::string kChildKey = "abcdef";
const std::string kReuseKey = "z";
const std::string kOldValue(256, 'A');
const std::string kReuseValue(256, 'R');

DB* writer_db = nullptr;
bool hook_invoked = false;
int remove_result = MBError::UNKNOWN_ERROR;
int reuse_add_result = MBError::UNKNOWN_ERROR;

void BeforeInternalNodeValueRead()
{
    if (hook_invoked)
        return;

    hook_invoked = true;
    remove_result = writer_db->Remove(kParentKey);
    if (remove_result == MBError::SUCCESS)
        reuse_add_result = writer_db->Add(kReuseKey, kReuseValue);
}

std::string ValueFrom(const MBData& data)
{
    if (data.buff == nullptr)
        return {};
    return std::string(
        reinterpret_cast<const char*>(data.buff), data.data_len);
}

} // namespace

int main()
{
    std::error_code error;
    std::filesystem::remove_all(kDbDir, error);
    error.clear();
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
    if (writer.Add(kParentKey, kOldValue) != MBError::SUCCESS
        || writer.Add(kChildKey, "child-value") != MBError::SUCCESS) {
        std::cerr << "failed to create an internal node with a value\n";
        return 1;
    }

    MBData old_data;
    if (writer.Find(kParentKey, old_data) != MBError::SUCCESS) {
        std::cerr << "failed to read the original internal-node value\n";
        return 1;
    }
    const size_t old_data_offset = old_data.data_offset;

    DB reader(kDbDir, CONSTS::ReaderOptions());
    if (!reader.is_open()) {
        std::cerr << "reader open failed: " << reader.StatusStr() << '\n';
        return 1;
    }

    writer_db = &writer;
    Dict::SetBeforeInternalNodeValueReadHookForTest(
        BeforeInternalNodeValueRead);

    MBData observed_data;
    const int find_result = reader.Find(kParentKey, observed_data);

    Dict::SetBeforeInternalNodeValueReadHookForTest(nullptr);
    writer_db = nullptr;

    if (!hook_invoked || remove_result != MBError::SUCCESS
        || reuse_add_result != MBError::SUCCESS) {
        std::cerr << "failed to create the intended Remove/Add interleaving: "
                  << "hook=" << hook_invoked
                  << " remove=" << MBError::get_error_str(remove_result)
                  << " reuse=" << MBError::get_error_str(reuse_add_result)
                  << '\n';
        return 1;
    }

    MBData reused_data;
    if (writer.Find(kReuseKey, reused_data) != MBError::SUCCESS
        || reused_data.data_offset != old_data_offset) {
        std::cerr << "test setup did not reuse the released value buffer\n";
        return 1;
    }

    const std::string observed_value = ValueFrom(observed_data);
    if (find_result == MBError::SUCCESS
        && observed_value == kReuseValue) {
        std::cerr << "REPRODUCED: Find(\"" << kParentKey
                  << "\") returned the unrelated value from \""
                  << kReuseKey << "\" after internal-node Remove()\n";
        return 2;
    }

    if (find_result != MBError::NOT_EXIST) {
        std::cerr << "unexpected lookup result: rc="
                  << MBError::get_error_str(find_result)
                  << " value_size=" << observed_value.size() << '\n';
        return 1;
    }

    std::cout << "PASS: overlapping internal-node Remove() did not expose "
                 "a reused value buffer\n";
    return 0;
}
