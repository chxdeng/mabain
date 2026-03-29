/**
 * Copyright (C) 2026 Cisco Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU General Public License, version 2,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

// @author Changxue Deng <chadeng@cisco.com>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

#include "../db.h"
#include "../drm_base.h"
#include "../error.h"
#include "../mabain_consts.h"
#include "../mb_rc.h"
#include "../resource_pool.h"
#include "../rollable_file.h"
#include "./jemalloc_rebuild_test_modes.h"

namespace {

constexpr char kArenaCursorTestDir[] = "/var/tmp/mabain_test/jemalloc_rebuild";
constexpr size_t kArenaCursorBlockSize = 4 * 1024 * 1024;
constexpr size_t kArenaCursorMemCap = 4 * kArenaCursorBlockSize;
constexpr int kArenaCursorMaxBlocks = 4;

using mabain::DB;
using mabain::IndexHeader;
using mabain::MBConfig;
using mabain::MBData;

MBConfig MakeJemallocRebuildConfig(int options, bool keep_db)
{
    MBConfig config = {};
    config.mbdir = kArenaCursorTestDir;
    config.options = options;
    config.block_size_index = kArenaCursorBlockSize;
    config.block_size_data = kArenaCursorBlockSize;
    config.max_num_index_block = kArenaCursorMaxBlocks;
    config.max_num_data_block = kArenaCursorMaxBlocks;
    config.memcap_index = kArenaCursorMemCap;
    config.memcap_data = kArenaCursorMemCap;
    config.num_entry_per_bucket = 500;
    config.jemalloc_keep_db = keep_db;
    return config;
}

int PrintUsage(const char* prog)
{
    std::cerr << "Usage: " << prog << " <mode>\n";
    std::cerr << "Supported modes:\n";
    for (const char* mode : mabain_test::kJemallocRebuildTestModes) {
        std::cerr << "  " << mode << "\n";
    }
    std::cerr << "Only modes implemented for the current step perform real validation.\n";
    return 2;
}

int RunScaffoldMode(const std::string& mode)
{
    std::cout << "jemalloc_restart_rebuild_test: scaffold-only mode '" << mode << "'\n";
    return 0;
}

int PrepareArenaCursorDir()
{
    if (system("mkdir -p /var/tmp/mabain_test") != 0) {
        return 1;
    }
    if (system("rm -rf /var/tmp/mabain_test/jemalloc_rebuild") != 0) {
        return 1;
    }
    if (system("mkdir -p /var/tmp/mabain_test/jemalloc_rebuild") != 0) {
        return 1;
    }
    return 0;
}

int VerifyArenaCursor(const std::string& path)
{
    mabain::RollableFile file(path, kArenaCursorBlockSize, kArenaCursorMemCap,
        mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, 4);

    if (file.PreAlloc(128) == nullptr) {
        std::cerr << "failed PreAlloc for " << path << "\n";
        return 1;
    }
    if (file.GetJemallocAllocSize() != 128u) {
        std::cerr << "unexpected initial alloc_size for " << path << "\n";
        return 1;
    }

    if (file.ResetJemalloc() != mabain::MBError::SUCCESS) {
        std::cerr << "ResetJemalloc failed for " << path << "\n";
        return 1;
    }

    const size_t reseed_offset = kArenaCursorBlockSize + 321;
    if (file.ReseedJemalloc(reseed_offset) != mabain::MBError::SUCCESS) {
        std::cerr << "ReseedJemalloc failed for " << path << "\n";
        return 1;
    }
    if (file.GetJemallocAllocSize() != reseed_offset) {
        std::cerr << "unexpected reseeded alloc_size for " << path << "\n";
        return 1;
    }

    size_t offset1 = 0;
    void* ptr1 = file.Malloc(128, offset1);
    if (ptr1 == nullptr || offset1 < reseed_offset) {
        std::cerr << "first allocation failed after reseed for " << path << "\n";
        return 1;
    }
    const size_t alloc_size1 = file.GetJemallocAllocSize();

    if (file.ResetJemalloc() != mabain::MBError::SUCCESS ||
        file.ReseedJemalloc(reseed_offset) != mabain::MBError::SUCCESS) {
        std::cerr << "reset+reseed replay failed for " << path << "\n";
        return 1;
    }

    size_t offset2 = 0;
    void* ptr2 = file.Malloc(128, offset2);
    if (ptr2 == nullptr || offset2 != offset1 || file.GetJemallocAllocSize() != alloc_size1) {
        std::cerr << "reset+reseed was not deterministic for " << path << "\n";
        return 1;
    }

    return 0;
}

int RunArenaCursorMode()
{
    if (PrepareArenaCursorDir() != 0) {
        std::cerr << "failed to prepare arena_cursor test directory\n";
        return 1;
    }

    mabain::ResourcePool::getInstance().RemoveAll();

    const int index_rc = VerifyArenaCursor(std::string(kArenaCursorTestDir) + "/_mabain_i");
    mabain::ResourcePool::getInstance().RemoveAll();
    if (index_rc != 0) {
        return index_rc;
    }

    const int data_rc = VerifyArenaCursor(std::string(kArenaCursorTestDir) + "/_mabain_d");
    mabain::ResourcePool::getInstance().RemoveAll();
    if (data_rc != 0) {
        return data_rc;
    }

    std::cout << "jemalloc_restart_rebuild_test: arena_cursor passed\n";
    return 0;
}

int VerifyFindValue(DB& db, const std::string& key, const std::string& value,
    const char* context)
{
    MBData data;
    const int rc = db.Find(key, data);
    if (rc != mabain::MBError::SUCCESS) {
        std::cerr << context << ": Find failed for key '" << key << "' rc=" << rc << "\n";
        return 1;
    }

    const std::string actual(reinterpret_cast<const char*>(data.buff), data.data_len);
    if (actual != value) {
        std::cerr << context << ": unexpected value for key '" << key
                  << "' expected='" << value << "' actual='" << actual << "'\n";
        return 1;
    }

    return 0;
}

int RunStartupGateMode()
{
    if (PrepareArenaCursorDir() != 0) {
        std::cerr << "failed to prepare startup_gate test directory\n";
        return 1;
    }

    mabain::ResourcePool::getInstance().RemoveAll();
    const std::string key("alpha");
    const std::string value("value-alpha");

    {
        MBConfig initial_cfg = MakeJemallocRebuildConfig(
            mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, false);
        DB initial_db(initial_cfg);
        if (!initial_db.is_open()) {
            std::cerr << "initial startup_gate open failed: " << initial_db.StatusStr() << "\n";
            return 1;
        }
        if (initial_db.Add(key, value) != mabain::MBError::SUCCESS) {
            std::cerr << "initial startup_gate Add failed\n";
            return 1;
        }
        if (initial_db.Count() != 1) {
            std::cerr << "initial startup_gate count mismatch\n";
            return 1;
        }
        initial_db.Close();
    }

    mabain::ResourcePool::getInstance().RemoveAll();

    MBConfig rebuild_cfg = MakeJemallocRebuildConfig(
        mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, true);
    DB rebuild_db(rebuild_cfg);
    if (!rebuild_db.is_open()) {
        std::cerr << "startup_gate reopen failed: " << rebuild_db.StatusStr() << "\n";
        return 1;
    }

    char hdr_page[mabain::RollableFile::page_size];
    std::ifstream in(std::string(kArenaCursorTestDir) + "/_mabain_h",
        std::ios::in | std::ios::binary);
    if (!in.is_open()) {
        std::cerr << "startup_gate failed to read persisted header\n";
        return 1;
    }
    in.read(hdr_page, sizeof(hdr_page));
    if (in.gcount() != static_cast<std::streamsize>(sizeof(hdr_page))) {
        std::cerr << "startup_gate short read on persisted header\n";
        return 1;
    }

    const IndexHeader* header = reinterpret_cast<const IndexHeader*>(hdr_page);
    if (header == nullptr || header->rebuild_state != REBUILD_STATE_PREP
        || !header->RebuildInProgress()) {
        std::cerr << "startup_gate did not enter PREP state\n";
        return 1;
    }
    if (rebuild_db.Count() != 1) {
        std::cerr << "startup_gate writer count mismatch\n";
        return 1;
    }
    if (VerifyFindValue(rebuild_db, key, value, "startup_gate writer") != 0) {
        return 1;
    }

    MBConfig reader_cfg = MakeJemallocRebuildConfig(mabain::CONSTS::ACCESS_MODE_READER, false);
    DB reader_db(reader_cfg);
    if (!reader_db.is_open()) {
        std::cerr << "startup_gate reader open failed: " << reader_db.StatusStr() << "\n";
        return 1;
    }
    if (VerifyFindValue(reader_db, key, value, "startup_gate reader") != 0) {
        return 1;
    }

    reader_db.Close();
    rebuild_db.Close();
    mabain::ResourcePool::getInstance().RemoveAll();
    std::cout << "jemalloc_restart_rebuild_test: startup_gate passed\n";
    return 0;
}

int RunAsyncRejectMode()
{
    if (PrepareArenaCursorDir() != 0) {
        std::cerr << "failed to prepare async_reject test directory\n";
        return 1;
    }

    mabain::ResourcePool::getInstance().RemoveAll();
    const std::string key("beta");
    const std::string value("value-beta");

    {
        MBConfig initial_cfg = MakeJemallocRebuildConfig(
            mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, false);
        DB initial_db(initial_cfg);
        if (!initial_db.is_open()) {
            std::cerr << "initial async_reject open failed: " << initial_db.StatusStr() << "\n";
            return 1;
        }
        if (initial_db.Add(key, value) != mabain::MBError::SUCCESS) {
            std::cerr << "initial async_reject Add failed\n";
            return 1;
        }
        initial_db.Close();
    }

    mabain::ResourcePool::getInstance().RemoveAll();

    {
        MBConfig reject_cfg = MakeJemallocRebuildConfig(
            mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC
                | mabain::CONSTS::ASYNC_WRITER_MODE,
            true);
        DB rejected_db(reject_cfg);
        if (rejected_db.is_open() || rejected_db.Status() != mabain::MBError::NOT_ALLOWED) {
            std::cerr << "async_reject expected NOT_ALLOWED, got " << rejected_db.StatusStr() << "\n";
            return 1;
        }
    }

    mabain::ResourcePool::getInstance().RemoveAll();

    MBConfig reader_cfg = MakeJemallocRebuildConfig(mabain::CONSTS::ACCESS_MODE_READER, false);
    DB reader_db(reader_cfg);
    if (!reader_db.is_open()) {
        std::cerr << "async_reject reader open failed: " << reader_db.StatusStr() << "\n";
        return 1;
    }
    if (VerifyFindValue(reader_db, key, value, "async_reject reader") != 0) {
        return 1;
    }

    reader_db.Close();
    mabain::ResourcePool::getInstance().RemoveAll();
    std::cout << "jemalloc_restart_rebuild_test: async_reject passed\n";
    return 0;
}

int RunShrinkOnlyMode()
{
    if (PrepareArenaCursorDir() != 0) {
        std::cerr << "failed to prepare shrink_only test directory\n";
        return 1;
    }

    mabain::ResourcePool::getInstance().RemoveAll();
    const std::string key("delta");
    const std::string value("value-delta");

    {
        MBConfig initial_cfg = MakeJemallocRebuildConfig(
            mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, false);
        DB initial_db(initial_cfg);
        if (!initial_db.is_open()) {
            std::cerr << "initial shrink_only open failed: " << initial_db.StatusStr() << "\n";
            return 1;
        }
        if (initial_db.Add(key, value) != mabain::MBError::SUCCESS) {
            std::cerr << "initial shrink_only Add failed\n";
            return 1;
        }
        initial_db.Close();
    }

    MBConfig rebuild_cfg = MakeJemallocRebuildConfig(
        mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, true);
    DB rebuild_db(rebuild_cfg);
    if (!rebuild_db.is_open()) {
        std::cerr << "shrink_only reopen failed: " << rebuild_db.StatusStr() << "\n";
        return 1;
    }

    mabain::ResourceCollection rc(rebuild_db);
    if (rc.StartupShrink() != mabain::MBError::SUCCESS) {
        std::cerr << "StartupShrink failed\n";
        return 1;
    }
    if (VerifyFindValue(rebuild_db, key, value, "shrink_only writer") != 0) {
        return 1;
    }

    IndexHeader* header = rebuild_db.GetDictPtr()->GetHeaderPtr();
    if (header == nullptr || header->rebuild_state != REBUILD_STATE_COPY
        || header->rebuild_index_alloc_end > header->rebuild_index_alloc_start
        || header->rebuild_data_alloc_end > header->rebuild_data_alloc_start) {
        std::cerr << "shrink_only metadata mismatch\n";
        return 1;
    }

    rebuild_db.Close();
    mabain::ResourcePool::getInstance().RemoveAll();
    std::cout << "jemalloc_restart_rebuild_test: shrink_only passed\n";
    return 0;
}

} // namespace

int main(int argc, char* argv[])
{
    if (argc != 2) {
        return PrintUsage(argv[0]);
    }

    std::string mode(argv[1]);
    if (!mabain_test::IsJemallocRebuildTestModeSupported(mode)) {
        return PrintUsage(argv[0]);
    }

    if (mode == "arena_cursor") {
        return RunArenaCursorMode();
    }
    if (mode == "startup_gate") {
        return RunStartupGateMode();
    }
    if (mode == "async_reject") {
        return RunAsyncRejectMode();
    }
    if (mode == "shrink_only") {
        return RunShrinkOnlyMode();
    }

    return RunScaffoldMode(mode);
}
