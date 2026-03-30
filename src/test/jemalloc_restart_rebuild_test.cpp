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

#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>
#include <vector>

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
constexpr char kFullCycleKeysFile[] = "/var/tmp/mabain_test/jemalloc_rebuild/full_cycle_keys.txt";
constexpr char kFullCycleStopFile[] = "/var/tmp/mabain_test/jemalloc_rebuild/full_cycle.stop";
constexpr uint32_t kFullCycleBlockSize = 4 * 1024 * 1024;
constexpr int kFullCycleMaxBlocks = 32;

using mabain::DB;
using mabain::IndexHeader;
using mabain::MBConfig;
using mabain::MBData;

MBConfig MakeSizedJemallocRebuildConfig(int options, bool keep_db,
    uint32_t block_size, int max_blocks)
{
    MBConfig config = {};
    config.mbdir = kArenaCursorTestDir;
    config.options = options;
    config.block_size_index = block_size;
    config.block_size_data = block_size;
    config.max_num_index_block = max_blocks;
    config.max_num_data_block = max_blocks;
    config.memcap_index = static_cast<size_t>(block_size) * max_blocks;
    config.memcap_data = static_cast<size_t>(block_size) * max_blocks;
    config.num_entry_per_bucket = 500;
    config.jemalloc_keep_db = keep_db;
    return config;
}

MBConfig MakeJemallocRebuildConfig(int options, bool keep_db)
{
    return MakeSizedJemallocRebuildConfig(
        options, keep_db, kArenaCursorBlockSize, kArenaCursorMaxBlocks);
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

int WriteKeysToFile(const std::vector<std::string>& keys, const std::string& path)
{
    std::ofstream out(path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        std::cerr << "failed to open key file for write: " << path << "\n";
        return 1;
    }
    for (const std::string& key : keys)
        out << key << "\n";
    return out.good() ? 0 : 1;
}

int LoadKeysFromFile(const std::string& path, std::vector<std::string>& keys)
{
    std::ifstream in(path, std::ios::in);
    if (!in.is_open()) {
        std::cerr << "failed to open key file for read: " << path << "\n";
        return 1;
    }

    std::string key;
    while (std::getline(in, key)) {
        if (!key.empty())
            keys.push_back(key);
    }
    if (!in.eof()) {
        std::cerr << "failed while reading key file: " << path << "\n";
        return 1;
    }
    if (keys.empty()) {
        std::cerr << "key file is empty: " << path << "\n";
        return 1;
    }
    return 0;
}

int ParseReaderConnectId(uint32_t& connect_id)
{
    const char* env = std::getenv("MABAIN_READER_CONNECT_ID");
    if (env == nullptr || *env == '\0') {
        std::cerr << "MABAIN_READER_CONNECT_ID is required for reader_loop\n";
        return 1;
    }

    char* end = nullptr;
    errno = 0;
    unsigned long parsed = std::strtoul(env, &end, 0);
    if (errno != 0 || end == env || *end != '\0' || parsed > 0xffffffffUL) {
        std::cerr << "invalid MABAIN_READER_CONNECT_ID: " << env << "\n";
        return 1;
    }

    connect_id = static_cast<uint32_t>(parsed);
    return 0;
}

int RunReaderLookupLoop(const std::vector<std::string>& keys, const std::string& value,
    const std::string& stop_file, uint32_t connect_id, uint32_t block_size, int max_blocks)
{
    if (keys.empty()) {
        std::cerr << "reader_loop key set is empty\n";
        return 1;
    }

    mabain::ResourcePool::getInstance().RemoveAll();
    MBConfig reader_cfg = MakeSizedJemallocRebuildConfig(
        mabain::CONSTS::ACCESS_MODE_READER, false, block_size, max_blocks);
    reader_cfg.connect_id = connect_id;
    DB reader_db(reader_cfg);
    if (!reader_db.is_open()) {
        std::cerr << "reader_loop open failed: " << reader_db.StatusStr() << "\n";
        return 1;
    }

    size_t iter = 0;
    while (true) {
        errno = 0;
        if (access(stop_file.c_str(), F_OK) == 0)
            break;
        if (errno != 0 && errno != ENOENT) {
            std::cerr << "reader_loop stop-file check failed\n";
            reader_db.Close();
            return 1;
        }

        const std::string& key = keys[iter % keys.size()];
        if (VerifyFindValue(reader_db, key, value, "reader_loop") != 0) {
            reader_db.Close();
            return 1;
        }

        iter++;
        if ((iter % 50000) == 0) {
            std::cout << "reader_loop connect_id=" << connect_id
                      << " lookups=" << iter << "\n";
        }
        if ((iter % 1024) == 0)
            usleep(1000);
    }

    reader_db.Close();
    mabain::ResourcePool::getInstance().RemoveAll();
    std::cout << "reader_loop connect_id=" << connect_id
              << " stopped after " << iter << " lookups\n";
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
    const std::string key2("alpha-2");
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
    if (header == nullptr || header->rebuild_state != REBUILD_STATE_NORMAL
        || header->RebuildInProgress()) {
        std::cerr << "startup_gate persisted header did not clear rebuild state\n";
        return 1;
    }
    const IndexHeader* live_header = rebuild_db.GetDictPtr()->GetHeaderPtr();
    if (live_header == nullptr || live_header->rebuild_state != REBUILD_STATE_NORMAL
        || live_header->RebuildInProgress()) {
        std::cerr << "startup_gate live header did not clear rebuild state\n";
        return 1;
    }
    if (rebuild_db.Count() != 1) {
        std::cerr << "startup_gate writer count mismatch\n";
        return 1;
    }
    if (VerifyFindValue(rebuild_db, key, value, "startup_gate writer") != 0) {
        return 1;
    }
    if (rebuild_db.Add(key2, value) != mabain::MBError::SUCCESS) {
        std::cerr << "startup_gate writer post-rebuild Add failed\n";
        return 1;
    }
    if (VerifyFindValue(rebuild_db, key2, value, "startup_gate writer post-rebuild add") != 0) {
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
    if (VerifyFindValue(reader_db, key2, value, "startup_gate reader post-rebuild add") != 0) {
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
        || header->rebuild_data_alloc_end > header->rebuild_data_alloc_start
        || header->rebuild_index_source_end < header->m_index_offset
        || header->rebuild_data_source_end < header->m_data_offset
        || header->rebuild_index_alloc_start != header->m_index_offset
        || header->rebuild_data_alloc_start != header->m_data_offset) {
        std::cerr << "shrink_only metadata mismatch\n";
        return 1;
    }

    rebuild_db.Close();
    mabain::ResourcePool::getInstance().RemoveAll();
    std::cout << "jemalloc_restart_rebuild_test: shrink_only passed\n";
    return 0;
}

int RunEvacuateOnlyMode()
{
    if (PrepareArenaCursorDir() != 0) {
        std::cerr << "failed to prepare evacuate_only test directory\n";
        return 1;
    }

    mabain::ResourcePool::getInstance().RemoveAll();
    const std::string key("epsilon");
    const std::string value("value-epsilon");

    {
        MBConfig initial_cfg = MakeJemallocRebuildConfig(
            mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, false);
        DB initial_db(initial_cfg);
        if (!initial_db.is_open()) {
            std::cerr << "initial evacuate_only open failed: " << initial_db.StatusStr() << "\n";
            return 1;
        }
        if (initial_db.Add(key, value) != mabain::MBError::SUCCESS) {
            std::cerr << "initial evacuate_only Add failed\n";
            return 1;
        }
        initial_db.Close();
    }

    MBConfig rebuild_cfg = MakeJemallocRebuildConfig(
        mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, true);
    DB rebuild_db(rebuild_cfg);
    if (!rebuild_db.is_open()) {
        std::cerr << "evacuate_only reopen failed: " << rebuild_db.StatusStr() << "\n";
        return 1;
    }

    mabain::ResourceCollection rc(rebuild_db);
    if (rc.StartupShrink() != mabain::MBError::SUCCESS) {
        std::cerr << "StartupShrink failed\n";
        return 1;
    }
    const IndexHeader* header = rebuild_db.GetDictPtr()->GetHeaderPtr();
    if (header == nullptr) {
        std::cerr << "evacuate_only missing header\n";
        return 1;
    }
    const size_t index_boundary = header->rebuild_index_alloc_end;
    const size_t data_boundary = header->rebuild_data_alloc_end;
    if (rc.StartupEvacuate() != mabain::MBError::SUCCESS) {
        std::cerr << "StartupEvacuate failed\n";
        return 1;
    }
    if (header->reusable_index_block_count > 1
        || header->reusable_data_block_count > 1
        || (header->reusable_index_block_count == 1
            && header->reusable_index_block[0].in_use != REUSABLE_BLOCK_STATE_READY)
        || (header->reusable_data_block_count == 1
            && header->reusable_data_block[0].in_use != REUSABLE_BLOCK_STATE_READY)) {
        std::cerr << "evacuate_only reusable queue mismatch\n";
        return 1;
    }
    if (header->rebuild_state != REBUILD_STATE_CUTOVER
        || header->rebuild_index_alloc_end != index_boundary
        || header->rebuild_data_alloc_end != data_boundary
        || rebuild_db.GetDictPtr()->GetMM()->GetJemallocAllocSize() < index_boundary
        || rebuild_db.GetDictPtr()->GetJemallocAllocSize() < data_boundary
        || header->rebuild_index_alloc_start < index_boundary
        || header->rebuild_data_alloc_start < data_boundary
        || (header->rebuild_index_alloc_start % header->index_block_size) != 0
        || (header->rebuild_data_alloc_start % header->data_block_size) != 0
        || header->rebuild_index_block_cursor < header->rebuild_index_alloc_start
        || header->rebuild_index_block_cursor > header->rebuild_index_source_end
        || header->rebuild_data_block_cursor < header->rebuild_data_alloc_start
        || header->rebuild_data_block_cursor > header->rebuild_data_source_end) {
        std::cerr << "evacuate_only metadata mismatch\n";
        return 1;
    }
    if (VerifyFindValue(rebuild_db, key, value, "evacuate_only writer") != 0) {
        return 1;
    }

    rebuild_db.Close();
    mabain::ResourcePool::getInstance().RemoveAll();
    std::cout << "jemalloc_restart_rebuild_test: evacuate_only passed\n";
    return 0;
}

int RunRecoverEvacuateMode()
{
    if (PrepareArenaCursorDir() != 0) {
        std::cerr << "failed to prepare recover_evacuate test directory\n";
        return 1;
    }

    mabain::ResourcePool::getInstance().RemoveAll();
    const std::string key("zeta");
    const std::string value("value-zeta");

    {
        MBConfig initial_cfg = MakeJemallocRebuildConfig(
            mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, false);
        DB initial_db(initial_cfg);
        if (!initial_db.is_open()) {
            std::cerr << "initial recover_evacuate open failed: " << initial_db.StatusStr() << "\n";
            return 1;
        }
        if (initial_db.Add(key, value) != mabain::MBError::SUCCESS) {
            std::cerr << "initial recover_evacuate Add failed\n";
            return 1;
        }
        initial_db.Close();
    }

    MBConfig rebuild_cfg = MakeJemallocRebuildConfig(
        mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, true);
    DB rebuild_db(rebuild_cfg);
    if (!rebuild_db.is_open()) {
        std::cerr << "recover_evacuate reopen failed: " << rebuild_db.StatusStr() << "\n";
        return 1;
    }

    mabain::ResourceCollection rc(rebuild_db);
    if (rc.StartupShrink() != mabain::MBError::SUCCESS || rc.StartupEvacuate() != mabain::MBError::SUCCESS) {
        std::cerr << "recover_evacuate initial handoff failed\n";
        return 1;
    }

    const IndexHeader* header = rebuild_db.GetDictPtr()->GetHeaderPtr();
    if (header == nullptr) {
        std::cerr << "recover_evacuate missing header\n";
        return 1;
    }
    const size_t index_source_start = header->rebuild_index_alloc_start;
    const size_t data_source_start = header->rebuild_data_alloc_start;
    const size_t index_boundary = header->rebuild_index_alloc_end;
    const size_t data_boundary = header->rebuild_data_alloc_end;
    const size_t index_block_cursor = header->rebuild_index_block_cursor;
    const size_t data_block_cursor = header->rebuild_data_block_cursor;
    const size_t index_source_end = header->rebuild_index_source_end;
    const size_t data_source_end = header->rebuild_data_source_end;

    if (rc.StartupEvacuate() != mabain::MBError::SUCCESS) {
        std::cerr << "recover_evacuate second handoff failed\n";
        return 1;
    }
    if (header->rebuild_state != REBUILD_STATE_CUTOVER
        || header->rebuild_index_alloc_start != index_source_start
        || header->rebuild_data_alloc_start != data_source_start
        || header->rebuild_index_alloc_end != index_boundary
        || header->rebuild_data_alloc_end != data_boundary
        || header->rebuild_index_block_cursor < index_block_cursor
        || header->rebuild_data_block_cursor < data_block_cursor
        || header->rebuild_data_block_cursor > header->rebuild_data_source_end
        || header->rebuild_index_source_end != index_source_end
        || header->rebuild_data_source_end != data_source_end
        || rebuild_db.GetDictPtr()->GetMM()->GetJemallocAllocSize() < index_boundary
        || rebuild_db.GetDictPtr()->GetJemallocAllocSize() < data_boundary) {
        std::cerr << "recover_evacuate metadata mismatch\n";
        return 1;
    }
    if (VerifyFindValue(rebuild_db, key, value, "recover_evacuate writer") != 0) {
        return 1;
    }

    rebuild_db.Close();
    mabain::ResourcePool::getInstance().RemoveAll();
    std::cout << "jemalloc_restart_rebuild_test: recover_evacuate passed\n";
    return 0;
}

int RunFullCyclePrepareMode()
{
    if (PrepareArenaCursorDir() != 0) {
        std::cerr << "failed to prepare full_cycle test directory\n";
        return 1;
    }

    const std::string value(256, 'v');
    std::vector<std::string> keys;
    std::vector<std::string> survivor_keys;

    (void)unlink(kFullCycleStopFile);
    mabain::ResourcePool::getInstance().RemoveAll();
    {
        MBConfig initial_cfg = MakeSizedJemallocRebuildConfig(
            mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC,
            false, kFullCycleBlockSize, kFullCycleMaxBlocks);
        DB initial_db(initial_cfg);
        if (!initial_db.is_open()) {
            std::cerr << "initial full_cycle_prepare open failed: " << initial_db.StatusStr() << "\n";
            return 1;
        }
        for (int i = 0;
             i < 300000
                 && initial_db.GetDictPtr()->GetMM()->GetExistingBlockEnd() < 4 * kFullCycleBlockSize;
             i++) {
            keys.push_back("full-cycle-" + std::to_string(i) + std::string(24, 'k'));
            if (initial_db.Add(keys.back(), value) != mabain::MBError::SUCCESS) {
                std::cerr << "initial full_cycle_prepare Add failed\n";
                return 1;
            }
        }
        if (initial_db.GetDictPtr()->GetMM()->GetExistingBlockEnd() < 4 * kFullCycleBlockSize
            || keys.size() < 1024) {
            std::cerr << "initial full_cycle_prepare dataset too small for pressure rebuild\n";
            return 1;
        }
        for (size_t i = 0; i + 512 < keys.size(); i++) {
            if (initial_db.Remove(keys[i]) != mabain::MBError::SUCCESS) {
                std::cerr << "initial full_cycle_prepare Remove failed\n";
                return 1;
            }
        }
        survivor_keys.assign(keys.end() - 256, keys.end());
        initial_db.Close();
    }

    if (WriteKeysToFile(survivor_keys, kFullCycleKeysFile) != 0)
        return 1;

    mabain::ResourcePool::getInstance().RemoveAll();
    std::cout << "jemalloc_restart_rebuild_test: full_cycle_prepare passed\n";
    return 0;
}

int RunReaderLoopMode()
{
    uint32_t connect_id = 0;
    if (ParseReaderConnectId(connect_id) != 0)
        return 1;

    std::vector<std::string> survivor_keys;
    if (LoadKeysFromFile(kFullCycleKeysFile, survivor_keys) != 0)
        return 1;

    const std::string value(256, 'v');
    return RunReaderLookupLoop(
        survivor_keys, value, kFullCycleStopFile, connect_id, kFullCycleBlockSize, kFullCycleMaxBlocks);
}

int RunFullCycleMode()
{
    std::vector<std::string> survivor_keys;
    if (LoadKeysFromFile(kFullCycleKeysFile, survivor_keys) != 0)
        return 1;

    const std::string value(256, 'v');
    mabain::ResourcePool::getInstance().RemoveAll();

    MBConfig rebuild_cfg = MakeSizedJemallocRebuildConfig(
        mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC,
        true, kFullCycleBlockSize, kFullCycleMaxBlocks);
    DB rebuild_db(rebuild_cfg);
    if (!rebuild_db.is_open()) {
        std::cerr << "full_cycle reopen failed: " << rebuild_db.StatusStr() << "\n";
        return 1;
    }

    const IndexHeader* header = rebuild_db.GetDictPtr()->GetHeaderPtr();
    if (header == nullptr || header->RebuildInProgress()) {
        std::cerr << "full_cycle writer still in rebuild state after open\n";
        rebuild_db.Close();
        return 1;
    }
    if (VerifyFindValue(rebuild_db, survivor_keys.back(), value, "full_cycle writer") != 0) {
        rebuild_db.Close();
        return 1;
    }
    if (rebuild_db.Add("full-cycle-post", value) != mabain::MBError::SUCCESS) {
        rebuild_db.Close();
        std::cerr << "full_cycle writer post-rebuild Add failed\n";
        return 1;
    }

    rebuild_db.Close();
    mabain::ResourcePool::getInstance().RemoveAll();
    std::cout << "jemalloc_restart_rebuild_test: full_cycle passed\n";
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
    if (mode == "evacuate_only") {
        return RunEvacuateOnlyMode();
    }
    if (mode == "recover_evacuate") {
        return RunRecoverEvacuateMode();
    }
    if (mode == "full_cycle_prepare") {
        return RunFullCyclePrepareMode();
    }
    if (mode == "reader_loop") {
        return RunReaderLoopMode();
    }
    if (mode == "full_cycle") {
        return RunFullCycleMode();
    }

    return RunScaffoldMode(mode);
}
