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
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
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

const std::string& ArenaCursorTestDir()
{
    static const std::string path = []() {
        const char* env = std::getenv("MABAIN_JEMALLOC_REBUILD_DIR");
        if (env != nullptr && env[0] != '\0')
            return std::string(env);
        return std::string("/var/tmp/mabain_test/jemalloc_rebuild.") + std::to_string(getpid());
    }();
    return path;
}

const std::string& FullCycleKeysFile()
{
    static const std::string path = ArenaCursorTestDir() + "/full_cycle_keys.txt";
    return path;
}

const std::string& FullCycleStopFile()
{
    static const std::string path = ArenaCursorTestDir() + "/full_cycle.stop";
    return path;
}

const std::string& FullCycleBurstKeysFile()
{
    static const std::string path = ArenaCursorTestDir() + "/full_cycle_burst_keys.txt";
    return path;
}

const std::string& FullCycleRebuildActiveFile()
{
    static const std::string path = ArenaCursorTestDir() + "/full_cycle_rebuild.active";
    return path;
}

std::string FullCycleReaderMetricsFile(uint32_t connect_id)
{
    return ArenaCursorTestDir() + "/reader_metrics." + std::to_string(connect_id) + ".txt";
}

constexpr size_t kArenaCursorBlockSize = 4 * 1024 * 1024;
constexpr size_t kArenaCursorMemCap = 4 * kArenaCursorBlockSize;
constexpr int kArenaCursorMaxBlocks = 4;

template <typename T>
T ReadPersistedHeaderField(const char* hdr_page, size_t offset)
{
    T value;
    memcpy(&value, hdr_page + offset, sizeof(value));
    return value;
}

bool PersistedRebuildInProgress(const char* hdr_page)
{
    return ReadPersistedHeaderField<int>(hdr_page, offsetof(mabain::IndexHeader, rebuild_state)) != REBUILD_STATE_NORMAL;
}

constexpr uint32_t kFullCycleBlockSize = 4 * 1024 * 1024;
constexpr int kFullCycleMaxBlocks = 64;
constexpr int kFullCycleBurstInsertCount = 20000;
constexpr size_t kFullCycleBurstValueSize = 65;
constexpr char kFullCycleBurstKeyPrefix[] = "full-cycle-burst-";
constexpr char kFullCyclePostInsertKey[] = "full-cycle-burst-19999";

using mabain::DB;
using mabain::IndexHeader;
using mabain::MBConfig;
using mabain::MBData;

MBConfig MakeSizedJemallocRebuildConfig(int options, bool keep_db,
    uint32_t block_size, int max_blocks)
{
    MBConfig config = {};
    config.mbdir = ArenaCursorTestDir().c_str();
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
    std::error_code ec;
    std::filesystem::create_directories(std::filesystem::path(ArenaCursorTestDir()).parent_path(), ec);
    if (ec)
        return 1;
    std::filesystem::remove_all(ArenaCursorTestDir(), ec);
    if (ec)
        return 1;
    std::filesystem::create_directories(ArenaCursorTestDir(), ec);
    return ec ? 1 : 0;
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

    const int index_rc = VerifyArenaCursor(ArenaCursorTestDir() + "/_mabain_i");
    mabain::ResourcePool::getInstance().RemoveAll();
    if (index_rc != 0) {
        return index_rc;
    }

    const int data_rc = VerifyArenaCursor(ArenaCursorTestDir() + "/_mabain_d");
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

int TimedVerifyFindValue(DB& db, const std::string& key, const std::string& value,
    const char* context, uint64_t& lookup_ns)
{
    const auto start = std::chrono::steady_clock::now();
    MBData data;
    const int rc = db.Find(key, data);
    const auto end = std::chrono::steady_clock::now();
    lookup_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
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

int WriteReaderMetrics(uint32_t connect_id, size_t overall_lookup_count, uint64_t overall_total_ns,
    size_t rebuild_lookup_count, uint64_t rebuild_total_ns, uint64_t rebuild_max_ns,
    uint64_t fast_slot_guard_count, uint64_t barrier_fallback_guard_count,
    size_t burst_checks)
{
    const std::string path = FullCycleReaderMetricsFile(connect_id);
    std::ofstream out(path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        std::cerr << "failed to open reader metrics file for write: " << path << "\n";
        return 1;
    }

    out << "connect_id=" << connect_id
        << " overall_lookup_count=" << overall_lookup_count
        << " overall_total_ns=" << overall_total_ns
        << " rebuild_lookup_count=" << rebuild_lookup_count
        << " rebuild_total_ns=" << rebuild_total_ns
        << " rebuild_max_ns=" << rebuild_max_ns
        << " fast_slot_guard_count=" << fast_slot_guard_count
        << " barrier_fallback_guard_count=" << barrier_fallback_guard_count
        << " burst_checks=" << burst_checks
        << "\n";
    if (!out) {
        std::cerr << "failed while writing reader metrics file: " << path << "\n";
        return 1;
    }
    return 0;
}

int WriteKeysToFile(const std::vector<std::string>& keys, const std::string& path)
{
    const std::string tmp_path = path + ".tmp";
    std::ofstream out(tmp_path, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        std::cerr << "failed to open key file for write: " << tmp_path << "\n";
        return 1;
    }
    for (const std::string& key : keys)
        out << key << "\n";
    out.close();
    if (!out) {
        std::cerr << "failed while writing key file: " << tmp_path << "\n";
        (void)unlink(tmp_path.c_str());
        return 1;
    }
    if (std::rename(tmp_path.c_str(), path.c_str()) != 0) {
        std::cerr << "failed to rename key file: " << tmp_path << " -> " << path << "\n";
        (void)unlink(tmp_path.c_str());
        return 1;
    }
    return 0;
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
    const std::string& stop_file, const std::string& burst_keys_file,
    uint32_t connect_id, uint32_t block_size, int max_blocks)
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
    size_t burst_checks = 0;
    size_t rebuild_lookup_count = 0;
    uint64_t overall_lookup_total_ns = 0;
    uint64_t rebuild_lookup_total_ns = 0;
    uint64_t rebuild_lookup_max_ns = 0;
    bool burst_loaded = false;
    std::vector<std::string> burst_keys;
    const std::string burst_value(kFullCycleBurstValueSize, 'w');
    while (true) {
        errno = 0;
        if (access(stop_file.c_str(), F_OK) == 0)
            break;
        if (errno != 0 && errno != ENOENT) {
            std::cerr << "reader_loop stop-file check failed\n";
            reader_db.Close();
            return 1;
        }

        errno = 0;
        const bool rebuild_active = access(FullCycleRebuildActiveFile().c_str(), F_OK) == 0;
        if (errno != 0 && errno != ENOENT) {
            std::cerr << "reader_loop rebuild-active check failed\n";
            reader_db.Close();
            return 1;
        }

        const std::string& key = keys[iter % keys.size()];
        uint64_t lookup_ns = 0;
        if (TimedVerifyFindValue(reader_db, key, value, "reader_loop", lookup_ns) != 0) {
            reader_db.Close();
            return 1;
        }
        overall_lookup_total_ns += lookup_ns;
        if (rebuild_active) {
            rebuild_lookup_count++;
            rebuild_lookup_total_ns += lookup_ns;
            if (lookup_ns > rebuild_lookup_max_ns)
                rebuild_lookup_max_ns = lookup_ns;
        }

        if (!burst_loaded) {
            errno = 0;
            if (access(burst_keys_file.c_str(), F_OK) == 0) {
                if (LoadKeysFromFile(burst_keys_file, burst_keys) != 0) {
                    reader_db.Close();
                    return 1;
                }
                burst_loaded = true;
                std::cout << "reader_loop connect_id=" << connect_id
                          << " loaded_burst_keys=" << burst_keys.size() << "\n";
            } else if (errno != 0 && errno != ENOENT) {
                std::cerr << "reader_loop burst-file check failed\n";
                reader_db.Close();
                return 1;
            }
        }

        if (burst_loaded) {
            const std::string& burst_key = burst_keys[burst_checks % burst_keys.size()];
            if (VerifyFindValue(reader_db, burst_key, burst_value, "reader_loop burst") != 0) {
                reader_db.Close();
                return 1;
            }
            burst_checks++;
        }

        iter++;
        if ((iter % 50000) == 0) {
            std::cout << "reader_loop connect_id=" << connect_id
                      << " lookups=" << iter
                      << " burst_checks=" << burst_checks
                      << " rebuild_lookups=" << rebuild_lookup_count
                      << " fast_slot_guard_count=" << reader_db.GetReaderGuardFastSlotCount()
                      << " barrier_fallback_guard_count=" << reader_db.GetReaderGuardBarrierFallbackCount();
            if (rebuild_lookup_count > 0) {
                std::cout << " rebuild_avg_ns=" << (rebuild_lookup_total_ns / rebuild_lookup_count);
            }
            std::cout << "\n";
        }
        if ((iter % 1024) == 0)
            usleep(1000);
    }

    if (WriteReaderMetrics(connect_id, iter, overall_lookup_total_ns, rebuild_lookup_count,
            rebuild_lookup_total_ns, rebuild_lookup_max_ns, reader_db.GetReaderGuardFastSlotCount(),
            reader_db.GetReaderGuardBarrierFallbackCount(), burst_checks) != 0) {
        reader_db.Close();
        return 1;
    }

    reader_db.Close();
    mabain::ResourcePool::getInstance().RemoveAll();
    std::cout << "reader_loop connect_id=" << connect_id
              << " stopped after " << iter << " lookups and " << burst_checks
              << " burst checks rebuild_lookup_count=" << rebuild_lookup_count
              << " fast_slot_guard_count=" << reader_db.GetReaderGuardFastSlotCount()
              << " barrier_fallback_guard_count=" << reader_db.GetReaderGuardBarrierFallbackCount();
    if (rebuild_lookup_count > 0) {
        std::cout << " rebuild_avg_ns=" << (rebuild_lookup_total_ns / rebuild_lookup_count)
                  << " rebuild_max_ns=" << rebuild_lookup_max_ns;
    }
    std::cout << "\n";
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
    std::ifstream in(ArenaCursorTestDir() + "/_mabain_h",
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

        if (ReadPersistedHeaderField<int>(hdr_page, offsetof(IndexHeader, rebuild_state)) != REBUILD_STATE_NORMAL
        || PersistedRebuildInProgress(hdr_page)) {
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

    MBConfig initial_cfg = MakeJemallocRebuildConfig(
        mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, false);
    DB rebuild_db(initial_cfg);
    if (!rebuild_db.is_open()) {
        std::cerr << "initial shrink_only open failed: " << rebuild_db.StatusStr() << "\n";
        return 1;
    }
    if (rebuild_db.Add(key, value) != mabain::MBError::SUCCESS) {
        std::cerr << "initial shrink_only Add failed\n";
        return 1;
    }
    IndexHeader* header = rebuild_db.GetDictPtr()->GetHeaderPtr();
    if (header == nullptr) {
        std::cerr << "shrink_only missing header\n";
        return 1;
    }
    header->ResetRebuildMetadata(REBUILD_STATE_PREP);

    mabain::ResourceCollection rc(rebuild_db);
    if (rc.StartupShrink() != mabain::MBError::SUCCESS) {
        std::cerr << "StartupShrink failed\n";
        return 1;
    }
    if (VerifyFindValue(rebuild_db, key, value, "shrink_only writer") != 0) {
        return 1;
    }

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

    MBConfig initial_cfg = MakeJemallocRebuildConfig(
        mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, false);
    DB rebuild_db(initial_cfg);
    if (!rebuild_db.is_open()) {
        std::cerr << "initial evacuate_only open failed: " << rebuild_db.StatusStr() << "\n";
        return 1;
    }
    if (rebuild_db.Add(key, value) != mabain::MBError::SUCCESS) {
        std::cerr << "initial evacuate_only Add failed\n";
        return 1;
    }
    IndexHeader* header = rebuild_db.GetDictPtr()->GetHeaderPtr();
    if (header == nullptr) {
        std::cerr << "evacuate_only missing header\n";
        return 1;
    }
    header->ResetRebuildMetadata(REBUILD_STATE_PREP);

    mabain::ResourceCollection rc(rebuild_db);
    if (rc.StartupShrink() != mabain::MBError::SUCCESS) {
        std::cerr << "StartupShrink failed\n";
        return 1;
    }
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

    MBConfig initial_cfg = MakeJemallocRebuildConfig(
        mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, false);
    DB rebuild_db(initial_cfg);
    if (!rebuild_db.is_open()) {
        std::cerr << "initial recover_evacuate open failed: " << rebuild_db.StatusStr() << "\n";
        return 1;
    }
    if (rebuild_db.Add(key, value) != mabain::MBError::SUCCESS) {
        std::cerr << "initial recover_evacuate Add failed\n";
        return 1;
    }
    IndexHeader* header = rebuild_db.GetDictPtr()->GetHeaderPtr();
    if (header == nullptr) {
        std::cerr << "recover_evacuate missing header\n";
        return 1;
    }
    header->ResetRebuildMetadata(REBUILD_STATE_PREP);

    mabain::ResourceCollection rc(rebuild_db);
    if (rc.StartupShrink() != mabain::MBError::SUCCESS || rc.StartupEvacuate() != mabain::MBError::SUCCESS) {
        std::cerr << "recover_evacuate initial handoff failed\n";
        return 1;
    }

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

    (void)unlink(FullCycleStopFile().c_str());
    (void)unlink(FullCycleBurstKeysFile().c_str());
    (void)unlink(FullCycleRebuildActiveFile().c_str());
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
        const size_t min_index_existing = 2 * kFullCycleBlockSize;
        const size_t min_data_existing = 4 * kFullCycleBlockSize;
        for (int i = 0;
             i < 6000000
                 && (initial_db.GetDictPtr()->GetMM()->GetExistingBlockEnd() < min_index_existing
                     || initial_db.GetDictPtr()->GetExistingBlockEnd() < min_data_existing);
             i++) {
            keys.push_back("full-cycle-" + std::to_string(i) + std::string(24, 'k'));
            if (initial_db.Add(keys.back(), value) != mabain::MBError::SUCCESS) {
                std::cerr << "initial full_cycle_prepare Add failed\n";
                return 1;
            }
        }
        const size_t index_existing = initial_db.GetDictPtr()->GetMM()->GetExistingBlockEnd();
        const size_t data_existing = initial_db.GetDictPtr()->GetExistingBlockEnd();
        std::cout << "full_cycle_prepare existing_block_end index=" << index_existing
                  << " data=" << data_existing << "\n";
        if (index_existing < min_index_existing
            || data_existing < min_data_existing
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

    if (WriteKeysToFile(survivor_keys, FullCycleKeysFile()) != 0)
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
    if (LoadKeysFromFile(FullCycleKeysFile(), survivor_keys) != 0)
        return 1;

    const std::string value(256, 'v');
    return RunReaderLookupLoop(
        survivor_keys, value, FullCycleStopFile(), FullCycleBurstKeysFile(),
        connect_id, kFullCycleBlockSize, kFullCycleMaxBlocks);
}

std::string MakeFullCycleBurstKey(int i)
{
    return std::string(kFullCycleBurstKeyPrefix) + std::to_string(i);
}

void LogWriterBurstState(const char* phase, int i, DB& db, const IndexHeader* header)
{
    std::cout << "full_cycle writer burst " << phase;
    if (i >= 0)
        std::cout << " i=" << i;
    std::cout << " ready_index=" << db.GetDictPtr()->GetMM()->GetReusableBlockCount()
              << " ready_data=" << db.GetDictPtr()->GetReusableBlockCount()
              << " index_alloc=" << db.GetDictPtr()->GetMM()->GetJemallocAllocSize()
              << " data_alloc=" << db.GetDictPtr()->GetJemallocAllocSize();
    if (header != nullptr) {
        std::cout << " m_index_offset=" << header->m_index_offset
                  << " m_data_offset=" << header->m_data_offset;
    }
    std::cout << "\n";
}

int ValidateReuseState(DB& db, const char* context, size_t min_pending,
    size_t min_total, uint32_t expected_tracking)
{
    const IndexHeader* header = db.GetDictPtr()->GetHeaderPtr();
    if (header == nullptr) {
        std::cerr << context << " missing header\n";
        return 1;
    }

    const size_t pending_index = header->reusable_index_block_count;
    const size_t pending_data = header->reusable_data_block_count;
    const size_t ready_index = db.GetDictPtr()->GetMM()->GetReusableBlockCount();
    const size_t ready_data = db.GetDictPtr()->GetReusableBlockCount();
    const uint32_t tracking = header->reader_epoch_tracking_active.load(MEMORY_ORDER_READER);

    std::cout << context << " pending_index=" << pending_index
              << " pending_data=" << pending_data
              << " ready_index=" << ready_index
              << " ready_data=" << ready_data
              << " tracking=" << tracking << "\n";

    return (tracking == expected_tracking
        && pending_index + pending_data >= min_pending
        && pending_index + pending_data + ready_index + ready_data >= min_total) ? 0 : 1;
}

int RunFullCycleMode()
{
    std::vector<std::string> survivor_keys;
    if (LoadKeysFromFile(FullCycleKeysFile(), survivor_keys) != 0)
        return 1;

    const std::string value(256, 'v');
    mabain::ResourcePool::getInstance().RemoveAll();

    MBConfig rebuild_cfg = MakeSizedJemallocRebuildConfig(
        mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC,
        true, kFullCycleBlockSize, kFullCycleMaxBlocks);
    {
        std::ofstream active(FullCycleRebuildActiveFile(), std::ios::out | std::ios::trunc);
        if (!active.is_open()) {
            std::cerr << "full_cycle failed to create rebuild-active marker\n";
            return 1;
        }
    }
    DB rebuild_db(rebuild_cfg);
    (void)unlink(FullCycleRebuildActiveFile().c_str());
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
    if (ValidateReuseState(rebuild_db, "full_cycle writer reuse_state", 0, 2, 0) != 0) {
        rebuild_db.Close();
        std::cerr << "full_cycle writer reuse_state did not show reusable stale blocks\n";
        return 1;
    }

    const size_t ready_index_before = rebuild_db.GetDictPtr()->GetMM()->GetReusableBlockCount();
    const size_t ready_data_before = rebuild_db.GetDictPtr()->GetReusableBlockCount();
    LogWriterBurstState("start", -1, rebuild_db, header);
    const std::string burst_value(kFullCycleBurstValueSize, 'w');
    std::vector<std::string> burst_keys;
    burst_keys.reserve(kFullCycleBurstInsertCount);
    for (int i = 0; i < kFullCycleBurstInsertCount; i++) {
        const std::string key = MakeFullCycleBurstKey(i);
        burst_keys.push_back(key);
        if (i < 4 || ((i + 1) % 500) == 0)
            LogWriterBurstState("before_add", i, rebuild_db, header);
        int rval = rebuild_db.Add(key, burst_value);
        if (rval != mabain::MBError::SUCCESS) {
            std::cerr << "full_cycle writer burst Add failed at i=" << i
                      << " rc=" << rval
                      << " err=" << mabain::MBError::get_error_str(rval) << "\n";
            LogWriterBurstState("after_add_failure", i, rebuild_db, header);
            rebuild_db.Close();
            return 1;
        }
        if (i < 4 || ((i + 1) % 500) == 0)
            LogWriterBurstState("after_add", i, rebuild_db, header);
    }
    if (WriteKeysToFile(burst_keys, FullCycleBurstKeysFile()) != 0) {
        rebuild_db.Close();
        return 1;
    }
    std::cout << "full_cycle writer published burst_keys=" << burst_keys.size() << "\n";
    if (VerifyFindValue(rebuild_db, kFullCyclePostInsertKey, burst_value, "full_cycle writer burst") != 0) {

        rebuild_db.Close();
        return 1;
    }
    const size_t ready_index_after = rebuild_db.GetDictPtr()->GetMM()->GetReusableBlockCount();
    const size_t ready_data_after = rebuild_db.GetDictPtr()->GetReusableBlockCount();
    std::cout << "full_cycle writer burst reuse ready_index_before=" << ready_index_before
              << " ready_index_after=" << ready_index_after
              << " ready_data_before=" << ready_data_before
              << " ready_data_after=" << ready_data_after << "\n";
    if (!(ready_index_after < ready_index_before || ready_data_after < ready_data_before)) {
        std::cout << "full_cycle writer burst did not consume reusable stale blocks in this small-value configuration\n";
    }

    rebuild_db.Close();
    mabain::ResourcePool::getInstance().RemoveAll();
    std::cout << "jemalloc_restart_rebuild_test: full_cycle passed\n";
    return 0;
}

int RunFullCycleInsertVerifyMode()
{
    const std::string value(256, 'v');
    mabain::ResourcePool::getInstance().RemoveAll();

    MBConfig cfg = MakeSizedJemallocRebuildConfig(
        mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC,
        true, kFullCycleBlockSize, kFullCycleMaxBlocks);
    DB db(cfg);
    if (!db.is_open()) {
        std::cerr << "full_cycle_insert_verify reopen failed: " << db.StatusStr() << "\n";
        return 1;
    }
    const IndexHeader* header = db.GetDictPtr()->GetHeaderPtr();
    if (header == nullptr || header->RebuildInProgress()) {
        std::cerr << "full_cycle_insert_verify writer still in rebuild state\n";
        db.Close();
        return 1;
    }
    if (db.Add(kFullCyclePostInsertKey, value) != mabain::MBError::SUCCESS
        || VerifyFindValue(db, kFullCyclePostInsertKey, value, "full_cycle_insert_verify writer") != 0) {
        db.Close();
        return 1;
    }
    db.Close();
    std::cout << "jemalloc_restart_rebuild_test: full_cycle_insert_verify passed\n";
    return 0;
}

int RunFullCycleVerifyReuseMode()
{
    std::vector<std::string> survivor_keys;
    if (LoadKeysFromFile(FullCycleKeysFile(), survivor_keys) != 0)
        return 1;

    const std::string value(256, 'v');
    mabain::ResourcePool::getInstance().RemoveAll();

    MBConfig verify_cfg = MakeSizedJemallocRebuildConfig(
        mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC,
        true, kFullCycleBlockSize, kFullCycleMaxBlocks);
    DB verify_db(verify_cfg);
    if (!verify_db.is_open()) {
        std::cerr << "full_cycle_verify_reuse reopen failed: " << verify_db.StatusStr() << "\n";
        return 1;
    }
    const std::string burst_value(kFullCycleBurstValueSize, 'w');
    if (VerifyFindValue(verify_db, survivor_keys.back(), value, "full_cycle_verify_reuse writer") != 0
        || VerifyFindValue(verify_db, kFullCyclePostInsertKey, burst_value, "full_cycle_verify_reuse burst") != 0
        || ValidateReuseState(verify_db, "full_cycle_verify_reuse reuse_state", 0, 2, 0) != 0) {
        verify_db.Close();
        return 1;
    }
    verify_db.Close();
    return 0;
}

} // namespace

int main(int argc, char* argv[])
{
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;
    mabain::DB::SetLogFile(ArenaCursorTestDir() + "/mabain.log");
    mabain::DB::LogDebug();

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
    if (mode == "full_cycle_insert_verify") {
        return RunFullCycleInsertVerifyMode();
    }
    if (mode == "full_cycle_verify_reuse") {
        int rval = RunFullCycleVerifyReuseMode();
        mabain::DB::CloseLogFile();
        return rval;
    }

    int rval = RunScaffoldMode(mode);
    mabain::DB::CloseLogFile();
    return rval;
}
