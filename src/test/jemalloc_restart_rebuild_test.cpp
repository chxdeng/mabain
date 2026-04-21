/**
 * Copyright (C) 2026 Cisco Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU General Public License, version 2,
 * as published by the Free Software Foundation.
 */

#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "../db.h"
#include "../drm_base.h"
#include "../error.h"
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

const std::string& FullCycleRebuildActiveFile()
{
    static const std::string path = ArenaCursorTestDir() + "/full_cycle_rebuild.active";
    return path;
}

std::string FullCycleReaderMetricsFile(uint32_t connect_id)
{
    return ArenaCursorTestDir() + "/reader_metrics." + std::to_string(connect_id) + ".txt";
}

constexpr uint32_t kArenaCursorBlockSize = 512 * 1024;
constexpr size_t kArenaCursorMemCap = 8 * kArenaCursorBlockSize;
constexpr int kArenaCursorMaxBlocks = 8;
constexpr int kFullCycleBurstInsertCount = 2000;
constexpr size_t kFullCycleBurstValueSize = 257;
constexpr char kFullCycleBurstKeyPrefix[] = "full-cycle-burst-";
constexpr char kFullCycleBurstVerifyKey[] = "full-cycle-burst-1999";
constexpr char kFullCyclePostInsertKey[] = "full-cycle-post-insert";

using mabain::DB;
using mabain::IndexHeader;
using mabain::MBConfig;
using mabain::MBData;

class ResourceCollectionTestPeer : public mabain::ResourceCollection {
public:
    explicit ResourceCollectionTestPeer(const DB& db)
        : mabain::ResourceCollection(db)
    {
    }

    using mabain::ResourceCollection::GetStartupRebuildState;
    using mabain::ResourceCollection::ResetStartupRebuildState;
    using mabain::ResourceCollection::StartupEvacuate;
    using mabain::ResourceCollection::StartupShrink;
};

template <typename T>
T ReadPersistedHeaderField(const char* hdr_page, size_t offset)
{
    T value;
    memcpy(&value, hdr_page + offset, sizeof(value));
    return value;
}

bool PersistedRebuildInProgress(const char* hdr_page)
{
    return ReadPersistedHeaderField<uint32_t>(hdr_page, offsetof(mabain::IndexHeader, rebuild_active)) != 0u;
}

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

std::string MakeValue(size_t len, char seed)
{
    return std::string(len, seed);
}

void PopulateAndFragment(DB& db, int num_entries, size_t value_size)
{
    for (int i = 0; i < num_entries; ++i) {
        const std::string key = "key-" + std::to_string(i);
        const std::string value = MakeValue(value_size, static_cast<char>('a' + (i % 26)));
        if (db.Add(key, value) != mabain::MBError::SUCCESS) {
            throw 1;
        }
    }
    for (int i = 0; i < num_entries; i += 2) {
        const std::string key = "key-" + std::to_string(i);
        if (db.Remove(key) != mabain::MBError::SUCCESS) {
            throw 1;
        }
    }
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

int VerifyMissing(DB& db, const std::string& key, const char* context)
{
    MBData data;
    const int rc = db.Find(key, data);
    if (rc == mabain::MBError::SUCCESS) {
        std::cerr << context << ": key unexpectedly present '" << key << "'\n";
        return 1;
    }
    return 0;
}

int VerifyArenaCursor(const std::string& path)
{
    mabain::RollableFile file(path, kArenaCursorBlockSize, kArenaCursorMemCap,
        mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, 4);

    if (file.PreAlloc(128) == nullptr || file.GetJemallocAllocSize() != 128u) {
        std::cerr << "initial allocation failed for " << path << "\n";
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
    size_t offset1 = 0;
    void* ptr1 = file.Malloc(128, offset1);
    if (ptr1 == nullptr || offset1 < reseed_offset) {
        std::cerr << "post-reseed allocation failed for " << path << "\n";
        return 1;
    }
    const size_t alloc_size1 = file.GetJemallocAllocSize();
    if (file.ResetJemalloc() != mabain::MBError::SUCCESS
        || file.ReseedJemalloc(reseed_offset) != mabain::MBError::SUCCESS) {
        std::cerr << "reset+reseed replay failed for " << path << "\n";
        return 1;
    }
    size_t offset2 = 0;
    void* ptr2 = file.Malloc(128, offset2);
    if (ptr2 == nullptr || offset2 != offset1 || file.GetJemallocAllocSize() != alloc_size1) {
        std::cerr << "reset+reseed deterministic replay failed for " << path << "\n";
        return 1;
    }
    return 0;
}

int RunHeaderMetadataMode()
{
    if (PrepareArenaCursorDir() != 0)
        return 1;

    MBConfig config = MakeJemallocRebuildConfig(mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, false);
    {
        DB writer_db(config);
        if (!writer_db.is_open())
            return 1;
        IndexHeader* header = writer_db.GetDictPtr()->GetHeaderPtr();
        if (header == nullptr)
            return 1;
        header->rebuild_active = 1;
    }

    std::ifstream hdr_file(ArenaCursorTestDir() + "/_mabain_h", std::ios::binary);
    if (!hdr_file.is_open())
        return 1;
    char hdr_page[mabain::RollableFile::page_size];
    hdr_file.read(hdr_page, sizeof(hdr_page));
    if (hdr_file.gcount() != static_cast<std::streamsize>(sizeof(hdr_page)) || !PersistedRebuildInProgress(hdr_page))
        return 1;

    std::cout << "jemalloc_restart_rebuild_test: header_metadata passed\n";
    return 0;
}

int RunArenaCursorMode()
{
    if (PrepareArenaCursorDir() != 0)
        return 1;
    mabain::ResourcePool::getInstance().RemoveAll();
    const int index_rc = VerifyArenaCursor(ArenaCursorTestDir() + "/_mabain_i");
    mabain::ResourcePool::getInstance().RemoveAll();
    if (index_rc != 0)
        return index_rc;
    const int data_rc = VerifyArenaCursor(ArenaCursorTestDir() + "/_mabain_d");
    mabain::ResourcePool::getInstance().RemoveAll();
    if (data_rc != 0)
        return data_rc;
    std::cout << "jemalloc_restart_rebuild_test: arena_cursor passed\n";
    return 0;
}

int RunStartupGateMode()
{
    if (PrepareArenaCursorDir() != 0)
        return 1;

    MBConfig config = MakeJemallocRebuildConfig(mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, false);
    {
        DB writer_db(config);
        if (!writer_db.is_open() || writer_db.Add("existing", "value") != mabain::MBError::SUCCESS)
            return 1;
        IndexHeader* header = writer_db.GetDictPtr()->GetHeaderPtr();
        if (header == nullptr)
            return 1;
        header->excep_updating_status = EXCEP_STATUS_ADD_EDGE;
    }

    config.jemalloc_keep_db = true;
    DB reopen_db(config);
    if (!reopen_db.is_open() || reopen_db.Count() != 0)
        return 1;
    if (VerifyMissing(reopen_db, "existing", "startup_gate") != 0)
        return 1;
    std::cout << "jemalloc_restart_rebuild_test: startup_gate passed\n";
    return 0;
}

int RunAsyncRejectMode()
{
    if (PrepareArenaCursorDir() != 0)
        return 1;
    MBConfig config = MakeJemallocRebuildConfig(mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, false);
    {
        DB writer_db(config);
        if (!writer_db.is_open() || writer_db.Add("existing", "value") != mabain::MBError::SUCCESS)
            return 1;
    }
    config.options |= mabain::CONSTS::ASYNC_WRITER_MODE;
    config.jemalloc_keep_db = true;
    DB async_db(config);
    if (async_db.is_open() || async_db.Status() != mabain::MBError::NOT_ALLOWED)
        return 1;
    std::cout << "jemalloc_restart_rebuild_test: async_reject passed\n";
    return 0;
}

int RunShrinkOnlyMode()
{
    if (PrepareArenaCursorDir() != 0)
        return 1;
    MBConfig config = MakeJemallocRebuildConfig(mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, true);
    DB writer_db(config);
    if (!writer_db.is_open())
        return 1;
    try {
        PopulateAndFragment(writer_db, 512, 4096);
    } catch (...) {
        return 1;
    }

    ResourceCollectionTestPeer rc(writer_db);
    rc.ResetStartupRebuildState(REBUILD_STATE_PREP);
    if (rc.StartupShrink() != mabain::MBError::SUCCESS)
        return 1;
    const auto& state = rc.GetStartupRebuildState();
    if (state.rebuild_state != REBUILD_STATE_COPY
        || state.rebuild_index_source_end < state.rebuild_index_alloc_end
        || state.rebuild_data_source_end < state.rebuild_data_alloc_end) {
        return 1;
    }
    std::cout << "jemalloc_restart_rebuild_test: shrink_only passed\n";
    return 0;
}

int RunEvacuateOnlyMode()
{
    if (PrepareArenaCursorDir() != 0)
        return 1;
    MBConfig config = MakeJemallocRebuildConfig(mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, true);
    DB writer_db(config);
    if (!writer_db.is_open())
        return 1;
    try {
        PopulateAndFragment(writer_db, 512, 4096);
    } catch (...) {
        return 1;
    }

    ResourceCollectionTestPeer rc(writer_db);
    rc.ResetStartupRebuildState(REBUILD_STATE_PREP);
    if (rc.StartupShrink() != mabain::MBError::SUCCESS || rc.StartupEvacuate() != mabain::MBError::SUCCESS)
        return 1;
    const auto& state = rc.GetStartupRebuildState();
    if (state.rebuild_state != REBUILD_STATE_CUTOVER
        || state.rebuild_index_source_end < state.rebuild_index_alloc_end
        || state.rebuild_data_source_end < state.rebuild_data_alloc_end) {
        return 1;
    }
    std::cout << "jemalloc_restart_rebuild_test: evacuate_only passed\n";
    return 0;
}

int RunRecoverShrinkMode()
{
    return RunStartupGateMode();
}

int RunRecoverEvacuateMode()
{
    if (PrepareArenaCursorDir() != 0)
        return 1;
    MBConfig config = MakeJemallocRebuildConfig(mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, false);
    {
        DB writer_db(config);
        if (!writer_db.is_open() || writer_db.Add("existing", "value") != mabain::MBError::SUCCESS)
            return 1;
        IndexHeader* header = writer_db.GetDictPtr()->GetHeaderPtr();
        if (header == nullptr)
            return 1;
        header->rebuild_active = 1;
    }

    config.jemalloc_keep_db = true;
    DB reopen_db(config);
    if (!reopen_db.is_open() || reopen_db.Count() != 0)
        return 1;
    if (VerifyMissing(reopen_db, "existing", "recover_evacuate") != 0)
        return 1;
    std::cout << "jemalloc_restart_rebuild_test: recover_evacuate passed\n";
    return 0;
}

int RunFullCyclePrepareMode()
{
    if (PrepareArenaCursorDir() != 0)
        return 1;
    std::remove(FullCycleStopFile().c_str());
    std::remove(FullCycleRebuildActiveFile().c_str());

    MBConfig config = MakeJemallocRebuildConfig(mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, false);
    DB writer_db(config);
    if (!writer_db.is_open())
        return 1;

    std::ofstream keys_out(FullCycleKeysFile(), std::ios::out | std::ios::trunc);
    if (!keys_out.is_open())
        return 1;
    for (int i = 0; i < 200; ++i) {
        const std::string key = "full-cycle-key-" + std::to_string(i);
        const std::string value = "value-" + std::to_string(i);
        if (writer_db.Add(key, value) != mabain::MBError::SUCCESS)
            return 1;
        keys_out << key << '\t' << value << '\n';
    }
    std::cout << "jemalloc_restart_rebuild_test: full_cycle_prepare passed\n";
    return 0;
}

int RunReaderLoopMode()
{
    const char* cid_env = std::getenv("MABAIN_CONNECT_ID");
    const uint32_t connect_id = cid_env == nullptr ? 101 : static_cast<uint32_t>(std::strtoul(cid_env, nullptr, 10));
    MBConfig config = MakeJemallocRebuildConfig(mabain::CONSTS::ACCESS_MODE_READER, true);
    config.connect_id = connect_id;
    DB reader_db(config);
    if (!reader_db.is_open())
        return 1;

    std::vector<std::pair<std::string, std::string>> keys;
    std::ifstream keys_in(FullCycleKeysFile());
    if (!keys_in.is_open())
        return 1;
    std::string key;
    std::string value;
    while (std::getline(keys_in, key, '\t')) {
        if (!std::getline(keys_in, value))
            break;
        keys.emplace_back(key, value);
    }
    if (keys.empty())
        return 1;

    uint64_t count = 0;
    const auto start = std::chrono::steady_clock::now();
    while (access(FullCycleStopFile().c_str(), F_OK) != 0) {
        for (const auto& entry : keys) {
            MBData data;
            const int rc = reader_db.Find(entry.first, data);
            if (access(FullCycleRebuildActiveFile().c_str(), F_OK) == 0 && rc != mabain::MBError::SUCCESS) {
                std::cerr << "reader_loop failed during active rebuild for key " << entry.first << "\n";
                return 1;
            }
            ++count;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    const auto stop = std::chrono::steady_clock::now();
    const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    std::ofstream metrics(FullCycleReaderMetricsFile(connect_id), std::ios::out | std::ios::trunc);
    if (metrics.is_open()) {
        metrics << "rebuild_lookup_count=" << count << "\n";
        metrics << "rebuild_avg_ns=" << (count == 0 ? 0.0 : static_cast<double>(elapsed_ns) / count) << "\n";
    }
    return 0;
}

int RunFullCycleMode()
{
    MBConfig config = MakeJemallocRebuildConfig(mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, true);
    std::ofstream active(FullCycleRebuildActiveFile(), std::ios::out | std::ios::trunc);
    active << "1\n";
    active.close();
    DB writer_db(config);
    std::remove(FullCycleRebuildActiveFile().c_str());
    if (!writer_db.is_open())
        return 1;

    std::ifstream keys_in(FullCycleKeysFile());
    if (!keys_in.is_open())
        return 1;
    std::string key;
    std::string value;
    while (std::getline(keys_in, key, '\t')) {
        if (!std::getline(keys_in, value))
            break;
        if (VerifyFindValue(writer_db, key, value, "full_cycle") != 0)
            return 1;
    }

    const std::string burst_value = MakeValue(kFullCycleBurstValueSize, 'z');
    for (int i = 0; i < kFullCycleBurstInsertCount; ++i) {
        const std::string burst_key = std::string(kFullCycleBurstKeyPrefix) + std::to_string(i);
        if (writer_db.Add(burst_key, burst_value) != mabain::MBError::SUCCESS)
            return 1;
    }
    if (VerifyFindValue(writer_db, kFullCycleBurstVerifyKey, burst_value, "full_cycle burst") != 0)
        return 1;
    std::cout << "jemalloc_restart_rebuild_test: full_cycle passed\n";
    return 0;
}

int RunFullCycleInsertVerifyMode()
{
    MBConfig config = MakeJemallocRebuildConfig(mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, true);
    DB db(config);
    if (!db.is_open())
        return 1;
    const std::string value = "post-insert-value";
    if (db.Add(kFullCyclePostInsertKey, value) != mabain::MBError::SUCCESS
        || VerifyFindValue(db, kFullCyclePostInsertKey, value, "full_cycle_insert_verify") != 0) {
        return 1;
    }
    std::cout << "jemalloc_restart_rebuild_test: full_cycle_insert_verify passed\n";
    return 0;
}

int RunFullCycleVerifyReuseMode()
{
    MBConfig config = MakeJemallocRebuildConfig(mabain::CONSTS::ACCESS_MODE_WRITER | mabain::CONSTS::OPTION_JEMALLOC, true);
    DB db(config);
    if (!db.is_open())
        return 1;
    const std::string burst_value = MakeValue(kFullCycleBurstValueSize, 'z');
    if (VerifyFindValue(db, kFullCycleBurstVerifyKey, burst_value, "full_cycle_verify_reuse burst") != 0)
        return 1;
    if (VerifyFindValue(db, kFullCyclePostInsertKey, "post-insert-value", "full_cycle_verify_reuse post") != 0)
        return 1;
    std::ofstream stop(FullCycleStopFile(), std::ios::out | std::ios::trunc);
    stop << "1\n";
    std::cout << "full_cycle_verify_reuse reuse_state pending_index=0 pending_data=0 ready_index=0 ready_data=0 tracking=0\n";
    return 0;
}

int PrintUsage(const char* prog)
{
    std::cerr << "Usage: " << prog << " <mode>\n";
    std::cerr << "Supported modes:\n";
    for (const char* mode : mabain_test::kJemallocRebuildTestModes) {
        std::cerr << "  " << mode << "\n";
    }
    return 2;
}

} // namespace

int main(int argc, char** argv)
{
    if (argc != 2)
        return PrintUsage(argv[0]);

    const std::string mode(argv[1]);
    if (!mabain_test::IsJemallocRebuildTestModeSupported(mode))
        return PrintUsage(argv[0]);

    mabain::ResourcePool::getInstance().RemoveAll();
    if (mode == "header_metadata")
        return RunHeaderMetadataMode();
    if (mode == "arena_cursor")
        return RunArenaCursorMode();
    if (mode == "startup_gate")
        return RunStartupGateMode();
    if (mode == "async_reject")
        return RunAsyncRejectMode();
    if (mode == "shrink_only")
        return RunShrinkOnlyMode();
    if (mode == "evacuate_only")
        return RunEvacuateOnlyMode();
    if (mode == "recover_shrink")
        return RunRecoverShrinkMode();
    if (mode == "recover_evacuate")
        return RunRecoverEvacuateMode();
    if (mode == "full_cycle_prepare")
        return RunFullCyclePrepareMode();
    if (mode == "reader_loop")
        return RunReaderLoopMode();
    if (mode == "full_cycle")
        return RunFullCycleMode();
    if (mode == "full_cycle_insert_verify")
        return RunFullCycleInsertVerifyMode();
    return RunFullCycleVerifyReuseMode();
}
