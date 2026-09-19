/**
 * Copyright (C) 2026 Cisco Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.
 */

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <unistd.h>

#include "../db.h"
#include "../mb_rc.h"
#include "../resource_pool.h"

namespace {

constexpr uint32_t kBlockSize = 16 * 1024 * 1024;
constexpr int kMaxBlocks = 8;
constexpr int kEntryCount = 2304;
constexpr size_t kValueSize = 16 * 1024;

class ResourceCollectionTestPeer : public mabain::ResourceCollection {
public:
    explicit ResourceCollectionTestPeer(const mabain::DB& db)
        : mabain::ResourceCollection(db)
    {
    }

    using mabain::ResourceCollection::GetStartupRebuildState;
    using mabain::ResourceCollection::ResetStartupRebuildState;
    using mabain::ResourceCollection::StartupEvacuate;
    using mabain::ResourceCollection::StartupRebuildComplete;
    using mabain::ResourceCollection::StartupShrink;
};

size_t AlignUp(size_t value, size_t alignment)
{
    return ((value + alignment - 1) / alignment) * alignment;
}

bool VerifyBatch(const mabain::ReusableBlockEntry* entries, uint32_t count,
    size_t first_block, size_t expected_count, const char* label)
{
    if (count != expected_count) {
        std::cerr << label << ": expected " << expected_count
                  << " quarantined blocks, got " << count << "\n";
        return false;
    }
    if (count == 0)
        return true;

    const uint64_t retire_epoch = entries[0].retire_epoch;
    if (retire_epoch == 0) {
        std::cerr << label << ": zero retirement epoch\n";
        return false;
    }
    for (uint32_t i = 0; i < count; i++) {
        if (entries[i].in_use != REUSABLE_BLOCK_STATE_QUARANTINED
            || entries[i].block_order != first_block + i
            || entries[i].retire_epoch != retire_epoch) {
            std::cerr << label << ": invalid batch entry " << i << "\n";
            return false;
        }
    }
    return true;
}

} // namespace

int main()
{
    namespace fs = std::filesystem;
    const std::string db_dir = "/var/tmp/mabain_test/jemalloc_rebuild_batch."
        + std::to_string(getpid());
    std::error_code ec;
    fs::remove_all(db_dir, ec);
    fs::create_directories(db_dir, ec);
    if (ec) {
        std::cerr << "failed to prepare test directory: " << ec.message() << "\n";
        return 1;
    }

    mabain::ResourcePool::getInstance().RemoveAll();
    int result = 0;
    {
        mabain::MBConfig config = {};
        config.mbdir = db_dir.c_str();
        config.options = mabain::CONSTS::ACCESS_MODE_WRITER
            | mabain::CONSTS::OPTION_JEMALLOC;
        config.block_size_index = kBlockSize;
        config.block_size_data = kBlockSize;
        config.max_num_index_block = kMaxBlocks;
        config.max_num_data_block = kMaxBlocks;
        config.memcap_index = static_cast<size_t>(kBlockSize) * kMaxBlocks;
        config.memcap_data = static_cast<size_t>(kBlockSize) * kMaxBlocks;
        config.num_entry_per_bucket = 500;
        config.jemalloc_keep_db = true;

        mabain::DB db(config);
        if (!db.is_open()) {
            std::cerr << "failed to open test database: " << db.Status() << "\n";
            result = 1;
        }

        std::vector<std::pair<std::string, std::string>> retained;
        for (int i = 0; result == 0 && i < kEntryCount; i++) {
            const std::string key = "batch-key-" + std::to_string(i);
            const std::string value(kValueSize, static_cast<char>('a' + (i % 26)));
            if (db.Add(key, value) != mabain::MBError::SUCCESS) {
                std::cerr << "Add failed at entry " << i << "\n";
                result = 1;
                break;
            }
            if ((i % 4) == 3)
                retained.emplace_back(key, value);
        }
        for (int i = 0; result == 0 && i < kEntryCount; i++) {
            if ((i % 4) == 3)
                continue;
            const std::string key = "batch-key-" + std::to_string(i);
            if (db.Remove(key) != mabain::MBError::SUCCESS) {
                std::cerr << "Remove failed at entry " << i << "\n";
                result = 1;
            }
        }

        ResourceCollectionTestPeer rc(db);
        rc.ResetStartupRebuildState(REBUILD_STATE_PREP);
        if (result == 0 && rc.StartupShrink() != mabain::MBError::SUCCESS) {
            std::cerr << "StartupShrink failed\n";
            result = 1;
        }

        size_t index_start = 0;
        size_t data_start = 0;
        size_t index_blocks = 0;
        size_t data_blocks = 0;
        if (result == 0) {
            const auto& state = rc.GetStartupRebuildState();
            index_start = AlignUp(state.rebuild_index_alloc_end, kBlockSize);
            data_start = AlignUp(state.rebuild_data_alloc_end, kBlockSize);
            if (state.rebuild_index_source_end > index_start) {
                index_blocks = (state.rebuild_index_source_end - index_start)
                    / kBlockSize;
            }
            if (state.rebuild_data_source_end > data_start) {
                data_blocks = (state.rebuild_data_source_end - data_start)
                    / kBlockSize;
            }
            if (index_blocks <= 1 && data_blocks <= 1) {
                std::cerr << "test setup did not create a multi-block evacuation window\n";
                result = 1;
            }
        }

        if (result == 0 && rc.StartupEvacuate() != mabain::MBError::SUCCESS) {
            std::cerr << "StartupEvacuate failed\n";
            result = 1;
        }

        if (result == 0) {
            const auto& state = rc.GetStartupRebuildState();
            const size_t expected_index = std::min<size_t>(
                MB_MAX_REUSABLE_BLOCKS, index_blocks);
            const size_t expected_data = std::min<size_t>(
                MB_MAX_REUSABLE_BLOCKS, data_blocks);
            if (state.rebuild_index_block_cursor
                    != index_start + expected_index * kBlockSize
                || state.rebuild_data_block_cursor
                    != data_start + expected_data * kBlockSize
                || !VerifyBatch(state.reusable_index_block,
                    state.reusable_index_block_count,
                    index_start / kBlockSize, expected_index, "index")
                || !VerifyBatch(state.reusable_data_block,
                    state.reusable_data_block_count,
                    data_start / kBlockSize, expected_data, "data")) {
                result = 1;
            }
        }

        for (int attempts = 0;
             result == 0 && !rc.StartupRebuildComplete() && attempts < 128;
             attempts++) {
            if (rc.StartupEvacuate() != mabain::MBError::SUCCESS) {
                std::cerr << "StartupEvacuate completion failed\n";
                result = 1;
            }
        }
        if (result == 0 && !rc.StartupRebuildComplete()) {
            std::cerr << "startup rebuild did not complete\n";
            result = 1;
        }

        for (const auto& entry : retained) {
            if (result != 0)
                break;
            mabain::MBData data;
            if (db.Find(entry.first, data) != mabain::MBError::SUCCESS
                || data.data_len != static_cast<int>(entry.second.size())
                || !std::equal(entry.second.begin(), entry.second.end(), data.buff)) {
                std::cerr << "value verification failed for " << entry.first << "\n";
                result = 1;
            }
        }
    }

    mabain::ResourcePool::getInstance().RemoveAll();
    fs::remove_all(db_dir, ec);
    if (result == 0)
        std::cout << "jemalloc_rebuild_batch_test: passed\n";
    return result;
}
