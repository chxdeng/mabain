/**
 * Copyright (C) 2026 Cisco Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU General Public License, version 2,
 * as published by the Free Software Foundation.
 */

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unistd.h>

#include <gtest/gtest.h>

#include "../db.h"
#include "../dict.h"
#include "../drm_base.h"
#include "../mb_rc.h"
#include "../resource_pool.h"
#include "../rollable_file.h"
#include "../test/jemalloc_rebuild_test_modes.h"

using namespace mabain;
using namespace mabain_test;

namespace {

const std::string& JemallocRebuildTestPath()
{
    static const std::string path = []() {
        const char* env = std::getenv("MABAIN_JEMALLOC_REBUILD_DIR");
        if (env != nullptr && env[0] != '\0')
            return std::string(env);
        return std::string("/var/tmp/mabain_test/jemalloc_rebuild.") + std::to_string(getpid());
    }();
    return path;
}

constexpr uint32_t kJemallocRebuildBlockSize = 1024 * 1024;
constexpr int kJemallocRebuildMaxBlocks = 8;

template <typename T>
T ReadPersistedHeaderField(const char* hdr_page, size_t offset)
{
    T value;
    memcpy(&value, hdr_page + offset, sizeof(value));
    return value;
}

bool PersistedRebuildInProgress(const char* hdr_page)
{
    return ReadPersistedHeaderField<uint32_t>(hdr_page, offsetof(IndexHeader, rebuild_active)) != 0u;
}

MBConfig MakeSizedJemallocRebuildConfig(int options, bool keep_db,
    uint32_t block_size, int max_blocks)
{
    MBConfig config = {};
    config.mbdir = JemallocRebuildTestPath().c_str();
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
        options, keep_db, kJemallocRebuildBlockSize, kJemallocRebuildMaxBlocks);
}

bool RemoveJemallocRebuildTestFiles()
{
    std::error_code ec;
    std::filesystem::create_directories(std::filesystem::path(JemallocRebuildTestPath()).parent_path(), ec);
    if (ec)
        return false;
    std::filesystem::remove_all(JemallocRebuildTestPath(), ec);
    return !ec;
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
        ASSERT_EQ(db.Add(key, value), MBError::SUCCESS);
    }
    for (int i = 0; i < num_entries; i += 2) {
        const std::string key = "key-" + std::to_string(i);
        ASSERT_EQ(db.Remove(key), MBError::SUCCESS);
    }
}

void ExpectMissing(DB& db, const std::string& key)
{
    MBData data;
    EXPECT_NE(db.Find(key, data), MBError::SUCCESS);
}

void ExpectValue(DB& db, const std::string& key, const std::string& value)
{
    MBData data;
    ASSERT_EQ(db.Find(key, data), MBError::SUCCESS);
    ASSERT_NE(data.buff, nullptr);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.buff), data.data_len), value);
}

class JemallocRebuildMetadataTest : public ::testing::Test {
public:
    void SetUp() override
    {
        ResourcePool::getInstance().RemoveAll();
        ASSERT_TRUE(RemoveJemallocRebuildTestFiles());
        std::error_code ec;
        std::filesystem::create_directories(JemallocRebuildTestPath(), ec);
        ASSERT_FALSE(ec);
    }

    void TearDown() override
    {
        ResourcePool::getInstance().RemoveAll();
        EXPECT_TRUE(RemoveJemallocRebuildTestFiles());
    }
};

} // namespace

namespace mabain {

class ResourceCollectionTestPeer : public ResourceCollection {
public:
    explicit ResourceCollectionTestPeer(const DB& db)
        : ResourceCollection(db)
    {
    }

    using ResourceCollection::DrainReusableBlocks;
    using ResourceCollection::GetStartupRebuildState;
    using ResourceCollection::QueueReusableBlock;
    using ResourceCollection::ReleaseReusableBlocks;
    using ResourceCollection::ResetStartupRebuildState;
    using ResourceCollection::StartupEvacuate;
    using ResourceCollection::StartupRebuildComplete;
    using ResourceCollection::StartupShrink;
};

class DBTestPeer {
public:
    static int RunStartupRebuild(DB& db)
    {
        return db.RunStartupRebuild();
    }

    static uint64_t BeginReaderEpochGuard(DB& db)
    {
        return db.BeginReaderEpochGuard();
    }

    static void EndReaderEpochGuard(DB& db, uint64_t epoch)
    {
        db.EndReaderEpochGuard(epoch);
    }
};

} // namespace mabain

TEST(JemallocRebuildHarnessTest, ModeListContainsExpectedPhases)
{
    EXPECT_EQ(kJemallocRebuildTestModes.size(), 13u);
    EXPECT_TRUE(IsJemallocRebuildTestModeSupported("header_metadata"));
    EXPECT_TRUE(IsJemallocRebuildTestModeSupported("recover_evacuate"));
    EXPECT_TRUE(IsJemallocRebuildTestModeSupported("full_cycle_verify_reuse"));
    EXPECT_FALSE(IsJemallocRebuildTestModeSupported("unknown_mode"));
}

TEST(JemallocRebuildHeaderHelperTest, SetRebuildActiveSetsMarker)
{
    IndexHeader header = {};
    header.SetRebuildActive();
    EXPECT_EQ(header.rebuild_active, 1u);
    EXPECT_TRUE(header.RebuildInProgress());
}

TEST(JemallocRebuildHeaderHelperTest, ClearRebuildMetadataClearsMarker)
{
    IndexHeader header = {};
    header.rebuild_active = 1;
    header.ClearRebuildMetadata();
    EXPECT_EQ(header.rebuild_active, 0u);
    EXPECT_FALSE(header.RebuildInProgress());
}

TEST_F(JemallocRebuildMetadataTest, NewDbInitializesRebuildMetadataToZero)
{
    MBConfig config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB writer_db(config);
    ASSERT_TRUE(writer_db.is_open());

    const IndexHeader* header = writer_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    EXPECT_EQ(header->rebuild_active, 0u);
    EXPECT_EQ(header->jemalloc_index_free_start, 0u);
    EXPECT_EQ(header->jemalloc_data_free_start, 0u);
    EXPECT_EQ(header->reader_epoch_slot_count, MB_MAX_READER_EPOCH_SLOT);
    EXPECT_EQ(header->reader_epoch.load(MEMORY_ORDER_READER), 1u);
}

TEST_F(JemallocRebuildMetadataTest, PrintHeaderIncludesRebuildMarker)
{
    MBConfig config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB writer_db(config);
    ASSERT_TRUE(writer_db.is_open());
    IndexHeader* header = writer_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    header->rebuild_active = 1;

    std::ostringstream out;
    writer_db.PrintHeader(out);
    EXPECT_NE(out.str().find("rebuild active: 1"), std::string::npos);
}

TEST_F(JemallocRebuildMetadataTest, PersistedHeaderKeepsRebuildMarker)
{
    MBConfig config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    {
        DB writer_db(config);
        ASSERT_TRUE(writer_db.is_open());
        IndexHeader* header = writer_db.GetDictPtr()->GetHeaderPtr();
        ASSERT_NE(header, nullptr);
        header->rebuild_active = 1;
    }

    std::ifstream hdr_file(JemallocRebuildTestPath() + "/_mabain_h", std::ios::binary);
    ASSERT_TRUE(hdr_file.is_open());
    char hdr_page[RollableFile::page_size];
    hdr_file.read(hdr_page, sizeof(hdr_page));
    ASSERT_EQ(hdr_file.gcount(), static_cast<std::streamsize>(sizeof(hdr_page)));
    EXPECT_TRUE(PersistedRebuildInProgress(hdr_page));
}

TEST_F(JemallocRebuildMetadataTest, QuarantinedBlockStaysUnavailableWhileReaderPinnedToRetireEpoch)
{
    MBConfig config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB writer_db(config);
    ASSERT_TRUE(writer_db.is_open());
    auto* header = writer_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);

    ResourceCollectionTestPeer rc(writer_db);
    ReusableBlockEntry entries[MB_MAX_REUSABLE_BLOCKS] = {};
    uint32_t count = 0;
    ASSERT_EQ(rc.QueueReusableBlock(entries, count, 2, 9), MBError::SUCCESS);
    header->reader_epoch_slot[0].connect_id.store(99, MEMORY_ORDER_WRITER);
    header->reader_epoch_slot[0].epoch.store(9, MEMORY_ORDER_WRITER);

    ASSERT_EQ(rc.ReleaseReusableBlocks(entries, count), MBError::SUCCESS);
    EXPECT_EQ(count, 1u);
    EXPECT_EQ(entries[0].in_use, REUSABLE_BLOCK_STATE_QUARANTINED);
}

TEST_F(JemallocRebuildMetadataTest, QuarantinedBlockBecomesReusableAfterReadersAdvance)
{
    MBConfig config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB writer_db(config);
    ASSERT_TRUE(writer_db.is_open());
    auto* header = writer_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);

    ResourceCollectionTestPeer rc(writer_db);
    ReusableBlockEntry entries[MB_MAX_REUSABLE_BLOCKS] = {};
    uint32_t count = 0;
    ASSERT_EQ(rc.QueueReusableBlock(entries, count, 2, 9), MBError::SUCCESS);
    header->reader_epoch_slot[0].connect_id.store(99, MEMORY_ORDER_WRITER);
    header->reader_epoch_slot[0].epoch.store(10, MEMORY_ORDER_WRITER);
    ASSERT_EQ(rc.ReleaseReusableBlocks(entries, count), MBError::SUCCESS);
    EXPECT_EQ(entries[0].in_use, REUSABLE_BLOCK_STATE_READY);
}

TEST_F(JemallocRebuildMetadataTest, ReaderEpochGuardUsesFastSlotWhenAvailable)
{
    MBConfig writer_config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB writer_db(writer_config);
    ASSERT_TRUE(writer_db.is_open());
    ASSERT_EQ(writer_db.Add("seed", "value"), MBError::SUCCESS);
    auto* header = writer_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);

    MBConfig config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_READER | CONSTS::OPTION_JEMALLOC, false);
    config.connect_id = 77;
    DB reader_db(config);
    ASSERT_TRUE(reader_db.is_open());
    header->reader_epoch_tracking_active.store(1, MEMORY_ORDER_WRITER);

    const uint64_t fast_before = reader_db.GetReaderGuardFastSlotCount();
    const uint64_t fallback_before = reader_db.GetReaderGuardBarrierFallbackCount();
    const uint64_t token = mabain::DBTestPeer::BeginReaderEpochGuard(reader_db);
    EXPECT_NE(token, 0u);
    EXPECT_EQ(reader_db.GetReaderGuardFastSlotCount(), fast_before + 1);
    EXPECT_EQ(reader_db.GetReaderGuardBarrierFallbackCount(), fallback_before);
    EXPECT_EQ(header->reader_epoch_slot[token - 1].connect_id.load(MEMORY_ORDER_READER), 77u);
    mabain::DBTestPeer::EndReaderEpochGuard(reader_db, token);
}

TEST_F(JemallocRebuildMetadataTest, ReaderEpochGuardFallsBackToBarrierWhenSlotsBusy)
{
    MBConfig writer_config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB writer_db(writer_config);
    ASSERT_TRUE(writer_db.is_open());
    ASSERT_EQ(writer_db.Add("seed", "value"), MBError::SUCCESS);
    auto* header = writer_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);

    MBConfig config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_READER | CONSTS::OPTION_JEMALLOC, false);
    config.connect_id = 88;
    DB reader_db(config);
    ASSERT_TRUE(reader_db.is_open());
    header->reader_epoch_tracking_active.store(1, MEMORY_ORDER_WRITER);
    for (uint32_t i = 0; i < MB_MAX_READER_EPOCH_SLOT; ++i)
        header->reader_epoch_slot[i].connect_id.store(i + 1, MEMORY_ORDER_WRITER);

    const uint64_t fast_before = reader_db.GetReaderGuardFastSlotCount();
    const uint64_t fallback_before = reader_db.GetReaderGuardBarrierFallbackCount();
    const uint64_t token = mabain::DBTestPeer::BeginReaderEpochGuard(reader_db);
    EXPECT_NE(token, 0u);
    EXPECT_EQ(reader_db.GetReaderGuardFastSlotCount(), fast_before);
    EXPECT_EQ(reader_db.GetReaderGuardBarrierFallbackCount(), fallback_before + 1);
    mabain::DBTestPeer::EndReaderEpochGuard(reader_db, token);
}

TEST_F(JemallocRebuildMetadataTest, ReaderEpochGuardEndClearsClaimedSlot)
{
    MBConfig writer_config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB writer_db(writer_config);
    ASSERT_TRUE(writer_db.is_open());
    ASSERT_EQ(writer_db.Add("seed", "value"), MBError::SUCCESS);
    auto* header = writer_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);

    MBConfig config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_READER | CONSTS::OPTION_JEMALLOC, false);
    config.connect_id = 123;
    DB reader_db(config);
    ASSERT_TRUE(reader_db.is_open());
    header->reader_epoch_tracking_active.store(1, MEMORY_ORDER_WRITER);

    const uint64_t token = mabain::DBTestPeer::BeginReaderEpochGuard(reader_db);
    ASSERT_NE(token, 0u);
    mabain::DBTestPeer::EndReaderEpochGuard(reader_db, token);
    EXPECT_EQ(header->reader_epoch_slot[token - 1].connect_id.load(MEMORY_ORDER_READER), 0u);
}

TEST_F(JemallocRebuildMetadataTest, RunStartupRebuildOnReaderReturnsControlledError)
{
    MBConfig writer_config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB writer_db(writer_config);
    ASSERT_TRUE(writer_db.is_open());
    ASSERT_EQ(writer_db.Add("existing", "value"), MBError::SUCCESS);
    writer_db.Close();

    MBConfig reader_config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_READER, true);
    DB reader_db(reader_config);
    ASSERT_TRUE(reader_db.is_open());
    EXPECT_EQ(mabain::DBTestPeer::RunStartupRebuild(reader_db), MBError::NOT_ALLOWED);
}

TEST_F(JemallocRebuildMetadataTest, RunStartupRebuildWithExceptionStatusClearsDbWithoutRecovery)
{
    MBConfig config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    {
        DB writer_db(config);
        ASSERT_TRUE(writer_db.is_open());
        ASSERT_EQ(writer_db.Add("existing", "value"), MBError::SUCCESS);
        auto* header = writer_db.GetDictPtr()->GetHeaderPtr();
        ASSERT_NE(header, nullptr);
        header->excep_updating_status = EXCEP_STATUS_ADD_EDGE;
    }

    config.jemalloc_keep_db = true;
    DB reopen_db(config);
    ASSERT_TRUE(reopen_db.is_open());
    EXPECT_EQ(reopen_db.Count(), 0);
    auto* header = reopen_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    EXPECT_EQ(header->rebuild_active, 0u);
    EXPECT_EQ(header->excep_updating_status, EXCEP_STATUS_NONE);
    ExpectMissing(reopen_db, "existing");
}

TEST_F(JemallocRebuildMetadataTest, RunStartupRebuildWithStaleMarkerClearsDb)
{
    MBConfig config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    {
        DB writer_db(config);
        ASSERT_TRUE(writer_db.is_open());
        ASSERT_EQ(writer_db.Add("existing", "value"), MBError::SUCCESS);
        auto* header = writer_db.GetDictPtr()->GetHeaderPtr();
        ASSERT_NE(header, nullptr);
        header->rebuild_active = 1;
    }

    config.jemalloc_keep_db = true;
    DB reopen_db(config);
    ASSERT_TRUE(reopen_db.is_open());
    EXPECT_EQ(reopen_db.Count(), 0);
    auto* header = reopen_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    EXPECT_EQ(header->rebuild_active, 0u);
    ExpectMissing(reopen_db, "existing");
}

TEST_F(JemallocRebuildMetadataTest, WarmRestartWithKeepDbCompletesStartupRebuildAndPreservesReads)
{
    MBConfig config = MakeSizedJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false, 512 * 1024, 8);
    {
        DB writer_db(config);
        ASSERT_TRUE(writer_db.is_open());
        PopulateAndFragment(writer_db, 256, 2048);
        ASSERT_EQ(writer_db.Add("steady", "value"), MBError::SUCCESS);
    }

    config.jemalloc_keep_db = true;
    DB reopen_db(config);
    ASSERT_TRUE(reopen_db.is_open());
    auto* header = reopen_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    EXPECT_EQ(header->rebuild_active, 0u);
    EXPECT_GT(header->jemalloc_index_free_start, 0u);
    EXPECT_GT(header->jemalloc_data_free_start, 0u);
    ExpectValue(reopen_db, "steady", "value");
}

TEST_F(JemallocRebuildMetadataTest, WarmRestartWithoutKeepDbResetsExistingJemallocData)
{
    MBConfig config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    {
        DB writer_db(config);
        ASSERT_TRUE(writer_db.is_open());
        ASSERT_EQ(writer_db.Add("existing", "value"), MBError::SUCCESS);
    }

    DB reopen_db(config);
    ASSERT_TRUE(reopen_db.is_open());
    EXPECT_EQ(reopen_db.Count(), 0);
    ExpectMissing(reopen_db, "existing");
}

TEST_F(JemallocRebuildMetadataTest, StartupShrinkCapturesCurrentJemallocTailsInRuntimeState)
{
    MBConfig config = MakeSizedJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, true, 512 * 1024, 8);
    DB writer_db(config);
    ASSERT_TRUE(writer_db.is_open());
    PopulateAndFragment(writer_db, 512, 4096);

    ResourceCollectionTestPeer rc(writer_db);
    rc.ResetStartupRebuildState(REBUILD_STATE_PREP);
    ASSERT_EQ(rc.StartupShrink(), MBError::SUCCESS);

    const auto& state = rc.GetStartupRebuildState();
    auto* header = writer_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    EXPECT_EQ(state.rebuild_state, REBUILD_STATE_COPY);
    EXPECT_EQ(state.rebuild_index_alloc_end, header->m_index_offset);
    EXPECT_EQ(state.rebuild_data_alloc_end, header->m_data_offset);
    EXPECT_GE(state.rebuild_index_source_end, state.rebuild_index_alloc_end);
    EXPECT_GE(state.rebuild_data_source_end, state.rebuild_data_alloc_end);
}

TEST_F(JemallocRebuildMetadataTest, StartupShrinkRejectsNonJemallocWriterGracefully)
{
    MBConfig config = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_WRITER, false);
    DB writer_db(config);
    ASSERT_TRUE(writer_db.is_open());
    ResourceCollectionTestPeer rc(writer_db);
    rc.ResetStartupRebuildState(REBUILD_STATE_PREP);
    EXPECT_EQ(rc.StartupShrink(), MBError::NOT_ALLOWED);
}

TEST_F(JemallocRebuildMetadataTest, StartupEvacuateAdvancesRuntimeState)
{
    MBConfig config = MakeSizedJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, true, 512 * 1024, 8);
    DB writer_db(config);
    ASSERT_TRUE(writer_db.is_open());
    PopulateAndFragment(writer_db, 512, 4096);

    ResourceCollectionTestPeer rc(writer_db);
    rc.ResetStartupRebuildState(REBUILD_STATE_PREP);
    ASSERT_EQ(rc.StartupShrink(), MBError::SUCCESS);
    ASSERT_EQ(rc.StartupEvacuate(), MBError::SUCCESS);

    const auto& state = rc.GetStartupRebuildState();
    EXPECT_EQ(state.rebuild_state, REBUILD_STATE_CUTOVER);
    EXPECT_GE(state.rebuild_index_block_cursor, 0u);
    EXPECT_GE(state.rebuild_data_block_cursor, 0u);
    EXPECT_GE(state.rebuild_index_source_end, state.rebuild_index_alloc_end);
    EXPECT_GE(state.rebuild_data_source_end, state.rebuild_data_alloc_end);
}

TEST_F(JemallocRebuildMetadataTest, StartupEvacuateRejectsEmptySourceWindowGracefully)
{
    MBConfig config = MakeSizedJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, true, 512 * 1024, 8);
    DB writer_db(config);
    ASSERT_TRUE(writer_db.is_open());
    ResourceCollectionTestPeer rc(writer_db);
    rc.ResetStartupRebuildState(REBUILD_STATE_COPY);
    auto& state = const_cast<StartupRebuildRuntimeState&>(rc.GetStartupRebuildState());
    state.rebuild_index_alloc_end = writer_db.GetDictPtr()->GetHeaderPtr()->index_block_size + 1;
    state.rebuild_data_alloc_end = writer_db.GetDictPtr()->GetHeaderPtr()->data_block_size + 1;
    state.rebuild_index_source_end = writer_db.GetDictPtr()->GetHeaderPtr()->index_block_size;
    state.rebuild_data_source_end = writer_db.GetDictPtr()->GetHeaderPtr()->data_block_size;
    EXPECT_EQ(rc.StartupEvacuate(), MBError::INVALID_SIZE);
}
