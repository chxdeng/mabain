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

#include <fstream>
#include <cstdlib>
#include <cstring>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "../db.h"
#include "../dict.h"
#include "../drm_base.h"
#include "../mb_rc.h"
#include "../resource_pool.h"
#include "../test/jemalloc_rebuild_test_modes.h"
#include "../version.h"

using namespace mabain;
using namespace mabain_test;

namespace {

constexpr const char* kJemallocRebuildTestPath = "/var/tmp/mabain_test/jemalloc_rebuild";
constexpr uint32_t kJemallocRebuildBlockSize = 4 * 1024 * 1024;
constexpr int kJemallocRebuildMaxBlocks = 4;
constexpr size_t kJemallocRebuildMemCap = kJemallocRebuildBlockSize * kJemallocRebuildMaxBlocks;

MBConfig MakeSizedJemallocRebuildConfig(int options, bool keep_db,
    uint32_t block_size, int max_blocks)
{
    MBConfig config = {};
    config.mbdir = kJemallocRebuildTestPath;
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

} // namespace
namespace mabain {
class ResourceCollectionTestPeer : public ResourceCollection {
public:
    explicit ResourceCollectionTestPeer(const DB& db)
        : ResourceCollection(db)
    {
    }

    using ResourceCollection::IsReaderEpochQuiesced;
    using ResourceCollection::QueueReusableBlock;
    using ResourceCollection::ReleaseReusableBlocks;
    using ResourceCollection::DrainReusableBlocks;
};

}
namespace {
int FindReaderEpochSlot(const IndexHeader* header, uint32_t connect_id)
{
    if (header == nullptr)
        return -1;
    for (uint32_t i = 0; i < header->reader_epoch_slot_count; i++) {
        if (header->reader_epoch_slot[i].connect_id.load(MEMORY_ORDER_READER) == connect_id)
            return static_cast<int>(i);
    }
    return -1;
}

void ExpectFindValue(DB& db, const std::string& key, const std::string& value)
{
    MBData data;
    ASSERT_EQ(db.Find(key, data), MBError::SUCCESS);
    ASSERT_NE(data.buff, nullptr);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.buff), data.data_len), value);
}

void RemoveJemallocRebuildTestFiles()
{
    if (system("mkdir -p /var/tmp/mabain_test") != 0) {
    }
    if (system("rm -rf /var/tmp/mabain_test/jemalloc_rebuild") != 0) {
    }
}

void CreateJemallocRebuildTestDir()
{
    if (system("mkdir -p /var/tmp/mabain_test/jemalloc_rebuild") != 0) {
    }
}

class JemallocRebuildMetadataTest : public ::testing::Test {
public:
    void SetUp() override
    {
        ResourcePool::getInstance().RemoveAll();
        RemoveJemallocRebuildTestFiles();
    }

    void TearDown() override
    {
        ResourcePool::getInstance().RemoveAll();
        RemoveJemallocRebuildTestFiles();
    }

    void CreateHeaderWithVersion(uint16_t major, uint16_t minor, uint16_t patch) const
    {
        CreateJemallocRebuildTestDir();
        char hdr[RollableFile::page_size];
        memset(hdr, 0, sizeof(hdr));
        IndexHeader* ptr = reinterpret_cast<IndexHeader*>(hdr);
        ptr->version[0] = major;
        ptr->version[1] = minor;
        ptr->version[2] = patch;
        ptr->version[3] = 0;
        std::ofstream out(std::string(kJemallocRebuildTestPath) + "/_mabain_h",
            std::ios::out | std::ios::binary);
        out.write(hdr, sizeof(hdr));
    }
};

} // namespace

TEST(JemallocRebuildHarnessTest, ModeListContainsExpectedPhases)
{
    EXPECT_EQ(kJemallocRebuildTestModes.size(), 9u);
    EXPECT_TRUE(IsJemallocRebuildTestModeSupported("header_metadata"));
    EXPECT_TRUE(IsJemallocRebuildTestModeSupported("arena_cursor"));
    EXPECT_TRUE(IsJemallocRebuildTestModeSupported("startup_gate"));
    EXPECT_TRUE(IsJemallocRebuildTestModeSupported("async_reject"));
    EXPECT_TRUE(IsJemallocRebuildTestModeSupported("shrink_only"));
    EXPECT_TRUE(IsJemallocRebuildTestModeSupported("evacuate_only"));
    EXPECT_TRUE(IsJemallocRebuildTestModeSupported("recover_shrink"));
    EXPECT_TRUE(IsJemallocRebuildTestModeSupported("recover_evacuate"));
    EXPECT_TRUE(IsJemallocRebuildTestModeSupported("full_cycle"));
    EXPECT_FALSE(IsJemallocRebuildTestModeSupported("unknown_mode"));
}

TEST(JemallocRebuildHarnessTest, ModeListHasUniqueNames)
{
    std::set<std::string> unique_modes(kJemallocRebuildTestModes.begin(),
        kJemallocRebuildTestModes.end());
    EXPECT_EQ(unique_modes.size(), kJemallocRebuildTestModes.size());
}

TEST(JemallocRebuildHeaderHelperTest, ResetRebuildMetadataClearsOffsetsAndKeepsRequestedState)
{
    IndexHeader header = {};
    header.rebuild_root_offset = 101;
    header.rebuild_index_alloc_start = 202;
    header.rebuild_data_alloc_start = 303;
    header.rebuild_cutover_index = 11;
    header.rebuild_index_alloc_end = 404;
    header.rebuild_data_alloc_end = 505;
    header.rebuild_index_source_end = 606;
    header.rebuild_data_source_end = 707;
    header.rebuild_index_block_cursor = 808;
    header.rebuild_data_block_cursor = 909;
    header.reusable_index_block_count = 3;
    header.reusable_data_block_count = 4;
    header.reusable_index_block[0].in_use = 1;
    header.reusable_data_block[0].in_use = 1;

    header.ResetRebuildMetadata(REBUILD_STATE_CUTOVER);

    EXPECT_EQ(header.rebuild_state, REBUILD_STATE_CUTOVER);
    EXPECT_EQ(header.rebuild_root_offset, 0u);
    EXPECT_EQ(header.rebuild_index_alloc_start, 0u);
    EXPECT_EQ(header.rebuild_data_alloc_start, 0u);
    EXPECT_EQ(header.rebuild_cutover_index, 0);
    EXPECT_EQ(header.rebuild_index_alloc_end, 0u);
    EXPECT_EQ(header.rebuild_data_alloc_end, 0u);
    EXPECT_EQ(header.rebuild_index_source_end, 0u);
    EXPECT_EQ(header.rebuild_data_source_end, 0u);
    EXPECT_EQ(header.rebuild_index_block_cursor, 0u);
    EXPECT_EQ(header.rebuild_data_block_cursor, 0u);
    EXPECT_EQ(header.reusable_index_block_count, 0u);
    EXPECT_EQ(header.reusable_data_block_count, 0u);
    EXPECT_EQ(header.reusable_index_block[0].in_use, 0u);
    EXPECT_EQ(header.reusable_data_block[0].in_use, 0u);
    EXPECT_TRUE(header.RebuildInProgress());
}

TEST(JemallocRebuildHeaderHelperTest, ClearRebuildMetadataRestoresNormalState)
{
    IndexHeader header = {};
    header.rebuild_state = REBUILD_STATE_POST;
    header.rebuild_root_offset = 901;
    header.rebuild_cutover_index = 33;

    header.ClearRebuildMetadata();

    EXPECT_EQ(header.rebuild_state, REBUILD_STATE_NORMAL);
    EXPECT_EQ(header.rebuild_root_offset, 0u);
    EXPECT_EQ(header.rebuild_cutover_index, 0);
    EXPECT_FALSE(header.RebuildInProgress());
}

TEST_F(JemallocRebuildMetadataTest, NewDbInitializesRebuildMetadataToZero)
{
    CreateJemallocRebuildTestDir();
    int options = CONSTS::WriterOptions();
    DB db(kJemallocRebuildTestPath, options);
    ASSERT_TRUE(db.is_open());
    IndexHeader* header = db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    EXPECT_EQ(header->rebuild_state, REBUILD_STATE_NORMAL);
    EXPECT_EQ(header->rebuild_root_offset, 0u);
    EXPECT_EQ(header->rebuild_index_alloc_start, 0u);
    EXPECT_EQ(header->rebuild_data_alloc_start, 0u);
    EXPECT_EQ(header->rebuild_cutover_index, 0);
    EXPECT_EQ(header->rebuild_index_alloc_end, 0u);
    EXPECT_EQ(header->rebuild_data_alloc_end, 0u);
    EXPECT_EQ(header->rebuild_index_source_end, 0u);
    EXPECT_EQ(header->rebuild_data_source_end, 0u);
    EXPECT_EQ(header->rebuild_index_block_cursor, 0u);
    EXPECT_EQ(header->rebuild_data_block_cursor, 0u);
    EXPECT_EQ(header->reusable_index_block_count, 0u);
    EXPECT_EQ(header->reusable_data_block_count, 0u);
    EXPECT_EQ(header->reader_epoch_tracking_active.load(MEMORY_ORDER_READER), 0u);
    EXPECT_EQ(header->reader_epoch_slot_count, MB_MAX_READER_EPOCH_SLOT);
    EXPECT_EQ(header->reader_epoch.load(MEMORY_ORDER_READER), 1u);
    db.Close();
}

TEST_F(JemallocRebuildMetadataTest, PrintHeaderIncludesRebuildMetadata)
{
    CreateJemallocRebuildTestDir();
    int options = CONSTS::WriterOptions();
    DB db(kJemallocRebuildTestPath, options);
    ASSERT_TRUE(db.is_open());
    IndexHeader* header = db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);

    header->rebuild_state = REBUILD_STATE_COPY;
    header->rebuild_root_offset = 1234;
    header->rebuild_index_alloc_start = 5678;
    header->rebuild_data_alloc_start = 6789;
    header->rebuild_cutover_index = 77;
    header->rebuild_index_alloc_end = 9876;
    header->rebuild_data_alloc_end = 8765;
    header->rebuild_index_source_end = 11111;
    header->rebuild_data_source_end = 22222;
    header->rebuild_index_block_cursor = 33333;
    header->rebuild_data_block_cursor = 44444;
    header->reader_epoch_tracking_active.store(1, MEMORY_ORDER_WRITER);
    header->reader_epoch_slot_count = 7;
    header->reader_epoch.store(99, MEMORY_ORDER_WRITER);
    header->reusable_index_block_count = 2;
    header->reusable_data_block_count = 5;

    std::ostringstream out;
    db.PrintHeader(out);
    const std::string header_text = out.str();
    EXPECT_NE(header_text.find("rebuild state: REBUILD_COPY (2)"), std::string::npos);
    EXPECT_NE(header_text.find("rebuild root offset: 1234"), std::string::npos);
    EXPECT_NE(header_text.find("rebuild index alloc start: 5678"), std::string::npos);
    EXPECT_NE(header_text.find("rebuild data alloc start: 6789"), std::string::npos);
    EXPECT_NE(header_text.find("rebuild cutover index: 77"), std::string::npos);
    EXPECT_NE(header_text.find("rebuild index alloc end: 9876"), std::string::npos);
    EXPECT_NE(header_text.find("rebuild data alloc end: 8765"), std::string::npos);
    EXPECT_NE(header_text.find("rebuild index source end: 11111"), std::string::npos);
    EXPECT_NE(header_text.find("rebuild data source end: 22222"), std::string::npos);
    EXPECT_NE(header_text.find("rebuild index block cursor: 33333"), std::string::npos);
    EXPECT_NE(header_text.find("rebuild data block cursor: 44444"), std::string::npos);
    EXPECT_NE(header_text.find("reader epoch tracking active: 1"), std::string::npos);
    EXPECT_NE(header_text.find("reader epoch slot count: 7"), std::string::npos);
    EXPECT_NE(header_text.find("reader epoch: 99"), std::string::npos);
    EXPECT_NE(header_text.find("reusable index block count: 2"), std::string::npos);
    EXPECT_NE(header_text.find("reusable data block count: 5"), std::string::npos);
    db.Close();
}

TEST_F(JemallocRebuildMetadataTest, ReaderEpochSlotStaysIdleWhenTrackingIsDisabled)
{
    CreateJemallocRebuildTestDir();
    const std::string key("reader-epoch-idle");
    const std::string value("value");

    MBConfig writer_cfg = MakeJemallocRebuildConfig(CONSTS::WriterOptions(), false);
    DB writer_db(writer_cfg);
    ASSERT_TRUE(writer_db.is_open());
    ASSERT_EQ(writer_db.Add(key, value), MBError::SUCCESS);

    MBConfig reader_cfg = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_READER, false);
    reader_cfg.connect_id = 0x3456;
    DB reader_db(reader_cfg);
    ASSERT_TRUE(reader_db.is_open());

    IndexHeader* header = writer_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    ASSERT_EQ(header->reader_epoch_tracking_active.load(MEMORY_ORDER_READER), 0u);

    const int slot = FindReaderEpochSlot(header, reader_cfg.connect_id);
    ASSERT_GE(slot, 0);
    EXPECT_EQ(header->reader_epoch_slot[slot].epoch.load(MEMORY_ORDER_READER), 0u);

    ExpectFindValue(reader_db, key, value);
    EXPECT_EQ(header->reader_epoch_slot[slot].epoch.load(MEMORY_ORDER_READER), 0u);
    reader_db.Close();
    writer_db.Close();
}

TEST_F(JemallocRebuildMetadataTest, ReaderEpochSlotIsClaimedOnOpenAndClearedOnClose)
{
    CreateJemallocRebuildTestDir();

    MBConfig writer_cfg = MakeJemallocRebuildConfig(CONSTS::WriterOptions(), false);
    DB writer_db(writer_cfg);
    ASSERT_TRUE(writer_db.is_open());

    MBConfig reader_cfg = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_READER, false);
    reader_cfg.connect_id = 0x4567;
    DB reader_db(reader_cfg);
    ASSERT_TRUE(reader_db.is_open());

    IndexHeader* header = writer_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);

    const int slot = FindReaderEpochSlot(header, reader_cfg.connect_id);
    ASSERT_GE(slot, 0);
    EXPECT_EQ(header->reader_epoch_slot[slot].connect_id.load(MEMORY_ORDER_READER),
        reader_cfg.connect_id);
    EXPECT_NE(header->reader_epoch_slot[slot].pid.load(MEMORY_ORDER_READER), 0u);
    EXPECT_EQ(header->reader_epoch_slot[slot].epoch.load(MEMORY_ORDER_READER), 0u);

    reader_db.Close();
    EXPECT_EQ(header->reader_epoch_slot[slot].connect_id.load(MEMORY_ORDER_READER), 0u);
    EXPECT_EQ(header->reader_epoch_slot[slot].pid.load(MEMORY_ORDER_READER), 0u);
    EXPECT_EQ(header->reader_epoch_slot[slot].epoch.load(MEMORY_ORDER_READER), 0u);
    writer_db.Close();
}

TEST_F(JemallocRebuildMetadataTest, ReaderEpochTrackingCanBeEnabledForCoveredLookupPaths)
{
    CreateJemallocRebuildTestDir();
    const std::string key("reader-epoch-active");
    const std::string value("value");

    MBConfig writer_cfg = MakeJemallocRebuildConfig(CONSTS::WriterOptions(), false);
    DB writer_db(writer_cfg);
    ASSERT_TRUE(writer_db.is_open());
    ASSERT_EQ(writer_db.Add(key, value), MBError::SUCCESS);

    MBConfig reader_cfg = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_READER, false);
    reader_cfg.connect_id = 0x5678;
    DB reader_db(reader_cfg);
    ASSERT_TRUE(reader_db.is_open());

    IndexHeader* header = writer_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    const int slot = FindReaderEpochSlot(header, reader_cfg.connect_id);
    ASSERT_GE(slot, 0);

    header->reader_epoch.store(17, MEMORY_ORDER_WRITER);
    header->reader_epoch_tracking_active.store(1, MEMORY_ORDER_WRITER);
    EXPECT_EQ(header->reader_epoch_slot[slot].epoch.load(MEMORY_ORDER_READER), 0u);

    ExpectFindValue(reader_db, key, value);

    EXPECT_EQ(header->reader_epoch_slot[slot].epoch.load(MEMORY_ORDER_READER), 0u);
    EXPECT_EQ(header->reader_epoch.load(MEMORY_ORDER_READER), 17u);
    reader_db.Close();
    writer_db.Close();
}

TEST_F(JemallocRebuildMetadataTest, ReaderEpochTrackingAlsoCoversFindLongestPrefix)
{
    CreateJemallocRebuildTestDir();
    const std::string key("reader-epoch-prefix");
    const std::string value("value-prefix");

    MBConfig writer_cfg = MakeJemallocRebuildConfig(CONSTS::WriterOptions(), false);
    DB writer_db(writer_cfg);
    ASSERT_TRUE(writer_db.is_open());
    ASSERT_EQ(writer_db.Add(key, value), MBError::SUCCESS);

    MBConfig reader_cfg = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_READER, false);
    reader_cfg.connect_id = 0x6789;
    DB reader_db(reader_cfg);
    ASSERT_TRUE(reader_db.is_open());

    IndexHeader* header = writer_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    const int slot = FindReaderEpochSlot(header, reader_cfg.connect_id);
    ASSERT_GE(slot, 0);

    header->reader_epoch.store(23, MEMORY_ORDER_WRITER);
    header->reader_epoch_tracking_active.store(1, MEMORY_ORDER_WRITER);
    EXPECT_EQ(header->reader_epoch_slot[slot].epoch.load(MEMORY_ORDER_READER), 0u);

    MBData data;
    ASSERT_EQ(reader_db.FindLongestPrefix(key, data), MBError::SUCCESS);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(data.buff), data.data_len), value);

    EXPECT_EQ(header->reader_epoch_slot[slot].epoch.load(MEMORY_ORDER_READER), 0u);
    EXPECT_EQ(header->reader_epoch.load(MEMORY_ORDER_READER), 23u);
    reader_db.Close();
    writer_db.Close();
}

TEST_F(JemallocRebuildMetadataTest, QuarantinedBlockStaysUnavailableWhileReaderPinnedToRetireEpoch)
{
    CreateJemallocRebuildTestDir();

    MBConfig writer_cfg = MakeJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB writer_db(writer_cfg);
    ASSERT_TRUE(writer_db.is_open());

    IndexHeader* header = writer_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    header->reader_epoch_tracking_active.store(1, MEMORY_ORDER_WRITER);
    header->reader_epoch_slot[0].connect_id.store(0x1111, MEMORY_ORDER_WRITER);
    header->reader_epoch_slot[0].pid.store(1, MEMORY_ORDER_WRITER);
    header->reader_epoch_slot[0].epoch.store(9, MEMORY_ORDER_WRITER);

    ResourceCollectionTestPeer rc(writer_db);
    ASSERT_EQ(rc.QueueReusableBlock(header->reusable_index_block,
        header->reusable_index_block_count, 2, 9), MBError::SUCCESS);
    EXPECT_EQ(header->reusable_index_block_count, 1u);

    ASSERT_EQ(rc.ReleaseReusableBlocks(header->reusable_index_block,
        header->reusable_index_block_count, writer_db.GetDictPtr()->GetMM()), MBError::SUCCESS);
    EXPECT_EQ(header->reusable_index_block_count, 1u);
    EXPECT_EQ(header->reusable_index_block[0].in_use, REUSABLE_BLOCK_STATE_QUARANTINED);
    EXPECT_EQ(writer_db.GetDictPtr()->GetMM()->GetReusableBlockCount(), 0u);
    EXPECT_EQ(header->reader_epoch_tracking_active.load(MEMORY_ORDER_READER), 1u);
    writer_db.Close();
}

TEST_F(JemallocRebuildMetadataTest, QuarantinedBlockBecomesReusableAfterReadersAdvance)
{
    CreateJemallocRebuildTestDir();

    MBConfig writer_cfg = MakeJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB writer_db(writer_cfg);
    ASSERT_TRUE(writer_db.is_open());

    IndexHeader* header = writer_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    header->reader_epoch_tracking_active.store(1, MEMORY_ORDER_WRITER);
    header->reader_epoch_slot[0].connect_id.store(0x2222, MEMORY_ORDER_WRITER);
    header->reader_epoch_slot[0].pid.store(1, MEMORY_ORDER_WRITER);
    header->reader_epoch_slot[0].epoch.store(11, MEMORY_ORDER_WRITER);

    ResourceCollectionTestPeer rc(writer_db);
    ASSERT_EQ(rc.QueueReusableBlock(header->reusable_index_block,
        header->reusable_index_block_count, 2, 9), MBError::SUCCESS);
    ASSERT_EQ(rc.QueueReusableBlock(header->reusable_data_block,
        header->reusable_data_block_count, 3, 9), MBError::SUCCESS);

    header->reader_epoch_slot[0].epoch.store(12, MEMORY_ORDER_WRITER);
    ASSERT_EQ(rc.ReleaseReusableBlocks(header->reusable_index_block,
        header->reusable_index_block_count, writer_db.GetDictPtr()->GetMM()), MBError::SUCCESS);
    ASSERT_EQ(rc.ReleaseReusableBlocks(header->reusable_data_block,
        header->reusable_data_block_count, writer_db.GetDictPtr()), MBError::SUCCESS);
    EXPECT_EQ(header->reusable_index_block_count, 1u);
    EXPECT_EQ(header->reusable_data_block_count, 1u);
    EXPECT_EQ(header->reusable_index_block[0].in_use, REUSABLE_BLOCK_STATE_READY);
    EXPECT_EQ(header->reusable_data_block[0].in_use, REUSABLE_BLOCK_STATE_READY);
    EXPECT_EQ(writer_db.GetDictPtr()->GetMM()->GetReusableBlockCount(), 0u);
    EXPECT_EQ(writer_db.GetDictPtr()->GetReusableBlockCount(), 0u);
    EXPECT_EQ(header->reader_epoch_tracking_active.load(MEMORY_ORDER_READER), 1u);

    ASSERT_EQ(rc.DrainReusableBlocks(header->reusable_index_block,
        header->reusable_index_block_count, writer_db.GetDictPtr()->GetMM()), MBError::SUCCESS);
    ASSERT_EQ(rc.DrainReusableBlocks(header->reusable_data_block,
        header->reusable_data_block_count, writer_db.GetDictPtr()), MBError::SUCCESS);
    EXPECT_EQ(header->reusable_index_block_count, 0u);
    EXPECT_EQ(header->reusable_data_block_count, 0u);
    EXPECT_EQ(writer_db.GetDictPtr()->GetMM()->GetReusableBlockCount(), 1u);
    EXPECT_EQ(writer_db.GetDictPtr()->GetReusableBlockCount(), 1u);
    writer_db.Close();
}

TEST_F(JemallocRebuildMetadataTest, RejectsOlderHeaderVersion)
{
    CreateHeaderWithVersion(1, 6, 2);
    int options = CONSTS::ReaderOptions();
    DB db(kJemallocRebuildTestPath, options);
    EXPECT_FALSE(db.is_open());
    EXPECT_EQ(db.Status(), MBError::VERSION_MISMATCH);
}

TEST_F(JemallocRebuildMetadataTest, PersistedHeaderKeepsRebuildMetadata)
{
    CreateJemallocRebuildTestDir();
    {
        DB writer_db(kJemallocRebuildTestPath, CONSTS::WriterOptions());
        ASSERT_TRUE(writer_db.is_open());
        IndexHeader* header = writer_db.GetDictPtr()->GetHeaderPtr();
        ASSERT_NE(header, nullptr);

        header->rebuild_state = REBUILD_STATE_COPY;
        header->rebuild_root_offset = 4321;
        header->rebuild_index_alloc_start = 1001;
        header->rebuild_data_alloc_start = 1002;
        header->rebuild_cutover_index = 12;
        header->rebuild_index_alloc_end = 2001;
        header->rebuild_data_alloc_end = 2002;
        header->rebuild_index_source_end = 3001;
        header->rebuild_data_source_end = 3002;
        header->rebuild_index_block_cursor = 4001;
        header->rebuild_data_block_cursor = 4002;
        header->reusable_index_block_count = 6;
        header->reusable_data_block_count = 7;
        writer_db.Close();
    }

    char hdr_page[RollableFile::page_size];
    std::ifstream in(std::string(kJemallocRebuildTestPath) + "/_mabain_h",
        std::ios::in | std::ios::binary);
    ASSERT_TRUE(in.is_open());
    in.read(hdr_page, sizeof(hdr_page));
    ASSERT_EQ(in.gcount(), static_cast<std::streamsize>(sizeof(hdr_page)));

    const IndexHeader* persisted = reinterpret_cast<const IndexHeader*>(hdr_page);
    EXPECT_EQ(persisted->rebuild_state, REBUILD_STATE_COPY);
    EXPECT_EQ(persisted->rebuild_root_offset, 4321u);
    EXPECT_EQ(persisted->rebuild_index_alloc_start, 1001u);
    EXPECT_EQ(persisted->rebuild_data_alloc_start, 1002u);
    EXPECT_EQ(persisted->rebuild_cutover_index, 12);
    EXPECT_EQ(persisted->rebuild_index_alloc_end, 2001u);
    EXPECT_EQ(persisted->rebuild_data_alloc_end, 2002u);
    EXPECT_EQ(persisted->rebuild_index_source_end, 3001u);
    EXPECT_EQ(persisted->rebuild_data_source_end, 3002u);
    EXPECT_EQ(persisted->rebuild_index_block_cursor, 4001u);
    EXPECT_EQ(persisted->rebuild_data_block_cursor, 4002u);
    EXPECT_EQ(persisted->reusable_index_block_count, 6u);
    EXPECT_EQ(persisted->reusable_data_block_count, 7u);
    EXPECT_TRUE(persisted->RebuildInProgress());
}

TEST_F(JemallocRebuildMetadataTest, WriterRecreatesDbAfterOlderHeaderVersionMismatch)
{
    CreateHeaderWithVersion(1, 6, 2);
    DB writer_db(kJemallocRebuildTestPath, CONSTS::WriterOptions());
    ASSERT_TRUE(writer_db.is_open());
    EXPECT_EQ(writer_db.Status(), MBError::SUCCESS);

    IndexHeader* header = writer_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    EXPECT_EQ(header->version[0], version[0]);
    EXPECT_EQ(header->version[1], version[1]);
    EXPECT_EQ(header->version[2], version[2]);
    EXPECT_EQ(header->rebuild_state, REBUILD_STATE_NORMAL);
    EXPECT_EQ(header->rebuild_root_offset, 0u);
    EXPECT_EQ(header->rebuild_index_alloc_start, 0u);
    EXPECT_EQ(header->rebuild_data_alloc_start, 0u);
    EXPECT_EQ(header->rebuild_cutover_index, 0);
    EXPECT_EQ(header->rebuild_index_alloc_end, 0u);
    EXPECT_EQ(header->rebuild_data_alloc_end, 0u);
    EXPECT_FALSE(header->RebuildInProgress());
    writer_db.Close();
}

TEST_F(JemallocRebuildMetadataTest, WarmRestartWithKeepDbEntersPrepStateAndPreservesReads)
{
    CreateJemallocRebuildTestDir();
    const std::string key("alpha");
    const std::string value("value-alpha");

    MBConfig initial_cfg = MakeJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB initial_db(initial_cfg);
    ASSERT_TRUE(initial_db.is_open());
    ASSERT_EQ(initial_db.Add(key, value), MBError::SUCCESS);
    ASSERT_EQ(initial_db.Count(), 1);
    initial_db.Close();

    MBConfig rebuild_cfg = MakeJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, true);
    DB rebuild_db(rebuild_cfg);
    ASSERT_TRUE(rebuild_db.is_open());

    IndexHeader* header = rebuild_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    EXPECT_EQ(header->rebuild_state, REBUILD_STATE_PREP);
    EXPECT_TRUE(header->RebuildInProgress());
    EXPECT_EQ(rebuild_db.Count(), 1);
    ExpectFindValue(rebuild_db, key, value);

    MBConfig reader_cfg = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_READER, false);
    DB reader_db(reader_cfg);
    ASSERT_TRUE(reader_db.is_open());
    ExpectFindValue(reader_db, key, value);
    reader_db.Close();
    rebuild_db.Close();
}

TEST_F(JemallocRebuildMetadataTest, StartupShrinkCapturesCurrentJemallocTails)
{
    CreateJemallocRebuildTestDir();
    const std::string key("delta");
    const std::string value("value-delta");

    MBConfig initial_cfg = MakeJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB initial_db(initial_cfg);
    ASSERT_TRUE(initial_db.is_open());
    ASSERT_EQ(initial_db.Add(key, value), MBError::SUCCESS);
    initial_db.Close();

    MBConfig rebuild_cfg = MakeJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, true);
    DB rebuild_db(rebuild_cfg);
    ASSERT_TRUE(rebuild_db.is_open());

    IndexHeader* header = rebuild_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    ASSERT_EQ(header->rebuild_state, REBUILD_STATE_PREP);
    ResourceCollection rc(rebuild_db);
    ASSERT_EQ(rc.StartupShrink(), MBError::SUCCESS);
    EXPECT_EQ(header->rebuild_state, REBUILD_STATE_COPY);
    EXPECT_EQ(header->rebuild_index_alloc_start, header->m_index_offset);
    EXPECT_EQ(header->rebuild_data_alloc_start, header->m_data_offset);
    EXPECT_LE(header->rebuild_index_alloc_end, header->rebuild_index_alloc_start);
    EXPECT_LE(header->rebuild_data_alloc_end, header->rebuild_data_alloc_start);
    EXPECT_GE(header->rebuild_index_source_end, header->m_index_offset);
    EXPECT_GE(header->rebuild_data_source_end, header->m_data_offset);
    EXPECT_GE(header->rebuild_index_source_end, header->rebuild_index_alloc_end);
    EXPECT_GE(header->rebuild_data_source_end, header->rebuild_data_alloc_end);
    rebuild_db.Close();
}

TEST_F(JemallocRebuildMetadataTest, StartupEvacuateSeparatesExactBoundaryFromSourceBlockStart)
{
    CreateJemallocRebuildTestDir();
    const std::string key("epsilon");
    const std::string value("value-epsilon");

    MBConfig initial_cfg = MakeJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB initial_db(initial_cfg);
    ASSERT_TRUE(initial_db.is_open());
    ASSERT_EQ(initial_db.Add(key, value), MBError::SUCCESS);
    initial_db.Close();

    MBConfig rebuild_cfg = MakeJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, true);
    DB rebuild_db(rebuild_cfg);
    ASSERT_TRUE(rebuild_db.is_open());

    ResourceCollection rc(rebuild_db);
    ASSERT_EQ(rc.StartupShrink(), MBError::SUCCESS);

    IndexHeader* header = rebuild_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    const size_t index_boundary = header->rebuild_index_alloc_end;
    const size_t data_boundary = header->rebuild_data_alloc_end;
    ASSERT_GT(header->index_block_size, 0u);
    ASSERT_GT(header->data_block_size, 0u);

    ASSERT_EQ(rc.StartupEvacuate(), MBError::SUCCESS);
    EXPECT_EQ(header->rebuild_state, REBUILD_STATE_CUTOVER);
    EXPECT_EQ(header->rebuild_index_alloc_end, index_boundary);
    EXPECT_EQ(header->rebuild_data_alloc_end, data_boundary);
    EXPECT_GE(rebuild_db.GetDictPtr()->GetMM()->GetJemallocAllocSize(), index_boundary);
    EXPECT_GE(rebuild_db.GetDictPtr()->GetJemallocAllocSize(), data_boundary);
    EXPECT_EQ(header->rebuild_index_alloc_start % header->index_block_size, 0u);
    EXPECT_EQ(header->rebuild_data_alloc_start % header->data_block_size, 0u);
    EXPECT_GE(header->rebuild_index_alloc_start, header->rebuild_index_alloc_end);
    EXPECT_GE(header->rebuild_data_alloc_start, header->rebuild_data_alloc_end);
    EXPECT_GE(header->rebuild_index_block_cursor, header->rebuild_index_alloc_start);
    EXPECT_LE(header->rebuild_index_block_cursor, header->rebuild_index_source_end);
    EXPECT_GE(header->rebuild_data_block_cursor, header->rebuild_data_alloc_start);
    EXPECT_LE(header->rebuild_data_block_cursor, header->rebuild_data_source_end);
    EXPECT_LE(header->reusable_index_block_count, 1u);
    EXPECT_LE(header->reusable_data_block_count, 1u);
    if (header->reusable_index_block_count == 1u) {
        EXPECT_EQ(header->reusable_index_block[0].in_use, REUSABLE_BLOCK_STATE_READY);
    }
    if (header->reusable_data_block_count == 1u) {
        EXPECT_EQ(header->reusable_data_block[0].in_use, REUSABLE_BLOCK_STATE_READY);
    }
    EXPECT_EQ(header->reader_epoch_tracking_active.load(MEMORY_ORDER_READER),
        (header->rebuild_index_block_cursor < header->rebuild_index_source_end
            || header->rebuild_data_block_cursor < header->rebuild_data_source_end) ? 1u : 0u);

    if (index_boundary % header->index_block_size == 0)
        EXPECT_EQ(header->rebuild_index_alloc_start, index_boundary);
    else
        EXPECT_GT(header->rebuild_index_alloc_start, index_boundary);

    if (data_boundary % header->data_block_size == 0)
        EXPECT_EQ(header->rebuild_data_alloc_start, data_boundary);
    else
        EXPECT_GT(header->rebuild_data_alloc_start, data_boundary);
    rebuild_db.Close();
}

TEST_F(JemallocRebuildMetadataTest, StartupEvacuateResumesFromCutoverState)
{
    CreateJemallocRebuildTestDir();
    const std::string key("zeta");
    const std::string value("value-zeta");

    MBConfig initial_cfg = MakeJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB initial_db(initial_cfg);
    ASSERT_TRUE(initial_db.is_open());
    ASSERT_EQ(initial_db.Add(key, value), MBError::SUCCESS);
    initial_db.Close();

    MBConfig rebuild_cfg = MakeJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, true);
    DB rebuild_db(rebuild_cfg);
    ASSERT_TRUE(rebuild_db.is_open());

    ResourceCollection rc(rebuild_db);
    ASSERT_EQ(rc.StartupShrink(), MBError::SUCCESS);
    ASSERT_EQ(rc.StartupEvacuate(), MBError::SUCCESS);

    IndexHeader* header = rebuild_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    const size_t index_source_start = header->rebuild_index_alloc_start;
    const size_t data_source_start = header->rebuild_data_alloc_start;
    const size_t index_boundary = header->rebuild_index_alloc_end;
    const size_t data_boundary = header->rebuild_data_alloc_end;
    const size_t index_source_end = header->rebuild_index_source_end;
    const size_t data_source_end = header->rebuild_data_source_end;
    const size_t index_cursor_1 = header->rebuild_index_block_cursor;
    const size_t data_cursor_1 = header->rebuild_data_block_cursor;

    ASSERT_EQ(rc.StartupEvacuate(), MBError::SUCCESS);
    EXPECT_EQ(header->rebuild_state, REBUILD_STATE_CUTOVER);
    EXPECT_EQ(header->rebuild_index_alloc_start, index_source_start);
    EXPECT_EQ(header->rebuild_data_alloc_start, data_source_start);
    EXPECT_EQ(header->rebuild_index_alloc_end, index_boundary);
    EXPECT_EQ(header->rebuild_data_alloc_end, data_boundary);
    EXPECT_EQ(header->rebuild_index_source_end, index_source_end);
    EXPECT_EQ(header->rebuild_data_source_end, data_source_end);
    EXPECT_GE(header->rebuild_index_block_cursor, index_cursor_1);
    EXPECT_GE(header->rebuild_data_block_cursor, data_cursor_1);
    EXPECT_LE(header->rebuild_data_block_cursor, header->rebuild_data_source_end);
    EXPECT_LE(header->rebuild_index_block_cursor, header->rebuild_index_source_end);
    EXPECT_GE(rebuild_db.GetDictPtr()->GetMM()->GetJemallocAllocSize(), index_boundary);
    EXPECT_GE(rebuild_db.GetDictPtr()->GetJemallocAllocSize(), data_boundary);
    ExpectFindValue(rebuild_db, key, value);
    rebuild_db.Close();
}

TEST_F(JemallocRebuildMetadataTest, StartupEvacuateReleasesOneFullIndexSourceBlockWhenReadersAreIdle)
{
    CreateJemallocRebuildTestDir();
    const uint32_t small_block_size = 4 * 1024 * 1024;
    const int small_max_blocks = 32;
    const std::string value(256, 'v');
    std::vector<std::string> keys;

    MBConfig initial_cfg = MakeSizedJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false,
        small_block_size, small_max_blocks);
    DB initial_db(initial_cfg);
    ASSERT_TRUE(initial_db.is_open());
    for (int i = 0;
         i < 200000 && initial_db.GetDictPtr()->GetMM()->GetExistingBlockEnd() < 2 * small_block_size;
         i++) {
        keys.push_back("evac-" + std::to_string(i) + std::string(24, 'k'));
        ASSERT_EQ(initial_db.Add(keys.back(), value), MBError::SUCCESS);
    }
    ASSERT_GE(initial_db.GetDictPtr()->GetMM()->GetExistingBlockEnd(), 2 * small_block_size);
    ASSERT_GT(keys.size(), 300u);

    for (size_t i = 0; i + 300 < keys.size(); i++)
        ASSERT_EQ(initial_db.Remove(keys[i]), MBError::SUCCESS);
    initial_db.Close();

    MBConfig rebuild_cfg = MakeSizedJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, true,
        small_block_size, small_max_blocks);
    DB rebuild_db(rebuild_cfg);
    ASSERT_TRUE(rebuild_db.is_open());

    ResourceCollection rc(rebuild_db);
    ASSERT_EQ(rc.StartupShrink(), MBError::SUCCESS);

    IndexHeader* header = rebuild_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    const size_t first_source_block =
        ((header->rebuild_index_alloc_end + header->index_block_size - 1) / header->index_block_size)
        * header->index_block_size;
    ASSERT_GE(header->rebuild_index_source_end,
        first_source_block + header->index_block_size);

    ASSERT_EQ(rc.StartupEvacuate(), MBError::SUCCESS);
    EXPECT_EQ(header->rebuild_state, REBUILD_STATE_CUTOVER);
    EXPECT_EQ(header->rebuild_index_block_cursor, first_source_block + header->index_block_size);
    EXPECT_EQ(header->reusable_index_block_count, 1u);
    EXPECT_EQ(header->reusable_index_block[0].in_use, REUSABLE_BLOCK_STATE_READY);
    EXPECT_EQ(rebuild_db.GetDictPtr()->GetMM()->GetReusableBlockCount(), 0u);

    ASSERT_EQ(rc.StartupEvacuate(), MBError::SUCCESS);
    EXPECT_GE(rebuild_db.GetDictPtr()->GetMM()->GetReusableBlockCount(), 1u);
    ExpectFindValue(rebuild_db, keys.back(), value);
    rebuild_db.Close();
}

TEST_F(JemallocRebuildMetadataTest, StartupEvacuateRejectsEmptySourceWindowGracefully)
{
    CreateJemallocRebuildTestDir();
    MBConfig rebuild_cfg = MakeJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, true);
    DB rebuild_db(rebuild_cfg);
    ASSERT_TRUE(rebuild_db.is_open());

    IndexHeader* header = rebuild_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    header->ResetRebuildMetadata(REBUILD_STATE_COPY);
    header->rebuild_index_alloc_end = header->index_block_size;
    header->rebuild_data_alloc_end = header->data_block_size;
    header->rebuild_index_source_end = header->index_block_size;
    header->rebuild_data_source_end = header->data_block_size;

    ResourceCollection rc(rebuild_db);
    EXPECT_EQ(rc.StartupEvacuate(), MBError::SUCCESS);
    EXPECT_EQ(header->reader_epoch_tracking_active.load(MEMORY_ORDER_READER), 0u);
    EXPECT_EQ(header->rebuild_index_block_cursor, header->rebuild_index_alloc_start);

    header->ResetRebuildMetadata(REBUILD_STATE_COPY);
    header->rebuild_index_alloc_end = header->index_block_size + 1;
    header->rebuild_data_alloc_end = header->data_block_size + 1;
    header->rebuild_index_source_end = header->index_block_size;
    header->rebuild_data_source_end = header->data_block_size + 1;

    EXPECT_EQ(rc.StartupEvacuate(), MBError::INVALID_SIZE);
    EXPECT_EQ(header->rebuild_state, REBUILD_STATE_COPY);
    rebuild_db.Close();
}

TEST_F(JemallocRebuildMetadataTest, StartupShrinkRejectsNonJemallocWriterGracefully)
{
    CreateJemallocRebuildTestDir();
    DB db(kJemallocRebuildTestPath, CONSTS::WriterOptions());
    ASSERT_TRUE(db.is_open());

    IndexHeader* header = db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    header->ResetRebuildMetadata(REBUILD_STATE_PREP);

    ResourceCollection rc(db);
    EXPECT_EQ(rc.StartupShrink(), MBError::NOT_ALLOWED);
    EXPECT_EQ(header->rebuild_state, REBUILD_STATE_PREP);
    EXPECT_EQ(header->rebuild_index_alloc_start, 0u);
    EXPECT_EQ(header->rebuild_data_alloc_start, 0u);
    EXPECT_EQ(header->rebuild_index_alloc_end, 0u);
    EXPECT_EQ(header->rebuild_data_alloc_end, 0u);

    MBData data;
    EXPECT_EQ(db.Find("missing", data), MBError::NOT_EXIST);
    db.Close();
}

TEST_F(JemallocRebuildMetadataTest, WarmRestartWithAsyncWriterIsRejectedAndKeepsExistingData)
{
    CreateJemallocRebuildTestDir();
    const std::string key("beta");
    const std::string value("value-beta");

    MBConfig initial_cfg = MakeJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB initial_db(initial_cfg);
    ASSERT_TRUE(initial_db.is_open());
    ASSERT_EQ(initial_db.Add(key, value), MBError::SUCCESS);
    initial_db.Close();

    {
        MBConfig reject_cfg = MakeJemallocRebuildConfig(
            CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC | CONSTS::ASYNC_WRITER_MODE, true);
        DB rejected_db(reject_cfg);
        EXPECT_FALSE(rejected_db.is_open());
        EXPECT_EQ(rejected_db.Status(), MBError::NOT_ALLOWED);
    }

    ResourcePool::getInstance().RemoveAll();

    MBConfig reader_cfg = MakeJemallocRebuildConfig(CONSTS::ACCESS_MODE_READER, false);
    DB reader_db(reader_cfg);
    ASSERT_TRUE(reader_db.is_open());
    ExpectFindValue(reader_db, key, value);
    reader_db.Close();

    char hdr_page[RollableFile::page_size];
    std::ifstream in(std::string(kJemallocRebuildTestPath) + "/_mabain_h",
        std::ios::in | std::ios::binary);
    ASSERT_TRUE(in.is_open());
    in.read(hdr_page, sizeof(hdr_page));
    ASSERT_EQ(in.gcount(), static_cast<std::streamsize>(sizeof(hdr_page)));

    const IndexHeader* header = reinterpret_cast<const IndexHeader*>(hdr_page);
    ASSERT_NE(header, nullptr);
    EXPECT_EQ(header->rebuild_state, REBUILD_STATE_NORMAL);
    EXPECT_FALSE(header->RebuildInProgress());
}

TEST_F(JemallocRebuildMetadataTest, WarmRestartWithoutKeepDbResetsExistingJemallocData)
{
    CreateJemallocRebuildTestDir();
    const std::string key("gamma");
    const std::string value("value-gamma");

    MBConfig initial_cfg = MakeJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB initial_db(initial_cfg);
    ASSERT_TRUE(initial_db.is_open());
    ASSERT_EQ(initial_db.Add(key, value), MBError::SUCCESS);
    ASSERT_EQ(initial_db.Count(), 1);
    initial_db.Close();

    MBConfig reopen_cfg = MakeJemallocRebuildConfig(
        CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC, false);
    DB reopen_db(reopen_cfg);
    ASSERT_TRUE(reopen_db.is_open());

    IndexHeader* header = reopen_db.GetDictPtr()->GetHeaderPtr();
    ASSERT_NE(header, nullptr);
    EXPECT_EQ(header->rebuild_state, REBUILD_STATE_NORMAL);
    EXPECT_FALSE(header->RebuildInProgress());
    EXPECT_EQ(reopen_db.Count(), 0);

    MBData data;
    EXPECT_EQ(reopen_db.Find(key, data), MBError::NOT_EXIST);
    reopen_db.Close();
}
