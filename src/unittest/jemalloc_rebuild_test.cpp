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

#include <gtest/gtest.h>

#include "../db.h"
#include "../dict.h"
#include "../drm_base.h"
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

MBConfig MakeJemallocRebuildConfig(int options, bool keep_db)
{
    MBConfig config = {};
    config.mbdir = kJemallocRebuildTestPath;
    config.options = options;
    config.block_size_index = kJemallocRebuildBlockSize;
    config.block_size_data = kJemallocRebuildBlockSize;
    config.max_num_index_block = kJemallocRebuildMaxBlocks;
    config.max_num_data_block = kJemallocRebuildMaxBlocks;
    config.memcap_index = kJemallocRebuildMemCap;
    config.memcap_data = kJemallocRebuildMemCap;
    config.num_entry_per_bucket = 500;
    config.jemalloc_keep_db = keep_db;
    return config;
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
    EXPECT_TRUE(IsJemallocRebuildTestModeSupported("copy_only"));
    EXPECT_TRUE(IsJemallocRebuildTestModeSupported("cutover_only"));
    EXPECT_TRUE(IsJemallocRebuildTestModeSupported("recover_copy"));
    EXPECT_TRUE(IsJemallocRebuildTestModeSupported("recover_cutover"));
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

    header.ResetRebuildMetadata(REBUILD_STATE_CUTOVER);

    EXPECT_EQ(header.rebuild_state, REBUILD_STATE_CUTOVER);
    EXPECT_EQ(header.rebuild_root_offset, 0u);
    EXPECT_EQ(header.rebuild_index_alloc_start, 0u);
    EXPECT_EQ(header.rebuild_data_alloc_start, 0u);
    EXPECT_EQ(header.rebuild_cutover_index, 0);
    EXPECT_EQ(header.rebuild_index_alloc_end, 0u);
    EXPECT_EQ(header.rebuild_data_alloc_end, 0u);
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
    db.Close();
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
