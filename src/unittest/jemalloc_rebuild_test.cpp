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

using namespace mabain;
using namespace mabain_test;

namespace {

constexpr const char* kJemallocRebuildTestPath = "/var/tmp/mabain_test/jemalloc_rebuild";

void RemoveJemallocRebuildTestFiles()
{
    if (system("mkdir -p /var/tmp/mabain_test") != 0) {
    }
    if (system("rm -rf /var/tmp/mabain_test/jemalloc_rebuild_*") != 0) {
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
        char hdr[RollableFile::page_size];
        memset(hdr, 0, sizeof(hdr));
        IndexHeader* ptr = reinterpret_cast<IndexHeader*>(hdr);
        ptr->version[0] = major;
        ptr->version[1] = minor;
        ptr->version[2] = patch;
        ptr->version[3] = 0;
        std::ofstream out(std::string(kJemallocRebuildTestPath) + "_mabain_h",
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
    int options = CONSTS::ACCESS_MODE_WRITER;
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
    int options = CONSTS::ACCESS_MODE_WRITER;
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
    int options = CONSTS::ACCESS_MODE_WRITER;
    DB db(kJemallocRebuildTestPath, options);
    EXPECT_FALSE(db.is_open());
    EXPECT_EQ(db.Status(), MBError::VERSION_MISMATCH);
}
