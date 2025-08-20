/**
 * Copyright (C) 2018 Cisco Inc.
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

#include <string>

#include <gtest/gtest.h>

#include "../dict.h"
#include "../detail/search_engine.h"
#include "../drm_base.h"
#include "../integer_4b_5b.h"
#include "../resource_pool.h"

using namespace mabain;

namespace {

#define DICT_TEST_DIR "/var/tmp/mabain_test/"
#define ONE_MEGA 1024 * 1024
#define FAKE_KEY "test-key-129ksjkjdjdkfjdkfjkdjfkdfjkdjkkdsalslsdlkflsdfsd"
#define FAKE_DATA "This is a test; fake data; sdkll vlksaflksafdlfsadflkdkvkvkv  ldlsldklkdsk4930 90234924894388438348348348878&^&^YYYYYY"

class DictTest : public ::testing::Test {
public:
    DictTest()
    {
        dict = NULL;
        header = NULL;
    }
    virtual ~DictTest()
    {
        DestroyDict();
    }

    virtual void SetUp()
    {
        std::string cmd = std::string("mkdir -p ") + DICT_TEST_DIR;
        if (system(cmd.c_str()) != 0) {
        }
    }
    virtual void TearDown()
    {
        ResourcePool::getInstance().RemoveAll();
        std::string cmd = std::string("rm -rf ") + DICT_TEST_DIR + "/_*";
        if (system(cmd.c_str()) != 0) {
        }
    }

    void InitDict(bool init_header, int opts, int blk_size, int bucket_sz)
    {
        dict = new Dict(std::string(DICT_TEST_DIR), init_header, 0,
            opts, 256LL * ONE_MEGA, 256LL * ONE_MEGA,
            32 * ONE_MEGA, blk_size, 100, 150, bucket_sz, 0, NULL);
        if (init_header) {
            EXPECT_EQ(dict->Init(0), MBError::SUCCESS);
        }
        if (init_header) {
            EXPECT_EQ(dict->Status(), MBError::SUCCESS);
        }
        header = dict->GetHeaderPtr();
        EXPECT_EQ(header != NULL, true);
    }

    int AddKV(int key_len, int data_len, bool overwrite)
    {
        MBData mbd;
        mbd.data_len = data_len;
        mbd.buff = (uint8_t*)FAKE_DATA;
        int rval = dict->Add((const uint8_t*)FAKE_KEY, key_len, mbd, overwrite);
        mbd.buff = NULL;
        return rval;
    }

    int GetNodeOffset(const uint8_t* node_key, int key_len, size_t& node_offset)
    {
        if (dict == NULL)
            return MBError::INVALID_ARG;
        MBData mbd;
        node_offset = 0;
        mbd.options = CONSTS::OPTION_FIND_AND_STORE_PARENT;
        mabain::detail::SearchEngine engine(*dict);
        int rval = engine.find(node_key, key_len, mbd);
        if (rval == MBError::IN_DICT) {
            node_offset = Get6BInteger(mbd.edge_ptrs.offset_ptr);
        }
        return rval;
    }

    void DestroyDict()
    {
        if (dict != NULL) {
            dict->Destroy();
            delete dict;
            dict = NULL;
        }
    }

protected:
    Dict* dict;
    IndexHeader* header;
};

TEST_F(DictTest, Constructor_test)
{
    int opts = CONSTS::ACCESS_MODE_WRITER | CONSTS::USE_SLIDING_WINDOW;
    InitDict(true, opts, 32 * ONE_MEGA, 100);
    DestroyDict();

    int rval = MBError::SUCCESS;
    try {
        InitDict(false, CONSTS::ACCESS_MODE_READER, 16 * ONE_MEGA, 100);
    } catch (int err) {
        rval = err;
    }

    EXPECT_EQ(rval, MBError::INVALID_SIZE);

    rval = MBError::SUCCESS;
    try {
        InitDict(false, CONSTS::ACCESS_MODE_WRITER, 32 * ONE_MEGA, 111);
    } catch (int err) {
        rval = err;
    }

    InitDict(false, CONSTS::ACCESS_MODE_READER, 32 * ONE_MEGA, 100);
}

TEST_F(DictTest, PrintHeader_test)
{
    InitDict(true, CONSTS::ACCESS_MODE_WRITER, 32 * ONE_MEGA, 128);
    dict->PrintHeader(std::cout);
}

TEST_F(DictTest, Add_test)
{
    InitDict(true, CONSTS::ACCESS_MODE_WRITER, 4 * ONE_MEGA, 10);

    uint8_t key[256];
    int key_len;
    MBData mbd;
    int rval;
    uint8_t* shm_ptr;

    key_len = 10;
    memcpy(key, "test-key-1", key_len);
    mbd.data_len = 100;
    mbd.Resize(mbd.data_len);
    memcpy(mbd.buff, FAKE_DATA, mbd.data_len);
    rval = dict->Add(key, key_len, mbd, false);
    EXPECT_EQ(rval, MBError::SUCCESS);
    rval = dict->Add(key, key_len, mbd, false);
    EXPECT_EQ(rval, MBError::IN_DICT);
    EXPECT_EQ(dict->Count(), 1);
    shm_ptr = dict->GetShmPtr(header->m_data_offset - mbd.data_len, mbd.data_len);
    EXPECT_EQ(shm_ptr != NULL, true);
    EXPECT_EQ(memcmp(shm_ptr, FAKE_DATA, mbd.data_len), 0);

    key_len = 10;
    memcpy(key, "test-key-2", key_len);
    mbd.data_len = 101;
    mbd.Resize(mbd.data_len);
    memcpy(mbd.buff, FAKE_DATA, mbd.data_len);
    rval = dict->Add(key, key_len, mbd, false);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(dict->Count(), 2);
    shm_ptr = dict->GetShmPtr(header->m_data_offset - mbd.data_len, mbd.data_len);
    EXPECT_EQ(shm_ptr != NULL, true);
    EXPECT_EQ(memcmp(shm_ptr, FAKE_DATA, mbd.data_len), 0);

    key_len = 10;
    memcpy(key, "test-key-3", key_len);
    mbd.data_len = 105;
    mbd.Resize(mbd.data_len);
    memcpy(mbd.buff, FAKE_DATA, mbd.data_len);
    rval = dict->Add(key, key_len, mbd, false);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(dict->Count(), 3);
    shm_ptr = dict->GetShmPtr(header->m_data_offset - mbd.data_len, mbd.data_len);
    EXPECT_EQ(shm_ptr != NULL, true);
    EXPECT_EQ(memcmp(shm_ptr, FAKE_DATA, mbd.data_len), 0);

    key_len = 10;
    memcpy(key, "test-key-2", key_len);
    mbd.data_len = 108;
    mbd.Resize(mbd.data_len);
    memcpy(mbd.buff, FAKE_DATA, mbd.data_len);
    rval = dict->Add(key, key_len, mbd, true);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(dict->Count(), 3);
    shm_ptr = dict->GetShmPtr(header->m_data_offset - mbd.data_len, mbd.data_len);
    EXPECT_EQ(shm_ptr != NULL, true);
    EXPECT_EQ(memcmp(shm_ptr, FAKE_DATA, mbd.data_len), 0);
}

TEST_F(DictTest, Find_test)
{
    InitDict(true, CONSTS::ACCESS_MODE_WRITER, 4 * ONE_MEGA, 10);
    int key_len;
    int data_len;
    MBData mbd;
    int rval;

    key_len = 10;
    {
        mabain::detail::SearchEngine engine(*dict);
        rval = engine.find((const uint8_t*)FAKE_KEY, key_len, mbd);
    }
    EXPECT_EQ(rval, MBError::NOT_EXIST);

    data_len = 32;
    rval = AddKV(key_len, data_len, false);
    EXPECT_EQ(rval, MBError::SUCCESS);
    dict->Flush();
    {
        mabain::detail::SearchEngine engine(*dict);
        rval = engine.find((const uint8_t*)FAKE_KEY, key_len, mbd);
    }
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(mbd.data_len, 32);
    EXPECT_EQ(memcmp(mbd.buff, FAKE_DATA, mbd.data_len), 0);
}

TEST_F(DictTest, FindPrefix_test)
{
    InitDict(true, CONSTS::ACCESS_MODE_WRITER, 4 * ONE_MEGA, 10);
    int key_len;
    int data_len;
    MBData mbd;
    int rval;

    key_len = 10;
    data_len = 32;
    rval = AddKV(key_len, data_len, false);
    EXPECT_EQ(rval, MBError::SUCCESS);
    key_len = 15;
    {
        mabain::detail::SearchEngine engine(*dict);
        rval = engine.findPrefix((const uint8_t*)FAKE_KEY, key_len, mbd);
    }
    EXPECT_EQ(MBError::SUCCESS, rval);
    EXPECT_EQ(mbd.data_len, 32);
    EXPECT_EQ(memcmp(mbd.buff, FAKE_DATA, mbd.data_len), 0);
}

TEST_F(DictTest, Remove_test)
{
    InitDict(true, CONSTS::ACCESS_MODE_WRITER, 4 * ONE_MEGA, 10);
    int key_len;
    int data_len;
    int rval;

    key_len = 10;
    rval = dict->Remove((const uint8_t*)FAKE_KEY, key_len);
    EXPECT_EQ(rval, MBError::NOT_EXIST);

    key_len = 13;
    data_len = 28;
    rval = AddKV(key_len, data_len, true);
    EXPECT_EQ(rval, MBError::SUCCESS);
    rval = dict->Remove((const uint8_t*)FAKE_KEY, key_len);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(dict->Count(), 0);
}

TEST_F(DictTest, RemoveAll_test)
{
    InitDict(true, CONSTS::ACCESS_MODE_WRITER, 8 * ONE_MEGA, 12);
    int rval;

    rval = AddKV(10, 50, true);
    EXPECT_EQ(rval, MBError::SUCCESS);
    rval = AddKV(11, 52, true);
    EXPECT_EQ(rval, MBError::SUCCESS);
    rval = AddKV(12, 54, true);
    EXPECT_EQ(rval, MBError::SUCCESS);
    rval = AddKV(19, 64, true);
    EXPECT_EQ(rval, MBError::SUCCESS);
    dict->PrintStats(std::cout);

    rval = dict->RemoveAll();
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(dict->Count(), 0);
    EXPECT_EQ(header->m_data_offset, dict->GetStartDataOffset());
    EXPECT_EQ(header->pending_data_buff_size, 0);
    EXPECT_EQ(header->eviction_bucket_index, 0);
    EXPECT_EQ(header->num_update, 0);
}

TEST_F(DictTest, ReserveData_test)
{
    InitDict(true, CONSTS::ACCESS_MODE_WRITER, 8 * ONE_MEGA, 12);
    int data_size;
    size_t offset;
    uint8_t* shm_ptr;

    data_size = 1;
    dict->ReserveData((const uint8_t*)FAKE_DATA, data_size, offset);
    offset += DATA_HDR_BYTE;
    shm_ptr = dict->GetShmPtr(offset, data_size);
    EXPECT_EQ(memcmp(shm_ptr, FAKE_DATA, data_size), 0);

    data_size = 19;
    dict->ReserveData((const uint8_t*)FAKE_DATA, data_size, offset);
    offset += DATA_HDR_BYTE;
    shm_ptr = dict->GetShmPtr(offset, data_size);
    EXPECT_EQ(memcmp(shm_ptr, FAKE_DATA, data_size), 0);
}

TEST_F(DictTest, WriteData_test)
{
    InitDict(true, CONSTS::ACCESS_MODE_WRITER, 8 * ONE_MEGA, 12);
    int data_size;
    size_t offset;
    uint8_t* shm_ptr;

    data_size = 14;
    offset = 111;
    header->m_data_offset = offset + data_size;
    dict->Reserve(offset, data_size, shm_ptr);
    dict->WriteData((const uint8_t*)FAKE_DATA, data_size, offset);
    shm_ptr = dict->GetShmPtr(offset, data_size);
    EXPECT_EQ(memcmp(shm_ptr, FAKE_DATA, data_size), 0);

    data_size = 33;
    offset = 1234;
    header->m_data_offset = offset + data_size;
    dict->WriteData((const uint8_t*)FAKE_DATA, data_size, offset);
    shm_ptr = dict->GetShmPtr(offset, data_size);
    EXPECT_EQ(memcmp(shm_ptr, FAKE_DATA, data_size), 0);
}

TEST_F(DictTest, PrintStats_test)
{
    InitDict(true, CONSTS::ACCESS_MODE_WRITER, 8 * ONE_MEGA, 12);
    dict->PrintStats(std::cout);
}

TEST_F(DictTest, ReadRootNode_test)
{
    InitDict(true, CONSTS::ACCESS_MODE_WRITER, 8 * ONE_MEGA, 12);
    AddKV(10, 15, true);
    AddKV(12, 50, true);
    AddKV(15, 34, true);
    AddKV(8, 22, true);

    EdgePtrs edge_ptrs;
    uint8_t buff[256];
    MBData mbd;
    int match;
    int rval;

    rval = dict->ReadRootNode(buff, edge_ptrs, match, mbd);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(edge_ptrs.offset, 264u);
    // Node header flag may have FLAG_NODE_SORTED set in new implementation
    EXPECT_EQ((int)(buff[0] & ~FLAG_NODE_SORTED), 0);
    EXPECT_EQ((int)buff[1], 255);
}

TEST_F(DictTest, ReadNode_test)
{
    InitDict(true, CONSTS::ACCESS_MODE_WRITER, 8 * ONE_MEGA, 12);
    AddKV(8, 22, true);
    AddKV(10, 15, true);
    AddKV(12, 50, true);
    AddKV(15, 34, true);

    int rval;
    size_t offset = 0;
    EdgePtrs edge_ptrs;
    uint8_t buff[256];
    MBData mbd;
    int match;

    rval = GetNodeOffset((const uint8_t*)FAKE_KEY, 8, offset);
    EXPECT_EQ(rval, MBError::IN_DICT);
    rval = dict->ReadNode(offset, buff, edge_ptrs, match, mbd, true);
    EXPECT_EQ(rval, MBError::SUCCESS);
    // Expect match flag; sorted bit may also be set
    EXPECT_EQ(((int)buff[0] & FLAG_NODE_MATCH), FLAG_NODE_MATCH);
    EXPECT_EQ((int)buff[1], 0);
    EXPECT_EQ(match, 2);
}

TEST_F(DictTest, ReadNextEdge_test)
{
    InitDict(true, CONSTS::ACCESS_MODE_WRITER, 8 * ONE_MEGA, 12);
    AddKV(10, 15, true);
    AddKV(12, 50, true);
    AddKV(15, 34, true);
    AddKV(8, 22, true);

    EdgePtrs edge_ptrs;
    uint8_t buff[256];
    MBData mbd;
    std::string match_str;
    size_t offset;
    int match;
    int rval;

    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    memset(buff, 0, 256);

    edge_ptrs.curr_nt = 15;
    buff[0] = 14;
    rval = dict->ReadNextEdge(buff, edge_ptrs, match, mbd, match_str, offset, true);
    EXPECT_EQ(rval, MBError::OUT_OF_BOUND);

    rval = dict->ReadRootNode(buff, edge_ptrs, match, mbd);
    EXPECT_EQ(rval, MBError::SUCCESS);
    rval = dict->ReadNextEdge(buff, edge_ptrs, match, mbd, match_str, offset, true);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(match, 0);
    EXPECT_EQ(offset, 0u);
}

TEST_F(DictTest, ReadNodeHeader_test)
{
    InitDict(true, CONSTS::ACCESS_MODE_WRITER, 4 * ONE_MEGA, 28);
    AddKV(10, 15, true);
    AddKV(12, 50, true);
    AddKV(15, 34, true);
    AddKV(8, 22, true);

    int rval;
    size_t offset = 0, data_offset, data_link_offset;
    int node_size;
    int match;
    rval = GetNodeOffset((const uint8_t*)FAKE_KEY, 8, offset);
    EXPECT_EQ(rval, MBError::IN_DICT);
    dict->ReadNodeHeader(offset, node_size, match, data_offset, data_link_offset);
    EXPECT_EQ(node_size, 22);
    EXPECT_EQ(match, 2);
    EXPECT_EQ(data_offset, 143u);
    EXPECT_EQ(data_link_offset, 3647u);
}

}
