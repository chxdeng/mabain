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

#include "../dict_mem.h"
#include "../drm_base.h"
#include "../integer_4b_5b.h"
#include "../resource_pool.h"

using namespace mabain;

namespace {

#define DICT_MEM_TEST_DIR "/var/tmp/mabain_test/"

class DictMemTest : public ::testing::Test
{
public:
    DictMemTest() {
        dmm = NULL;
        header = NULL;
        memset(&lfree, 0, sizeof(lfree));
    }
    virtual ~DictMemTest() {
        DestroyDMM();
    }

    virtual void SetUp() {
        std::string cmd = std::string("mkdir -p ") + DICT_MEM_TEST_DIR;
        if(system(cmd.c_str()) != 0) {
        }
    }
    virtual void TearDown() {
        std::string cmd = std::string("rm -rf ") + DICT_MEM_TEST_DIR + "/_*";
        if(system(cmd.c_str()) != 0) {
        }
    }

    void Init() {
        dmm = new DictMem(std::string(DICT_MEM_TEST_DIR), true, 8*1024*1024,
                      CONSTS::ACCESS_MODE_WRITER | CONSTS::USE_SLIDING_WINDOW,
                      8*1024*1024, 1, 0);
        dmm->InitRootNode();
        EXPECT_EQ(dmm->IsValid(), true);
        header = dmm->GetHeaderPtr();
        EXPECT_EQ(header != NULL, true);
        lfree.LockFreeInit(&header->lock_free, header, CONSTS::ACCESS_MODE_WRITER);
        dmm->InitLockFreePtr(&lfree);
    }

    void DestroyDMM() {
        if(dmm != NULL) {
            dmm->Destroy();
            delete dmm;
            dmm = NULL;
        }
    }

protected:
    DictMem *dmm;
    IndexHeader *header;
    LockFree lfree;
};

TEST_F(DictMemTest, Constructor_test)
{
    dmm = new DictMem(std::string(DICT_MEM_TEST_DIR), true, 8*1024*1024,
                      CONSTS::ACCESS_MODE_WRITER, 8*1024*1024, 1, 0);
    dmm->PrintStats(std::cout);
    EXPECT_EQ(dmm->IsValid(), false);
    dmm->InitRootNode();
    EXPECT_EQ(dmm->IsValid(), true);
    dmm->PrintStats(std::cout);
    DestroyDMM();

    dmm = new DictMem(std::string(DICT_MEM_TEST_DIR), false, 8*1024*1024,
                      CONSTS::ACCESS_MODE_WRITER, 8*1024*1024, 1, 0);
    EXPECT_EQ(dmm->IsValid(), true);
    DestroyDMM();

    dmm = new DictMem(std::string(DICT_MEM_TEST_DIR), false, 8*1024*1024,
                      CONSTS::ACCESS_MODE_READER, 8*1024*1024, 1, 0);
    EXPECT_EQ(dmm->IsValid(), true);

    DestroyDMM();

    int error = 0;
    try {
        dmm = new DictMem(std::string(DICT_MEM_TEST_DIR), false, 8*1024*1024,
                          CONSTS::ACCESS_MODE_WRITER, 12*1024*1024, 1, 0);
    } catch (int err) {
        error = err;
    }
    EXPECT_EQ(error, MBError::INVALID_SIZE);
    EXPECT_EQ(dmm==NULL, true);
}

TEST_F(DictMemTest, AddRootEdge_test)
{
    Init();

    EdgePtrs edge_ptrs;
    size_t offset;
    uint8_t *shm_ptr;

    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'm', edge_ptrs), MBError::SUCCESS);
    dmm->AddRootEdge(edge_ptrs, (const uint8_t *) "mabain-test", 11, 1234);
    EXPECT_EQ(edge_ptrs.flag_ptr[0], EDGE_FLAG_DATA_OFF);
    offset = Get5BInteger(edge_ptrs.ptr);
    shm_ptr = dmm->GetShmPtr(offset, 10);
    EXPECT_EQ(shm_ptr != NULL, true);
    if(memcmp(shm_ptr, "abain-test", 10)) {
        EXPECT_EQ(1, 2);
    }
    offset = Get6BInteger(edge_ptrs.offset_ptr);
    EXPECT_EQ(offset, 1234u);
    shm_ptr = dmm->GetShmPtr(header->excep_lf_offset, EDGE_SIZE);
    EXPECT_EQ(shm_ptr != NULL, true);
    if(memcmp(shm_ptr, edge_ptrs.ptr, EDGE_SIZE)) {
        EXPECT_EQ(1, 2);
    }
    EXPECT_EQ(header->excep_updating_status, EXCEP_STATUS_NONE);

    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'x', edge_ptrs), MBError::SUCCESS);
    dmm->AddRootEdge(edge_ptrs, (const uint8_t *) "xyz", 3, 2234);
    EXPECT_EQ(edge_ptrs.flag_ptr[0], EDGE_FLAG_DATA_OFF);
    offset = Get6BInteger(edge_ptrs.offset_ptr);
    EXPECT_EQ(offset, 2234u);
    shm_ptr = dmm->GetShmPtr(header->excep_lf_offset, EDGE_SIZE);
    EXPECT_EQ(shm_ptr != NULL, true);
    if(memcmp(shm_ptr, edge_ptrs.ptr, EDGE_SIZE)) {
        EXPECT_EQ(1, 2);
    }
    EXPECT_EQ(header->excep_updating_status, EXCEP_STATUS_NONE);
}

TEST_F(DictMemTest, InsertNode_test)
{
    Init();

    EdgePtrs edge_ptrs;
    size_t offset;
    uint8_t *shm_ptr;
    MBData mbd;
    int rval;

    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'm', edge_ptrs), MBError::SUCCESS);
    dmm->AddRootEdge(edge_ptrs, (const uint8_t *) "mabain-test", 11, 1234);

    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'm', edge_ptrs), MBError::SUCCESS);
    EXPECT_EQ(edge_ptrs.len_ptr[0] >= 6, true);
    rval = dmm->InsertNode(edge_ptrs, 6, 1334, mbd);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(header->excep_updating_status, EXCEP_STATUS_NONE);
    offset = Get6BInteger(edge_ptrs.offset_ptr);
    shm_ptr = dmm->GetShmPtr(offset, dmm->GetNodeSizePtr()[0]);
    EXPECT_EQ(shm_ptr != NULL, true);
    EXPECT_EQ(shm_ptr[0], FLAG_NODE_NONE | FLAG_NODE_MATCH);
    EXPECT_EQ(shm_ptr[1], 0);
    EXPECT_EQ(shm_ptr[8], (uint8_t)'-');
    EXPECT_EQ(Get6BInteger(shm_ptr+2), 1334u);
    shm_ptr = dmm->GetShmPtr(header->excep_lf_offset, EDGE_SIZE);
    EXPECT_EQ(shm_ptr != NULL, true);
    if(memcmp(shm_ptr, edge_ptrs.ptr, EDGE_SIZE)) {
        EXPECT_EQ(1, 2);
    }
    EXPECT_EQ(edge_ptrs.len_ptr[0], 6);
    EXPECT_EQ(edge_ptrs.flag_ptr[0], 0);
    if(memcmp(edge_ptrs.ptr, "abain", 5)) {
        EXPECT_EQ(1, 2);
    }
}

TEST_F(DictMemTest, InsertNode_test1)
{
    Init();

    EdgePtrs edge_ptrs;
    size_t offset;
    uint8_t *shm_ptr;
    MBData mbd;
    int rval;

    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'm', edge_ptrs), MBError::SUCCESS);
    dmm->AddRootEdge(edge_ptrs, (const uint8_t *) "mabain-testabcdefghijk", 22, 1234);

    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'm', edge_ptrs), MBError::SUCCESS);
    EXPECT_EQ(edge_ptrs.len_ptr[0] >= 6, true);
    rval = dmm->InsertNode(edge_ptrs, 6, 1334, mbd);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(header->excep_updating_status, EXCEP_STATUS_NONE);
    offset = Get6BInteger(edge_ptrs.offset_ptr);
    shm_ptr = dmm->GetShmPtr(offset, dmm->GetNodeSizePtr()[0]);
    EXPECT_EQ(shm_ptr != NULL, true);
    EXPECT_EQ(shm_ptr[0], FLAG_NODE_NONE | FLAG_NODE_MATCH);
    EXPECT_EQ(shm_ptr[1], 0);
    EXPECT_EQ(shm_ptr[8], (uint8_t)'-');
    EXPECT_EQ(Get6BInteger(shm_ptr+2), 1334u);
    shm_ptr = dmm->GetShmPtr(header->excep_lf_offset, EDGE_SIZE);
    EXPECT_EQ(shm_ptr != NULL, true);
    if(memcmp(shm_ptr, edge_ptrs.ptr, EDGE_SIZE)) {
        EXPECT_EQ(1, 2);
    }
    EXPECT_EQ(edge_ptrs.len_ptr[0], 6);
    EXPECT_EQ(edge_ptrs.flag_ptr[0], 0);
    if(memcmp(edge_ptrs.ptr, "abain", 5)) {
        EXPECT_EQ(1, 2);
    }
}

TEST_F(DictMemTest, InsertNode_test2)
{
    Init();

    EdgePtrs edge_ptrs;
    size_t offset;
    uint8_t *shm_ptr;
    MBData mbd;
    int rval;

    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'x', edge_ptrs), MBError::SUCCESS);
    dmm->AddRootEdge(edge_ptrs, (const uint8_t *) "xxxxxxxmabain-test", 18, 1234);

    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'x', edge_ptrs), MBError::SUCCESS);
    EXPECT_EQ(edge_ptrs.len_ptr[0] >= 6, true);
    rval = dmm->InsertNode(edge_ptrs, 13, 1334, mbd);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(header->excep_updating_status, EXCEP_STATUS_NONE);
    offset = Get6BInteger(edge_ptrs.offset_ptr);
    shm_ptr = dmm->GetShmPtr(offset, dmm->GetNodeSizePtr()[0]);
    EXPECT_EQ(shm_ptr != NULL, true);
    EXPECT_EQ(shm_ptr[0], FLAG_NODE_NONE | FLAG_NODE_MATCH);
    EXPECT_EQ(shm_ptr[1], 0);
    EXPECT_EQ(shm_ptr[8], (uint8_t)'-');
    EXPECT_EQ(Get6BInteger(shm_ptr+2), 1334u);
    shm_ptr = dmm->GetShmPtr(header->excep_lf_offset, EDGE_SIZE);
    EXPECT_EQ(shm_ptr != NULL, true);
    if(memcmp(shm_ptr, edge_ptrs.ptr, EDGE_SIZE)) {
        EXPECT_EQ(1, 2);
    }
    EXPECT_EQ(edge_ptrs.len_ptr[0], 13);
    EXPECT_EQ(edge_ptrs.flag_ptr[0], 0);
    EXPECT_EQ(Get5BInteger(edge_ptrs.ptr), 3631u);
}

TEST_F(DictMemTest, AddLink_test)
{
    Init();
    EdgePtrs edge_ptrs;
    uint8_t *shm_ptr;
    MBData mbd;
    int rval;

    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'm', edge_ptrs), MBError::SUCCESS);
    dmm->AddRootEdge(edge_ptrs, (const uint8_t *) "mabain-test", 18, 1234);

    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'm', edge_ptrs), MBError::SUCCESS);
    rval = dmm->AddLink(edge_ptrs, 7, (const uint8_t *)"klsakkslslsldds",
                        15, 12345, mbd);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(edge_ptrs.offset, 1681u);
    EXPECT_EQ(header->excep_updating_status, EXCEP_STATUS_NONE);
    EXPECT_EQ(Get6BInteger(edge_ptrs.offset_ptr), 3609u);
    shm_ptr = dmm->GetShmPtr(3609, 10);
    EXPECT_EQ((int)shm_ptr[0], 0);
    EXPECT_EQ((int)shm_ptr[1], 1);
    EXPECT_EQ((char)shm_ptr[8], 't');
    EXPECT_EQ((char)shm_ptr[9], 'k');
    EXPECT_EQ(Get5BInteger(edge_ptrs.ptr), 3655u);
    shm_ptr = dmm->GetShmPtr(3655, 10);
    EXPECT_EQ(std::string((const char *)shm_ptr, 5).compare("abain"), 0);
}

TEST_F(DictMemTest, UpdateNode_test)
{
    Init();
    EdgePtrs edge_ptrs;
    uint8_t *shm_ptr;
    MBData mbd;
    int rval;

    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'm', edge_ptrs), MBError::SUCCESS);
    dmm->AddRootEdge(edge_ptrs, (const uint8_t *) "mabain-test", 18, 1234);

    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'm', edge_ptrs), MBError::SUCCESS);
    rval = dmm->AddLink(edge_ptrs, 7, (const uint8_t *)"klsakkslslsldds",
                        15, 12345, mbd);
    EXPECT_EQ(rval, MBError::SUCCESS);

    uint8_t tmp_buff[256];
    int match_len = 7;
    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'm', edge_ptrs), MBError::SUCCESS);
    bool next = dmm->FindNext((const uint8_t*)"abcdefg", 7, match_len, edge_ptrs, tmp_buff);
    EXPECT_EQ(next, false);
    rval = dmm->UpdateNode(edge_ptrs, (const uint8_t*)"abcdefg", 7, 12345);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(edge_ptrs.offset, 1681u);
    EXPECT_EQ(Get5BInteger(edge_ptrs.ptr), 3655u);
    shm_ptr = dmm->GetShmPtr(3655, 6);
    EXPECT_EQ(std::string((const char*)shm_ptr, 6).compare("abain-"), 0);
}

TEST_F(DictMemTest, FindNext_test)
{
    Init();
    EdgePtrs edge_ptrs;
    MBData mbd;
    int rval;

    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'm', edge_ptrs), MBError::SUCCESS);
    dmm->AddRootEdge(edge_ptrs, (const uint8_t *) "mabain-abc", 18, 1234);

    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'm', edge_ptrs), MBError::SUCCESS);
    rval = dmm->AddLink(edge_ptrs, 7, (const uint8_t *)"hijk", 4, 12345, mbd);
    EXPECT_EQ(rval, MBError::SUCCESS);

    bool next;
    uint8_t key[256];
    uint8_t tmp_buff[256];
    int match_len = 7;

    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'm', edge_ptrs), MBError::SUCCESS);
    next = dmm->FindNext((const uint8_t*)"xyz293ksk", 9, match_len,
                              edge_ptrs, tmp_buff);
    EXPECT_EQ(next, false);
    rval = dmm->UpdateNode(edge_ptrs, (const uint8_t*)"xyz293ksk", 9, 22345);
    EXPECT_EQ(rval, MBError::SUCCESS);

    for(int i = 0; i < 256; i++) {
        key[i] = (uint8_t) i;
    }
    for(int i = 0; i < 256; i++) {
        memset(&edge_ptrs, 0, sizeof(edge_ptrs));
        EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'm', edge_ptrs), MBError::SUCCESS);
        next = dmm->FindNext((const uint8_t*)key+i, 1, match_len,
                             edge_ptrs, tmp_buff);
        if((char)key[i] == 'a' || (char)key[i] == 'h' || (char) key[i] == 'x') {
            EXPECT_EQ(next, true);
        } else {
            EXPECT_EQ(next, false);
        }
    }
}

TEST_F(DictMemTest, GetRootEdge_test)
{
    Init();

    EdgePtrs edge_ptrs;
    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge_Writer(false, 'm', edge_ptrs), MBError::SUCCESS);
    dmm->AddRootEdge(edge_ptrs, (const uint8_t *) "mabain-unittest", 15, 1234);
    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    EXPECT_EQ(dmm->GetRootEdge(false, 'm', edge_ptrs), MBError::SUCCESS);
    EXPECT_EQ(edge_ptrs.offset, 1681u);
    EXPECT_EQ(Get5BInteger(edge_ptrs.ptr), 3592u);

    uint8_t *ptr = dmm->GetShmPtr(3592, 14);
    EXPECT_EQ(memcmp(ptr, "abain-unittest", 14)==0, true);
}

TEST_F(DictMemTest, GetRootEdge_Writer_test)
{
    Init();

    EdgePtrs edge_ptrs;
    memset(&edge_ptrs, 0, sizeof(edge_ptrs));
    for(int i = 0; i < 256; i++) {
        EXPECT_EQ(dmm->GetRootEdge_Writer(false, i, edge_ptrs), MBError::SUCCESS);
    }
}

TEST_F(DictMemTest, ClearRootEdge_test)
{
    Init();
    uint8_t buff[1024];
    EdgePtrs edge_ptrs;

    dmm->WriteData(buff, 0, dmm->GetRootOffset());

    dmm->ClearRootEdge(0);
    dmm->GetRootEdge(0, 0, edge_ptrs); 
    EXPECT_EQ(memcmp(edge_ptrs.edge_buff, DictMem::empty_edge, EDGE_SIZE)==0, true);

    dmm->ClearRootEdge(10);
    dmm->GetRootEdge(0, 10, edge_ptrs); 
    EXPECT_EQ(memcmp(edge_ptrs.edge_buff, DictMem::empty_edge, EDGE_SIZE)==0, true);

    dmm->ClearRootEdge(111);
    dmm->GetRootEdge(0, 111, edge_ptrs); 
    EXPECT_EQ(memcmp(edge_ptrs.edge_buff, DictMem::empty_edge, EDGE_SIZE)==0, true);
}

TEST_F(DictMemTest, ReserveData_test)
{
    Init();

    uint8_t buff[256];
    int size;
    size_t offset = 0;

    size = 100;
    dmm->ReserveData(buff, size, offset, false);
    EXPECT_EQ(header->m_index_offset, 3692u);
    EXPECT_EQ(header->m_index_offset, offset+100);
}

TEST_F(DictMemTest, NextEdge_test)
{
    Init();
}

TEST_F(DictMemTest, RemoveEdgeByIndex_test)
{
    Init();
}

TEST_F(DictMemTest, InitRootNode_test)
{
    Init();

    dmm->InitRootNode();
    EXPECT_EQ(header->m_index_offset, 3592u);
}

TEST_F(DictMemTest, WriteEdge_test)
{
    Init();

    header->m_index_offset = 10000;
    EdgePtrs edge_ptrs;
    edge_ptrs.offset = 1234;
    edge_ptrs.ptr = edge_ptrs.edge_buff;
    dmm->WriteEdge(edge_ptrs);
    EXPECT_EQ(header->excep_updating_status, EXCEP_STATUS_NONE);
}

TEST_F(DictMemTest, WriteData_test)
{
    Init();

    size_t offset;
    int size;
    uint8_t buff[32];
    uint8_t *shm_ptr;

    offset = 12345;
    size = 21;
    header->m_index_offset = offset + 10000;
    for(int i = 0; i < size; i++) {
        buff[i] = (uint8_t) i;
    }
    dmm->WriteData(buff, size, offset);
    dmm->Flush();
    shm_ptr = dmm->GetShmPtr(offset, size);
    EXPECT_EQ(shm_ptr != NULL, true); 
    for(int i = 0; i < size; i++) {
        EXPECT_EQ((int)shm_ptr[i], i);
    }
}

TEST_F(DictMemTest, Flush_test)
{
   Init();
   uint8_t buff[32];
   header->m_index_offset = 10000000;
   dmm->WriteData(buff, 32, 10000);
   dmm->Flush();
}

}
