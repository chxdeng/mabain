/**
 * Copyright (C) 2017 Cisco Inc.
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

#include <list>
#include <stdlib.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "../error.h"
#include "../free_list.h"

using namespace mabain;

namespace {

class FreeListTest : public ::testing::Test {
public:
    FreeListTest() { }
    virtual ~FreeListTest() { }
    virtual void SetUp() { }
    virtual void TearDown() { }

protected:
};

TEST_F(FreeListTest, AddBufferByIndex_test)
{
    int rval;
    FreeList flist("./freelist", 4, 1000);

    rval = flist.AddBufferByIndex(0, 100);
    EXPECT_EQ(rval, 0);
    EXPECT_EQ(flist.Count(), 1);
    EXPECT_EQ(flist.GetBufferCountByIndex(0), 1u);

    rval = flist.AddBufferByIndex(111, 128);
    EXPECT_EQ(rval, 0);
    EXPECT_EQ(flist.Count(), 2);
    EXPECT_EQ(flist.GetBufferCountByIndex(111), 1u);
}

TEST_F(FreeListTest, RemoveBufferByIndex_test)
{
    FreeList flist("./freelist", 8, 2000);

    flist.AddBufferByIndex(33, 72);
    EXPECT_EQ(flist.RemoveBufferByIndex(33), 72u);

    flist.AddBufferByIndex(44, 328);
    flist.AddBufferByIndex(44, 1024);
    flist.AddBufferByIndex(102, 8);
    EXPECT_EQ(flist.RemoveBufferByIndex(44), 328u);
    EXPECT_EQ(flist.RemoveBufferByIndex(102), 8u);
    EXPECT_EQ(flist.RemoveBufferByIndex(44), 1024u);
}

TEST_F(FreeListTest, GetAlignmentSize_test)
{
    FreeList flist("./freelist", 8, 1222);

    EXPECT_EQ(flist.GetAlignmentSize(128), 128);
    EXPECT_EQ(flist.GetAlignmentSize(129), 136);
    EXPECT_EQ(flist.GetAlignmentSize(130), 136);
    EXPECT_EQ(flist.GetAlignmentSize(131), 136);
    EXPECT_EQ(flist.GetAlignmentSize(132), 136);
    EXPECT_EQ(flist.GetAlignmentSize(133), 136);
    EXPECT_EQ(flist.GetAlignmentSize(134), 136);
    EXPECT_EQ(flist.GetAlignmentSize(135), 136);
    EXPECT_EQ(flist.GetAlignmentSize(136), 136);
    EXPECT_EQ(flist.GetAlignmentSize(137), 144);
}

TEST_F(FreeListTest, GetBufferIndex_test)
{
    FreeList flist("./freelist", 4, 222);

    EXPECT_EQ(flist.GetBufferIndex(1), 0);
    EXPECT_EQ(flist.GetBufferIndex(2), 0);
    EXPECT_EQ(flist.GetBufferIndex(3), 0);
    EXPECT_EQ(flist.GetBufferIndex(4), 0);
    EXPECT_EQ(flist.GetBufferIndex(5), 1);
}

TEST_F(FreeListTest, GetBufferCountByIndex_test)
{
    FreeList flist("./freelist", 4, 333);

    flist.AddBufferByIndex(7, 4);
    flist.AddBufferByIndex(7, 24);
    flist.AddBufferByIndex(7, 8);

    EXPECT_EQ(flist.GetBufferCountByIndex(7), 3u);
}

TEST_F(FreeListTest, GetBufferSizeByIndex_test)
{
    FreeList flist("./freelist", 4, 333);

    flist.AddBufferByIndex(13, 64);
    flist.AddBufferByIndex(3, 28);
    flist.AddBufferByIndex(101, 256);
    flist.AddBufferByIndex(101, 516);

    EXPECT_EQ(flist.GetBufferCountByIndex(13), 1u);
    EXPECT_EQ(flist.GetBufferCountByIndex(3), 1u);
    EXPECT_EQ(flist.GetBufferCountByIndex(101), 2u);
}

TEST_F(FreeListTest, ReleaseBuffer_test)
{
    int rval;
    FreeList flist("./freelist", 4, 444);

    rval = flist.ReleaseBuffer(16, 8);
    EXPECT_EQ(rval, MBError::SUCCESS);
    rval = flist.ReleaseBuffer(124, 34);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(flist.GetBufferCountByIndex(flist.GetBufferIndex(8)), 1u);
    EXPECT_EQ(flist.GetBufferCountByIndex(flist.GetBufferIndex(34)), 1u);
}

TEST_F(FreeListTest, AddBuffer_test)
{
    int rval;
    FreeList flist("./freelist", 4, 555);

    rval = flist.AddBuffer(40, 34);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(flist.GetBufferCountByIndex(flist.GetBufferIndex(34)), 1u);
    rval = flist.AddBuffer(144, 34);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(flist.GetBufferCountByIndex(flist.GetBufferIndex(34)), 2u);
    rval = flist.AddBuffer(196, 35);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(flist.GetBufferCountByIndex(flist.GetBufferIndex(34)), 3u);

    rval = flist.AddBuffer(260, 135);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(flist.GetBufferCountByIndex(flist.GetBufferIndex(135)), 1u);
}

TEST_F(FreeListTest, RemoveBuffer_test)
{
    int rval;
    FreeList flist("./freelist", 4, 555);
    size_t offset;

    flist.AddBufferByIndex(98, 96);
    flist.AddBufferByIndex(98, 196);
    rval = flist.RemoveBuffer(offset, flist.GetBufferSizeByIndex(98));
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(offset, 96u);
    rval = flist.RemoveBuffer(offset, flist.GetBufferSizeByIndex(98));
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(offset, 196u);
    rval = flist.RemoveBuffer(offset, flist.GetBufferSizeByIndex(98));
    EXPECT_EQ(rval, MBError::NO_MEMORY);
}

TEST_F(FreeListTest, GetTotSize_test)
{
    FreeList flist("./freelist", 4, 555);
    size_t tot = 0;

    flist.AddBuffer(8, 183);
    tot += flist.GetAlignmentSize(183);
    flist.AddBuffer(240, 44);
    tot += flist.GetAlignmentSize(44);
    flist.AddBufferByIndex(33, 1024);
    tot += flist.GetBufferSizeByIndex(33);
    flist.AddBufferByIndex(133, 2056);
    tot += flist.GetBufferSizeByIndex(133);
    EXPECT_EQ(flist.GetTotSize(), tot);
    EXPECT_EQ(flist.Count(), 4);
}

TEST_F(FreeListTest, StoreLoadEmpty_test)
{
    int rval;
    FreeList flist("./freelist", 4, 555);

    rval = flist.StoreListOnDisk();
    EXPECT_EQ(rval, MBError::SUCCESS);
    rval = access("./freelist", R_OK);
    EXPECT_EQ(rval, -1);
}

TEST_F(FreeListTest, StoreLoadSmallFilling_test)
{
    int rval;
    size_t offset;
    FreeList flist("./freelist", 4, 555);

    flist.AddBuffer(32, 17);
    flist.AddBuffer(84, 11);
    flist.AddBuffer(144, 23);
    flist.AddBuffer(444, 3);
    rval = flist.StoreListOnDisk();
    EXPECT_EQ(rval, MBError::SUCCESS);
    rval = access("./freelist", F_OK);
    EXPECT_EQ(rval, 0);
    EXPECT_EQ(flist.Count(), 0);
    EXPECT_EQ(flist.GetTotSize(), 0u);

    flist.LoadListFromDisk();
    rval = access("./freelist", R_OK);
    EXPECT_EQ(rval, -1);
    rval = flist.RemoveBuffer(offset, 11);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(offset, 84u);
    rval = flist.RemoveBuffer(offset, 23);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(offset, 144u);
    rval = flist.RemoveBuffer(offset, 17);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(offset, 32u);
    rval = flist.RemoveBuffer(offset, 2);
    EXPECT_EQ(rval, MBError::SUCCESS);
    EXPECT_EQ(offset, 444u);
}

TEST_F(FreeListTest, StoreLoadFullFilling_test)
{
    int rval;
    size_t offset;
    int size;
    int num_buff = 1011;
    FreeList flist("./freelist", 4, num_buff);
    std::list<size_t> buff_list;

    srand(time(NULL));

    offset = 0;
    for (int i = 0; i < num_buff; i++) {
        size = rand() % 111 + 2;
        flist.AddBuffer(offset, size);
        buff_list.push_back(offset);
        buff_list.push_back(size);
        offset += flist.GetAlignmentSize(size);
    }

    rval = flist.StoreListOnDisk();
    EXPECT_EQ(rval, MBError::SUCCESS);
    rval = access("./freelist", F_OK);
    EXPECT_EQ(rval, 0);
    EXPECT_EQ(flist.Count(), 0);
    EXPECT_EQ(flist.GetTotSize(), 0u);

    flist.LoadListFromDisk();
    rval = access("./freelist", R_OK);
    EXPECT_EQ(rval, -1);

    for (std::list<size_t>::iterator it = buff_list.begin(); it != buff_list.end(); ++it) {
        offset = *it;
        it++;
        size = *it;
        rval = flist.RemoveBuffer(offset, size);
        EXPECT_EQ(rval, MBError::SUCCESS);
    }
    size = 34;
    rval = flist.RemoveBuffer(offset, size);
    EXPECT_EQ(rval, MBError::NO_MEMORY);
}

TEST_F(FreeListTest, StoreLoadHalfFilling_test)
{
    int rval;
    size_t offset;
    int size;
    int num_buff = 1000;
    FreeList flist("./freelist", 4, num_buff);
    std::list<size_t> buff_list;

    srand(time(NULL));

    offset = 24;
    for (int i = 0; i < num_buff; i++) {
        size = rand() % 98 + 1;
        if (i % 2 == 0) {
            flist.AddBuffer(offset, size);
            buff_list.push_back(offset);
            buff_list.push_back(size);
        }
        offset += flist.GetAlignmentSize(size);
    }

    rval = flist.StoreListOnDisk();
    EXPECT_EQ(rval, MBError::SUCCESS);
    rval = access("./freelist", F_OK);
    EXPECT_EQ(rval, 0);
    EXPECT_EQ(flist.Count(), 0);
    EXPECT_EQ(flist.GetTotSize(), 0u);

    flist.LoadListFromDisk();
    rval = access("./freelist", R_OK);
    EXPECT_EQ(rval, -1);

    for (std::list<size_t>::iterator it = buff_list.begin(); it != buff_list.end(); ++it) {
        offset = *it;
        it++;
        size = *it;
        rval = flist.RemoveBuffer(offset, size);
        EXPECT_EQ(rval, MBError::SUCCESS);
    }
    size = 34;
    rval = flist.RemoveBuffer(offset, size);
    EXPECT_EQ(rval, MBError::NO_MEMORY);
}

}
