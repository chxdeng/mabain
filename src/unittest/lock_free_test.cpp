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

#include "../lock_free.h"
#include "../mabain_consts.h"
#include "../error.h"
#include "../drm_base.h"

using namespace mabain;

namespace {

class LockFreeTest : public ::testing::Test
{
public:
    LockFreeTest() {
        int size = sizeof(lock_free_data);
        memset((char*) &lock_free_data, 0, size);
        size = sizeof(header);
        memset((char*) &header, 0, size);
        lfree.LockFreeInit(&lock_free_data, &header, CONSTS::ACCESS_MODE_WRITER);
    }
    virtual ~LockFreeTest() {
    }

    virtual void SetUp() {
    }
    virtual void TearDown() {
    }

protected:
    LockFreeShmData lock_free_data;
    LockFree lfree;
    IndexHeader header;
};

TEST_F(LockFreeTest, WriterLockFreeStart_test)
{
    lfree.WriterLockFreeStart(101);
    EXPECT_EQ(lock_free_data.counter, 0u);
    EXPECT_EQ(lock_free_data.offset, 101u);
}

TEST_F(LockFreeTest, WriterLockFreeStop_test)
{
    lock_free_data.counter = 0xFFFFFFFF;
    lfree.WriterLockFreeStart(102);
    lfree.WriterLockFreeStop();
    EXPECT_EQ(lock_free_data.counter, 0u);
    EXPECT_EQ(lock_free_data.offset_cache[0xFFFFFFFF % MAX_OFFSET_CACHE], 102u);
}

TEST_F(LockFreeTest, ReaderLockFreeStart_test)
{
    lock_free_data.counter = 99;
    lock_free_data.offset = 12345;
    for(int i = 0; i < MAX_OFFSET_CACHE; i++) {
        lock_free_data.offset_cache[i] = i*100 + 3;
    }

    LockFreeData snapshot;
    lfree.ReaderLockFreeStart(snapshot);
    EXPECT_EQ(snapshot.counter, 99u);
}

TEST_F(LockFreeTest, ReaderLockFreeStop_test)
{
    lock_free_data.counter = 99;
    lock_free_data.offset = 12345;
    for(int i = 0; i < MAX_OFFSET_CACHE; i++) {
        lock_free_data.offset_cache[i] = (i + 1)*1000 + 233;
    }

    size_t offset;
    int rval;
    MBData mbd;

    offset = 100001;
    LockFreeData snapshot;
    lfree.WriterLockFreeStart(offset);
    lfree.WriterLockFreeStop();
    lfree.ReaderLockFreeStart(snapshot);
    rval = lfree.ReaderLockFreeStop(snapshot, offset, mbd);
    EXPECT_EQ(rval, MBError::SUCCESS);
    rval = lfree.ReaderLockFreeStop(snapshot, offset+1000, mbd);
    EXPECT_EQ(rval, MBError::SUCCESS);
}

TEST_F(LockFreeTest, ReaderLockFreeStop_test_1)
{
    lock_free_data.counter = 999;
    lock_free_data.offset = 12345;
    for(int i = 0; i < MAX_OFFSET_CACHE; i++) {
        lock_free_data.offset_cache[i] = (i + 1)*1000 + 48213;
    }

    size_t offset;
    int rval;
    MBData mbd;

    offset = 510036;
    LockFreeData snapshot;
    lfree.WriterLockFreeStart(offset);
    lfree.ReaderLockFreeStart(snapshot);
    rval = lfree.ReaderLockFreeStop(snapshot, offset, mbd);
    EXPECT_EQ(rval, MBError::TRY_AGAIN);
    rval = lfree.ReaderLockFreeStop(snapshot, offset+1000, mbd);
    EXPECT_EQ(rval, MBError::SUCCESS);
    lfree.WriterLockFreeStop();
}

TEST_F(LockFreeTest, ReaderLockFreeStop_test_2)
{
    lock_free_data.counter = 999;
    lock_free_data.offset = 12345;
    for(int i = 0; i < MAX_OFFSET_CACHE; i++) {
        lock_free_data.offset_cache[i] = (i + 1)*1000 + 48213;
    }

    size_t offset;
    int rval;
    MBData mbd;

    offset = 510036;
    LockFreeData snapshot;
    lfree.ReaderLockFreeStart(snapshot);
    lfree.WriterLockFreeStart(offset);
    rval = lfree.ReaderLockFreeStop(snapshot, offset,mbd);
    EXPECT_EQ(rval, MBError::TRY_AGAIN);
    rval = lfree.ReaderLockFreeStop(snapshot, offset+1000, mbd);
    EXPECT_EQ(rval, MBError::SUCCESS);
    lfree.WriterLockFreeStop();
}

TEST_F(LockFreeTest, ReaderLockFreeStop_test_3)
{
    lock_free_data.counter = 999;
    lock_free_data.offset = 12345;
    for(int i = 0; i < MAX_OFFSET_CACHE; i++) {
        lock_free_data.offset_cache[i] = (i + 1)*1000 + 48213;
    }

    size_t offset;
    int rval;
    MBData mbd;

    offset = 510036;
    LockFreeData snapshot;
    lfree.ReaderLockFreeStart(snapshot);
    lfree.WriterLockFreeStart(offset);
    lfree.WriterLockFreeStop();
    lfree.WriterLockFreeStart(offset+1000);
    rval = lfree.ReaderLockFreeStop(snapshot, offset, mbd);
    EXPECT_EQ(rval, MBError::TRY_AGAIN);
    rval = lfree.ReaderLockFreeStop(snapshot, offset+1000, mbd);
    EXPECT_EQ(rval, MBError::TRY_AGAIN);
    rval = lfree.ReaderLockFreeStop(snapshot, offset+2000, mbd);
    EXPECT_EQ(rval, MBError::SUCCESS);
    lfree.WriterLockFreeStop();
}

TEST_F(LockFreeTest, ReaderLockFreeStop_test_4)
{
    lock_free_data.counter = 999;
    lock_free_data.offset = 12345;
    for(int i = 0; i < MAX_OFFSET_CACHE; i++) {
        lock_free_data.offset_cache[i] = (i + 1)*1000 + 48213;
    }

    size_t offset;
    int rval;
    MBData mbd;

    offset = 510036;
    LockFreeData snapshot;
    lfree.ReaderLockFreeStart(snapshot);

    for(int i = 0; i < MAX_OFFSET_CACHE; i++) {
        lfree.WriterLockFreeStart(offset + i*1000);
        if(i < MAX_OFFSET_CACHE-1) {
            lfree.WriterLockFreeStop();
        }
    }
    for(int i = 0; i < MAX_OFFSET_CACHE; i++) {
        rval = lfree.ReaderLockFreeStop(snapshot, offset + i*1000, mbd);
        EXPECT_EQ(rval, MBError::TRY_AGAIN);
    }
    rval = lfree.ReaderLockFreeStop(snapshot, offset + 1100, mbd);
    EXPECT_EQ(rval, MBError::SUCCESS);
    lfree.WriterLockFreeStop();
}

TEST_F(LockFreeTest, ReaderLockFreeStop_test_5)
{
    lock_free_data.counter = 999;
    lock_free_data.offset = 12345;
    for(int i = 0; i < MAX_OFFSET_CACHE; i++) {
        lock_free_data.offset_cache[i] = (i + 1)*1000 + 48213;
    }

    size_t offset;
    int rval;
    MBData mbd;

    offset = 510036;
    LockFreeData snapshot;
    lfree.ReaderLockFreeStart(snapshot);

    for(int i = 0; i < MAX_OFFSET_CACHE; i++) {
        lfree.WriterLockFreeStart(offset + i*1000);
        lfree.WriterLockFreeStop();
    }
    for(int i = 0; i < MAX_OFFSET_CACHE; i++) {
        rval = lfree.ReaderLockFreeStop(snapshot, offset + i*1000, mbd);
        EXPECT_EQ(rval, MBError::TRY_AGAIN);
    }
    rval = lfree.ReaderLockFreeStop(snapshot, offset + 1100, mbd);
    EXPECT_EQ(rval, MBError::TRY_AGAIN);
    lfree.WriterLockFreeStop();
}

// Test for https://github.com/chxdeng/mabain/issues/35
TEST_F(LockFreeTest, ReaderLockFreeStop_test_6)
{
    lock_free_data.counter = 232811;
    lock_free_data.offset = 54321;
    for(int i = 0; i < MAX_OFFSET_CACHE; i++) {
        lock_free_data.offset_cache[i] = (i + 1)*1000 + 148213;
    }

    size_t offset;
    int rval;
    MBData mbd;

    offset = 54321;
    LockFreeData snapshot;

    // edge added
    lfree.ReaderLockFreeStart(snapshot);
    header.excep_updating_status = EXCEP_STATUS_ADD_EDGE;
    lfree.WriterLockFreeStart(offset);

    rval = lfree.ReaderLockFreeStop(snapshot, offset, mbd);
    EXPECT_EQ(MBError::TRY_AGAIN, rval);
    EXPECT_TRUE(mbd.options & CONSTS::OPTION_READ_SAVED_EDGE);

    lfree.ReaderLockFreeStart(snapshot);
    rval = lfree.ReaderLockFreeStop(snapshot, offset, mbd);
    EXPECT_EQ(MBError::SUCCESS, rval);
    EXPECT_FALSE(mbd.options & CONSTS::OPTION_READ_SAVED_EDGE);

    // edge remooved
    header.excep_updating_status = EXCEP_STATUS_REMOVE_EDGE;
    lfree.WriterLockFreeStart(offset);
    lfree.ReaderLockFreeStart(snapshot);
    rval = lfree.ReaderLockFreeStop(snapshot, offset, mbd);
    EXPECT_EQ(MBError::TRY_AGAIN, rval);
    EXPECT_TRUE(mbd.options & CONSTS::OPTION_READ_SAVED_EDGE);

    lfree.ReaderLockFreeStart(snapshot);
    rval = lfree.ReaderLockFreeStop(snapshot, offset, mbd);
    EXPECT_EQ(MBError::SUCCESS, rval);
    EXPECT_FALSE(mbd.options & CONSTS::OPTION_READ_SAVED_EDGE);
}

}
