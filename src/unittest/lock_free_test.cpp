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

using namespace mabain;

namespace {

class LockFreeTest : public ::testing::Test
{
public:
    LockFreeTest() {
        memset(&lock_free_data, 0, sizeof(lock_free_data));
        lfree.LockFreeInit(&lock_free_data, CONSTS::ACCESS_MODE_WRITER);
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
};

TEST_F(LockFreeTest, WriterLockFreeStart_test)
{
    lfree.WriterLockFreeStart(101);
    EXPECT_EQ(lock_free_data.counter, 0);
    EXPECT_EQ(lock_free_data.offset, 101);
}

TEST_F(LockFreeTest, WriterLockFreeStop_test)
{
    lock_free_data.counter = 0xFFFFFFFF;
    lfree.WriterLockFreeStart(102);
    lfree.WriterLockFreeStop();
    EXPECT_EQ(lock_free_data.counter, 0);
    EXPECT_EQ(lock_free_data.offset_cache[0xFFFFFFFF % MAX_OFFSET_CACHE], 102);
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
    EXPECT_EQ(snapshot.counter, 99);
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

    offset = 100001;
    LockFreeData snapshot;
    lfree.WriterLockFreeStart(offset);
    lfree.WriterLockFreeStop();
    lfree.ReaderLockFreeStart(snapshot);
    rval = lfree.ReaderLockFreeStop(snapshot, offset);
    EXPECT_EQ(rval, MBError::SUCCESS);
    rval = lfree.ReaderLockFreeStop(snapshot, offset+1000);
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

    offset = 510036;
    LockFreeData snapshot;
    lfree.WriterLockFreeStart(offset);
    lfree.ReaderLockFreeStart(snapshot);
    rval = lfree.ReaderLockFreeStop(snapshot, offset);
    EXPECT_EQ(rval, MBError::TRY_AGAIN);
    rval = lfree.ReaderLockFreeStop(snapshot, offset+1000);
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

    offset = 510036;
    LockFreeData snapshot;
    lfree.ReaderLockFreeStart(snapshot);
    lfree.WriterLockFreeStart(offset);
    rval = lfree.ReaderLockFreeStop(snapshot, offset);
    EXPECT_EQ(rval, MBError::TRY_AGAIN);
    rval = lfree.ReaderLockFreeStop(snapshot, offset+1000);
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

    offset = 510036;
    LockFreeData snapshot;
    lfree.ReaderLockFreeStart(snapshot);
    lfree.WriterLockFreeStart(offset);
    lfree.WriterLockFreeStop();
    lfree.WriterLockFreeStart(offset+1000);
    rval = lfree.ReaderLockFreeStop(snapshot, offset);
    EXPECT_EQ(rval, MBError::TRY_AGAIN);
    rval = lfree.ReaderLockFreeStop(snapshot, offset+1000);
    EXPECT_EQ(rval, MBError::TRY_AGAIN);
    rval = lfree.ReaderLockFreeStop(snapshot, offset+2000);
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
        rval = lfree.ReaderLockFreeStop(snapshot, offset + i*1000);
        EXPECT_EQ(rval, MBError::TRY_AGAIN);
    }
    rval = lfree.ReaderLockFreeStop(snapshot, offset + 1100);
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

    offset = 510036;
    LockFreeData snapshot;
    lfree.ReaderLockFreeStart(snapshot);

    for(int i = 0; i < MAX_OFFSET_CACHE; i++) {
        lfree.WriterLockFreeStart(offset + i*1000);
        lfree.WriterLockFreeStop();
    }
    for(int i = 0; i < MAX_OFFSET_CACHE; i++) {
        rval = lfree.ReaderLockFreeStop(snapshot, offset + i*1000);
        EXPECT_EQ(rval, MBError::TRY_AGAIN);
    }
    rval = lfree.ReaderLockFreeStop(snapshot, offset + 1100);
    EXPECT_EQ(rval, MBError::TRY_AGAIN);
    lfree.WriterLockFreeStop();
}

}
