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

#include <gtest/gtest.h>

#include "../db.h"
#include "../resource_pool.h"

using namespace mabain;

namespace {

class MemoryOnlyTest : public ::testing::Test
{
public:
    MemoryOnlyTest() {
    }
    virtual ~MemoryOnlyTest() {
    }

    virtual void SetUp() {
        db = nullptr;
    }
    virtual void TearDown() {
        if(db != nullptr) {
            db->Close();
            delete db;
        }
        ResourcePool::getInstance().RemoveAll();
    }

protected:
    DB *db;
};

TEST_F(MemoryOnlyTest, MemoryOnlyTest_test_constructor_1)
{
    int options = CONSTS::WriterOptions() | CONSTS::MEMORY_ONLY_MODE;
    db = new DB("test_constructor_1", options);
    EXPECT_EQ(MBError::SUCCESS, db->Status());
}

TEST_F(MemoryOnlyTest, MemoryOnlyTest_test_constructor_2)
{
    int options = CONSTS::ReaderOptions() | CONSTS::MEMORY_ONLY_MODE;
    db = new DB("test_constructor_2", options);
    EXPECT_EQ(MBError::NO_DB, db->Status());
}

TEST_F(MemoryOnlyTest, MemoryOnlyTest_test_add)
{
    int options = CONSTS::WriterOptions() | CONSTS::MEMORY_ONLY_MODE;
    db = new DB("test_add", options);

    int num = 10000;
    std::string key;
    for(int i = 0; i < num; i++) {
        key = std::string("test_add_") + std::to_string(i);
        db->Add(key, key);
    }

    MBData mbd;
    int rval;
    std::string value;
    for(int i = 0; i < num; i++) {
        key = std::string("test_add_") + std::to_string(i);
        rval = db->Find(key, mbd);
        EXPECT_EQ(MBError::SUCCESS, rval); 
        value = std::string((char *)mbd.buff, mbd.data_len);
        EXPECT_EQ(key, value);
    }
}

TEST_F(MemoryOnlyTest, MemoryOnlyTest_test_async)
{
    int options = CONSTS::WriterOptions() | CONSTS::MEMORY_ONLY_MODE;
    options |= CONSTS::ASYNC_WRITER_MODE;
    db = new DB("test_async", options);

    options = CONSTS::ReaderOptions() | CONSTS::MEMORY_ONLY_MODE;
    DB db_r = DB("test_async", options);

    db_r.Close();
}

}
