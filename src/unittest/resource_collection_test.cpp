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

#include <unistd.h>
#include <stdlib.h>
#include <iostream>
#include <openssl/sha.h>
#include <sys/time.h>

#include <gtest/gtest.h>

#define TEST_MBSHRINK_FRIENDS \
    friend class MBShrinkTest;

#include "../db.h"
#include "../mabain_consts.h"
#include "../mb_rc.h"
#include "../error.h"
#include "./test_key.h"

#define DB_DIR "/var/tmp/mabain_test/"
#define KEY_TYPE_INT    0
#define KEY_TYPE_SHA256 1

using namespace mabain;

namespace {

class ResourceCollectionTest : public ::testing::Test
{
public:
    ResourceCollectionTest() : db(DB(DB_DIR, CONSTS::ACCESS_MODE_WRITER, 128ULL*1024*1024, 128ULL*1024*1024)) {
        if(!db.is_open()) {
            std::cerr << "failed to open mabain db: " << db.StatusStr() << "\n";
            abort();
        }

        key_type = MABAIN_TEST_KEY_TYPE_INT;
        srand(time(NULL));
    }
    virtual ~ResourceCollectionTest() {
        db.Close();
    }
    virtual void SetUp() {
        db.RemoveAll();
    }
    virtual void TearDown() {
    }

    void Populate(long num, bool *exist) {
        TestKey tkey = TestKey(key_type);
        for(long i = 0; i < num; i++) {
            std::string key = tkey.get_key(i);
            std::string value = key;
            int rval = db.Add(key.c_str(), key.length(), value.c_str(), value.length());
            assert(rval == MBError::SUCCESS);
            exist[i] = true;
        }
    }

    void DeleteRandom(long num, bool *exist) {
        long count = 0;
        int64_t tot_count = db.Count();
        TestKey tkey = TestKey(key_type);
        while(true) {
            long ikey = rand() % tot_count;
            std::string key = tkey.get_key(ikey);
            int rval = db.Remove(key.c_str(), key.length());
            if(rval == MBError::SUCCESS) {
                exist[ikey] = false;
                count++;
                if(count >= num) break;
            }
        }
    }

    void DeleteOdd(long num,  bool *exist) {
        TestKey tkey = TestKey(key_type);
        for(long i = 0; i < num; i++) {
            if(i % 2 == 0) continue;
            std::string key = tkey.get_key(i);
            std::string value = key;
            db.Remove(key.c_str(), key.length());
            exist[i] = false;
        }
    }

    void DeleteRange(long start, long end, bool *exist) {
        TestKey tkey = TestKey(key_type);
        for(long i = start; i < end; i++) {
            std::string key = tkey.get_key(i);
            std::string value = key;
            db.Remove(key.c_str(), key.length());
            exist[i] = false;
       }
   }

   void VerifyKeyValue(long ikey, bool found) {
       TestKey tkey = TestKey(key_type);
       std::string key = tkey.get_key(ikey);
       std::string value = key;
       MBData mbd;
       int rval = db.Find(key.c_str(), key.length(), mbd);
       if(found) {
           EXPECT_EQ(rval, MBError::SUCCESS);
           EXPECT_TRUE(value == std::string(reinterpret_cast<char*>(mbd.buff), mbd.data_len));
       } else {
           EXPECT_EQ(rval, MBError::NOT_EXIST);
       }
   }

protected:
    DB db;
    int key_type;
};

TEST_F(ResourceCollectionTest, RC_reorder_index_test)
{
    key_type = KEY_TYPE_INT;
    ResourceCollection rc(db, RESOURCE_COLLECTION_TYPE_INDEX);

    long tot = 53245;
    bool *exist = new bool[tot];
    Populate(tot, exist);
    rc.ReclaimResource(0, 0);
    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete [] exist;
}

TEST_F(ResourceCollectionTest, RC_reorder_data_test)
{
    key_type = MABAIN_TEST_KEY_TYPE_SHA_128;
    ResourceCollection rc(db, RESOURCE_COLLECTION_TYPE_DATA);

    long tot = 35275;
    bool *exist = new bool[tot];
    Populate(tot, exist);
    rc.ReclaimResource(0, 0);
    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete [] exist;
}

TEST_F(ResourceCollectionTest, RC_reorder_index_data_test)
{
    key_type = MABAIN_TEST_KEY_TYPE_SHA_256;
    ResourceCollection rc(db, RESOURCE_COLLECTION_TYPE_INDEX |
                              RESOURCE_COLLECTION_TYPE_DATA);

    long tot = 35275;
    bool *exist = new bool[tot];
    Populate(tot, exist);
    rc.ReclaimResource(0, 0);
    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete [] exist;
}

TEST_F(ResourceCollectionTest, RC_delete_odd_collect_index_test)
{
    key_type = MABAIN_TEST_KEY_TYPE_INT;
    ResourceCollection rc(db, RESOURCE_COLLECTION_TYPE_INDEX);

    long tot = 128471;
    bool *exist = new bool[tot];
    Populate(tot, exist);
    DeleteOdd(tot, exist);
    rc.ReclaimResource(0, 0);
    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete [] exist;
}

TEST_F(ResourceCollectionTest, RC_delete_random_collect_data_test)
{
    key_type = MABAIN_TEST_KEY_TYPE_SHA_128;
    ResourceCollection rc(db, RESOURCE_COLLECTION_TYPE_DATA);

    long tot = 34521;
    bool *exist = new bool[tot];
    Populate(tot, exist);
    DeleteRandom(tot, exist);
    rc.ReclaimResource(0, 0);
    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete [] exist;
}

TEST_F(ResourceCollectionTest, RC_delete_random_collect_index_data_test)
{
    key_type = MABAIN_TEST_KEY_TYPE_SHA_256;
    ResourceCollection rc(db, RESOURCE_COLLECTION_TYPE_INDEX |
                              RESOURCE_COLLECTION_TYPE_DATA);

    long tot = 34521;
    bool *exist = new bool[tot];
    Populate(tot, exist);
    DeleteRandom(tot, exist);
    rc.ReclaimResource(0, 0);
    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete [] exist;
}

TEST_F(ResourceCollectionTest, RC_delete_range_collect_index_data_test)
{
    key_type = MABAIN_TEST_KEY_TYPE_SHA_256;
    ResourceCollection rc(db, RESOURCE_COLLECTION_TYPE_INDEX |
                              RESOURCE_COLLECTION_TYPE_DATA);

    long tot = 34521;
    bool *exist = new bool[tot];
    Populate(tot, exist);
    DeleteRange(0, 23, exist);
    DeleteRange(1110, 1118, exist);
    DeleteRange(29110, 29301, exist);
    rc.ReclaimResource(0, 0);
    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete [] exist;
}

TEST_F(ResourceCollectionTest, RC_delete_random_collect_index_data_add_test)
{
    key_type = MABAIN_TEST_KEY_TYPE_SHA_128;
    ResourceCollection rc(db, RESOURCE_COLLECTION_TYPE_INDEX |
                              RESOURCE_COLLECTION_TYPE_DATA);

    long tot = 55569;
    bool *exist = new bool[tot];
    Populate(tot, exist);
    DeleteRandom(tot, exist);
    rc.ReclaimResource(0, 0);
    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    db.Close();

    db = DB(DB_DIR, CONSTS::ACCESS_MODE_WRITER, 128ULL*1024*1024, 128ULL*1024*1024);
    if(!db.is_open()) {
        std::cerr << "failed top open db\n";
        exit(0);
    }

    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    key_type = MABAIN_TEST_KEY_TYPE_INT;
    tot = 12343;
    Populate(tot, exist); 
    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete [] exist;
}

}
