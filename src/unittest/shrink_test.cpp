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

#include <gtest/gtest.h>

#define TEST_MBSHRINK_FRIENDS \
    friend class MBShrinkTest;

#include "../db.h"
#include "../mabain_consts.h"
#include "../mb_shrink.h"
#include "../error.h"

#define DB_DIR "/var/tmp/mabain_test/"
#define KEY_TYPE_INT    0
#define KEY_TYPE_SHA256 1

using namespace mabain;

namespace {

static char sha256_str[65];
static const char* get_sha256_str(int key)
{
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, (unsigned char*)&key, 4);
    SHA256_Final(hash, &sha256);
    int i = 0;
    for(i = 0; i < SHA256_DIGEST_LENGTH; i++)
    {
        sprintf(sha256_str + (i * 2), "%02x", hash[i]);
    }
    sha256_str[64] = 0;
    return (const char*)sha256_str;
}

class MBShrinkTest : public ::testing::Test
{
public:
    MBShrinkTest() : db(DB(DB_DIR, CONSTS::ACCESS_MODE_WRITER)) {
        if(!db.is_open()) {
            std::cerr << "failed to open mabain db: " << db.StatusStr() << "\n";
            abort();
        }

        //key_type = KEY_TYPE_SHA256;
        key_type = KEY_TYPE_INT;
        srand(time(NULL));
    }
    virtual ~MBShrinkTest() {
        db.Close();
    }
    virtual void SetUp() {
        db.RemoveAll();
    }
    virtual void TearDown() {
    }

    void Populate(long num, bool *exist) {
        for(long i = 0; i < num; i++) {
            std::string key;
            if(key_type == KEY_TYPE_SHA256) {
                key = get_sha256_str(i);
            } else {
                key = "key" + std::to_string(i);
            }
            std::string value = "value" + std::to_string(i);
            int rval = db.Add(key.c_str(), key.length(), value.c_str(), value.length());
            assert(rval == MBError::SUCCESS);
            exist[i] = true;
        }
    }

    void DeleteRandom(long num, bool *exist) {
        long count = 0;
        int64_t tot_count = db.Count();
        while(true) {
            long ikey = rand() % tot_count;
            std::string key;
            if(key_type == KEY_TYPE_SHA256) {
                key = get_sha256_str(ikey);
            } else {
                key = "key" + std::to_string(ikey);
            }
            int rval = db.Remove(key.c_str(), key.length());
            if(rval == MBError::SUCCESS) {
                exist[ikey] = false;
                count++;
                if(count >= num) break;
            }
        }
    }

    void DeleteOdd(long num,  bool *exist) {
        for(long i = 0; i < num; i++) {
            if(i % 2 == 0) continue;
            std::string key;
            if(key_type == KEY_TYPE_SHA256) {
                key = get_sha256_str(i);
            } else {
                key = "key" + std::to_string(i);
            }
            std::string value = "value" + std::to_string(i);
            db.Remove(key.c_str(), key.length());
            exist[i] = false;
        }
    }

   void DeleteRange(long start, long end, bool *exist) {
       for(long i = start; i < end; i++) {
            std::string key;
            if(key_type == KEY_TYPE_SHA256) {
                key = get_sha256_str(i);
            } else {
                key = "key" + std::to_string(i);
            }
            std::string value = "value" + std::to_string(i);
            db.Remove(key.c_str(), key.length());
            exist[i] = false;
       }
   }

   void VerifyKeyValue(long ikey, bool found) {
       std::string key;
       if(key_type == KEY_TYPE_SHA256) {
           key = get_sha256_str(ikey);
       } else {
          key = "key" + std::to_string(ikey);
       }
       std::string value = "value" + std::to_string(ikey);
       MBData mbd;
       int rval = db.Find(key.c_str(), key.length(), mbd);
       if(found) {
           EXPECT_EQ(rval, MBError::SUCCESS);
           //check value
           EXPECT_TRUE(value == std::string(reinterpret_cast<char*>(mbd.buff), mbd.data_len));
       } else {
           EXPECT_EQ(rval, MBError::NOT_EXIST);
       }
   }

protected:
    DB db;
    int key_type;
};

TEST_F(MBShrinkTest, DataShrink_delete_odd_test)
{
    MBShrink mbs(db);

    long tot = 10000;
    bool *exist = new bool[tot];
    Populate(tot, exist);

    std::cout << "=============================\n";
    std::cout << "DB stats before removing\n";
    db.PrintStats();
    DeleteOdd(tot, exist);
    std::cout << "=============================\n";
    std::cout << "DB stats after removing\n";
    db.PrintStats();

    mbs.BuildDataLink();
    mbs.ScanData();
    std::cout << "=============================\n";
    std::cout << "DB stats after shrinking\n";
    db.PrintStats();

    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete [] exist;
}

TEST_F(MBShrinkTest, IndexShrink_delete_odd_test)
{
    MBShrink mbs(db);

    long tot = 100000;
    bool *exist = new bool[tot];
    Populate(tot, exist);

    std::cout << "=============================\n";
    std::cout << "DB stats before removing\n";
    db.PrintStats();
    DeleteOdd(tot, exist);
    std::cout << "=============================\n";
    std::cout << "DB stats after removing\n";
    db.PrintStats();

    mbs.BuildIndexLink();
    mbs.ScanDictMem();
    std::cout << "=============================\n";
    std::cout << "DB stats after shrinking\n";
    db.PrintStats();

    // check
    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete [] exist;
}

TEST_F(MBShrinkTest, IndexShrink_delete_random_test)
{
    MBShrink mbs(db);
    long tot = 600030;
    bool *exist = new bool[tot];
    Populate(tot, exist);

    std::cout << "=============================\n";
    std::cout << "DB stats before removing\n";
    db.PrintStats();
    DeleteRandom(long(tot*0.52111), exist);
    std::cout << "=============================\n";
    std::cout << "DB stats after removing\n";
    db.PrintStats();

    int rval;
    try {
        rval = mbs.ShrinkIndex();
    } catch (int err) {
        std::cerr << "caught error: " << MBError::get_error_str(err) << "\n";
        rval = err;
    }
    EXPECT_EQ(MBError::SUCCESS, rval);
    std::cout << "=============================\n";
    std::cout << "DB stats after shrinking\n";
    db.PrintStats();
    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete [] exist;
}

TEST_F(MBShrinkTest, DataShrink_delete_random_test)
{
    MBShrink mbs(db);
    long tot = 540032;
    bool *exist = new bool[tot];
    Populate(tot, exist);

    std::cout << "=============================\n";
    std::cout << "DB stats before removing\n";
    db.PrintStats();
    DeleteRandom(long(tot*0.52111), exist);
    std::cout << "=============================\n";
    std::cout << "DB stats after removing\n";
    db.PrintStats();

    int rval;
    try {
        rval = mbs.ShrinkData();
    } catch (int err) {
        std::cout << "caught error in ShrinkData: " << MBError::get_error_str(err) << "\n";
        rval = err;
    }
    EXPECT_EQ(MBError::SUCCESS, rval);
    std::cout << "=============================\n";
    std::cout << "DB stats after shrinking\n";
    db.PrintStats();
    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete [] exist;
}

TEST_F(MBShrinkTest, Shrink_delete_random_test)
{
    MBShrink mbs(db);
    long tot = 700034;
    bool *exist = new bool[tot];
    Populate(tot, exist);

    std::cout << "=============================\n";
    std::cout << "DB stats before removing\n";
    db.PrintStats();
    DeleteRandom(long(tot*0.62111), exist);
    std::cout << "=============================\n";
    std::cout << "DB stats after removing\n";
    db.PrintStats();

    int rval;
    rval = mbs.Shrink(100*1024*1024LL, 100*1024*1024);
    EXPECT_EQ(MBError::SUCCESS, rval);
    std::cout << "=============================\n";
    std::cout << "DB stats after shrinking\n";
    db.PrintStats();
    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete [] exist;
}

TEST_F(MBShrinkTest, Shrink_delete_range_test)
{
    MBShrink mbs(db);
    long tot = 700034;
    bool *exist = new bool[tot];
    Populate(tot, exist);

    std::cout << "=============================\n";
    std::cout << "DB stats before removing\n";
    db.PrintStats();
    DeleteRange(long(700034*0.11222), long(700034*0.73333333333), exist);
    std::cout << "=============================\n";
    std::cout << "DB stats after removing\n";
    db.PrintStats();

    int rval;
    rval = mbs.Shrink();
    EXPECT_EQ(MBError::SUCCESS, rval);
    std::cout << "=============================\n";
    std::cout << "DB stats after shrinking\n";
    db.PrintStats();
    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete [] exist;
}

TEST_F(MBShrinkTest, Shrink_sha256_delete_random_test)
{
    key_type = KEY_TYPE_SHA256;

    MBShrink mbs(db);
    long tot = 1500000;
    bool *exist = new bool[tot];
    Populate(tot, exist);

    std::cout << "=============================\n";
    std::cout << "DB stats before removing\n";
    db.PrintStats();
    DeleteRandom(long(tot*0.72111), exist);
    std::cout << "=============================\n";
    std::cout << "DB stats after removing\n";
    db.PrintStats();

    int rval;
    rval = mbs.Shrink();
    EXPECT_EQ(MBError::SUCCESS, rval);
    std::cout << "=============================\n";
    std::cout << "DB stats after shrinking\n";
    db.PrintStats();
    for(long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete [] exist;
}

}
