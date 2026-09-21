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

#include <iostream>
#include <openssl/sha.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>

#include <gtest/gtest.h>

#define TEST_MBSHRINK_FRIENDS \
    friend class MBShrinkTest;

#include "../db.h"
#include "../error.h"
#include "../mabain_consts.h"
#include "../mb_rc.h"
#include "../resource_pool.h"
#include "./test_key.h"

#define DB_DIR "/var/tmp/mabain_test/"
#define KEY_TYPE_INT 0
#define KEY_TYPE_SHA256 1

using namespace mabain;

namespace mabain {

class RCReplayTestPeer {
public:
    static void SetReplayBoundaries(ResourceCollection& rc,
        size_t index_offset, size_t data_offset)
    {
        rc.rc_index_offset = index_offset;
        rc.rc_data_offset = data_offset;
    }

    static int ProcessRCTree(ResourceCollection& rc)
    {
        return rc.ProcessRCTree();
    }
};

} // namespace mabain

namespace {

class ResourceCollectionTest : public ::testing::Test {
public:
    ResourceCollectionTest()
        : db(NULL)
    {
        key_type = MABAIN_TEST_KEY_TYPE_INT;
        srand(time(NULL));
    }
    virtual ~ResourceCollectionTest()
    {
        if (db != NULL)
            delete db;
    }
    virtual void SetUp()
    {
        std::string cmd = std::string("mkdir -p ") + DB_DIR;
        if (system(cmd.c_str()) != 0) {
        }
        cmd = std::string("rm ") + DB_DIR + "_*";
        if (system(cmd.c_str()) != 0) {
        }
        db = new DB(DB_DIR, CONSTS::ACCESS_MODE_WRITER, 128ULL * 1024 * 1024, 128ULL * 1024 * 1024);
        if (!db->is_open()) {
            std::cerr << "failed to open mabain db: " << db->StatusStr() << "\n";
            abort();
        }
    }
    virtual void TearDown()
    {
        db->Close();
        ResourcePool::getInstance().RemoveAll();
    }

    void Populate(long num, bool* exist)
    {
        TestKey tkey = TestKey(key_type);
        for (long i = 0; i < num; i++) {
            std::string key = tkey.get_key(i);
            std::string value = key;
            int rval = db->Add(key.c_str(), key.length(), value.c_str(), value.length());
            assert(rval == MBError::SUCCESS);
            exist[i] = true;
        }
    }

    void DeleteRandom(long num, bool* exist)
    {
        long count = 0;
        int64_t tot_count = db->Count();
        TestKey tkey = TestKey(key_type);
        while (true) {
            long ikey = rand() % tot_count;
            std::string key = tkey.get_key(ikey);
            int rval = db->Remove(key.c_str(), key.length());
            if (rval == MBError::SUCCESS) {
                exist[ikey] = false;
                count++;
                if (count >= num)
                    break;
            }
        }
    }

    void DeleteOdd(long num, bool* exist)
    {
        TestKey tkey = TestKey(key_type);
        for (long i = 0; i < num; i++) {
            if (i % 2 == 0)
                continue;
            std::string key = tkey.get_key(i);
            std::string value = key;
            db->Remove(key.c_str(), key.length());
            exist[i] = false;
        }
    }

    void DeleteRange(long start, long end, bool* exist)
    {
        TestKey tkey = TestKey(key_type);
        for (long i = start; i < end; i++) {
            std::string key = tkey.get_key(i);
            std::string value = key;
            db->Remove(key.c_str(), key.length());
            exist[i] = false;
        }
    }

    void VerifyKeyValue(long ikey, bool found)
    {
        TestKey tkey = TestKey(key_type);
        std::string key = tkey.get_key(ikey);
        std::string value = key;
        MBData mbd;
        int rval = db->Find(key.c_str(), key.length(), mbd);
        if (found) {
            EXPECT_EQ(rval, MBError::SUCCESS);
            EXPECT_TRUE(value == std::string(reinterpret_cast<char*>(mbd.buff), mbd.data_len));
        } else {
            EXPECT_EQ(rval, MBError::NOT_EXIST);
        }
    }

protected:
    DB* db;
    int key_type;
};

TEST_F(ResourceCollectionTest, RC_reorder_index_test)
{
    key_type = KEY_TYPE_INT;
    ResourceCollection rc(*db, RESOURCE_COLLECTION_TYPE_INDEX);

    long tot = 53245;
    bool* exist = new bool[tot];
    Populate(tot, exist);
    rc.ReclaimResource(0, 0, 10000000000LL, 10000000000LL);
    for (long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete[] exist;
}

TEST_F(ResourceCollectionTest, RC_reorder_data_test)
{
    key_type = MABAIN_TEST_KEY_TYPE_SHA_128;
    ResourceCollection rc(*db, RESOURCE_COLLECTION_TYPE_DATA);

    long tot = 35275;
    bool* exist = new bool[tot];
    Populate(tot, exist);
    rc.ReclaimResource(0, 0, 10000000000LL, 10000000000LL);
    for (long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete[] exist;
}

TEST_F(ResourceCollectionTest, RC_reorder_index_data_test)
{
    key_type = MABAIN_TEST_KEY_TYPE_SHA_256;
    ResourceCollection rc(*db, RESOURCE_COLLECTION_TYPE_INDEX | RESOURCE_COLLECTION_TYPE_DATA);

    long tot = 35275;
    bool* exist = new bool[tot];
    Populate(tot, exist);
    rc.ReclaimResource(0, 0, 10000000000LL, 10000000000LL);
    for (long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete[] exist;
}

TEST_F(ResourceCollectionTest, RC_delete_odd_collect_index_test)
{
    key_type = MABAIN_TEST_KEY_TYPE_INT;
    ResourceCollection rc(*db, RESOURCE_COLLECTION_TYPE_INDEX);

    long tot = 128471;
    bool* exist = new bool[tot];
    Populate(tot, exist);
    DeleteOdd(tot, exist);
    rc.ReclaimResource(0, 0, 10000000000LL, 10000000000LL);
    for (long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete[] exist;
}

TEST_F(ResourceCollectionTest, RC_delete_random_collect_data_test)
{
    key_type = MABAIN_TEST_KEY_TYPE_SHA_128;
    ResourceCollection rc(*db, RESOURCE_COLLECTION_TYPE_DATA);

    long tot = 34521;
    bool* exist = new bool[tot];
    Populate(tot, exist);
    DeleteRandom(tot, exist);
    rc.ReclaimResource(0, 0, 10000000000LL, 10000000000LL);
    for (long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete[] exist;
}

TEST_F(ResourceCollectionTest, RC_delete_random_collect_index_data_test)
{
    key_type = MABAIN_TEST_KEY_TYPE_SHA_256;
    ResourceCollection rc(*db, RESOURCE_COLLECTION_TYPE_INDEX | RESOURCE_COLLECTION_TYPE_DATA);

    long tot = 34521;
    bool* exist = new bool[tot];
    Populate(tot, exist);
    DeleteRandom(tot, exist);
    rc.ReclaimResource(0, 0, 10000000000LL, 10000000000LL);
    for (long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete[] exist;
}

TEST_F(ResourceCollectionTest, RC_delete_range_collect_index_data_test)
{
    key_type = MABAIN_TEST_KEY_TYPE_SHA_256;
    ResourceCollection rc(*db, RESOURCE_COLLECTION_TYPE_INDEX | RESOURCE_COLLECTION_TYPE_DATA);

    long tot = 34521;
    bool* exist = new bool[tot];
    Populate(tot, exist);
    DeleteRange(0, 23, exist);
    DeleteRange(1110, 1118, exist);
    DeleteRange(29110, 29301, exist);
    rc.ReclaimResource(0, 0, 10000000000LL, 10000000000LL);
    for (long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete[] exist;
}

TEST_F(ResourceCollectionTest, RC_delete_random_collect_index_data_add_test)
{
    key_type = MABAIN_TEST_KEY_TYPE_SHA_128;
    ResourceCollection rc(*db, RESOURCE_COLLECTION_TYPE_INDEX | RESOURCE_COLLECTION_TYPE_DATA);

    long tot = 55569;
    bool* exist = new bool[tot];
    Populate(tot, exist);
    DeleteRandom(tot, exist);
    rc.ReclaimResource(0, 0, 10000000000LL, 10000000000LL);
    for (long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    db->Close();
    delete db;

    db = new DB(DB_DIR, CONSTS::ACCESS_MODE_WRITER, 128ULL * 1024 * 1024, 128ULL * 1024 * 1024);
    if (!db->is_open()) {
        std::cerr << "failed top open db\n";
        exit(0);
    }

    for (long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    key_type = MABAIN_TEST_KEY_TYPE_INT;
    tot = 12343;
    Populate(tot, exist);
    for (long i = 0; i < tot; i++) {
        VerifyKeyValue(i, exist[i]);
    }

    delete[] exist;
}

TEST_F(ResourceCollectionTest, RCReplayClearsTreeOnlyAfterCompleteSuccess)
{
    Dict* dict = db->GetDictPtr();
    ASSERT_NE(dict, nullptr);
    DictMem* dmm = dict->GetMM();
    ASSERT_NE(dmm, nullptr);
    IndexHeader* header = dict->GetHeaderPtr();
    ASSERT_NE(header, nullptr);

    const size_t main_index_offset = header->m_index_offset;
    const size_t main_data_offset = header->m_data_offset;
    const size_t rc_index_offset = dmm->GetResourceCollectionOffset();
    const size_t rc_data_offset = dict->GetResourceCollectionOffset();

    header->m_index_offset = rc_index_offset;
    header->m_data_offset = rc_data_offset;
    const size_t rc_root = dmm->InitRootNode_RC();
    ASSERT_NE(rc_root, 0u);
    header->rc_root_offset.store(rc_root, MEMORY_ORDER_WRITER);

    const std::string key = "rc-replay-key";
    const std::string value = "rc-replay-value";
    MBData rc_data;
    rc_data.options = CONSTS::OPTION_RC_MODE;
    rc_data.buff = reinterpret_cast<uint8_t*>(const_cast<char*>(value.data()));
    rc_data.data_len = value.size();
    ASSERT_EQ(dict->Add(reinterpret_cast<const uint8_t*>(key.data()),
                  key.size(), rc_data, true),
        MBError::SUCCESS);
    rc_data.buff = nullptr;
    ASSERT_EQ(header->rc_count, 1);

    header->m_index_offset = main_index_offset;
    header->m_data_offset = main_data_offset;
    ResourceCollection rc(*db);
    RCReplayTestPeer::SetReplayBoundaries(
        rc, rc_index_offset, rc_data_offset);
    EXPECT_EQ(RCReplayTestPeer::ProcessRCTree(rc), MBError::SUCCESS);
    EXPECT_EQ(header->rc_root_offset.load(MEMORY_ORDER_READER), 0u);
    EXPECT_EQ(header->rc_count, 0);

    MBData found;
    ASSERT_EQ(db->Find(key, found), MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(found.buff), found.data_len),
        value);
}

TEST_F(ResourceCollectionTest, RCReplayFailureRetainsAuthoritativeTree)
{
    Dict* dict = db->GetDictPtr();
    ASSERT_NE(dict, nullptr);
    DictMem* dmm = dict->GetMM();
    ASSERT_NE(dmm, nullptr);
    IndexHeader* header = dict->GetHeaderPtr();
    ASSERT_NE(header, nullptr);

    const size_t main_index_offset = header->m_index_offset;
    const size_t main_data_offset = header->m_data_offset;
    const size_t rc_index_offset = dmm->GetResourceCollectionOffset();
    const size_t rc_data_offset = dict->GetResourceCollectionOffset();

    header->m_index_offset = rc_index_offset;
    header->m_data_offset = rc_data_offset;
    const size_t rc_root = dmm->InitRootNode_RC();
    ASSERT_NE(rc_root, 0u);
    header->rc_root_offset.store(rc_root, MEMORY_ORDER_WRITER);

    const std::string key = "retained-rc-key";
    const std::string value = "retained-rc-value";
    MBData rc_data;
    rc_data.options = CONSTS::OPTION_RC_MODE;
    rc_data.buff = reinterpret_cast<uint8_t*>(const_cast<char*>(value.data()));
    rc_data.data_len = value.size();
    ASSERT_EQ(dict->Add(reinterpret_cast<const uint8_t*>(key.data()),
                  key.size(), rc_data, true),
        MBError::SUCCESS);
    rc_data.buff = nullptr;
    ASSERT_EQ(header->rc_count, 1);

    header->m_index_offset = main_index_offset;
    header->m_data_offset = main_data_offset;
    ResourceCollection rc(*db);
    RCReplayTestPeer::SetReplayBoundaries(
        rc, rc_index_offset, main_data_offset);
    EXPECT_EQ(RCReplayTestPeer::ProcessRCTree(rc), MBError::OUT_OF_BOUND);
    EXPECT_EQ(header->rc_root_offset.load(MEMORY_ORDER_READER), rc_root);
    EXPECT_EQ(header->rc_count, 1);

    MBData found;
    ASSERT_EQ(db->Find(key, found), MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(found.buff), found.data_len),
        value);

    // The replay guard must be removed on failure so ordinary allocations are
    // not constrained after the caller handles the RC error.
    EXPECT_EQ(db->Add("main-key", "main-value"), MBError::SUCCESS);

    header->rc_root_offset.store(0, MEMORY_ORDER_WRITER);
    header->rc_count = 0;
    header->rc_flag.store(ASYNC_RC_IDLE, std::memory_order_release);
    dmm->ClearRootEdges_RC();
}

TEST_F(ResourceCollectionTest, FailedRCRejectsNewQueueReservations)
{
    Dict* dict = db->GetDictPtr();
    ASSERT_NE(dict, nullptr);
    IndexHeader* header = dict->GetHeaderPtr();
    ASSERT_NE(header, nullptr);

    header->rc_flag.store(ASYNC_RC_FAILED, std::memory_order_release);
    EXPECT_EQ(dict->SHMQ_Add("key", 3, "value", 5, true),
        MBError::NO_RESOURCE);
    header->rc_flag.store(ASYNC_RC_IDLE, std::memory_order_release);
}

TEST_F(ResourceCollectionTest, FailedRCReplayCompletesAfterReopen)
{
    Dict* dict = db->GetDictPtr();
    ASSERT_NE(dict, nullptr);
    DictMem* dmm = dict->GetMM();
    ASSERT_NE(dmm, nullptr);
    IndexHeader* header = dict->GetHeaderPtr();
    ASSERT_NE(header, nullptr);

    const size_t main_index_offset = header->m_index_offset;
    const size_t main_data_offset = header->m_data_offset;
    const size_t rc_index_offset = dmm->GetResourceCollectionOffset();
    const size_t rc_data_offset = dict->GetResourceCollectionOffset();

    header->m_index_offset = rc_index_offset;
    header->m_data_offset = rc_data_offset;
    const size_t rc_root = dmm->InitRootNode_RC();
    ASSERT_NE(rc_root, 0u);
    header->rc_root_offset.store(rc_root, MEMORY_ORDER_WRITER);

    const std::string key = "retained-across-reopen";
    const std::string value = "replayed-after-reopen";
    MBData rc_data;
    rc_data.options = CONSTS::OPTION_RC_MODE;
    rc_data.buff = reinterpret_cast<uint8_t*>(const_cast<char*>(value.data()));
    rc_data.data_len = value.size();
    ASSERT_EQ(dict->Add(reinterpret_cast<const uint8_t*>(key.data()),
                  key.size(), rc_data, true),
        MBError::SUCCESS);
    rc_data.buff = nullptr;
    ASSERT_EQ(header->rc_count, 1);

    header->m_index_offset = main_index_offset;
    header->m_data_offset = main_data_offset;
    ResourceCollection rc(*db);
    RCReplayTestPeer::SetReplayBoundaries(
        rc, rc_index_offset, main_data_offset);
    ASSERT_EQ(RCReplayTestPeer::ProcessRCTree(rc), MBError::OUT_OF_BOUND);
    ASSERT_EQ(header->rc_flag.load(std::memory_order_acquire),
        ASYNC_RC_FAILED);
    header->rc_m_index_off_pre = main_index_offset;
    header->rc_m_data_off_pre = main_data_offset;

    ASSERT_EQ(db->Close(), MBError::SUCCESS);
    delete db;
    db = nullptr;
    ResourcePool::getInstance().RemoveAll();

    db = new DB(DB_DIR,
        CONSTS::ACCESS_MODE_WRITER | CONSTS::ASYNC_WRITER_MODE,
        128ULL * 1024 * 1024, 128ULL * 1024 * 1024);
    ASSERT_TRUE(db->is_open()) << db->StatusStr();

    dict = db->GetDictPtr();
    ASSERT_NE(dict, nullptr);
    header = dict->GetHeaderPtr();
    ASSERT_NE(header, nullptr);

    for (int retry = 0;
         retry < 5000
         && header->rc_flag.load(std::memory_order_acquire)
             != ASYNC_RC_IDLE;
         ++retry) {
        usleep(1000);
    }

    EXPECT_EQ(header->rc_flag.load(std::memory_order_acquire),
        ASYNC_RC_IDLE);
    EXPECT_EQ(header->rc_root_offset.load(MEMORY_ORDER_READER), 0u);
    EXPECT_EQ(header->rc_count, 0);
    EXPECT_EQ(header->rc_m_index_off_pre, 0u);
    EXPECT_EQ(header->rc_m_data_off_pre, 0u);

    DB reader(DB_DIR, CONSTS::ACCESS_MODE_READER,
        128ULL * 1024 * 1024, 128ULL * 1024 * 1024);
    ASSERT_TRUE(reader.is_open()) << reader.StatusStr();
    MBData found;
    ASSERT_EQ(reader.Find(key, found), MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(found.buff), found.data_len),
        value);
}

}
