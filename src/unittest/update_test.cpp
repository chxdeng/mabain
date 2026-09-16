#include <cstdlib>
#include <list>
#include <stdlib.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "../db.h"
#include "../mb_data.h"
#include "../resource_pool.h"
#include "./test_key.h"

#define MB_DIR "/var/tmp/mabain_test/"

using namespace mabain;

namespace {

class UpdateTest : public ::testing::Test {
public:
    UpdateTest()
    {
        db = NULL;
    }
    virtual ~UpdateTest()
    {
        if (db != NULL)
            delete db;
    }
    virtual void SetUp()
    {
        std::string cmd = std::string("mkdir -p ") + MB_DIR;
        if (system(cmd.c_str()) != 0) {
        }
        cmd = std::string("rm ") + MB_DIR + "_*";
        if (system(cmd.c_str()) != 0) {
        }
        db = new DB(MB_DIR, CONSTS::WriterOptions());
    }
    virtual void TearDown()
    {
        db->Close();
        ResourcePool::getInstance().RemoveAll();
    }

protected:
    DB* db;
};

TEST_F(UpdateTest, Update_all)
{
    TestKey tkey(MABAIN_TEST_KEY_TYPE_INT);
    TestKey tkey1(MABAIN_TEST_KEY_TYPE_SHA_128);
    int num = 1000;
    std::string key;
    int rval;
    for (int i = 0; i < num; i++) {
        key = tkey.get_key(i);
        rval = db->Add(key, key);
        EXPECT_EQ(rval, MBError::SUCCESS);
        key = tkey1.get_key(i);
        rval = db->Add(key, key);
        EXPECT_EQ(rval, MBError::SUCCESS);
    }
    for (int i = 0; i < num; i++) {
        key = tkey.get_key(i);
        rval = db->Add(key, key);
        EXPECT_EQ(rval, MBError::IN_DICT);
        key = tkey1.get_key(i);
        rval = db->Add(key, key);
        EXPECT_EQ(rval, MBError::IN_DICT);

        key = tkey.get_key(i);
        rval = db->Add(key, key + "_new", true);
        EXPECT_EQ(rval, MBError::SUCCESS);
        key = tkey1.get_key(i);
        rval = db->Add(key, key + "_new", true);
        EXPECT_EQ(rval, MBError::SUCCESS);
    }

    MBData mbd;
    for (int i = 0; i < num; i++) {
        key = tkey.get_key(i);
        rval = db->Find(key, mbd);
        EXPECT_EQ(rval, MBError::SUCCESS);
        EXPECT_EQ(std::string((const char*)mbd.buff, mbd.data_len) == key + "_new", true);
        key = tkey1.get_key(i);
        rval = db->Find(key, mbd);
        EXPECT_EQ(rval, MBError::SUCCESS);
        EXPECT_EQ(std::string((const char*)mbd.buff, mbd.data_len) == key + "_new", true);
    }
}

TEST_F(UpdateTest, Update_random)
{
    srand(time(NULL));
    TestKey tkey(MABAIN_TEST_KEY_TYPE_INT);
    TestKey tkey1(MABAIN_TEST_KEY_TYPE_SHA_128);
    int num = 3456;
    std::string key;
    int rval;
    bool* added;

    added = new bool[num];
    for (int i = 0; i < num; i++) {
        added[i] = false;
    }

    for (int i = 0; i < num; i++) {
        if (rand() % 100 < 21)
            continue;

        key = tkey.get_key(i);
        rval = db->Add(key, key);
        EXPECT_EQ(rval, MBError::SUCCESS);
        key = tkey1.get_key(i);
        rval = db->Add(key, key);
        EXPECT_EQ(rval, MBError::SUCCESS);
        added[i] = true;
    }
    for (int i = 0; i < num; i++) {
        if (added[i]) {
            key = tkey.get_key(i);
            rval = db->Add(key, key);
            EXPECT_EQ(rval, MBError::IN_DICT);
            key = tkey1.get_key(i);
            rval = db->Add(key, key);
            EXPECT_EQ(rval, MBError::IN_DICT);

            key = tkey.get_key(i);
            rval = db->Add(key, key + "_new", true);
            EXPECT_EQ(rval, MBError::SUCCESS);
            key = tkey1.get_key(i);
            rval = db->Add(key, key + "_new", true);
            EXPECT_EQ(rval, MBError::SUCCESS);
        } else {
            key = tkey.get_key(i);
            rval = db->Add(key, key);
            EXPECT_EQ(rval, MBError::SUCCESS);
            key = tkey1.get_key(i);
            rval = db->Add(key, key);
            EXPECT_EQ(rval, MBError::SUCCESS);
        }
    }

    MBData mbd;
    std::string value;
    for (int i = 0; i < num; i++) {
        key = tkey.get_key(i);
        rval = db->Find(key, mbd);
        EXPECT_EQ(rval, MBError::SUCCESS);
        value = key;
        if (added[i])
            value += "_new";
        EXPECT_EQ(std::string((const char*)mbd.buff, mbd.data_len) == value, true);
        key = tkey1.get_key(i);
        rval = db->Find(key, mbd);
        value = key;
        if (added[i])
            value += "_new";
        EXPECT_EQ(rval, MBError::SUCCESS);
        EXPECT_EQ(std::string((const char*)mbd.buff, mbd.data_len) == value, true);
    }

    delete[] added;
}

TEST_F(UpdateTest, LeafOverwriteDoesNotReuseLiveValueBuffer)
{
    const std::string key = "leaf-key";
    const std::string old_value(256, 'A');
    const std::string new_value(256, 'B');

    ASSERT_EQ(db->Add(key, old_value), MBError::SUCCESS);
    MBData before;
    ASSERT_EQ(db->Find(key, before), MBError::SUCCESS);
    const size_t old_offset = before.data_offset;

    ASSERT_EQ(db->Add(key, new_value, true), MBError::SUCCESS);
    MBData after;
    ASSERT_EQ(db->Find(key, after), MBError::SUCCESS);
    EXPECT_NE(after.data_offset, old_offset);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(after.buff), after.data_len),
        new_value);

    // The old buffer is released only after publication and remains reusable.
    const std::string reuse_key = "z";
    const std::string reuse_value(256, 'C');
    ASSERT_EQ(db->Add(reuse_key, reuse_value), MBError::SUCCESS);
    MBData reused;
    ASSERT_EQ(db->Find(reuse_key, reused), MBError::SUCCESS);
    EXPECT_EQ(reused.data_offset, old_offset);
}

TEST_F(UpdateTest, NodeValueOverwriteDoesNotReuseLiveValueBuffer)
{
    const std::string parent_key = "a";
    const std::string child_key = "ab";
    const std::string old_value(256, 'A');
    const std::string new_value(256, 'B');

    ASSERT_EQ(db->Add(parent_key, old_value), MBError::SUCCESS);
    ASSERT_EQ(db->Add(child_key, "child-value"), MBError::SUCCESS);

    MBData before;
    ASSERT_EQ(db->Find(parent_key, before), MBError::SUCCESS);
    const size_t old_offset = before.data_offset;

    ASSERT_EQ(db->Add(parent_key, new_value, true), MBError::SUCCESS);
    MBData after;
    ASSERT_EQ(db->Find(parent_key, after), MBError::SUCCESS);
    EXPECT_NE(after.data_offset, old_offset);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(after.buff), after.data_len),
        new_value);

    MBData child;
    ASSERT_EQ(db->Find(child_key, child), MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(child.buff), child.data_len),
        "child-value");
}

}
