#include <unistd.h>
#include <stdlib.h>
#include <list>

#include <gtest/gtest.h>

#include "../async_writer.h"
#include "../db.h"
#include "./test_key.h"
#include "../resource_pool.h"

using namespace mabain;
const char *db_dir = "/var/tmp/mabain_test/";

namespace {

class EvictionTest : public ::testing::Test
{
public:
    EvictionTest() {
        db_async = NULL;
        db = NULL;
        memset(&mbconf, 0, sizeof(mbconf));
        ResourcePool::getInstance().RemoveAll();
        std::string cmd = std::string("rm ") + db_dir + "_mabain_*";
        if(system(cmd.c_str()) != 0) {
        }
    }
    virtual ~EvictionTest() {}
    virtual void SetUp() {
        std::string cmd = std::string("mkdir -p ") + db_dir;
        if(system(cmd.c_str()) != 0) {
        }
        mbconf.mbdir = db_dir;
        mbconf.memcap_index = 64*1024*1024LL;
        mbconf.memcap_data = 64*1024*1024LL;
        mbconf.block_size_index = 8*1024*1024LL;
        mbconf.block_size_data = 16*1024*1024LL;
    }
    void OpenDB(int entry_per_bucket) {
        mbconf.num_entry_per_bucket = entry_per_bucket;
        mbconf.options = CONSTS::ACCESS_MODE_WRITER | CONSTS::USE_SLIDING_WINDOW |
                         CONSTS::ASYNC_WRITER_MODE;
        db_async = new DB(mbconf);
        assert(db_async->is_open());

        mbconf.options = CONSTS::ACCESS_MODE_READER | CONSTS::USE_SLIDING_WINDOW;
        db = new DB(mbconf);
        assert(db->is_open());
        assert(db->SetAsyncWriterPtr(db_async) == MBError::SUCCESS);
        assert(db->AsyncWriterEnabled());
    }
    void Insert(int n0, int n) {
        TestKey tkey(MABAIN_TEST_KEY_TYPE_INT);
        std::string key;
        for(int i = 0; i < n; i++) {
            key = tkey.get_key(i + n0);
            assert(db->Add(key, key) == MBError::SUCCESS);
        }
    }
    virtual void TearDown() {
        if(db != NULL) {
            db->UnsetAsyncWriterPtr(db_async);
            db->Close();
            delete db;
            db = NULL;
        }
        if(db_async != NULL) {
            db_async->Close();
            delete db_async;
            db_async = NULL;
        }
    }

protected:
    MBConfig mbconf;
    DB *db_async;
    DB *db;
};

TEST_F(EvictionTest, bucket_256_test)
{
    int entry_per_bucket = 256;
    int num = 10000;
    MBData mbd;
    int rval;
    std::string key;

    OpenDB(entry_per_bucket);
    Insert(0, num);
    TestKey tkey(MABAIN_TEST_KEY_TYPE_INT);
    int index = 0;
    for(int i = 0; i < num; i++) {
        key = tkey.get_key(i);
        assert(db->Find(key, mbd) == MBError::SUCCESS);
        EXPECT_EQ(mbd.bucket_index, index);
        if((i+1) % entry_per_bucket == 0)
            index++;
    }

    //Prune by db size
    db->CollectResource(1000000000, 1000000000, 100, 10000000000);
    while(db->AsyncWriterBusy()) {
        usleep(100);
    }
    for(int i = 0; i < num; i++) {
        key = tkey.get_key(i);
        rval = db->Find(key, mbd);
        if(i < 5*entry_per_bucket) {
            EXPECT_EQ(rval, MBError::NOT_EXIST);
        } else {
            EXPECT_EQ(rval, MBError::SUCCESS);
        }
    }

    Insert(num, 1000);
    num += 1000;
    while(db->AsyncWriterBusy()) {
        usleep(100);
    }
    //Prune by db count
    db->CollectResource(1000000000, 1000000000, 1000000000, 1000);
    while(db->AsyncWriterBusy()) {
        usleep(100);
    }
    for(int i = entry_per_bucket; i < num; i++) {
        key = tkey.get_key(i);
        rval = db->Find(key, mbd);
        if(i < 10*entry_per_bucket) {
            EXPECT_EQ(rval, MBError::NOT_EXIST);
        } else {
            EXPECT_EQ(rval, MBError::SUCCESS);
        }
    }
}

}
