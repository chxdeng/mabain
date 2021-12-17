#include <string>
#include <thread>
#include <iostream>

#include <gtest/gtest.h>

#include "../db.h"
#include "../resource_pool.h"

using namespace mabain;

namespace {

#define MB_DIR "/var/tmp/mabain_test/"

class WriterLockTest : public ::testing::Test
{
public:
    WriterLockTest() {
    }
    virtual ~WriterLockTest() {
    }
    virtual void SetUp() {
        std::string cmd = std::string("rm ") + MB_DIR + "_*";
        if(system(cmd.c_str()) != 0) {
        }
    }
    virtual void TearDown() {
        ResourcePool::getInstance().RemoveAll();
    }

protected:
};

TEST_F(WriterLockTest, test_lock)
{
    int options = CONSTS::WriterOptions();
    DB db(MB_DIR, options);
    EXPECT_TRUE(db.is_open());

    DB db1(MB_DIR, options);
    EXPECT_TRUE(db1.Status() == MBError::WRITER_EXIST);

    db.Close();
    DB db2(MB_DIR, options);
    EXPECT_TRUE(db2.is_open());

    options = CONSTS::ReaderOptions();
    DB db3(MB_DIR, options);
    EXPECT_TRUE(db3.is_open());

    DB db4(MB_DIR, options);
    EXPECT_TRUE(db4.is_open());

    db4 = db3;
    EXPECT_TRUE(db4.is_open());

    DB db5(db4);
    EXPECT_TRUE(db5.is_open());
}

}
