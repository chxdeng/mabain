#include <string>
#include <thread>

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
    db1.Close();
    db1 = DB(MB_DIR, options);
    EXPECT_TRUE(db1.is_open());
    db1.Close();
}

}
