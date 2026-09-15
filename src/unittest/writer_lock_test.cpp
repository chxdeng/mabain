#include <iostream>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "../db.h"
#include "../resource_pool.h"

using namespace mabain;

namespace {

#define MB_DIR "/var/tmp/mabain_test/"

class WriterLockTest : public ::testing::Test {
public:
    WriterLockTest()
    {
    }
    virtual ~WriterLockTest()
    {
    }
    virtual void SetUp()
    {
        std::string cmd = std::string("rm ") + MB_DIR + "_*";
        if (system(cmd.c_str()) != 0) {
        }
    }
    virtual void TearDown()
    {
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

TEST_F(WriterLockTest, RawOffsetMutationRequiresWriterMode)
{
    DB writer(MB_DIR, CONSTS::WriterOptions());
    ASSERT_TRUE(writer.is_open());
    ASSERT_EQ(writer.Add("key", "value"), MBError::SUCCESS);

    MBData found;
    ASSERT_EQ(writer.Find("key", found), MBError::SUCCESS);
    const size_t payload_offset
        = found.data_offset + static_cast<size_t>(DB::GetDataHeaderSize());

    ASSERT_NE(writer.GetDataPtrByOffset(payload_offset), nullptr);
    ASSERT_EQ(writer.WriteDataByOffset(payload_offset, "V", 1),
        MBError::SUCCESS);

    MBData updated;
    ASSERT_EQ(writer.Find("key", updated), MBError::SUCCESS);
    ASSERT_EQ(updated.data_len, 5);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(updated.buff),
                  static_cast<size_t>(updated.data_len)),
        "Value");

    DB reader(MB_DIR, CONSTS::ReaderOptions());
    ASSERT_TRUE(reader.is_open());
    EXPECT_EQ(reader.WriteDataByOffset(payload_offset, "X", 1),
        MBError::NOT_ALLOWED);
    const uint8_t* reader_data = reader.GetDataPtrByOffset(payload_offset);
    ASSERT_NE(reader_data, nullptr);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(reader_data), 5),
        "Value");

    MBData unchanged;
    ASSERT_EQ(reader.Find("key", unchanged), MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(unchanged.buff),
                  static_cast<size_t>(unchanged.data_len)),
        "Value");
}

}
