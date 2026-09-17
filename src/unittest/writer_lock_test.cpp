#include <iostream>
#include <limits>
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

TEST_F(WriterLockTest, FailedWriterCloseKeepsActiveWriterMarker)
{
    const int options = CONSTS::WriterOptions();
    DB active_writer(MB_DIR, options);
    ASSERT_TRUE(active_writer.is_open());

    {
        DB rejected_writer(MB_DIR, options);
        ASSERT_EQ(rejected_writer.Status(), MBError::WRITER_EXIST);
    }

    DB still_rejected_writer(MB_DIR, options);
    ASSERT_EQ(still_rejected_writer.Status(), MBError::WRITER_EXIST);

    ASSERT_EQ(active_writer.Close(), MBError::SUCCESS);

    DB replacement_writer(MB_DIR, options);
    EXPECT_TRUE(replacement_writer.is_open());
}

TEST_F(WriterLockTest, AsyncWriterStatusMethodsAreSafeAfterClose)
{
    DB writer(MB_DIR, CONSTS::WriterOptions());
    ASSERT_TRUE(writer.is_open());
    EXPECT_TRUE(writer.AsyncWriterEnabled());
    EXPECT_FALSE(writer.AsyncWriterBusy());

    ASSERT_EQ(writer.Close(), MBError::SUCCESS);
    EXPECT_FALSE(writer.AsyncWriterEnabled());
    EXPECT_FALSE(writer.AsyncWriterBusy());
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

TEST_F(WriterLockTest, RawOffsetAPIsRejectInvalidInput)
{
    DB writer(MB_DIR, CONSTS::WriterOptions());
    ASSERT_TRUE(writer.is_open());

    EXPECT_EQ(writer.WriteDataByOffset(0, nullptr, 1),
        MBError::INVALID_ARG);
    EXPECT_EQ(writer.WriteDataByOffset(0, "X", 0),
        MBError::INVALID_ARG);
    EXPECT_EQ(writer.WriteDataByOffset(0, "X", -1),
        MBError::INVALID_ARG);

    MBData data;
    EXPECT_EQ(writer.ReadDataByOffset(
                  std::numeric_limits<size_t>::max(), data),
        MBError::READ_ERROR);
}

TEST_F(WriterLockTest, JemallocRawReadRejectsCrossBlockOffset)
{
    constexpr uint32_t block_size = 1024 * 1024;
    constexpr int max_blocks = 4;
    MBConfig config = {};
    config.mbdir = MB_DIR;
    config.options = CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC;
    config.block_size_index = block_size;
    config.block_size_data = block_size;
    config.max_num_index_block = max_blocks;
    config.max_num_data_block = max_blocks;
    config.memcap_index = static_cast<size_t>(block_size) * max_blocks;
    config.memcap_data = static_cast<size_t>(block_size) * max_blocks;
    config.num_entry_per_bucket = 500;

    DB writer(config);
    ASSERT_TRUE(writer.is_open());

    MBData data;
    EXPECT_EQ(writer.ReadDataByOffset(block_size - 1, data),
        MBError::READ_ERROR);
}

}
