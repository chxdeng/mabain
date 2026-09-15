/**
 * Regression tests for FindLowerBound bound-key reconstruction.
 */

#include <filesystem>
#include <string>

#include <gtest/gtest.h>

#include "../db.h"
#include "../error.h"
#include "../resource_pool.h"

using namespace mabain;

namespace {

constexpr const char* kTestDir = "/var/tmp/mabain_lower_bound_key_test/";
constexpr const char* kStoredKey = "abcdefghij";
constexpr const char* kStoredValue = "value";

class LowerBoundKeyReconstructionTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        std::error_code error;
        std::filesystem::remove_all(kTestDir, error);
        ASSERT_FALSE(error) << error.message();
        ASSERT_TRUE(std::filesystem::create_directories(kTestDir, error));
        ASSERT_FALSE(error) << error.message();

        db = new DB(kTestDir, CONSTS::WriterOptions());
        ASSERT_NE(db, nullptr);
        ASSERT_TRUE(db->is_open()) << db->StatusStr();
        ASSERT_EQ(db->Add(kStoredKey, 10, kStoredValue, 5), MBError::SUCCESS);
    }

    void TearDown() override
    {
        if (db != nullptr) {
            db->Close();
            delete db;
            db = nullptr;
        }
        ResourcePool::getInstance().RemoveAll();
        std::error_code error;
        std::filesystem::remove_all(kTestDir, error);
        EXPECT_FALSE(error) << error.message();
    }

    DB* db = nullptr;
};

TEST_F(LowerBoundKeyReconstructionTest, ReplacesExistingBoundKeyContents)
{
    MBData data;
    std::string bound_key = "stale";

    ASSERT_EQ(db->FindLowerBound(std::string("z"), data, &bound_key), MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.buff), data.data_len), kStoredValue);
    EXPECT_EQ(bound_key, kStoredKey);
}

TEST_F(LowerBoundKeyReconstructionTest, ReadsLongEdgeOutsideMappedWindow)
{
    ASSERT_EQ(db->Close(), MBError::SUCCESS);
    delete db;
    db = nullptr;
    ResourcePool::getInstance().RemoveAll();

    // A zero index-memory cap keeps index blocks outside the mmap window while
    // ordinary RandomRead-based lookup remains supported.
    MBConfig reader_config {};
    reader_config.mbdir = kTestDir;
    reader_config.options = CONSTS::ReaderOptions();
    DB reader(reader_config);
    ASSERT_TRUE(reader.is_open()) << reader.StatusStr();

    MBData data;
    std::string bound_key;
    ASSERT_EQ(reader.FindLowerBound(std::string("z"), data, &bound_key), MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.buff), data.data_len), kStoredValue);
    EXPECT_EQ(bound_key, kStoredKey);

    EXPECT_EQ(reader.Close(), MBError::SUCCESS);
}

} // namespace
