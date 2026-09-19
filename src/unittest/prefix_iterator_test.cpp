/**
 * Regression tests for prefix iterator result filtering.
 */

#include <filesystem>
#include <set>
#include <string>

#include <gtest/gtest.h>

#include "../db.h"
#include "../error.h"
#include "../resource_pool.h"

using namespace mabain;

namespace {

constexpr const char* kTestDir = "/var/tmp/mabain_prefix_iterator_test/";

class PrefixIteratorTest : public ::testing::Test {
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

        ASSERT_EQ(db->Add("ab", "ancestor"), MBError::SUCCESS);
        ASSERT_EQ(db->Add("abcd", "exact"), MBError::SUCCESS);
        ASSERT_EQ(db->Add("abcde", "descendant"), MBError::SUCCESS);
        ASSERT_EQ(db->Add("abce", "neighbor"), MBError::SUCCESS);
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

    std::set<std::string> CollectKeys(const std::string& prefix)
    {
        std::set<std::string> keys;
        for (DB::iterator iter = db->begin(prefix); iter != db->end(); ++iter)
            keys.insert(iter.key);
        return keys;
    }

    DB* db = nullptr;
};

TEST_F(PrefixIteratorTest, ReturnsOnlyKeysBeginningWithCompletePrefix)
{
    const std::set<std::string> expected { "abcd", "abcde" };
    EXPECT_EQ(CollectKeys("abcd"), expected);
}

TEST_F(PrefixIteratorTest, EmptyPrefixReturnsEveryKey)
{
    const std::set<std::string> expected {
        "ab", "abcd", "abcde", "abce"
    };
    EXPECT_EQ(CollectKeys(""), expected);
}

} // namespace
