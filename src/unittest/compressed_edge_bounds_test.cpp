/**
 * Regression tests for lookup queries that end inside a compressed edge.
 */

#include <filesystem>
#include <string>

#include <sys/mman.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "../db.h"
#include "../error.h"
#include "../resource_pool.h"

using namespace mabain;

namespace {

constexpr const char* kTestDir = "/var/tmp/mabain_compressed_edge_bounds_test/";

class CompressedEdgeBoundsTest : public ::testing::Test {
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

        ASSERT_EQ(db->Add("a", 1, "short", 5), MBError::SUCCESS);
        ASSERT_EQ(db->Add("abcdef", 6, "long", 4), MBError::SUCCESS);
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

// Put the two-byte query at the end of a readable page. Any attempt to compare
// bytes beyond the declared query length immediately touches the protected page.
const char* GuardedTwoByteQuery(void*& mapping, size_t& mapping_size)
{
    const long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0)
        return nullptr;

    mapping_size = static_cast<size_t>(page_size) * 2;
    mapping = mmap(nullptr, mapping_size, PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mapping == MAP_FAILED) {
        mapping = nullptr;
        return nullptr;
    }

    char* const page = static_cast<char*>(mapping);
    if (mprotect(page + page_size, static_cast<size_t>(page_size), PROT_NONE) != 0) {
        munmap(mapping, mapping_size);
        mapping = nullptr;
        return nullptr;
    }

    char* const query = page + page_size - 2;
    query[0] = 'a';
    query[1] = 'b';
    return query;
}

TEST_F(CompressedEdgeBoundsTest, ExactLookupDoesNotReadPastShortQuery)
{
    EXPECT_EXIT(
        {
            void* mapping = nullptr;
            size_t mapping_size = 0;
            const char* query = GuardedTwoByteQuery(mapping, mapping_size);
            if (query == nullptr)
                _exit(2);

            MBData data;
            const int result = db->Find(query, 2, data);
            munmap(mapping, mapping_size);
            _exit(result == MBError::NOT_EXIST ? 0 : 3);
        },
        ::testing::ExitedWithCode(0), "");
}

TEST_F(CompressedEdgeBoundsTest, LongestPrefixDoesNotReadPastShortQuery)
{
    EXPECT_EXIT(
        {
            void* mapping = nullptr;
            size_t mapping_size = 0;
            const char* query = GuardedTwoByteQuery(mapping, mapping_size);
            if (query == nullptr)
                _exit(2);

            MBData data;
            const int result = db->FindLongestPrefix(query, 2, data);
            const bool valid = result == MBError::SUCCESS
                && data.match_len == 1
                && data.data_len == 5
                && std::string(reinterpret_cast<const char*>(data.buff), data.data_len)
                    == "short";
            munmap(mapping, mapping_size);
            _exit(valid ? 0 : 3);
        },
        ::testing::ExitedWithCode(0), "");
}

TEST_F(CompressedEdgeBoundsTest, LowerBoundDoesNotReadPastShortQuery)
{
    EXPECT_EXIT(
        {
            void* mapping = nullptr;
            size_t mapping_size = 0;
            const char* query = GuardedTwoByteQuery(mapping, mapping_size);
            if (query == nullptr)
                _exit(2);

            MBData data;
            std::string bound_key;
            const int result = db->FindLowerBound(query, 2, data, &bound_key);
            const bool valid = result == MBError::SUCCESS && bound_key == "a"
                && data.data_len == 5
                && std::string(reinterpret_cast<const char*>(data.buff), data.data_len)
                    == "short";
            munmap(mapping, mapping_size);
            _exit(valid ? 0 : 3);
        },
        ::testing::ExitedWithCode(0), "");
}

TEST_F(CompressedEdgeBoundsTest, LowerBoundUsesLongerEdgeThatDivergesLower)
{
    MBData data;
    std::string bound_key;

    ASSERT_EQ(db->FindLowerBound("abz", 3, data, &bound_key),
        MBError::SUCCESS);
    EXPECT_EQ(bound_key, "abcdef");
    ASSERT_EQ(data.data_len, 4);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.buff),
                  static_cast<size_t>(data.data_len)),
        "long");
}

} // namespace
