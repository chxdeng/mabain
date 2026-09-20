/**
 * Regression tests for lookup queries that end inside a compressed edge.
 */

#include <filesystem>
#include <initializer_list>
#include <string>
#include <utility>

#include <sys/mman.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "../db.h"
#include "../dict.h"
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

    void RecreateWithKeys(
        std::initializer_list<std::pair<std::string, std::string>> entries)
    {
        db->Close();
        delete db;
        db = nullptr;
        ResourcePool::getInstance().RemoveAll();

        std::error_code error;
        std::filesystem::remove_all(kTestDir, error);
        ASSERT_FALSE(error) << error.message();
        ASSERT_TRUE(std::filesystem::create_directories(kTestDir, error));
        ASSERT_FALSE(error) << error.message();

        db = new DB(kTestDir, CONSTS::WriterOptions());
        ASSERT_NE(db, nullptr);
        ASSERT_TRUE(db->is_open()) << db->StatusStr();
        for (const auto& entry : entries)
            ASSERT_EQ(db->Add(entry.first, entry.second), MBError::SUCCESS);
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

TEST_F(CompressedEdgeBoundsTest, LowerBoundPrefersShorterCurrentRootSubtree)
{
    RecreateWithKeys({ { "0z", "early" }, { "aa", "current" } });

    MBData data;
    std::string bound_key;
    ASSERT_EQ(db->FindLowerBound("abz", 3, data, &bound_key),
        MBError::SUCCESS);
    EXPECT_EQ(bound_key, "aa");
    ASSERT_EQ(data.data_len, 7);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.buff),
                  static_cast<size_t>(data.data_len)),
        "current");
}

TEST_F(CompressedEdgeBoundsTest, LowerBoundPrefersLongerRootThatDivergesLower)
{
    RecreateWithKeys({ { "0z", "early" }, { "abcd", "current" } });

    MBData data;
    std::string bound_key;
    ASSERT_EQ(db->FindLowerBound("abz", 3, data, &bound_key),
        MBError::SUCCESS);
    EXPECT_EQ(bound_key, "abcd");
    ASSERT_EQ(data.data_len, 7);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.buff),
                  static_cast<size_t>(data.data_len)),
        "current");
}

TEST_F(CompressedEdgeBoundsTest, LowerBoundRejectsLongerRootForQueryPrefix)
{
    RecreateWithKeys({ { "0z", "early" }, { "abcd", "current" } });

    MBData data;
    std::string bound_key;
    ASSERT_EQ(db->FindLowerBound("ab", 2, data, &bound_key),
        MBError::SUCCESS);
    EXPECT_EQ(bound_key, "0z");
    ASSERT_EQ(data.data_len, 5);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.buff),
                  static_cast<size_t>(data.data_len)),
        "early");
}

TEST_F(CompressedEdgeBoundsTest, LowerBoundRejectsGreaterCurrentRootSubtree)
{
    RecreateWithKeys({ { "0z", "early" }, { "ac", "current" } });

    MBData data;
    std::string bound_key;
    ASSERT_EQ(db->FindLowerBound("abz", 3, data, &bound_key),
        MBError::SUCCESS);
    EXPECT_EQ(bound_key, "0z");
    ASSERT_EQ(data.data_len, 5);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.buff),
                  static_cast<size_t>(data.data_len)),
        "early");
}

TEST_F(CompressedEdgeBoundsTest, RcPrefixWinnerPreservesBufferMetadata)
{
    Dict* dict = db->GetDictPtr();
    ASSERT_NE(dict, nullptr);
    IndexHeader* header = dict->GetHeaderPtr();
    ASSERT_NE(header, nullptr);

    const size_t rc_root_offset = dict->GetMM()->InitRootNode_RC();
    ASSERT_NE(rc_root_offset, 0U);
    header->rc_root_offset.store(rc_root_offset, MEMORY_ORDER_WRITER);

    const std::string rc_key = "ab";
    const std::string rc_value = "rc-value";
    MBData rc_data;
    rc_data.options = CONSTS::OPTION_RC_MODE;
    rc_data.buff = reinterpret_cast<uint8_t*>(
        const_cast<char*>(rc_value.data()));
    rc_data.data_len = static_cast<int>(rc_value.size());
    ASSERT_EQ(dict->Add(reinterpret_cast<const uint8_t*>(rc_key.data()),
                  static_cast<int>(rc_key.size()), rc_data, false),
        MBError::SUCCESS);

    // The main tree matches "a", but the longer "ab" match comes from the
    // resource-collection tree and must replace this preallocated buffer.
    MBData result(128, 0);
    ASSERT_EQ(db->FindLongestPrefix("abz", 3, result), MBError::SUCCESS);
    EXPECT_EQ(result.match_len, 2);
    ASSERT_EQ(result.data_len, static_cast<int>(rc_value.size()));
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(result.buff),
                  static_cast<size_t>(result.data_len)),
        rc_value);
    EXPECT_EQ(result.buff_len, static_cast<int>(rc_value.size()));
}

} // namespace
