/**
 * Prefix cache update tests
 */

#include <cstring>
#include <string>

#include <gtest/gtest.h>

#include "../db.h"
#include "../dict.h"
#include "../resource_pool.h"
#include "../util/prefix_cache.h"

using namespace mabain;

namespace {

#define MB_DIR "/var/tmp/mabain_test/"

class PrefixCacheTest : public ::testing::Test {
public:
    PrefixCacheTest()
        : db(nullptr)
    {
    }
    ~PrefixCacheTest() override
    {
        if (db) {
            db->Close();
            delete db;
            db = nullptr;
        }
    }

    void SetUp() override
    {
        std::string cmd = std::string("mkdir -p ") + MB_DIR;
        if (system(cmd.c_str()) != 0) {
        }
        cmd = std::string("rm -f ") + MB_DIR + "_*";
        if (system(cmd.c_str()) != 0) {
        }

        db = new DB(MB_DIR, CONSTS::WriterOptions());
        ASSERT_NE(db, nullptr);
        // Prefix cache configuration is done at DB creation time.
    }

    void TearDown() override
    {
        if (db) {
            db->Close();
        }
        ResourcePool::getInstance().RemoveAll();
    }

protected:
    DB* db;
};

TEST_F(PrefixCacheTest, PutOnAdd)
{
    // Add several keys that share short prefixes to seed the cache at 2/3 bytes
    const char* keys[] = { "ab0-key", "ab1-key", "abc-key", "abcd-key", "zz-top", "za-key" };
    MBData md;
    for (const char* k : keys) {
        std::string v = std::string(k) + ":val";
        ASSERT_EQ(db->Add(k, (int)strlen(k), v.c_str(), (int)v.size(), false), MBError::SUCCESS);
    }

    Dict* dict = db->GetDictPtr();
    ASSERT_NE(dict, nullptr);

    uint64_t hit = 0, miss = 0, put = 0;
    size_t entries = 0;
    int n = 0;
    dict->GetPrefixCacheStats(hit, miss, put, entries, n);

    // Expect seeding happened during Add
    EXPECT_GT(put, 0u);
    EXPECT_GT(entries, 0u);
    EXPECT_EQ(n, 3); // shared cache supports up to 3-byte seeds
}

TEST_F(PrefixCacheTest, NoPutOnFind)
{
    // Seed via Add first
    const char* keys[] = { "pq0", "pq1", "pqr2", "pqs3" };
    MBData md;
    for (const char* k : keys) {
        ASSERT_EQ(db->Add(k, (int)strlen(k), k, (int)strlen(k), false), MBError::SUCCESS);
    }

    Dict* dict = db->GetDictPtr();
    ASSERT_NE(dict, nullptr);
    // Reset put counter; entries remain but put should stay 0 if Find doesn't seed
    dict->ResetPrefixCacheStats();

    // Perform finds via a fresh reader handle without enabling shared cache
    DB rdb(MB_DIR, CONSTS::ReaderOptions());
    for (const char* k : keys) {
        md.Clear();
        ASSERT_EQ(rdb.Find(k, (int)strlen(k), md), MBError::SUCCESS);
    }

    uint64_t hit = 0, miss = 0, put = 0;
    size_t entries = 0;
    int n = 0;
    dict->GetPrefixCacheStats(hit, miss, put, entries, n);

    EXPECT_EQ(put, 0u);
    EXPECT_GT(entries, 0u);
    EXPECT_EQ(n, 3);
}

TEST_F(PrefixCacheTest, SeedFromCache_GetDepth)
{
    // Add keys with common prefixes, seeding cache entries for 2/3-byte prefixes
    const char* keys[] = { "xy0-aaa", "xy1-bbb", "xyz-ccc", "xyzz-ddd" };
    for (const char* k : keys) {
        ASSERT_EQ(db->Add(k, (int)strlen(k), k, (int)strlen(k), false), MBError::SUCCESS);
    }

    Dict* dict = db->GetDictPtr();
    ASSERT_NE(dict, nullptr);
    PrefixCache* pc = dict->ActivePrefixCache();
    ASSERT_NE(pc, nullptr);

    PrefixCacheEntry e {};
    int n = pc->GetDepth(reinterpret_cast<const uint8_t*>("xyQ"), 3, e);
    // Should have at least a 2-byte seed hit
    EXPECT_GE(n, 2);
    EXPECT_LE(n, 3);
    EXPECT_GT(e.edge_offset, (size_t)0);

    // Unrelated prefix should miss
    PrefixCacheEntry e2 {};
    int n2 = pc->GetDepth(reinterpret_cast<const uint8_t*>("zz"), 2, e2);
    EXPECT_EQ(n2, 0);

    // Very short keys (<2) should not hit
    PrefixCacheEntry e3 {};
    int n3 = pc->GetDepth(reinterpret_cast<const uint8_t*>("x"), 1, e3);
    EXPECT_EQ(n3, 0);
}

} // namespace
