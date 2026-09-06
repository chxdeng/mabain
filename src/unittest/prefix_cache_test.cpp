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

namespace mabain {

class PrefixCacheTestPeer {
public:
    static int CopyAfterOverwrite(PrefixCache& cache, const uint8_t* key,
        const PrefixCacheEntry& replacement, PrefixCacheEntry& out)
    {
        uint16_t p2 = static_cast<uint16_t>(static_cast<uint16_t>(key[0])
            | (static_cast<uint16_t>(key[1]) << 8));
        PrefixCacheEntry& slot = cache.tab2[static_cast<size_t>(p2) & cache.mask2];
        uint32_t before = PrefixCache::LoadEntryCounterForTest(slot);

        // Force a complete overwrite between the reader's pre-copy snapshot
        // and its post-copy validation.
        cache.PutAtDepth(key, 2, replacement);
        return PrefixCache::CopyStableEntryForTest(slot, before, out)
            ? 2
            : PrefixCache::UNSTABLE;
    }
};

} // namespace mabain

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

        db = new DB(MB_DIR, CONSTS::WriterOptions() | CONSTS::OPTION_PREFIX_CACHE);
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

TEST_F(PrefixCacheTest, AsyncWriterSeedsSharedCache)
{
    db->Close();
    delete db;
    db = nullptr;
    ResourcePool::getInstance().RemoveAll();

    std::string cmd = std::string("rm -f ") + MB_DIR + "_*";
    ASSERT_EQ(system(cmd.c_str()), 0);

    db = new DB(MB_DIR, CONSTS::WriterOptions()
            | CONSTS::ASYNC_WRITER_MODE | CONSTS::OPTION_PREFIX_CACHE);
    ASSERT_NE(db, nullptr);
    ASSERT_TRUE(db->is_open());

    PrefixCache* pc = db->GetDictPtr()->ActivePrefixCache();
    ASSERT_NE(pc, nullptr);

    const std::string key = "async-prefix-key";
    const std::string value = "async-value";
    ASSERT_EQ(db->Add(key, value, false), MBError::SUCCESS);

    PrefixCacheEntry out {};
    EXPECT_GT(pc->GetDepth(reinterpret_cast<const uint8_t*>(key.data()),
                  static_cast<int>(key.size()), out),
        0);

    DB cached_reader(MB_DIR,
        CONSTS::ReaderOptions() | CONSTS::OPTION_PREFIX_CACHE);
    ASSERT_TRUE(cached_reader.is_open());
    MBData data;
    ASSERT_EQ(cached_reader.Find(key, data), MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.buff), data.data_len),
        value);
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
    Dict* dict = db->GetDictPtr();
    ASSERT_NE(dict, nullptr);
    PrefixCache* pc = dict->ActivePrefixCache();
    ASSERT_NE(pc, nullptr);

    PrefixCacheEntry seed4 {};
    seed4.edge_offset = 111;
    pc->PutAtDepth(reinterpret_cast<const uint8_t*>("xy0-aaa"), 4, seed4);

    PrefixCacheEntry e {};
    int n = pc->GetDepth(reinterpret_cast<const uint8_t*>("xy0-aaa"), 7, e);
    EXPECT_EQ(n, 4);
    EXPECT_EQ(e.edge_offset, seed4.edge_offset);

    PrefixCacheEntry seed2 {};
    seed2.edge_offset = 222;
    pc->PutAtDepth(reinterpret_cast<const uint8_t*>("xy0-aaa"), 2, seed2);

    PrefixCacheEntry e2 {};
    int n2 = pc->GetDepth(reinterpret_cast<const uint8_t*>("xyQ"), 3, e2);
    EXPECT_EQ(n2, 2);
    EXPECT_EQ(e2.edge_offset, seed2.edge_offset);

    PrefixCacheEntry e3 {};
    int n3 = pc->GetDepth(reinterpret_cast<const uint8_t*>("zz"), 2, e3);
    EXPECT_EQ(n3, 0);

    PrefixCacheEntry e4 {};
    int n4 = pc->GetDepth(reinterpret_cast<const uint8_t*>("x"), 1, e4);
    EXPECT_EQ(n4, 0);
}

TEST_F(PrefixCacheTest, LongCompressedEdgeSeedsAllCanonicalDepths)
{
    const std::string key = "fixed-prefix-key";
    ASSERT_EQ(db->Add(key, key, false), MBError::SUCCESS);

    PrefixCache* pc = db->GetDictPtr()->ActivePrefixCache();
    ASSERT_NE(pc, nullptr);

    PrefixCacheEntry out {};
    EXPECT_EQ(pc->GetDepth(reinterpret_cast<const uint8_t*>(key.data()), 2, out), 2);
    EXPECT_EQ(pc->GetDepth(reinterpret_cast<const uint8_t*>(key.data()), 3, out), 3);
    EXPECT_EQ(pc->GetDepth(reinterpret_cast<const uint8_t*>(key.data()), 4, out), 4);
}

TEST_F(PrefixCacheTest, FoldedFourByteIndexDistributesFixedLeadingPrefixes)
{
    PrefixCache* pc = db->GetDictPtr()->ActivePrefixCache();
    ASSERT_NE(pc, nullptr);
    ASSERT_GT(pc->Cap4(), 0u);

    const uint8_t key1[] = { 'A', 'A', 0, 0 };
    const uint8_t key2[] = { 'A', 'A', 1, 0 };
    PrefixCacheEntry seed1 {};
    PrefixCacheEntry seed2 {};
    seed1.edge_offset = 111;
    seed2.edge_offset = 222;

    pc->PutAtDepth(key1, 4, seed1);
    pc->PutAtDepth(key2, 4, seed2);

    PrefixCacheEntry out {};
    ASSERT_EQ(pc->GetDepth(key1, 4, out), 4);
    EXPECT_EQ(out.edge_offset, seed1.edge_offset);
    ASSERT_EQ(pc->GetDepth(key2, 4, out), 4);
    EXPECT_EQ(out.edge_offset, seed2.edge_offset);
}

TEST_F(PrefixCacheTest, OverwriteBetweenCounterChecksIsRejected)
{
    Dict* dict = db->GetDictPtr();
    ASSERT_NE(dict, nullptr);
    PrefixCache* pc = dict->ActivePrefixCache();
    ASSERT_NE(pc, nullptr);

    const uint8_t key[] = { 'q', 'r' };
    PrefixCacheEntry old_entry {};
    old_entry.edge_offset = 0x1111111111111111ULL;
    memset(old_entry.edge_buff, 0x11, sizeof(old_entry.edge_buff));
    old_entry.edge_skip = 1;
    old_entry.lf_counter = 1;
    pc->PutAtDepth(key, 2, old_entry);

    PrefixCacheEntry new_entry {};
    new_entry.edge_offset = 0x2222222222222222ULL;
    memset(new_entry.edge_buff, 0x22, sizeof(new_entry.edge_buff));
    new_entry.edge_skip = 2;
    new_entry.lf_counter = 2;

    PrefixCacheEntry rejected {};
    EXPECT_EQ(PrefixCacheTestPeer::CopyAfterOverwrite(*pc, key, new_entry,
                  rejected),
        PrefixCache::UNSTABLE);

    PrefixCacheEntry stable {};
    ASSERT_EQ(pc->GetDepth(key, 2, stable), 2);
    EXPECT_EQ(stable.edge_offset, new_entry.edge_offset);
    EXPECT_EQ(memcmp(stable.edge_buff, new_entry.edge_buff,
                  sizeof(stable.edge_buff)),
        0);
    EXPECT_EQ(stable.edge_skip, new_entry.edge_skip);
    EXPECT_EQ(stable.lf_counter, 3u); // old and new origin bits are preserved
}

TEST_F(PrefixCacheTest, HierarchicalInvalidationIsScoped)
{
    PrefixCache* pc = db->GetDictPtr()->ActivePrefixCache();
    ASSERT_NE(pc, nullptr);

    const uint8_t ab[] = { 'A', 'B' };
    const uint8_t ac[] = { 'A', 'C' };
    const uint8_t zb[] = { 'Z', 'B' };
    PrefixCacheEntry seed {};
    seed.edge_offset = 123;

    pc->PutAtDepth(ab, 2, seed);
    pc->PutAtDepth(ac, 2, seed);
    pc->PutAtDepth(zb, 2, seed);

    PrefixCacheEntry out {};
    ASSERT_EQ(pc->GetDepth(ab, 2, out), 2);
    ASSERT_EQ(pc->GetDepth(ac, 2, out), 2);
    ASSERT_EQ(pc->GetDepth(zb, 2, out), 2);

    pc->InvalidatePrefix2(ab, 2);
    EXPECT_EQ(pc->GetDepth(ab, 2, out), 0);
    EXPECT_EQ(pc->GetDepth(ac, 2, out), 2);
    EXPECT_EQ(pc->GetDepth(zb, 2, out), 2);

    pc->PutAtDepth(ab, 2, seed);
    pc->InvalidateRoot('A');
    EXPECT_EQ(pc->GetDepth(ab, 2, out), 0);
    EXPECT_EQ(pc->GetDepth(ac, 2, out), 0);
    EXPECT_EQ(pc->GetDepth(zb, 2, out), 2);

    pc->PutAtDepth(ab, 2, seed);
    pc->PutAtDepth(ac, 2, seed);
    pc->InvalidateAll();
    EXPECT_EQ(pc->GetDepth(ab, 2, out), 0);
    EXPECT_EQ(pc->GetDepth(ac, 2, out), 0);
    EXPECT_EQ(pc->GetDepth(zb, 2, out), 0);
}

TEST_F(PrefixCacheTest, RemoveCannotReadValueFromReusedDataOffset)
{
    const std::string removed_key("ABCD", 4);
    const std::string replacement_key("WXYZ", 4);
    const std::string old_value("value-two", 9);
    const std::string replacement_value("new-value", 9);

    ASSERT_EQ(db->Add(removed_key, "value-one", false), MBError::SUCCESS);
    ASSERT_EQ(db->Add(removed_key, old_value, true), MBError::SUCCESS);

    DB cached_reader(MB_DIR,
        CONSTS::ReaderOptions() | CONSTS::OPTION_PREFIX_CACHE);
    ASSERT_TRUE(cached_reader.is_open());

    MBData before;
    ASSERT_EQ(cached_reader.Find(removed_key, before), MBError::SUCCESS);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(before.buff),
                  before.data_len),
        old_value);

    ASSERT_EQ(db->Remove(removed_key), MBError::SUCCESS);
    ASSERT_EQ(db->Add(replacement_key, replacement_value, false),
        MBError::SUCCESS);

    MBData removed;
    EXPECT_EQ(cached_reader.Find(removed_key, removed), MBError::NOT_EXIST);

    DB fresh_cached_reader(MB_DIR,
        CONSTS::ReaderOptions() | CONSTS::OPTION_PREFIX_CACHE);
    ASSERT_TRUE(fresh_cached_reader.is_open());
    removed.Clear();
    EXPECT_EQ(fresh_cached_reader.Find(removed_key, removed),
        MBError::NOT_EXIST);

    MBData replacement;
    ASSERT_EQ(cached_reader.Find(replacement_key, replacement),
        MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(replacement.buff),
                  replacement.data_len),
        replacement_value);
}

TEST_F(PrefixCacheTest, StructuralRemoveInvalidatesOnlyNecessaryScope)
{
    const std::string abcd = "ABCD";
    const std::string abce = "ABCE";
    const std::string aczz = "ACZZ";

    ASSERT_EQ(db->Add(abcd, abcd, false), MBError::SUCCESS);
    ASSERT_EQ(db->Add(abce, abce, false), MBError::SUCCESS);
    ASSERT_EQ(db->Add(aczz, aczz, false), MBError::SUCCESS);
    ASSERT_EQ(db->Add(abcd, abcd, true), MBError::SUCCESS);
    ASSERT_EQ(db->Add(abce, abce, true), MBError::SUCCESS);
    ASSERT_EQ(db->Add(aczz, aczz, true), MBError::SUCCESS);

    PrefixCache* pc = db->GetDictPtr()->ActivePrefixCache();
    ASSERT_NE(pc, nullptr);
    PrefixCacheEntry out {};
    ASSERT_GT(pc->GetDepth(reinterpret_cast<const uint8_t*>(abce.data()),
                  static_cast<int>(abce.size()), out),
        0);
    ASSERT_GT(pc->GetDepth(reinterpret_cast<const uint8_t*>(aczz.data()),
                  static_cast<int>(aczz.size()), out),
        0);

    ASSERT_EQ(db->Remove(abcd), MBError::SUCCESS);

    // The rebuilt AB subtree invalidates its two-byte generation. The AC
    // partition shares byte 0 but remains valid because the first child node
    // itself was not rebuilt.
    EXPECT_EQ(pc->GetDepth(reinterpret_cast<const uint8_t*>(abce.data()),
                  static_cast<int>(abce.size()), out),
        0);
    EXPECT_GT(pc->GetDepth(reinterpret_cast<const uint8_t*>(aczz.data()),
                  static_cast<int>(aczz.size()), out),
        0);

    DB cached_reader(MB_DIR,
        CONSTS::ReaderOptions() | CONSTS::OPTION_PREFIX_CACHE);
    ASSERT_TRUE(cached_reader.is_open());
    MBData data;
    EXPECT_EQ(cached_reader.Find(abcd, data), MBError::NOT_EXIST);
    data.Clear();
    EXPECT_EQ(cached_reader.Find(abce, data), MBError::SUCCESS);
    data.Clear();
    EXPECT_EQ(cached_reader.Find(aczz, data), MBError::SUCCESS);
}

TEST_F(PrefixCacheTest, ShallowStructuralRemoveInvalidatesFirstByteScope)
{
    const std::string ab = "AB";
    const std::string ac = "AC";
    const std::string zb = "ZB";

    ASSERT_EQ(db->Add(ab, ab, false), MBError::SUCCESS);
    ASSERT_EQ(db->Add(ac, ac, false), MBError::SUCCESS);
    ASSERT_EQ(db->Add(zb, zb, false), MBError::SUCCESS);

    PrefixCache* pc = db->GetDictPtr()->ActivePrefixCache();
    ASSERT_NE(pc, nullptr);
    PrefixCacheEntry seed {};
    seed.edge_offset = 123;
    pc->PutAtDepth(reinterpret_cast<const uint8_t*>(ab.data()), 2, seed);
    pc->PutAtDepth(reinterpret_cast<const uint8_t*>(ac.data()), 2, seed);
    PrefixCacheEntry out {};
    ASSERT_EQ(pc->GetDepth(reinterpret_cast<const uint8_t*>(ab.data()), 2, out), 2);
    ASSERT_EQ(pc->GetDepth(reinterpret_cast<const uint8_t*>(ac.data()), 2, out), 2);
    ASSERT_EQ(pc->GetDepth(reinterpret_cast<const uint8_t*>(zb.data()), 2, out), 2);

    ASSERT_EQ(db->Remove(ab), MBError::SUCCESS);

    EXPECT_EQ(pc->GetDepth(reinterpret_cast<const uint8_t*>(ab.data()), 2, out), 0);
    EXPECT_EQ(pc->GetDepth(reinterpret_cast<const uint8_t*>(ac.data()), 2, out), 0);
    EXPECT_EQ(pc->GetDepth(reinterpret_cast<const uint8_t*>(zb.data()), 2, out), 2);

    DB cached_reader(MB_DIR,
        CONSTS::ReaderOptions() | CONSTS::OPTION_PREFIX_CACHE);
    ASSERT_TRUE(cached_reader.is_open());
    MBData data;
    EXPECT_EQ(cached_reader.Find(ab, data), MBError::NOT_EXIST);
    data.Clear();
    EXPECT_EQ(cached_reader.Find(ac, data), MBError::SUCCESS);
    data.Clear();
    EXPECT_EQ(cached_reader.Find(zb, data), MBError::SUCCESS);
}

TEST_F(PrefixCacheTest, RemoveAllInvalidatesCachedOffsetsBeforeReuse)
{
    const std::string old_key = "ABCD";
    const std::string new_key = "WXYZ";
    const std::string old_value = "old-value";
    const std::string new_value = "new-value";

    ASSERT_EQ(db->Add(old_key, old_value, false), MBError::SUCCESS);

    DB cached_reader(MB_DIR,
        CONSTS::ReaderOptions() | CONSTS::OPTION_PREFIX_CACHE);
    ASSERT_TRUE(cached_reader.is_open());
    MBData data;
    ASSERT_EQ(cached_reader.Find(old_key, data), MBError::SUCCESS);

    ASSERT_EQ(db->RemoveAll(), MBError::SUCCESS);
    ASSERT_EQ(db->Add(new_key, new_value, false), MBError::SUCCESS);

    data.Clear();
    EXPECT_EQ(cached_reader.Find(old_key, data), MBError::NOT_EXIST);
    data.Clear();
    ASSERT_EQ(cached_reader.Find(new_key, data), MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.buff), data.data_len),
        new_value);
}

TEST_F(PrefixCacheTest, JemallocRemoveAllPreservesEmbeddedCacheRegion)
{
    db->Close();
    delete db;
    db = nullptr;
    ResourcePool::getInstance().RemoveAll();

    std::string cmd = std::string("rm -f ") + MB_DIR + "_*";
    ASSERT_EQ(system(cmd.c_str()), 0);

    constexpr uint32_t block_size = 32 * 1024 * 1024;
    MBConfig writer_config = {};
    writer_config.mbdir = MB_DIR;
    writer_config.options = CONSTS::WriterOptions()
        | CONSTS::OPTION_JEMALLOC | CONSTS::OPTION_PREFIX_CACHE;
    writer_config.block_size_index = block_size;
    writer_config.block_size_data = block_size;
    writer_config.max_num_index_block = 1;
    writer_config.max_num_data_block = 1;
    writer_config.memcap_index = block_size;
    writer_config.memcap_data = block_size;
    writer_config.num_entry_per_bucket = 500;
    writer_config.jemalloc_keep_db = true;
    db = new DB(writer_config);
    ASSERT_NE(db, nullptr);
    ASSERT_TRUE(db->is_open());
    ASSERT_NE(db->GetDictPtr()->ActivePrefixCache(), nullptr);

    const std::string old_key = "ABCD";
    const std::string new_key = "WXYZ";
    ASSERT_EQ(db->Add(old_key, "old-value", false), MBError::SUCCESS);

    MBConfig reader_config = writer_config;
    reader_config.options = CONSTS::ReaderOptions()
        | CONSTS::OPTION_JEMALLOC | CONSTS::OPTION_PREFIX_CACHE;
    DB cached_reader(reader_config);
    ASSERT_TRUE(cached_reader.is_open());
    MBData data;
    ASSERT_EQ(cached_reader.Find(old_key, data), MBError::SUCCESS);

    ASSERT_EQ(db->RemoveAll(), MBError::SUCCESS);
    ASSERT_EQ(db->Add(new_key, "new-value", false), MBError::SUCCESS);

    data.Clear();
    EXPECT_EQ(cached_reader.Find(old_key, data), MBError::NOT_EXIST);
    data.Clear();
    ASSERT_EQ(cached_reader.Find(new_key, data), MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.buff), data.data_len),
        "new-value");
}

} // namespace
