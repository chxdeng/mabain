/**
 * HashMap value-record lookup tests.
 */

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <unistd.h>

#include <gtest/gtest.h>

#include "../error.h"
#include "../hash_map_api.h"
#include "../hash_map_internal.h"
#include "../mabain_consts.h"
#include "../mb_data.h"
#include "../resource_pool.h"

using namespace mabain;

namespace mabain {

class HashMapCollisionTestAccess {
public:
    static uint64_t Hash(const uint8_t* key, int length)
    {
        return HashMapImpl::normalize_hash(HashMapImpl::fnv1a64(key, length));
    }
};

} // namespace mabain

namespace {

class HashMapValueLookupTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        base = "/var/tmp/mabain_hashmap_value_unit_"
            + std::to_string(getpid()) + "_"
            + ::testing::UnitTest::GetInstance()->current_test_info()->name();
        RemoveFiles();
    }

    void TearDown() override
    {
        ResourcePool::getInstance().RemoveAll();
        RemoveFiles();
    }

    HashMapValueConfig Config() const
    {
        HashMapValueConfig config;
        config.value_block_size = 4ULL * 1024ULL * 1024ULL;
        config.value_memcap = 8ULL * 1024ULL * 1024ULL;
        config.reader_slots = 16;
        config.reclaim_threshold_bytes = 64ULL * 1024ULL;
        config.max_retired_bytes = 1ULL * 1024ULL * 1024ULL;
        config.max_load_percent = 85;
        return config;
    }

    void RemoveFiles()
    {
        ResourcePool::getInstance().RemoveAll();
        const std::filesystem::path base_path(base);
        const std::filesystem::path directory = base_path.parent_path();
        const std::string prefix
            = base_path.filename().string() + "_hashmap";
        std::error_code error;
        if (!std::filesystem::exists(directory, error))
            return;
        for (const auto& entry :
            std::filesystem::directory_iterator(directory, error)) {
            if (error)
                break;
            const std::string name = entry.path().filename().string();
            if (name.compare(0, prefix.size(), prefix) == 0)
                std::filesystem::remove(entry.path(), error);
        }
    }

    std::string base;
};

TEST_F(HashMapValueLookupTest, ReadsBinaryMaximumSizedRecord)
{
    HashMap map(base, 1024, CONSTS::ACCESS_MODE_WRITER, Config(), 1);
    std::string key(static_cast<size_t>(CONSTS::MAX_KEY_LENGHTH), '\0');
    std::string value(static_cast<size_t>(CONSTS::MAX_DATA_SIZE), '\0');
    for (size_t i = 0; i < key.size(); ++i)
        key[i] = static_cast<char>((i * 17U) & 0xffU);
    for (size_t i = 0; i < value.size(); ++i)
        value[i] = static_cast<char>((i * 29U) & 0xffU);

    ASSERT_EQ(map.PutValue(reinterpret_cast<const uint8_t*>(key.data()),
                  static_cast<int>(key.size()),
                  reinterpret_cast<const uint8_t*>(value.data()),
                  static_cast<int>(value.size())),
        MBError::SUCCESS);

    MBData output;
    ASSERT_EQ(map.GetValue(reinterpret_cast<const uint8_t*>(key.data()),
                  static_cast<int>(key.size()), output),
        MBError::SUCCESS);
    ASSERT_EQ(output.data_len, static_cast<int>(value.size()));
    EXPECT_EQ(std::memcmp(output.buff, value.data(), value.size()), 0);

    std::string missing = key;
    missing[0] ^= 0x01;
    EXPECT_EQ(map.GetValue(reinterpret_cast<const uint8_t*>(missing.data()),
                  static_cast<int>(missing.size()), output),
        MBError::NOT_EXIST);
    EXPECT_EQ(output.data_len, 0);

    const std::string oversized_key = key + "x";
    EXPECT_EQ(map.GetValue(
                  reinterpret_cast<const uint8_t*>(oversized_key.data()),
                  static_cast<int>(oversized_key.size()), output),
        MBError::INVALID_ARG);
    EXPECT_EQ(output.data_len, 0);
}

TEST_F(HashMapValueLookupTest, ReadsRecordsAcrossValueBlockBoundary)
{
    // Large jemalloc size classes consume more than the requested 32,767
    // bytes. This count crosses into value block 1 while staying within the
    // two-block test memcap.
    constexpr size_t kRecordCount = 130;
    HashMap writer(base, 1024, CONSTS::ACCESS_MODE_WRITER, Config(), 1);
    std::string value(static_cast<size_t>(CONSTS::MAX_DATA_SIZE), 'v');

    for (size_t i = 0; i < kRecordCount; ++i) {
        const std::string key = "boundary-key-" + std::to_string(i);
        std::memcpy(value.data(), &i, sizeof(i));
        ASSERT_EQ(writer.PutValue(
                      reinterpret_cast<const uint8_t*>(key.data()),
                      static_cast<int>(key.size()),
                      reinterpret_cast<const uint8_t*>(value.data()),
                      static_cast<int>(value.size())),
            MBError::SUCCESS)
            << i;
    }
    ASSERT_TRUE(std::filesystem::exists(
        base + "_hashmap_values_g1_1"));

    HashMap reader(base, 1024, CONSTS::ACCESS_MODE_READER, Config(), 1);
    for (size_t i = 0; i < kRecordCount; ++i) {
        const std::string key = "boundary-key-" + std::to_string(i);
        MBData output;
        ASSERT_EQ(reader.GetValue(
                      reinterpret_cast<const uint8_t*>(key.data()),
                      static_cast<int>(key.size()), output),
            MBError::SUCCESS)
            << i;
        ASSERT_EQ(output.data_len, CONSTS::MAX_DATA_SIZE) << i;
        size_t stored_index = 0;
        std::memcpy(&stored_index, output.buff, sizeof(stored_index));
        EXPECT_EQ(stored_index, i);
    }
}

TEST_F(HashMapValueLookupTest, CollisionProbePreservesExactKeyIdentity)
{
    constexpr uint64_t kFingerprintMask = 0xFFFF000000000000ULL;
    constexpr uint64_t kHomeMask = 1023;
    std::unordered_map<uint64_t, size_t> signatures;
    size_t first_id = 0;
    size_t second_id = 0;
    bool found = false;
    for (size_t id = 0; id < 100000; ++id) {
        const std::string key = "collision-key-" + std::to_string(id);
        const uint64_t hash = HashMapCollisionTestAccess::Hash(
            reinterpret_cast<const uint8_t*>(key.data()),
            static_cast<int>(key.size()));
        const uint64_t signature
            = (hash & kFingerprintMask) | (hash & kHomeMask);
        const auto inserted = signatures.emplace(signature, id);
        if (!inserted.second) {
            first_id = inserted.first->second;
            second_id = id;
            found = true;
            break;
        }
    }
    ASSERT_TRUE(found);

    const std::string first_key
        = "collision-key-" + std::to_string(first_id);
    const std::string second_key
        = "collision-key-" + std::to_string(second_id);
    const std::string first_value = "first-value";
    const std::string second_value = "second-value";
    HashMap map(base, 1024, CONSTS::ACCESS_MODE_WRITER, Config(), 1);
    ASSERT_EQ(map.PutValue(
                  reinterpret_cast<const uint8_t*>(first_key.data()),
                  static_cast<int>(first_key.size()),
                  reinterpret_cast<const uint8_t*>(first_value.data()),
                  static_cast<int>(first_value.size())),
        MBError::SUCCESS);
    ASSERT_EQ(map.PutValue(
                  reinterpret_cast<const uint8_t*>(second_key.data()),
                  static_cast<int>(second_key.size()),
                  reinterpret_cast<const uint8_t*>(second_value.data()),
                  static_cast<int>(second_value.size())),
        MBError::SUCCESS);

    MBData output;
    ASSERT_EQ(map.GetValue(
                  reinterpret_cast<const uint8_t*>(second_key.data()),
                  static_cast<int>(second_key.size()), output),
        MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(output.buff),
                  static_cast<size_t>(output.data_len)),
        second_value);

    ASSERT_EQ(map.Erase(reinterpret_cast<const uint8_t*>(first_key.data()),
                  static_cast<int>(first_key.size())),
        MBError::SUCCESS);
    ASSERT_EQ(map.GetValue(
                  reinterpret_cast<const uint8_t*>(second_key.data()),
                  static_cast<int>(second_key.size()), output),
        MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(output.buff),
                  static_cast<size_t>(output.data_len)),
        second_value);
}

} // namespace
