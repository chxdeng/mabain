/**
 * HashMap value-record lookup tests.
 */

#include <atomic>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unistd.h>
#include <vector>

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

class HashMapValueTestAccess {
public:
    static int HoldCurrentView(HashMapImpl& map, std::atomic<bool>& ready,
        const std::atomic<bool>& release)
    {
        return map.hold_value_view_for_test(ready, release);
    }

    static size_t RetiredViewCount(HashMapImpl& map)
    {
        return map.retired_value_view_count_for_test();
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
    output.data_ptr = output.buff;
    EXPECT_EQ(map.GetValue(reinterpret_cast<const uint8_t*>(missing.data()),
                  static_cast<int>(missing.size()), output),
        MBError::NOT_EXIST);
    EXPECT_EQ(output.data_len, 0);
    EXPECT_EQ(output.data_ptr, nullptr);

    const std::string oversized_key = key + "x";
    output.data_ptr = output.buff;
    EXPECT_EQ(map.GetValue(
                  reinterpret_cast<const uint8_t*>(oversized_key.data()),
                  static_cast<int>(oversized_key.size()), output),
        MBError::INVALID_ARG);
    EXPECT_EQ(output.data_len, 0);
    EXPECT_EQ(output.data_ptr, nullptr);
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

TEST_F(HashMapValueLookupTest, LongLivedReaderCrossesWriterGenerations)
{
    constexpr size_t kGenerations = 32;
    const std::string key = "generation-transition-key";
    const HashMapValueConfig config = Config();

    {
        HashMap writer(base, 1024, CONSTS::ACCESS_MODE_WRITER, config, 1);
        const std::string value = "generation-0";
        ASSERT_EQ(writer.PutValue(
                      reinterpret_cast<const uint8_t*>(key.data()),
                      static_cast<int>(key.size()),
                      reinterpret_cast<const uint8_t*>(value.data()),
                      static_cast<int>(value.size())),
            MBError::SUCCESS);
    }

    HashMapImpl reader(base, 1024, CONSTS::ACCESS_MODE_READER, config, 1);
    for (size_t generation = 1; generation < kGenerations; ++generation) {
        const std::string expected
            = "generation-" + std::to_string(generation);

        std::atomic<bool> view_held { false };
        std::atomic<bool> release_view { false };
        std::atomic<int> hold_result { MBError::TRY_AGAIN };
        std::thread holder([&]() {
            hold_result.store(HashMapValueTestAccess::HoldCurrentView(
                                  reader, view_held, release_view),
                std::memory_order_release);
        });
        while (!view_held.load(std::memory_order_acquire))
            std::this_thread::yield();

        int write_result = MBError::SUCCESS;
        try {
            HashMap writer(
                base, 1024, CONSTS::ACCESS_MODE_WRITER, config, 1);
            write_result = writer.PutValue(
                reinterpret_cast<const uint8_t*>(key.data()),
                static_cast<int>(key.size()),
                reinterpret_cast<const uint8_t*>(expected.data()),
                static_cast<int>(expected.size()));
        } catch (int error) {
            write_result = error;
        }

        MBData output;
        const int lookup_result = reader.GetValue(
            reinterpret_cast<const uint8_t*>(key.data()),
            static_cast<int>(key.size()), output);
        const size_t held_retired_views
            = HashMapValueTestAccess::RetiredViewCount(reader);

        release_view.store(true, std::memory_order_release);
        holder.join();
        const size_t quiescent_retired_views
            = HashMapValueTestAccess::RetiredViewCount(reader);

        ASSERT_EQ(hold_result.load(std::memory_order_acquire), MBError::SUCCESS)
            << generation;
        ASSERT_EQ(write_result, MBError::SUCCESS) << generation;
        ASSERT_EQ(lookup_result, MBError::SUCCESS) << generation;
        ASSERT_EQ(output.data_len, static_cast<int>(expected.size()))
            << generation;
        ASSERT_EQ(std::memcmp(output.buff, expected.data(), expected.size()), 0)
            << generation;
        ASSERT_EQ(held_retired_views, 1U) << generation;
        ASSERT_EQ(quiescent_retired_views, 0U) << generation;
    }
}

TEST_F(HashMapValueLookupTest, ThreadExitCanRaceMapDestruction)
{
    constexpr size_t kIterations = 64;
    constexpr size_t kReaderThreads = 16;
    const std::string key = "thread-exit-race-key";
    const std::string expected = "thread-exit-race-value";
    HashMapValueConfig config = Config();
    config.reader_slots = kReaderThreads;

    for (size_t iteration = 0; iteration < kIterations; ++iteration) {
        RemoveFiles();
        auto map = std::make_unique<HashMap>(base, 1024,
            CONSTS::ACCESS_MODE_WRITER, config, 1);
        ASSERT_EQ(map->PutValue(
                      reinterpret_cast<const uint8_t*>(key.data()),
                      static_cast<int>(key.size()),
                      reinterpret_cast<const uint8_t*>(expected.data()),
                      static_cast<int>(expected.size())),
            MBError::SUCCESS);

        HashMap* const lookup_map = map.get();
        std::atomic<size_t> ready { 0 };
        std::atomic<bool> start_destruction { false };
        std::atomic<bool> failed { false };
        std::vector<std::thread> readers;
        readers.reserve(kReaderThreads);
        for (size_t reader = 0; reader < kReaderThreads; ++reader) {
            readers.emplace_back([&]() {
                MBData output;
                const int result = lookup_map->GetValue(
                    reinterpret_cast<const uint8_t*>(key.data()),
                    static_cast<int>(key.size()), output);
                if (result != MBError::SUCCESS
                    || output.data_len != static_cast<int>(expected.size())
                    || std::memcmp(output.buff, expected.data(),
                           expected.size())
                        != 0) {
                    failed.store(true, std::memory_order_relaxed);
                }
                ready.fetch_add(1, std::memory_order_release);
                while (!start_destruction.load(std::memory_order_acquire))
                    std::this_thread::yield();
                // Returning destroys this thread's slot cache while another
                // thread destroys the map that issued its cleanup token.
            });
        }

        while (ready.load(std::memory_order_acquire) != kReaderThreads)
            std::this_thread::yield();
        std::thread destroyer([&]() {
            while (!start_destruction.load(std::memory_order_acquire))
                std::this_thread::yield();
            map.reset();
        });
        start_destruction.store(true, std::memory_order_release);
        for (std::thread& reader : readers)
            reader.join();
        destroyer.join();
        ASSERT_FALSE(failed.load(std::memory_order_relaxed)) << iteration;
    }
}

} // namespace
