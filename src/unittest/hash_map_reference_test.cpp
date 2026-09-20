/**
 * Reference-mode HashMap deletion and probe-chain tests.
 */

#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "../error.h"
#include "../hash_map_internal.h"
#include "../mabain_consts.h"
#include "../resource_pool.h"

using namespace mabain;

namespace mabain {

class HashMapReferenceTestAccess {
public:
    static size_t Capacity(const HashMapImpl& map)
    {
        return map.hdr_->capacity;
    }

    static size_t Home(const HashMapImpl& map, const std::string& key)
    {
        const uint64_t hash = HashMapImpl::normalize_hash(
            HashMapImpl::fnv1a64(
                reinterpret_cast<const uint8_t*>(key.data()),
                static_cast<int>(key.size())));
        return map.index_of(hash);
    }

    static uint64_t Hash(const HashMapImpl& map, const std::string& key)
    {
        (void)map;
        return HashMapImpl::normalize_hash(HashMapImpl::fnv1a64(
            reinterpret_cast<const uint8_t*>(key.data()),
            static_cast<int>(key.size())));
    }

    static uint64_t BucketHash(const HashMapImpl& map, size_t index)
    {
        return map.compact_
            ? map.bucket_compact_ptr(index)->hash.load(
                std::memory_order_relaxed)
            : map.bucket_full_ptr(index)->hash.load(
                std::memory_order_relaxed);
    }

    static uint64_t Used(const HashMapImpl& map)
    {
        return map.hdr_->used.load(std::memory_order_relaxed);
    }

    static uint64_t Tombstones(const HashMapImpl& map)
    {
        return map.hdr_->tombstones.load(std::memory_order_relaxed);
    }

    static uint64_t Generation(const HashMapImpl& map)
    {
        return map.hdr_->generation.load(std::memory_order_relaxed);
    }

    static void SetGeneration(HashMapImpl& map, uint64_t generation)
    {
        map.hdr_->generation.store(generation, std::memory_order_release);
    }

    static void SetFullBucketSequence(
        HashMapImpl& map, size_t index, uint32_t sequence)
    {
        HashMapImpl::BucketFull* bucket = map.bucket_full_ptr(index);
        const uint64_t old_meta
            = bucket->key_meta.load(std::memory_order_relaxed);
        const uint64_t new_meta = static_cast<uint32_t>(old_meta)
            | (static_cast<uint64_t>(sequence) << 32);
        bucket->key_meta.store(new_meta, std::memory_order_release);
    }
};

} // namespace mabain

namespace {

constexpr size_t kCapacity = 1024;
constexpr size_t kMemcapMb = 1;

class HashMapReferenceEraseTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        base_ = "/var/tmp/mabain_hashmap_reference_unit_"
            + std::to_string(getpid()) + "_"
            + ::testing::UnitTest::GetInstance()->current_test_info()->name();
        RemoveFiles();
    }

    void TearDown() override
    {
        ResourcePool::getInstance().RemoveAll();
        RemoveFiles();
    }

    std::string Path(bool compact) const
    {
        return base_ + (compact ? "_compact" : "_full");
    }

    std::unique_ptr<HashMapImpl> Open(bool compact)
    {
        return std::make_unique<HashMapImpl>(Path(compact), kCapacity,
            CONSTS::ACCESS_MODE_WRITER, 1, 24, kMemcapMb, compact);
    }

    void Close(std::unique_ptr<HashMapImpl>& map)
    {
        map.reset();
        ResourcePool::getInstance().RemoveAll();
    }

    static int Put(HashMapImpl& map, const std::string& key, size_t value)
    {
        return map.Put(reinterpret_cast<const uint8_t*>(key.data()),
            static_cast<int>(key.size()), value, true);
    }

    static int Erase(HashMapImpl& map, const std::string& key)
    {
        return map.Erase(reinterpret_cast<const uint8_t*>(key.data()),
            static_cast<int>(key.size()));
    }

    static bool Get(
        const HashMapImpl& map, const std::string& key, size_t& value)
    {
        return map.Get(reinterpret_cast<const uint8_t*>(key.data()),
            static_cast<int>(key.size()), value);
    }

    static std::vector<std::string> KeysForHome(
        const HashMapImpl& map, size_t home, size_t count)
    {
        std::vector<std::string> keys;
        for (size_t candidate = 0; keys.size() < count; ++candidate) {
            const std::string key = "collision-key-"
                + std::to_string(candidate);
            if (HashMapReferenceTestAccess::Home(map, key) == home)
                keys.push_back(key);
        }
        return keys;
    }

    void RemoveFiles()
    {
        ResourcePool::getInstance().RemoveAll();
        std::error_code error;
        std::filesystem::remove(Path(true) + "_hashmap0", error);
        error.clear();
        std::filesystem::remove(Path(false) + "_hashmap0", error);
    }

    std::string base_;
};

TEST_F(HashMapReferenceEraseTest, CompactsWrapAroundCollisionChain)
{
    for (bool compact : { true, false }) {
        std::unique_ptr<HashMapImpl> map = Open(compact);
        ASSERT_EQ(HashMapReferenceTestAccess::Capacity(*map), kCapacity);
        const size_t home = kCapacity - 2;
        const std::vector<std::string> keys = KeysForHome(*map, home, 6);

        for (size_t index = 0; index < keys.size(); ++index)
            ASSERT_EQ(Put(*map, keys[index], index + 100), MBError::SUCCESS);

        const uint64_t generation_before
            = HashMapReferenceTestAccess::Generation(*map);
        ASSERT_EQ(Erase(*map, keys[1]), MBError::SUCCESS);
        EXPECT_EQ(HashMapReferenceTestAccess::Generation(*map),
            generation_before + 2);
        EXPECT_EQ(HashMapReferenceTestAccess::Used(*map), 5U);
        EXPECT_EQ(HashMapReferenceTestAccess::Tombstones(*map), 0U);

        size_t value = 0;
        EXPECT_FALSE(Get(*map, keys[1], value));
        for (size_t index = 0; index < keys.size(); ++index) {
            if (index == 1)
                continue;
            ASSERT_TRUE(Get(*map, keys[index], value));
            EXPECT_EQ(value, index + 100);
        }

        const std::vector<size_t> surviving { 0, 2, 3, 4, 5 };
        for (size_t offset = 0; offset < surviving.size(); ++offset) {
            const size_t bucket = (home + offset) & (kCapacity - 1);
            EXPECT_EQ(HashMapReferenceTestAccess::BucketHash(*map, bucket),
                HashMapReferenceTestAccess::Hash(
                    *map, keys[surviving[offset]]));
        }
        EXPECT_EQ(HashMapReferenceTestAccess::BucketHash(
                      *map, (home + surviving.size()) & (kCapacity - 1)),
            0U);
        Close(map);
    }
}

TEST_F(HashMapReferenceEraseTest, DeletesFromCompletelyFullTable)
{
    for (bool compact : { true, false }) {
        std::unique_ptr<HashMapImpl> map = Open(compact);
        std::vector<std::string> keys;
        keys.reserve(kCapacity);
        for (size_t index = 0; index < kCapacity; ++index) {
            keys.push_back("full-table-key-" + std::to_string(index));
            ASSERT_EQ(Put(*map, keys.back(), index + 1), MBError::SUCCESS);
        }
        ASSERT_EQ(HashMapReferenceTestAccess::Used(*map), kCapacity);

        const size_t erased = kCapacity / 2;
        ASSERT_EQ(Erase(*map, keys[erased]), MBError::SUCCESS);
        EXPECT_EQ(HashMapReferenceTestAccess::Used(*map), kCapacity - 1);
        EXPECT_EQ(HashMapReferenceTestAccess::Tombstones(*map), 0U);

        size_t value = 0;
        EXPECT_FALSE(Get(*map, keys[erased], value));
        for (size_t index = 0; index < kCapacity; ++index) {
            if (index == erased)
                continue;
            ASSERT_TRUE(Get(*map, keys[index], value)) << index;
            EXPECT_EQ(value, index + 1) << index;
        }

        const std::string replacement = "full-table-replacement";
        ASSERT_EQ(Put(*map, replacement, 999999), MBError::SUCCESS);
        ASSERT_TRUE(Get(*map, replacement, value));
        EXPECT_EQ(value, 999999U);
        EXPECT_EQ(HashMapReferenceTestAccess::Used(*map), kCapacity);
        EXPECT_EQ(HashMapReferenceTestAccess::Tombstones(*map), 0U);
        Close(map);
    }
}

TEST_F(HashMapReferenceEraseTest, ChurnDoesNotAccumulateTombstones)
{
    constexpr size_t kLiveKeys = 700;
    constexpr size_t kRounds = 20000;

    for (bool compact : { true, false }) {
        std::unique_ptr<HashMapImpl> map = Open(compact);
        std::vector<std::string> live_keys;
        live_keys.reserve(kLiveKeys);
        for (size_t index = 0; index < kLiveKeys; ++index) {
            live_keys.push_back("churn-key-" + std::to_string(index));
            ASSERT_EQ(Put(*map, live_keys.back(), index), MBError::SUCCESS);
        }

        for (size_t round = 0; round < kRounds; ++round) {
            const size_t slot = round % kLiveKeys;
            ASSERT_EQ(Erase(*map, live_keys[slot]), MBError::SUCCESS)
                << round;
            live_keys[slot]
                = "churn-key-" + std::to_string(kLiveKeys + round);
            ASSERT_EQ(Put(*map, live_keys[slot], kLiveKeys + round),
                MBError::SUCCESS)
                << round;
        }

        EXPECT_EQ(HashMapReferenceTestAccess::Used(*map), kLiveKeys);
        EXPECT_EQ(HashMapReferenceTestAccess::Tombstones(*map), 0U);
        for (size_t slot = 0; slot < kLiveKeys; ++slot) {
            size_t value = 0;
            ASSERT_TRUE(Get(*map, live_keys[slot], value)) << slot;
        }
        Close(map);
    }
}

TEST_F(HashMapReferenceEraseTest, WriterRestartResetsInterruptedGeneration)
{
    for (bool compact : { true, false }) {
        std::unique_ptr<HashMapImpl> map = Open(compact);
        ASSERT_EQ(Put(*map, "before-restart", 11), MBError::SUCCESS);
        const uint64_t generation
            = HashMapReferenceTestAccess::Generation(*map);
        HashMapReferenceTestAccess::SetGeneration(*map, generation | 1U);
        Close(map);

        map = Open(compact);
        EXPECT_EQ(HashMapReferenceTestAccess::Used(*map), 0U);
        EXPECT_EQ(HashMapReferenceTestAccess::Tombstones(*map), 0U);
        EXPECT_EQ(HashMapReferenceTestAccess::Generation(*map) & 1U, 0U);
        ASSERT_EQ(Put(*map, "after-restart", 22), MBError::SUCCESS);
        size_t value = 0;
        ASSERT_TRUE(Get(*map, "after-restart", value));
        EXPECT_EQ(value, 22U);
        Close(map);
    }
}

TEST_F(HashMapReferenceEraseTest, ValidationFailureLeavesTableUnchanged)
{
    std::unique_ptr<HashMapImpl> map = Open(false);
    const size_t home = 17;
    const std::vector<std::string> keys = KeysForHome(*map, home, 3);
    for (size_t index = 0; index < keys.size(); ++index)
        ASSERT_EQ(Put(*map, keys[index], index + 1), MBError::SUCCESS);

    const uint64_t generation_before
        = HashMapReferenceTestAccess::Generation(*map);
    HashMapReferenceTestAccess::SetFullBucketSequence(*map,
        (home + 1) & (kCapacity - 1),
        std::numeric_limits<uint32_t>::max() - 1);
    EXPECT_EQ(Erase(*map, keys[0]), MBError::TRY_AGAIN);
    EXPECT_EQ(HashMapReferenceTestAccess::Generation(*map),
        generation_before);
    EXPECT_EQ(HashMapReferenceTestAccess::Used(*map), keys.size());
    EXPECT_EQ(HashMapReferenceTestAccess::Tombstones(*map), 0U);

    for (size_t index = 0; index < keys.size(); ++index) {
        size_t value = 0;
        ASSERT_TRUE(Get(*map, keys[index], value)) << index;
        EXPECT_EQ(value, index + 1) << index;
    }

    HashMapReferenceTestAccess::SetGeneration(
        *map, std::numeric_limits<uint64_t>::max() - 1);
    EXPECT_EQ(Erase(*map, keys[0]), MBError::NO_RESOURCE);
    EXPECT_EQ(HashMapReferenceTestAccess::Used(*map), keys.size());
    EXPECT_EQ(HashMapReferenceTestAccess::Tombstones(*map), 0U);
    Close(map);
}

} // namespace
