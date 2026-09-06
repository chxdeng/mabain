/**
 * Copyright (C) 2026 Cisco Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2.
 */

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>

#include <gtest/gtest.h>

#include "../db.h"
#include "../detail/search_engine.h"
#include "../dict.h"
#include "../resource_pool.h"
#include "../shm_queue_mgr.h"

using namespace mabain;

namespace {

class AsyncWriterDeadlineTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        db_dir = "/var/tmp/mabain_async_deadline_test_" + std::to_string(getpid());
        db_path = db_dir + "/";

        std::error_code ec;
        std::filesystem::remove_all(db_dir, ec);
        ec.clear();
        ASSERT_TRUE(std::filesystem::create_directories(db_dir, ec));
        ASSERT_FALSE(ec);

        MBConfig config = { 0 };
        config.mbdir = db_path.c_str();
        config.options = CONSTS::WriterOptions() | CONSTS::ASYNC_WRITER_MODE;
        config.memcap_index = 64 * 1024 * 1024LL;
        config.memcap_data = 64 * 1024 * 1024LL;
        config.queue_size = 4;
        config.queue_dir = db_dir.c_str();
        config.async_queue_reservation_timeout_sec = 2;

        db = new DB(config);
        ASSERT_TRUE(db->is_open()) << db->StatusStr();
        dict = db->GetDictPtr();
        ASSERT_NE(dict, nullptr);
        header = dict->GetHeaderPtr();
        queue = dict->GetAsyncQueuePtr();
        reservation_time_ms = dict->GetAsyncQueueReservationTimePtr();
        ASSERT_NE(header, nullptr);
        ASSERT_NE(queue, nullptr);
        ASSERT_NE(reservation_time_ms, nullptr);

        // Let the async writer enter its idle wait before injecting a gap.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    void TearDown() override
    {
        delete db;
        db = nullptr;
        ResourcePool::getInstance().RemoveAll();

        std::error_code ec;
        std::filesystem::remove_all(db_dir, ec);
    }

    void FillAdd(AsyncNode& node, const std::string& key, const std::string& value)
    {
        ASSERT_LE(key.size(), static_cast<size_t>(MB_ASYNC_SHM_KEY_SIZE));
        ASSERT_LE(value.size(), static_cast<size_t>(MB_ASYNC_SHM_DATA_SIZE));

        std::copy(key.begin(), key.end(), node.key);
        std::copy(value.begin(), value.end(), node.data);
        node.key_len = static_cast<int>(key.size());
        node.data_len = static_cast<int>(value.size());
        node.overwrite = true;
        node.type = MABAIN_ASYNC_TYPE_ADD;
    }

    bool WaitUntilReleased(const AsyncNode& node, std::chrono::milliseconds timeout)
    {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (!node.in_use.load(std::memory_order_acquire))
                return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return !node.in_use.load(std::memory_order_acquire);
    }

    bool WaitUntilReservationStarted(uint32_t slot_index,
        std::chrono::milliseconds timeout)
    {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (reservation_time_ms[slot_index].load(std::memory_order_acquire) != 0)
                return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return reservation_time_ms[slot_index].load(std::memory_order_acquire) != 0;
    }

    void ReserveFirstAndPublishSecond(AsyncNode*& first, AsyncNode*& second,
        const std::string& key, const std::string& second_value)
    {
        uint32_t base = header->queue_index.load(std::memory_order_acquire);
        first = &queue[base % header->async_queue_size];
        second = &queue[(base + 1) % header->async_queue_size];

        first->num_reader.store(1, std::memory_order_release);
        reservation_time_ms[base % header->async_queue_size].store(
            SHMQ_GetMonotonicTimeMs(), std::memory_order_release);
        second->num_reader.store(1, std::memory_order_release);
        reservation_time_ms[(base + 1) % header->async_queue_size].store(
            SHMQ_GetMonotonicTimeMs(), std::memory_order_release);
        FillAdd(*second, key, second_value);
        second->in_use.store(true, std::memory_order_release);
        header->queue_index.store(base + 2, std::memory_order_release);
        dict->SHMQ_Signal();
    }

    DB* db = nullptr;
    Dict* dict = nullptr;
    IndexHeader* header = nullptr;
    AsyncNode* queue = nullptr;
    std::atomic<uint64_t>* reservation_time_ms = nullptr;
    std::string db_dir;
    std::string db_path;
};

TEST_F(AsyncWriterDeadlineTest, UsesConfiguredReservationTimeout)
{
    MBConfig config = { 0 };
    db->GetDBConfig(config);

    EXPECT_EQ(config.async_queue_reservation_timeout_sec, 2U);
    EXPECT_EQ(CONSTS::DEFAULT_ASYNC_QUEUE_RESERVATION_TIMEOUT_SEC, 3600);
}

TEST_F(AsyncWriterDeadlineTest, PreReservationGapUsesConfiguredTimeout)
{
    const std::string key = "delayed-reservation-key";
    AsyncNode* first = nullptr;
    AsyncNode* second = nullptr;
    uint32_t base = header->queue_index.load(std::memory_order_acquire);
    first = &queue[base % header->async_queue_size];
    second = &queue[(base + 1) % header->async_queue_size];

    second->num_reader.store(1, std::memory_order_release);
    reservation_time_ms[(base + 1) % header->async_queue_size].store(
        SHMQ_GetMonotonicTimeMs(), std::memory_order_release);
    FillAdd(*second, key, "second");
    second->in_use.store(true, std::memory_order_release);
    header->queue_index.store(base + 2, std::memory_order_release);
    dict->SHMQ_Signal();

    // Anchor the delay to the writer observing the unpublished first slot.
    // Without this synchronization, late writer scheduling could let the old
    // one-second grace implementation pass without exercising its timeout.
    ASSERT_TRUE(WaitUntilReservationStarted(base % header->async_queue_size,
        std::chrono::seconds(3)));

    // This exceeds the old hard-coded one-second grace but remains below the
    // configured two-second reservation timeout.
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    EXPECT_TRUE(second->in_use.load(std::memory_order_acquire));

    first->num_reader.store(1, std::memory_order_release);
    reservation_time_ms[base % header->async_queue_size].store(
        SHMQ_GetMonotonicTimeMs(), std::memory_order_release);
    FillAdd(*first, key, "first");
    first->in_use.store(true, std::memory_order_release);
    dict->SHMQ_Signal();

    ASSERT_TRUE(WaitUntilReleased(*first, std::chrono::seconds(3)));
    ASSERT_TRUE(WaitUntilReleased(*second, std::chrono::seconds(3)));
}

TEST_F(AsyncWriterDeadlineTest, LaterSignalDoesNotSkipEarlierSlot)
{
    const std::string key = "deadline-order-key";
    const std::string first_value = "first";
    const std::string second_value = "second";
    AsyncNode* first = nullptr;
    AsyncNode* second = nullptr;

    ReserveFirstAndPublishSecond(first, second, key, second_value);

    // Repeated signals must not restart or bypass the first slot's deadline.
    for (int i = 0; i < 20; ++i) {
        dict->SHMQ_Signal();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(second->in_use.load(std::memory_order_acquire));

    FillAdd(*first, key, first_value);
    first->in_use.store(true, std::memory_order_release);
    dict->SHMQ_Signal();

    ASSERT_TRUE(WaitUntilReleased(*first, std::chrono::seconds(2)));
    ASSERT_TRUE(WaitUntilReleased(*second, std::chrono::seconds(2)));

    MBData data;
    detail::SearchEngine engine(*dict);
    ASSERT_EQ(engine.find(reinterpret_cast<const uint8_t*>(key.data()),
                  static_cast<int>(key.size()), data),
        MBError::SUCCESS);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(data.buff), data.data_len),
        second_value);
}

TEST_F(AsyncWriterDeadlineTest, UnfinishedSlotIsSkippedAfterDeadline)
{
    const std::string key = "deadline-timeout-key";
    AsyncNode* first = nullptr;
    AsyncNode* second = nullptr;

    auto start = std::chrono::steady_clock::now();
    ReserveFirstAndPublishSecond(first, second, key, "second");
    ASSERT_TRUE(WaitUntilReleased(*second, std::chrono::seconds(4)));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    EXPECT_GE(elapsed.count(), 1900);
    EXPECT_LT(elapsed.count(), 3500);
    EXPECT_FALSE(first->in_use.load(std::memory_order_acquire));
    EXPECT_EQ(first->num_reader.load(std::memory_order_acquire), 0);
    EXPECT_EQ(reservation_time_ms[first - queue].load(std::memory_order_acquire), 0U);

    uint16_t expected = 0;
    EXPECT_TRUE(first->num_reader.compare_exchange_strong(expected, 1,
        std::memory_order_acq_rel, std::memory_order_acquire));
    first->num_reader.store(0, std::memory_order_release);
}

} // namespace
