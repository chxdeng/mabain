/**
 * Copyright (C) 2026 Cisco Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2.
 */

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>

#include "../db.h"
#include "../dict.h"
#include "../resource_pool.h"
#include "../shm_queue_mgr.h"

using namespace mabain;

namespace {

constexpr uint32_t kTimeoutSec = 1;

int Fail(std::unique_ptr<DB>& db, const std::string& db_dir,
    const std::string& message, int code)
{
    std::cerr << message << std::endl;
    db.reset();
    ResourcePool::getInstance().RemoveAll();
    std::error_code ec;
    std::filesystem::remove_all(db_dir, ec);
    return code;
}

} // namespace

int main()
{
    std::string db_dir = "/var/tmp/mabain_shmq_timeout_test_" + std::to_string(getpid());
    std::string db_path = db_dir + "/";
    std::error_code ec;
    std::filesystem::remove_all(db_dir, ec);
    ec.clear();
    if (!std::filesystem::create_directories(db_dir, ec) || ec) {
        std::cerr << "failed to create test directory: " << ec.message() << std::endl;
        return 1;
    }

    MBConfig config = { 0 };
    config.mbdir = db_path.c_str();
    config.options = CONSTS::WriterOptions() | CONSTS::ASYNC_WRITER_MODE;
    config.memcap_index = 64 * 1024 * 1024LL;
    config.memcap_data = 64 * 1024 * 1024LL;
    config.queue_size = 4;
    config.queue_dir = db_dir.c_str();
    config.async_queue_reservation_timeout_sec = kTimeoutSec;

    std::unique_ptr<DB> db(new DB(config));
    if (!db->is_open())
        return Fail(db, db_dir, std::string("DB open failed: ") + db->StatusStr(), 2);

    Dict* dict = db->GetDictPtr();
    IndexHeader* header = dict == nullptr ? nullptr : dict->GetHeaderPtr();
    AsyncNode* queue = dict == nullptr ? nullptr : dict->GetAsyncQueuePtr();
    std::atomic<uint64_t>* reservation_time_ms = dict == nullptr
        ? nullptr
        : dict->GetAsyncQueueReservationTimePtr();
    if (header == nullptr || queue == nullptr || reservation_time_ms == nullptr)
        return Fail(db, db_dir, "async queue was not initialized", 3);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    uint32_t base = header->queue_index.load(std::memory_order_acquire);
    uint32_t first_index = base % header->async_queue_size;
    uint32_t second_index = (base + 1) % header->async_queue_size;
    AsyncNode& first = queue[first_index];
    AsyncNode& second = queue[second_index];

    first.num_reader.store(1, std::memory_order_release);
    reservation_time_ms[first_index].store(
        SHMQ_GetMonotonicTimeMs(), std::memory_order_release);

    const std::string key = "shmq-timeout-key";
    const std::string value = "published-after-stale-reservation";
    if (key.size() > MB_ASYNC_SHM_KEY_SIZE || value.size() > MB_ASYNC_SHM_DATA_SIZE)
        return Fail(db, db_dir, "test data exceeds queue node capacity", 4);

    second.num_reader.store(1, std::memory_order_release);
    reservation_time_ms[second_index].store(
        SHMQ_GetMonotonicTimeMs(), std::memory_order_release);
    std::copy(key.begin(), key.end(), second.key);
    std::copy(value.begin(), value.end(), second.data);
    second.key_len = static_cast<int>(key.size());
    second.data_len = static_cast<int>(value.size());
    second.overwrite = true;
    second.type = MABAIN_ASYNC_TYPE_ADD;
    second.in_use.store(true, std::memory_order_release);

    auto start = std::chrono::steady_clock::now();
    header->queue_index.store(base + 2, std::memory_order_release);
    dict->SHMQ_Signal();

    auto deadline = start + std::chrono::seconds(3);
    while (second.in_use.load(std::memory_order_acquire)
        && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    if (second.in_use.load(std::memory_order_acquire))
        return Fail(db, db_dir, "writer did not advance past the stale reservation", 5);
    if (elapsed.count() < 900 || elapsed.count() >= 2500)
        return Fail(db, db_dir, "reservation was reclaimed outside the expected window", 6);
    if (first.num_reader.load(std::memory_order_acquire) != 0)
        return Fail(db, db_dir, "stale physical slot remained reserved", 7);
    if (reservation_time_ms[first_index].load(std::memory_order_acquire) != 0)
        return Fail(db, db_dir, "stale reservation timestamp was not cleared", 8);

    uint16_t expected = 0;
    if (!first.num_reader.compare_exchange_strong(expected, 1,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        return Fail(db, db_dir, "reclaimed physical slot could not be acquired", 9);
    }
    first.num_reader.store(0, std::memory_order_release);

    db.reset();
    ResourcePool::getInstance().RemoveAll();
    std::filesystem::remove_all(db_dir, ec);
    if (ec) {
        std::cerr << "test passed, but cleanup failed: " << ec.message() << std::endl;
        return 10;
    }

    std::cout << "shmq_reservation_timeout_test passed; stale slot reclaimed after "
              << elapsed.count() << " ms" << std::endl;
    return 0;
}
