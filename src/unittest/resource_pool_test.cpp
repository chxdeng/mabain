/**
 * Copyright (C) 2026 Cisco Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "../error.h"
#include "../resource_pool.h"

using namespace mabain;

TEST(ResourcePoolTest, CheckExistenceIsStableDuringConcurrentMutation)
{
    ResourcePool& pool = ResourcePool::getInstance();
    pool.RemoveAll();

    const std::string stable_path = "resource-pool-stable-entry";
    ASSERT_EQ(pool.AddResourceByPath(stable_path, nullptr), MBError::SUCCESS);

    std::atomic<bool> start(false);
    std::atomic<bool> done(false);
    std::atomic<bool> missing(false);
    std::atomic<uint32_t> checks(0);

    std::thread reader([&]() {
        while (!start.load(std::memory_order_acquire))
            std::this_thread::yield();

        do {
            if (!pool.CheckExistence(stable_path))
                missing.store(true, std::memory_order_relaxed);
            checks.fetch_add(1, std::memory_order_relaxed);
        } while (!done.load(std::memory_order_acquire));
    });

    std::thread mutator([&]() {
        std::vector<std::string> paths;
        paths.reserve(5000);
        start.store(true, std::memory_order_release);

        for (int i = 0; i < 5000; ++i) {
            paths.push_back("resource-pool-churn-" + std::to_string(i));
            pool.AddResourceByPath(paths.back(), nullptr);
        }
        for (const std::string& path : paths)
            pool.RemoveResourceByPath(path);

        done.store(true, std::memory_order_release);
    });

    reader.join();
    mutator.join();

    EXPECT_GT(checks.load(std::memory_order_relaxed), 0U);
    EXPECT_FALSE(missing.load(std::memory_order_relaxed));
    EXPECT_TRUE(pool.CheckExistence(stable_path));

    pool.RemoveResourceByPath(stable_path);
    EXPECT_FALSE(pool.CheckExistence(stable_path));
    pool.RemoveAll();
}
