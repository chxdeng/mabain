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

#include <filesystem>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "../db.h"
#include "../dict.h"
#include "../drm_base.h"
#include "../error.h"
#include "../mabain_consts.h"
#include "../resource_pool.h"
#include "../shm_queue_mgr.h"

using namespace mabain;

namespace {

const char* const ASYNC_INPUT_TEST_DIR = "/var/tmp/mabain_async_input_validation_test";

class AsyncInputValidationTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        ResourcePool::getInstance().RemoveAll();
        std::error_code ec;
        std::filesystem::remove_all(ASYNC_INPUT_TEST_DIR, ec);
        ASSERT_FALSE(ec);
        std::filesystem::create_directories(ASYNC_INPUT_TEST_DIR, ec);
        ASSERT_FALSE(ec);

        db = std::make_unique<DB>(ASYNC_INPUT_TEST_DIR, CONSTS::WriterOptions());
        ASSERT_TRUE(db->is_open());
        dict = db->GetDictPtr();
        ASSERT_NE(dict, nullptr);
        header = dict->GetHeaderPtr();
        ASSERT_NE(header, nullptr);
    }

    void TearDown() override
    {
        db.reset();
        ResourcePool::getInstance().RemoveAll();
        std::error_code ec;
        std::filesystem::remove_all(ASYNC_INPUT_TEST_DIR, ec);
        EXPECT_FALSE(ec);
    }

    std::unique_ptr<DB> db;
    Dict* dict = nullptr;
    IndexHeader* header = nullptr;
};

TEST_F(AsyncInputValidationTest, PublicAddRejectsInvalidPointersAndLengths)
{
    const char key[] = "key";
    const char data[] = "data";
    const uint32_t queue_index = header->queue_index.load(std::memory_order_acquire);

    EXPECT_EQ(db->AddAsync(nullptr, 1, data, 1), MBError::INVALID_ARG);
    EXPECT_EQ(db->AddAsync(key, 1, nullptr, 1), MBError::INVALID_ARG);
    EXPECT_EQ(db->AddAsync(key, -1, data, 1), MBError::OUT_OF_BOUND);
    EXPECT_EQ(db->AddAsync(key, 1, data, -1), MBError::OUT_OF_BOUND);
    EXPECT_EQ(db->AddAsync(key, 0, data, 1), MBError::OUT_OF_BOUND);
    EXPECT_EQ(db->AddAsync(key, 1, data, 0), MBError::OUT_OF_BOUND);
    EXPECT_EQ(db->AddAsync(key, CONSTS::MAX_KEY_LENGHTH + 1, data, 1),
        MBError::OUT_OF_BOUND);
    EXPECT_EQ(db->AddAsync(key, 1, data, CONSTS::MAX_DATA_SIZE + 1),
        MBError::OUT_OF_BOUND);

    EXPECT_EQ(header->queue_index.load(std::memory_order_acquire), queue_index);
}

TEST_F(AsyncInputValidationTest, RadixKeyLengthBoundary)
{
    const std::string max_key(
        static_cast<size_t>(CONSTS::MAX_KEY_LENGHTH - 1), 'a');
    const std::string rejected_key(
        static_cast<size_t>(CONSTS::MAX_KEY_LENGHTH), 'b');
    const std::string value("v");

    ASSERT_EQ(db->Add(max_key, value), MBError::SUCCESS);
    MBData found;
    ASSERT_EQ(db->Find(max_key, found), MBError::SUCCESS);
    ASSERT_EQ(found.data_len, 1);
    EXPECT_EQ(found.buff[0], static_cast<uint8_t>('v'));
    ASSERT_EQ(dict->Count(), 1);

    EXPECT_EQ(db->Add(rejected_key, value), MBError::OUT_OF_BOUND);
    EXPECT_EQ(dict->Count(), 1);

    uint8_t value_byte = 'v';
    MBData direct_data;
    direct_data.buff = &value_byte;
    direct_data.data_len = 1;
    EXPECT_EQ(dict->Add(
                  reinterpret_cast<const uint8_t*>(rejected_key.data()),
                  static_cast<int>(rejected_key.size()), direct_data, false),
        MBError::OUT_OF_BOUND);
    direct_data.buff = nullptr;
    EXPECT_EQ(dict->Count(), 1);
}

TEST_F(AsyncInputValidationTest, QueueBoundaryRejectsInvalidInputBeforeReservation)
{
    const char key[] = "key";
    const char data[] = "data";
    const uint32_t queue_index = header->queue_index.load(std::memory_order_acquire);

    EXPECT_EQ(dict->SHMQ_Add(nullptr, 1, data, 1, false), MBError::INVALID_ARG);
    EXPECT_EQ(dict->SHMQ_Add(key, 1, nullptr, 1, false), MBError::INVALID_ARG);
    EXPECT_EQ(dict->SHMQ_Add(key, -1, data, 1, false), MBError::OUT_OF_BOUND);
    EXPECT_EQ(dict->SHMQ_Add(key, 1, data, -1, false), MBError::OUT_OF_BOUND);
    EXPECT_EQ(dict->SHMQ_Add(key, 0, data, 1, false), MBError::OUT_OF_BOUND);
    EXPECT_EQ(dict->SHMQ_Add(key, 1, data, 0, false), MBError::OUT_OF_BOUND);
    EXPECT_EQ(dict->SHMQ_Add(key, MB_ASYNC_SHM_KEY_SIZE + 1, data, 1, false),
        MBError::OUT_OF_BOUND);
    EXPECT_EQ(dict->SHMQ_Add(key, 1, data, MB_ASYNC_SHM_DATA_SIZE + 1, false),
        MBError::OUT_OF_BOUND);
    EXPECT_EQ(dict->SHMQ_Remove(nullptr, 1), MBError::INVALID_ARG);
    EXPECT_EQ(dict->SHMQ_Remove(key, -1), MBError::OUT_OF_BOUND);
    EXPECT_EQ(dict->SHMQ_Remove(key, 0), MBError::OUT_OF_BOUND);
    EXPECT_EQ(dict->SHMQ_Remove(key, MB_ASYNC_SHM_KEY_SIZE + 1),
        MBError::OUT_OF_BOUND);

    EXPECT_EQ(header->queue_index.load(std::memory_order_acquire), queue_index);
}

} // namespace
