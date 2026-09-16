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

#include <cerrno>
#include <filesystem>
#include <fstream>
#include <string>
#include <unistd.h>

#include <gtest/gtest.h>

#include "../db.h"
#include "../error.h"
#include "../util/utils.h"

using namespace mabain;

namespace {

class UtilsQueueCleanupTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        root_dir = "/var/tmp/mabain_utils_queue_cleanup_" + std::to_string(getpid());
        db_dir = root_dir + "/db";
        queue_dir = root_dir + "/queues";

        std::error_code error;
        std::filesystem::remove_all(root_dir, error);
        error.clear();
        ASSERT_TRUE(std::filesystem::create_directories(db_dir, error));
        ASSERT_FALSE(error);
        ASSERT_TRUE(std::filesystem::create_directories(queue_dir, error));
        ASSERT_FALSE(error);
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(root_dir, error);
    }

    static void CreateFile(const std::string& path)
    {
        std::ofstream file(path);
        ASSERT_TRUE(file.is_open());
        file << "test";
        ASSERT_TRUE(file.good());
    }

    std::string root_dir;
    std::string db_dir;
    std::string queue_dir;
};

TEST_F(UtilsQueueCleanupTest, RemovesOnlyQueueOwnedByDatabase)
{
    const std::string header_path = db_dir + "/_mabain_h";
    CreateFile(header_path);
    const uint64_t queue_id = get_file_inode(header_path);
    ASSERT_NE(queue_id, 0u);

    const std::string owned_queue =
        queue_dir + "/_mabain_q" + std::to_string(queue_id);
    const std::string unrelated_queue =
        queue_dir + "/_mabain_q" + std::to_string(queue_id + 1);
    CreateFile(owned_queue);
    CreateFile(unrelated_queue);

    remove_db_queue_file(db_dir, queue_dir.c_str());

    EXPECT_FALSE(std::filesystem::exists(owned_queue));
    EXPECT_TRUE(std::filesystem::exists(unrelated_queue));

    CreateFile(owned_queue);
    remove_db_queue_file(db_dir + "/", queue_dir.c_str());

    EXPECT_FALSE(std::filesystem::exists(owned_queue));
    EXPECT_TRUE(std::filesystem::exists(unrelated_queue));
}

TEST_F(UtilsQueueCleanupTest, MissingHeaderDoesNotRemoveQueues)
{
    const std::string unrelated_queue = queue_dir + "/_mabain_q12345";
    CreateFile(unrelated_queue);

    remove_db_queue_file(db_dir, queue_dir.c_str());

    EXPECT_TRUE(std::filesystem::exists(unrelated_queue));
}

class UtilsInitLockTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        root_dir = "/var/tmp/mabain_utils_init_lock_" + std::to_string(getpid());
        db_dir = root_dir + "/db";
        lock_path = db_dir + "/_mbh_lock";

        std::error_code error;
        std::filesystem::remove_all(root_dir, error);
        error.clear();
        ASSERT_TRUE(std::filesystem::create_directories(db_dir, error));
        ASSERT_FALSE(error);
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(root_dir, error);
    }

    std::string root_dir;
    std::string db_dir;
    std::string lock_path;
};

TEST_F(UtilsInitLockTest, ReadersShareButWriterDoesNotBlock)
{
    int first_reader = acquire_init_file_lock(lock_path, true);
    ASSERT_GE(first_reader, 0);
    int second_reader = acquire_init_file_lock(lock_path, true);
    ASSERT_GE(second_reader, 0);

    errno = 0;
    int writer = acquire_init_file_lock(lock_path, false);
    const int writer_errno = errno;
    EXPECT_LT(writer, 0);
    EXPECT_TRUE(writer_errno == EWOULDBLOCK || writer_errno == EAGAIN);

    release_file_lock(first_reader);
    release_file_lock(second_reader);
    writer = acquire_init_file_lock(lock_path, false);
    ASSERT_GE(writer, 0);
    release_file_lock(writer);
}

TEST_F(UtilsInitLockTest, ReaderDoesNotBlockBehindWriter)
{
    int writer = acquire_init_file_lock(lock_path, false);
    ASSERT_GE(writer, 0);

    errno = 0;
    int reader = acquire_init_file_lock(lock_path, true);
    const int reader_errno = errno;
    EXPECT_LT(reader, 0);
    EXPECT_TRUE(reader_errno == EWOULDBLOCK || reader_errno == EAGAIN);

    release_file_lock(writer);
}

TEST_F(UtilsInitLockTest, BusyLockLeavesDatabaseUnopened)
{
    int writer = acquire_init_file_lock(lock_path, false);
    ASSERT_GE(writer, 0);

    MBConfig config = {};
    config.mbdir = db_dir.c_str();
    config.options = CONSTS::ACCESS_MODE_READER;
    DB reader(config);

    EXPECT_EQ(reader.Status(), MBError::TRY_AGAIN);
    EXPECT_FALSE(std::filesystem::exists(db_dir + "/_mabain_h"));
    release_file_lock(writer);
}

}
