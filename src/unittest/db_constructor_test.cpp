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

#include <array>
#include <new>

#include <gtest/gtest.h>

#include "../db.h"
#include "../error.h"
#include "../mabain_consts.h"

using namespace mabain;

TEST(DBConstructorTest, NullPathLeavesObjectSafeToDestroy)
{
    // Make missing initialization deterministic instead of depending on stack contents.
    alignas(DB) std::array<unsigned char, sizeof(DB)> storage;
    storage.fill(0xA5);

    DB* db = new (storage.data()) DB(nullptr, CONSTS::WriterOptions());
    const int status = db->Status();
    const int options = db->GetDBOptions();
    Dict* const dict = db->GetDictPtr();
    const bool db_dir_empty = db->GetDBDir().empty();
    MBConfig config;
    db->GetDBConfig(config);

    // Destruction is part of the regression: Close() must see only safe defaults.
    db->~DB();

    EXPECT_EQ(status, MBError::NOT_INITIALIZED);
    EXPECT_EQ(options, 0);
    EXPECT_EQ(dict, nullptr);
    EXPECT_TRUE(db_dir_empty);
    EXPECT_EQ(config.mbdir, nullptr);
    EXPECT_EQ(config.options, 0);
    EXPECT_EQ(config.memcap_index, 0u);
    EXPECT_EQ(config.memcap_data, 0u);
    EXPECT_EQ(config.data_size, 0);
    EXPECT_EQ(config.connect_id, 0u);
    EXPECT_EQ(config.queue_size, 0u);
    EXPECT_EQ(config.queue_dir, nullptr);
    EXPECT_FALSE(config.jemalloc_keep_db);
    EXPECT_EQ(config.async_queue_reservation_timeout_sec, 0u);
}
