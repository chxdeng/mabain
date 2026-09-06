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

#include <cstdio>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

#include "../db.h"
#include "../logger.h"

namespace {

const char* const MAIN_LOG_FILE = "/var/tmp/mabain_test/mabain.log";
const char* const BOUNDS_TEST_LOG_FILE = "/var/tmp/mabain_test/logger_bounds_test.log";

}

TEST(LoggerTest, FormattedMessageIsBounded)
{
    mabain::DB::CloseLogFile();
    std::remove(BOUNDS_TEST_LOG_FILE);
    mabain::DB::SetLogFile(BOUNDS_TEST_LOG_FILE);

    const std::string oversized_message(1024, 'x');
    mabain::Logger::Log(LOG_LEVEL_INFO, "%s", oversized_message.c_str());

    mabain::DB::CloseLogFile();
    std::ifstream log(BOUNDS_TEST_LOG_FILE);
    std::string line;
    const bool read_ok = static_cast<bool>(std::getline(log, line));
    log.close();
    std::remove(BOUNDS_TEST_LOG_FILE);

    // Restore the log used by the shared unit-test process before asserting.
    mabain::DB::SetLogFile(MAIN_LOG_FILE);

    ASSERT_TRUE(read_ok);
    const std::string truncated_message(255, 'x');
    ASSERT_GE(line.size(), truncated_message.size());
    EXPECT_EQ(line.compare(line.size() - truncated_message.size(),
                  truncated_message.size(), truncated_message),
        0);
    EXPECT_EQ(line.find(std::string(256, 'x')), std::string::npos);
}
