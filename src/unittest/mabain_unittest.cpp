/**
 * Copyright (C) 2017 Cisco Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU General Public License, version 2,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

// @author Changxue Deng <chadeng@cisco.com>

#include <gtest/gtest.h>

#include "../db.h"
#define MB_DIR "/var/tmp/mabain_test/"

GTEST_API_ int main(int argc, char** argv)
{
    testing::InitGoogleTest(&argc, argv);

    mode_t mode = 0777;
    mkdir(MB_DIR, mode);

    mabain::DB::SetLogFile("/var/tmp/mabain_test/mabain.log");
    int rval = RUN_ALL_TESTS();

    mabain::DB::CloseLogFile();
    return rval;
}
