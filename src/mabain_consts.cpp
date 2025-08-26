/**
 * Copyright (C) 2025 Cisco Inc.
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

#include "mabain_consts.h"

namespace mabain {

const int CONSTS::ACCESS_MODE_READER = 0x0;
const int CONSTS::ACCESS_MODE_WRITER = 0x1;
const int CONSTS::ASYNC_WRITER_MODE = 0x2;
const int CONSTS::SYNC_ON_WRITE = 0x4;
const int CONSTS::USE_SLIDING_WINDOW = 0x8;
const int CONSTS::MEMORY_ONLY_MODE = 0x10;
const int CONSTS::READ_ONLY_DB = 0x20;

const int CONSTS::OPTION_FIND_AND_STORE_PARENT = 0x2;
const int CONSTS::OPTION_RC_MODE = 0x4;
const int CONSTS::OPTION_READ_SAVED_EDGE = 0x8;
const int CONSTS::OPTION_INTERNAL_NODE_BOUND = 0x10;
const int CONSTS::OPTION_SHMQ_RETRY = 0x20;
const int CONSTS::OPTION_JEMALLOC = 0x40;
const int CONSTS::OPTION_KEY_ONLY = 0x80;
const int CONSTS::OPTION_PREFIX_CACHE = 0x100;

const int CONSTS::MAX_KEY_LENGHTH = 256;
const int CONSTS::MAX_DATA_SIZE = 0x7FFF;

// Limit how many times readers retry when lock-free snapshot reports TRY_AGAIN
const int CONSTS::LOCK_FREE_RETRY_LIMIT = 1000;
const int CONSTS::FIND_TRAVERSAL_LIMIT = 1000;

int CONSTS::WriterOptions()
{
    int options = ACCESS_MODE_WRITER;
    return options;
}

int CONSTS::ReaderOptions()
{
    int options = ACCESS_MODE_READER;
    return options;
}

}
