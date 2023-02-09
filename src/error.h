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

#ifndef __MBERROR_H__
#define __MBERROR_H__

#undef TRY_AGAIN

namespace mabain {

// mabain errors
class MBError {
public:
    enum mb_error {
        SUCCESS = 0,
        NO_MEMORY = 1,
        OUT_OF_BOUND = 2,
        INVALID_ARG = 3,
        NOT_INITIALIZED = 4,
        NOT_EXIST = 5,
        IN_DICT = 6,
        MMAP_FAILED = 7,
        NOT_ALLOWED = 8,
        OPEN_FAILURE = 9,
        WRITE_ERROR = 10,
        READ_ERROR = 11,
        INVALID_SIZE = 12,
        TRY_AGAIN = 13,
        ALLOCATION_ERROR = 14,
        MUTEX_ERROR = 15,
        UNKNOWN_ERROR = 16,
        WRITER_EXIST = 17,
        NO_RESOURCE = 18,
        DB_CLOSED = 19,
        BUFFER_LOST = 20,
        THREAD_FAILED = 21,
        RC_SKIPPED = 22,
        VERSION_MISMATCH = 23,

        // NO_DB should be the last enum.
        NO_DB
    };

    static const int MAX_ERROR_CODE;
    static const char* error_str[];
    static const char* get_error_str(int err);
};

}

#endif
