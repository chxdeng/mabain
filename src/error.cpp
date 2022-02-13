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

#include "error.h"

namespace mabain {

const int MBError::MAX_ERROR_CODE = NO_DB;
const char* MBError::error_str[] = {
    "success",
    "no memory",
    "out of bound",
    "invalid argument",
    "not initialized",
    "no existence",
    "found in DB",
    "mmap failed",
    "no permission",
    "file open failure",
    "file write error",
    "file read error",
    "size not right",
    "try again",
    "resource allocation error",
    "mutex error",
    "unknown error",
    "writer already running",
    "no resource available",
    "database closed",
    "buffer discarded", // buffer will be reclaimed by shrink
    "failed to create thread",
    "rc skipped",
    "version mismatch",
    "max append size exceeded",

    ///////////////////////////////////
    "DB not exist",
};

const char* MBError::get_error_str(int err)
{
    if(err < 0)
        return "db error";
    else if(err > MAX_ERROR_CODE)
        return "invalid error code";

    return error_str[err];
}

}
