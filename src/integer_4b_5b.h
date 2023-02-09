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

#ifndef __INTEGER_4B_5B_H__
#define __INTEGER_4B_5B_H__

#include <stdint.h>
#include <string.h>

namespace mabain {

#define MAX_4B_OFFSET 0xFFFFFFFF
#define MAX_5B_OFFSET 0xFFFFFFFFFF
#define MAX_6B_OFFSET 0xFFFFFFFFFFFF

// write and read 5-byte 6-byte unsigned integer
// Note this is based on engianness.

inline void Write5BInteger(uint8_t* buffer, size_t offset)
{
#ifdef __DEBUG__
    if (offset > MAX_5B_OFFSET) {
        std::cerr << "OFFSET " << offset << " TOO LARGE FOR 5 BYTES\n";
        abort();
    }
#endif

    uint8_t* src = reinterpret_cast<uint8_t*>(&offset);
#ifndef __BIG__ENDIAN__
    memcpy(buffer, src, 5);
#else
    memcpy(buffer, src + 3, 5);
#endif
}

inline size_t Get5BInteger(const uint8_t* buffer)
{
    size_t offset = 0;
    uint8_t* target = reinterpret_cast<uint8_t*>(&offset);
#ifndef __BIG__ENDIAN__
    memcpy(target, buffer, 5);
#else
    memcpy(target + 3, buffer, 5);
#endif
    return offset;
}

inline void Write6BInteger(uint8_t* buffer, size_t offset)
{
#ifdef __DEBUG__
    if (offset > MAX_6B_OFFSET) {
        std::cerr << "OFFSET " << offset << "TOO LARGE FOR SIX BYTES\n";
        abort();
    }
#endif

    uint8_t* src = reinterpret_cast<uint8_t*>(&offset);
#ifndef __BIG__ENDIAN__
    memcpy(buffer, src, 6);
#else
    memcpy(buffer, src + 2, 6);
#endif
}

inline size_t Get6BInteger(const uint8_t* buffer)
{
    size_t offset = 0;
    uint8_t* target = reinterpret_cast<uint8_t*>(&offset);
#ifndef __BIG__ENDIAN__
    memcpy(target, buffer, 6);
#else
    memcpy(target + 2, buffer, 6);
#endif
    return offset;
}

}

#endif
