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

#include <stdio.h>

#include "hexbin.h"

int bin_2_hex(const uint8_t* data, int len, char* buff, int buf_size)
{
    if (buf_size < 2 * len + 1)
        return -1;
    for (int i = 0; i < len; i++) {
        sprintf(buff + 2 * i, "%02x", data[i]);
    }
    len *= 2;
    buff[len] = '\0';
    return len;
}

static inline int hex_to_half_byte(char hex)
{
    switch (hex) {
    case '0':
        return 0;
    case '1':
        return 1;
    case '2':
        return 2;
    case '3':
        return 3;
    case '4':
        return 4;
    case '5':
        return 5;
    case '6':
        return 6;
    case '7':
        return 7;
    case '8':
        return 8;
    case '9':
        return 9;
    case 'a':
    case 'A':
        return 10;
    case 'b':
    case 'B':
        return 11;
    case 'c':
    case 'C':
        return 12;
    case 'd':
    case 'D':
        return 13;
    case 'e':
    case 'E':
        return 14;
    case 'f':
    case 'F':
        return 15;
    default:
        return -1;
    }
}

int hex_2_bin(const char* data, int len, uint8_t* buff, int buf_size)
{
    if (len % 2 != 0)
        return -1;
    len = len / 2;
    if (buf_size < len)
        return -1;

    char high_byte, low_byte;
    for (int i = 0; i < len; i++) {
        high_byte = hex_to_half_byte(data[2 * i]);
        low_byte = hex_to_half_byte(data[2 * i + 1]);
        if (high_byte < 0 || low_byte < 0)
            return -1;
        buff[i] = (high_byte << 4) + low_byte;
    }

    return len;
}
