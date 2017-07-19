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

#include <iostream>

#include "mb_data.h"
#include "error.h"

namespace mabain {

MBData::MBData()
{
    data_len = 0;
    buff_len = 0;
    buff = NULL;

    match_len = 0;
    next = false;
    options = 0;
}

MBData::MBData(int size, int match_options)
{
    buff_len = size;
    if(buff_len > 0)
    {
        buff = static_cast<uint8_t*>(malloc(buff_len));
        if(buff == NULL) buff_len = 0;
    }
    else
        buff = NULL;

    data_len = 0;
    match_len = 0;
    next = false;
    options = match_options;
}

MBData::~MBData()
{
    if(buff)
        free(buff);
}

// This function is for prefix match only.
void MBData::Clear()
{
    match_len = 0;
    data_len = 0;
    next = false;
}

int MBData::Resize(int size)
{
    if(size > buff_len)
    {
        buff_len = size;
        if(buff != NULL)
            free(buff);

        buff = static_cast<uint8_t*>(malloc(buff_len));
        if(buff == NULL)
        {
            buff_len = 0;
            return MBError::NO_MEMORY;
        }
    }
    return MBError::SUCCESS;
}

}
