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

#include <string.h>

#include "mb_data.h"
#include "error.h"

namespace mabain {

MBData::MBData()
{
    data_len = 0;
    buff_len = 0;
    buff = NULL;
    data_offset = 0;

    match_len = 0;
    options = 0;
    free_buffer = false;
}

MBData::MBData(int size, int match_options)
{
    buff_len = size;
    buff = NULL;
    if(buff_len > 0)
        buff = reinterpret_cast<uint8_t*>(malloc(buff_len + 1));

    if(buff != NULL)
    {
        free_buffer = true;
    }
    else
    {
        buff_len = 0;
        free_buffer = false;
    }

    data_len = 0;
    match_len = 0;
    options = match_options;
    data_offset = 0;
}

// Caller must free data.
int MBData::TransferValueTo(uint8_t* &data, int &dlen)
{
    if(buff == NULL || data_len <= 0)
    {
        dlen = 0;
        data = NULL;
        return MBError::INVALID_ARG;
    }

    if(free_buffer)
    {
        data = buff;
        buff = NULL;
        dlen = data_len;
        free_buffer = false; 
        buff_len = 0;
    }
    else
    {
        data = (uint8_t *) malloc(data_len + 1);
        if(data == NULL)
            return MBError::NO_MEMORY;
        memcpy(data, buff, data_len);
        dlen = data_len;
    }

    return MBError::SUCCESS;
}

// Data must be allocated using malloc or calloc.
int MBData::TransferValueFrom(uint8_t* &data, int dlen)
{
    if(data == NULL)
        return MBError::INVALID_ARG;

    if(free_buffer && buff != NULL)
        free(buff);
    buff = data;
    buff_len = dlen;
    data_len = dlen;
    free_buffer = true;

    data = NULL;
    return MBError::SUCCESS;
}

MBData::~MBData()
{
    if(free_buffer)
        free(buff);
}

// This function is for prefix match only.
void MBData::Clear()
{
    match_len = 0;
    data_len = 0;
    data_offset = 0;
    options = 0;
}

int MBData::Resize(int size)
{
    if(size > buff_len)
    {
        buff_len = size;
        if(free_buffer)
        {
            free(buff);
        }
        else
        {
            free_buffer = true;
        }

        buff = reinterpret_cast<uint8_t*>(malloc(buff_len + 1));
        if(buff == NULL)
        {
            buff_len = 0;
            free_buffer = false;
            return MBError::NO_MEMORY;
        }

    }
    return MBError::SUCCESS;
}

}
