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
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <assert.h>

#include "free_list.h"
#include "error.h"
#include "logger.h"
#include "lock_free.h"

namespace mabain {

FreeList::FreeList(const std::string &file_path, int buff_alignment,
                   int max_n_buff, int max_buff_per_list)
               : list_path(file_path),
                 alignment(buff_alignment),
                 max_num_buffer(max_n_buff),
                 max_buffer_per_list(max_buff_per_list),
                 buffer_free_list(NULL),
                 count(0),
                 tot_size(0)
{
    // rel_parent_off in ResourceCollection is defined as 2-byte signed integer.
    // The maximal buffer size cannot be greather than 32767.
    assert(GetBufferSizeByIndex(max_n_buff-1) <= 65535);

    Logger::Log(LOG_LEVEL_INFO, "%s maximum number of buffers: %d", file_path.c_str(),
                max_num_buffer);
    buffer_free_list = new MBlsq*[max_num_buffer];
    for(int i = 0; i < max_num_buffer; i++)
    {
        buffer_free_list[i] = new MBlsq(NULL);
    }

    memset(buf_cache, 0, sizeof(buf_cache));
    buf_cache_index = 0;
}

FreeList::~FreeList()
{
    if(buffer_free_list)
    {
        for(int i = 0; i < max_num_buffer; i++)
        {
            if(buffer_free_list[i])
            {
                while(buffer_free_list[i]->Count())
                    buffer_free_list[i]->RemoveIntFromHead();
                delete buffer_free_list[i];
            }
        }
        delete [] buffer_free_list;
    }
}

int FreeList::ReuseBuffer(int buf_index, size_t offset)
{
    int rval = MBError::BUFFER_LOST;
    for(int i = buf_index - 1; i > 0; i--)
    {
        if(buffer_free_list[i]->Count() > (unsigned) max_buffer_per_list)
            continue;

        if(buffer_free_list[i]->AddIntToTail(offset) == MBError::SUCCESS)
        {
            count++;
            tot_size += (i + 1) * alignment;
            rval = MBError::SUCCESS;
        }
        break;
    }

    return rval;
}

int FreeList::AddBuffer(size_t offset, int size)
{
    int rval = MBError::SUCCESS;

    int buf_index = GetBufferIndex(size);
    assert(buf_index < max_num_buffer);

    if(buffer_free_list[buf_index]->Count() > (unsigned)max_buffer_per_list)
    {
        ReuseBuffer(buf_index, offset); 
        return rval;
    }

    buffer_free_list[buf_index]->AddIntToTail(offset);
    count++;
    tot_size += (buf_index + 1) * alignment;
    return rval;
}

int FreeList::RemoveBuffer(size_t &offset, int size)
{
    int rval = MBError::NO_MEMORY;

    int buf_index = GetBufferIndex(size);
    if(buffer_free_list[buf_index]->Count() > 0)
    {
        offset = buffer_free_list[buf_index]->RemoveIntFromHead();
        rval = MBError::SUCCESS;
        count--;
        tot_size -= (buf_index + 1) * alignment;
    }

    return rval;
}

size_t FreeList::GetTotSize() const
{
    return tot_size;
}

int64_t FreeList::Count() const
{
    return count;
}

int FreeList::StoreListOnDisk()
{
    if(buffer_free_list == NULL)
        return MBError::NOT_ALLOWED;

    int rval = MBError::SUCCESS;

    if(count == 0)
        return rval;

    std::ofstream freelist_f(list_path.c_str(), std::fstream::out | std::fstream::binary);
    if(!freelist_f.is_open())
    {
        Logger::Log(LOG_LEVEL_ERROR, "cannot open file " + list_path);
        return MBError::OPEN_FAILURE;
    }

    Logger::Log(LOG_LEVEL_INFO, "%s write %lld buffers to list disk: %llu", list_path.c_str(),
                count, tot_size);
    for(int buf_index = 0; buf_index < max_num_buffer; buf_index++)
    {
        int64_t buf_count = buffer_free_list[buf_index]->Count();
        if(buf_count > 0)
        {
            // write list header (buffer index, buffer count)
            freelist_f.write((char *)&buf_index, sizeof(int));
            freelist_f.write((char *)&buf_count, sizeof(int64_t));
            for(int i = 0; i < buf_count; i++)
            {
                size_t offset = buffer_free_list[buf_index]->RemoveIntFromHead();
                freelist_f.write((char *) &offset, sizeof(size_t));
                count--;
                tot_size -= (buf_index + 1) * alignment;
            }
        }
    }

    freelist_f.close();

    return rval;
}

int FreeList::LoadListFromDisk()
{
    if(buffer_free_list == NULL)
        return MBError::NOT_ALLOWED;

    if(access(list_path.c_str(), F_OK) != 0)
    {
        if(errno == ENOENT)
        {
            Logger::Log(LOG_LEVEL_INFO, list_path + " does not exist");
            return MBError::SUCCESS;
        }

        char err_buf[32];
        Logger::Log(LOG_LEVEL_ERROR, "cannot access %s with full permission: ",
                                 list_path.c_str(),
                                 strerror_r(errno, err_buf, sizeof(err_buf)));
        return MBError::NOT_ALLOWED;
    }

    // Read the file
    std::ifstream freelist_f(list_path.c_str(), std::fstream::in | std::fstream::binary);
    if(!freelist_f.is_open())
        return MBError::OPEN_FAILURE;

    while(!freelist_f.eof())
    {
        int buf_index;
        int64_t buf_count;
        // Read header
        freelist_f.read((char *) &buf_index, sizeof(int));
        freelist_f.read((char *) &buf_count, sizeof(int64_t));
        if(freelist_f.eof())
            break;
        for(int i = 0; i < buf_count; i++)
        {
            size_t offset;
            freelist_f.read((char *) &offset, sizeof(size_t));
            buffer_free_list[buf_index]->AddIntToTail(offset);
            count++;
            tot_size += (buf_index + 1) * alignment;
        }
    }

    freelist_f.close();

    // Remove the file
    if(unlink(list_path.c_str()) != 0)
    {
        char err_buf[32];
        Logger::Log(LOG_LEVEL_ERROR, "failed to delete file %s: %s ", list_path.c_str(),
                    strerror_r(errno, err_buf, sizeof(err_buf)));
        return MBError::WRITE_ERROR;
    }

    Logger::Log(LOG_LEVEL_INFO, "%s read %lld buffers to free list: %llu",
                list_path.c_str(), count, tot_size);

    return MBError::SUCCESS;
}

void FreeList::ReleaseAlignmentBuffer(size_t old_offset, size_t alignment_offset)
{
    if(alignment_offset <= old_offset)
        return;

    int rval = AddBuffer(old_offset, alignment_offset - old_offset);
    if(rval != MBError::SUCCESS)
        Logger::Log(LOG_LEVEL_ERROR, "failed to release alignment buffer");
}

void FreeList::Empty()
{
    for(int i = 0; i < max_num_buffer; i++)
    {
        if(buffer_free_list[i])
        {
            buffer_free_list[i]->Clear();
        }
    }
    count = 0;
    tot_size = 0;
}

bool FreeList::GetBufferByIndex(int buf_index, size_t &offset)
{
#ifdef __DEBUG__
    assert(buf_index < max_num_buffer);
#endif
    MBlsq *flist = buffer_free_list[buf_index];
    if(flist->Count() > 0)
    {
        offset = flist->RemoveIntFromHead();
        count--;
        tot_size -= (buf_index + 1) * alignment;
        return true;
    }

    return false;
}

}
