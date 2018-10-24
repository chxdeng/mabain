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

#ifndef __FREE_LIST_H__
#define __FREE_LIST_H__

#include <cstdlib>
#include <string>

#include "mb_lsq.h"
#include "error.h"
#include "lock_free.h"

#define MAX_BUFFER_PER_LIST    256

// Manage resource allocation/free using linked list
namespace mabain {

typedef struct _BufferCache
{
    size_t buf_index;
    size_t buf_offset;
} BufferCache;

class FreeList
{
public:
    FreeList(const std::string &file_path, size_t buff_alignment, size_t max_n_buff,
             size_t max_buff_per_list = MAX_BUFFER_PER_LIST);
    ~FreeList();

    // Free a buffer by adding it to the free list
    int AddBuffer(size_t offset, size_t size);
    // Reserve a buffer by removing it from the free list
    int RemoveBuffer(size_t &offset, size_t size);
    // Release alignment buffer
    void ReleaseAlignmentBuffer(size_t old_offset, size_t alignment_offset);

    bool GetBufferByIndex(size_t buf_index, size_t &offset);

    void Empty();

    // Read buffer list from disk
    int LoadListFromDisk();
    // Save buffer list to disk
    int StoreListOnDisk();
    // Get buffer count
    int64_t Count() const;
    // Get total freed buffer size in the list
    size_t GetTotSize() const;

    inline int      AddBufferByIndex(size_t buf_index, size_t offset);
    inline size_t   RemoveBufferByIndex(size_t buf_index);
    inline size_t   GetAlignmentSize(size_t size) const;
    inline size_t   GetBufferIndex(size_t size) const;
    inline uint64_t GetBufferCountByIndex(size_t buf_index) const;
    inline size_t   GetBufferSizeByIndex(size_t buf_index) const;
    inline int      ReleaseBuffer(size_t offset, size_t size);

private:
    int ReuseBuffer(size_t buf_index, size_t offset);

    // file path where the list will be serialized and stored
    std::string list_path;
    // buffer/memory alignment
    size_t alignment;
    // maximum number of buffers
    size_t max_num_buffer;
    // maximum buffer per list
    // This restriction is to limit memory usage.
    size_t max_buffer_per_list;
    // number of separated buffers
    MBlsq **buffer_free_list;
    // total count of freed buffers
    int64_t count;
    // totol size allocted for all the buffers
    size_t tot_size;
};

inline size_t FreeList::GetAlignmentSize(size_t size) const
{
#ifdef __DEBUG__
    assert(size > 0 && size < max_num_buffer*alignment);
#endif
    size_t alignment_mod = size % alignment;
    if(alignment_mod == 0)
        return size;
    return (size + alignment - alignment_mod);
}

inline size_t FreeList::GetBufferIndex(size_t size) const
{
#ifdef __DEBUG__
    assert(size > 0 && size < max_num_buffer*alignment);
#endif
    return ((size - 1) / alignment);
}

inline uint64_t FreeList::GetBufferCountByIndex(size_t buf_index) const
{
#ifdef __DEBUG__
    assert(buf_index < max_num_buffer);
#endif
    return buffer_free_list[buf_index]->Count();
}

inline size_t FreeList::GetBufferSizeByIndex(size_t buf_index) const
{
#ifdef __DEBUG__
    assert(buf_index < max_num_buffer);
#endif
    return (buf_index + 1) * alignment;
}

inline int FreeList::AddBufferByIndex(size_t buf_index, size_t offset)
{
#ifdef __DEBUG__
    assert(buf_index < max_num_buffer);
#endif

    if(buffer_free_list[buf_index]->Count() > (unsigned)max_buffer_per_list)
    {
        ReuseBuffer(buf_index, offset);
        return MBError::SUCCESS;
    }

    count++;
    tot_size += (buf_index + 1) * alignment;
    return buffer_free_list[buf_index]->AddIntToTail(offset);
}

inline size_t FreeList::RemoveBufferByIndex(size_t buf_index)
{
#ifdef __DEBUG__
    assert(buf_index < max_num_buffer);
#endif
    count--;
    tot_size -= (buf_index + 1) * alignment;
    return buffer_free_list[buf_index]->RemoveIntFromHead();
}

inline int FreeList::ReleaseBuffer(size_t offset, size_t size)
{
#ifdef __DEBUG__
    assert(size > 0 && size < max_num_buffer*alignment);
#endif
    size_t buf_index = GetBufferIndex(size);
    return AddBufferByIndex(buf_index, offset);
}

}

#endif
