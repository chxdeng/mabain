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

#include "lock_free.h"
#include "mabain_consts.h"
#include "error.h"
#include "integer_4b_5b.h"

namespace mabain {

LockFree::LockFree()
{
    shm_data_ptr = NULL;
}

LockFree::~LockFree()
{
}

void LockFree::LockFreeInit(LockFreeShmData *lock_free_ptr, int mode)
{
    shm_data_ptr = lock_free_ptr;
    if(mode & CONSTS::ACCESS_MODE_WRITER)
    {
        // Clear the lock free data
        shm_data_ptr->counter.store(0, MEMORY_ORDER_WRITER);
        shm_data_ptr->offset.store(MAX_6B_OFFSET, MEMORY_ORDER_WRITER);
    }
}

//////////////////////////////////////////////////
// DO NOT CHANGE THE STORE ORDER IN THIS FUNCTION.
//////////////////////////////////////////////////
void LockFree::WriterLockFreeStop()
{
    int index = shm_data_ptr->counter % MAX_OFFSET_CACHE;
    shm_data_ptr->offset_cache[index].store(shm_data_ptr->offset, MEMORY_ORDER_WRITER);

    shm_data_ptr->counter.fetch_add(1, MEMORY_ORDER_WRITER);
    shm_data_ptr->offset.store(MAX_6B_OFFSET, MEMORY_ORDER_WRITER);
}

//////////////////////////////////////////////////
// DO NOT CHANGE THE LOAD ORDER IN THIS FUNCTION.
//////////////////////////////////////////////////
int LockFree::ReaderLockFreeStop(const LockFreeData &snapshot, size_t reader_offset)
{
    LockFreeData curr;
    curr.offset = shm_data_ptr->offset.load(MEMORY_ORDER_READER);
    curr.counter = shm_data_ptr->counter.load(MEMORY_ORDER_READER);

    if(curr.offset == reader_offset)
        return MBError::TRY_AGAIN;

    // Note it is expected that count_diff can overflow.
    uint32_t count_diff = curr.counter - snapshot.counter;
    if(count_diff == 0)
        return MBError::SUCCESS; // Writer was doing nothing. Reader can proceed.
    if(count_diff >= MAX_OFFSET_CACHE)
        return MBError::TRY_AGAIN; // Cache is overwritten. Have to retry.

    for(unsigned i = 0; i < count_diff; i++)
    {
        int index = (snapshot.counter + i) % MAX_OFFSET_CACHE;
        if(reader_offset == shm_data_ptr->offset_cache[index].load(MEMORY_ORDER_READER))
            return MBError::TRY_AGAIN;
    }

    // Need to recheck the counter difference
    count_diff = shm_data_ptr->counter.load(MEMORY_ORDER_READER) - snapshot.counter;
    if(count_diff >= MAX_OFFSET_CACHE)
        return MBError::TRY_AGAIN;

    // Writer was modifying different edges. It is safe to for the reader to proceed.
    return MBError::SUCCESS;
}

}
