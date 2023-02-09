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

#ifndef __LOCK_FREE_H__
#define __LOCK_FREE_H__

#include <atomic>
#include <stdint.h>
#include <string.h>

#include "mb_data.h"

namespace mabain {

// C++11 std::atomic shared memory variable for lock-free
// reader/writer concurrency.

#define MAX_OFFSET_CACHE 4
#define MEMORY_ORDER_WRITER std::memory_order_release
#define MEMORY_ORDER_READER std::memory_order_consume

struct _IndexHeader;
typedef struct _IndexHeader IndexHeader;

typedef struct _LockFreeData {
    uint32_t counter;
    size_t offset;
} LockFreeData;

typedef struct _LockFreeShmData {
    std::atomic<uint32_t> counter;
    std::atomic<size_t> offset;
    std::atomic<size_t> offset_cache[MAX_OFFSET_CACHE];
} LockFreeShmData;

class LockFree {
public:
    LockFree();
    ~LockFree();

    void LockFreeInit(LockFreeShmData* lock_free_ptr, IndexHeader* hdr, int mode = 0);
    inline void WriterLockFreeStart(size_t offset);
    void WriterLockFreeStop();
    inline void ReaderLockFreeStart(LockFreeData& snapshot);
    // If there was race condition, this function returns MBError::TRY_AGAIN.
    int ReaderLockFreeStop(const LockFreeData& snapshot, size_t reader_offset,
        MBData& mbdata);

private:
    LockFreeShmData* shm_data_ptr;
    const IndexHeader* header;
};

inline void LockFree::WriterLockFreeStart(size_t offset)
{
    shm_data_ptr->offset.store(offset, MEMORY_ORDER_WRITER);
}

inline void LockFree::ReaderLockFreeStart(LockFreeData& snapshot)
{
    snapshot.counter = shm_data_ptr->counter.load(MEMORY_ORDER_READER);
}

}

#endif
