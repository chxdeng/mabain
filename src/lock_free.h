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

#include <stdint.h>
#include <string.h>
#include <atomic>

namespace mabain {

// C++11 std::atomic shared memory variable for lock-free
// reader/writer concurrency.

#define MAX_OFFSET_CACHE 4

typedef struct _LockFreeData
{
    bool     modify_flag;
    uint32_t counter;
    size_t   offset;
} LockFreeData;

typedef struct _LockFreeShmData
{
    std::atomic<bool>     modify_flag;
    std::atomic<uint32_t> counter;
    std::atomic<size_t>   offset;
    std::atomic<size_t>   offset_cache[MAX_OFFSET_CACHE];
    size_t                edge_offset_cache[MAX_OFFSET_CACHE];
    size_t                node_offset_cache[MAX_OFFSET_CACHE];
} LockFreeShmData;

class LockFree
{
public:
    ~LockFree();

    static void LockFreeInit(LockFreeShmData *lock_free_ptr, int mode = 0);
    static void WriterLockFreeStart(size_t offset);
    static void WriterLockFreeStop();
    static int  ReaderLockFreeStart(LockFreeData &snapshot);
    // If there was race condition, this function returns MBError::TRY_AGAIN.
    static int  ReaderLockFreeStop(const LockFreeData &snapshot, size_t reader_offset);
    static void PushOffsets(size_t edge_off, size_t node_off);
    static bool ReleasedOffsetInUse(size_t offset);

private:
    LockFree();

    static LockFreeShmData *shm_data_ptr;
};

}

#endif
