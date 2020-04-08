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

#include "lock.h"
#include "./util/shm_mutex.h"

namespace mabain {

void MBLock::Init(pthread_mutex_t *lock)
{
    mb_lock_ptr = lock;
}

MBLock::MBLock() : mb_lock_ptr(nullptr)
{
}

MBLock::~MBLock()
{
}

int MBLock::Lock()
{
    if(mb_lock_ptr == NULL)
        return -1;
    return ShmMutexLock(*mb_lock_ptr);
}

int MBLock::UnLock()
{
    if(mb_lock_ptr == NULL)
        return -1;
    return pthread_mutex_unlock(mb_lock_ptr);
}

}
