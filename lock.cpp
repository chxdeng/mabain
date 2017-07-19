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

namespace mabain {

#ifdef __SHM_LOCK__
pthread_rwlock_t*  MBLock::mb_rw_lock_ptr = NULL;
#else
pthread_rwlock_t MBLock::mb_rw_lock = PTHREAD_RWLOCK_INITIALIZER;
pthread_rwlock_t*  MBLock::mb_rw_lock_ptr = &MBLock::mb_rw_lock;
#endif

void MBLock::Init(pthread_rwlock_t *rw_lock)
{
#ifdef __SHM_LOCK__
    mb_rw_lock_ptr = rw_lock;
#endif
}

MBLock::MBLock()
{
}

MBLock::~MBLock()
{
}

}
