/**
 * Copyright (C) 2018 Cisco Inc.
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

#ifndef __SHM_MUTEX_H__
#define __SHM_MUTEX_H__

#include <pthread.h>

namespace mabain {

int  InitShmMutex(pthread_mutex_t *mutex);
int  InitShmRWLock(pthread_rwlock_t *lock);
int  InitShmCond(pthread_cond_t *cond);

}

#endif
