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

#ifndef __LOCK_H__
#define __LOCK_H__

#include <pthread.h>

namespace mabain {

// multiple-thread/process lock

class MBLock
{
public:
    MBLock();
    ~MBLock();

    inline int Lock();
    inline int UnLock();
    inline int TryLock();

    void Init(pthread_mutex_t *lock);

private:
    pthread_mutex_t *mb_lock_ptr;
};

}

#endif
