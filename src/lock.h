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

// multiple-thread/process reader/writer lock

class MBLock
{
public:
    ~MBLock();

    // Writer lock
    static inline int WrLock();
    // Reader lock
    static inline int RdLock();
    // Writer/reader unlock
    static inline int UnLock();

    static inline int TryWrLock();

    static void Init(pthread_rwlock_t *rw_lock);

private:
    MBLock();

    // a global lock variable
    static pthread_rwlock_t mb_rw_lock;
    static pthread_rwlock_t *mb_rw_lock_ptr;
};

inline int MBLock::WrLock()
{
    if(mb_rw_lock_ptr == NULL)
        return -1;
    return pthread_rwlock_wrlock(mb_rw_lock_ptr);
}

inline int MBLock::RdLock()
{
    if(mb_rw_lock_ptr == NULL)
        return -1;
    return pthread_rwlock_rdlock(mb_rw_lock_ptr);
}

inline int MBLock::UnLock()
{
    if(mb_rw_lock_ptr == NULL)
        return -1;
    return pthread_rwlock_unlock(mb_rw_lock_ptr);
}

inline int MBLock::TryWrLock()
{
    if(mb_rw_lock_ptr == NULL)
        return -1;
    return pthread_rwlock_trywrlock(mb_rw_lock_ptr);
}

}

#endif
