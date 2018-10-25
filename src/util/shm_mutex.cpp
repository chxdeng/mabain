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

#include "shm_mutex.h"
#include "../logger.h"
#include "../error.h"

namespace mabain {

int InitShmMutex(pthread_mutex_t *mutex)
{
    if(mutex == NULL)
        return MBError::INVALID_ARG;

    pthread_mutexattr_t attr;
    if(pthread_mutexattr_init(&attr))
    {
        Logger::Log(LOG_LEVEL_WARN, "pthread_mutexkattr_init failed");
        return MBError::MUTEX_ERROR;
    }
    if(pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED))
    {
        Logger::Log(LOG_LEVEL_WARN, "failed to set mutex/PTHREAD_PROCESS_SHARED");
        pthread_mutexattr_destroy(&attr);
        return MBError::MUTEX_ERROR;
    }

    // Cannot set mutex as robust because of the glibc bug
    if(pthread_mutex_init(mutex, &attr))
    {
        Logger::Log(LOG_LEVEL_WARN, "pthread_mutex_init failed");
        pthread_mutexattr_destroy(&attr);
        return MBError::MUTEX_ERROR;
    }
    pthread_mutexattr_destroy(&attr);

    return MBError::SUCCESS;
}

int InitShmRWLock(pthread_rwlock_t *lock)
{
    if(lock == NULL)
        return MBError::INVALID_ARG;

    pthread_rwlockattr_t attr;
    if(pthread_rwlockattr_init(&attr))
    {
        Logger::Log(LOG_LEVEL_WARN, "pthread_rwlockattr_init failed");
        return MBError::MUTEX_ERROR;
    }
    if(pthread_rwlockattr_setpshared(&attr, PTHREAD_PROCESS_SHARED))
    {
        Logger::Log(LOG_LEVEL_WARN, "failed to set rwlock/PTHREAD_PROCESS_SHARED");
        pthread_rwlockattr_destroy(&attr);
        return MBError::MUTEX_ERROR;
    }

    if(pthread_rwlock_init(lock, &attr))
    {
        Logger::Log(LOG_LEVEL_WARN, "pthread_rwlock_init failed");
        pthread_rwlockattr_destroy(&attr);
        return MBError::MUTEX_ERROR;
    }
    pthread_rwlockattr_destroy(&attr);

    return MBError::SUCCESS;
}

int InitShmCond(pthread_cond_t *cond)
{
    if(cond == NULL)
        return MBError::INVALID_ARG;

    pthread_condattr_t attr;
    if(pthread_condattr_init(&attr))
    {
        Logger::Log(LOG_LEVEL_WARN, "pthread_condattr_init failed");
        return MBError::MUTEX_ERROR;
    }
    if(pthread_condattr_setpshared(&attr, PTHREAD_PROCESS_SHARED))
    {
        Logger::Log(LOG_LEVEL_WARN, "failed to set cond/PTHREAD_PROCESS_SHARED");
        pthread_condattr_destroy(&attr);
        return MBError::MUTEX_ERROR;
    }

    if(pthread_cond_init(cond, &attr))
    {
        Logger::Log(LOG_LEVEL_WARN, "pthread_cond_init failed");
        pthread_condattr_destroy(&attr);
        return MBError::MUTEX_ERROR;
    }
    pthread_condattr_destroy(&attr);

    return MBError::SUCCESS;
}

}
