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
#include "../error.h"
#include "../logger.h"
#include <unistd.h>

namespace mabain {

int ShmMutexLock(pthread_mutex_t& mutex)
{
    int rval = pthread_mutex_lock(&mutex);
#ifndef __APPLE__
    if (rval == EOWNERDEAD) {
        Logger::Log(LOG_LEVEL_WARN, "owner died without unlock");
        rval = pthread_mutex_consistent(&mutex);
        if (rval != 0)
            Logger::Log(LOG_LEVEL_ERROR, "failed to recover mutex %d", errno);
    } else if (rval != 0) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to lock mutex %d", errno);
    }
#endif

    return rval;
}

int InitShmMutex(pthread_mutex_t* mutex)
{
    if (mutex == NULL)
        return MBError::INVALID_ARG;

    pthread_mutexattr_t attr;
    if (pthread_mutexattr_init(&attr)) {
        Logger::Log(LOG_LEVEL_WARN, "pthread_mutexkattr_init failed");
        return MBError::MUTEX_ERROR;
    }
#ifndef __APPLE__
    // Set mutex priority protocol to avoid hang in glibc
    // See https://bugzilla.redhat.com/show_bug.cgi?id=1401665 and
    // https://bugs.launchpad.net/ubuntu/+source/glibc/+bug/1706780
    if (pthread_mutexattr_setprotocol(&attr, PTHREAD_PRIO_INHERIT)) {
        Logger::Log(LOG_LEVEL_WARN, "failed to set mutex priority protocol");
        pthread_mutexattr_destroy(&attr);
        return MBError::MUTEX_ERROR;
    }
    if (pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST)) {
        Logger::Log(LOG_LEVEL_WARN, "failed to set mutex to robust");
        pthread_mutexattr_destroy(&attr);
        return MBError::MUTEX_ERROR;
    }
#endif
    if (pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED)) {
        Logger::Log(LOG_LEVEL_WARN, "failed to set mutex/PTHREAD_PROCESS_SHARED");
        pthread_mutexattr_destroy(&attr);
        return MBError::MUTEX_ERROR;
    }

    if (pthread_mutex_init(mutex, &attr)) {
        Logger::Log(LOG_LEVEL_WARN, "pthread_mutex_init failed");
        pthread_mutexattr_destroy(&attr);
        return MBError::MUTEX_ERROR;
    }
    pthread_mutexattr_destroy(&attr);

    return MBError::SUCCESS;
}

}
