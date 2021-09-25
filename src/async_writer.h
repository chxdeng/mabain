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

#ifndef __ASYNC_WRITER_H__
#define __ASYNC_WRITER_H__

#include <mutex>
#include <pthread.h>

#include "db.h"
#include "dict.h"
#include "mb_backup.h"
#include "error.h"

namespace mabain {

class AsyncWriter
{
public:
    ~AsyncWriter();

    int  StopAsyncThread();
    int  ProcessTask(int ntasks, bool rc_mode);
    int  AddWithLock(const char *key, int len, MBData &mbdata);

    static AsyncWriter* CreateInstance(DB *db_ptr);
    static AsyncWriter* GetInstance();

private:
    AsyncWriter(DB *db_ptr);
    static void *async_thread_wrapper(void *context);
    AsyncNode* AcquireSlot();
    int PrepareSlot(AsyncNode *node_ptr) const;
    void* async_writer_thread();
    uint32_t NextShmSlot(uint32_t windex, uint32_t qindex);

    // db pointer
    DB *db;
    Dict *dict;

    // thread id
    pthread_t tid;
    bool stop_processing;

    AsyncNode *queue;
    IndexHeader *header;

    bool is_rc_running;
    char *rc_backup_dir;
    bool remove_original;

    std::timed_mutex writer_lock;
    static AsyncWriter *writer_instance;
};

}

#endif
