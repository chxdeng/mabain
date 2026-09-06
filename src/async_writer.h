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

#include <atomic>
#include <mutex>
#include <pthread.h>

#include "db.h"
#include "dict.h"
#include "drm_base.h"
#include "error.h"
#include "mb_backup.h"
#include "shm_queue_mgr.h"

namespace mabain {

class AsyncWriter {
public:
    ~AsyncWriter();

    int StopAsyncThread();
    int ProcessTask(int ntasks, bool rc_mode);
    int AddWithLock(const char* key, int len, MBData& mbdata, bool overwrite);

    static AsyncWriter* CreateInstance(DB* db_ptr);
    static AsyncWriter* GetInstance();

private:
    AsyncWriter(DB* db_ptr);
    static void* async_thread_wrapper(void* context);
    AsyncNode* AcquireSlot();
    int PrepareSlot(AsyncNode* node_ptr) const;
    void* async_writer_thread();

    // db pointer
    DB* db;
    Dict* dict;

    // thread id
    pthread_t tid;
    std::atomic<bool> stop_processing;

    AsyncNode* queue;
    std::atomic<uint64_t>* reservation_time_ms;
    IndexHeader* header;
    uint64_t reservation_timeout_ms;

    bool is_rc_running;
    char* rc_backup_dir;

    std::timed_mutex writer_lock;
    // Process-local shortcut for producer handles targeting the same DB.
    // Assumes one async-writer DB per process and that it outlives local producers.
    static AsyncWriter* writer_instance;
};

}

#endif
