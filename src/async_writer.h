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

#ifndef __SHM_QUEUE__
    void UpdateNumUsers(int delta);
    int  Add(const char *key, int key_len, const char *data, int data_len, bool overwrite);
    int  Remove(const char *key, int len);
    int  RemoveAll();
    int  Backup(const char *backup_dir);
    int  CollectResource(int64_t m_index_rc_size, int64_t m_data_rc_size, 
                         int64_t max_dbsz, int64_t max_dbcnt);
    bool Busy() const;
#endif

    int  StopAsyncThread();
    int  ProcessTask(int ntasks, bool rc_mode);
    int  AddWithLock(const char *key, int len, MBData &mbdata, bool overwrite);

    static AsyncWriter* CreateInstance(DB *db_ptr);
    static AsyncWriter* GetInstance();

private:
    AsyncWriter(DB *db_ptr);
    static void *async_thread_wrapper(void *context);
    AsyncNode* AcquireSlot();
    int PrepareSlot(AsyncNode *node_ptr) const;
    void* async_writer_thread();
#ifdef __SHM_QUEUE__
    uint32_t NextShmSlot(uint32_t windex, uint32_t qindex);
#endif

    // db pointer
    DB *db;
    Dict *dict;

    // thread id
    pthread_t tid;
    bool stop_processing;

    AsyncNode *queue;
#ifdef __SHM_QUEUE__
    IndexHeader *header;
#else
    std::atomic<int> num_users;
    std::atomic<uint32_t> queue_index;
    uint32_t writer_index;
#endif

    bool is_rc_running;
    char *rc_backup_dir;

    std::timed_mutex writer_lock;
    static AsyncWriter *writer_instance;
};

}

#endif
