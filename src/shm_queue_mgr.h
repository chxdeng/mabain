/**
 * Copyright (C) 2020 Cisco Inc.
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

#ifndef __SHM_QUEUE_MGR_H__
#define __SHM_QUEUE_MGR_H__

#include <atomic>

#include "db.h"

namespace mabain {

#define MABAIN_ASYNC_TYPE_NONE       0
#define MABAIN_ASYNC_TYPE_ADD        1
#define MABAIN_ASYNC_TYPE_REMOVE     2
#define MABAIN_ASYNC_TYPE_REMOVE_ALL 3
#define MABAIN_ASYNC_TYPE_RC         4
#define MABAIN_ASYNC_TYPE_BACKUP     5
#define MABAIN_ASYNC_TYPE_FREEZE     6

#define MB_ASYNC_SHM_KEY_SIZE      256
#define MB_ASYNC_SHM_DATA_SIZE     1024
#define MB_ASYNC_SHM_LOCK_TMOUT    5

typedef struct _AsyncNode
{
    std::atomic<bool> in_use;
    std::atomic<uint16_t> num_reader;

    char key[MB_ASYNC_SHM_KEY_SIZE];
    char data[MB_ASYNC_SHM_DATA_SIZE];
    int key_len;
    int data_len;
    bool overwrite;
    char type;
} AsyncNode;

typedef struct _shm_lock_and_queue
{
    int initialized;
    pthread_mutex_t lock;
    AsyncNode queue[MB_MAX_NUM_SHM_QUEUE_NODE];
} shm_lock_and_queue;

class ShmQueueMgr
{
public:
    ShmQueueMgr();
    ~ShmQueueMgr();
    shm_lock_and_queue* CreateFile(uint64_t qid, int qsize, const char *queue_dir, int options);

private:
    void InitShmObjects(shm_lock_and_queue *slaq, int queue_size);
};

}

#endif
