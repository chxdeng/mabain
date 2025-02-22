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

#include <iostream>
#include <sys/stat.h>

#include "error.h"
#include "logger.h"
#include "resource_pool.h"
#include "shm_queue_mgr.h"
#include "util/shm_mutex.h"

namespace mabain {

ShmQueueMgr::ShmQueueMgr()
{
}

void ShmQueueMgr::InitShmObjects(shm_lock_and_queue* slaq, int queue_size)
{
    int rval = MBError::SUCCESS;

    rval = InitShmMutex(&slaq->lock);
    if (rval != MBError::SUCCESS)
        throw rval;

    slaq->initialized = 1;
}

shm_lock_and_queue* ShmQueueMgr::CreateFile(uint64_t qid, int qsize,
    const char* queue_dir, int options)
{
    if (qsize > MB_MAX_NUM_SHM_QUEUE_NODE)
        throw(int) MBError::INVALID_SIZE;
    std::string qfile_path;
    if (queue_dir != NULL)
        qfile_path = std::string(queue_dir) + "/_mabain_q" + std::to_string(qid);
    else
        qfile_path = "/dev/shm/_mabain_q" + std::to_string(qid);

    bool init_queue = false;
    if (access(qfile_path.c_str(), F_OK))
        init_queue = true;

    bool map_qfile = true;
    int q_buff_size = sizeof(shm_lock_and_queue);
    if (qsize < MB_MAX_NUM_SHM_QUEUE_NODE)
        q_buff_size -= sizeof(AsyncNode) * (MB_MAX_NUM_SHM_QUEUE_NODE - qsize);
    qfile = ResourcePool::getInstance().OpenFile(qfile_path,
        CONSTS::ACCESS_MODE_WRITER,
        q_buff_size,
        map_qfile,
        options & CONSTS::ACCESS_MODE_WRITER);
    shm_lock_and_queue* slaq = NULL;
    if (qfile != NULL && map_qfile)
        slaq = reinterpret_cast<shm_lock_and_queue*>(qfile->GetMapAddr());
    if (slaq == NULL)
        throw(int) MBError::MMAP_FAILED;

    if (options & CONSTS::ACCESS_MODE_WRITER) {
        if (init_queue)
            slaq->initialized = 0;
        if (slaq->initialized == 0) {
            Logger::Log(LOG_LEVEL_DEBUG, "initializing shared memory queue");
            InitShmObjects(slaq, qsize);
        }
    } else {
        if (slaq->initialized == 0) {
            Logger::Log(LOG_LEVEL_ERROR, "shared memory queue not intialized");
            throw(int) MBError::NOT_INITIALIZED;
        }
    }

    return slaq;
}

ShmQueueMgr::~ShmQueueMgr()
{
}

}
