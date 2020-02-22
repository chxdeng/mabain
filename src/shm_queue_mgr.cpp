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

#include "shm_queue_mgr.h"
#include "logger.h"
#include "error.h"
#include "resource_pool.h"

namespace mabain {

ShmQueueMgr::ShmQueueMgr()
{
}

shm_lock_and_queue* ShmQueueMgr::CreateFile(uint64_t qid, int qsize,
                                            const char *queue_dir)
{
    if(qsize > MB_MAX_NUM_SHM_QUEUE_NODE)
        throw (int) MBError::INVALID_SIZE;
    std::string qfile_path;
    if(queue_dir != NULL)
        qfile_path = std::string(queue_dir) + "/_mabain_q" + std::to_string(qid);
    else
        qfile_path = "/dev/shm/_mabain_q" + std::to_string(qid);
    std::shared_ptr<MmapFileIO> qfile;
    bool map_qfile = true;
    int q_buff_size = sizeof(shm_lock_and_queue);
    if(qsize < MB_MAX_NUM_SHM_QUEUE_NODE)
        q_buff_size -= sizeof(AsyncNode) * (MB_MAX_NUM_SHM_QUEUE_NODE - qsize);
    qfile = ResourcePool::getInstance().OpenFile(qfile_path,
                                                 CONSTS::ACCESS_MODE_WRITER,
                                                 q_buff_size,
                                                 map_qfile,
                                                 true);
    shm_lock_and_queue *slaq = NULL;
    if(qfile != NULL && map_qfile)
        slaq =  reinterpret_cast<shm_lock_and_queue *>(qfile->GetMapAddr());
    if(slaq == NULL)
        throw (int) MBError::MMAP_FAILED;

    return slaq;
}

ShmQueueMgr::~ShmQueueMgr()
{
}

}
