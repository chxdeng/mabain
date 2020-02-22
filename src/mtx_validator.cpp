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

#include "mtx_validator.h"
#include "resource_pool.h"

namespace mabain {

MutexValidator::MutexValidator(const std::string &mbdir, const MBConfig &config)
{
    is_running = true;

    assert(sizeof(IndexHeader) <= (unsigned) RollableFile::page_size);
    bool map_hdr = true;
    size_t hdr_size = RollableFile::page_size;
#ifdef __SHM_QUEUE__
    hdr_size += sizeof(AsyncNode) * config.queue_size;
#endif
    header_file = ResourcePool::getInstance().OpenFile(mbdir + "_mabain_h",
                                                       config.options,
                                                       hdr_size,
                                                       map_hdr,
                                                       false);
    if(header_file == NULL)
    {
        std::cerr << "failed top open header file: " << mbdir + "_mabain_h\n";
        throw (int) MBError::OPEN_FAILURE;
    }
    header = reinterpret_cast<IndexHeader *>(header_file->GetMapAddr());
    if(header == NULL)
        throw (int) MBError::MMAP_FAILED;

    if((int) config.queue_size != header->async_queue_size)
        throw (int) MBError::INVALID_SIZE;

    tid = std::thread( [this]() { ValidateMutex(); } );
    tid.detach();
}

bool MutexValidator::IsRunning()
{
    return is_running;
}

MutexValidator::~MutexValidator()
{
}

void MutexValidator::InvalidateDBVersion()
{
    if(header != NULL) header->version[0] = 0;
    header_file->Flush();
}

void MutexValidator::ValidateMutex()
{
    pthread_rwlock_t *db_lck = &(header->mb_rw_lock);
    int rval = pthread_rwlock_rdlock(db_lck);
    if(0 != rval)
    {
        std::cerr << "mutex validation rwlock rdlock failed: " << rval << std::endl;
        return;
    }
    pthread_rwlock_unlock(db_lck);

    pthread_rwlock_wrlock(db_lck);
    if(0 != rval)
    {
        std::cerr << "mutex validation rwlock wrlock failed: " << rval << std::endl;
        return;
    }
    pthread_rwlock_unlock(db_lck);

#ifdef __SHM_QUEUE__
    AsyncNode *queue = reinterpret_cast<AsyncNode *>((char*)header + RollableFile::page_size);
    for(int i = 0; i < header->async_queue_size; i++)
    {
        rval = pthread_mutex_lock(&queue[i].mutex);
        if(0 != rval)
        {
            std::cerr << "mutex validation shmq mutex lock failed: " << i << " " << rval << std::endl;
            return;
        }
        pthread_mutex_unlock(&queue[i].mutex);
    }
#endif

    is_running = false;
}

}
