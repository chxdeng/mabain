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

#include <pthread.h>

#include "dict.h"
#include "error.h"
#include "async_writer.h"

#ifdef __SHM_QUEUE__

namespace mabain {

int Dict::SHMQ_Add(const char *key, int key_len, const char *data, int data_len,
                   bool overwrite)
{
    if(key_len > MB_ASYNC_SHM_KEY_SIZE || data_len > MB_ASYNC_SHM_DATA_SIZE)
    {
        return MBError::OUT_OF_BOUND;
    }

    AsyncNode *node_ptr = SHMQ_AcquireSlot();
    if(node_ptr == NULL)
        return MBError::MUTEX_ERROR;

    memcpy(node_ptr->key, key, key_len);
    memcpy(node_ptr->data, data, data_len);
    node_ptr->key_len = key_len;
    node_ptr->data_len = data_len;
    node_ptr->overwrite = overwrite;

    node_ptr->type = MABAIN_ASYNC_TYPE_ADD;
    return SHMQ_PrepareSlot(node_ptr);
}

int Dict::SHMQ_Remove(const char *key, int len)
{
    if(len > MB_ASYNC_SHM_KEY_SIZE)
        return MBError::OUT_OF_BOUND;

    AsyncNode *node_ptr = SHMQ_AcquireSlot();
    if(node_ptr == NULL)
        return MBError::MUTEX_ERROR;

    memcpy(node_ptr->key, key, len);
    node_ptr->key_len = len;
    node_ptr->type = MABAIN_ASYNC_TYPE_REMOVE;
    return SHMQ_PrepareSlot(node_ptr);
}

int Dict::SHMQ_RemoveAll()
{
    AsyncNode *node_ptr = SHMQ_AcquireSlot();
    if(node_ptr == NULL)
        return MBError::MUTEX_ERROR;

    node_ptr->type = MABAIN_ASYNC_TYPE_REMOVE_ALL;
    return SHMQ_PrepareSlot(node_ptr);
}

int Dict::SHMQ_Backup(const char *backup_dir)
{
    if(backup_dir == NULL)
        return MBError::INVALID_ARG;
    if(strlen(backup_dir) >= MB_ASYNC_SHM_DATA_SIZE)
        return MBError::OUT_OF_BOUND;

    AsyncNode *node_ptr = SHMQ_AcquireSlot();
    if(node_ptr == NULL)
        return MBError::MUTEX_ERROR;
    snprintf(node_ptr->data, MB_ASYNC_SHM_DATA_SIZE, "%s", backup_dir);
    node_ptr->type = MABAIN_ASYNC_TYPE_BACKUP;
    return SHMQ_PrepareSlot(node_ptr);
}

int Dict::SHMQ_CollectResource(int64_t m_index_rc_size,
                               int64_t m_data_rc_size,
                               int64_t max_dbsz,
                               int64_t max_dbcnt)
{
    AsyncNode *node_ptr = SHMQ_AcquireSlot();
    if(node_ptr == NULL)
        return MBError::MUTEX_ERROR;

    int64_t *data_ptr = (int64_t *) node_ptr->data;
    node_ptr->data_len = sizeof(int64_t)*4;
    data_ptr[0] = m_index_rc_size;
    data_ptr[1] = m_data_rc_size;
    data_ptr[2] = max_dbsz;
    data_ptr[3] = max_dbcnt;
    node_ptr->type = MABAIN_ASYNC_TYPE_RC;

    return SHMQ_PrepareSlot(node_ptr);
}

AsyncNode* Dict::SHMQ_AcquireSlot() const
{
    uint32_t index = header->queue_index.fetch_add(1, std::memory_order_release);
    AsyncNode *node_ptr = queue + (index % MB_MAX_NUM_SHM_QUEUE_NODE);

    struct timespec tm_exp;
    tm_exp.tv_sec = time(NULL) + MB_ASYNC_SHM_LOCK_TMOUT;
    tm_exp.tv_nsec = 0;
    int rval = pthread_mutex_timedlock(&node_ptr->mutex, &tm_exp);
    if(rval != 0)
    {
        Logger::Log(LOG_LEVEL_ERROR, "shared memory mutex lock failed: %d", rval);
        return NULL;
    }

    while(node_ptr->in_use.load(std::memory_order_consume))
    {
        tm_exp.tv_sec += MB_ASYNC_SHM_LOCK_TMOUT;
        rval =  pthread_cond_timedwait(&node_ptr->cond, &node_ptr->mutex, &tm_exp);
        if(rval != 0)
        {
            Logger::Log(LOG_LEVEL_ERROR, "shared memory conditional wait failed: %d", rval);
            if(rval == ETIMEDOUT)
            {
                 Logger::Log(LOG_LEVEL_INFO, "pthread_cond_timedwait timedout, "
                             "check if async writer is running");
            }
            if((rval = pthread_mutex_unlock(&node_ptr->mutex)) != 0)
                Logger::Log(LOG_LEVEL_ERROR, "shm mutex unlock failed: %d", rval);
            return NULL;
        }
    }

    return node_ptr;
}

int Dict::SHMQ_PrepareSlot(AsyncNode *node_ptr) const
{
    node_ptr->in_use.store(true, std::memory_order_release);
    pthread_cond_signal(&node_ptr->cond);
    if(pthread_mutex_unlock(&node_ptr->mutex) != 0)
        return MBError::MUTEX_ERROR;

    return MBError::SUCCESS;
}

bool Dict::SHMQ_Busy() const
{
    if((header->queue_index.load(std::memory_order_consume) != header->writer_index) || header->rc_flag == 1)
        return true;

    size_t rc_off = header->rc_root_offset.load(std::memory_order_consume);
    return rc_off != 0;
}

}

#endif
