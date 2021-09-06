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
#include <sys/time.h>

#include "dict.h"
#include "error.h"
#include "async_writer.h"
#include "./util/shm_mutex.h"

namespace mabain {

int Dict::SHMQ_Add(const char *key, int key_len, const char *data, int data_len,
                   bool overwrite)
{
    if(key_len > MB_ASYNC_SHM_KEY_SIZE || data_len > MB_ASYNC_SHM_DATA_SIZE)
    {
        return MBError::OUT_OF_BOUND;
    }

    int err = MBError::SUCCESS;
    AsyncNode *node_ptr = SHMQ_AcquireSlot(err);
    if(node_ptr == nullptr)
        return err;

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

    int err = MBError::SUCCESS;
    AsyncNode *node_ptr = SHMQ_AcquireSlot(err);
    if(node_ptr == nullptr)
        return err;

    memcpy(node_ptr->key, key, len);
    node_ptr->key_len = len;
    node_ptr->type = MABAIN_ASYNC_TYPE_REMOVE;
    return SHMQ_PrepareSlot(node_ptr);
}

int Dict::SHMQ_RemoveAll()
{
    int err = MBError::SUCCESS;
    AsyncNode *node_ptr = SHMQ_AcquireSlot(err);
    if(node_ptr == nullptr)
        return err;

    node_ptr->type = MABAIN_ASYNC_TYPE_REMOVE_ALL;
    return SHMQ_PrepareSlot(node_ptr);
}

int Dict::SHMQ_Backup(const char *backup_dir, bool remove_original)
{
    if(backup_dir == nullptr)
        return MBError::INVALID_ARG;
    if(strlen(backup_dir) >= MB_ASYNC_SHM_DATA_SIZE)
        return MBError::OUT_OF_BOUND;

    int err = MBError::SUCCESS;
    AsyncNode *node_ptr = SHMQ_AcquireSlot(err);
    if(node_ptr == nullptr)
        return err;
    snprintf(node_ptr->data, MB_ASYNC_SHM_DATA_SIZE, "%s", backup_dir);
    if (remove_original)
        node_ptr->type = MABAIN_ASYNC_TYPE_FREEZE;
    else
        node_ptr->type = MABAIN_ASYNC_TYPE_BACKUP;
    return SHMQ_PrepareSlot(node_ptr);
}

int Dict::SHMQ_CollectResource(int64_t m_index_rc_size,
                               int64_t m_data_rc_size,
                               int64_t max_dbsz,
                               int64_t max_dbcnt)
{
    int err = MBError::SUCCESS;
    AsyncNode *node_ptr = SHMQ_AcquireSlot(err);
    if(node_ptr == nullptr)
        return err;

    int64_t *data_ptr = reinterpret_cast<int64_t *>(node_ptr->data);
    node_ptr->data_len = sizeof(int64_t)*4;
    data_ptr[0] = m_index_rc_size;
    data_ptr[1] = m_data_rc_size;
    data_ptr[2] = max_dbsz;
    data_ptr[3] = max_dbcnt;
    node_ptr->type = MABAIN_ASYNC_TYPE_RC;

    return SHMQ_PrepareSlot(node_ptr);
}

AsyncNode* Dict::SHMQ_AcquireSlot(int &err) const
{
    uint32_t index = header->queue_index.fetch_add(1, std::memory_order_release);
    AsyncNode *node_ptr = queue + (index % header->async_queue_size);

    if(node_ptr->in_use.load(std::memory_order_consume))
    {
        // This slot is being processed by writer.
        err = MBError::TRY_AGAIN;
        return nullptr;
    }

    uint16_t nreader = node_ptr->num_reader.fetch_add(1, std::memory_order_release);
    if(nreader != 0)
    {
        // This slot is being processed by another reader.
        err = MBError::TRY_AGAIN;
        return nullptr;
    }

    return node_ptr;
}

int Dict::SHMQ_PrepareSlot(AsyncNode *node_ptr)
{
    node_ptr->in_use.store(true, std::memory_order_release);

    mbp.Signal();
    return MBError::SUCCESS;
}

void Dict::SHMQ_Signal()
{
    mbp.Signal();
}

bool Dict::SHMQ_Busy() const
{
    if((header->queue_index.load(std::memory_order_consume) != header->writer_index) || header->rc_flag == 1)
        return true;

    size_t rc_off = header->rc_root_offset.load(std::memory_order_consume);
    return rc_off != 0;
}

}
