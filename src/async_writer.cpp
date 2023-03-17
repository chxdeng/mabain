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

#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include "async_writer.h"
#include "dict.h"
#include "error.h"
#include "integer_4b_5b.h"
#include "logger.h"
#include "mb_data.h"
#include "mb_rc.h"

namespace mabain {

AsyncWriter* AsyncWriter::writer_instance = NULL;

AsyncWriter* AsyncWriter::CreateInstance(DB* db_ptr)
{
    writer_instance = new AsyncWriter(db_ptr);
    return writer_instance;
}

AsyncWriter* AsyncWriter::GetInstance()
{
    return writer_instance;
}

AsyncWriter::AsyncWriter(DB* db_ptr)
    : db(db_ptr)
    , tid(0)
    , stop_processing(false)
    , queue(NULL)
    , header(NULL)
{
    dict = NULL;
    if (!(db_ptr->GetDBOptions() & CONSTS::ACCESS_MODE_WRITER))
        throw(int) MBError::NOT_ALLOWED;
    if (db == NULL)
        throw(int) MBError::INVALID_ARG;
    dict = db->GetDictPtr();
    if (dict == NULL)
        throw(int) MBError::NOT_INITIALIZED;

    // initialize shared memory queue pointer
    header = dict->GetHeaderPtr();
    if (header == NULL)
        throw(int) MBError::NOT_INITIALIZED;
    queue = dict->GetAsyncQueuePtr();
    header->rc_flag.store(0, std::memory_order_release);

    rc_backup_dir = NULL;
    // start the thread
    if (pthread_create(&tid, NULL, async_thread_wrapper, this) != 0) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to create async thread");
        tid = 0;
        throw(int) MBError::THREAD_FAILED;
    }
}

AsyncWriter::~AsyncWriter()
{
}

int AsyncWriter::StopAsyncThread()
{
    stop_processing = true;
    dict->SHMQ_Signal();

    if (tid != 0) {
        Logger::Log(LOG_LEVEL_DEBUG, "joining async writer thread");
        pthread_join(tid, NULL);
    }

    return MBError::SUCCESS;
}

// Run a given number of tasks if they are available.
// This function should only be called by rc or pruner.
int AsyncWriter::ProcessTask(int ntasks, bool rc_mode)
{
    AsyncNode* node_ptr;
    MBData mbd;
    int rval = MBError::SUCCESS;
    int count = 0;

    while (count < ntasks) {
        node_ptr = &queue[header->writer_index % header->async_queue_size];

        if (node_ptr->in_use.load(std::memory_order_consume)) {
            switch (node_ptr->type) {
            case MABAIN_ASYNC_TYPE_ADD:
                if (rc_mode)
                    mbd.options = CONSTS::OPTION_RC_MODE;
                mbd.buff = (uint8_t*)node_ptr->data;
                mbd.data_len = node_ptr->data_len;
                try {
                    rval = dict->Add((uint8_t*)node_ptr->key, node_ptr->key_len, mbd, node_ptr->overwrite);
                } catch (int err) {
                    rval = err;
                    Logger::Log(LOG_LEVEL_ERROR, "dict->Add throws error %s",
                        MBError::get_error_str(err));
                }
                break;
            case MABAIN_ASYNC_TYPE_REMOVE:
                // FIXME
                // Removing entries during rc is currently not supported.
                // The index or data index could have been reset in fucntion ResourceColletion::Finish.
                // However, the deletion may be run for some entry which still exist in the rc root tree.
                // This requires modifying buffer in high end, where the offset for writing is greather than
                // header->m_index_offset. This causes exception thrown from DictMem::WriteData.
                // Note this is not a problem for Dict::Add since Add does not modify buffers in high end.
                // This problem will be fixed when resolving issue: https://github.com/chxdeng/mabain/issues/21
                rval = MBError::SUCCESS;
                break;
            case MABAIN_ASYNC_TYPE_REMOVE_ALL:
                if (!rc_mode) {
                    try {
                        rval = dict->RemoveAll();
                    } catch (int err) {
                        Logger::Log(LOG_LEVEL_ERROR, "dict->Add throws error %s",
                            MBError::get_error_str(err));
                        rval = err;
                    }
                } else {
                    rval = MBError::SUCCESS;
                }
                break;
            case MABAIN_ASYNC_TYPE_RC:
                // ignore rc task since it is running already.
                rval = MBError::RC_SKIPPED;
                break;
            case MABAIN_ASYNC_TYPE_NONE:
                rval = MBError::SUCCESS;
                break;
            case MABAIN_ASYNC_TYPE_BACKUP:
                // clean up existing backup dir varibale buffer.
                if (rc_backup_dir != NULL)
                    free(rc_backup_dir);
                rc_backup_dir = (char*)malloc(node_ptr->data_len + 1);
                memcpy(rc_backup_dir, node_ptr->data, node_ptr->data_len);
                rc_backup_dir[node_ptr->data_len] = '\0';
                rval = MBError::SUCCESS;
                break;
            default:
                rval = MBError::INVALID_ARG;
                break;
            }

            header->writer_index++;
            node_ptr->num_reader.store(0, std::memory_order_release);
            node_ptr->type = MABAIN_ASYNC_TYPE_NONE;
            node_ptr->in_use.store(false, std::memory_order_release);
            mbd.Clear();
            count++;
        } else {
            // done processing
            count = ntasks;
        }

        if (rval != MBError::SUCCESS) {
            Logger::Log(LOG_LEVEL_DEBUG, "failed to run update %d: %s",
                (int)node_ptr->type, MBError::get_error_str(rval));
        }
    }

    if (stop_processing)
        return MBError::RC_SKIPPED;
    return MBError::SUCCESS;
}

uint32_t AsyncWriter::NextShmSlot(uint32_t windex, uint32_t qindex)
{
    int cnt = 0;
    while (windex != qindex) {
        if (queue[windex % header->async_queue_size].in_use.load(std::memory_order_consume))
            break;
        if (++cnt > header->async_queue_size) {
            windex = qindex;
            break;
        }

        windex++;
    }

    return windex;
}

void* AsyncWriter::async_writer_thread()
{
    AsyncNode* node_ptr;
    MBData mbd;
    int rval;
    int64_t min_index_size = 0;
    int64_t min_data_size = 0;
    int64_t max_dbsize = MAX_6B_OFFSET;
    int64_t max_dbcount = MAX_6B_OFFSET;
    bool skip;
    MBPipe mbp(db->GetDBDir(), CONSTS::ACCESS_MODE_WRITER);

    Logger::Log(LOG_LEVEL_DEBUG, "async writer started");
    ResourceCollection rc(*db);
    writer_lock.lock();
    rc.ExceptionRecovery();
    writer_lock.unlock();

    while (!stop_processing) {
        node_ptr = &queue[header->writer_index % header->async_queue_size];

        skip = false;
        while (!node_ptr->in_use.load(std::memory_order_consume)) {
            if (stop_processing) {
                skip = true;
                break;
            }

#define __ASYNC_THREAD_SLEEP_TIME 1000
            mbp.Wait(__ASYNC_THREAD_SLEEP_TIME);

            auto windex = header->writer_index;
            auto qindex = header->queue_index.load(std::memory_order_consume);
            if (windex != qindex) {
                // Reader process may have exited unexpectedly. Recover index.
                skip = true;
                header->writer_index = NextShmSlot(windex, qindex);
                break;
            }
        }

        if (skip)
            continue;

        // process the node
        switch (node_ptr->type) {
        case MABAIN_ASYNC_TYPE_ADD:
            mbd.buff = (uint8_t*)node_ptr->data;
            mbd.data_len = node_ptr->data_len;
            writer_lock.lock();
            try {
                rval = dict->Add((uint8_t*)node_ptr->key, node_ptr->key_len, mbd,
                    node_ptr->overwrite);
            } catch (int err) {
                Logger::Log(LOG_LEVEL_ERROR, "dict->Add throws error %s",
                    MBError::get_error_str(err));
                rval = err;
            }
            writer_lock.unlock();
            break;
        case MABAIN_ASYNC_TYPE_REMOVE:
            mbd.options |= CONSTS::OPTION_FIND_AND_STORE_PARENT;
            writer_lock.lock();
            try {
                rval = dict->Remove((uint8_t*)node_ptr->key, node_ptr->key_len, mbd);
            } catch (int err) {
                Logger::Log(LOG_LEVEL_ERROR, "dict->Remmove throws error %s",
                    MBError::get_error_str(err));
                rval = err;
            }
            writer_lock.unlock();
            mbd.options &= ~CONSTS::OPTION_FIND_AND_STORE_PARENT;
            break;
        case MABAIN_ASYNC_TYPE_REMOVE_ALL:
            writer_lock.lock();
            try {
                rval = dict->RemoveAll();
            } catch (int err) {
                Logger::Log(LOG_LEVEL_ERROR, "dict->RemoveAll throws error %s",
                    MBError::get_error_str(err));
                rval = err;
            }
            writer_lock.unlock();
            break;
        case MABAIN_ASYNC_TYPE_RC:
            rval = MBError::SUCCESS;
            header->rc_flag.store(1, std::memory_order_release);
            {
                int64_t* data_ptr = reinterpret_cast<int64_t*>(node_ptr->data);
                min_index_size = data_ptr[0];
                min_data_size = data_ptr[1];
                max_dbsize = data_ptr[2];
                max_dbcount = data_ptr[3];
            }
            break;
        case MABAIN_ASYNC_TYPE_NONE:
            rval = MBError::SUCCESS;
            break;
        case MABAIN_ASYNC_TYPE_BACKUP:
            try {
                DBBackup mbbk(*db);
                rval = mbbk.Backup((const char*)node_ptr->data);
            } catch (int error) {
                rval = error;
            }
            break;
        default:
            rval = MBError::INVALID_ARG;
            break;
        }

        header->writer_index++;
        node_ptr->num_reader.store(0, std::memory_order_release);
        node_ptr->type = MABAIN_ASYNC_TYPE_NONE;
        node_ptr->in_use.store(false, std::memory_order_release);

        if (rval != MBError::SUCCESS) {
            Logger::Log(LOG_LEVEL_DEBUG, "failed to run update %d: %s",
                (int)node_ptr->type, MBError::get_error_str(rval));
        }

        mbd.Clear();

        if (header->rc_flag.load(std::memory_order_consume) == 1) {
            rval = MBError::SUCCESS;
            writer_lock.lock();
            try {
                ResourceCollection rc = ResourceCollection(*db);
                rc.ReclaimResource(min_index_size, min_data_size, max_dbsize, max_dbcount, this);
            } catch (int error) {
                if (error != MBError::RC_SKIPPED) {
                    Logger::Log(LOG_LEVEL_WARN, "rc failed :%s", MBError::get_error_str(error));
                    rval = error;
                }
            }
            writer_lock.unlock();

            header->rc_flag.store(0, std::memory_order_release);
            if (rc_backup_dir != NULL) {
                if (rval == MBError::SUCCESS) {
                    dict->SHMQ_Backup(rc_backup_dir);
                }
                free(rc_backup_dir);
                rc_backup_dir = NULL;
            }
        }
    }

    mbd.buff = NULL;
    Logger::Log(LOG_LEVEL_DEBUG, "async writer exiting");
    return NULL;
}

void* AsyncWriter::async_thread_wrapper(void* context)
{
    AsyncWriter* instance_ptr = reinterpret_cast<AsyncWriter*>(context);
    return instance_ptr->async_writer_thread();
}

int AsyncWriter::AddWithLock(const char* key, int len, MBData& mbdata, bool overwrite)
{
    if (header->rc_flag.load(std::memory_order_relaxed))
        return MBError::TRY_AGAIN;

    using Ms = std::chrono::milliseconds;
    if (writer_lock.try_lock_for(Ms(1000))) {
        int rval;
        try {
            rval = dict->Add(reinterpret_cast<const uint8_t*>(key), len, mbdata, overwrite);
        } catch (int error) {
            rval = error;
        }
        writer_lock.unlock();
        return rval;
    }

    return MBError::TRY_AGAIN;
}

}
