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

#include <unistd.h>
#include <string.h>

#include "async_writer.h"
#include "error.h"
#include "logger.h"
#include "mb_data.h"
#include "mb_rc.h"

namespace mabain {

const int AsyncWriter::max_num_queue_node = 2048;

static void free_async_node(AsyncNode *node_ptr)
{
    if(node_ptr == NULL)
        return;
    if(node_ptr->key != NULL)
        free(node_ptr->key);
    if(node_ptr->data != NULL)
        free(node_ptr->data);
    free(node_ptr);
}

AsyncWriter::AsyncWriter(DB *db_ptr) : db(db_ptr), queue(NULL), tid(0)
{
    if(!(db_ptr->GetDBOptions() & CONSTS::ACCESS_MODE_WRITER))
        throw (int) MBError::NOT_ALLOWED;

    dict = NULL;
    queue = new MBlsq(NULL);

    if(pthread_mutex_init(&q_mutex, NULL) != 0)
    {
        Logger::Log(LOG_LEVEL_ERROR, "failed to init mutex");
        throw (int) MBError::THREAD_FAILED;
    }
    if(pthread_cond_init(&q_cond, NULL) != 0)
    {
        Logger::Log(LOG_LEVEL_ERROR, "failed to init condition variable");
        throw (int) MBError::THREAD_FAILED;
    }

    stop_processing = false;

    if(db == NULL)
        throw (int) MBError::INVALID_ARG;
    dict = db->GetDictPtr();
    if(dict == NULL) 
        throw (int) MBError::NOT_INITIALIZED;

    // start the thread
    if(pthread_create(&tid, NULL, async_thread_wrapper, this) != 0)
    {
        Logger::Log(LOG_LEVEL_ERROR, "failed to create async thread");
        tid = 0;
        throw (int) MBError::THREAD_FAILED;
    }

}

AsyncWriter::~AsyncWriter()
{
    pthread_mutex_destroy(&q_mutex);
    pthread_cond_destroy(&q_cond);
    if(queue != NULL)
        delete queue;
}

void AsyncWriter::StopAsyncThread()
{
    stop_processing = true;

    if(pthread_mutex_lock(&q_mutex) != 0)
        Logger::Log(LOG_LEVEL_ERROR, "failed to lock async queue mutex");
    pthread_cond_signal(&q_cond);
    if(pthread_mutex_unlock(&q_mutex) != 0)
        Logger::Log(LOG_LEVEL_ERROR, "failed to unlock async queue mutex");

    if(tid != 0)
        pthread_join(tid, NULL);
}

int AsyncWriter::Add(const char *key, int key_len, const char *data,
                     int data_len, bool overwrite)
{
    AsyncNode *node_ptr = (AsyncNode *) malloc(sizeof(*node_ptr));
    if(node_ptr == NULL)
        return MBError::NO_MEMORY;
    node_ptr->key = (char *) malloc(key_len);
    node_ptr->data = (char *) malloc(data_len);
    if(node_ptr->key == NULL || node_ptr->data == NULL)
    {
        free_async_node(node_ptr);
        return MBError::NO_MEMORY;
    }
    memcpy(node_ptr->key, key, key_len);
    memcpy(node_ptr->data, data, data_len);
    node_ptr->key_len = key_len;
    node_ptr->data_len = data_len;
    node_ptr->overwrite = overwrite;

    node_ptr->min_index_rc_size = 0;
    node_ptr->min_data_rc_size = 0;
    node_ptr->type = MABAIN_ASYNC_TYPE_ADD;

    return Enqueue(node_ptr);
}

int AsyncWriter::Remove(const char *key, int len)
{
    AsyncNode *node_ptr = (AsyncNode *) calloc(1, sizeof(*node_ptr));
    if(node_ptr == NULL)
        return MBError::NO_MEMORY;
    node_ptr->key = (char *) malloc(len);
    if(node_ptr->key == NULL)
    {
        free_async_node(node_ptr);
        return MBError::NO_MEMORY;
    }
    memcpy(node_ptr->key, key, len);
    node_ptr->key_len = len;
    node_ptr->type = MABAIN_ASYNC_TYPE_REMOVE;

    return Enqueue(node_ptr);
}

int AsyncWriter::RemoveAll()
{
    AsyncNode *node_ptr = (AsyncNode *) calloc(1, sizeof(*node_ptr));
    if(node_ptr == NULL)
        return MBError::NO_MEMORY;
    node_ptr->type = MABAIN_ASYNC_TYPE_REMOVE_ALL;

    return Enqueue(node_ptr);
}

int  AsyncWriter::CollectResource(int m_index_rc_size, int m_data_rc_size)
{
    AsyncNode *node_ptr = (AsyncNode *) calloc(1, sizeof(*node_ptr));
    if(node_ptr == NULL)
        return MBError::NO_MEMORY;
    node_ptr->min_index_rc_size = m_index_rc_size;
    node_ptr->min_data_rc_size = m_data_rc_size;
    node_ptr->type = MABAIN_ASYNC_TYPE_RC;

    return Enqueue(node_ptr);
}

int AsyncWriter::Enqueue(AsyncNode *node_ptr)
{
    int rval;
    int64_t queue_cnt;

    if(pthread_mutex_lock(&q_mutex) != 0)
    {
        free_async_node(node_ptr);
        Logger::Log(LOG_LEVEL_ERROR, "failed to lock async queue mutex");
        throw (int) MBError::MUTEX_ERROR;
    }

    queue_cnt = queue->Count();
    if(queue_cnt > max_num_queue_node)
        pthread_cond_wait(&q_cond, &q_mutex);

    // add to queue
    rval = queue->AddToTail(node_ptr);
    pthread_cond_signal(&q_cond);

    if(pthread_mutex_unlock(&q_mutex) != 0)
    {
        Logger::Log(LOG_LEVEL_ERROR, "failed to unlock async queue mutex");
        throw (int) MBError::MUTEX_ERROR;
    }

    if(rval != MBError::SUCCESS)
        free_async_node(node_ptr);
    return rval;    
}

void* AsyncWriter::async_writer_thread()
{
    AsyncNode *node_ptr;
    MBData mbd;
    int rval;

    Logger::Log(LOG_LEVEL_INFO, "async writer started");
    while(true)
    {
        if(pthread_mutex_lock(&q_mutex) != 0)
        {
            Logger::Log(LOG_LEVEL_ERROR, "failed to lock async queue mutex");
            throw (int) MBError::MUTEX_ERROR;
        }

        node_ptr = (AsyncNode *) queue->RemoveFromHead(); 
        if(node_ptr == NULL)
        {
            if(!stop_processing)
                pthread_cond_wait(&q_cond, &q_mutex);
        }
        else
        {
            pthread_cond_signal(&q_cond);
        }

        if(pthread_mutex_unlock(&q_mutex) != 0)
        {
            if(node_ptr != NULL)
                free_async_node(node_ptr);
            Logger::Log(LOG_LEVEL_ERROR, "failed to unock async queue mutex");
            throw (int) MBError::MUTEX_ERROR;
        }

        if(node_ptr != NULL)
        {
            // process the node
            switch(node_ptr->type)
            {
                case MABAIN_ASYNC_TYPE_ADD:
                    mbd.buff = (uint8_t *) node_ptr->data;
                    mbd.data_len = node_ptr->data_len;
                    rval = dict->Add((uint8_t *)node_ptr->key, node_ptr->key_len, mbd, node_ptr->overwrite);
                    if(rval != MBError::SUCCESS)
                        Logger::Log(LOG_LEVEL_DEBUG, "failed to add to db %s", MBError::get_error_str(rval)); 
                    break;
                case MABAIN_ASYNC_TYPE_REMOVE:
                    mbd.options |= CONSTS::OPTION_FIND_AND_STORE_PARENT;
                    rval = dict->Remove((uint8_t *)node_ptr->key, node_ptr->key_len, mbd);
                    if(rval != MBError::SUCCESS)
                        Logger::Log(LOG_LEVEL_DEBUG, "failed to delete from db %s", MBError::get_error_str(rval)); 
                    mbd.options &= ~CONSTS::OPTION_FIND_AND_STORE_PARENT;
                    break;
                case MABAIN_ASYNC_TYPE_REMOVE_ALL:
                    rval = dict->RemoveAll();
                    if(rval != MBError::SUCCESS)
                        Logger::Log(LOG_LEVEL_ERROR, "failed to delete all from db %s", MBError::get_error_str(rval)); 
                    break;
                case MABAIN_ASYNC_TYPE_RC:
                    {
                        try {
                            ResourceCollection rc(*db);
                            rc.ReclaimResource(node_ptr->min_index_rc_size, node_ptr->min_data_rc_size);
                        } catch (int error) {
                            if(error != MBError::RC_SKIPPED)
                            {
                                Logger::Log(LOG_LEVEL_ERROR, "failed to run gc: %s",
                                            MBError::get_error_str(error));
                            }
                        }
                    }
                    break; 
                default:
                    break;
            }

            free_async_node(node_ptr);
            mbd.Clear();
        }
        else if(stop_processing)
        {
            if(queue->Count() == 0) // No need to lock at this point since stop_processing is set.
                break;
        }
    }

    mbd.buff = NULL;
    Logger::Log(LOG_LEVEL_INFO, "async writer exiting");
    return NULL;
}

void* AsyncWriter::async_thread_wrapper(void *context)
{
    AsyncWriter *instance_ptr = static_cast<AsyncWriter *>(context);
    return instance_ptr->async_writer_thread();
}

}
