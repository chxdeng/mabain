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
#include "mb_shrink.h"

namespace mabain {

const int AsyncWriter::max_num_queue_node = 2048;

static void free_async_node(AsyncNode *node_ptr)
{
    if(node_ptr == NULL)
        return;
    if(node_ptr->key)
        free(node_ptr->key);
    if(node_ptr->data)
        free(node_ptr->data);
    free(node_ptr);
}

AsyncWriter::AsyncWriter(DB *db_ptr) : db(db_ptr)
{
    dict = NULL;
    queue = new MBlsq(NULL);
    q_mutex = PTHREAD_MUTEX_INITIALIZER;
    q_cond = PTHREAD_COND_INITIALIZER;
    stop_processing = false;
    min_index_shrink_size = 0;
    min_data_shrink_size = 0;

    if(db == NULL)
        throw (int) MBError::NOT_INITIALIZED;
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
    delete queue;
}

void AsyncWriter::StopAsyncThread()
{
    stop_processing = true;

    pthread_mutex_lock(&q_mutex);
    pthread_cond_signal(&q_cond);
    pthread_mutex_unlock(&q_mutex);

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

int  AsyncWriter::Shrink(size_t min_index_shk_size, size_t min_data_shk_size)
{
    AsyncNode *node_ptr = (AsyncNode *) calloc(1, sizeof(*node_ptr));
    if(node_ptr == NULL)
        return MBError::NO_MEMORY;
    node_ptr->type = MABAIN_ASYNC_TYPE_SHRINK;
    min_index_shrink_size = min_index_shk_size;
    min_data_shrink_size = min_data_shk_size;

    return Enqueue(node_ptr);
}

int AsyncWriter::Enqueue(AsyncNode *node_ptr)
{
    int rval;
    int64_t queue_cnt;

    pthread_mutex_lock(&q_mutex);
    queue_cnt = queue->Count();
    if(queue_cnt > max_num_queue_node)
        pthread_cond_wait(&q_cond, &q_mutex);

    // add to queue
    rval = queue->AddToTail(node_ptr);
    pthread_cond_signal(&q_cond);
    pthread_mutex_unlock(&q_mutex);

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
        pthread_mutex_lock(&q_mutex);
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
        pthread_mutex_unlock(&q_mutex);

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
                break;
            case MABAIN_ASYNC_TYPE_REMOVE_ALL:
                rval = dict->RemoveAll();
                if(rval != MBError::SUCCESS)
                    Logger::Log(LOG_LEVEL_ERROR, "failed to delete all from db %s", MBError::get_error_str(rval)); 
                break;
            case MABAIN_ASYNC_TYPE_SHRINK:
                {
                    MBShrink shk(*db);
                    shk.Shrink(min_index_shrink_size, min_data_shrink_size);
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
