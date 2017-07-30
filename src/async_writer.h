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

#include <pthread.h>

#include "db.h"
#include "util/mb_lsq.h"
#include "dict.h"

namespace mabain {

#define MABAIN_ASYNC_TYPE_ADD        1
#define MABAIN_ASYNC_TYPE_REMOVE     2
#define MABAIN_ASYNC_TYPE_REMOVE_ALL 3
#define MABAIN_ASYNC_TYPE_SHRINK     4

typedef struct _AsyncNode
{
    char *key;
    char *data;
    int key_len;
    int data_len;
    bool overwrite;
    char type;
} AsyncNode;

class AsyncWriter
{
public:

    AsyncWriter(DB *db_ptr);
    ~AsyncWriter();

    // async add/remove by enqueue
    int  Add(const char *key, int key_len, const char *data, int data_len, bool overwrite);
    int  Remove(const char *key, int len);
    int  RemoveAll();
    int  Shrink(size_t min_index_shk_size, size_t min_data_shk_size);
    void StopAsyncThread();

private:
    static void *async_thread_wrapper(void *context);
    static const int max_num_queue_node;

    void* async_writer_thread();
    int Enqueue(AsyncNode *node_ptr);

    // db pointer
    DB *db;
    Dict *dict;

    // async queue
    MBlsq *queue;
    // queue mutex
    pthread_mutex_t q_mutex;
    pthread_cond_t  q_cond;
    // thread id
    pthread_t tid;

    bool stop_processing;
    size_t min_index_shrink_size;
    size_t min_data_shrink_size;
};

}

#endif
