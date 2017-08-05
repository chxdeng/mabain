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

#ifndef __DB_H__
#define __DB_H__

#include <iostream>
#include <string>

#include "mb_data.h"
#include "error.h"
#include "lock.h"

#define DB_ITER_STATE_INIT         0x00
#define DB_ITER_STATE_MORE         0x01
#define DB_ITER_STATE_DONE         0x02
#define DATA_BLOCK_SIZE            128LLU*1024*1024
#define INDEX_BLOCK_SIZE           128LLU*1024*1024
#define BUFFER_TYPE_NONE           0
#define BUFFER_TYPE_EDGE_STR       1
#define BUFFER_TYPE_NODE           2
#define MATCH_NONE                 0
#define MATCH_EDGE                 1
#define MATCH_NODE                 2

namespace mabain {

class Dict;
class MBlsq;
class MBShrink;
class AsyncWriter;
struct _IndexNode;

// Database handle class
class DB
{
public:
    // DB iterator class as an inner class
    class iterator
    {
    friend class MBShrink;

    public:
        std::string key;
        MBData value;

        iterator(const DB &db, int iter_state);
        // Copy constructor
        iterator(const iterator &rhs);
        void init();
        int Initialize();
        ~iterator();

        // operator overloading
        bool operator!=(const iterator &rhs);
        const iterator& operator++();

    private:
        class iter_node
        {
        public:
            size_t node_off;
            std::string key;
            size_t parent_edge_off;
            uint32_t node_counter;
            iter_node(size_t offset, const std::string &ckey, size_t edge_off, uint32_t counter);
        };

        iterator* next();
        void iter_obj_init();
        int  next_index_buffer(size_t &parent_node_off, struct _IndexNode *inp);
        int  node_offset_modified(const std::string &key, size_t node_off, MBData &mbd);
        int  edge_offset_modified(const std::string &key, size_t edge_off, MBData &mbd);

        const DB &db_ref;
        int state;
        EdgePtrs edge_ptrs;
        // temp buffer to hold the node
        uint8_t node_buff[NUM_ALPHABET+NODE_EDGE_KEY_FIRST];
        int match;
        MBlsq *node_stack;
        std::string curr_key;
        std::string match_str;
        uint32_t curr_node_counter;
        size_t curr_node_offset;
    };

    // db_path: database directory
    // db_options: db access option (read/write)
    // memcap_index: maximum memory size in bytes for key index
    // memcap_data: maximum memory size for data file mapping
    // data_size: the value size; if zero, the value size will be variable.
    // id: the connector id
    DB(const std::string &db_path, int db_options, size_t memcap_index=64*1024*1024LL,
       size_t memcap_data=0, int data_size = 0, uint32_t  id = 0);
    ~DB();

    // Add a key-value pair
    int Add(const char* key, int len, const char* data, int data_len=0,
            bool overwrite=false);
    int Add(const char* key, int len, MBData &data, bool overwrite=false);
    int Add(const std::string &key, const std::string &value, bool overwrite=false);
    // Find an entry by exact match using a key
    int Find(const char* key, int len, MBData &mdata) const;
    int Find(const std::string &key, MBData &mdata) const;
    // Find all possible prefix matches using a key
    // This is not fully implemented yet.
    int FindPrefix(const char* key, int len, MBData &data) const;
    // Find the longest prefix match using a key
    int FindLongestPrefix(const char* key, int len, MBData &data) const;
    int FindLongestPrefix(const std::string &key, MBData &data) const;
    // Remove an entry using a key
    int Remove(const char *key, int len, MBData &data);
    int Remove(const char *key, int len);
    int Remove(const std::string &key);
    int RemoveAll();
    // Close the DB handle
    int Close();

    // DB shrink
    int Shrink(size_t min_index_shk_size = INDEX_BLOCK_SIZE/2,
               size_t min_data_shk_size  = DATA_BLOCK_SIZE/2);

    // multi-thread or multi-process concurrency/locking
    int WrLock();
    int RdLock();
    int UnLock();
    int TryWrLock();
    int ClearLock() const;

    int UpdateNumHandlers(int mode, int delta);

    // level 0: only error will be logged.
    // level 1: error and warn will be logged.
    // level 2: error, warn, and info will be logged. This is the default setting.
    // level 3: error, warn, info and debug will be logged.
    static int  SetLogLevel(int level);

    // Print database stats
    void PrintStats(std::ostream &out_stream = std::cout) const;
    // current count of key-value pair
    int64_t Count() const;
    // DB status
    int Status() const;
    // DB status string
    const char *StatusStr() const;
    // Check if DB is opened successfully
    bool is_open() const;

    Dict* GetDictPtr() const;

    //iterator
    const iterator begin() const;
    const iterator end() const;

private:
    Dict *dict;
    int status;
    // DB connector ID
    uint32_t identifier;

    // db lock
    MBLock lock;

    AsyncWriter *async_writer;
};

}

#endif
