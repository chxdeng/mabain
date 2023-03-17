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
#include <vector>

#include "error.h"
#include "integer_4b_5b.h"
#include "lock.h"
#include "mb_data.h"

namespace mabain {

#define MB_MAX_NUM_SHM_QUEUE_NODE 8
#define MB_SHM_RETRY_TIMEOUT 1000000 // 1 second

class Dict;
class MBlsq;
class LockFree;
class AsyncWriter;
struct _DBTraverseNode;

typedef struct _MBConfig {
    const char* mbdir;
    int options;
    size_t memcap_index;
    size_t memcap_data;
    int data_size;
    uint32_t connect_id;
    uint32_t block_size_index;
    uint32_t block_size_data;
    int max_num_data_block;
    int max_num_index_block;

    // For automatic eviction
    // All entries in the oldest buckets will be pruned.
    int num_entry_per_bucket;
    uint32_t queue_size;
    const char* queue_dir;
} MBConfig;

// Database handle class
class DB {
public:
    // DB iterator class as an inner class
    class iterator {
        friend class DBTraverseBase;

    public:
        std::string key;
        MBData value;
        int match;

        iterator(const DB& db, int iter_state);
        // Copy constructor
        iterator(const iterator& rhs);
        void init(bool check_async_mode = true);
        int init_no_next();
        ~iterator();

        // operator overloading
        bool operator!=(const iterator& rhs);
        const iterator& operator++();

    private:
        int get_node_offset(const std::string& node_key, size_t& parent_edge_off,
            size_t& node_offset);
        int load_node(const std::string& curr_node_key, size_t& parent_edge_off);
        int load_kv_for_node(const std::string& curr_node_key);
        int load_kvs(const std::string& curr_node_key, MBlsq* chid_node_list);
        void iter_obj_init();
        bool next_dbt_buffer(struct _DBTraverseNode* dbt_n);
        void add_node_offset(size_t node_offset);
        iterator* next();

        const DB& db_ref;
        int state;
        EdgePtrs edge_ptrs;
        // temp buffer to hold the node
        uint8_t node_buff[NUM_ALPHABET + NODE_EDGE_KEY_FIRST];
        MBlsq* node_stack;
        MBlsq* kv_per_node;
        LockFree* lfree;
    };

    // db_path: database directory
    // db_options: db access option (read/write)
    // memcap_index: maximum memory size in bytes for key index
    // memcap_data: maximum memory size for data file mapping
    // data_size: the value size; if zero, the value size will be variable.
    // id: the connector id
    DB(const char* db_path, int db_options, size_t memcap_index = 64 * 1024 * 1024LL,
        size_t memcap_data = 64 * 1024 * 1024LL, uint32_t id = 0, uint32_t queue_size = MB_MAX_NUM_SHM_QUEUE_NODE);
    DB(MBConfig& config);
    ~DB();

    DB(const DB& db);
    const DB& operator=(const DB& db);

    // Add a key-value pair
    int Add(const char* key, int len, const char* data, int data_len, bool overwrite = false);
    int Add(const char* key, int len, MBData& data, bool overwrite = false);
    int Add(const std::string& key, const std::string& value, bool overwrite = false);
    int AddAsync(const char* key, int len, const char* data, int data_len, bool overwrite = false);
    // Find an entry by exact match using a key
    int Find(const char* key, int len, MBData& mdata) const;
    int Find(const std::string& key, MBData& mdata) const;
    // Find the longest prefix match using a key
    int FindLongestPrefix(const char* key, int len, MBData& data) const;
    int FindLongestPrefix(const std::string& key, MBData& data) const;
    // Find all possible prefix matches using a key
    int FindPrefix(const std::string& key, AllPrefixResults& result);
    // Find lower bound does not support reader/writer concurrency currently.
    // FindLowerBound returns that largest entry that is not greater than the given key.
    int FindLowerBound(const char* key, int len, MBData& data) const;
    int FindLowerBound(const std::string& key, MBData& data) const;
    // Remove an entry using a key
    int Remove(const char* key, int len);
    int Remove(const std::string& key);
    int RemoveAll();
    int RemoveAllSync();
    // DB Backup
    int Backup(const char* backup_dir);

    // Close the DB handle
    int Close();
    void Flush() const;
    static void ClearResources(const std::string& path);

    // Garbage collection
    // min_index_rc_size and min_data_rc_size are the threshold for trigering garbage
    // collector. If the pending index buffer size is less than min_index_rc_size,
    // rc will be ignored for index segment. If the pending data buffer size is less
    // than min_data_rc_size, rc will be ignored for data segment.
    // eviction will be ignored if db size is less than 0xFFFFFFFFFFFF and db count is
    // less than 0xFFFFFFFFFFFF.
    int CollectResource(int64_t min_index_rc_size = 33554432, int64_t min_data_rc_size = 33554432,
        int64_t max_dbsiz = MAX_6B_OFFSET, int64_t max_dbcnt = MAX_6B_OFFSET);

    // Multi-thread update using async thread
    bool AsyncWriterEnabled() const;
    bool AsyncWriterBusy() const;

    // multi-thread or multi-process locking for DB management
    int Lock();
    int UnLock();
    int ClearLock() const;

    int UpdateNumHandlers(int mode, int delta);

    // level 0: only error will be logged.
    // level 1: error and warn will be logged.
    // level 2: error, warn, and info will be logged. This is the default setting.
    // level 3: error, warn, info and debug will be logged.
    static void SetLogFile(const std::string& log_file);
    static int SetLogLevel(int level);
    static void LogDebug();
    static void CloseLogFile();

    // Print database stats
    void PrintStats(std::ostream& out_stream = std::cout) const;
    void PrintHeader(std::ostream& out_stream = std::cout) const;
    // current count of key-value pair
    int64_t Count() const;
    // DB status
    int Status() const;
    // DB status string
    const char* StatusStr() const;
    // Check if DB is opened successfully
    bool is_open() const;

    Dict* GetDictPtr() const;
    int GetDBOptions() const;
    const std::string& GetDBDir() const;

    void GetDBConfig(MBConfig& config) const;

    //iterator
    const iterator begin(bool check_async_mode = true, bool rc_mode = false) const;
    const iterator end() const;

private:
    void InitDB(MBConfig& config);
    void InitDBEx(MBConfig& config);
    void ReInit(MBConfig& config);
    void PreCheckDB(const MBConfig& config, bool& init_header, bool& update_header);
    void PostDBUpdate(const MBConfig& config, bool init_header, bool update_header);
    static int ValidateConfig(MBConfig& config);

    // DB directory
    std::string mb_dir;
    int options;
    Dict* dict;
    int status;

    // DB connector ID
    uint32_t identifier;

    // db lock
    MBLock lock;
    MBConfig dbConfig;

    AsyncWriter* async_writer;

    int writer_lock_fd;
};

}

#endif
