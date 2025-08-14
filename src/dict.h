/**
 * Copyright (C) 2025 Cisco Inc.
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

#ifndef __DICT_H__
#define __DICT_H__

#include <memory>
#include <stdint.h>
#include <string.h>
#include <string>

#include "async_writer.h"
#include "dict_mem.h"
#include "drm_base.h"
#include "lock_free.h"
#include "mb_data.h"
#include "mb_pipe.h"
#include "rollable_file.h"
#include "shm_queue_mgr.h"
#include "util/prefix_cache_iface.h"
// forward declare
namespace mabain {
namespace detail { class SearchEngine; }
class PrefixCache;
class PrefixCacheShared;
}

namespace mabain {

struct _AsyncNode;
typedef struct _AsyncNode AsyncNode;
struct _shm_lock_and_queue;
typedef struct _shm_lock_and_queue shm_lock_and_queue;


// dictionary class
// This is the work horse class for basic db operations (add, find and remove).
class Dict : public DRMBase {
public:
    Dict(const std::string& mbdir, bool init_header, int datasize,
        int db_options, size_t memsize_index, size_t memsize_data,
        uint32_t block_sz_index, uint32_t block_sz_data,
        int max_num_index_blk, int max_num_data_blk,
        int64_t entry_per_bucket, uint32_t queue_size,
        const char* queue_dir);
    virtual ~Dict();
    void Destroy();

    // Called by writer only
    int Init(uint32_t id);
    // Add key-value pair
    int Add(const uint8_t* key, int len, MBData& data, bool overwrite);
    // Bound search implemented internally in SearchEngine (DB uses SearchEngine directly).
    int ReadDataByOffset(size_t offset, MBData& data) const;

    // Delete entry by key
    int Remove(const uint8_t* key, int len);
    // Delete entry by key
    int Remove(const uint8_t* key, int len, MBData& data);

    // Delete all entries
    int RemoveAll();

    // multiple-process updates using shared memory queue
    int SHMQ_Add(const char* key, int key_len, const char* data, int data_len,
        bool overwrite);
    int SHMQ_Remove(const char* key, int len);
    int SHMQ_RemoveAll();
    int SHMQ_Backup(const char* backup_dir);
    int SHMQ_CollectResource(int64_t m_index_rc_size, int64_t m_data_rc_size,
        int64_t max_dbsz, int64_t max_dbcnt);
    void SHMQ_Signal();
    bool SHMQ_Busy() const;

    void ReserveData(const uint8_t* buff, int size, size_t& offset);
    void WriteData(const uint8_t* buff, unsigned len, size_t offset) const;

    // Print dictinary stats
    void PrintStats(std::ostream* out_stream) const;
    void PrintStats(std::ostream& out_stream) const;
    int Status() const;
    int64_t Count() const;
    size_t GetRootOffset() const;
    size_t GetStartDataOffset() const;

    DictMem* GetMM() const;

    LockFree* GetLockFreePtr();

    // Used for DB iterator
    int ReadNextEdge(const uint8_t* node_buff, EdgePtrs& edge_ptrs, int& match,
        MBData& data, std::string& match_str, size_t& node_off,
        bool rd_kv = true) const;
    int ReadNode(size_t node_off, uint8_t* node_buff, EdgePtrs& edge_ptrs,
        int& match, MBData& data, bool rd_kv = true) const;
    void ReadNodeHeader(size_t node_off, int& node_size, int& match,
        size_t& data_offset, size_t& data_link_offset);
    int ReadRootNode(uint8_t* node_buff, EdgePtrs& edge_ptrs, int& match,
        MBData& data) const;

    pthread_mutex_t* GetShmLockPtr() const;
    AsyncNode* GetAsyncQueuePtr() const;

    void UpdateNumReader(int delta) const;
    int UpdateNumWriter(int delta) const;

    void Flush() const;
    void Purge() const;
    int ExceptionRecovery();

    // Optional: enable/disable prefix cache for Find
    void EnablePrefixCache(int n, size_t capacity = 65536);
    void DisablePrefixCache();
    bool PrefixCacheEnabled() const { return static_cast<bool>(prefix_cache); }
    void GetPrefixCacheStats(uint64_t& hit, uint64_t& miss, uint64_t& put,
        size_t& entries, int& n) const;
    void ResetPrefixCacheStats();
    void PrintPrefixCacheStats(std::ostream& os) const;

    // Shared prefix cache (multi-process, writer-managed updates)
    void EnableSharedPrefixCache(int n, size_t capacity = 65536, uint32_t assoc = 4);
    void DisableSharedPrefixCache();
    bool SharedPrefixCacheEnabled() const { return static_cast<bool>(prefix_cache_shared); }
    void PrintSharedPrefixCacheStats(std::ostream& os) const;
    PrefixCacheIface* ActivePrefixCache() const;
    void SetSharedPrefixCacheReadOnly(bool ro) { shared_pc_readonly = ro; }

private:
    // Allow internal SearchEngine to orchestrate lookups without exposing members publicly
    friend class detail::SearchEngine;
    // Search internals moved to detail::SearchEngine
    // Prefix traversal helpers moved to SearchEngine.
    // Helpers removed from header; SearchEngine owns traversal.
    int ReleaseBuffer(size_t offset);
    int UpdateDataBuffer(EdgePtrs& edge_ptrs, bool overwrite, MBData& mbd, bool& inc_count);
    int ReadDataFromEdge(MBData& data, const EdgePtrs& edge_ptrs) const;
    int ReadDataFromNode(MBData& data, const uint8_t* node_ptr) const;
    int DeleteDataFromEdge(MBData& data, EdgePtrs& edge_ptrs);
    int ReadNodeMatch(size_t node_off, int& match, MBData& data) const;
    int SHMQ_PrepareSlot(AsyncNode* node_ptr);
    AsyncNode* SHMQ_AcquireSlot(int& err) const;

    // Bound traversal helpers moved to SearchEngine.
    void reserveDataFL(const uint8_t* buff, int size, size_t& offset);
    int ReleaseBuffer(size_t offset, int size);
    void ReleaseAlignmentBuffer(size_t offset, size_t alignment_off);

    // Memory management
    DictMem mm;

    // dict status
    int status;

    LockFree lfree;

    size_t reader_rc_off;
    AsyncNode* queue;
    shm_lock_and_queue* slaq;
    MBPipe mbp;
    // Hold a reference to shared memory queue file so that the async thread can access it during process exit
    ShmQueueMgr qmgr;

    // Optional lookup accelerators for Find
    std::unique_ptr<PrefixCache> prefix_cache;
    std::unique_ptr<PrefixCacheShared> prefix_cache_shared;
    std::string mbdir_;
    bool shared_pc_readonly = false;

    // Prefix-cache helpers (no side effects when cache disabled)
    void MaybePutCache(const uint8_t* full_key, int full_len, int consumed,
        const EdgePtrs& edge_ptrs) const;
};

}

#endif
