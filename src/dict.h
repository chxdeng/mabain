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

#ifndef __DICT_H__
#define __DICT_H__

#include <stdint.h>
#include <string>

#include "drm_base.h"
#include "dict_mem.h"
#include "rollable_file.h"
#include "mb_data.h"
#include "lock_free.h"

namespace mabain {

// dictionary class
// This is the work horse class for basic db operations (add, find and remove).
class Dict : public DRMBase
{
public:
    Dict(const std::string &mbdir, bool init_header, int datasize,
         int db_options, size_t memsize_index, size_t memsize_data,
         uint32_t block_sz_index, uint32_t block_sz_data,
         int max_num_index_blk, int max_num_data_blk,
         int64_t entry_per_bucket);
    virtual ~Dict();
    void Destroy();

    // Called by writer only
    int Init(uint32_t id);
    // Add key-value pair
    int Add(const uint8_t *key, int len, MBData &data, bool overwrite);
    // Find value by key
    int Find(const uint8_t *key, int len, MBData &data);
    // Find value by key using prefix match
    int FindPrefix(const uint8_t *key, int len, MBData &data);
    // Delete entry by key
    int Remove(const uint8_t *key, int len);
    // Delete entry by key
    int Remove(const uint8_t *key, int len, MBData &data);

    // Delete all entries
    int RemoveAll();

    void ReserveData(const uint8_t* buff, int size, size_t &offset);
    void WriteData(const uint8_t *buff, unsigned len, size_t offset) const;

    // Print dictinary stats
    void PrintStats(std::ostream *out_stream) const;
    void PrintStats(std::ostream &out_stream) const;
    void PrintHeader(std::ostream &out_stream) const;
    int Status() const;
    int64_t Count() const;
    size_t GetRootOffset() const;
    size_t GetStartDataOffset() const;

    DictMem *GetMM() const;

    LockFree* GetLockFreePtr();

    // Used for DB iterator
    int  ReadNextEdge(const uint8_t *node_buff, EdgePtrs &edge_ptrs, int &match,
                 MBData &data, std::string &match_str, size_t &node_off,
                 bool rd_kv = true) const;
    int  ReadNode(size_t node_off, uint8_t *node_buff, EdgePtrs &edge_ptrs,
                 int &match, MBData &data, bool rd_kv = true) const;
    void ReadNodeHeader(size_t node_off, int &node_size, int &match,
                 size_t &data_offset, size_t &data_link_offset);
    int  ReadRootNode(uint8_t *node_buff, EdgePtrs &edge_ptrs, int &match,
                 MBData &data) const;

    // Shared memory mutex
    int InitShmMutex();
    pthread_rwlock_t* GetShmLockPtrs() const;

    void UpdateNumReader(int delta) const;
    int  UpdateNumWriter(int delta) const;

    void ResetSlidingWindow() const;
    void Flush() const;
    int  ExceptionRecovery();

private:
    int ReleaseBuffer(size_t offset);
    int UpdateDataBuffer(EdgePtrs &edge_ptrs, bool overwrite, const uint8_t *buff,
                         int len, bool &inc_count);
    int ReadDataFromEdge(MBData &data, const EdgePtrs &edge_ptrs) const;
    int ReadDataFromNode(MBData &data, const uint8_t *node_ptr) const;
    int DeleteDataFromEdge(MBData &data, EdgePtrs &edge_ptrs);
    int ReadNodeMatch(size_t node_off, int &match, MBData &data) const;

    // DB access permission
    int options;
    // Memory management
    DictMem mm;

    // dict status
    int status;

    LockFree lfree;
};

}

#endif
