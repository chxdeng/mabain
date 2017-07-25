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

#include "dict_mem.h"
#include "rollable_file.h"
#include "mb_data.h"

#define DATA_BUFFER_ALIGNMENT       8
#define DATA_SIZE_BYTE              2

namespace mabain {

// dictionary class
// This is the work horse class for basic db operations (add, find and remove).
class Dict
{
public:
    Dict(const std::string &mbdir, bool init_header, int datasize, int db_options,
         size_t memsize_index, size_t memsize_data, bool use_sliding_map=true,
         bool sync_on_write=false);
    ~Dict();
    void Destroy();

    // Called by writer only
    int Init(uint32_t id);
    // Add key-value pair
    int Add(const uint8_t *key, int len, MBData &data, bool overwrite=true);
    // Find value by key
    int Find(const uint8_t *key, int len, MBData &data) const;
    // Find value by key using prefix match
    int FindPrefix(const uint8_t *key, int len, MBData &data) const;
    // Delete entry by key
    int Remove(const uint8_t *key, int len);
    // Delete entry by key
    int Remove(const uint8_t *key, int len, MBData &data);

    // Delete all entries
    int RemoveAll();

    inline void WriteData(const uint8_t *buff, unsigned len, size_t offset) const;

    // Print dictinary stats
    void PrintStats(std::ostream *out_stream) const;
    void PrintStats(std::ostream &out_stream) const;
    int Status() const;
    int64_t Count() const;
    int GetDBOptions() const;
    size_t GetRootOffset() const;
    size_t GetStartDataOffset() const;

    FreeList *GetFreeList() const;
    DictMem *GetMM() const;
    IndexHeader *GetHeader() const;
    const std::string& GetDBDir() const;

    // Used for DB iterator
    int ReadNextEdge(const uint8_t *node_buff, EdgePtrs &edge_ptrs, int &match,
                 MBData &data, std::string &match_str, size_t &node_off) const;
    int ReadNode(size_t node_off, uint8_t *node_buff, EdgePtrs &edge_ptrs,
                 int &match, MBData &data) const;
    int ReadRootNode(uint8_t *node_buff, EdgePtrs &edge_ptrs, int &match,
                 MBData &data) const;
    inline int ReadData(uint8_t *buff, int len, size_t offset, bool use_sliding_mmap=false) const;
    inline int Reserve(size_t &offset, int size, uint8_t* &ptr);
    void ReserveData(const uint8_t* buff, int size, size_t &offset);

    // Shared memory mutex
    int InitShmMutex();
    void SetShmLockPtrs() const;

    void UpdateNumReader(int delta) const;
    int  UpdateNumWriter(int delta) const;

    void ResetSlidingWindow() const;

private:
    int ReleaseBuffer(size_t offset);
    int UpdateDataBuffer(EdgePtrs &edge_ptrs, bool overwrite, const uint8_t *buff,
                         int len, bool &inc_count);
    int ReadDataFromEdge(MBData &data, const EdgePtrs &edge_ptrs) const;
    int ReadDataFromNode(MBData &data, const uint8_t *node_ptr) const;
    int DeleteDataFromEdge(MBData &data, EdgePtrs &edge_ptrs);

    // DB directory
    std::string mb_dir;
    // DB access permission
    int options;
    // Memory management
    DictMem mm;
    // Database files for storing values
    RollableFile *db_file;
    // buffer free lists
    FreeList *free_lists;

    // dict status
    int status;
    // Header pointer
    IndexHeader *header;
};

inline int Dict::ReadData(uint8_t *buff, int len, size_t offset, bool use_sliding_mmap) const
{
    //if(offset + len > header->m_data_offset)
    //    return -1;

    return db_file->RandomRead(buff, len, offset, use_sliding_mmap);
}

inline void Dict::WriteData(const uint8_t *buff, unsigned len, size_t offset) const
{
    if(offset + len > header->m_data_offset)
    {
        std::cerr << "invalid dict write: " << offset << " " << len << " "
                  << header->m_data_offset << "\n";
        throw (int) MBError::OUT_OF_BOUND;
    }

    if(db_file->RandomWrite(buff, len, offset) != len)
        throw (int) MBError::WRITE_ERROR;
}

inline int Dict::Reserve(size_t &offset, int size, uint8_t* &ptr)
{
    return db_file->Reserve(offset, size, ptr);
}

}

#endif
