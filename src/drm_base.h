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

#ifndef __DRM_BASE_H__
#define __DRM_BASE_H__

#include "rollable_file.h"
#include "free_list.h"

#define DATA_BUFFER_ALIGNMENT      1
#define DATA_SIZE_BYTE             2
#define DATA_HDR_BYTE              4
#define OFFSET_SIZE                6
#define EDGE_SIZE                  13
#define EDGE_LEN_POS               5
#define EDGE_FLAG_POS              6
#define EDGE_FLAG_DATA_OFF         0x01
#define FLAG_NODE_MATCH            0x01
#define FLAG_NODE_NONE             0x0
#define BUFFER_ALIGNMENT           1
#define LOCAL_EDGE_LEN             6
#define LOCAL_EDGE_LEN_M1          5
#define EDGE_NODE_LEADING_POS      7
#define EXCEP_STATUS_NONE          0
#define EXCEP_STATUS_ADD_EDGE      1
#define EXCEP_STATUS_ADD_DATA_OFF  2
#define EXCEP_STATUS_ADD_NODE      3
#define EXCEP_STATUS_REMOVE_EDGE   4
#define EXCEP_STATUS_CLEAR_EDGE    5
#define EXCEP_STATUS_RC_NODE       6
#define EXCEP_STATUS_RC_EDGE_STR   7
#define EXCEP_STATUS_RC_DATA       8
#define EXCEP_STATUS_RC_TREE       9
#define MB_EXCEPTION_BUFF_SIZE     16

namespace mabain {

// Mabain DB header
typedef struct _IndexHeader
{
    uint16_t version[4];
    int      data_size;
    int64_t  count;
    size_t   m_data_offset;
    size_t   m_index_offset;
    int64_t  pending_data_buff_size;
    int64_t  pending_index_buff_size;
    int64_t  n_states;
    int64_t  n_edges;
    int64_t  edge_str_size;
    int      num_writer;
    int      num_reader;
    std::atomic<size_t> shm_index_sliding_start;
    std::atomic<size_t> shm_data_sliding_start;

    // Lock-free data structure
    LockFreeShmData lock_free;

    // read/write lock
    pthread_rwlock_t mb_rw_lock;

    // block size
    uint32_t index_block_size;
    uint32_t data_block_size;
    // number of entry per bucket for eviction
    int64_t  entry_per_bucket;
    // number of DB insertions and updates
    // used for assigning bucket
    int64_t  num_update;
    uint16_t eviction_bucket_index;

    // temp variables used for abnormal writer terminations
    int     excep_updating_status;
    uint8_t excep_buff[MB_EXCEPTION_BUFF_SIZE];
    size_t  excep_offset;
    size_t  excep_lf_offset;

    // index root offset for insertions during rc
    size_t               rc_m_index_off_pre;
    size_t               rc_m_data_off_pre;
    std::atomic<size_t>  rc_root_offset;
    int64_t              rc_count;

    // multi-process async queue
    int                   async_queue_size;
    std::atomic<uint32_t> queue_index;
    uint32_t              writer_index;
} IndexHeader;

// An abstract interface class for Dict and DictMem
class DRMBase
{
public:
    DRMBase()
    {
        // Derived classes will initialize these objects.
        kv_file = NULL;
        free_lists = NULL;
    }

    ~DRMBase()
    {
    }

    inline virtual void WriteData(const uint8_t *buff, unsigned len, size_t offset) const = 0;
    inline int Reserve(size_t &offset, int size, uint8_t* &ptr);
    inline uint8_t* GetShmPtr(size_t offset, int size) const;
    inline size_t CheckAlignment(size_t offset, int size) const;
    inline int ReadData(uint8_t *buff, unsigned len, size_t offset) const;
    inline size_t GetResourceCollectionOffset() const;

    FreeList *GetFreeList() const
    {
        return free_lists;
    }

    IndexHeader *GetHeaderPtr() const
    {
        return header;
    }

protected:
    IndexHeader *header;
    RollableFile *kv_file;
    FreeList *free_lists;
};

inline void DRMBase::WriteData(const uint8_t *buff, unsigned len, size_t offset) const
{
    if(kv_file->RandomWrite(buff, len, offset) != len)
        throw (int) MBError::WRITE_ERROR;
}

inline int DRMBase::Reserve(size_t &offset, int size, uint8_t* &ptr)
{
    return kv_file->Reserve(offset, size, ptr);
}

inline uint8_t* DRMBase::GetShmPtr(size_t offset, int size) const
{
    return kv_file->GetShmPtr(offset, size);
}

inline size_t DRMBase::CheckAlignment(size_t offset, int size) const
{
    return kv_file->CheckAlignment(offset, size);
}

inline int DRMBase::ReadData(uint8_t *buff, unsigned len, size_t offset) const
{
    return kv_file->RandomRead(buff, len, offset);
}

inline size_t DRMBase::GetResourceCollectionOffset() const
{
     return kv_file->GetResourceCollectionOffset();
}

}

#endif
