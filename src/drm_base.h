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

#ifndef __DRM_BASE_H__
#define __DRM_BASE_H__
#ifdef __DEBUG__
#include <unordered_map>
#endif

#include "free_list.h"
#include "rollable_file.h"

#define DATA_BUFFER_ALIGNMENT 1
#define DATA_SIZE_BYTE 2
#define DATA_HDR_BYTE 4
#define OFFSET_SIZE 6
#define EDGE_SIZE 13
#define EDGE_LEN_POS 5
#define EDGE_FLAG_POS 6
#define EDGE_FLAG_DATA_OFF 0x01
#define FLAG_NODE_MATCH 0x01
#define FLAG_NODE_SORTED 0x02
#define FLAG_NODE_NONE 0x0
#define BUFFER_ALIGNMENT 1
#define LOCAL_EDGE_LEN 6
#define LOCAL_EDGE_LEN_M1 5
#define EDGE_NODE_LEADING_POS 7
#define EXCEP_STATUS_NONE 0
#define EXCEP_STATUS_ADD_EDGE 1
#define EXCEP_STATUS_ADD_DATA_OFF 2
#define EXCEP_STATUS_ADD_NODE 3
#define EXCEP_STATUS_REMOVE_EDGE 4
#define EXCEP_STATUS_CLEAR_EDGE 5
#define EXCEP_STATUS_RC_NODE 6
#define EXCEP_STATUS_RC_EDGE_STR 7
#define EXCEP_STATUS_RC_DATA 8
#define EXCEP_STATUS_RC_TREE 9
#define MB_EXCEPTION_BUFF_SIZE 16

#define MAX_BUFFER_RESERVE_SIZE 8192
#define NUM_BUFFER_RESERVE MAX_BUFFER_RESERVE_SIZE / BUFFER_ALIGNMENT
#define MAX_DATA_BUFFER_RESERVE_SIZE 0xFFFF
#define NUM_DATA_BUFFER_RESERVE MAX_DATA_BUFFER_RESERVE_SIZE / DATA_BUFFER_ALIGNMENT

#define JEMALLOC_ALIGNMENT 8

namespace mabain {

// Mabain DB header
typedef struct _IndexHeader {
    uint16_t version[4];
    int data_size;
    int64_t count;
    size_t m_data_offset;
    size_t m_index_offset;
    int64_t pending_data_buff_size;
    int64_t pending_index_buff_size;
    int64_t n_states;
    int64_t n_edges;
    int64_t edge_str_size;
    int num_writer;
    int num_reader;
    int64_t shm_queue_id;
    int writer_options;
    int dummy;

    // Lock-free data structure
    LockFreeShmData lock_free;

    // read/write lock
    char padding[56]; // pthread_rwlock_t mb_rw_lock;

    // block size
    uint32_t index_block_size;
    uint32_t data_block_size;
    // number of entry per bucket for eviction
    int64_t entry_per_bucket;
    // number of DB insertions and updates
    // used for assigning bucket
    int64_t num_update;
    uint16_t eviction_bucket_index;

    // temp variables used for abnormal writer terminations
    int excep_updating_status;
    uint8_t excep_buff[MB_EXCEPTION_BUFF_SIZE];
    size_t excep_offset;
    size_t excep_lf_offset;

    // index root offset for insertions during rc
    size_t rc_m_index_off_pre;
    size_t rc_m_data_off_pre;
    std::atomic<size_t> rc_root_offset;
    int64_t rc_count;

    // multi-process async queue
    int async_queue_size;
    std::atomic<uint32_t> queue_index;
    uint32_t writer_index;
    std::atomic<uint32_t> rc_flag;
} IndexHeader;

// An abstract interface class for Dict and DictMem
class DRMBase {
public:
    DRMBase(const std::string& mbdir, int opts, bool index)
        : options(opts)
    {
        // derived class will initialize header and kv_file
        header = nullptr;
        kv_file = nullptr;
        free_lists = nullptr;
        if (opts & CONSTS::ACCESS_MODE_WRITER) {
            if (!(opts & CONSTS::OPTION_JEMALLOC)) {
                if (index) {
                    free_lists = new FreeList(mbdir + "_ibfl", BUFFER_ALIGNMENT, NUM_BUFFER_RESERVE);
                } else {
                    free_lists = new FreeList(mbdir + "_dbfl", BUFFER_ALIGNMENT, NUM_BUFFER_RESERVE);
                }
            }
        }
    }

    ~DRMBase()
    {
    }

    inline virtual void WriteData(const uint8_t* buff, unsigned len, size_t offset) const = 0;
    inline int Reserve(size_t& offset, int size, uint8_t*& ptr);
    inline uint8_t* GetShmPtr(size_t offset, int size) const;
    inline size_t CheckAlignment(size_t offset, int size) const;
    inline int ReadData(uint8_t* buff, unsigned len, size_t offset) const;
    inline size_t GetResourceCollectionOffset() const;
    inline void RemoveUnused(size_t max_size, bool writer_mode = false);

    FreeList* GetFreeList() const
    {
        return free_lists;
    }

    IndexHeader* GetHeaderPtr() const
    {
        return header;
    }

    void PrintHeader(std::ostream& out_stream) const;

    static void ValidateHeaderFile(const std::string& header_path, int mode, int queue_size,
        bool& update_header);

protected:
    static void ReadHeaderVersion(const std::string& header_path, uint16_t ver[4]);
    static void ReadHeader(const std::string& header_path, uint8_t* buff, int buf_size);
    static void WriteHeader(const std::string& header_path, uint8_t* buff);

    int options;
    IndexHeader* header;
    RollableFile* kv_file;
    // free_lists is the old way of managing memory. It will be replaced by jemalloc.
    FreeList* free_lists;

#ifdef __DEBUG__
#define __BUFFER_TRACKER_IN_USE 1
#define __BUFFER_TRACKER_RELEASED 2
#define __BUFFER_TRACKER_INVALID 3
    void add_tracking_buffer(size_t offset, int size = 0)
    {
        if (buffer_map.find(offset) == buffer_map.end()) {
            buffer_map[offset] = __BUFFER_TRACKER_IN_USE;
        } else {
            buffer_map[offset] = __BUFFER_TRACKER_INVALID;
        }
    }
    void remove_tracking_buffer(size_t offset, int size = 0)
    {
        if (buffer_map.find(offset) == buffer_map.end()) {
            buffer_map[offset] = __BUFFER_TRACKER_INVALID;
        } else {
            if (buffer_map[offset] != __BUFFER_TRACKER_IN_USE) {
                buffer_map[offset] = __BUFFER_TRACKER_INVALID;
            } else {
                buffer_map.erase(offset); // remove from tracking
            }
        }
        buffer_map.erase(offset);
    }
    // buffer tracker
    std::unordered_map<size_t, int> buffer_map;
#endif
};

inline void DRMBase::WriteData(const uint8_t* buff, unsigned len, size_t offset) const
{
    if (options & CONSTS::OPTION_JEMALLOC) {
        kv_file->MemWrite(buff, len, offset);
    } else {
        if (kv_file->RandomWrite(buff, len, offset) != len)
            throw (int)MBError::WRITE_ERROR;
    }
}

inline int DRMBase::Reserve(size_t& offset, int size, uint8_t*& ptr)
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

// API for both jemalloc and non-jemalloc
inline int DRMBase::ReadData(uint8_t* buff, unsigned len, size_t offset) const
{
    if (options & CONSTS::OPTION_JEMALLOC) {
        return kv_file->MemRead(buff, len, offset);
    }
    return kv_file->RandomRead(buff, len, offset);
}

inline size_t DRMBase::GetResourceCollectionOffset() const
{
    return kv_file->GetResourceCollectionOffset();
}

inline void DRMBase::RemoveUnused(size_t max_size, bool writer_mode)
{
    return kv_file->RemoveUnused(max_size, writer_mode);
}

}

#endif
