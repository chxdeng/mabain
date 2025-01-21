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

#include <errno.h>
#include <iostream>
#include <stdlib.h>

#include "async_writer.h"
#include "db.h"
#include "dict.h"
#include "dict_mem.h"
#include "error.h"
#include "integer_4b_5b.h"
#include "mabain_consts.h"

#define DATA_HEADER_SIZE 32

#define READER_LOCK_FREE_START \
    LockFreeData snapshot;     \
    int lf_ret;                \
    lfree.ReaderLockFreeStart(snapshot);
#define READER_LOCK_FREE_STOP(edgeoff, data)                        \
    lf_ret = lfree.ReaderLockFreeStop(snapshot, (edgeoff), (data)); \
    if (lf_ret != MBError::SUCCESS)                                 \
        return lf_ret;

namespace mabain {

Dict::Dict(const std::string& mbdir, bool init_header, int datasize,
    int db_options, size_t memsize_index, size_t memsize_data,
    uint32_t block_sz_idx, uint32_t block_sz_data,
    int max_num_index_blk, int max_num_data_blk,
    int64_t entry_per_bucket, uint32_t queue_size,
    const char* queue_dir)
    : DRMBase(mbdir, db_options, false)
    , mm(mbdir, init_header, memsize_index, db_options, block_sz_idx, max_num_index_blk, queue_size)
    , queue(NULL)
{
    status = MBError::NOT_INITIALIZED;
    reader_rc_off = 0;
    slaq = NULL;

    header = mm.GetHeaderPtr();
    if (header == NULL) {
        Logger::Log(LOG_LEVEL_ERROR, "header not mapped");
        throw(int) MBError::MMAP_FAILED;
    }

    if (!init_header) {
        // confirm block size is the same
        if (block_sz_data != 0 && header->data_block_size != block_sz_data) {
            std::cerr << "mabain data block size not match " << block_sz_data << ": "
                      << header->data_block_size << std::endl;
            PrintHeader(std::cout);
            Destroy();
            throw(int) MBError::INVALID_SIZE;
        }
    } else {
        header->data_block_size = block_sz_data;
    }

    if (!(db_options & CONSTS::READ_ONLY_DB)) {
        // initialize shared memory queue
        ShmQueueMgr qmgr;
        slaq = qmgr.CreateFile(header->shm_queue_id, queue_size, queue_dir, db_options);
        queue = slaq->queue;
    }
    lfree.LockFreeInit(&header->lock_free, header, db_options);
    mm.InitLockFreePtr(&lfree);
    mbp = MBPipe(mbdir, 0);

    // Open data file
    kv_file = new RollableFile(mbdir + "_mabain_d",
        static_cast<size_t>(header->data_block_size),
        memsize_data, db_options, max_num_data_blk);

    // If init_header is false, we can set the dict status to SUCCESS.
    // Otherwise, the status will be set in the Init.
    if (init_header) {
        // Initialize header
        // We known that only writers will set init_header to true.
        header->entry_per_bucket = entry_per_bucket;
        header->index_block_size = block_sz_idx;
        header->data_block_size = block_sz_data;
        header->data_size = datasize;
        header->count = 0;
        header->m_data_offset = GetStartDataOffset(); // start from a non-zero offset
    } else {
        if (options & CONSTS::ACCESS_MODE_WRITER) {
            if (header->entry_per_bucket != entry_per_bucket) {
                std::cerr << "mabain count per bucket not match\n";
            }
            // Check if jemalloc option is conststent
            if ((options & CONSTS::OPTION_JEMALLOC) && !(header->writer_options & CONSTS::OPTION_JEMALLOC)) {
                std::cerr << "mabain jemalloc option not match\n";
                Destroy();
                throw(int) MBError::INVALID_ARG;
            }
            if (!(options & CONSTS::OPTION_JEMALLOC) && (header->writer_options & CONSTS::OPTION_JEMALLOC)) {
                std::cerr << "mabain jemalloc option not match header\n";
                Destroy();
                throw(int) MBError::INVALID_ARG;
            }
        }
    }
    if (mm.IsValid())
        status = MBError::SUCCESS;
}

Dict::~Dict()
{
}

// This function only needs to be called by writer.
int Dict::Init(uint32_t id)
{
    Logger::Log(LOG_LEVEL_DEBUG, "connector %u initializing db", id);
    if (!(options & CONSTS::ACCESS_MODE_WRITER)) {
        Logger::Log(LOG_LEVEL_ERROR, "dict initialization not allowed for non-writer");
        return MBError::NOT_ALLOWED;
    }

    if (status != MBError::NOT_INITIALIZED) {
        // status can be NOT_INITIALIZED or SUCCESS.
        Logger::Log(LOG_LEVEL_WARN, "connector %u dict already initialized", id);
        return MBError::SUCCESS;
    }

    if (header == NULL) {
        Logger::Log(LOG_LEVEL_ERROR, "connector %u header not mapped", id);
        return MBError::ALLOCATION_ERROR;
    }

    Logger::Log(LOG_LEVEL_DEBUG, "connector %u initializing DictMem", id);
    mm.InitRootNode();

    if (header->data_size > CONSTS::MAX_DATA_SIZE) {
        Logger::Log(LOG_LEVEL_ERROR, "data size %d is too large", header->data_size);
        return MBError::INVALID_SIZE;
    }

    if (mm.IsValid())
        status = MBError::SUCCESS;

    return status;
}

void Dict::Destroy()
{
    mm.Destroy();

    if (free_lists != NULL)
        delete free_lists;

    if (kv_file != NULL)
        delete kv_file;
}

int Dict::Status() const
{
    return status;
}

// Add a key-value pair
// if overwrite is true and an entry with input key already exists, the old data will
// be overwritten. Otherwise, IN_DICT will be returned.
int Dict::Add(const uint8_t* key, int len, MBData& data, bool overwrite)
{
    if (!(options & CONSTS::ACCESS_MODE_WRITER)) {
        return MBError::NOT_ALLOWED;
    }
    if (len > CONSTS::MAX_KEY_LENGHTH || data.data_len > CONSTS::MAX_DATA_SIZE || len <= 0 || data.data_len <= 0)
        return MBError::OUT_OF_BOUND;

    EdgePtrs edge_ptrs;
    int rval;

    rval = mm.GetRootEdge_Writer(data.options & CONSTS::OPTION_RC_MODE, key[0], edge_ptrs);
    if (rval != MBError::SUCCESS)
        return rval;

    if (edge_ptrs.len_ptr[0] == 0) {
        ReserveData(data.buff, data.data_len, data.data_offset);
        // Add the first edge along this edge
        mm.AddRootEdge(edge_ptrs, key, len, data.data_offset);
        if (data.options & CONSTS::OPTION_RC_MODE) {
            header->rc_count++;
        } else {
            header->count++;
            header->num_update++;
        }

        return MBError::SUCCESS;
    }

    bool inc_count = true;
    int i;
    const uint8_t* key_buff;
    uint8_t tmp_key_buff[NUM_ALPHABET];
    const uint8_t* p = key;
    int edge_len = edge_ptrs.len_ptr[0];
    if (edge_len > LOCAL_EDGE_LEN) {
        if (mm.ReadData(tmp_key_buff, edge_len - 1, Get5BInteger(edge_ptrs.ptr)) != edge_len - 1)
            return MBError::READ_ERROR;
        key_buff = tmp_key_buff;
    } else {
        key_buff = edge_ptrs.ptr;
    }
    if (edge_len < len) {
        for (i = 1; i < edge_len; i++) {
            if (key_buff[i - 1] != key[i])
                break;
        }
        if (i >= edge_len) {
            int match_len;
            bool next;
            p += edge_len;
            len -= edge_len;
            while ((next = mm.FindNext(p, len, match_len, edge_ptrs, tmp_key_buff))) {
                if (match_len < edge_ptrs.len_ptr[0])
                    break;

                p += match_len;
                len -= match_len;
                if (len <= 0)
                    break;
            }
            if (!next) {
                ReserveData(data.buff, data.data_len, data.data_offset);
                rval = mm.UpdateNode(edge_ptrs, p, len, data.data_offset);
            } else if (match_len < static_cast<int>(edge_ptrs.len_ptr[0])) {
                if (len > match_len) {
                    ReserveData(data.buff, data.data_len, data.data_offset);
                    rval = mm.AddLink(edge_ptrs, match_len, p + match_len, len - match_len,
                        data.data_offset, data);
                } else if (len == match_len) {
                    ReserveData(data.buff, data.data_len, data.data_offset);
                    rval = mm.InsertNode(edge_ptrs, match_len, data.data_offset, data);
                }
            } else if (len == 0) {
                rval = UpdateDataBuffer(edge_ptrs, overwrite, data.buff, data.data_len, inc_count);
            }
        } else {
            ReserveData(data.buff, data.data_len, data.data_offset);
            rval = mm.AddLink(edge_ptrs, i, p + i, len - i, data.data_offset, data);
        }
    } else {
        for (i = 1; i < len; i++) {
            if (key_buff[i - 1] != key[i])
                break;
        }
        if (i < len) {
            ReserveData(data.buff, data.data_len, data.data_offset);
            rval = mm.AddLink(edge_ptrs, i, p + i, len - i, data.data_offset, data);
        } else {
            if (edge_ptrs.len_ptr[0] > len) {
                ReserveData(data.buff, data.data_len, data.data_offset);
                rval = mm.InsertNode(edge_ptrs, i, data.data_offset, data);
            } else {
                rval = UpdateDataBuffer(edge_ptrs, overwrite, data.buff, data.data_len, inc_count);
            }
        }
    }

    if (data.options & CONSTS::OPTION_RC_MODE) {
        if (rval == MBError::SUCCESS)
            header->rc_count++;
    } else {
        if (rval == MBError::SUCCESS)
            header->num_update++;
        if (inc_count)
            header->count++;
    }
    return rval;
}

int Dict::ReadDataFromEdge(MBData& data, const EdgePtrs& edge_ptrs) const
{
    size_t data_off;
    if (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF) {
        data_off = Get6BInteger(edge_ptrs.offset_ptr);
    } else {
        uint8_t node_buff[NODE_EDGE_KEY_FIRST];
        if (mm.ReadData(node_buff, NODE_EDGE_KEY_FIRST, Get6BInteger(edge_ptrs.offset_ptr))
            != NODE_EDGE_KEY_FIRST)
            return MBError::READ_ERROR;
        if (!(node_buff[0] & FLAG_NODE_MATCH))
            return MBError::NOT_EXIST;
        data_off = Get6BInteger(node_buff + 2);
    }
    data.data_offset = data_off;

    uint16_t data_len[2];
    // Read data length first
    if (ReadData(reinterpret_cast<uint8_t*>(&data_len[0]), DATA_HDR_BYTE, data_off)
        != DATA_HDR_BYTE)
        return MBError::READ_ERROR;
    data_off += DATA_HDR_BYTE;
    if (data.buff_len < data_len[0] + 1) {
        if (data.Resize(data_len[0]) != MBError::SUCCESS)
            return MBError::NO_MEMORY;
    }

    if (ReadData(data.buff, data_len[0], data_off) != data_len[0])
        return MBError::READ_ERROR;

    data.data_len = data_len[0];
    data.bucket_index = data_len[1];
    return MBError::SUCCESS;
}

// Delete operations:
//   If this is a leaf node, need to remove the edge. Otherwise, unset the match flag.
//   Also need to set the delete flag in the data block so that it can be reclaimed later.
int Dict::DeleteDataFromEdge(MBData& data, EdgePtrs& edge_ptrs)
{
    int rval = MBError::SUCCESS;
    size_t data_off;
    uint16_t data_len;
    int rel_size;

    // Check if this is a leaf node first by using the EDGE_FLAG_DATA_OFF bit
    if (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF) {
        data_off = Get6BInteger(edge_ptrs.offset_ptr);
        if (ReadData(reinterpret_cast<uint8_t*>(&data_len), DATA_SIZE_BYTE, data_off)
            != DATA_SIZE_BYTE)
            return MBError::READ_ERROR;
        if (options & CONSTS::OPTION_JEMALLOC) {
            rel_size = data_len + DATA_HDR_BYTE;
        } else {
            rel_size = free_lists->GetAlignmentSize(data_len + DATA_HDR_BYTE);
        }
        ReleaseBuffer(data_off, rel_size);

        rval = mm.RemoveEdgeByIndex(edge_ptrs, data);
    } else {
        // No exception handling in this case
        header->excep_lf_offset = 0;
        header->excep_offset = 0;

        uint8_t node_buff[NODE_EDGE_KEY_FIRST];
        size_t node_off = Get6BInteger(edge_ptrs.offset_ptr);

        // Read node header
        if (mm.ReadData(node_buff, NODE_EDGE_KEY_FIRST, node_off) != NODE_EDGE_KEY_FIRST)
            return MBError::READ_ERROR;

        if (node_buff[0] & FLAG_NODE_MATCH) {
            // Unset the match flag
            node_buff[0] &= ~FLAG_NODE_MATCH;
            mm.WriteData(&node_buff[0], 1, node_off);

            // Release data buffer
            data_off = Get6BInteger(node_buff + 2);
            if (ReadData(reinterpret_cast<uint8_t*>(&data_len), DATA_SIZE_BYTE, data_off)
                != DATA_SIZE_BYTE)
                return MBError::READ_ERROR;

            if (options & CONSTS::OPTION_JEMALLOC) {
                rel_size = data_len + DATA_HDR_BYTE;
            } else {
                rel_size = free_lists->GetAlignmentSize(data_len + DATA_HDR_BYTE);
            }
            ReleaseBuffer(data_off, rel_size);
        } else {
            rval = MBError::NOT_EXIST;
        }
    }

    return rval;
}

int Dict::ReadDataFromNode(MBData& data, const uint8_t* node_ptr) const
{
    size_t data_off = Get6BInteger(node_ptr + 2);
    if (data_off == 0)
        return MBError::NOT_EXIST;

    data.data_offset = data_off;

    // Read data length first
    uint16_t data_len[2];
    if (ReadData(reinterpret_cast<uint8_t*>(&data_len[0]), DATA_HDR_BYTE, data_off)
        != DATA_HDR_BYTE)
        return MBError::READ_ERROR;
    data_off += DATA_HDR_BYTE;

    if (data.buff_len < data_len[0] + 1) {
        if (data.Resize(data_len[0]) != MBError::SUCCESS)
            return MBError::NO_MEMORY;
    }
    if (ReadData(data.buff, data_len[0], data_off) != data_len[0])
        return MBError::READ_ERROR;

    data.data_len = data_len[0];
    data.bucket_index = data_len[1];
    return MBError::SUCCESS;
}

int Dict::FindPrefix(const uint8_t* key, int len, MBData& data)
{
    int rval;
    MBData data_rc;
    size_t rc_root_offset = header->rc_root_offset.load(MEMORY_ORDER_READER);
    if (rc_root_offset != 0) {
        reader_rc_off = rc_root_offset;
        rval = FindPrefix_Internal(rc_root_offset, key, len, data_rc);
#ifdef __LOCK_FREE__
        while (rval == MBError::TRY_AGAIN) {
            nanosleep((const struct timespec[]) { { 0, 10L } }, NULL);
            data_rc.Clear();
            rval = FindPrefix_Internal(rc_root_offset, key, len, data_rc);
        }
#endif
        if (rval != MBError::NOT_EXIST && rval != MBError::SUCCESS)
            return rval;
        data.options &= ~(CONSTS::OPTION_RC_MODE | CONSTS::OPTION_READ_SAVED_EDGE);
    } else {
        if (reader_rc_off != 0) {
            reader_rc_off = 0;
            RemoveUnused(0);
            mm.RemoveUnused(0);
        }
    }

    rval = FindPrefix_Internal(0, key, len, data);
#ifdef __LOCK_FREE__
    while (rval == MBError::TRY_AGAIN) {
        nanosleep((const struct timespec[]) { { 0, 10L } }, NULL);
        data.Clear();
        rval = FindPrefix_Internal(0, key, len, data);
    }
#endif

    // The longer match wins.
    if (data_rc.match_len > data.match_len) {
        data_rc.TransferValueTo(data.buff, data.data_len);
        rval = MBError::SUCCESS;
    }
    return rval;
}

int Dict::FindPrefix_Internal(size_t root_off, const uint8_t* key, int len, MBData& data)
{
    int rval;
    EdgePtrs& edge_ptrs = data.edge_ptrs;
#ifdef __LOCK_FREE__
    READER_LOCK_FREE_START
#endif

    rval = mm.GetRootEdge(data.options & CONSTS::OPTION_RC_MODE, key[0], edge_ptrs);
    if (rval != MBError::SUCCESS)
        return MBError::READ_ERROR;

    if (edge_ptrs.len_ptr[0] == 0) {
#ifdef __LOCK_FREE__
        READER_LOCK_FREE_STOP(edge_ptrs.offset, data)
#endif
        return MBError::NOT_EXIST;
    }

    // Compare edge string
    const uint8_t* key_buff;
    uint8_t* node_buff = data.node_buff;
    const uint8_t* p = key;
    int edge_len = edge_ptrs.len_ptr[0];
    int edge_len_m1 = edge_len - 1;
    if (edge_len > LOCAL_EDGE_LEN) {
        if (mm.ReadData(node_buff, edge_len_m1, Get5BInteger(edge_ptrs.ptr))
            != edge_len_m1) {
#ifdef __LOCK_FREE__
            READER_LOCK_FREE_STOP(edge_ptrs.offset, data)
#endif
            return MBError::READ_ERROR;
        }
        key_buff = node_buff;
    } else {
        key_buff = edge_ptrs.ptr;
    }

    rval = MBError::NOT_EXIST;
    if (edge_len < len) {
        if (edge_len > 1 && memcmp(key_buff, key + 1, edge_len_m1) != 0) {
#ifdef __LOCK_FREE__
            READER_LOCK_FREE_STOP(edge_ptrs.offset, data)
#endif
            return MBError::NOT_EXIST;
        }

        len -= edge_len;
        p += edge_len;

        if (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF) {
            // prefix match for leaf node
#ifdef __LOCK_FREE__
            READER_LOCK_FREE_STOP(edge_ptrs.offset, data)
#endif
            data.match_len = p - key;
            return ReadDataFromEdge(data, edge_ptrs);
        }

        uint8_t last_node_buffer[NODE_EDGE_KEY_FIRST];
#ifdef __LOCK_FREE__
        size_t edge_offset_prev = edge_ptrs.offset;
#endif
        int last_prefix_rval = MBError::NOT_EXIST;
        while (true) {
            rval = mm.NextEdge(p, edge_ptrs, node_buff, data);
            if (rval != MBError::READ_ERROR) {
                if (node_buff[0] & FLAG_NODE_MATCH) {
                    data.match_len = p - key;
                    memcpy(last_node_buffer, node_buff, NODE_EDGE_KEY_FIRST);
                    last_prefix_rval = MBError::SUCCESS;
                }
            }

            if (rval != MBError::SUCCESS)
                break;

#ifdef __LOCK_FREE__
            READER_LOCK_FREE_STOP(edge_offset_prev, data)
#endif
            edge_len = edge_ptrs.len_ptr[0];
            edge_len_m1 = edge_len - 1;
            // match edge string
            if (edge_len > LOCAL_EDGE_LEN) {
                if (mm.ReadData(node_buff, edge_len_m1, Get5BInteger(edge_ptrs.ptr))
                    != edge_len_m1) {
                    rval = MBError::READ_ERROR;
                    break;
                }
                key_buff = node_buff;
            } else {
                key_buff = edge_ptrs.ptr;
            }

            if ((edge_len > 1 && memcmp(key_buff, p + 1, edge_len_m1) != 0) || edge_len == 0) {
                rval = MBError::NOT_EXIST;
                break;
            }

            len -= edge_len;
            p += edge_len;
            if (len <= 0 || (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF)) {
                data.match_len = p - key;
                rval = ReadDataFromEdge(data, edge_ptrs);
                break;
            }
#ifdef __LOCK_FREE__
            edge_offset_prev = edge_ptrs.offset;
#endif
        }

        if (rval == MBError::NOT_EXIST && last_prefix_rval != rval)
            rval = ReadDataFromNode(data, last_node_buffer);
    } else if (edge_len == len) {
        if (edge_len_m1 == 0 || memcmp(key_buff, key + 1, edge_len_m1) == 0) {
            data.match_len = len;
            rval = ReadDataFromEdge(data, edge_ptrs);
        }
    }

#ifdef __LOCK_FREE__
    READER_LOCK_FREE_STOP(edge_ptrs.offset, data)
#endif
    return rval;
}

int Dict::Find(const uint8_t* key, int len, MBData& data)
{
    int rval;
    size_t rc_root_offset = header->rc_root_offset.load(MEMORY_ORDER_READER);

    if (rc_root_offset != 0) {
        reader_rc_off = rc_root_offset;
        rval = Find_Internal(rc_root_offset, key, len, data);
#ifdef __LOCK_FREE__
        while (rval == MBError::TRY_AGAIN) {
            nanosleep((const struct timespec[]) { { 0, 10L } }, NULL);
            rval = Find_Internal(rc_root_offset, key, len, data);
        }
#endif
        if (rval == MBError::SUCCESS) {
            data.match_len = len;
            return rval;
        } else if (rval != MBError::NOT_EXIST)
            return rval;
        data.options &= ~(CONSTS::OPTION_RC_MODE | CONSTS::OPTION_READ_SAVED_EDGE);
    } else {
        if (reader_rc_off != 0) {
            reader_rc_off = 0;
            RemoveUnused(0);
            mm.RemoveUnused(0);
        }
    }

    rval = Find_Internal(0, key, len, data);
#ifdef __LOCK_FREE__
    while (rval == MBError::TRY_AGAIN) {
        nanosleep((const struct timespec[]) { { 0, 10L } }, NULL);
        rval = Find_Internal(0, key, len, data);
    }
#endif
    if (rval == MBError::SUCCESS)
        data.match_len = len;

    return rval;
}

int Dict::Find_Internal(size_t root_off, const uint8_t* key, int len, MBData& data)
{
    EdgePtrs& edge_ptrs = data.edge_ptrs;
#ifdef __LOCK_FREE__
    READER_LOCK_FREE_START
#endif
    int rval;
    rval = mm.GetRootEdge(root_off, key[0], edge_ptrs);

    if (rval != MBError::SUCCESS)
        return MBError::READ_ERROR;
    if (edge_ptrs.len_ptr[0] == 0) {
#ifdef __LOCK_FREE__
        READER_LOCK_FREE_STOP(edge_ptrs.offset, data)
#endif
        return MBError::NOT_EXIST;
    }

    // Compare edge string
    const uint8_t* key_buff;
    uint8_t* node_buff = data.node_buff;
    const uint8_t* p = key;
    int edge_len = edge_ptrs.len_ptr[0];
    int edge_len_m1 = edge_len - 1;

    rval = MBError::NOT_EXIST;
    if (edge_len > LOCAL_EDGE_LEN) {
        size_t edge_str_off_lf = Get5BInteger(edge_ptrs.ptr);
        if (mm.ReadData(node_buff, edge_len_m1, edge_str_off_lf) != edge_len_m1) {
#ifdef __LOCK_FREE__
            READER_LOCK_FREE_STOP(edge_ptrs.offset, data)
#endif
            return MBError::READ_ERROR;
        }
        key_buff = node_buff;
    } else {
        key_buff = edge_ptrs.ptr;
    }

    if (edge_len < len) {
        if ((edge_len > 1 && memcmp(key_buff, key + 1, edge_len_m1) != 0)
            || (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF)) {
#ifdef __LOCK_FREE__
            READER_LOCK_FREE_STOP(edge_ptrs.offset, data)
#endif
            return MBError::NOT_EXIST;
        }

        len -= edge_len;
        p += edge_len;

#ifdef __LOCK_FREE__
        size_t edge_offset_prev = edge_ptrs.offset;
#endif
        while (true) {
            rval = mm.NextEdge(p, edge_ptrs, node_buff, data);
            if (rval != MBError::SUCCESS)
                break;

#ifdef __LOCK_FREE__
            READER_LOCK_FREE_STOP(edge_offset_prev, data)
#endif
            edge_len = edge_ptrs.len_ptr[0];
            edge_len_m1 = edge_len - 1;
            // match edge string
            if (edge_len > LOCAL_EDGE_LEN) {
                size_t edge_str_off_lf = Get5BInteger(edge_ptrs.ptr);
                if (mm.ReadData(node_buff, edge_len_m1, edge_str_off_lf) != edge_len_m1) {
                    rval = MBError::READ_ERROR;
                    break;
                }
                key_buff = node_buff;
            } else {
                key_buff = edge_ptrs.ptr;
            }

            if ((edge_len_m1 > 0 && memcmp(key_buff, p + 1, edge_len_m1) != 0) || edge_len_m1 < 0) {
                rval = MBError::NOT_EXIST;
                break;
            }

            len -= edge_len;
            if (len <= 0) {
                // If this is for remove operation, return IN_DICT to caller.
                if (data.options & CONSTS::OPTION_FIND_AND_STORE_PARENT)
                    rval = MBError::IN_DICT;
                else
                    rval = ReadDataFromEdge(data, edge_ptrs);
                break;
            } else {
                if (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF) {
                    // Reach a leaf node and no match found
                    rval = MBError::NOT_EXIST;
                    break;
                }
            }
            p += edge_len;
#ifdef __LOCK_FREE__
            edge_offset_prev = edge_ptrs.offset;
#endif
        }
    } else if (edge_len == len) {
        if (len > 1 && memcmp(key_buff, key + 1, len - 1) != 0) {
            rval = MBError::NOT_EXIST;
        } else {
            // If this is for remove operation, return IN_DICT to caller.
            if (data.options & CONSTS::OPTION_FIND_AND_STORE_PARENT) {
                data.edge_ptrs.curr_node_offset = mm.GetRootOffset();
                data.edge_ptrs.curr_nt = 1;
                data.edge_ptrs.curr_edge_index = 0;
                data.edge_ptrs.parent_offset = data.edge_ptrs.offset;
                rval = MBError::IN_DICT;
            } else {
                rval = ReadDataFromEdge(data, edge_ptrs);
            }
        }
    }

#ifdef __LOCK_FREE__
    READER_LOCK_FREE_STOP(edge_ptrs.offset, data)
#endif
    return rval;
}

void Dict::PrintStats(std::ostream* out_stream) const
{
    if (out_stream != NULL)
        return PrintStats(*out_stream);
    return PrintStats(std::cout);
}

void Dict::PrintStats(std::ostream& out_stream) const
{
    if (status != MBError::SUCCESS)
        return;

    out_stream << "DB stats:\n";
    out_stream << "\tWriter option: " << header->writer_options << std::endl;
    out_stream << "\tNumber of DB writer: " << header->num_writer << std::endl;
    out_stream << "\tNumber of DB reader: " << header->num_reader << std::endl;
    out_stream << "\tEntry count in DB: " << header->count << std::endl;
    out_stream << "\tEntry count per bucket: " << header->entry_per_bucket << std::endl;
    out_stream << "\tEviction bucket index: " << header->eviction_bucket_index << std::endl;
    out_stream << "\tData block size: " << header->data_block_size << std::endl;
    // The pending_data_buff_size in jemalloc mode is the total size of all allocated data buffers
    // The pending_data_buff_size in non-jemalloc mode is the total size of all free data buffers
    if (options & CONSTS::OPTION_JEMALLOC) {
        out_stream << "\tAllocated data memory size: " << header->pending_data_buff_size << std::endl;
    } else if (free_lists != nullptr) {
        out_stream << "\tData size: " << header->m_data_offset << std::endl;
        out_stream << "\tPending buffer size: " << header->pending_data_buff_size << std::endl;
        out_stream << "\tTrackable buffer size: " << free_lists->GetTotSize() << std::endl;
    }
    mm.PrintStats(out_stream);

    kv_file->PrintStats(out_stream);

#ifdef __DEBUG__
    out_stream << "Size of tracking buffer: " << buffer_map.size() << std::endl;
#endif
}

int64_t Dict::Count() const
{
    if (header == NULL) {
        Logger::Log(LOG_LEVEL_WARN, "db was not initialized successfully: %s",
            MBError::get_error_str(status));
        return 0;
    }

    return header->count;
}

// For DB iterator
int Dict::ReadNextEdge(const uint8_t* node_buff, EdgePtrs& edge_ptrs,
    int& match, MBData& data, std::string& match_str,
    size_t& node_off, bool rd_kv) const
{
    if (edge_ptrs.curr_nt > static_cast<int>(node_buff[1]))
        return MBError::OUT_OF_BOUND;

    if (mm.ReadData(edge_ptrs.edge_buff, EDGE_SIZE, edge_ptrs.offset) != EDGE_SIZE)
        return MBError::READ_ERROR;

    node_off = 0;
    match_str = "";

    int rval = MBError::SUCCESS;
    InitTempEdgePtrs(edge_ptrs);
    if (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF) {
        // match of leaf node
        match = MATCH_EDGE;
        if (rd_kv) {
            rval = ReadDataFromEdge(data, edge_ptrs);
            if (rval != MBError::SUCCESS)
                return rval;
        }
    } else {
        match = MATCH_NONE;
        if (edge_ptrs.len_ptr[0] > 0) {
            node_off = Get6BInteger(edge_ptrs.offset_ptr);
            if (rd_kv)
                rval = ReadNodeMatch(node_off, match, data);
        }
    }

    if (edge_ptrs.len_ptr[0] > 0 && rd_kv) {
        int edge_len_m1 = edge_ptrs.len_ptr[0] - 1;
        match_str = std::string(1, (const char)node_buff[NODE_EDGE_KEY_FIRST + edge_ptrs.curr_nt]);
        if (edge_len_m1 > LOCAL_EDGE_LEN_M1) {
            if (mm.ReadData(data.node_buff, edge_len_m1, Get5BInteger(edge_ptrs.ptr)) != edge_len_m1)
                return MBError::READ_ERROR;
            match_str += std::string(reinterpret_cast<char*>(data.node_buff), edge_len_m1);
        } else if (edge_len_m1 > 0) {
            match_str += std::string(reinterpret_cast<char*>(edge_ptrs.ptr), edge_len_m1);
        }
    }

    edge_ptrs.curr_nt++;
    edge_ptrs.offset += EDGE_SIZE;
    return rval;
}

// For DB iterator
int Dict::ReadNode(size_t node_off, uint8_t* node_buff, EdgePtrs& edge_ptrs,
    int& match, MBData& data, bool rd_kv) const
{
    if (mm.ReadData(node_buff, NODE_EDGE_KEY_FIRST, node_off) != NODE_EDGE_KEY_FIRST)
        return MBError::READ_ERROR;

    edge_ptrs.curr_nt = 0;
    int nt = node_buff[1] + 1;
    node_off += NODE_EDGE_KEY_FIRST;
    if (mm.ReadData(node_buff + NODE_EDGE_KEY_FIRST, nt, node_off) != nt)
        return MBError::READ_ERROR;

    int rval = MBError::SUCCESS;
    edge_ptrs.offset = node_off + nt;
    if (node_buff[0] & FLAG_NODE_MATCH) {
        // match of non-leaf node
        match = MATCH_NODE;
        if (rd_kv)
            rval = ReadDataFromNode(data, node_buff);
    } else {
        // no match at the non-leaf node
        match = MATCH_NONE;
    }

    return rval;
}

void Dict::ReadNodeHeader(size_t node_off, int& node_size, int& match,
    size_t& data_offset, size_t& data_link_offset)
{
    uint8_t node_buff[NODE_EDGE_KEY_FIRST];
    if (mm.ReadData(node_buff, NODE_EDGE_KEY_FIRST, node_off) != NODE_EDGE_KEY_FIRST)
        throw(int) MBError::READ_ERROR;

    node_size = mm.GetNodeSizePtr()[node_buff[1]];
    if (node_buff[0] & FLAG_NODE_MATCH) {
        match = MATCH_NODE;
        data_offset = Get6BInteger(node_buff + 2);
        data_link_offset = node_off + 2;
    }
}

int Dict::ReadNodeMatch(size_t node_off, int& match, MBData& data) const
{
    uint8_t node_buff[NODE_EDGE_KEY_FIRST];
    if (mm.ReadData(node_buff, NODE_EDGE_KEY_FIRST, node_off) != NODE_EDGE_KEY_FIRST)
        return MBError::READ_ERROR;

    int rval = MBError::SUCCESS;
    if (node_buff[0] & FLAG_NODE_MATCH) {
        match = MATCH_NODE;
        rval = ReadDataFromNode(data, node_buff);
        if (rval != MBError::SUCCESS)
            return rval;
    }

    return MBError::SUCCESS;
}

size_t Dict::GetRootOffset() const
{
    return mm.GetRootOffset();
}

// For DB iterator
int Dict::ReadRootNode(uint8_t* node_buff, EdgePtrs& edge_ptrs, int& match,
    MBData& data) const
{
    size_t root_off;
    if (data.options & CONSTS::OPTION_RC_MODE)
        root_off = header->rc_root_offset;
    else
        root_off = mm.GetRootOffset();
    return ReadNode(root_off, node_buff, edge_ptrs, match, data);
}

int Dict::Remove(const uint8_t* key, int len)
{
    MBData data(0, CONSTS::OPTION_FIND_AND_STORE_PARENT);
    return Remove(key, len, data);
}

int Dict::Remove(const uint8_t* key, int len, MBData& data)
{
    if (!(options & CONSTS::ACCESS_MODE_WRITER)) {
        return MBError::NOT_ALLOWED;
    }
    if (data.options & CONSTS::OPTION_RC_MODE) {
        // FIXME Remove in RC mode not implemented yet!!!
        return MBError::INVALID_ARG;
    }

    // The DELETE flag must be set
    if (!(data.options & CONSTS::OPTION_FIND_AND_STORE_PARENT))
        return MBError::INVALID_ARG;

    int rval;
    rval = Find(key, len, data);
    if (rval == MBError::IN_DICT) {
        rval = DeleteDataFromEdge(data, data.edge_ptrs);
        while (rval == MBError::TRY_AGAIN) {
            data.Clear();
            len -= data.edge_ptrs.len_ptr[0];
#ifdef __DEBUG__
            assert(len > 0);
#endif
            rval = Find(key, len, data);
            if (MBError::IN_DICT == rval) {
                rval = mm.RemoveEdgeByIndex(data.edge_ptrs, data);
            }
        }
    }

    if (rval == MBError::SUCCESS) {
        header->count--;
    }

    return rval;
}

int Dict::RemoveAll()
{
    int rval = MBError::SUCCESS;

    mm.ClearMem(); // clear memory will re-initialize jemalloc
    if (options & CONSTS::OPTION_JEMALLOC) {
        mm.InitRootNode();
        kv_file->ResetJemalloc();
        for (int c = 0; c < NUM_ALPHABET; c++) {
            rval = mm.ClearRootEdge(c);
            if (rval != MBError::SUCCESS)
                break;
        }
    } else {
        for (int c = 0; c < NUM_ALPHABET; c++) {
            rval = mm.ClearRootEdge(c);
            if (rval != MBError::SUCCESS)
                break;
        }
        header->m_data_offset = GetStartDataOffset();
        free_lists->Empty();
    }
    header->pending_data_buff_size = 0;
    header->count = 0;
    header->eviction_bucket_index = 0;
    header->num_update = 0;
    return rval;
}

pthread_mutex_t* Dict::GetShmLockPtr() const
{
    return &(slaq->lock);
}

AsyncNode* Dict::GetAsyncQueuePtr() const
{
    return slaq->queue;
}

// Reserve buffer and write to it
// The pending_data_buff_size in jemalloc mode is the total size of all allocated data buffers
// The pending_data_buff_size in non-jemalloc mode is the total size of all free data buffers
void Dict::ReserveData(const uint8_t* buff, int size, size_t& offset)
{
    if (options & CONSTS::OPTION_JEMALLOC) {
        int buf_size = size + DATA_HDR_BYTE;
        void* ptr = kv_file->Malloc(buf_size, offset);
        if (ptr == NULL) {
            Logger::Log(LOG_LEVEL_ERROR, "failed to allocate memory for data buffer");
            throw MBError::NO_MEMORY;
        }
        uint16_t dsize[2];
        dsize[0] = static_cast<uint16_t>(size);
        // store bucket index for LRU eviction
        dsize[1] = (header->num_update / header->entry_per_bucket) % 0xFFFF;
        if (dsize[1] == header->eviction_bucket_index && header->num_update > header->entry_per_bucket) {
            header->eviction_bucket_index++;
        }
        memcpy(ptr, &dsize[0], DATA_HDR_BYTE);
        memcpy(static_cast<uint8_t*>(ptr) + DATA_HDR_BYTE, buff, size);
        // update the size of pending data buffer in the header
        header->pending_data_buff_size += (buf_size + JEMALLOC_ALIGNMENT - 1) & ~(JEMALLOC_ALIGNMENT - 1);
    } else {
        reserveDataFL(buff, size, offset);
    }

#ifdef __DEBUG__
    // offset is allocated, add it to the tracking map
    // note if reserve failed, an exception will be thrown before this point
    add_tracking_buffer(offset, size);
#endif
}

void Dict::reserveDataFL(const uint8_t* buff, int size, size_t& offset)
{
#ifdef __DEBUG__
    assert(size <= CONSTS::MAX_DATA_SIZE);
#endif
    int buf_size = free_lists->GetAlignmentSize(size + DATA_HDR_BYTE);
    int buf_index = free_lists->GetBufferIndex(buf_size);
    uint16_t dsize[2];
    dsize[0] = static_cast<uint16_t>(size);
    // store bucket index for LRU eviction
    dsize[1] = (header->num_update / header->entry_per_bucket) % 0xFFFF;
    if (dsize[1] == header->eviction_bucket_index && header->num_update > header->entry_per_bucket) {
        header->eviction_bucket_index++;
    }

    if (free_lists->GetBufferCountByIndex(buf_index) > 0) {
        offset = free_lists->RemoveBufferByIndex(buf_index);
        WriteData(reinterpret_cast<const uint8_t*>(&dsize[0]), DATA_HDR_BYTE, offset);
        WriteData(buff, size, offset + DATA_HDR_BYTE);
        header->pending_data_buff_size -= buf_size;
    } else {
        size_t old_off = header->m_data_offset;
        uint8_t* ptr;

        int rval = kv_file->Reserve(header->m_data_offset, buf_size, ptr);
        if (rval != MBError::SUCCESS)
            throw rval;

        // Checking missing buffer due to alignment
        if (old_off < header->m_data_offset) {
            ReleaseAlignmentBuffer(old_off, header->m_data_offset);
            header->pending_data_buff_size += header->m_data_offset - old_off;
        }

        offset = header->m_data_offset;
        header->m_data_offset += buf_size;
        if (ptr != NULL) {
            memcpy(ptr, &dsize[0], DATA_HDR_BYTE);
            memcpy(ptr + DATA_HDR_BYTE, buff, size);
        } else {
            WriteData(reinterpret_cast<const uint8_t*>(&dsize[0]), DATA_HDR_BYTE, offset);
            WriteData(buff, size, offset + DATA_HDR_BYTE);
        }
    }
}

int Dict::ReleaseBuffer(size_t offset, int size)
{
#ifdef __DEBUG__
    remove_tracking_buffer(offset, size);
#endif
    if (options & CONSTS::OPTION_JEMALLOC) {
        kv_file->Free(offset);
        size_t rel_size = ((size_t)size + JEMALLOC_ALIGNMENT - 1) & ~(JEMALLOC_ALIGNMENT - 1);
        header->pending_data_buff_size -= (int64_t)rel_size;
        if (header->pending_data_buff_size < 0) {
            Logger::Log(LOG_LEVEL_DEBUG, "pending data buffer size is negative: %d",
                header->pending_data_buff_size);
            header->pending_data_buff_size = 0;
        }
        return MBError::SUCCESS;
    } else {
        header->pending_data_buff_size += size;
        return free_lists->ReleaseBuffer(offset, size);
    }
}

void Dict::ReleaseAlignmentBuffer(size_t offset, size_t alignment_off)
{
    // alignment is not a real buffer, so no need to track it
    if (options & CONSTS::OPTION_JEMALLOC) {
        // no-op
    } else {
        free_lists->ReleaseAlignmentBuffer(offset, alignment_off);
    }
}

int Dict::ReleaseBuffer(size_t offset)
{
#ifdef __DEBUG__
    remove_tracking_buffer(offset);
#endif
    // First read the size of the data buffer
    uint16_t data_size;
    if (ReadData(reinterpret_cast<uint8_t*>(&data_size), DATA_SIZE_BYTE, offset) != DATA_SIZE_BYTE) {
        if (options & CONSTS::OPTION_JEMALLOC) {
            // For jemalloc mode, we can just free the buffer and return
            kv_file->Free(offset);
        }
        return MBError::READ_ERROR;
    }
    data_size += DATA_HDR_BYTE;
    if (options & CONSTS::OPTION_JEMALLOC) {
        kv_file->Free(offset);

        size_t rel_size = ((size_t)data_size + JEMALLOC_ALIGNMENT - 1) & ~(JEMALLOC_ALIGNMENT - 1);
        header->pending_data_buff_size -= (int64_t)rel_size;
        if (header->pending_data_buff_size < 0) {
            Logger::Log(LOG_LEVEL_DEBUG, "pending data buffer size is negative: %d",
                header->pending_data_buff_size);
            header->pending_data_buff_size = 0;
        }
        return MBError::SUCCESS;
    } else {
        int rel_size = free_lists->GetAlignmentSize(data_size);
        header->pending_data_buff_size += rel_size;
        return free_lists->ReleaseBuffer(offset, rel_size);
    }
}

int Dict::UpdateDataBuffer(EdgePtrs& edge_ptrs, bool overwrite, const uint8_t* buff,
    int len, bool& inc_count)
{
    size_t data_off;

    if (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF) {
        inc_count = false;
        // leaf node
        if (!overwrite)
            return MBError::IN_DICT;

        data_off = Get6BInteger(edge_ptrs.offset_ptr);
        if (ReleaseBuffer(data_off) != MBError::SUCCESS)
            Logger::Log(LOG_LEVEL_WARN, "failed to release data buffer: %llu", data_off);
        ReserveData(buff, len, data_off);
        Write6BInteger(edge_ptrs.offset_ptr, data_off);

        header->excep_lf_offset = edge_ptrs.offset;
        memcpy(header->excep_buff, edge_ptrs.offset_ptr, OFFSET_SIZE);
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStart(edge_ptrs.offset);
#endif
        header->excep_updating_status = EXCEP_STATUS_ADD_DATA_OFF;
        mm.WriteData(edge_ptrs.offset_ptr, OFFSET_SIZE, edge_ptrs.offset + EDGE_NODE_LEADING_POS);
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStop();
#endif
        header->excep_updating_status = EXCEP_STATUS_NONE;
    } else {
        uint8_t* node_buff = header->excep_buff;
        size_t node_off = Get6BInteger(edge_ptrs.offset_ptr);

        if (mm.ReadData(node_buff, NODE_EDGE_KEY_FIRST, node_off) != NODE_EDGE_KEY_FIRST)
            return MBError::READ_ERROR;

        if (node_buff[0] & FLAG_NODE_MATCH) {
            inc_count = false;
            if (!overwrite)
                return MBError::IN_DICT;

            data_off = Get6BInteger(node_buff + 2);
            if (ReleaseBuffer(data_off) != MBError::SUCCESS)
                Logger::Log(LOG_LEVEL_WARN, "failed to release data buffer %llu", data_off);

            node_buff[NODE_EDGE_KEY_FIRST] = 0;
        } else {
            // set the match flag
            node_buff[0] |= FLAG_NODE_MATCH;

            node_buff[NODE_EDGE_KEY_FIRST] = 1;
        }

        ReserveData(buff, len, data_off);
        Write6BInteger(node_buff + 2, data_off);

        header->excep_offset = node_off;
#ifdef __LOCK_FREE__
        header->excep_lf_offset = edge_ptrs.offset;
        lfree.WriterLockFreeStart(edge_ptrs.offset);
#endif
        header->excep_updating_status = EXCEP_STATUS_ADD_NODE;
        mm.WriteData(node_buff, NODE_EDGE_KEY_FIRST, node_off);
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStop();
#endif
        header->excep_updating_status = EXCEP_STATUS_NONE;
    }

    return MBError::SUCCESS;
}

// delta should be either +1 or -1.
void Dict::UpdateNumReader(int delta) const
{
    header->num_reader += delta;
    if (header->num_reader < 0)
        header->num_reader = 0;

    Logger::Log(LOG_LEVEL_DEBUG, "number of reader is set to: %d",
        header->num_reader);
}

// delta should be either +1 or -1.
int Dict::UpdateNumWriter(int delta) const
{
    if (delta > 0) {
        // Only one writer allowed
        if (header->num_writer > 0) {
            Logger::Log(LOG_LEVEL_WARN, "writer was not shutdown cleanly previously");
            header->num_writer = 1;
            // Reset number of reader too.
            header->num_reader = 0;
            return MBError::WRITER_EXIST;
        }

        header->num_writer = 1;
    } else if (delta < 0) {
        header->num_writer = 0;
        header->lock_free.offset = MAX_6B_OFFSET;
    }

    Logger::Log(LOG_LEVEL_DEBUG, "number of writer is set to: %d", header->num_writer);
    return MBError::SUCCESS;
}

DictMem* Dict::GetMM() const
{
    return (DictMem*)&mm;
}

size_t Dict::GetStartDataOffset() const
{
    // TODO: handle data header allocation in jemalloc mode.
    // Note this header is not used currently
    return DATA_HEADER_SIZE;
}

LockFree* Dict::GetLockFreePtr()
{
    return &lfree;
}

void Dict::Flush() const
{
    if (!(options & CONSTS::ACCESS_MODE_WRITER))
        return;

    if (kv_file != NULL)
        kv_file->Flush();
    mm.Flush();
}

void Dict::Purge() const
{
    if ((options & CONSTS::ACCESS_MODE_WRITER) && (options & CONSTS::OPTION_JEMALLOC)) {
        if (kv_file != nullptr)
            kv_file->Purge();
        mm.Purge();
    }
}

// Recovery from abnormal writer terminations (segfault, kill -9 etc)
// during DB updates (insertion, replacing and deletion).
int Dict::ExceptionRecovery()
{
    if (header == NULL)
        return MBError::NOT_INITIALIZED;

    int rval = MBError::SUCCESS;
    if (header->excep_updating_status == EXCEP_STATUS_NONE) {
        Logger::Log(LOG_LEVEL_DEBUG, "writer was shutdown successfully previously");
        return rval;
    }

    Logger::Log(LOG_LEVEL_INFO, "writer was not shutdown gracefully with exception status %d",
        header->excep_updating_status);
    // Dumper header before running recover
    std::ofstream* logstream = Logger::GetLogStream();
    if (logstream != NULL) {
        PrintHeader(*logstream);
    } else {
        PrintHeader(std::cout);
    }

    switch (header->excep_updating_status) {
    case EXCEP_STATUS_ADD_EDGE:
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStart(header->excep_lf_offset);
#endif
        mm.WriteData(header->excep_buff, EDGE_SIZE, header->excep_lf_offset);
        header->count++;
        break;
    case EXCEP_STATUS_ADD_DATA_OFF:
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStart(header->excep_lf_offset);
#endif
        mm.WriteData(header->excep_buff, OFFSET_SIZE,
            header->excep_lf_offset + EDGE_NODE_LEADING_POS);
        break;
    case EXCEP_STATUS_ADD_NODE:
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStart(header->excep_lf_offset);
#endif
        mm.WriteData(header->excep_buff, NODE_EDGE_KEY_FIRST,
            header->excep_offset);
        if (header->excep_buff[NODE_EDGE_KEY_FIRST])
            header->count++;
        break;
    case EXCEP_STATUS_REMOVE_EDGE:
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStart(header->excep_lf_offset);
#endif
        Write6BInteger(header->excep_buff, header->excep_offset);
        mm.WriteData(header->excep_buff, OFFSET_SIZE,
            header->excep_lf_offset + EDGE_NODE_LEADING_POS);
        break;
    case EXCEP_STATUS_CLEAR_EDGE:
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStart(header->excep_lf_offset);
#endif
        mm.WriteData(DictMem::empty_edge, EDGE_SIZE, header->excep_lf_offset);
        header->count--;
        break;
    case EXCEP_STATUS_RC_NODE:
    case EXCEP_STATUS_RC_DATA:
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStart(header->excep_lf_offset);
#endif
        mm.WriteData(header->excep_buff, OFFSET_SIZE, header->excep_offset);
        break;
    case EXCEP_STATUS_RC_EDGE_STR:
#ifdef __LOCK_FREE__
        lfree.WriterLockFreeStart(header->excep_lf_offset);
#endif
        mm.WriteData(header->excep_buff, OFFSET_SIZE - 1, header->excep_offset);
        break;
    default:
        Logger::Log(LOG_LEVEL_ERROR, "unknown exception status: %d",
            header->excep_updating_status);
        rval = MBError::INVALID_ARG;
    }
#ifdef __LOCK_FREE__
    lfree.WriterLockFreeStop();
#endif

    if (rval == MBError::SUCCESS) {
        header->excep_updating_status = EXCEP_STATUS_NONE;
        Logger::Log(LOG_LEVEL_INFO, "successfully recovered from abnormal termination");
    } else {
        Logger::Log(LOG_LEVEL_ERROR, "failed to recover from abnormal termination");
    }

    return rval;
}

void Dict::WriteData(const uint8_t* buff, unsigned len, size_t offset) const
{
    if (options & CONSTS::OPTION_JEMALLOC) {
        kv_file->MemWrite(buff, len, offset);
    } else {
        if (offset + len > header->m_data_offset) {
            std::cerr << "invalid dict write: " << offset << " " << len << " "
                      << header->m_data_offset << "\n";
            throw(int) MBError::OUT_OF_BOUND;
        }

        if (kv_file->RandomWrite(buff, len, offset) != len)
            throw(int) MBError::WRITE_ERROR;
    }
}

int Dict::ReadDataByOffset(size_t offset, MBData& data) const
{
    uint16_t hdr[2];
    if (ReadData(reinterpret_cast<uint8_t*>(&hdr[0]), DATA_HDR_BYTE, offset) != DATA_HDR_BYTE) {
        return MBError::READ_ERROR;
    }

    // store bucket index
    data.data_len = hdr[0]; // store data length
    data.bucket_index = hdr[1];
    data.Resize(hdr[0]); // resize data buffer using size from header
    offset += DATA_HDR_BYTE;
    if (ReadData(data.buff, hdr[0], offset) != hdr[0]) {
        return MBError::READ_ERROR;
    }
    return MBError::SUCCESS;
}

}
