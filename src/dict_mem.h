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

#ifndef __DICTMEM_H__
#define __DICTMEM_H__

#include <string>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>

#include "drm_base.h"
#include "db.h"
#include "mabain_consts.h"
#include "mb_data.h"
#include "rollable_file.h"
#include "mb_lsq.h"
#include "free_list.h"
#include "lock_free.h"
#include "error.h"

namespace mabain {

typedef struct _NodePtrs
{
    size_t   offset;
    uint8_t *ptr;

    uint8_t *edge_key_ptr;
    uint8_t *edge_ptr;
} NodePtrs;

// Memory management class for the dictionary
class DictMem : public DRMBase
{
public:
    DictMem(const std::string &mbdir, bool init_header, size_t memsize,
            int mode, uint32_t block_size, int max_num_blk);
    void Destroy();
    virtual ~DictMem();

    bool IsValid() const;
    void PrintStats(std::ostream &out_stream) const;

    void InitNodePtrs(uint8_t *ptr, int nt, NodePtrs &node_ptrs);
    void InitEdgePtrs(const NodePtrs &node_ptrs, int index,
                  EdgePtrs &edge_ptrs);
    void AddRootEdge(EdgePtrs &edge_ptrs, const uint8_t *key, int len,
                  size_t data_offset);
    int  InsertNode(EdgePtrs &edge_ptrs, int match_len, size_t data_offset,
                  MBData &data);
    int  AddLink(EdgePtrs &edge_ptrs, int match_len, const uint8_t *key,
                  int key_len, size_t data_off, MBData &data);
    int  UpdateNode(EdgePtrs &edge_ptrs, const uint8_t *key, int key_len,
                  size_t data_off);
    bool FindNext(const unsigned char *key, int keylen, int &match_len,
                  EdgePtrs &edge_ptr, uint8_t *key_tmp) const;
    int  GetRootEdge(int nt, EdgePtrs &edge_ptrs) const;
    int  GetRootEdge_Writer(int nt, EdgePtrs &edge_ptrs) const;
    int  ClearRootEdge(int nt) const;
    void ReserveData(const uint8_t* key, int size, size_t &offset,
                     bool map_new_sliding=true);
    int  NextEdge(const uint8_t *key, EdgePtrs &edge_ptrs,
                  uint8_t *tmp_buff, bool update_parent_info=false) const;
    int  RemoveEdgeByIndex(const EdgePtrs &edge_ptrs, MBData &data);
    void InitRootNode();
    inline void WriteEdge(const EdgePtrs &edge_ptrs) const;
    void WriteData(const uint8_t *buff, unsigned len, size_t offset) const;
    inline size_t GetRootOffset() const;
    void ClearMem() const;
    const int* GetNodeSizePtr() const;
    void ResetSlidingWindow() const;

    void InitLockFreePtr(LockFree *lf);

    void Flush() const;

    // empty edge, used for clearing edges
    static const uint8_t empty_edge[EDGE_SIZE];

private:
    bool     ReserveNode(int nt, size_t &offset, uint8_t* &ptr);
    void     ReleaseNode(size_t offset, int nt);
    void     ReleaseBuffer(size_t offset, int size);
    void     UpdateTailEdge(EdgePtrs &edge_ptrs, int match_len, MBData &data,
                            EdgePtrs &tail_edge, uint8_t &new_key_first,
                            bool &map_new_sliding);
    void     UpdateHeadEdge(EdgePtrs &edge_ptrs, int match_len,
                            MBData &data, int &release_buffer_size,
                            size_t &edge_str_off, bool &map_new_sliding);
    void     RemoveRootEdge(const EdgePtrs &edge_ptrs);
    int      RemoveEdgeSizeN(const EdgePtrs &edge_ptrs, int nt, size_t node_offset,
                            uint8_t *old_node_buffer, size_t &str_off_rel,
                            int &str_size_rel, size_t parent_edge_offset);
    int      RemoveEdgeSizeOne(uint8_t *old_node_buffer, size_t parent_edge_offset,
                            size_t node_offset, int nt, size_t &str_off_rel,
                            int &str_size_rel);

    int *node_size;
    bool is_valid;

    size_t root_offset;
    uint8_t *node_ptr;

    // lock free pointer
    LockFree *lfree;

    // header file
    MmapFileIO *header_file;
};

inline void DictMem::WriteEdge(const EdgePtrs &edge_ptrs) const
{
    if(edge_ptrs.offset + EDGE_SIZE > header->m_index_offset)
    {
        std::cerr << "invalid edge write: " << edge_ptrs.offset << " " << EDGE_SIZE
                  << " " << header->m_index_offset << "\n";
        throw (int) MBError::OUT_OF_BOUND;
    }

    // for segfault recovery
    header->excep_lf_offset = edge_ptrs.offset;
    header->excep_updating_status = EXCEP_STATUS_ADD_EDGE;

    if(kv_file->RandomWrite(edge_ptrs.ptr, EDGE_SIZE, edge_ptrs.offset) != EDGE_SIZE)
        throw (int) MBError::WRITE_ERROR;

    // unset the segault flag
    header->excep_updating_status = EXCEP_STATUS_NONE;
}

inline size_t DictMem::GetRootOffset() const
{
    return root_offset;
}

// update the edge pointers for fast access
// node_ptrs.offset and node_ptrs.ptr[1] must already be populated before calling this function
inline void DictMem::InitEdgePtrs(const NodePtrs &node_ptrs, int index, EdgePtrs &edge_ptrs)
{
    int edge_off = NODE_EDGE_KEY_FIRST + node_ptrs.ptr[1] + 1  + index*EDGE_SIZE;
    edge_ptrs.offset = node_ptrs.offset + edge_off;
    edge_ptrs.ptr = node_ptrs.ptr + edge_off;
    edge_ptrs.len_ptr = edge_ptrs.ptr + EDGE_LEN_POS;
    edge_ptrs.flag_ptr = edge_ptrs.ptr + EDGE_FLAG_POS;
    edge_ptrs.offset_ptr = edge_ptrs.flag_ptr + 1;
}

inline void InitTempEdgePtrs(EdgePtrs &edge_ptrs)
{
    edge_ptrs.ptr = edge_ptrs.edge_buff;
    edge_ptrs.len_ptr = edge_ptrs.ptr + EDGE_LEN_POS;
    edge_ptrs.flag_ptr = edge_ptrs.ptr + EDGE_FLAG_POS;
    edge_ptrs.offset_ptr = edge_ptrs.flag_ptr + 1;
}

// node_ptrs.offset must be populated before caling this function
inline void DictMem::InitNodePtrs(uint8_t *ptr, int nt, NodePtrs &node_ptrs)
{
    node_ptrs.ptr = ptr;
    nt++;
    node_ptrs.edge_key_ptr = ptr + NODE_EDGE_KEY_FIRST;
    node_ptrs.edge_ptr = node_ptrs.edge_key_ptr + nt;
}

}

#endif
