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

#include <string>
#include <iostream>
#include <limits.h>

#include "db.h"
#include "dict_mem.h"
#include "logger.h"
#include "error.h"
#include "integer_4b_5b.h"
#include "version.h"
#include "resource_pool.h"
#include "async_writer.h"

#define OFFSET_SIZE_P1             7

#define MAX_BUFFER_RESERVE_SIZE    8192
#define NUM_BUFFER_RESERVE         MAX_BUFFER_RESERVE_SIZE/BUFFER_ALIGNMENT

namespace mabain {

/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
// EDGE MEMORY LAYOUT
// Edge size is 13 bytes.
// X************    leading byte of edge key offset
// *XXXX********    edge key string or edge key offset
// *****X*******    edge key length
// ******X******    flag (0x01) indicating the data offset; This bit can used to
//                      determine if the node if a leaf-node.
//                  flag (0x02) indicating this edge owns the allocated bufifer
// *******X*****    leading byte of next node offset or data offset
// ********xxxxX    next node offset of data offset
/////////////////////////////////////////////////////////////////////////////////////
// NODE MEMORY LAYOUT
// Node size is 1 + 1 + 6 + NT + NT*13
// X************   flags (0x01 bit indicating match found)
// *X***********   nt-1, nt is the number of edges for this node.
// **XXXXXX*****   data offset
// NT bytes of first characters of each edge
// NT edges        NT*13 bytes
// Since we use 6-byte to store both the index and data offset, the maximum size for
// data and index is 281474976710655 bytes (or 255T).
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////


DictMem::DictMem(const std::string &mbdir, bool init_header, size_t memsize,
                 int mode, uint32_t block_size, int max_num_blk)
               : is_valid(false)
{
    root_offset = 0;
    root_offset_rc = 0;
    node_ptr = NULL;
    kv_file = NULL;
    node_size = NULL;

    assert(sizeof(IndexHeader) <= (unsigned) RollableFile::page_size);
    bool map_hdr = true;
    bool create_hdr = false;
    size_t hdr_size = RollableFile::page_size;
    if(mode & CONSTS::ACCESS_MODE_WRITER)
        create_hdr = true;
#ifdef __SHM_QUEUE__
    hdr_size += sizeof(AsyncNode) * MB_MAX_NUM_SHM_QUEUE_NODE;
#endif
    header_file = ResourcePool::getInstance().OpenFile(mbdir + "_mabain_h",
                                                       mode,
                                                       hdr_size,
                                                       map_hdr,
                                                       create_hdr);
    header = (IndexHeader *) header_file->GetMapAddr();
    if(header == NULL)
        return;

    // Both reader and writer need to open the mmapped file.
    if(!init_header)
    {
        if(block_size != 0 && header->index_block_size != block_size)
        {
            std::cerr << "mabain index block size not match\n";
            Destroy();
            throw (int) MBError::INVALID_SIZE;
        }
    }
    else
    {
        memset(header, 0, sizeof(IndexHeader));
        header->index_block_size = block_size;
    }
    kv_file = new RollableFile(mbdir + "_mabain_i",
                               static_cast<size_t>(header->index_block_size),
                               memsize, mode, max_num_blk);

    kv_file->InitShmSlidingAddr(&header->shm_index_sliding_start);

    if(!(mode & CONSTS::ACCESS_MODE_WRITER))
    {
        node_size = NULL;
        is_valid = true;
        return;
    }

    ////////////////////////////////////
    // Writor only initialization below
    ////////////////////////////////////

    node_size = new int[NUM_ALPHABET];

    for(int i = 0; i < NUM_ALPHABET; i++)
    {
        int nt = i + 1;
        node_size[i] = 1 + 1 + OFFSET_SIZE + nt + nt*EDGE_SIZE;
    }

    node_ptr = new uint8_t[ node_size[NUM_ALPHABET-1] ];
    free_lists = new FreeList(mbdir+"_ibfl", BUFFER_ALIGNMENT, NUM_BUFFER_RESERVE);

    if(init_header)
    {
        // Set up DB version
        header->version[0] = version[0];
        header->version[1] = version[1];
        header->version[2] = version[2];
        header->version[3] = 0;
        // Cannot set is_valid to true.
        // More init to be dobe in InitRootNode.
    }
    else
    {
        is_valid = true;
    }
    Logger::Log(LOG_LEVEL_INFO, "set up mabain db version to %u.%u.%u",
                header->version[0], header->version[1], header->version[2]);
}

// The whole edge is initizlized to zero.
const uint8_t DictMem::empty_edge[] = {0};

void DictMem::InitRootNode()
{
#ifdef __DEBUG__
    assert(header != NULL);
#endif
    header->m_index_offset = 0;

    bool node_move;
    uint8_t *root_node;

    node_move = ReserveNode(NUM_ALPHABET-1, root_offset, root_node);
#ifdef __DEBUG__
    assert(root_offset == 0);
#endif

    root_node[0] = FLAG_NODE_NONE;
    root_node[1] = NUM_ALPHABET-1;
    for(int i = 0; i < NUM_ALPHABET; i++)
    {
        root_node[NODE_EDGE_KEY_FIRST+i] = static_cast<uint8_t>(i);
    }

    if(node_move)
        WriteData(root_node, node_size[NUM_ALPHABET-1], root_offset);

    // Everything is running fine if reaching this point.
    is_valid = true;
}

DictMem::~DictMem()
{
}

void DictMem::Destroy()
{
    if(kv_file != NULL)
        delete kv_file;

    if(free_lists != NULL)
        delete free_lists;

    if(node_size != NULL)
        delete [] node_size;
    if(node_ptr != NULL)
        delete [] node_ptr;
}

bool DictMem::IsValid() const
{
    return is_valid;
}

// Add root edge
void DictMem::AddRootEdge(EdgePtrs &edge_ptrs, const uint8_t *key,
                         int len, size_t data_offset)
{
    edge_ptrs.len_ptr[0] = len;
    if(len > LOCAL_EDGE_LEN)
    {
        size_t edge_str_off;
        ReserveData(key+1, len-1, edge_str_off);
        Write5BInteger(edge_ptrs.ptr, edge_str_off);
    }
    else
    {
        memcpy(edge_ptrs.ptr, key+1, len-1);
    }

    edge_ptrs.flag_ptr[0] = EDGE_FLAG_DATA_OFF;
    Write6BInteger(edge_ptrs.offset_ptr, data_offset);

#ifdef __LOCK_FREE__
    header->excep_lf_offset = edge_ptrs.offset;
    header->excep_updating_status = EXCEP_STATUS_ADD_EDGE;
    lfree->WriterLockFreeStart(edge_ptrs.offset);
#endif
    WriteEdge(edge_ptrs);
#ifdef __LOCK_FREE__
    lfree->WriterLockFreeStop();
    header->excep_updating_status = EXCEP_STATUS_NONE;
#endif
}

void DictMem::UpdateTailEdge(EdgePtrs &edge_ptrs, int match_len, MBData &data,
                             EdgePtrs &tail_edge, uint8_t &new_key_first,
                             bool &map_new_sliding)
{
    int edge_len = edge_ptrs.len_ptr[0] - match_len;
    tail_edge.len_ptr[0] = edge_len;
    if(edge_len > LOCAL_EDGE_LEN)
    {
        // Old key len must be greater than LOCAL_EDGE_LEN too.
        // Load the string with length edge_len.
        size_t new_key_off;
        size_t edge_str_off = Get5BInteger(edge_ptrs.ptr);
        if(ReadData(data.node_buff, edge_len, edge_str_off + match_len - 1)
                   != edge_len)
            throw (int) MBError::READ_ERROR;          
        new_key_first = data.node_buff[0];

        // Reserve the key buffer
        ReserveData(data.node_buff+1, edge_len-1, new_key_off, map_new_sliding);
        map_new_sliding = false;
        Write5BInteger(tail_edge.ptr, new_key_off);
    }
    else
    {
        if(edge_ptrs.len_ptr[0] > LOCAL_EDGE_LEN)
        {
            if(ReadData(data.node_buff, edge_len, Get5BInteger(edge_ptrs.ptr)+match_len-1)
                       != edge_len)
                throw (int) MBError::READ_ERROR;
            new_key_first = data.node_buff[0];
            if(edge_len > 1)
                memcpy(tail_edge.ptr, data.node_buff+1, edge_len-1);
        }
        else
        {
            // Both new and old keys are local
            new_key_first = edge_ptrs.ptr[match_len - 1];
            if(edge_len > 1)
                memcpy(tail_edge.ptr, edge_ptrs.ptr+match_len, edge_len-1);
        }
    }

    // 7 = 1 + OFFSET_SIZE
    memcpy(tail_edge.flag_ptr, edge_ptrs.flag_ptr, OFFSET_SIZE_P1);
}

//The old edge becomes head edge.
void DictMem::UpdateHeadEdge(EdgePtrs &edge_ptrs, int match_len,
                             MBData &data, int &release_buffer_size,
                             size_t &edge_str_off, bool &map_new_sliding)
{
    int match_len_m1 = match_len - 1;
    if(edge_ptrs.len_ptr[0] > LOCAL_EDGE_LEN)
    {
        if(match_len <= LOCAL_EDGE_LEN)
        {
            edge_str_off = Get5BInteger(edge_ptrs.ptr);
            release_buffer_size = edge_ptrs.len_ptr[0] - 1;
            // Old key is remote but new key is local. Need to read the old key.
            if(match_len_m1 > 0)
            {
                if(ReadData(edge_ptrs.ptr, match_len_m1, edge_str_off) != match_len_m1)
                    throw (int) MBError::READ_ERROR;
            }
        }
        else
        {
            edge_str_off = Get5BInteger(edge_ptrs.ptr);
            release_buffer_size = edge_ptrs.len_ptr[0] - 1;
            // Load the string with length edge_len - 1
            if(ReadData(data.node_buff, match_len_m1, edge_str_off) != match_len_m1)
                throw (int) MBError::READ_ERROR;
            // Reserve the key buffer
            size_t new_key_off;
            ReserveData(data.node_buff, match_len_m1, new_key_off, map_new_sliding);
            map_new_sliding = false;
            Write5BInteger(edge_ptrs.ptr, new_key_off);
        }
    }

    edge_ptrs.len_ptr[0] = match_len;
    edge_ptrs.flag_ptr[0] = 0;
}

// Insert a new node on the current edge
// The old edge becomes two edges.
int DictMem::InsertNode(EdgePtrs &edge_ptrs, int match_len,
                        size_t data_offset, MBData &data)
{
    NodePtrs node_ptrs;
    EdgePtrs new_edge_ptrs;
    bool node_move;
    uint8_t *node; 
    bool map_new_sliding = false;

    // The new node has one edge. nt1 = nt - 1 = 0
    node_move = ReserveNode(0, node_ptrs.offset, node);
    if(node_move)
        map_new_sliding = true;

    InitNodePtrs(node, 0, node_ptrs);
    node[1] = 0;
    InitEdgePtrs(node_ptrs, 0, new_edge_ptrs);

    uint8_t new_key_first;
    UpdateTailEdge(edge_ptrs, match_len, data, new_edge_ptrs, new_key_first,
                   map_new_sliding);

    int release_buffer_size = 0;
    size_t edge_str_off = 0;
    UpdateHeadEdge(edge_ptrs, match_len, data, release_buffer_size, edge_str_off,
                   map_new_sliding);
    Write6BInteger(edge_ptrs.offset_ptr, node_ptrs.offset);

    // Update the new node
    // match found for the new node
    node[0] = FLAG_NODE_NONE | FLAG_NODE_MATCH;
    // Update data offset in the node
    Write6BInteger(node_ptrs.ptr+2, data_offset);
    // Update the first character in edge key
    node_ptrs.edge_key_ptr[0] = new_key_first;

    if(node_move)
        WriteData(node, node_size[0], node_ptrs.offset);

    if(release_buffer_size > 0)
        ReleaseBuffer(edge_str_off, release_buffer_size);
#ifdef __LOCK_FREE__
    header->excep_lf_offset = edge_ptrs.offset;
    header->excep_updating_status = EXCEP_STATUS_ADD_EDGE;
    lfree->WriterLockFreeStart(edge_ptrs.offset);
#endif
    WriteEdge(edge_ptrs);
#ifdef __LOCK_FREE__
    lfree->WriterLockFreeStop();
    header->excep_updating_status = EXCEP_STATUS_NONE;
#endif

    header->n_edges++;
    return MBError::SUCCESS;
}

// Insert a new node on the current edge.
// Add a new edge to the new node. The new node will have two edges.
// The old edge becomes two edges.
int DictMem::AddLink(EdgePtrs &edge_ptrs, int match_len, const uint8_t *key,
                     int key_len, size_t data_off, MBData &data)
{
    NodePtrs node_ptrs;
    EdgePtrs new_edge_ptrs[2];
    bool node_move;
    uint8_t* node;
    bool map_new_sliding = false;

    // The new node has two edge. nt1 = nt - 1 = 1
    node_move = ReserveNode(1, node_ptrs.offset, node);
    if(node_move)
        map_new_sliding = true;
    InitNodePtrs(node, 1, node_ptrs);
    node[0] = FLAG_NODE_NONE;
    node[1] = 1;
    InitEdgePtrs(node_ptrs, 0, new_edge_ptrs[0]);
    InitEdgePtrs(node_ptrs, 1, new_edge_ptrs[1]);

    uint8_t new_key_first;
    UpdateTailEdge(edge_ptrs, match_len, data, new_edge_ptrs[0], new_key_first,
                   map_new_sliding);

    int release_buffer_size = 0;
    size_t edge_str_off;
    UpdateHeadEdge(edge_ptrs, match_len, data, release_buffer_size, edge_str_off,
                   map_new_sliding);
    Write6BInteger(edge_ptrs.offset_ptr, node_ptrs.offset);

    // Update the new node
    // match not found for the new node, should not set node[1] and data offset
    node_ptrs.edge_key_ptr[0] = new_key_first;
    node_ptrs.edge_key_ptr[1] = key[0];

    // Update the new edge
    new_edge_ptrs[1].len_ptr[0] = key_len;
    if(key_len > LOCAL_EDGE_LEN)
    {
        size_t new_key_off;
        ReserveData(key+1, key_len-1, new_key_off, map_new_sliding);
        Write5BInteger(new_edge_ptrs[1].ptr, new_key_off);
    }
    else
    {
        // edge key is local
        if(key_len > 1)
            memcpy(new_edge_ptrs[1].ptr, key+1, key_len-1);
    }
    // Indicate this new edge holds a data offset
    new_edge_ptrs[1].flag_ptr[0] = EDGE_FLAG_DATA_OFF;
    Write6BInteger(new_edge_ptrs[1].offset_ptr, data_off);

    if(node_move)
        WriteData(node, node_size[1], node_ptrs.offset);

    // Update the parent edge
    if(release_buffer_size > 0)
        ReleaseBuffer(edge_str_off, release_buffer_size);
#ifdef __LOCK_FREE__
    header->excep_lf_offset = edge_ptrs.offset;
    header->excep_updating_status = EXCEP_STATUS_ADD_EDGE;
    lfree->WriterLockFreeStart(edge_ptrs.offset);
#endif
    WriteEdge(edge_ptrs);
#ifdef __LOCK_FREE__
    lfree->WriterLockFreeStop();
    header->excep_updating_status = EXCEP_STATUS_NONE;
#endif

    header->n_edges += 2;
    return MBError::SUCCESS;
}

// Add a new edge in current node
// This invloves creating a new node and copying data from old node to the new node
// and updating the child node offset in edge_ptrs (parent edge).
int DictMem::UpdateNode(EdgePtrs &edge_ptrs, const uint8_t *key, int key_len,
                        size_t data_off)
{
    int nt = edge_ptrs.curr_nt + 1;
    bool node_move;
    NodePtrs node_ptrs;
    uint8_t* node;
    bool map_new_sliding = false;

    node_move = ReserveNode(nt, node_ptrs.offset, node);
    if(node_move)
        map_new_sliding = true;
    InitNodePtrs(node, nt, node_ptrs);

    // Load the old node
    size_t old_node_off = Get6BInteger(edge_ptrs.offset_ptr);
    int release_node_index = -1;
    if(nt == 0)
    {
        // Change from empty node to node with one edge
        // The old empty node stored the data offset instead of child node off.
        if(edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF)
        {
            Write6BInteger(node_ptrs.ptr+2, old_node_off);
            edge_ptrs.flag_ptr[0] &= ~EDGE_FLAG_DATA_OFF;
            node[0] = FLAG_NODE_MATCH | FLAG_NODE_NONE;
        }
    }
    else
    {
#ifdef __DEBUG__
        assert(nt > 0);
#endif

        // Copy old node
        int copy_size = NODE_EDGE_KEY_FIRST + nt;
        if(ReadData(node_ptrs.ptr, copy_size, old_node_off) != copy_size)
            return MBError::READ_ERROR;
        if(ReadData(node_ptrs.ptr+copy_size+1, EDGE_SIZE*nt, old_node_off+copy_size) !=
                    EDGE_SIZE*nt)
            return MBError::READ_ERROR;

        release_node_index = nt - 1;
    }

    node[1] = static_cast<uint8_t>(nt);

    // Update the first edge key character for the new edge
    node_ptrs.edge_key_ptr[nt] = key[0];

    Write6BInteger(edge_ptrs.offset_ptr, node_ptrs.offset);

    // Create the new edge
    EdgePtrs new_edge_ptrs;
    InitEdgePtrs(node_ptrs, nt, new_edge_ptrs);
    new_edge_ptrs.len_ptr[0] = key_len;
    if(key_len > LOCAL_EDGE_LEN)
    {
        size_t new_key_off;
        ReserveData(key+1, key_len-1, new_key_off, map_new_sliding);
        Write5BInteger(new_edge_ptrs.ptr, new_key_off);
    }
    else
    {
        // edge key is local
        if(key_len > 1)
            memcpy(new_edge_ptrs.ptr, key+1, key_len-1);
    }

    // Indicate this new edge holds a data offset
    new_edge_ptrs.flag_ptr[0] = EDGE_FLAG_DATA_OFF;
    Write6BInteger(new_edge_ptrs.offset_ptr, data_off);

    if(node_move)
        WriteData(node, node_size[nt], node_ptrs.offset);

    if(release_node_index >= 0)
        ReleaseNode(old_node_off, release_node_index);
#ifdef __LOCK_FREE__
    header->excep_lf_offset = edge_ptrs.offset;
    header->excep_updating_status = EXCEP_STATUS_ADD_EDGE;
    lfree->WriterLockFreeStart(edge_ptrs.offset);
#endif
    WriteEdge(edge_ptrs);
#ifdef __LOCK_FREE__
    lfree->WriterLockFreeStop();
    header->excep_updating_status = EXCEP_STATUS_NONE;
#endif

    header->n_edges++;
    return MBError::SUCCESS;
}

bool DictMem::FindNext(const unsigned char *key, int keylen, int &match_len,
                       EdgePtrs &edge_ptr, uint8_t *key_tmp) const
{
    if(edge_ptr.flag_ptr[0] & EDGE_FLAG_DATA_OFF)
    {
        edge_ptr.curr_nt = -1;
        return false;
    }

    size_t node_off = Get6BInteger(edge_ptr.offset_ptr);
#ifdef __DEBUG__
    assert(node_off != 0);
#endif
    if(ReadData(key_tmp, 1, node_off+1) != 1)
        return false;
    int nt = key_tmp[0];
    edge_ptr.curr_nt = nt;
    nt++;
    // Load edge key first
    node_off += NODE_EDGE_KEY_FIRST;
    if(ReadData(key_tmp, nt, node_off) != nt)
        return false;
    int i;
    for(i = 0; i < nt; i++)
    {
        if(key_tmp[i] == key[0])
            break;
    }

    if(i >= nt)
        return false;

    match_len = 1;

    // Load the new edge
    edge_ptr.offset = node_off + nt + i*EDGE_SIZE;
    if(ReadData(header->excep_buff, EDGE_SIZE, edge_ptr.offset) != EDGE_SIZE)
        return false;
    uint8_t *key_string_ptr;
    int len = edge_ptr.len_ptr[0] - 1;
    if(len > LOCAL_EDGE_LEN_M1)
    {
        if(ReadData(key_tmp, len, Get5BInteger(edge_ptr.ptr)) != len)
            return false;
        key_string_ptr = key_tmp;
    }
    else if(len > 0)
    {
        key_string_ptr = header->excep_buff;
    }
    else
    {
        return true;
    }

    for(i = 1; i < keylen; i++)
    {
        if(key_string_ptr[i-1] != key[i] || i > len)
            break;
        match_len++;
    }

    return true;
}

// Reserve buffer for a new node.
// The allocated in-memory buffer must be initialized to zero.
bool DictMem::ReserveNode(int nt, size_t &offset, uint8_t* &ptr)
{
#ifdef __DEBUG__
    assert(nt >= 0 && nt < 256);
#endif

    int buf_size = free_lists->GetAlignmentSize(node_size[nt]);
    int buf_index = free_lists->GetBufferIndex(buf_size);

    header->n_states++;
#ifdef __LOCK_FREE__
    if(free_lists->GetBufferByIndex(buf_index, offset))
    {
        ptr = node_ptr;
        memset(ptr, 0, buf_size); 
        header->pending_index_buff_size -= buf_size;
        return true;
    }
#else
    if(free_lists->GetBufferCountByIndex(buf_index) > 0)
    {
        offset = free_lists->RemoveBufferByIndex(buf_index);
        ptr = node_ptr;
        memset(ptr, 0, buf_size); 
        header->pending_index_buff_size -= buf_size;
        return true;
    }
#endif

    ptr = NULL;
    size_t old_off = header->m_index_offset;
    bool node_move = false;
    int rval = kv_file->Reserve(header->m_index_offset, buf_size, ptr);
    if(rval != MBError::SUCCESS)
        throw rval;
    if(ptr == NULL)
    {
        node_move = true;
        ptr = node_ptr;
    }

    //Checking missing buffer due to alignment
    if(old_off < header->m_index_offset)
    {
        free_lists->ReleaseAlignmentBuffer(old_off, header->m_index_offset);
        header->pending_index_buff_size += header->m_index_offset - old_off;
    }

    memset(ptr, 0, buf_size);
    offset = header->m_index_offset;
    header->m_index_offset += buf_size;
    return node_move;
}

// Reserve buffer for a new key
void DictMem::ReserveData(const uint8_t* key, int size, size_t &offset,
                          bool map_new_sliding)
{
    int buf_index = free_lists->GetBufferIndex(size);
    int buf_size  = free_lists->GetAlignmentSize(size);

#ifdef __LOCK_FREE__
    if(free_lists->GetBufferByIndex(buf_index, offset))
    {
        WriteData(key, size, offset);
        header->pending_index_buff_size -= buf_size;
    }
#else
    if(free_lists->GetBufferCountByIndex(buf_index) > 0)
    {
        offset = free_lists->RemoveBufferByIndex(buf_index);
        WriteData(key, size, offset);
        header->pending_index_buff_size -= buf_size;
    }
#endif
    else
    {
        size_t old_off = header->m_index_offset;
        uint8_t *ptr;

        int rval = kv_file->Reserve(header->m_index_offset, buf_size, ptr,
                                    map_new_sliding);
        if(rval != MBError::SUCCESS)
            throw rval;

        //Checking missing buffer due to alignment
        if(old_off < header->m_index_offset)
        {
            free_lists->ReleaseAlignmentBuffer(old_off, header->m_index_offset);
            header->pending_index_buff_size += header->m_index_offset - old_off;
        }

        offset = header->m_index_offset;
        header->m_index_offset += buf_size;
        if(ptr != NULL)
            memcpy(ptr, key, size);
        else
            WriteData(key, size, offset);
    }

    header->edge_str_size += buf_size;
}

// Release node buffer
void DictMem::ReleaseNode(size_t offset, int nt)
{
    if(nt < 0)
        return;

    int buf_index = free_lists->GetBufferIndex(node_size[nt]);
    int rval = free_lists->AddBufferByIndex(buf_index, offset);
    if(rval == MBError::SUCCESS)
        header->n_states--;
    else
        Logger::Log(LOG_LEVEL_ERROR, "failed to release node buffer");
    header->pending_index_buff_size += free_lists->GetAlignmentSize(node_size[nt]);
}

// Release edge string buffer
void DictMem::ReleaseBuffer(size_t offset, int size)
{
    int rval = free_lists->ReleaseBuffer(offset, size);
    if(rval != MBError::SUCCESS)
        Logger::Log(LOG_LEVEL_ERROR, "failed to release buffer");
    else
        header->edge_str_size -= free_lists->GetAlignmentSize(size);
    header->pending_index_buff_size += free_lists->GetAlignmentSize(size);
}

int DictMem::GetRootEdge(size_t rc_off, int nt, EdgePtrs &edge_ptrs) const
{
    if(rc_off != 0)
        edge_ptrs.offset = rc_off + NODE_EDGE_KEY_FIRST + NUM_ALPHABET + nt*EDGE_SIZE;
    else
        edge_ptrs.offset = root_offset + NODE_EDGE_KEY_FIRST + NUM_ALPHABET + nt*EDGE_SIZE;
    if(ReadData(edge_ptrs.edge_buff, EDGE_SIZE, edge_ptrs.offset) != EDGE_SIZE)
        return MBError::READ_ERROR;

    InitTempEdgePtrs(edge_ptrs);
    return MBError::SUCCESS;
}

// The temp edge is written to shared memory for handling segfault situations.
// When writer restarts from segfault, it will retry WriteEdge so that the DB is
// maintained consistently.
int DictMem::GetRootEdge_Writer(bool rc_mode, int nt, EdgePtrs &edge_ptrs) const
{
    if(rc_mode)
    {
        if(root_offset_rc == 0)
            throw (int) MBError::UNKNOWN_ERROR;
        edge_ptrs.offset = root_offset_rc + NODE_EDGE_KEY_FIRST + NUM_ALPHABET + nt*EDGE_SIZE;
    }
    else
    {
        edge_ptrs.offset = root_offset + NODE_EDGE_KEY_FIRST + NUM_ALPHABET + nt*EDGE_SIZE;
    }
    if(ReadData(header->excep_buff, EDGE_SIZE, edge_ptrs.offset) != EDGE_SIZE)
        return MBError::READ_ERROR;

    edge_ptrs.ptr = header->excep_buff;
    edge_ptrs.len_ptr = edge_ptrs.ptr + EDGE_LEN_POS;
    edge_ptrs.flag_ptr = edge_ptrs.ptr + EDGE_FLAG_POS;
    edge_ptrs.offset_ptr = edge_ptrs.flag_ptr + 1;
    return MBError::SUCCESS;
}

/////////////////////////////////////////////
// Init root node in resource collection mode
/////////////////////////////////////////////
size_t DictMem::InitRootNode_RC()
{
    bool node_move;
    uint8_t *root_node;

    node_move = ReserveNode(NUM_ALPHABET-1, root_offset_rc, root_node);
    root_node[0] = FLAG_NODE_NONE;
    root_node[1] = NUM_ALPHABET-1;
    for(int i = 0; i < NUM_ALPHABET; i++)
    {
        root_node[NODE_EDGE_KEY_FIRST+i] = static_cast<uint8_t>(i);
    }

    if(node_move)
        WriteData(root_node, node_size[NUM_ALPHABET-1], root_offset_rc);

    return root_offset_rc;
}

// No need to call LokcFree for removing all DB entries.
// Note readers may get READ_ERROR as return, which should be expected
// considering the full DB is deleted.
int DictMem::ClearRootEdge(int nt) const
{
    size_t offset = root_offset + NODE_EDGE_KEY_FIRST + NUM_ALPHABET + nt*EDGE_SIZE;
#ifdef __LOCK_FREE__
    header->excep_lf_offset = offset;
    header->excep_updating_status = EXCEP_STATUS_CLEAR_EDGE;
    lfree->WriterLockFreeStart(offset);
#endif
    WriteData(DictMem::empty_edge, EDGE_SIZE, offset);
#ifdef __LOCK_FREE__
    lfree->WriterLockFreeStop();
    header->excep_updating_status = EXCEP_STATUS_NONE;
#endif

    return MBError::SUCCESS;
}

int DictMem::ClearRootEdges_RC() const
{
    if(root_offset_rc == 0)
        return MBError::INVALID_ARG;

    size_t offset;
    for(int i = 0; i < NUM_ALPHABET; i++)
    {
        offset = root_offset_rc + NODE_EDGE_KEY_FIRST + NUM_ALPHABET + i*EDGE_SIZE;
#ifdef __LOCK_FREE__
        header->excep_lf_offset = offset;
        header->excep_updating_status = EXCEP_STATUS_CLEAR_EDGE;
        lfree->WriterLockFreeStart(offset);
#endif
        DRMBase::WriteData(DictMem::empty_edge, EDGE_SIZE, offset);
#ifdef __LOCK_FREE__
        lfree->WriterLockFreeStop();
        header->excep_updating_status = EXCEP_STATUS_NONE;
#endif
    }

    return MBError::SUCCESS;
}

void DictMem::ClearMem() const
{
    int root_node_size = free_lists->GetAlignmentSize(node_size[NUM_ALPHABET-1]);
    header->m_index_offset = root_offset + root_node_size;
    header->n_states = 1; // Keep the root node
    header->n_edges = 0;
    header->edge_str_size = 0;
    free_lists->Empty();
    header->pending_index_buff_size = 0;
}

int DictMem::NextEdge(const uint8_t *key, EdgePtrs &edge_ptrs, uint8_t *node_buff,
                      MBData &mbdata) const
{
    size_t node_off;
    // Check if need to read saved edge
    if((mbdata.options & CONSTS::OPTION_READ_SAVED_EDGE) && edge_ptrs.offset == mbdata.edge_ptrs.offset)
        node_off = Get6BInteger(mbdata.edge_ptrs.offset_ptr);
    else
        node_off = Get6BInteger(edge_ptrs.offset_ptr);

    int byte_read;
    byte_read = ReadData(node_buff, NODE_EDGE_KEY_FIRST, node_off);
    if(byte_read != NODE_EDGE_KEY_FIRST)
        return MBError::READ_ERROR;

    int nt = node_buff[1] + 1;
    byte_read = ReadData(node_buff+NODE_EDGE_KEY_FIRST, nt, node_off+NODE_EDGE_KEY_FIRST);
    if(byte_read != nt)
        return MBError::READ_ERROR;

    int ret = MBError::NOT_EXIST;
    for(int i = 0; i < nt; i++)
    {
        if(node_buff[i+NODE_EDGE_KEY_FIRST] == key[0])
        {
            if(mbdata.options & CONSTS::OPTION_FIND_AND_STORE_PARENT)
            {
                // update parent node/edge info for deletion
                edge_ptrs.curr_nt = nt;
                edge_ptrs.curr_edge_index = i;
                edge_ptrs.parent_offset = edge_ptrs.offset;
                edge_ptrs.curr_node_offset = node_off;
            }
            size_t offset_new = node_off + NODE_EDGE_KEY_FIRST + nt + i*EDGE_SIZE;
            byte_read = ReadData(edge_ptrs.edge_buff, EDGE_SIZE, offset_new);
            if(byte_read != EDGE_SIZE)
            {
                ret = MBError::READ_ERROR;
                break;
            }

            edge_ptrs.offset = offset_new;
            ret = MBError::SUCCESS;
            break;
        }
    }

    return ret;
}

void DictMem::RemoveRootEdge(const EdgePtrs &edge_ptrs)
{
    // Clear the edge
    // Root node needs special handling.
    if(edge_ptrs.len_ptr[0] > LOCAL_EDGE_LEN)
        ReleaseBuffer(Get5BInteger(edge_ptrs.ptr), edge_ptrs.len_ptr[0]-1);
#ifdef __LOCK_FREE__
    header->excep_lf_offset = edge_ptrs.offset;
    header->excep_updating_status = EXCEP_STATUS_CLEAR_EDGE;
    lfree->WriterLockFreeStart(edge_ptrs.offset);
#endif
    WriteData(DictMem::empty_edge, EDGE_SIZE, edge_ptrs.offset);
#ifdef __LOCK_FREE__
    lfree->WriterLockFreeStop();
    header->excep_updating_status = EXCEP_STATUS_NONE;
#endif
}

int DictMem::RemoveEdgeSizeN(const EdgePtrs &edge_ptrs,
                             int nt,
                             size_t node_offset,
                             uint8_t *old_node_buffer,
                             size_t &str_off_rel,
                             int &str_size_rel,
                             size_t parent_edge_offset)
{
    // Reserve a new node with nt-1
    bool node_move;
    size_t new_node_offset;
    uint8_t *node;

    // Reserve for the new node
    node_move = ReserveNode(nt-2, new_node_offset, node);

    // Copy data from old node
    uint8_t *first_key_ptr = node + NODE_EDGE_KEY_FIRST;
    uint8_t *edge_ptr = first_key_ptr + nt - 1;
    uint8_t old_edge_buff[16];
    size_t old_edge_offset = node_offset + NODE_EDGE_KEY_FIRST + nt;
    memcpy(node, old_node_buffer, NODE_EDGE_KEY_FIRST);
    node[1] = nt - 2;
    for(int i = 0; i < nt; i++)
    {
        // load the edge
        if(ReadData(old_edge_buff, EDGE_SIZE, old_edge_offset) != EDGE_SIZE)
            return MBError::READ_ERROR;

        if(i == edge_ptrs.curr_edge_index)
        {
            // Need to release this edge string buffer
            if(old_edge_buff[EDGE_LEN_POS] > LOCAL_EDGE_LEN)
            {
                str_off_rel = Get5BInteger(old_edge_buff);
                str_size_rel = old_edge_buff[EDGE_LEN_POS]-1;
            }
        }
        else
        {
            first_key_ptr[0] = old_node_buffer[NODE_EDGE_KEY_FIRST+i];
            memcpy(edge_ptr, old_edge_buff, EDGE_SIZE);

            first_key_ptr++;
            edge_ptr += EDGE_SIZE;
        }
        old_edge_offset += EDGE_SIZE;
    }

    // Write the new node before free
    if(node_move)
        WriteData(node, node_size[nt-2], new_node_offset);

    // Update the link from parent edge to the new node offset
    Write6BInteger(header->excep_buff, new_node_offset);
#ifdef __LOCK_FREE__
    lfree->WriterLockFreeStart(parent_edge_offset);
#endif
    WriteData(header->excep_buff, OFFSET_SIZE, parent_edge_offset+EDGE_NODE_LEADING_POS);
#ifdef __LOCK_FREE__
    lfree->WriterLockFreeStop();
#endif

    return MBError::SUCCESS;
}

int DictMem::RemoveEdgeSizeOne(uint8_t *old_node_buffer,
                               size_t parent_edge_offset,
                               size_t node_offset,
                               int nt,
                               size_t &str_off_rel,
                               int &str_size_rel)
{
    int rval = MBError::SUCCESS;

    if(old_node_buffer[0] & FLAG_NODE_MATCH)
    {
        uint8_t *parent_edge_buff = header->excep_buff;
        size_t data_offset = Get6BInteger(old_node_buffer+2);
        parent_edge_buff[0] = EDGE_FLAG_DATA_OFF;
        Write6BInteger(parent_edge_buff+1, data_offset);
#ifdef __LOCK_FREE__
        lfree->WriterLockFreeStart(parent_edge_offset);
#endif
        // Write the one-byte flag and 6-byte offset
        WriteData(parent_edge_buff, OFFSET_SIZE_P1, parent_edge_offset+EDGE_FLAG_POS);
#ifdef __LOCK_FREE__
        lfree->WriterLockFreeStop();
#endif
    }
    else
    {
        // This is an internal node.
        rval = MBError::TRY_AGAIN;
    }

    uint8_t old_edge_buff[16];
    size_t old_edge_offset = node_offset + NODE_EDGE_KEY_FIRST + nt;
    if(ReadData(old_edge_buff, EDGE_SIZE, old_edge_offset) != EDGE_SIZE)
        return MBError::READ_ERROR;
    if(old_edge_buff[EDGE_LEN_POS] > LOCAL_EDGE_LEN)
    {
        str_off_rel = Get5BInteger(old_edge_buff);
        str_size_rel = old_edge_buff[EDGE_LEN_POS]-1;
    }

    return rval;
}

int DictMem::RemoveEdgeByIndex(const EdgePtrs &edge_ptrs, MBData &data)
{
    header->excep_offset = edge_ptrs.curr_node_offset;

    if(header->excep_offset == root_offset)
    {
        RemoveRootEdge(edge_ptrs);
        return MBError::SUCCESS;
    }

    int nt = edge_ptrs.curr_nt;
    if(nt < 1) return MBError::INVALID_ARG;

    uint8_t *old_node_buffer = data.node_buff;
    // load the current node
    if(ReadData(old_node_buffer, NODE_EDGE_KEY_FIRST + nt, edge_ptrs.curr_node_offset)
               != NODE_EDGE_KEY_FIRST + nt)
        return MBError::READ_ERROR;

    int rval = MBError::SUCCESS;
    size_t str_off_rel;
    int    str_size_rel = 0;

    header->excep_lf_offset = edge_ptrs.parent_offset;
    header->excep_updating_status = EXCEP_STATUS_REMOVE_EDGE;
    if(nt > 1)
    {
        rval = RemoveEdgeSizeN(edge_ptrs, nt, header->excep_offset, old_node_buffer,
                               str_off_rel, str_size_rel, header->excep_lf_offset);
    }
    else
    {
        rval = DictMem::RemoveEdgeSizeOne(old_node_buffer, header->excep_lf_offset,
                               header->excep_offset, nt, str_off_rel, str_size_rel);
    }
    header->excep_updating_status = EXCEP_STATUS_NONE;

    header->n_edges--;
    ReleaseNode(header->excep_offset, nt-1);
    if(str_size_rel > 0)
        ReleaseBuffer(str_off_rel, str_size_rel);

    // Clear the edge
#ifdef __LOCK_FREE__
    header->excep_lf_offset = edge_ptrs.offset;
    header->excep_updating_status = EXCEP_STATUS_CLEAR_EDGE;
    lfree->WriterLockFreeStart(edge_ptrs.offset);
#endif
    WriteData(DictMem::empty_edge, EDGE_SIZE, edge_ptrs.offset);
#ifdef __LOCK_FREE__
    lfree->WriterLockFreeStop();
    header->excep_updating_status = EXCEP_STATUS_NONE;
#endif

    return rval;
}

// Should only be called by writer
void DictMem::PrintStats(std::ostream &out_stream) const
{
    if(!is_valid)
        return;

    out_stream << "Dict Memory Stats:" << std::endl;
    out_stream << "\tIndex size: " << header->m_index_offset << std::endl;
    out_stream << "\tIndex block size: " << header->index_block_size << std::endl;
    out_stream << "\tNumber of edges: " << header->n_edges << std::endl;
    out_stream << "\tNumber of nodes: " << header->n_states << std::endl;
    out_stream << "\tEdge string size: " << header->edge_str_size << std::endl;
    out_stream << "\tEdge size: " << header->n_edges*EDGE_SIZE << std::endl;
    out_stream << "\tException flag: " << header->excep_updating_status << std::endl;
    out_stream << "\tPending Buffer Size: " << header->pending_index_buff_size << std::endl;
    if(free_lists != NULL)
        out_stream << "\tTrackable Buffer Size: " << free_lists->GetTotSize() << std::endl;
    kv_file->PrintStats(out_stream);
}

const int* DictMem::GetNodeSizePtr() const
{
    return node_size;
}

void DictMem::ResetSlidingWindow() const
{
    kv_file->ResetSlidingWindow();
    if(header != NULL)
        header->shm_index_sliding_start.store(0, std::memory_order_relaxed);
}

void DictMem::InitLockFreePtr(LockFree *lf)
{
    lfree = lf;
}

void DictMem::Flush() const
{
    if(kv_file != NULL)
        kv_file->Flush();
    if(header_file != NULL)
        header_file->Flush();
}

void DictMem::WriteData(const uint8_t *buff, unsigned len, size_t offset) const
{
    if(offset + len > header->m_index_offset)
    {
        std::cerr << "invalid dmm write: " << offset << " " << len << " "
                  << header->m_index_offset << "\n";
        throw (int) MBError::OUT_OF_BOUND;
    }

    if(kv_file->RandomWrite(buff, len, offset) != len)
        throw (int) MBError::WRITE_ERROR;
}

}
