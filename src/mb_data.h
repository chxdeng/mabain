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

#ifndef __MBDATA_H__
#define __MBDATA_H__

#include <stdint.h>
#include <stdlib.h>
#include <vector>

#include "mabain_consts.h"

#define NUM_ALPHABET 256
#define NODE_EDGE_KEY_FIRST 8
#define DB_ITER_STATE_INIT 0x00
#define DB_ITER_STATE_MORE 0x01
#define DB_ITER_STATE_DONE 0x02
#define DATA_BLOCK_SIZE_DEFAULT 16LLU * 1024 * 1024 // 16M
#define INDEX_BLOCK_SIZE_DEFAULT 16LLU * 1024 * 1024 // 16M
#define BLOCK_SIZE_ALIGN 4 * 1024 * 1024 // 4K
#define BUFFER_TYPE_NONE 0
#define BUFFER_TYPE_EDGE_STR 0x01
#define BUFFER_TYPE_NODE 0x02
#define BUFFER_TYPE_DATA 0x04
#define MATCH_NONE 0
#define MATCH_EDGE 1
#define MATCH_NODE 2
#define MATCH_NODE_OR_EDGE 3

namespace mabain {

typedef struct _EdgePtrs {
    size_t offset;
    uint8_t* ptr;

    uint8_t* len_ptr;
    uint8_t* flag_ptr;
    uint8_t* offset_ptr;

    // temp buffer for edge, must be bigger than EDGE_SIZE.
    uint8_t edge_buff[16];
    // temp usage for calling UpdateNode or entry removing
    int curr_nt;
    // temp usage for entry removing
    size_t curr_node_offset;
    // temp usage for entry removing
    int curr_edge_index;
    // temp usage for entry removing and rc
    size_t parent_offset;
} EdgePtrs;

// Data class for find and remove
// All memeber variable in this class should be kept public so that it can
// be easily accessed by the caller to get the data/value buffer and buffer len.
class MBData {
public:
    MBData();
    MBData(int size, int options);
    ~MBData();
    // Clear the data for repeated usage
    void Clear();
    int Resize(int size);
    int TransferValueTo(uint8_t*& data, int& dlen);
    int TransferValueFrom(uint8_t*& data, int dlen);

    // data length
    int data_len;
    // data buffer
    uint8_t* buff;
    // buffer length
    int buff_len;

    // data offset
    size_t data_offset;
    uint16_t bucket_index;

    // Search options
    int options;

    // match length so far; only populated when match is found.
    int match_len;
    struct _EdgePtrs edge_ptrs;
    // temp buffer to hold the node
    uint8_t node_buff[NUM_ALPHABET + NODE_EDGE_KEY_FIRST];

private:
    bool free_buffer;
};

}

#endif
