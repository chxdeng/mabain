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

#ifndef __MBDATA_H__
#define __MBDATA_H__

#include <stdint.h>
#include <stdlib.h>

#include "mabain_consts.h"

#define NUM_ALPHABET               256
#define NODE_EDGE_KEY_FIRST        8

namespace mabain {

typedef struct _EdgePtrs
{
    size_t offset;
    uint8_t *ptr;

    uint8_t *len_ptr;
    uint8_t *flag_ptr;
    uint8_t *offset_ptr;

    // temp buffer for edge, must be bigger than EDGE_SIZE.
    uint8_t edge_buff[16];
    // temp usage for calling UpdateNode or entry removing
    int curr_nt;
    // temp usage for entry removing and shrinking
    size_t curr_node_offset;
    // temp usage for entry removing
    int curr_edge_index;
    // temp usage for entry removing and shrinking
    size_t parent_offset;
} EdgePtrs;

// Data class for find and remove
// All memeber variable in this class should be kept public so that it can
// be easily accessed by the caller to get the data/value buffer and buffer len.
class MBData
{
public:
    MBData();
    MBData(int size, int options);
    ~MBData();
    // Clear the data for repeated usage
    void Clear();
    int  Resize(int size);
    void SetValue(char *buff, int size);

    // data length
    int data_len;
    // data buffer
    uint8_t *buff;
    // buffer length
    int buff_len;

    // data offset
    size_t data_offset;

    // Search options
    int options;

    // temp data for multiple common prefix search
    // If true, indicate the search should be continued for common prefix search
    bool next;
    // match length so far; only populated when match is found.
    int match_len;
    struct _EdgePtrs edge_ptrs;
    // temp buffer to hold the node
    uint8_t node_buff[NUM_ALPHABET+NODE_EDGE_KEY_FIRST];

private:
    bool free_buffer;
};

}

#endif
