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

#ifndef __MABAIN_H__
#define __MABAIN_H__

namespace mabain {

#define ACCESS_MODE_READER         0x00
#define ACCESS_MODE_WRITER         0x01
#define DB_ITER_STATE_INIT         0x00
#define DB_ITER_STATE_MORE         0x01
#define DB_ITER_STATE_DONE         0x02

#define OPTION_ALL_PREFIX          0x01
#define OPTION_FIND_AND_DELETE     0x02
#define MAX_KEY_LENGHTH            256

#define NUM_ALPHABET               256
#define DATA_BLOCK_SIZE            128LLU*1024*1024
#define MAX_DATA_SIZE              0x7FFF
#define DATA_SIZE_BYTE             2
#define HEADER_PADDING_SIZE        32

#define OFFSET_SIZE                6
#define OFFSET_SIZE_P1             7

#define MIN_INDEX_MEM_SIZE         32*1024
#define INDEX_BLOCK_SIZE           128LLU*1024*1024
#define LOCAL_EDGE_LEN             6
#define LOCAL_EDGE_LEN_M1          5

#define EDGE_SIZE                  13
#define EDGE_LEN_POS               5
#define EDGE_FLAG_POS              6
#define EDGE_NODE_LEADING_POS      7
#define EDGE_FLAG_DATA_OFF         0x01

#define FLAG_NODE_MATCH            0x01
#define FLAG_NODE_NONE             0x0
#define NODE_EDGE_KEY_FIRST        8

#define MAX_BUFFER_RESERVE_SIZE         8192
#define BUFFER_ALIGNMENT                4
#define NUM_BUFFER_RESERVE              MAX_BUFFER_RESERVE_SIZE/BUFFER_ALIGNMENT

#define MAX_DATA_BUFFER_RESERVE_SIZE    MAX_DATA_SIZE
#define DATA_BUFFER_ALIGNMENT           8
#define NUM_DATA_BUFFER_RESERVE         MAX_DATA_BUFFER_RESERVE_SIZE/DATA_BUFFER_ALIGNMENT

#define BUFFER_TYPE_NONE                0
#define BUFFER_TYPE_EDGE_STR            1
#define BUFFER_TYPE_NODE                2

#define MATCH_NONE                      0
#define MATCH_EDGE                      1
#define MATCH_NODE                      2

}

#endif
