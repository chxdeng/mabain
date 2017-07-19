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

#ifndef __MB_SHRINK_H__
#define __MB_SHRINK_H__

#include <vector>

#include "db.h"
#include "dict.h"
#include "mabain_consts.h"

namespace mabain {

typedef struct _IndexNode
{
    size_t parent_off; // original offset for indexing
    // node offset will be updated after shrinking for index link.
    // for data link, it is used as the edge offset for lock-free.
    size_t node_edge_off;
    short  rel_parent_off; // relative offset (not changing after moving node)
    short  buffer_size;
    char   buffer_type;
} IndexNode;

// Shrink the disk space
class MBShrink
{
public:
    MBShrink(DB &db);
    ~MBShrink();

    int Shrink(int64_t min_index_shk_size = INDEX_BLOCK_SIZE/2,
               int64_t min_data_shk_size  = DATA_BLOCK_SIZE/2);

#ifndef __UNIT_TEST__
private:
#endif
    int    MoveIndexBuffer(size_t src, const std::string &key, int size,
                           size_t parent, int rel_off, int buff_type);
    int    MoveDataBuffer(size_t src, int size, size_t parent, size_t edge_off);
    void   UpdateOffset(const std::string &key, size_t offset_curr);
    size_t GetCurrOffset(size_t offset_orig);
    int    OpenLinkDB();
    int    BuildIndexLink();
    int    ScanDictMem();
    int    ShrinkIndex();
    int    BuildDataLink();
    int    ScanData();
    int    ShrinkData();

private:
    void   ReallocBuffer(int size);

    DB &db_ref;

    // DiskFree does not own any of these pointers.
    Dict *dict;
    DictMem *dmm;
    FreeList *index_free_lists;
    FreeList *data_free_lists;
    const int *node_size;
    IndexHeader *header;

    // pointer owned by DiskFree
    DB *db_link;
    uint8_t *buffer;
    int buff_size;
    size_t index_start_off;
    long long index_shrink_size;
    size_t min_index_scan_off;
    size_t max_index_scan_off;
    size_t data_start_off;
    long long data_shrink_size;
    size_t min_data_scan_off;
    size_t max_data_scan_off;
};

}

#endif
