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

#ifndef __DBTraverseBase_H__
#define __DBTraverseBase_H__

#include "db.h"
#include "dict.h"

namespace mabain {

typedef struct _DBTraverseNode
{
    size_t edge_offset;
    size_t node_offset;
    size_t node_link_offset;
    int    node_size;
    size_t edgestr_offset;
    size_t edgestr_link_offset;
    int    edgestr_size;
    size_t data_offset;
    size_t data_link_offset;
    int    data_size;
    int    buffer_type;
} DBTraverseNode;

// An abstract base class for writer to traverse mabain DB 
class DBTraverseBase
{
public:
    DBTraverseBase(const DB &db);
    ~DBTraverseBase();

    // Traverse DB via DFS
    void TraverseDB(int arg = 0);

protected:
    virtual void DoTask(int arg, DBTraverseNode &dbt_node) = 0;
    void BufferCopy(size_t offset_dst, uint8_t *ptr_dst,
                    size_t offset_src, const uint8_t *ptr_src,
                    int size, DRMBase *drm);

    // DBTraverseBase does not own these objects or pointers.
    const DB &db_ref;
    Dict *dict;
    DictMem *dmm;
    IndexHeader *header;
    FreeList *index_free_lists;
    FreeList *data_free_lists;
    // ResourceCollection does not own lfree pointer.
    LockFree *lfree;

    // Used for tracking index and data sizes that have been traversed.
    size_t   index_size;
    size_t   data_size;

private:
    void GetAlignmentSize(DBTraverseNode &dbt_node) const;
    void ResizeRWBuffer(int size);

    uint8_t *rw_buffer; 
    int      rw_buffer_size;
};

}

#endif
