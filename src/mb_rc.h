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

#ifndef __MB_RC_H__
#define __MB_RC_H__

#include "db.h"
#include "dict.h"
#include "mbt_base.h"
#include "async_writer.h"

#define RESOURCE_COLLECTION_TYPE_INDEX            0x01
#define RESOURCE_COLLECTION_TYPE_DATA             0x02
#define RESOURCE_COLLECTION_PHASE_REORDER         1
#define RESOURCE_COLLECTION_PHASE_COLLECT         2

namespace mabain {

// A garbage collector class
class ResourceCollection : public DBTraverseBase
{
public:
    ResourceCollection(const DB &db,
        int rct = RESOURCE_COLLECTION_TYPE_INDEX |
                  RESOURCE_COLLECTION_TYPE_DATA);
    virtual ~ResourceCollection();

    void ReclaimResource(int64_t min_index_size, int64_t min_data_size,
                         int64_t max_dbsz, int64_t max_dbcnt,
                         AsyncWriter *awr = NULL);

private:
    void DoTask(int phase, DBTraverseNode &dbt_node);
    void Prepare(int64_t min_index_size, int64_t min_data_size);
    void CollectBuffers();
    void ReorderBuffers();
    void Finish();
    bool MoveIndexBuffer(int phase, size_t &offset_src, int size);
    bool MoveDataBuffer(int phase, size_t &offset_src, int size);
    int  LRUEviction();
    void ProcessRCTree();

    int      rc_type;
    int      index_rc_status;
    int      data_rc_status;
    int      index_reorder_status;
    int      data_reorder_status;

    // data for print gc stats only
    int64_t  index_reorder_cnt;
    int64_t  data_reorder_cnt;

    // Async writer pointer
    AsyncWriter *async_writer_ptr;

    size_t min_index_off_rc;
    size_t min_data_off_rc;

    int64_t rc_loop_counter;
};

}

#endif
