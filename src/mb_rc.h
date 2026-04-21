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

#include "async_writer.h"
#include "db.h"
#include "dict.h"
#include "mbt_base.h"

#define RESOURCE_COLLECTION_TYPE_INDEX 0x01
#define RESOURCE_COLLECTION_TYPE_DATA 0x02

#define RESOURCE_COLLECTION_PHASE_REORDER 0x01
#define RESOURCE_COLLECTION_PHASE_COLLECT 0x02
#define RESOURCE_COLLECTION_PHASE_EVACUATE_INDEX 0x04
#define RESOURCE_COLLECTION_PHASE_EVACUATE_DATA 0x08

namespace mabain {

class ResourceCollectionTestPeer;

typedef struct _StartupRebuildRuntimeState {
    int rebuild_state;
    size_t rebuild_index_alloc_end;
    size_t rebuild_data_alloc_end;
    size_t rebuild_index_source_end;
    size_t rebuild_data_source_end;
    size_t rebuild_index_block_cursor;
    size_t rebuild_data_block_cursor;
    uint32_t reusable_index_block_count;
    uint32_t reusable_data_block_count;
    ReusableBlockEntry reusable_index_block[MB_MAX_REUSABLE_BLOCKS];
    ReusableBlockEntry reusable_data_block[MB_MAX_REUSABLE_BLOCKS];

    void Reset(int state)
    {
        rebuild_state = state;
        rebuild_index_alloc_end = 0;
        rebuild_data_alloc_end = 0;
        rebuild_index_source_end = 0;
        rebuild_data_source_end = 0;
        rebuild_index_block_cursor = 0;
        rebuild_data_block_cursor = 0;
        reusable_index_block_count = 0;
        reusable_data_block_count = 0;
        for (int i = 0; i < MB_MAX_REUSABLE_BLOCKS; i++) {
            reusable_index_block[i].Clear();
            reusable_data_block[i].Clear();
        }
    }

    void Clear()
    {
        Reset(REBUILD_STATE_NORMAL);
    }
} StartupRebuildRuntimeState;

// A garbage collector class
class ResourceCollection : public DBTraverseBase {
public:
    ResourceCollection(const DB& db,
        int rct = RESOURCE_COLLECTION_TYPE_INDEX | RESOURCE_COLLECTION_TYPE_DATA);
    virtual ~ResourceCollection();

    void ReclaimResource(int64_t min_index_size, int64_t min_data_size,
        int64_t max_dbsz, int64_t max_dbcnt,
        AsyncWriter* awr = NULL);

    // Startup-only dense shrink preparation for jemalloc keep-db mode.
    // Returns MBError and must leave the current root authoritative on failure.
    int StartupShrink();

    // Startup-only allocator handoff after dense shrink.
    // Returns MBError and keeps the live root unchanged.
    int StartupEvacuate();
    void ResetStartupRebuildState(int state);
    bool StartupRebuildComplete() const;
    void GetStartupRebuildProgress(size_t& index_cursor, size_t& data_cursor,
        uint32_t& index_reusable, uint32_t& data_reusable) const;
    const StartupRebuildRuntimeState& GetStartupRebuildState() const;

    // This function should be called when writer starts up.
    int ExceptionRecovery();

    friend class ResourceCollectionTestPeer;

private:
    void DoTask(int phase, DBTraverseNode& dbt_node);
    void Prepare(int64_t min_index_size, int64_t min_data_size);
    void CollectBuffers();
    void ReorderBuffers();
    void Finish();
    bool MoveDataBufferEvacuate(size_t& offset_src, int size);
    bool MoveIndexBufferEvacuate(size_t& offset_src, int size);
    bool MoveIndexBuffer(int phase, size_t& offset_src, int size);
    bool HasPendingReusableBlocks(const ReusableBlockEntry* entries, uint32_t entry_count) const;
    bool IsReaderEpochQuiesced(uint64_t retire_epoch) const;
    int ReleaseReusableBlocks(ReusableBlockEntry* entries, uint32_t& entry_count);
    int DrainReusableBlocks(ReusableBlockEntry* entries, uint32_t& entry_count, DRMBase* drm);
    int QueueReusableBlock(ReusableBlockEntry* entries, uint32_t& entry_count,
        size_t block_order, uint64_t retire_epoch);
    int EvacuateOneIndexBlock();
    int EvacuateOneDataBlock();
    bool MoveDataBuffer(int phase, size_t& offset_src, int size);
    int LRUEviction(int64_t max_dbsz, int64_t max_dbcnt);
    void ProcessRCTree();

    int rc_type;
    int index_rc_status;
    int data_rc_status;
    int index_reorder_status;
    int data_reorder_status;

    // data for print gc stats only
    int64_t index_reorder_cnt;
    int64_t data_reorder_cnt;

    // Async writer pointer
    AsyncWriter* async_writer_ptr;

    // resource collection offsets
    size_t rc_index_offset;
    size_t rc_data_offset;
    int64_t rc_loop_counter;

    int64_t db_cnt;
    size_t edge_str_size;
    int64_t node_cnt;

    // Current full source block being evacuated in Step 6.
    size_t evacuate_index_block_start;
    size_t evacuate_index_block_end;
    size_t evacuate_data_block_start;
    size_t evacuate_data_block_end;
    StartupRebuildRuntimeState startup_rebuild_;
};

}

#endif
