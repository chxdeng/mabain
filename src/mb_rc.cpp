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

#include <sys/time.h>

#include "mb_rc.h"
#include "dict.h"
#include "dict_mem.h"
#include "integer_4b_5b.h"

#define MAX_PRUNE_COUNT      3                 // maximum lru eviction attempts
#define NUM_ASYNC_TASK       10                // number of other tasks to be checked during eviction
#define PRUNE_TASK_CHECK     10               // every Xth eviction check Y number of other tasks
#define RC_TASK_CHECK        10               // every Xth async task try to reclaim resources
#define MIN_RC_OFFSET_GAP    1ULL*1024*1024    // 1M


namespace mabain {

ResourceCollection::ResourceCollection(const DB &db, int rct)
                   : DBTraverseBase(db), rc_type(rct)
{
    async_writer_ptr = NULL;
}

ResourceCollection::~ResourceCollection()
{
}

#define CIRCULAR_INDEX_DIFF(x, y) ((x)>(y) ? ((x)-(y)) : (0xFFFF-(y)+(x)))
#define CIRCULAR_PRUNE_DIFF(x, y) ((x)>=(y) ? ((x)-(y)) : (0xFFFF-(y)+(x)))
int ResourceCollection::LRUEviction(int64_t max_dbsz, int64_t max_dbcnt)
{
    int64_t pruned = 0;
    int64_t count = 0;
    int rval = MBError::SUCCESS;

    Logger::Log(LOG_LEVEL_INFO, "running LRU eviction for bucket %u", header->eviction_bucket_index);

    uint16_t index_diff = CIRCULAR_INDEX_DIFF(header->eviction_bucket_index,
                              (header->num_update/header->entry_per_bucket) % 0xFFFF);
    double ratio = 0.15;
    int64_t tot_size = (int64_t) (header->m_data_offset + header->m_index_offset);
    if (tot_size > max_dbsz)
        ratio = (tot_size - max_dbsz) * 0.88 / max_dbsz;
    else if (header->count > max_dbcnt)
        ratio = (header->count - max_dbcnt) * 0.88 / max_dbcnt;
    if (ratio > 0.5) ratio = 0.5;
    uint16_t prune_diff = uint16_t((0xFFFF - index_diff) * ratio);

    DB db_itr(db_ref);
    for(DB::iterator iter = db_itr.begin(false); iter != db_itr.end(); ++iter)
    {
        if(CIRCULAR_PRUNE_DIFF(iter.value.bucket_index, header->eviction_bucket_index) < prune_diff)
        {
            if (async_writer_ptr != NULL)
            {
                rval = dict->Remove((const uint8_t *)iter.key.data(), iter.key.size());
            }
            else
                rval = dict->Remove((const uint8_t *)iter.key.data(), iter.key.size());
            if(rval != MBError::SUCCESS)
                Logger::Log(LOG_LEVEL_DEBUG, "failed to run eviction %s", MBError::get_error_str(rval));
            else
                pruned++;
        }

        if(async_writer_ptr != NULL)
        {
            if(count++ > PRUNE_TASK_CHECK)
            {
                count = 0;
                rval = async_writer_ptr->ProcessTask(NUM_ASYNC_TASK, false);
                if(rval == MBError::RC_SKIPPED)
                    break;
            }
        }
    }

    if(rval != MBError::RC_SKIPPED)
    {
        // It is expected that eviction_bucket_index can overflow since we are only
        // interested in circular difference.
        header->eviction_bucket_index += prune_diff;
        // If not enough pruned, need to retry.
        if(pruned < int64_t(prune_diff * header->entry_per_bucket * 0.75))
            rval = MBError::TRY_AGAIN;
        Logger::Log(LOG_LEVEL_INFO, "LRU eviction done %d pruned, current bucket index %u",
                    pruned, header->eviction_bucket_index);
    }
    else
    {
        Logger::Log(LOG_LEVEL_INFO, "LRU eviction skipped %d pruned", pruned);
    }

    return rval;
}

void ResourceCollection::ReclaimResource(int64_t min_index_size,
                                         int64_t min_data_size,
                                         int64_t max_dbsz,
                                         int64_t max_dbcnt,
                                         AsyncWriter *awr)
{
    if(!db_ref.is_open())
        throw db_ref.Status();

    async_writer_ptr = awr;
    timeval start, stop;
    uint64_t timediff;

    // Check LRU eviction first
    if(header->m_data_offset + header->m_index_offset > (size_t) max_dbsz ||
            header->count > max_dbcnt)
    {
        int cnt = 0;
        gettimeofday(&start,NULL);
        while(cnt < MAX_PRUNE_COUNT) {
            if(LRUEviction(max_dbsz, max_dbcnt) != MBError::TRY_AGAIN)
                break;
            cnt++;
        }
        gettimeofday(&stop,NULL);
        timediff = (stop.tv_sec - start.tv_sec)*1000000 + (stop.tv_usec - start.tv_usec);
        if(timediff > 1000000)
        {
            Logger::Log(LOG_LEVEL_INFO, "LRU eviction finished in %lf seconds",
                    timediff/1000000.);
        }
        else
        {
            Logger::Log(LOG_LEVEL_INFO, "LRU eviction finished in %lf milliseconds",
                    timediff/1000.);
        }
    }

    if(min_index_size > 0 || min_data_size > 0)
    {
        Prepare(min_index_size, min_data_size);
        Logger::Log(LOG_LEVEL_INFO, "defragmentation started for [index - %s] [data - %s]",
                rc_type & RESOURCE_COLLECTION_TYPE_INDEX ? "yes":"no",
                rc_type & RESOURCE_COLLECTION_TYPE_DATA ? " yes":"no");
        gettimeofday(&start, NULL);

        ReorderBuffers();
        CollectBuffers();
        Finish();

        gettimeofday(&stop, NULL);
        async_writer_ptr = NULL;
        timediff = (stop.tv_sec - start.tv_sec)*1000000 + (stop.tv_usec - start.tv_usec);
        if(timediff > 1000000)
        {
            Logger::Log(LOG_LEVEL_INFO, "defragmentation finished in %lf seconds",
                    timediff/1000000.);
        }
        else
        {
            Logger::Log(LOG_LEVEL_INFO, "defragmentation finished in %lf milliseconds",
                    timediff/1000.);
        }
    }
}

/////////////////////////////////////////////////////////
////////////////// Private Methods //////////////////////
/////////////////////////////////////////////////////////

void ResourceCollection::Prepare(int64_t min_index_size, int64_t min_data_size)
{
    // make sure there is enough grabaged index buffers before initiating collection
    if(min_index_size == 0 || header->pending_index_buff_size < min_index_size)
    {
        rc_type &= ~RESOURCE_COLLECTION_TYPE_INDEX;
    }

    // make sure there is enough grabaged data buffers before initiating collection
    if(min_data_size == 0 || header->pending_data_buff_size < min_data_size)
    {
        rc_type &= ~RESOURCE_COLLECTION_TYPE_DATA;
    }

    // minimum defragmentation throshold is not reached, skip grabage collection
    if(rc_type == 0)
    {
        Logger::Log(LOG_LEVEL_DEBUG, "pending_index_buff_size (%llu) min_index_size (%llu)",
                header->pending_index_buff_size, min_index_size);

        Logger::Log(LOG_LEVEL_DEBUG, "pending_data_buff_size (%llu) min_data_size (%llu)",
                header->pending_data_buff_size, min_data_size);

        Logger::Log(LOG_LEVEL_DEBUG, "garbage collection skipped since pending"
                "sizes smaller than required");
        throw (int) MBError::RC_SKIPPED;
    }

    index_free_lists->Empty();
    data_free_lists->Empty();

    rc_loop_counter = 0;
    index_reorder_cnt = 0;
    data_reorder_cnt = 0;
    index_rc_status = MBError::NOT_INITIALIZED;
    data_rc_status = MBError::NOT_INITIALIZED;
    index_reorder_status = MBError::NOT_INITIALIZED;
    data_reorder_status = MBError::NOT_INITIALIZED;
    header->rc_m_index_off_pre = header->m_index_offset;
    header->rc_m_data_off_pre = header->m_data_offset;

    if(async_writer_ptr)
    {
        // set rc root to at the end of max size
        rc_index_offset = dmm->GetResourceCollectionOffset();
        rc_data_offset = dict->GetResourceCollectionOffset();

        // make sure there is some space left at the end of current index
        // [start|....|current_index_offset|...MIN_RC_OFFSET_GAP/more...|rc_index_offset|.....|end]
        if(rc_index_offset < header->m_index_offset + MIN_RC_OFFSET_GAP ||
                rc_data_offset < header->m_data_offset + MIN_RC_OFFSET_GAP)
        {
            Logger::Log(LOG_LEVEL_WARN, "not enough space for rc, index: "
                        "%llu %d, %llu, data: %llu %d, %llu",
                        header->m_index_offset, MIN_RC_OFFSET_GAP, rc_index_offset,
                        header->m_data_offset, MIN_RC_OFFSET_GAP, rc_data_offset);
            throw (int) MBError::OUT_OF_BOUND;
        }

        // update current indexs to rc offsets
        header->m_index_offset = rc_index_offset;
        header->m_data_offset  = rc_data_offset;

        // create rc root node
        size_t rc_off = dmm->InitRootNode_RC();
        header->rc_root_offset.store(rc_off, MEMORY_ORDER_WRITER);
    }

    Logger::Log(LOG_LEVEL_DEBUG, "setting rc index off start to: %llu", header->m_index_offset);
    Logger::Log(LOG_LEVEL_DEBUG, "setting rc data off start to: %llu", header->m_data_offset);
}

void ResourceCollection::CollectBuffers()
{
    if((rc_type & RESOURCE_COLLECTION_TYPE_INDEX) &&
       (index_reorder_status != MBError::SUCCESS))
        return;
    if((rc_type & RESOURCE_COLLECTION_TYPE_DATA) &&
       (data_reorder_status != MBError::SUCCESS))
        return;

    TraverseDB(RESOURCE_COLLECTION_PHASE_COLLECT);

    if(rc_type & RESOURCE_COLLECTION_TYPE_INDEX)
        index_rc_status = MBError::SUCCESS;
    if(rc_type & RESOURCE_COLLECTION_TYPE_DATA)
        data_rc_status = MBError::SUCCESS;
}

void ResourceCollection::Finish()
{
    if(index_rc_status == MBError::SUCCESS)
    {
        Logger::Log(LOG_LEVEL_INFO, "index buffer size reclaimed: %lld",
                    (header->rc_m_index_off_pre > index_size) ?
                    (header->rc_m_index_off_pre - index_size) : 0);
        header->m_index_offset = index_size;
        header->pending_index_buff_size = 0;
    }
    else
    {
        if(header->rc_m_index_off_pre == 0)
            throw (int) MBError::INVALID_ARG;
        header->m_index_offset = header->rc_m_index_off_pre;
    }
    if(data_rc_status == MBError::SUCCESS)
    {
        Logger::Log(LOG_LEVEL_INFO, "data buffer size reclaimed: %lld",
                    (header->rc_m_data_off_pre > data_size) ?
                    (header->rc_m_data_off_pre - data_size) : 0);
        header->m_data_offset = data_size;
        header->pending_data_buff_size = 0;
    }
    else
    {
        if(header->rc_m_data_off_pre == 0)
            throw (int) MBError::INVALID_ARG;
        header->m_data_offset = header->rc_m_data_off_pre;
    }

    if(async_writer_ptr != NULL)
    {
        index_free_lists->Empty();
        data_free_lists->Empty();
        ProcessRCTree();
    }

    header->rc_m_index_off_pre = 0;
    header->rc_m_data_off_pre = 0;

    dict->RemoveUnused(header->m_data_offset, true);
    dmm->RemoveUnused(header->m_index_offset, true);
}

bool ResourceCollection::MoveIndexBuffer(int phase, size_t &offset_src, int size)
{
    index_size = dmm->CheckAlignment(index_size, size);

    if(index_size == offset_src)
        return false;

    uint8_t *ptr_src;
    uint8_t *ptr_dst;
    size_t offset_dst;
    int rval;

    if(phase == RESOURCE_COLLECTION_PHASE_REORDER)
    {
        if(index_size + size <= offset_src)
            return false;

        offset_dst = header->m_index_offset;
        rval = dmm->Reserve(offset_dst, size, ptr_dst);
        if(rval != MBError::SUCCESS)
            throw rval;
        header->m_index_offset = offset_dst + size;
        index_reorder_cnt++;
    }
    else
    {
#ifdef __DEBUG__
        assert(index_size + size <= offset_src);
#endif
        offset_dst = index_size;
        ptr_dst = dmm->GetShmPtr(offset_dst, size);
    }

    ptr_src = dmm->GetShmPtr(offset_src, size);
    BufferCopy(offset_dst, ptr_dst, offset_src, ptr_src, size, dmm);

    offset_src = offset_dst;
    return true;
}

bool ResourceCollection::MoveDataBuffer(int phase, size_t &offset_src, int size)
{
    data_size = dict->CheckAlignment(data_size, size);
    if(data_size == offset_src)
        return false;

    uint8_t *ptr_src;
    uint8_t *ptr_dst;
    size_t offset_dst;
    int rval;

    if(phase == RESOURCE_COLLECTION_PHASE_REORDER)
    {
        if(data_size + size < offset_src)
            return false;

        offset_dst = header->m_data_offset;
        rval = dict->Reserve(offset_dst, size, ptr_dst);
        if(rval != MBError::SUCCESS)
            throw rval;
        header->m_data_offset = offset_dst + size;
        data_reorder_cnt++;
    }
    else
    {
#ifdef __DEBUG__
        assert(data_size + size <= offset_src);
#endif
        offset_dst = data_size;
        ptr_dst = dict->GetShmPtr(offset_dst, size);
    }

    ptr_src = dict->GetShmPtr(offset_src, size);
    BufferCopy(offset_dst, ptr_dst, offset_src, ptr_src, size, dict);

    offset_src = offset_dst;
    return true;
}

void ResourceCollection::DoTask(int phase, DBTraverseNode &dbt_node)
{
    if(phase == RESOURCE_COLLECTION_PHASE_REORDER)
    {
        // collect stats for adjusting values in header
        if(dbt_node.buffer_type & BUFFER_TYPE_DATA)
            db_cnt++;
        if(dbt_node.buffer_type & BUFFER_TYPE_EDGE_STR)
            edge_str_size += dbt_node.edgestr_size;
        if(dbt_node.buffer_type & BUFFER_TYPE_NODE)
            node_cnt++;
    }

    header->excep_lf_offset = dbt_node.edge_offset;
    if(rc_type & RESOURCE_COLLECTION_TYPE_INDEX)
    {
        if(dbt_node.buffer_type & BUFFER_TYPE_NODE)
        {
            if(MoveIndexBuffer(phase, dbt_node.node_offset, dbt_node.node_size))
            {
                Write6BInteger(header->excep_buff, dbt_node.node_offset);
#ifdef __LOCK_FREE__
                lfree->WriterLockFreeStart(dbt_node.edge_offset);
#endif
                header->excep_offset = dbt_node.node_link_offset;
                header->excep_updating_status = EXCEP_STATUS_RC_NODE;
                dmm->WriteData(header->excep_buff, OFFSET_SIZE, dbt_node.node_link_offset);
                header->excep_updating_status = 0;
#ifdef __LOCK_FREE__
                lfree->WriterLockFreeStop();
#endif
                // Update data_link_offset since node may have been moved.
                if(dbt_node.buffer_type & BUFFER_TYPE_DATA)
                    dbt_node.data_link_offset = dbt_node.node_offset + 2;
            }
            index_size += dbt_node.node_size;
        }

        if(dbt_node.buffer_type & BUFFER_TYPE_EDGE_STR)
        {
            if(MoveIndexBuffer(phase, dbt_node.edgestr_offset, dbt_node.edgestr_size))
            {
                Write5BInteger(header->excep_buff, dbt_node.edgestr_offset);
#ifdef __LOCK_FREE__
                lfree->WriterLockFreeStart(dbt_node.edge_offset);
#endif
                header->excep_offset = dbt_node.edgestr_link_offset;
                header->excep_updating_status = EXCEP_STATUS_RC_EDGE_STR;
                dmm->WriteData(header->excep_buff, OFFSET_SIZE-1, dbt_node.edgestr_link_offset);
                header->excep_updating_status = 0;
#ifdef __LOCK_FREE__
                lfree->WriterLockFreeStop();
#endif
            }
            index_size += dbt_node.edgestr_size;
        }
    }

    if(rc_type & RESOURCE_COLLECTION_TYPE_DATA)
    {
        if(dbt_node.buffer_type & BUFFER_TYPE_DATA)
        {
            if(MoveDataBuffer(phase, dbt_node.data_offset, dbt_node.data_size))
            {
                Write6BInteger(header->excep_buff, dbt_node.data_offset);
#ifdef __LOCK_FREE__
                lfree->WriterLockFreeStart(dbt_node.edge_offset);
#endif
                header->excep_offset = dbt_node.data_link_offset;;
                header->excep_updating_status = EXCEP_STATUS_RC_DATA;
                dmm->WriteData(header->excep_buff, OFFSET_SIZE, dbt_node.data_link_offset);
                header->excep_updating_status = 0;
#ifdef __LOCK_FREE__
                lfree->WriterLockFreeStop();
#endif
            }
            data_size += dbt_node.data_size;
        }
    }

    header->excep_updating_status = 0;

    if(async_writer_ptr != NULL)
    {
        if(rc_loop_counter++ > RC_TASK_CHECK)
        {
            rc_loop_counter = 0;
            async_writer_ptr->ProcessTask(NUM_ASYNC_TASK, true);
        }
    }
}


void ResourceCollection::ReorderBuffers()
{
    if(rc_type & RESOURCE_COLLECTION_TYPE_INDEX)
        Logger::Log(LOG_LEVEL_INFO, "index size before reorder: %llu", header->m_index_offset);
    if(rc_type & RESOURCE_COLLECTION_TYPE_DATA)
        Logger::Log(LOG_LEVEL_INFO, "data size before reorder: %llu", header->m_data_offset);

    db_cnt = 0;
    edge_str_size = 0;
    node_cnt = 0;

    TraverseDB(RESOURCE_COLLECTION_PHASE_REORDER);

    if(rc_type & RESOURCE_COLLECTION_TYPE_INDEX)
        Logger::Log(LOG_LEVEL_INFO, "index size after reorder: %llu", header->m_index_offset);
    if(rc_type & RESOURCE_COLLECTION_TYPE_DATA)
        Logger::Log(LOG_LEVEL_INFO, "data size after reorder: %llu", header->m_data_offset);

    if(rc_type & RESOURCE_COLLECTION_TYPE_INDEX)
    {
        index_reorder_status = MBError::SUCCESS;
        Logger::Log(LOG_LEVEL_INFO, "number of index buffer reordered: %lld", index_reorder_cnt);
    }
    if(rc_type & RESOURCE_COLLECTION_TYPE_DATA)
    {
        data_reorder_status = MBError::SUCCESS;
        Logger::Log(LOG_LEVEL_INFO, "number of data buffer reordered: %lld", data_reorder_cnt);
    }

    if(db_cnt != header->count)
    {
        Logger::Log(LOG_LEVEL_INFO, "adjusting db count to %lld from %lld", db_cnt, header->count);
        header->count = db_cnt;
    }
    header->edge_str_size = edge_str_size;
    header->n_states = node_cnt;
}

void ResourceCollection::ProcessRCTree()
{
    Logger::Log(LOG_LEVEL_INFO, "resource collection done, traversing the rc tree %llu entries", header->rc_count);

    int count = 0;
    int rval;
    DB db_itr(db_ref);
    for(DB::iterator iter = db_itr.begin(false, true); iter != db_itr.end(); ++iter)
    {
        iter.value.options = 0;
        rval = dict->Add((const uint8_t *)iter.key.data(), iter.key.size(), iter.value, true);
        if(rval != MBError::SUCCESS)
            Logger::Log(LOG_LEVEL_WARN, "failed to add: %s", MBError::get_error_str(rval));
        if(count++ > RC_TASK_CHECK)
        {
            count = 0;
            async_writer_ptr->ProcessTask(NUM_ASYNC_TASK, false);
        }

        if(header->m_index_offset > rc_index_offset ||
           header->m_data_offset > rc_data_offset)
        {
            Logger::Log(LOG_LEVEL_ERROR, "not enough space for insertion: %llu, %llu",
                        header->m_index_offset, header->m_data_offset);
            break;
        }
    }

    header->rc_count = 0;
    header->rc_root_offset.store(0, MEMORY_ORDER_WRITER);

    // Clear the rc tree
    dmm->ClearRootEdges_RC();
}

int ResourceCollection::ExceptionRecovery()
{
    if(!db_ref.is_open())
        return db_ref.Status();

    int rval = MBError::SUCCESS;
    if(header->rc_m_index_off_pre != 0 && header->rc_m_data_off_pre != 0)
    {
        Logger::Log(LOG_LEVEL_WARN, "previous rc was not completed successfully, retrying...");
        try {
            // This is a blocking call and should be called when writer starts up.
            ReclaimResource(1, 1, MAX_6B_OFFSET, MAX_6B_OFFSET, NULL);
        } catch (int err) {
            if(err != MBError::RC_SKIPPED)
                rval = err;
        }

        if(rval != MBError::SUCCESS)
        {
            Logger::Log(LOG_LEVEL_ERROR, "failed to run rc recovery: %s, clear db!!!", MBError::get_error_str(rval));
            dict->RemoveAll();
        }
    }

    header->rc_root_offset = 0;
    header->rc_count = 0;

    return rval;
}

}
