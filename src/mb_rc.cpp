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

#define PRUNE_TASK_CHECK     100
#define MAX_PRUNE_COUNT      3

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
int ResourceCollection::LRUEviction()
{
    int64_t pruned = 0;
    int64_t count = 0;
    int rval = MBError::SUCCESS;

    Logger::Log(LOG_LEVEL_INFO, "running LRU eviction for bucket %u", header->eviction_bucket_index);

    uint16_t index_diff = CIRCULAR_INDEX_DIFF(header->eviction_bucket_index,
                              (header->num_update/header->entry_per_bucket) % 0xFFFF);
    uint16_t prune_diff = 0xFFFF - index_diff;
    if(index_diff < 655)
        prune_diff = uint16_t(prune_diff * 0.35); // prune 35%
    else if(index_diff < 3276)
        prune_diff = uint16_t(prune_diff * 0.25); // prune 25%
    else if(index_diff < 6554)
        prune_diff = uint16_t(prune_diff * 0.15); // prune 15%
    else if(index_diff < 9830)
        prune_diff = uint16_t(prune_diff * 0.10); // prune 10%
    else
        prune_diff = uint16_t(prune_diff * 0.05); // prune 5%
    if(prune_diff == 0)
        prune_diff = 1;

    for(DB::iterator iter = db_ref.begin(false); iter != db_ref.end(); ++iter)
    {
        if(CIRCULAR_PRUNE_DIFF(iter.value.bucket_index, header->eviction_bucket_index) < prune_diff)
        {
            rval = dict->Remove((const uint8_t *)iter.key.data(), iter.key.size());
            if(rval != MBError::SUCCESS)
                Logger::Log(LOG_LEVEL_DEBUG, "failed to run eviction %s", MBError::get_error_str(rval));
            else
                pruned++;
        }

        count++;
        if(async_writer_ptr != NULL && (count % PRUNE_TASK_CHECK == 0))
        {
            rval = async_writer_ptr->ProcessTask(1);
            if(rval == MBError::RC_SKIPPED)
                break;
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
            if(LRUEviction() != MBError::TRY_AGAIN)
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
        Logger::Log(LOG_LEVEL_INFO, "defragmentation started");
        gettimeofday(&start,NULL);

        ReorderBuffers();
        CollectBuffers();
        Finish();

        gettimeofday(&stop,NULL);
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
    if(header->pending_index_buff_size < min_index_size)
    {
        rc_type &= ~RESOURCE_COLLECTION_TYPE_INDEX;
    }
    if(header->pending_data_buff_size < min_data_size)
    {
        rc_type &= ~RESOURCE_COLLECTION_TYPE_DATA;
    }
    if(rc_type == 0)
    {
        Logger::Log(LOG_LEVEL_DEBUG, "garbage collection skipped since pending "
                                    "sizes smaller than required");
        throw (int) MBError::RC_SKIPPED;
    }

    if(rc_type & RESOURCE_COLLECTION_TYPE_INDEX)
        index_free_lists->Empty();
    if(rc_type & RESOURCE_COLLECTION_TYPE_DATA)
        data_free_lists->Empty();

    index_reorder_cnt = 0;
    data_reorder_cnt = 0;
    index_rc_status = MBError::NOT_INITIALIZED;
    data_rc_status = MBError::NOT_INITIALIZED;
    index_reorder_status = MBError::NOT_INITIALIZED;
    data_reorder_status = MBError::NOT_INITIALIZED;
    m_index_off_pre = header->m_index_offset;
    m_data_off_pre = header->m_data_offset;
}

void ResourceCollection::CollectBuffers()
{
    if((rc_type & RESOURCE_COLLECTION_TYPE_INDEX) &&
       (index_reorder_status != MBError::SUCCESS))
        return;
    if((rc_type & RESOURCE_COLLECTION_TYPE_DATA) &&
       (data_reorder_status != MBError::SUCCESS))
        return;

    dmm->ResetSlidingWindow();
    dict->ResetSlidingWindow();
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
                    (m_index_off_pre>index_size) ? (m_index_off_pre-index_size) : 0);
        header->m_index_offset = index_size;
        header->pending_index_buff_size = 0;
    }
    if(data_rc_status == MBError::SUCCESS)
    {
        Logger::Log(LOG_LEVEL_INFO, "data buffer size reclaimed: %lld",
                    (m_data_off_pre>data_size) ? (m_data_off_pre-data_size) : 0);
        header->m_data_offset = data_size;
        header->pending_data_buff_size = 0;
    }
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
}


void ResourceCollection::ReorderBuffers()
{
    dmm->ResetSlidingWindow();
    dict->ResetSlidingWindow();

    if(rc_type & RESOURCE_COLLECTION_TYPE_INDEX)
        Logger::Log(LOG_LEVEL_INFO, "index size before reorder: %llu", header->m_index_offset);
    if(rc_type & RESOURCE_COLLECTION_TYPE_DATA)
        Logger::Log(LOG_LEVEL_INFO, "data size before reorder: %llu", header->m_data_offset);

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
}

}
