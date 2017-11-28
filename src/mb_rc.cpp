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

namespace mabain {

ResourceCollection::ResourceCollection(const DB &db, int rct)
                   : DBTraverseBase(db), rc_type(rct)
{
}

ResourceCollection::~ResourceCollection()
{
}

void ResourceCollection::ReclaimResource(int min_index_size, int min_data_size)
{
    if(!db_ref.is_open())
        throw db_ref.Status();

    Logger::Log(LOG_LEVEL_INFO, "ResourceCollection started");

    timeval start, stop;
    gettimeofday(&start,NULL);

    Prepare(min_index_size, min_data_size);
    ReorderBuffers();
    CollectBuffers();

    Finish();
    gettimeofday(&stop,NULL);

    uint64_t timediff = (stop.tv_sec - start.tv_sec)*1000000 + (stop.tv_usec - start.tv_usec);
    if(timediff > 1000000)
    {
        Logger::Log(LOG_LEVEL_INFO, "ResourceCollection finished in %lf seconds",
                    timediff/1000000.);
        std::cout << "resourceCollection finished in " << timediff/1000000. << " seconds\n";
    }
    else
    {
        Logger::Log(LOG_LEVEL_INFO, "ResourceCollection finished in %lf milliseconds",
                    timediff/1000.);
        std::cout << "resourceCollection finished in " << timediff/1000. << " milliseconds\n";
    }
}

/////////////////////////////////////////////////////////
////////////////// Private Methods //////////////////////
/////////////////////////////////////////////////////////

void ResourceCollection::Prepare(int min_index_size, int min_data_size)
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
        Logger::Log(LOG_LEVEL_INFO, "gc skipped");
        throw (int) MBError::SUCCESS;
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

    Logger::Log(LOG_LEVEL_INFO, "DB stats before running rc");
    db_ref.PrintStats(*Logger::GetLogStream());
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
                    m_index_off_pre - index_offset_top);
        header->m_index_offset = index_offset_top;
        header->pending_index_buff_size = 0;
    }
    if(data_rc_status == MBError::SUCCESS)
    {
        Logger::Log(LOG_LEVEL_INFO, "data buffer size reclaimed: %lld",
                    m_data_off_pre - data_offset_top);
        header->m_data_offset = data_offset_top;
        header->pending_data_buff_size = 0;
    }

    Logger::Log(LOG_LEVEL_INFO, "DB stats after running rc");
    db_ref.PrintStats(*Logger::GetLogStream());
}

bool ResourceCollection::MoveIndexBuffer(int phase, size_t &offset_src, int size)
{
    index_offset_top = dmm->CheckAlignment(index_offset_top, size);

    if(index_offset_top == offset_src)
        return false;

    uint8_t *ptr_src;
    uint8_t *ptr_dst;
    size_t offset_dst;
    int rval;

    if(phase == RESOURCE_COLLECTION_PHASE_REORDER)
    {
        if(index_offset_top + size <= offset_src)
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
        assert(index_offset_top + size <= offset_src);
        offset_dst = index_offset_top;
        ptr_dst = dmm->GetShmPtr(offset_dst, size);
    }

    ptr_src = dmm->GetShmPtr(offset_src, size);
    BufferCopy(offset_dst, ptr_dst, offset_src, ptr_src, size, dmm);

    offset_src = offset_dst;
    return true;
}

bool ResourceCollection::MoveDataBuffer(int phase, size_t &offset_src, int size)
{
    data_offset_top = dict->CheckAlignment(data_offset_top, size);
    if(data_offset_top == offset_src)
        return false;

    uint8_t *ptr_src;
    uint8_t *ptr_dst;
    size_t offset_dst;
    int rval;

    if(phase == RESOURCE_COLLECTION_PHASE_REORDER)
    {
        if(data_offset_top + size < offset_src)
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
        assert(data_offset_top + size <= offset_src);
        offset_dst = data_offset_top;
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
            index_offset_top += dbt_node.node_size;
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
            index_offset_top += dbt_node.edgestr_size;
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
            data_offset_top += dbt_node.data_size;
        }
    }

    header->excep_updating_status = 0;
}


void ResourceCollection::ReorderBuffers()
{
    dmm->ResetSlidingWindow();
    dict->ResetSlidingWindow();

    Logger::Log(LOG_LEVEL_INFO, "index size before reorder: %llu", header->m_index_offset);
    Logger::Log(LOG_LEVEL_INFO, "data size before reorder: %llu", header->m_data_offset);

    TraverseDB(RESOURCE_COLLECTION_PHASE_REORDER);

    Logger::Log(LOG_LEVEL_INFO, "index size after reorder: %llu", header->m_index_offset);
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
