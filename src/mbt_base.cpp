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

#include "mbt_base.h"

namespace mabain {

DBTraverseBase::DBTraverseBase(const DB& db)
    : db_ref(db)
    , rw_buffer(NULL)
{
    if (!(db.GetDBOptions() & CONSTS::ACCESS_MODE_WRITER))
        throw(int) MBError::NOT_ALLOWED;

    dict = db_ref.GetDictPtr();
    if (dict == NULL)
        throw(int) MBError::NOT_INITIALIZED;

    dmm = dict->GetMM();
    if (dmm == NULL)
        throw(int) MBError::NOT_INITIALIZED;

    header = dict->GetHeaderPtr();
    if (header == NULL)
        throw(int) MBError::NOT_INITIALIZED;

    index_free_lists = dmm->GetFreeList();
    if (index_free_lists == NULL)
        throw(int) MBError::NOT_INITIALIZED;

    data_free_lists = dict->GetFreeList();
    if (data_free_lists == NULL)
        throw(int) MBError::NOT_INITIALIZED;

    lfree = dict->GetLockFreePtr();
    if (lfree == NULL)
        throw(int) MBError::NOT_INITIALIZED;

    rw_buffer_size = 1024;
    rw_buffer = new uint8_t[rw_buffer_size];
}

DBTraverseBase::~DBTraverseBase()
{
    if (rw_buffer != NULL)
        delete[] rw_buffer;
}

void DBTraverseBase::TraverseDB(int arg)
{
    DB::iterator iter = DB::iterator(db_ref, DB_ITER_STATE_INIT);
    int rval = iter.init_no_next();
    if (rval != MBError::SUCCESS)
        throw rval;

    DBTraverseNode dbt_node;
    index_size = dmm->GetRootOffset() + dmm->GetNodeSizePtr()[NUM_ALPHABET - 1];
    data_size = dict->GetStartDataOffset();
    while (iter.next_dbt_buffer(&dbt_node)) {
        GetAlignmentSize(dbt_node);

        // Run-time determination
        DoTask(arg, dbt_node);

        if (dbt_node.buffer_type & BUFFER_TYPE_NODE) {
            iter.add_node_offset(dbt_node.node_offset);
        }
    }
}

void DBTraverseBase::GetAlignmentSize(DBTraverseNode& dbt_node) const
{
    if (dbt_node.buffer_type & BUFFER_TYPE_EDGE_STR)
        dbt_node.edgestr_size = index_free_lists->GetAlignmentSize(dbt_node.edgestr_size);

    if (dbt_node.buffer_type & BUFFER_TYPE_NODE)
        dbt_node.node_size = index_free_lists->GetAlignmentSize(dbt_node.node_size);

    if (dbt_node.buffer_type & BUFFER_TYPE_DATA) {
        uint16_t data_size[2];
        if (dict->ReadData((uint8_t*)&data_size[0], DATA_HDR_BYTE, dbt_node.data_offset)
            != DATA_HDR_BYTE)
            throw(int) MBError::READ_ERROR;
        dbt_node.data_size = data_free_lists->GetAlignmentSize(data_size[0] + DATA_HDR_BYTE);
    }
}

void DBTraverseBase::BufferCopy(size_t offset_dst, uint8_t* ptr_dst,
    size_t offset_src, const uint8_t* ptr_src,
    int size, DRMBase* drm)
{
    if (ptr_src != NULL) {
        if (ptr_dst != NULL) {
            memcpy(ptr_dst, ptr_src, size);
        } else {
            drm->WriteData(ptr_src, size, offset_dst);
        }
    } else {
        if (size > rw_buffer_size)
            ResizeRWBuffer(size);
        if (drm->ReadData(rw_buffer, size, offset_src) != size)
            throw(int) MBError::READ_ERROR;

        if (ptr_dst != NULL) {
            memcpy(ptr_dst, rw_buffer, size);
        } else {
            drm->WriteData(rw_buffer, size, offset_dst);
        }
    }
}

void DBTraverseBase::ResizeRWBuffer(int size)
{
    if (rw_buffer != NULL)
        delete[] rw_buffer;
    rw_buffer = new uint8_t[size];
    rw_buffer_size = size;
}

}
