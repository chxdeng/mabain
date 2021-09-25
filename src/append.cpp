#include <iostream>

#include "append.h"

#define MAX_APPEND_SIZE 512

namespace mabain {

Append::Append(Dict &dict_ref, EdgePtrs &eptrs)
                              : dict(dict_ref), 
                                edge_ptrs(eptrs),
                                existing_key(false),
                                old_data_offset(0)
{
}

Append::~Append()
{
}

bool Append::IsExistingKey() const
{
    return existing_key;
}

size_t Append::GetOldDataOffset() const
{
    return old_data_offset;
}

// data_offset: both input and output
int Append::AppendDataBuffer(size_t &data_offset, const uint8_t *buff, int buff_len)
{
    uint16_t curr_data_size;
    if (dict.ReadData(reinterpret_cast<uint8_t*>(&curr_data_size),
                      DATA_SIZE_BYTE, data_offset) != DATA_SIZE_BYTE) {
        return MBError::READ_ERROR;
    }
    uint16_t appended_data_len = curr_data_size + buff_len;
    if (appended_data_len > MAX_APPEND_SIZE) {
        old_data_offset = data_offset;
        dict.ReserveData(buff, buff_len, data_offset);
    } else {
        uint8_t *combined_buff = new uint8_t[appended_data_len];
        if (dict.ReadData(combined_buff, curr_data_size, data_offset + DATA_HDR_BYTE)
                != curr_data_size) {
            delete []combined_buff;
            return MBError::READ_ERROR;
        }
        memcpy(combined_buff + curr_data_size, buff, buff_len);
        dict.ReleaseBuffer(data_offset);
        dict.ReserveData(combined_buff, appended_data_len, data_offset); 
        delete []combined_buff;
    }
    return MBError::SUCCESS;
}

int Append::AddDataToLeafNode(const uint8_t *buff, int buff_len)
{
    int rval;
    existing_key = true;
    size_t data_offset = Get6BInteger(edge_ptrs.offset_ptr);
    rval = AppendDataBuffer(data_offset, buff, buff_len);
    if (rval != MBError::SUCCESS) {
        return rval;
    }
    Write6BInteger(edge_ptrs.offset_ptr, data_offset);

    IndexHeader *header = dict.GetHeaderPtr();
    LockFree *lfree = dict.GetLockFreePtr();
#ifdef __LOCK_FREE__
    lfree->WriterLockFreeStart(edge_ptrs.offset);
#endif
    header->excep_updating_status = EXCEP_STATUS_ADD_DATA_OFF;
    dict.GetMM()->WriteData(edge_ptrs.offset_ptr, OFFSET_SIZE, edge_ptrs.offset+EDGE_NODE_LEADING_POS);
#ifdef __LOCK_FREE__
    lfree->WriterLockFreeStop();
#endif
    return MBError::SUCCESS;
}

int Append::AddDataBuffer(const uint8_t *buff, int buff_len)
{
    if (edge_ptrs.flag_ptr[0] & EDGE_FLAG_DATA_OFF) {
        // leaf node
        return AddDataToLeafNode(buff, buff_len);
    }

    IndexHeader *header = dict.GetHeaderPtr();
    // non-leaf node
    uint8_t *node_buff = header->excep_buff;
    size_t node_off = Get6BInteger(edge_ptrs.offset_ptr);
    if (dict.GetMM()->ReadData(node_buff, NODE_EDGE_KEY_FIRST, node_off)
            != NODE_EDGE_KEY_FIRST) {
        return MBError::READ_ERROR;
    }

    size_t data_off;
    if (node_buff[0] & FLAG_NODE_MATCH) {
        // existing key
        existing_key = true; 
        data_off = Get6BInteger(node_buff+2);
        int rval = AppendDataBuffer(data_off, buff, buff_len);
        if (rval != MBError::SUCCESS) {
            return rval;
        }
        node_buff[NODE_EDGE_KEY_FIRST] = 0;
    } else {
        // new key
        node_buff[0] |= FLAG_NODE_MATCH;
        node_buff[NODE_EDGE_KEY_FIRST] = 1;
        dict.ReserveData(buff, buff_len, data_off);
    }
    Write6BInteger(node_buff+2, data_off);

    LockFree *lfree = dict.GetLockFreePtr();
#ifdef __LOCK_FREE__
    header->excep_lf_offset = edge_ptrs.offset;
    lfree->WriterLockFreeStart(edge_ptrs.offset);
#endif
    header->excep_updating_status = EXCEP_STATUS_ADD_NODE;
    dict.GetMM()->WriteData(node_buff, NODE_EDGE_KEY_FIRST, node_off); 
#ifdef __LOCK_FREE__
    lfree->WriterLockFreeStop();
#endif
    header->excep_updating_status = EXCEP_STATUS_NONE;
    return MBError::SUCCESS;
}

}
