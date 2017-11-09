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

#include <iostream>
#include <sys/stat.h>
#include <dirent.h>

#include "mb_shrink.h"
#include "error.h"
#include "logger.h"
#include "integer_4b_5b.h"

namespace mabain {

MBShrink::MBShrink(DB &db) : db_ref(db), db_link(NULL)
{
    index_start_off = 0;
    index_shrink_size = 0;
    data_start_off = 0;
    data_shrink_size = 0;
    min_data_scan_off = 0;
    max_data_scan_off = 0;
    buffer = NULL;
    buff_size = 0;

    dict = db_ref.GetDictPtr();
    if(dict == NULL)
        return;

    if(!(dict->GetDBOptions() & CONSTS::ACCESS_MODE_WRITER))
    {
        Logger::Log(LOG_LEVEL_ERROR, "readers are not allowed to perform db shrink");
        dict = NULL;
        return;
    }

    dmm = dict->GetMM();
    header = dict->GetHeader();
    data_free_lists = dict->GetFreeList();
    if(header == NULL || dmm == NULL || data_free_lists == NULL)
    {
        dict = NULL;
        return;
    }

    index_free_lists = dmm->GetFreeList();
    node_size = dmm->GetNodeSizePtr();
    if(node_size == NULL || index_free_lists == NULL)
        dict = NULL;

    min_index_scan_off = dmm->GetRootOffset() + node_size[NUM_ALPHABET-1];
    max_index_scan_off = header->m_index_offset;
    min_data_scan_off = dict->GetStartDataOffset();
    max_data_scan_off = header->m_data_offset;

    lfree = dict->GetLockFreePtr(); 
}

MBShrink::~MBShrink()
{
    if(db_link != NULL)
    {
        std::string link_dir = db_link->GetDictPtr()->GetDBDir();
        DIR *dir = opendir(link_dir.c_str());
        if(dir != NULL)
        {
            struct dirent *next_file;
            std::string filepath;
            while((next_file = readdir(dir)) != NULL)
            {
                if(strcmp(next_file->d_name, "..") == 0 ||
                   strcmp(next_file->d_name, ".") == 0)
                    continue;
                filepath = link_dir + next_file->d_name;
                if(unlink(filepath.c_str()) != 0)
                {
                    Logger::Log(LOG_LEVEL_WARN, "failed to remove %s", filepath.c_str());   
                }
            }
            closedir(dir);
        }
        else
        {
            Logger::Log(LOG_LEVEL_WARN, "failed to open dir %s", link_dir.c_str());
        }

        db_link->Close();
        delete db_link;
    }

    if(buffer != NULL)
        delete [] buffer;
}

int MBShrink::Shrink(int64_t min_index_shk_size, int64_t min_data_shk_size)
{
    if(dict == NULL)
        return MBError::NOT_INITIALIZED;

    int rval = MBError::SUCCESS;
    if(header->pending_index_buff_size < min_index_shk_size)
    {
        Logger::Log(LOG_LEVEL_INFO, "no need to shrink index, "
                 "required shrink size: %llu, available size: %llu", 
                  min_index_shk_size, header->pending_index_buff_size);
    }
    else
    {
        Logger::Log(LOG_LEVEL_INFO, "start index shrinking");
        try {
            rval = ShrinkIndex();
        } catch (int err) {
            std::cerr << "Caught exception when calling ShrinkIndex: "
                      << MBError::get_error_str(rval) << "\n";
            rval = err;
        }
        if(rval != MBError::SUCCESS)
            return rval;
        Logger::Log(LOG_LEVEL_INFO, "shrinked index size: %lld",index_shrink_size);
    }

    if(header->pending_data_buff_size < min_data_shk_size)
    {
        Logger::Log(LOG_LEVEL_INFO, "no need to shrink data, "
                "required shrink size: %llu, available size: %llu", 
                min_data_shk_size, header->pending_data_buff_size);
    }
    else
    {
        Logger::Log(LOG_LEVEL_INFO, "start data shrinking");
        try {
            rval = ShrinkData();
        } catch (int err) {
            std::cerr << "Caught exception when calling ShrinkData: " 
                      << MBError::get_error_str(rval) << "\n";
            rval = err;
        }
        if(rval == MBError::SUCCESS)
            Logger::Log(LOG_LEVEL_INFO, "shrinked data size: %lld", data_shrink_size);
    }

    return rval;
}

size_t MBShrink::GetCurrOffset(size_t offset_orig)
{
    assert(db_link != NULL);

    MBData mbd;
    std::string key;
    key = std::to_string(offset_orig);
    int rval = db_link->Find(key.c_str(), key.length(), mbd);
    if(rval == MBError::SUCCESS)
    {
        IndexNode *idp = (IndexNode *) mbd.buff;
        return idp->node_edge_off;
    }
    return offset_orig;
}

void MBShrink::UpdateOffset(const std::string &key, size_t offset_curr)
{
    assert(db_link != NULL);
    MBData mbd;
    int rval = db_link->Find(key.c_str(), key.length(), mbd);
    assert(rval == MBError::SUCCESS);
    IndexNode *idp = (IndexNode *) mbd.buff;
    idp->node_edge_off = offset_curr;
    db_link->GetDictPtr()->WriteData((const uint8_t *)idp, sizeof(*idp),
                                      mbd.data_offset+DATA_SIZE_BYTE);
}

int MBShrink::MoveIndexBuffer(size_t src, const std::string &key, int size, size_t parent,
                              int rel_off, int buff_type)
{
    assert(src > index_start_off && index_start_off > 0);

    ReallocBuffer(size);

    int rval = dmm->ReadData(buffer, size, src, false);
    if(rval != size)
        return MBError::READ_ERROR;

    size_t tmp_off = index_start_off;
    uint8_t *ptr_low = NULL;
    rval = dmm->Reserve(tmp_off, size, ptr_low);
    if(rval != MBError::SUCCESS)
    {
        Logger::Log(LOG_LEVEL_ERROR, "failed to reserve low index %s",
                MBError::get_error_str(rval));
        return rval;
    }
    // check alignment
    if(tmp_off != index_start_off)
    {
        int align_size = tmp_off - index_start_off;
        assert(align_size > 0);
        assert(align_size == index_free_lists->GetAlignmentSize(align_size));
        index_free_lists->AddBuffer(index_start_off, align_size);
        header->pending_index_buff_size += align_size;

        index_shrink_size -= align_size;
        assert(index_shrink_size >= 0);
        if(index_shrink_size > 0)
            index_start_off = tmp_off;
        else
            index_start_off = 0;
    }

    if(src > index_start_off && index_shrink_size > 0)
    {
        uint8_t buff[OFFSET_SIZE];
        int byte_to_write;
        size_t write_offset;
        parent = GetCurrOffset(parent);

        write_offset = parent + rel_off;
        if(BUFFER_TYPE_NODE == buff_type)
        {
            Write6BInteger(buff, index_start_off);
            byte_to_write = OFFSET_SIZE;
#ifdef __LOCK_FREE__
            lfree->WriterLockFreeStart(write_offset - EDGE_NODE_LEADING_POS);
#endif
        }
        else
        {
            Write5BInteger(buff, index_start_off);
            byte_to_write = EDGE_LEN_POS;
#ifdef __LOCK_FREE__
            lfree->WriterLockFreeStart(write_offset);
#endif
        }

        if(ptr_low != NULL)
            memcpy(ptr_low, buffer, size);
        else
            dmm->WriteData(buffer, size, index_start_off);
        dmm->WriteData(buff, byte_to_write, write_offset);

#ifdef __LOCK_FREE__
        lfree->WriterLockFreeStop();
#endif

        UpdateOffset(key, index_start_off);
        index_start_off += size;
    }
    else
    {
        if(index_start_off > 0 && index_shrink_size > 0)
        {
            header->pending_index_buff_size += index_free_lists->GetAlignmentSize(index_shrink_size);
            index_free_lists->AddBuffer(index_start_off, index_shrink_size); 
        }
        index_start_off = 0;
        index_shrink_size = 0;
    }

    return MBError::SUCCESS;
}

//TODO: considering multi-thread implementation of scaning index blocks???
int MBShrink::ScanDictMem()
{
    if(db_link == NULL)
        return MBError::NOT_INITIALIZED;

    MBData mbd;
    IndexNode *inp;
    size_t curr_off = min_index_scan_off;
    size_t max_off = max_index_scan_off;
    int rval = MBError::SUCCESS;
    int size_counter = 0;
    std::string key;
    while(curr_off < max_off)
    {
        key = std::to_string(curr_off);
        rval = db_link->Find(key.c_str(), key.length(), mbd);
        if(rval == MBError::SUCCESS)
        {
            inp = (IndexNode *) mbd.buff; 
            if(index_start_off > 0)
            {
                rval = MoveIndexBuffer(curr_off, key,
                                       inp->buffer_size,
                                       inp->parent_off,
                                       inp->rel_parent_off,
                                       inp->buffer_type);
                if(rval != MBError::SUCCESS)
                    break;
            }
            curr_off += inp->buffer_size;
            size_counter += inp->buffer_size;
        }
        else if(rval == MBError::NOT_EXIST)
        {
            if(index_start_off == 0)
            {
                index_start_off = curr_off;
                index_shrink_size = BUFFER_ALIGNMENT;
            }
            else
            {
                index_shrink_size += BUFFER_ALIGNMENT;
            }
            curr_off += BUFFER_ALIGNMENT;
            size_counter += BUFFER_ALIGNMENT;
        }
        else
        {
            break;
        }

        if(size_counter > (int)INDEX_BLOCK_SIZE/4)
        {
            size_counter = 0;
            std::cout << "scanned index size " << curr_off << "\n";
            Logger::Log(LOG_LEVEL_INFO, "scanned index size: %llu", curr_off);
        }
    }

    if(rval == MBError::NOT_EXIST)
        rval = MBError::SUCCESS;

    if(db_link->RemoveAll() != MBError::SUCCESS)
        Logger::Log(LOG_LEVEL_ERROR, "failed to empty db_link for shrink index");
    return rval;
}

int MBShrink::BuildIndexLink()
{
    int rval;

    rval = OpenLinkDB();
    if(rval != MBError::SUCCESS)
        return rval;

    rval = db_link->RemoveAll();
    if(rval != MBError::SUCCESS)
        return rval;

    DB::iterator iter = DB::iterator(db_ref, DB_ITER_STATE_INIT);
    rval = iter.init_no_next();
    if(rval != MBError::SUCCESS)
        return rval;

    IndexNode idx_node;
    int btype;
    size_t parent_node_off = dmm->GetRootOffset();
    std::string key;
    while((btype = iter.next_index_buffer(parent_node_off, &idx_node)) != BUFFER_TYPE_NONE)
    {
        if(idx_node.node_edge_off < min_index_scan_off ||
           idx_node.node_edge_off >= max_index_scan_off)
            continue;
        if(btype == BUFFER_TYPE_NODE)
        {
            int nt = iter.node_buff[1];
            idx_node.buffer_size = index_free_lists->GetAlignmentSize(node_size[nt]);
            idx_node.buffer_type = BUFFER_TYPE_NODE;
        }
        else
        {
            idx_node.buffer_size = index_free_lists->GetAlignmentSize(iter.edge_ptrs.len_ptr[0]-1);
            idx_node.buffer_type = BUFFER_TYPE_EDGE_STR;
        }
        key = std::to_string(idx_node.node_edge_off);
        rval = db_link->Add(key.c_str(),
                            key.length(),
                            reinterpret_cast<const char *>(&idx_node),
                            sizeof(IndexNode));
        if(rval != MBError::SUCCESS)
            break;
    }

    std::cout << "index link count: " << db_link->Count() << "\n";
    Logger::Log(LOG_LEVEL_INFO, "index link count: %lld", db_link->Count());
    return rval;
}

int MBShrink::ShrinkIndex()
{
    Logger::Log(LOG_LEVEL_INFO, "empty index free lists");
    index_free_lists->Empty();
    dmm->ResetSlidingWindow();

    int rval = MBError::SUCCESS;
    int64_t old_pending_size = header->pending_index_buff_size;
    index_start_off = 0;
    index_shrink_size = 0;
    min_index_scan_off = dmm->GetRootOffset() + node_size[NUM_ALPHABET-1];
    max_index_scan_off = 0;
    while(min_index_scan_off < header->m_index_offset)
    {
        max_index_scan_off += 3*INDEX_BLOCK_SIZE;
        if(max_index_scan_off > header->m_index_offset ||
          (header->m_index_offset - max_index_scan_off) < INDEX_BLOCK_SIZE/2)
        {
            max_index_scan_off = header->m_index_offset;
        }

        Logger::Log(LOG_LEVEL_INFO, "Building index link: %llu to %llu",
                min_index_scan_off, max_index_scan_off);
        std::cout << "Building index link: " << min_index_scan_off <<
                     " to " << max_index_scan_off << "\n";
        rval = BuildIndexLink();
        if(rval != MBError::SUCCESS)
        {
            Logger::Log(LOG_LEVEL_WARN, "failed to build index link for DB shrinking: %s",
                    MBError::get_error_str(rval));
            break;
        }

        Logger::Log(LOG_LEVEL_INFO, "Shrinking index block: %llu to %llu",
                min_index_scan_off, max_index_scan_off);
        std::cout << "Shrinking index block: " << min_index_scan_off <<
                     " to " << max_index_scan_off << "\n";
        rval = ScanDictMem();
        if(rval != MBError::SUCCESS)
        {
            Logger::Log(LOG_LEVEL_ERROR, "failed to shrink DB index: %s",
                    MBError::get_error_str(rval));
            break;
        }

        std::cout << "Current index shrink start offset: " << index_start_off
                  << " size: " << index_shrink_size << "\n";
        min_index_scan_off = max_index_scan_off;
    }

    if(rval == MBError::SUCCESS)
    {
        if(index_start_off > 0)
        {
            Logger::Log(LOG_LEVEL_INFO, "reset max index offset to %llu", index_start_off);
            header->pending_index_buff_size -= old_pending_size;
            header->m_index_offset = index_start_off;
        }
    }

    return rval;
}

int MBShrink::BuildDataLink()
{
    int rval;

    rval = OpenLinkDB();
    if(rval != MBError::SUCCESS)
        return rval;

    rval = db_link->RemoveAll();
    if(rval != MBError::SUCCESS)
        return rval;

    IndexNode idx_node;
    std::string key;
    for(DB::iterator iter = db_ref.begin(); iter != db_ref.end(); ++iter)
    {
        idx_node.node_edge_off = iter.value.data_offset;
        if(idx_node.node_edge_off < min_data_scan_off ||
           idx_node.node_edge_off >= max_data_scan_off)
            continue;

        key = std::to_string(idx_node.node_edge_off);
        idx_node.rel_parent_off = 0; // not used for data link
        idx_node.buffer_size = data_free_lists->GetAlignmentSize(iter.value.data_len + DATA_SIZE_BYTE);

        // idx_node.parent_off is the offset where data have to
        // be modified for new data offset. Use idx_node.node_edge_off to
        // save the edge offset for lock-free. Note the name is confusing here.
        // node_edge_off is otherwise not used at all.
        if(iter.match == MATCH_NODE)
        {
            idx_node.parent_off = iter.edge_ptrs.curr_node_offset + 2;
            idx_node.node_edge_off = iter.edge_ptrs.parent_offset; //This is the parent edge offset.
        }
        else
        {
            idx_node.parent_off = iter.edge_ptrs.parent_offset + EDGE_NODE_LEADING_POS;
            idx_node.node_edge_off = iter.edge_ptrs.parent_offset;
        }

        rval = db_link->Add(key.c_str(),
                            key.length(),
                            reinterpret_cast<const char *>(&idx_node),
                            sizeof(IndexNode));
        if(rval != MBError::SUCCESS)
            break;

    }

    std::cout << "data link count: " << db_link->Count() << "\n";
    Logger::Log(LOG_LEVEL_INFO, "data link count: %lld", db_link->Count());
    return rval;
}

int MBShrink::MoveDataBuffer(size_t src, int size, size_t parent, size_t edge_off)
{
    assert(src > data_start_off && data_shrink_size > 0);

    ReallocBuffer(size);

    int rval = dict->ReadData(buffer, size, src, false);
    if(rval != size)
        return MBError::READ_ERROR;

    size_t tmp_off = data_start_off;
    uint8_t *ptr_low = NULL;
    rval = dict->Reserve(tmp_off, size, ptr_low);
    if(rval != MBError::SUCCESS)
    {
        Logger::Log(LOG_LEVEL_ERROR, "failed to reserve data low buffer %s",
                MBError::get_error_str(rval));
        return rval;
    }

    if(tmp_off != data_start_off)
    {
        int align_size = tmp_off - data_start_off;
        assert(align_size > 0);
        assert(align_size == data_free_lists->GetAlignmentSize(align_size));
        data_free_lists->AddBuffer(data_start_off, align_size);
        header->pending_data_buff_size += align_size;

        data_shrink_size -= align_size;
        assert(data_shrink_size >= 0);
        if(data_shrink_size > 0)
            data_start_off = tmp_off;
        else
            data_start_off = 0;
    }

    if(src > data_start_off && data_shrink_size > 0)
    {
        uint8_t buff[OFFSET_SIZE];

        Write6BInteger(buff, data_start_off);
#ifdef __LOCK_FREE__
        lfree->WriterLockFreeStart(edge_off);
#endif
        if(ptr_low != NULL)
            memcpy(ptr_low, buffer, size);
        else
            dict->WriteData(buffer, size, data_start_off);
        dmm->WriteData(buff, OFFSET_SIZE, parent);
#ifdef __LOCK_FREE__
        lfree->WriterLockFreeStop();
#endif
        data_start_off += size;
    }
    else
    {
        if(data_start_off > 0 && data_shrink_size > 0)
        {
            data_free_lists->AddBuffer(data_start_off, data_shrink_size);
            header->pending_data_buff_size += data_free_lists->GetAlignmentSize(data_shrink_size);
        }
        data_start_off = 0;
        data_shrink_size = 0;
    }

    return MBError::SUCCESS;
}

int MBShrink::ScanData()
{
    if(db_link == NULL)
        return MBError::NOT_INITIALIZED;

    MBData mbd;
    IndexNode *inp;
    size_t curr_off = min_data_scan_off;
    size_t max_off = max_data_scan_off;
    int rval = MBError::SUCCESS;
    int size_counter = 0;
    std::string key;
    while(curr_off < max_off)
    {
        key = std::to_string(curr_off);
        rval = db_link->Find(key.c_str(), key.length(), mbd);
        if(rval == MBError::SUCCESS)
        {
            inp = (IndexNode *) mbd.buff;
            if(data_start_off > 0)
            {
                rval = MoveDataBuffer(curr_off,
                                      inp->buffer_size,
                                      inp->parent_off,
                                      inp->node_edge_off); // node_off is actually edge off.
                if(rval != MBError::SUCCESS)
                    break;
            }
            curr_off += inp->buffer_size;
            size_counter += inp->buffer_size;
        }
        else if(rval == MBError::NOT_EXIST)
        {
            if(data_start_off == 0)
            {
                data_start_off = curr_off;
                data_shrink_size = DATA_BUFFER_ALIGNMENT;
            }
            else
            {
                data_shrink_size += DATA_BUFFER_ALIGNMENT;
            }
            curr_off += DATA_BUFFER_ALIGNMENT; 
            size_counter += DATA_BUFFER_ALIGNMENT;
        }
        else
        {
            break;
        }

        if(size_counter > (int)DATA_BLOCK_SIZE/4)
        {
            size_counter = 0;
            std::cout << "scanned data size " << curr_off << "\n";
            Logger::Log(LOG_LEVEL_INFO, "scanned data size: %llu", curr_off);
        }
    }

    if(rval == MBError::NOT_EXIST)
        rval = MBError::SUCCESS;

    if(db_link->RemoveAll() != MBError::SUCCESS)
        Logger::Log(LOG_LEVEL_ERROR, "failed to empty db_link for shrink data");
    return rval;
}

int MBShrink::ShrinkData()
{
    Logger::Log(LOG_LEVEL_INFO, "empty data free lists");
    data_free_lists->Empty();
    dict->ResetSlidingWindow();

    int rval = MBError::SUCCESS;
    int64_t old_pending_size = header->pending_data_buff_size;
    data_start_off = 0;
    data_shrink_size = 0;
    min_data_scan_off = dict->GetStartDataOffset();
    max_data_scan_off = 0;
    while(min_data_scan_off < header->m_data_offset)
    {
        max_data_scan_off += 3*DATA_BLOCK_SIZE;
        if(max_data_scan_off > header->m_data_offset ||
          (header->m_data_offset - max_data_scan_off) < DATA_BLOCK_SIZE/2)
        {
            max_data_scan_off = header->m_data_offset;
        }

        Logger::Log(LOG_LEVEL_INFO, "Building data link: %llu to %llu",
                min_data_scan_off, max_data_scan_off);
        std::cout << "Building data link: " << min_data_scan_off <<
                     " to " << max_data_scan_off << "\n";
        rval = BuildDataLink();
        if(rval != MBError::SUCCESS)
        {
            Logger::Log(LOG_LEVEL_WARN, "failed to build data link for DB shrinking: %s",
                    MBError::get_error_str(rval));
            break;
        }

        Logger::Log(LOG_LEVEL_INFO, "Scanning data block: %llu to %llu",
                min_data_scan_off, max_data_scan_off);
        std::cout << "Scanning data block: " << min_data_scan_off <<
                     " to " << max_data_scan_off << "\n";
        rval = ScanData();
        if(rval != MBError::SUCCESS)
        {
            Logger::Log(LOG_LEVEL_ERROR, "failed to shrink DB data: %s",
                    MBError::get_error_str(rval));
            break;
        }

        std::cout << "Current data shrink start offset: " << data_start_off
                  << " size: " << data_shrink_size << "\n";
        min_data_scan_off  = max_data_scan_off;
    }

    if(rval == MBError::SUCCESS)
    {
        if(data_start_off > 0)
        {
            Logger::Log(LOG_LEVEL_INFO, "reset max data offset to %llu", data_start_off);
            header->pending_data_buff_size -= old_pending_size;
            header->m_data_offset = data_start_off;
        }
    }

    return rval;
}

#define LINK_SHM_SIZE 134217728
int MBShrink::OpenLinkDB()
{
    if(dict == NULL)
        return MBError::NOT_INITIALIZED;

    if(db_link != NULL)
        return MBError::SUCCESS;

    std::string db_link_dir = dict->GetDBDir() + "db_shrink/";
    if(mkdir(db_link_dir.c_str(), 0755) < 0)
    {
        if(errno != EEXIST)
        {
            Logger::Log(LOG_LEVEL_ERROR, "failed to create %s", db_link_dir.c_str());
            return MBError::OPEN_FAILURE;
        }
    }
    db_link = new DB(db_link_dir, CONSTS::ACCESS_MODE_WRITER | CONSTS::NO_GLOBAL_INIT,
                     LINK_SHM_SIZE, LINK_SHM_SIZE, sizeof(IndexNode), 0);
    if(!db_link->is_open())
    {
        Logger::Log(LOG_LEVEL_ERROR, " failed to open link db: %s", db_link->StatusStr());
        return MBError::OPEN_FAILURE;
    }

    return MBError::SUCCESS;
}

void MBShrink::ReallocBuffer(int size)
{
    if(buff_size >= size)
        return;

    if(buffer != NULL)
        delete [] buffer;
    buffer = new uint8_t[size];
}

}
