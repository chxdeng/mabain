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
#include <unistd.h>
#include <sys/syscall.h>

#include <errno.h>

#include "dict.h"
#include "db.h"
#include "error.h"
#include "logger.h"
#include "mb_lsq.h"
#include "version.h"
#include "mb_shrink.h"
#include "integer_4b_5b.h"

namespace mabain {

// Current mabain version 1.0.0
uint16_t version[4] = {1, 0, 0, 0};

DB::~DB()
{
    Logger::Close();
}

int DB::Close(bool shutdown_global)
{
    int rval = MBError::SUCCESS;

    if(dict != NULL)
    {
        if(shutdown_global)
        {
            int db_opts = dict->GetDBOptions();
            if(db_opts & ACCESS_MODE_WRITER)
                dict->PrintStats(Logger::GetLogStream());
            UpdateNumHandlers(db_opts, -1);
        }

        dict->Destroy();
        delete dict;
        dict = NULL;
    }
    else
    {
        rval = status;
    }

    status = MBError::DB_CLOSED;
    Logger::Log(LOG_LEVEL_INFO, "connector %u disconnected from DB", identifier);
    return rval;
}

int DB::UpdateNumHandlers(int mode, int delta)
{
    int rval = MBError::SUCCESS;

    WrLock();

    if(mode & ACCESS_MODE_WRITER)
        rval = dict->UpdateNumWriter(delta);
    else
        dict->UpdateNumReader(delta);

    UnLock();

    return rval;
}

// Constructor for initializing DB handle
DB::DB(const std::string &db_path, size_t memcap_index, size_t memcap_data,
       int db_options, int data_size, uint32_t id, bool init_global)
{
    status = MBError::NOT_INITIALIZED;
    dict = NULL;

    // If id not given, use thread ID
    if(id == 0)
        id = static_cast<uint32_t>(syscall(SYS_gettid));
    identifier = id;

    // Check if the DB directory exist with proper permission
    if(access(db_path.c_str(), F_OK))
    {
        char err_buf[32];
        std::cerr << "database directory check for " + db_path + " failed: " +
                     strerror_r(errno, err_buf, sizeof(err_buf)) << std::endl;
        status = MBError::NO_DB;
        return;
    }

    std::string db_path_tmp;
    if(db_path[db_path.length()-1] != '/')
        db_path_tmp = db_path + "/";
    else
        db_path_tmp = db_path;

    if((db_options & ACCESS_MODE_WRITER) && init_global)
        Logger::InitLogFile(db_path_tmp + "mabain.log");
    else
        Logger::SetLogLevel(LOG_LEVEL_WARN);

    // Check if DB exist. This can be done by check existence of the first index file.
    // If this is the first time the DB is opened and it is in writer mode, then we
    // need to update the header for the first time. If only reader access mode is
    // required and the file does not exist, we should bail here and the DB open will
    // not be successful.
    bool init_header = false;
    std::string index_file_0 = db_path_tmp + "_mabain_i0";
    if(access(index_file_0.c_str(), R_OK))
    {
        if(db_options & ACCESS_MODE_WRITER)
        {
            init_header = true;
        }
        else
        {
            Logger::Log(LOG_LEVEL_ERROR, "database check " + db_path + " failed");
            status = MBError::NO_DB;
            return;
        }
    }

    bool sliding_mmap = false;
    if(db_options & ACCESS_MODE_WRITER)
        sliding_mmap = true;
    dict = new Dict(db_path_tmp, init_header, data_size, db_options, memcap_index,
                    memcap_data, sliding_mmap, false);

    if((db_options & ACCESS_MODE_WRITER) && init_header)
    {
        Logger::Log(LOG_LEVEL_INFO, "open a new db %s", db_path_tmp.c_str());
        dict->Init(identifier);
#ifdef __SHM_LOCK__
        if(init_global)
            dict->InitShmMutex();
#endif
    }

    if(init_global)
        dict->SetShmLockPtrs();

    if(dict->Status() != MBError::SUCCESS)
    {
        Logger::Log(LOG_LEVEL_ERROR, "failed to iniitialize dict: %s ",
                    MBError::get_error_str(dict->Status()));
        return;
    }

    if(init_global)
    {
        status = UpdateNumHandlers(db_options, 1);
        if(status != MBError::SUCCESS)
        {
            Logger::Log(LOG_LEVEL_ERROR, "failed to initialize db: %s",
                        MBError::get_error_str(dict->Status()));
            return;
        }
    }

    Logger::Log(LOG_LEVEL_INFO, "connector %u successfully opened DB %s for %s",
                identifier, db_path.c_str(),
                (db_options & ACCESS_MODE_WRITER) ? "writing":"reading");
    status = MBError::SUCCESS;
}

int DB::Status() const
{
    return status;
}

bool DB::is_open() const
{
    return status == MBError::SUCCESS;
}

const char* DB::StatusStr() const
{
    return MBError::get_error_str(status);
}

// Find the exact key match
int DB::Find(const char* key, int len, MBData &mdata) const
{
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    int rval;
    rval = dict->Find(reinterpret_cast<const uint8_t*>(key), len, mdata);
#ifdef __LOCK_FREE__
    while(rval == MBError::TRY_AGAIN)
    {
        nanosleep((const struct timespec[]){{0, 10L}}, NULL);
        rval = dict->Find(reinterpret_cast<const uint8_t*>(key), len, mdata);
    }
#endif

    if(rval == MBError::SUCCESS)
        mdata.match_len = len;

    return rval;
}

// Find all possible prefix matches. The caller needs to call this function
// repeatedly if data.next is true.
int DB::FindPrefix(const char* key, int len, MBData &data) const
{
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    if(data.match_len >= len)
        return MBError::OUT_OF_BOUND;

    int rval;
    rval = dict->FindPrefix(reinterpret_cast<const uint8_t*>(key+data.match_len),
                            len-data.match_len, data);

    return rval;
}

// Find the longest prefix match
int DB::FindLongestPrefix(const char* key, int len, MBData &data) const
{
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    data.match_len = 0;

    int rval;
    rval = dict->FindPrefix(reinterpret_cast<const uint8_t*>(key), len, data);
#ifdef __LOCK_FREE__
    while(rval == MBError::TRY_AGAIN)
    {
        nanosleep((const struct timespec[]){{0, 10L}}, NULL);
        data.Clear();
        rval = dict->FindPrefix(reinterpret_cast<const uint8_t*>(key), len, data);
    }
#endif

    return rval;
}

// Add a key-value pair
int DB::Add(const char* key, int len, MBData &mbdata, bool overwrite)
{
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    int rval;
    rval = dict->Add(reinterpret_cast<const uint8_t*>(key), len, mbdata, overwrite);

    return rval;
}

int DB::Add(const char* key, int len, const char* data, int data_len, bool overwrite)
{
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    MBData mbdata;
    mbdata.data_len = data_len;
    mbdata.buff = (uint8_t*) data;

    int rval;
    rval = dict->Add(reinterpret_cast<const uint8_t*>(key), len, mbdata, overwrite);

    mbdata.buff = NULL;
    return rval;
}

// Delete entry by key
int DB::Remove(const char *key, int len, MBData &data)
{
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    data.options |= OPTION_FIND_AND_DELETE;

    int rval;
    rval = dict->Remove(reinterpret_cast<const uint8_t*>(key), len, data);

    return rval;
}

int DB::Remove(const char *key, int len)
{
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    int rval;
    rval = dict->Remove(reinterpret_cast<const uint8_t*>(key), len);

    return rval;
}

int DB::RemoveAll()
{
    if(status != MBError::SUCCESS)
        return -1;

    int rval;
    rval = dict->RemoveAll();
    return rval;
}

int DB::Shrink(size_t min_index_shk_size, size_t min_data_shk_size)
{
    if(status != MBError::SUCCESS)
        return -1;
    MBShrink shr(*this);
    int rval = shr.Shrink(min_index_shk_size, min_data_shk_size);
    dict->PrintStats(Logger::GetLogStream());
    return rval;
}

int64_t DB::Count() const
{
    if(status != MBError::SUCCESS)
        return -1;

    return dict->Count();
}

void DB::PrintStats(std::ostream &out_stream) const
{
    if(status != MBError::SUCCESS)
        return;

    Logger::Log(LOG_LEVEL_INFO, "printing DB stats");
    dict->PrintStats(out_stream);
}

int DB::WrLock() const
{
    return MBLock::WrLock();
}

int DB::RdLock() const
{
    return MBLock::RdLock();
}

int DB::UnLock() const
{
    return MBLock::UnLock();
}

int DB::TryWrLock() const
{
    return MBLock::TryWrLock();
}

int DB::ClearLock() const
{
#ifdef __SHM_LOCK__
    // No db handler should hold mutex when this is called.
    return dict->InitShmMutex();
#else
    // Nothing needs to be done if we don't use shared memory mutex.
    return MBError::SUCCESS;
#endif
}

int DB::SetLogLevel(int level)
{
    return Logger::SetLogLevel(level);
}

Dict* DB::GetDictPtr() const
{
    return dict;
}

////////////////////
// DB iterator
////////////////////

const DB::iterator DB::begin() const
{
    DB::iterator iter = iterator(*this, DB_ITER_STATE_INIT);
    iter.init();

    return iter;
}

const DB::iterator DB::end() const
{
    return iterator(*this, DB_ITER_STATE_DONE);
}

DB::iterator::iter_node::iter_node(size_t offset, const std::string &ckey, size_t edge_off)
                       : node_off(offset),
                         key(ckey),
                         parent_edge_off(edge_off)
{
}

void DB::iterator::iter_obj_init()
{
    node_stack = NULL;

    if(state == DB_ITER_STATE_INIT)
    {
        state = DB_ITER_STATE_MORE;
        curr_key = "";
        node_stack = new MBlsq(free);
    }

    if(!(db_ref.dict->GetDBOptions() & ACCESS_MODE_WRITER))
    {
        // Only writer can iterate the DB since we don't enforce the lock.
        Logger::Log(LOG_LEVEL_WARN, "connector %u no permission for db iteration",
                    db_ref.identifier);
        // Set iterator state to DB_ITER_STATE_DONE
        state = DB_ITER_STATE_DONE;
    }
}

DB::iterator::iterator(const DB &db, int iter_state)
                     : db_ref(db), state(iter_state)
{
    iter_obj_init();
}

DB::iterator::iterator(const iterator &rhs)
                     : db_ref(rhs.db_ref), state(rhs.state)
{
    iter_obj_init();
}

DB::iterator::~iterator()
{
    if(node_stack)
        delete node_stack;
}

// Initialize the iterator, get the very first key-value pair.
void DB::iterator::init()
{
    int rval = db_ref.dict->ReadRootNode(node_buff, edge_ptrs, match, value);
    if(rval == MBError::SUCCESS)
    {
        if(next() == NULL)
            state = DB_ITER_STATE_DONE;
    }
    else
    {
        state = DB_ITER_STATE_DONE;
        Logger::Log(LOG_LEVEL_WARN, "failed to read root node for iterator");
    }
}

// Initialize the iterator, but do not get the first key-value pair.
// This is for internal use only.
int DB::iterator::Initialize()
{
    int rval = db_ref.dict->ReadRootNode(node_buff, edge_ptrs, match, value);
    if(rval != MBError::SUCCESS)
        state = DB_ITER_STATE_DONE;
    return rval;
}

const DB::iterator& DB::iterator::operator++()
{
    if(next() == NULL)
    {
        if(node_stack)
            delete node_stack;
        node_stack = NULL;
        state = DB_ITER_STATE_DONE;
    }
    return *this;
}

// This overloaded operator should only be used for iterator state check.
bool DB::iterator::operator!=(const iterator &rhs)
{
    return state != rhs.state;
}

// Find the next match using depth-first search.
// Example to use DB iterator
// for(DB::iterator iter = db.begin(); iter != db.end(); ++iter) {
//     std::cout << iter.key << "\n";
// }
DB::iterator* DB::iterator::next()
{
    int rval;
    size_t node_off;

    do {
        // Get the next edge in current node
        while((rval = db_ref.dict->ReadNextEdge(node_buff, edge_ptrs, match,
                      value, match_str, node_off)) == MBError::SUCCESS)
        {
            if(match)
            {
                key = curr_key + match_str;
                return this;
            }
            if(node_off > 0)
            {
                // Push the node to stack
                rval = node_stack->AddToHead(new iter_node(node_off, curr_key + match_str,
                                                           edge_ptrs.parent_offset));
                if(rval != MBError::SUCCESS)
                    throw rval;
            }
        }

        // All edges in current node have been iterated, need to pop the latest node.
        if(rval == MBError::OUT_OF_BOUND)
        {
            iter_node *inode = reinterpret_cast<iter_node*>(node_stack->RemoveFromHead());
            if(inode == NULL)
                break;

            rval = db_ref.dict->ReadNode(inode->node_off, node_buff, edge_ptrs, match, value);
            if(rval != MBError::SUCCESS)
                throw rval;

            if(match)
            {
                // for db shrinking
                edge_ptrs.curr_node_offset = inode->node_off;
                edge_ptrs.parent_offset = inode->parent_edge_off;

                delete inode;
                key = curr_key;
                return this;
            }

            curr_key = inode->key;
            delete inode;
        }
        else
        {
            throw rval;
        }
    } while(true);

    return NULL;
}

int DB::iterator::next_index_buffer(size_t &parent_node_off, struct _IndexNode *inp)
{
    int rval;
    size_t node_off;
    size_t parent_off;

    do {
        parent_off = edge_ptrs.offset;
        while((rval = db_ref.dict->ReadNextEdge(node_buff, edge_ptrs, match,
                      value, match_str, node_off)) == MBError::SUCCESS)
        {
            if(node_off > 0)
            {
                struct _IndexNode *inode = (struct _IndexNode *) calloc(sizeof(*inode), 1);
                if(inode == NULL)
                    throw (int) MBError::NO_MEMORY;
                inode->parent_off = parent_node_off;
                inode->node_edge_off = node_off;
                inode->rel_parent_off = (int) (parent_off + EDGE_NODE_LEADING_POS - parent_node_off);
                assert(inode->rel_parent_off > 0);
                rval = node_stack->AddToHead(inode);
                if(rval != MBError::SUCCESS)
                    throw rval;
            }

            if(edge_ptrs.len_ptr[0] > LOCAL_EDGE_LEN)
            {
                inp->node_edge_off = Get5BInteger(edge_ptrs.ptr);
                inp->parent_off = parent_node_off;
                inp->rel_parent_off = (int) (parent_off - parent_node_off);
                return BUFFER_TYPE_EDGE_STR;
            }

            parent_off = edge_ptrs.offset;
        }

        // All edges in current node have been iterated, need to pop the latest node.
        if(rval == MBError::OUT_OF_BOUND)
        {
            struct _IndexNode *inode = reinterpret_cast<struct _IndexNode*>(node_stack->RemoveFromHead());
            if(inode == NULL)
                break;

            parent_node_off = inode->node_edge_off;
            rval = db_ref.dict->ReadNode(inode->node_edge_off, node_buff, edge_ptrs, match, value);
            if(rval != MBError::SUCCESS)
                throw rval;
            
            inp->parent_off     = inode->parent_off;
            inp->node_edge_off  = inode->node_edge_off;
            inp->rel_parent_off = inode->rel_parent_off;

            free(inode);
            return BUFFER_TYPE_NODE;
        }
        else
        {
            throw rval;
        }
    } while(true);

    return BUFFER_TYPE_NONE;
}

} // namespace mabain
