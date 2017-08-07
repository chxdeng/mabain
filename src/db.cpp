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
#include "async_writer.h"

namespace mabain {

// Current mabain version 1.0.0
uint16_t version[4] = {1, 0, 0, 0};

DB::~DB()
{
    if(async_writer != NULL)
        delete async_writer;
}

int DB::Close()
{
    int rval = MBError::SUCCESS;
    int db_opts = dict->GetDBOptions();

    if(async_writer != NULL)
    {
        async_writer->StopAsyncThread();
    }

    if(dict != NULL)
    {
        if(!(db_opts & CONSTS::NO_GLOBAL_INIT))
        {
            if(db_opts & CONSTS::ACCESS_MODE_WRITER)
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
    if((db_opts & CONSTS::ACCESS_MODE_WRITER) && !(db_opts & CONSTS::NO_GLOBAL_INIT))
        Logger::Close();
    return rval;
}

int DB::UpdateNumHandlers(int mode, int delta)
{
    int rval = MBError::SUCCESS;

    WrLock();

    if(mode & CONSTS::ACCESS_MODE_WRITER)
        rval = dict->UpdateNumWriter(delta);
    else
        dict->UpdateNumReader(delta);

    UnLock();

    return rval;
}

// Constructor for initializing DB handle
DB::DB(const std::string &db_path, int db_options, size_t memcap_index, size_t memcap_data,
       int data_size, uint32_t id)
{
    status = MBError::NOT_INITIALIZED;
    dict = NULL;
    async_writer = NULL;

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

    if((db_options & CONSTS::ACCESS_MODE_WRITER))
        Logger::InitLogFile(db_path_tmp + "mabain.log");
    else
        Logger::SetLogLevel(LOG_LEVEL_WARN);
    Logger::Log(LOG_LEVEL_INFO, "connector %u DB options: %d", id, db_options);

    // Check if DB exist. This can be done by check existence of the first index file.
    // If this is the first time the DB is opened and it is in writer mode, then we
    // need to update the header for the first time. If only reader access mode is
    // required and the file does not exist, we should bail here and the DB open will
    // not be successful.
    bool init_header = false;
    std::string index_file_0 = db_path_tmp + "_mabain_i0";
    if(access(index_file_0.c_str(), R_OK))
    {
        if(db_options & CONSTS::ACCESS_MODE_WRITER)
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

    dict = new Dict(db_path_tmp, init_header, data_size, db_options, memcap_index,
                    memcap_data, db_options & CONSTS::USE_SLIDING_WINDOW,
                    db_options & CONSTS::SYNC_ON_WRITE);

    if((db_options & CONSTS::ACCESS_MODE_WRITER) && init_header)
    {
        Logger::Log(LOG_LEVEL_INFO, "open a new db %s", db_path_tmp.c_str());
        dict->Init(identifier);
#ifdef __SHM_LOCK__
        dict->InitShmMutex();
#endif
    }

    if(dict->Status() != MBError::SUCCESS)
    {
        Logger::Log(LOG_LEVEL_ERROR, "failed to iniitialize dict: %s ",
                    MBError::get_error_str(dict->Status()));
        return;
    }

    if(!(db_options & CONSTS::NO_GLOBAL_INIT))
    {
        lock.Init(dict->GetShmLockPtrs());
        status = UpdateNumHandlers(db_options, 1);
        if(status != MBError::SUCCESS)
        {
            Logger::Log(LOG_LEVEL_ERROR, "failed to initialize db: %s",
                        MBError::get_error_str(dict->Status()));
            return;
        }
    }

    if(db_options & CONSTS::ACCESS_MODE_WRITER)
    {
        if(db_options & CONSTS::ASYNC_WRITER_MODE)
        {
            try {
                async_writer = new AsyncWriter(this);
            } catch (int error) {
                if(async_writer != NULL) delete async_writer;
                async_writer = NULL;
                Logger::Log(LOG_LEVEL_ERROR, "failed to start async writer thread");
            }
        }
        if(async_writer == NULL)
            Logger::Log(LOG_LEVEL_INFO, "async writer was disabled");
    }

    Logger::Log(LOG_LEVEL_INFO, "connector %u successfully opened DB %s for %s",
                identifier, db_path.c_str(),
                (db_options & CONSTS::ACCESS_MODE_WRITER) ? "writing":"reading");
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

int DB::Find(const std::string &key, MBData &mdata) const
{
    return Find(key.data(), key.size(), mdata);
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

int DB::FindLongestPrefix(const std::string &key, MBData &data) const
{
    return FindLongestPrefix(key.data(), key.size(), data);
}

// Add a key-value pair
int DB::Add(const char* key, int len, MBData &mbdata, bool overwrite)
{
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    if(async_writer != NULL)
        return async_writer->Add(key, len, reinterpret_cast<const char *>(mbdata.buff),
                                 mbdata.data_len, overwrite);

    int rval;
    rval = dict->Add(reinterpret_cast<const uint8_t*>(key), len, mbdata, overwrite);

    return rval;
}

int DB::Add(const char* key, int len, const char* data, int data_len, bool overwrite)
{
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    if(async_writer != NULL)
        return async_writer->Add(key, len, data, data_len, overwrite);

    MBData mbdata;
    mbdata.data_len = data_len;
    mbdata.buff = (uint8_t*) data;

    int rval;
    rval = dict->Add(reinterpret_cast<const uint8_t*>(key), len, mbdata, overwrite);

    mbdata.buff = NULL;
    return rval;
}

int DB::Add(const std::string &key, const std::string &value, bool overwrite)
{
    return Add(key.data(), key.size(), value.data(), value.size(), overwrite);
}

// Delete entry by key
int DB::Remove(const char *key, int len, MBData &data)
{
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    if(async_writer != NULL)
        return async_writer->Remove(key, len);

    data.options |= CONSTS::OPTION_FIND_AND_STORE_PARENT;

    int rval;
    rval = dict->Remove(reinterpret_cast<const uint8_t*>(key), len, data);

    return rval;
}

int DB::Remove(const char *key, int len)
{
    if(status != MBError::SUCCESS)
        return MBError::NOT_INITIALIZED;

    if(async_writer != NULL)
        return async_writer->Remove(key, len);

    int rval;
    rval = dict->Remove(reinterpret_cast<const uint8_t*>(key), len);

    return rval;
}

int DB::Remove(const std::string &key)
{
    return Remove(key.data(), key.size());
}

int DB::RemoveAll()
{
    if(status != MBError::SUCCESS)
        return -1;

    if(async_writer != NULL)
        return async_writer->RemoveAll();

    int rval;
    rval = dict->RemoveAll();
    return rval;
}

int DB::Shrink(size_t min_index_shk_size, size_t min_data_shk_size)
{
    if(status != MBError::SUCCESS)
        return -1;

    if(async_writer != NULL)
        return async_writer->Shrink(min_index_shk_size, min_data_shk_size);

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

int DB::WrLock()
{
    return lock.WrLock();
}

int DB::RdLock()
{
    return lock.RdLock();
}

int DB::UnLock()
{
    return lock.UnLock();
}

int DB::TryWrLock()
{
    return lock.TryWrLock();
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

/////////////////////////////////////////////////////////////////////
// DB iterator
// Example to use DB iterator
// for(DB::iterator iter = db.begin(); iter != db.end(); ++iter) {
//     std::cout << iter.key << "\n";
// }
/////////////////////////////////////////////////////////////////////

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

DB::iterator::iter_node::iter_node(size_t offset, const std::string &ckey, size_t edge_off, uint32_t counter)
                       : node_off(offset),
                         key(ckey),
                         parent_edge_off(edge_off),
                         node_counter(counter)
{
}

void DB::iterator::iter_obj_init()
{
    node_stack = NULL;
#ifdef __LOCK_FREE__
    lfree = db_ref.dict->GetLockFreePtr();
#endif

    if(state == DB_ITER_STATE_INIT)
    {
        state = DB_ITER_STATE_MORE;
        curr_key = "";
        node_stack = new MBlsq(free);
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
#ifdef __LOCK_FREE__
    curr_node_offset = 0;
    curr_node_counter = lfree->LoadCounter();
#endif

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

int DB::iterator::node_offset_modified(const std::string &key, size_t node_off, MBData &mbd)
{
    int rval;
    mbd.options |= CONSTS::OPTION_FIND_AND_STORE_PARENT;
    mbd.edge_ptrs.curr_node_offset = 0;
    while(true)
    {
        rval = db_ref.dict->Find((const uint8_t *)key.c_str(), key.size(), mbd);
        if(rval != MBError::TRY_AGAIN)
            break;
        nanosleep((const struct timespec[]){{0, 10L}}, NULL);
    }

    if(rval == MBError::IN_DICT)
    {
        mbd.edge_ptrs.curr_node_offset = Get6BInteger(mbd.edge_ptrs.offset_ptr);
        if(node_off != mbd.edge_ptrs.curr_node_offset)
            return MBError::NODE_OFF_CHANGED;
    }

    return rval;
}

int DB::iterator::edge_offset_modified(const std::string &key, size_t edge_off, MBData &mbd)
{
    int rval;
    mbd.options |= CONSTS::OPTION_FIND_AND_STORE_PARENT;
    while(true)
    {
        rval = db_ref.dict->Find((const uint8_t *)key.c_str(), key.size(), mbd);
        if(rval != MBError::TRY_AGAIN)
            break;
        nanosleep((const struct timespec[]){{0, 10L}}, NULL);
    }

    if(rval == MBError::IN_DICT)
    {
        if(edge_off != mbd.edge_ptrs.offset)
            return MBError::EDGE_OFF_CHANGED;
    }
        
    return rval;
}

// Find the next match using depth-first search.
DB::iterator* DB::iterator::next()
{
    int rval;
    size_t node_off;
#ifdef __LOCK_FREE__
    LockFreeData snapshot;
    int lf_ret;
    size_t edge_off_prev;
    int nt_prev;
    MBData mbd;
#endif
    uint32_t node_counter = 0;

    do {
        while(true)
        {
#ifdef __LOCK_FREE__
            edge_off_prev = edge_ptrs.offset;
            nt_prev = edge_ptrs.curr_nt;
            lfree->ReaderLockFreeStart(snapshot);
#endif
            // Get the next edge in current node
            rval = db_ref.dict->ReadNextEdge(node_buff, edge_ptrs, match, value, match_str, node_off);
#ifdef __LOCK_FREE__
            lf_ret = lfree->ReaderLockFreeStop(snapshot, edge_off_prev);
            if(lf_ret == MBError::TRY_AGAIN)
            {
                edge_ptrs.offset = edge_off_prev;
                edge_ptrs.curr_nt = nt_prev;
                nanosleep((const struct timespec[]){{0, 10L}}, NULL);
                continue;
            }
#endif
            if(rval != MBError::SUCCESS)
                break;

#ifdef __LOCK_FREE__
            if(lfree->ReaderValidateNodeOffset(curr_node_counter, curr_node_offset, node_counter))
            {
                lf_ret = edge_offset_modified(curr_key+match_str, edge_off_prev, mbd);
                if(lf_ret == MBError::EDGE_OFF_CHANGED)
                {
                    // prepare for the next edge with updated offset
                    edge_ptrs.offset = mbd.edge_ptrs.offset + EDGE_SIZE;
                    edge_ptrs.curr_nt++;
                }
                else if(lf_ret == MBError::NOT_EXIST)
                {
                    // go to next edge
                    match = false;
                    node_off = 0;    
                }
            }
#endif

            if(match)
            {
                key = curr_key + match_str;
                return this;
            }
            if(node_off > 0)
            {
                // Push the node to stack
                rval = node_stack->AddToHead(new iter_node(node_off, curr_key + match_str,
                                                           edge_ptrs.parent_offset, node_counter));
                if(rval != MBError::SUCCESS)
                    throw rval;
            }
        }

        // All edges in current node have been iterated, need to pop the latest node.
        if(rval == MBError::OUT_OF_BOUND)
        {
            iter_node *inode;
            while(true)
            {
                inode = reinterpret_cast<iter_node*>(node_stack->RemoveFromHead());
                if(inode == NULL)
                    return NULL;

                rval = db_ref.dict->ReadNode(inode->node_off, node_buff, edge_ptrs, match, value);

#ifdef __LOCK_FREE__
                if(lfree->ReaderValidateNodeOffset(inode->node_counter, inode->node_off, node_counter))
                {
                    lf_ret = node_offset_modified(inode->key, inode->node_off, mbd);
                    // retrieve the next node
                    if(lf_ret != MBError::IN_DICT)
                    {
                        if(lf_ret == MBError::NODE_OFF_CHANGED)
                        {
                            inode->node_off = mbd.edge_ptrs.curr_node_offset;
                            inode->node_counter = node_counter;
                            // Update the node with current offset and push it back to the stack
                            rval = node_stack->AddToHead(inode);
                            if(rval != MBError::SUCCESS)
                                throw rval;
                        }
                        continue;
                    }

                    curr_node_counter = node_counter;
                    curr_node_offset = inode->node_off;
                }
#endif

                break;
            }

            if(rval != MBError::SUCCESS)
                throw rval;

            curr_key = inode->key;
            if(match)
            {
                key = curr_key;
                // for db shrinking
                edge_ptrs.curr_node_offset = inode->node_off;
                edge_ptrs.parent_offset = inode->parent_edge_off;
                delete inode;
                return this;
            }

            delete inode;
        }
        else
        {
            throw rval;
        }
    } while(true);

    return NULL;
}

// There is no need to perform lock-free check in next_index_buffer since it can only be called
// by writer.
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
