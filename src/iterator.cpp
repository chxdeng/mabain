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

#include "db.h"
#include "dict.h"
#include "integer_4b_5b.h"
#include "mbt_base.h"
#include "detail/search_engine.h"

namespace mabain {

typedef struct _iterator_node {
    std::string* key;
    uint8_t* data;
    int data_len;
    uint16_t bucket_index;
} iterator_node;

static void free_iterator_node(void* n)
{
    if (n == NULL)
        return;

    iterator_node* inode = (iterator_node*)n;
    if (inode->key != NULL)
        delete inode->key;
    if (inode->data != NULL)
        free(inode->data);

    free(inode);
}

static iterator_node* new_iterator_node(const std::string& key, MBData* mbdata)
{
    iterator_node* inode = (iterator_node*)malloc(sizeof(*inode));
    if (inode == NULL)
        throw (int)MBError::NO_MEMORY;

    inode->key = new std::string(key);
    if (mbdata != NULL) {
        inode->bucket_index = mbdata->bucket_index;
        mbdata->TransferValueTo(inode->data, inode->data_len);
        if (inode->data == NULL || inode->data_len <= 0) {
            free_iterator_node(inode);
            inode = NULL;
        }
    } else {
        inode->data = NULL;
        inode->data_len = 0;
    }

    return inode;
}

/////////////////////////////////////////////////////////////////////
// DB iterator
// Example to use DB iterator
// for(DB::iterator iter = db.begin(); iter != db.end(); ++iter) {
//     std::cout << iter.key << "\n";
// }
// To match all pairs under a prefix subtree
// for(DB::iterator iter = db.begin(prefix); iter != db.end(); ++iter) {
//     std::cout << iter.key << "\n";
// }
/////////////////////////////////////////////////////////////////////

const DB::iterator DB::begin(bool check_async_mode, bool rc_mode) const
{
    DB::iterator iter = iterator(*this, DB_ITER_STATE_INIT);
    iter.prefix = "";
    if (rc_mode)
        iter.value.options |= CONSTS::OPTION_RC_MODE;
    iter.init(check_async_mode);

    return iter;
}

// iterator for all prefix match
const DB::iterator DB::begin(const std::string& prefix) const
{
    DB::iterator iter = iterator(*this, DB_ITER_STATE_INIT);
    iter.prefix = prefix;
    iter.init(true);
    return iter;
}

const DB::iterator DB::end() const
{
    return iterator(*this, DB_ITER_STATE_DONE);
}

void DB::iterator::iter_obj_init()
{
    node_stack = NULL;
    kv_per_node = NULL;
    lfree = NULL;

    if (!(db_ref.GetDBOptions() & CONSTS::ACCESS_MODE_WRITER)) {
#ifdef __LOCK_FREE__
        lfree = db_ref.dict->GetLockFreePtr();
#endif
    }

    if (state == DB_ITER_STATE_INIT)
        state = DB_ITER_STATE_MORE;
}

DB::iterator::iterator(const DB& db, int iter_state)
    : db_ref(db)
    , state(iter_state)
{
    iter_obj_init();
}

DB::iterator::iterator(const iterator& rhs)
    : db_ref(rhs.db_ref)
    , state(rhs.state)
{
    iter_obj_init();
}

DB::iterator::~iterator()
{
    if (node_stack != NULL)
        delete node_stack;
    if (kv_per_node != NULL)
        delete kv_per_node;
}

// Initialize the iterator, get the very first key-value pair.
void DB::iterator::init(bool check_async_mode)
{
    // Writer in async mode cannot be used for lookup
    if (check_async_mode && (db_ref.options & CONSTS::ASYNC_WRITER_MODE)) {
        state = DB_ITER_STATE_DONE;
        return;
    }

    node_stack = new MBlsq(free_iterator_node);
    kv_per_node = new MBlsq(free_iterator_node);

    load_kv_for_node("");
    if (next() == NULL)
        state = DB_ITER_STATE_DONE;
}

// Initialize the iterator, but do not get the first key-value pair.
// This is used for resource collection.
int DB::iterator::init_no_next()
{
    node_stack = new MBlsq(NULL);
    kv_per_node = NULL;

    int rval = db_ref.dict->ReadRootNode(node_buff, edge_ptrs, match, value);
    if (rval != MBError::SUCCESS)
        state = DB_ITER_STATE_DONE;
    return rval;
}

const DB::iterator& DB::iterator::operator++()
{
    if (next() == NULL)
        state = DB_ITER_STATE_DONE;

    return *this;
}

// This overloaded operator should only be used for iterator state check.
bool DB::iterator::operator!=(const iterator& rhs)
{
    return state != rhs.state;
}

// match the key with prefix or prefix with key
bool DB::iterator::match_prefix(const std::string& key)
{
    if (prefix.size() == 0 || key.size() == 0) {
        return true;
    }
    if (key.size() <= prefix.size()) {
        return memcmp(key.data(), prefix.data(), key.size()) == 0;
    }
    return memcmp(key.data(), prefix.data(), prefix.size()) == 0;
}

int DB::iterator::get_node_offset(const std::string& node_key,
    size_t& parent_edge_off,
    size_t& node_offset)
{
    int rval;

    node_offset = 0;
    value.options |= CONSTS::OPTION_FIND_AND_STORE_PARENT;
    detail::SearchEngine engine(*db_ref.dict);
    while (true) {
        rval = engine.find(reinterpret_cast<const uint8_t*>(node_key.data()),
            static_cast<int>(node_key.size()), value);
        if (rval != MBError::TRY_AGAIN)
            break;
        nanosleep((const struct timespec[]) { { 0, 10L } }, NULL);
    }

    if (rval == MBError::IN_DICT) {
        parent_edge_off = edge_ptrs.parent_offset;
        node_offset = Get6BInteger(value.edge_ptrs.offset_ptr);
        rval = MBError::SUCCESS;
    }
    return rval;
}

int DB::iterator::load_kvs(const std::string& curr_node_key,
    MBlsq* child_node_list)
{
    int rval;
    size_t child_node_off;
    std::string match_str;
    iterator_node* inode;

    while (true) {
        if (lfree == NULL) {
            rval = db_ref.dict->ReadNextEdge(node_buff, edge_ptrs, match, value,
                match_str, child_node_off);
        } else {
#ifdef __LOCK_FREE__
            LockFreeData snapshot;
            int lf_ret;
            size_t edge_off_prev = edge_ptrs.offset;
            lfree->ReaderLockFreeStart(snapshot);
#endif
            rval = db_ref.dict->ReadNextEdge(node_buff, edge_ptrs, match, value,
                match_str, child_node_off);
#ifdef __LOCK_FREE__
            lf_ret = lfree->ReaderLockFreeStop(snapshot, edge_off_prev, value);
            if (lf_ret == MBError::TRY_AGAIN)
                return lf_ret;
#endif
        }

        if (rval != MBError::SUCCESS)
            break;

        match_str = curr_node_key + match_str;
        if (match_prefix(match_str)) {
            if (child_node_off > 0) {
                inode = new_iterator_node(match_str, NULL);
                if (inode != NULL) {
                    rval = child_node_list->AddToTail(inode);
                    if (rval != MBError::SUCCESS) {
                        free_iterator_node(inode);
                        return rval;
                    }
                }
            }

            if (match != MATCH_NONE) {
                inode = new_iterator_node(match_str, &value);
                if (inode != NULL) {
                    rval = kv_per_node->AddToTail(inode);
                    if (rval != MBError::SUCCESS) {
                        free_iterator_node(inode);
                        return rval;
                    }
                }
            }
        }
    }

    if (rval == MBError::OUT_OF_BOUND)
        rval = MBError::SUCCESS;
    return rval;
}

int DB::iterator::load_node(const std::string& curr_node_key,
    size_t& parent_edge_off)
{
    int rval;

    if (curr_node_key.size() == 0) {
        rval = db_ref.dict->ReadRootNode(node_buff, edge_ptrs, match, value);
    } else {
        size_t node_offset;
        rval = get_node_offset(curr_node_key, parent_edge_off, node_offset);
        if (rval != MBError::SUCCESS)
            return rval;
        rval = db_ref.dict->ReadNode(node_offset, node_buff, edge_ptrs, match,
            value, false);
    }

    return rval;
}

int DB::iterator::load_kv_for_node(const std::string& curr_node_key)
{
    int rval;
    MBlsq child_node_list(free_iterator_node);
    size_t parent_edge_off;

    if (lfree == NULL) {
        rval = load_node(curr_node_key, parent_edge_off);
        if (rval == MBError::SUCCESS)
            rval = load_kvs(curr_node_key, &child_node_list);
    } else {
#ifdef __LOCK_FREE__
        LockFreeData snapshot;
        int lf_ret;
#endif
        while (true) {
#ifdef __LOCK_FREE__
            lfree->ReaderLockFreeStart(snapshot);
#endif
            rval = load_node(curr_node_key, parent_edge_off);
            if (rval == MBError::SUCCESS) {
                rval = load_kvs(curr_node_key, &child_node_list);
#ifdef __LOCK_FREE__
                if (rval == MBError::TRY_AGAIN) {
                    kv_per_node->Clear();
                    child_node_list.Clear();
                    continue;
                }
#endif
            }
#ifdef __LOCK_FREE__
            lf_ret = lfree->ReaderLockFreeStop(snapshot, parent_edge_off, value);
            if (lf_ret == MBError::TRY_AGAIN) {
                kv_per_node->Clear();
                child_node_list.Clear();
                continue;
            }
#endif
            break;
        }
    }

    if (rval == MBError::SUCCESS) {
        iterator_node* inode;
        while ((inode = (iterator_node*)child_node_list.RemoveFromHead())) {
            node_stack->AddToHead(inode);
        }
    } else {
        std::cerr << "failed to run ietrator: " << MBError::get_error_str(rval) << "\n";
        kv_per_node->Clear();
        child_node_list.Clear();
    }
    return rval;
}

// Find next iterator match
DB::iterator* DB::iterator::next()
{
    iterator_node* inode;

    while (kv_per_node->Count() == 0) {
        inode = (iterator_node*)node_stack->RemoveFromHead();
        if (inode == NULL)
            return NULL;

        int rval = load_kv_for_node(*inode->key);
        free_iterator_node(inode);
        if (rval != MBError::SUCCESS)
            return NULL;
    }

    if (kv_per_node->Count() > 0) {
        inode = (iterator_node*)kv_per_node->RemoveFromHead();
        match = MATCH_NODE_OR_EDGE;
        key = *inode->key;
        value.TransferValueFrom(inode->data, inode->data_len);
        value.bucket_index = inode->bucket_index;
        free_iterator_node(inode);
        return this;
    }

    return NULL;
}

// There is no need to perform lock-free check in next_dbt_buffer
// since it can only be called by writer.
bool DB::iterator::next_dbt_buffer(struct _DBTraverseNode* dbt_n)
{
    int rval;
    size_t node_off;
    size_t curr_edge_off;
    std::string match_str;

    memset(dbt_n, 0, sizeof(*dbt_n));
    do {
        curr_edge_off = edge_ptrs.offset;
        while ((rval = db_ref.dict->ReadNextEdge(node_buff, edge_ptrs, match,
                    value, match_str, node_off, false))
            == MBError::SUCCESS) {
            if (edge_ptrs.len_ptr[0] > LOCAL_EDGE_LEN) {
                dbt_n->edgestr_offset = Get5BInteger(edge_ptrs.ptr);
                dbt_n->edgestr_size = edge_ptrs.len_ptr[0] - 1;
                dbt_n->edgestr_link_offset = curr_edge_off;
                dbt_n->buffer_type |= BUFFER_TYPE_EDGE_STR;
            }

            if (node_off > 0) {
                dbt_n->node_offset = node_off;
                dbt_n->node_link_offset = curr_edge_off + EDGE_NODE_LEADING_POS;
                dbt_n->buffer_type |= BUFFER_TYPE_NODE;
                db_ref.dict->ReadNodeHeader(node_off, dbt_n->node_size, match, dbt_n->data_offset,
                    dbt_n->data_link_offset);
                if (match == MATCH_NODE)
                    dbt_n->buffer_type |= BUFFER_TYPE_DATA;
            } else if (match == MATCH_EDGE) {
                dbt_n->data_offset = Get6BInteger(edge_ptrs.offset_ptr);
                dbt_n->data_link_offset = curr_edge_off + EDGE_NODE_LEADING_POS;
                dbt_n->buffer_type |= BUFFER_TYPE_DATA;
            }

            if (dbt_n->buffer_type != BUFFER_TYPE_NONE) {
                dbt_n->edge_offset = curr_edge_off;
                return true;
            }

            curr_edge_off = edge_ptrs.offset;
        }

        if (rval == MBError::OUT_OF_BOUND) {
            node_off = (size_t)node_stack->RemoveIntFromHead();
            if (node_off == 0)
                break;
            rval = db_ref.dict->ReadNode(node_off, node_buff, edge_ptrs, match,
                value, false);
            if (rval != MBError::SUCCESS)
                throw rval;
        } else {
            throw rval;
        }
    } while (true);

    return false;
}

// Add an node offset to the iterator queue
void DB::iterator::add_node_offset(size_t node_offset)
{
    int rval = node_stack->AddIntToHead(node_offset);
    if (rval != MBError::SUCCESS)
        throw rval;
}

}
