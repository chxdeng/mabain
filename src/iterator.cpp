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

namespace mabain {

typedef struct _iterator_node
{
    std::string *key;
    uint8_t     *data;
    int          data_len;
} iterator_node;

static void free_iterator_node(void *n)
{
    if(n == NULL)
        return;

    iterator_node *inode = (iterator_node *) n;
    if(inode->key != NULL)
        delete inode->key;
    if(inode->data != NULL)
        free(inode->data);

    free(inode);
}

static iterator_node* new_iterator_node(const std::string &key, MBData *mbdata)
{
    iterator_node *inode = (iterator_node *) malloc(sizeof(*inode));
    if(inode == NULL)
        throw (int) MBError::NO_MEMORY;

    inode->key = new std::string(key);
    if(mbdata != NULL)
    {
        mbdata->TransferValueTo(inode->data, inode->data_len);
        if(inode->data == NULL || inode->data_len <= 0)
        {
            free_iterator_node(inode);
            inode = NULL;
        }
    }
    else
    {
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

void DB::iterator::iter_obj_init()
{
    node_stack = NULL;
    kv_per_node = NULL;

#ifdef __LOCK_FREE__
    lfree = db_ref.dict->GetLockFreePtr();
#endif

    if(state == DB_ITER_STATE_INIT)
        state = DB_ITER_STATE_MORE;
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
    if(node_stack != NULL)
        delete node_stack;
    if(kv_per_node != NULL)
        delete kv_per_node;
}

// Initialize the iterator, get the very first key-value pair.
void DB::iterator::init()
{
    node_stack = new MBlsq(free_iterator_node);
    kv_per_node = new MBlsq(free_iterator_node);

    load_kv_for_node("");
    if(next() == NULL)
        state = DB_ITER_STATE_DONE;
}

// Initialize the iterator, but do not get the first key-value pair.
// This is used for resource collection.
int DB::iterator::init_no_next()
{
    node_stack = new MBlsq(NULL);
    kv_per_node = NULL;

    int rval = db_ref.dict->ReadRootNode(node_buff, edge_ptrs, match, value);
    if(rval != MBError::SUCCESS)
        state = DB_ITER_STATE_DONE;
    return rval;
}

const DB::iterator& DB::iterator::operator++()
{
    try
    {
        if(next() == NULL)
            state = DB_ITER_STATE_DONE;
    }
    catch (int error)
    {
        std::cerr << "iterator next() exception: "
                  << MBError::get_error_str(error) << "\n";
        state = DB_ITER_STATE_DONE;
    }

    return *this;
}

// This overloaded operator should only be used for iterator state check.
bool DB::iterator::operator!=(const iterator &rhs)
{
    return state != rhs.state;
}

int DB::iterator::get_node_offset(const std::string &node_key,
                                  size_t &node_offset)
{
    int rval;

    node_offset = 0;
    value.options |= CONSTS::OPTION_FIND_AND_STORE_PARENT;
    while(true)
    {
        rval = db_ref.dict->Find((const uint8_t *)node_key.c_str(),
                                 node_key.size(), value);
        if(rval != MBError::TRY_AGAIN)
            break;
        nanosleep((const struct timespec[]){{0, 10L}}, NULL);
    }

    if(rval == MBError::IN_DICT)
    {
        node_offset = Get6BInteger(value.edge_ptrs.offset_ptr);
        rval = MBError::SUCCESS;
    }
    return rval;
}

int DB::iterator::load_kvs(const std::string &curr_node_key,
                           size_t &curr_node_offset,
                           MBlsq *child_node_list)
{
    int rval;
    size_t child_node_off;
    std::string match_str;
    iterator_node *inode;
    size_t edge_off_prev = 0;
#ifdef __LOCK_FREE__
    LockFreeData snapshot;
    int lf_ret;
    int nt_prev;
#endif

    while(true)
    {
#ifdef __LOCK_FREE__
        if(edge_off_prev == 0)
            rval = load_node(curr_node_key, curr_node_offset);
        edge_off_prev = edge_ptrs.offset;
        nt_prev = edge_ptrs.curr_nt;
        lfree->ReaderLockFreeStart(snapshot);
#else
        if(edge_off_prev == 0)
        {
            rval = load_node(curr_node_key, curr_node_offset);
            edge_off_prev = edge_ptrs.offset;
        }
#endif
        rval = db_ref.dict->ReadNextEdge(node_buff, edge_ptrs, match, value,
                            match_str, child_node_off);
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

        if(child_node_off > 0)
        {
            inode = new_iterator_node(curr_node_key + match_str, NULL);
            if(inode != NULL)
            {
                rval = child_node_list->AddToTail(inode);
                if(rval != MBError::SUCCESS)
                {
                    free_iterator_node(inode);
                    return rval;
                }
            }
        }

        if(match != MATCH_NONE)
        {
            inode = new_iterator_node(curr_node_key + match_str, &value);
            if(inode != NULL)
            {
                rval = kv_per_node->AddToTail(inode);
                if(rval != MBError::SUCCESS)
                {
                    free_iterator_node(inode);
                    return rval;
                }
            }
        }
    }

    if(rval == MBError::OUT_OF_BOUND)
        rval = MBError::SUCCESS;
    return rval;
}

int DB::iterator::load_node(const std::string &curr_node_key,
                            size_t &curr_node_offset)
{
    int rval;

    if(curr_node_key.size() == 0)
    {
        // root node offset never changes, it is OK to arbitrarily use zero here.
         curr_node_offset = 0;
         rval = db_ref.dict->ReadRootNode(node_buff, edge_ptrs, match, value);
    }
    else
    {
        rval = get_node_offset(curr_node_key, curr_node_offset);
        if(rval != MBError::SUCCESS)
            return rval;
        rval = db_ref.dict->ReadNode(curr_node_offset, node_buff, edge_ptrs,
                                     match, value, false);
    }

    return rval;
}

int DB::iterator::load_kv_for_node(const std::string &curr_node_key)
{
    int rval;
    size_t curr_node_offset;
    MBlsq child_node_list(free_iterator_node);

    while(true)
    {
        rval = load_kvs(curr_node_key, curr_node_offset, &child_node_list);

#ifdef __LOCK_FREE__
        if(curr_node_offset != 0)
        {
            size_t new_node_offset;
            int lf_ret;
            lf_ret = get_node_offset(curr_node_key, new_node_offset);    
            if(lf_ret == MBError::SUCCESS && new_node_offset != curr_node_offset)
            {
                // Reload node
                kv_per_node->Clear();
                curr_node_offset = new_node_offset;
                child_node_list.Clear();
                continue;
            }
            else if(lf_ret != MBError::SUCCESS)
            {
                rval = lf_ret;
            }
        }
#endif

        break;
    }

    if(rval == MBError::SUCCESS)
    {
        iterator_node *inode;
        while((inode = (iterator_node *) child_node_list.RemoveFromHead()))
        {
            node_stack->AddToHead(inode);
        }
    }
    else
    {
        child_node_list.Clear();
    }
    return rval;
}

// Find next iterator match
DB::iterator* DB::iterator::next()
{
    iterator_node *inode;

    while(kv_per_node->Count() == 0)
    {
        inode = (iterator_node *) node_stack->RemoveFromHead();
        if(inode == NULL)
            return NULL;

        int rval = load_kv_for_node(*inode->key);
        free_iterator_node(inode);
        if(rval != MBError::SUCCESS)
            return NULL;
    }

    if(kv_per_node->Count() > 0)
    {
        inode = (iterator_node *) kv_per_node->RemoveFromHead();
        match = MATCH_NODE_OR_EDGE;
        key = *inode->key;
        value.TransferValueFrom(inode->data, inode->data_len);
        free_iterator_node(inode);
        return this;
    }

    return NULL;
}

// There is no need to perform lock-free check in next_dbt_buffer
// since it can only be called by writer.
bool DB::iterator::next_dbt_buffer(struct _DBTraverseNode *dbt_n)
{
    int rval;
    size_t node_off;
    size_t curr_edge_off;
    std::string match_str;

    memset(dbt_n, 0, sizeof(*dbt_n));
    do {
        curr_edge_off = edge_ptrs.offset;
        while((rval = db_ref.dict->ReadNextEdge(node_buff, edge_ptrs, match,
                      value, match_str, node_off, false)) == MBError::SUCCESS)
        {
            if(edge_ptrs.len_ptr[0] > LOCAL_EDGE_LEN)
            {
                dbt_n->edgestr_offset       = Get5BInteger(edge_ptrs.ptr);
                dbt_n->edgestr_size         = edge_ptrs.len_ptr[0] - 1;
                dbt_n->edgestr_link_offset  = curr_edge_off;
                dbt_n->buffer_type         |= BUFFER_TYPE_EDGE_STR;
            }

            if(node_off > 0)
            {
                dbt_n->node_offset         = node_off;
                dbt_n->node_link_offset    = curr_edge_off + EDGE_NODE_LEADING_POS;
                dbt_n->buffer_type        |= BUFFER_TYPE_NODE;
                db_ref.dict->ReadNodeHeader(node_off, dbt_n->node_size, match, dbt_n->data_offset,
                                            dbt_n->data_link_offset);
                if(match == MATCH_NODE)
                    dbt_n->buffer_type |= BUFFER_TYPE_DATA;
            }
            else if(match == MATCH_EDGE)
            {
                dbt_n->data_offset      = Get6BInteger(edge_ptrs.offset_ptr);
                dbt_n->data_link_offset = curr_edge_off + EDGE_NODE_LEADING_POS;
                dbt_n->buffer_type     |= BUFFER_TYPE_DATA;
            }

            if(dbt_n->buffer_type != BUFFER_TYPE_NONE)
            {
                dbt_n->edge_offset = curr_edge_off;
                return true;
            }

            curr_edge_off = edge_ptrs.offset;
        }

        if(rval == MBError::OUT_OF_BOUND)
        {
            node_off = (size_t ) node_stack->RemoveIntFromHead();
            if(node_off == 0)
                break;
            rval = db_ref.dict->ReadNode(node_off, node_buff, edge_ptrs, match,
                                         value, false);
            if(rval != MBError::SUCCESS)
                throw rval;
        }
        else
        {
            throw rval;
        }
    } while(true);

    return false;
}

// Add an node offset to the iterator queue
void DB::iterator::add_node_offset(size_t node_offset)
{
    int rval = node_stack->AddIntToHead(node_offset);
    if(rval != MBError::SUCCESS)
        throw rval; 
}

}
