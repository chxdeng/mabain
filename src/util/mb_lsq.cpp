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

#include "mb_lsq.h"
#include "error.h"

namespace mabain {

MBlsq::MBlsq(void (*free_fn)(void *)) : FreeFunc(free_fn)
{
    head = NULL;
    tail = NULL;
    count = 0;
    peek_node = NULL;
}

void MBlsq::Clear()
{
    LSQ_Node *node;
    while(head != NULL)
    {
        node = head;
        head = head->next;
        if(FreeFunc)
            FreeFunc(node->data.data_ptr);
        free(node);
    }

    head = NULL;
    tail = NULL;
    count = 0;
}

MBlsq::~MBlsq()
{
    Clear();
}

uint64_t MBlsq::Count() const
{
    return count;
}

int MBlsq::AddToHead(void *ptr)
{
    LSQ_Node *node = (LSQ_Node *) malloc(sizeof(*node));
    if(node == NULL)
        return MBError::NO_MEMORY;

    node->data.data_ptr = ptr;
    node->next = head;
    head = node;
    if(count == 0)
        tail = node;

    count++;
    return MBError::SUCCESS;
}

int MBlsq::AddIntToHead(int64_t value)
{
    LSQ_Node *node = (LSQ_Node *) malloc(sizeof(*node));
    if(node == NULL)
        return MBError::NO_MEMORY;

    node->data.value = value;
    node->next = head;
    head = node;
    if(count == 0)
        tail = node;

    count++;
    return MBError::SUCCESS;
}

int MBlsq::AddIntToTail(int64_t value)
{
    LSQ_Node *node = (LSQ_Node *) malloc(sizeof(*node));
    if(node == NULL)
        return MBError::NO_MEMORY;

    node->data.value = value;
    node->next = NULL;
    if(tail)
    {
        tail->next = node;
        tail = node;
    }
    else
    {
        head = node;
        tail = node;
    }

    count++;
    return MBError::SUCCESS;
}

int MBlsq::AddToTail(void *ptr)
{
    LSQ_Node *node = (LSQ_Node *) malloc(sizeof(*node));
    if(node == NULL)
        return MBError::NO_MEMORY;

    node->data.data_ptr = ptr;
    node->next = NULL;
    if(tail != NULL)
    {
        tail->next = node;
    }
    else
    {
        head = node;
    }

    tail = node;
    count++;
    return MBError::SUCCESS;
}

void* MBlsq::RemoveFromHead()
{
    void *data;

    if(count > 1)
    {
        LSQ_Node *node = head;
        data = head->data.data_ptr;
        head = head->next;
        count--;
        free(node);
    }
    else if(count == 1)
    {
        LSQ_Node *node = head;
        data = head->data.data_ptr;
        head = NULL;
        tail = NULL;
        count = 0;
        free(node);
    }
    else
    {
        data = NULL;
    }

    return data;
}

int64_t MBlsq::RemoveIntFromHead()
{
    int64_t value;

    if(count > 1)
    {
        LSQ_Node *node = head;
        value = head->data.value;
        head = head->next;
        count--;
        free(node);
    }
    else if(count == 1)
    {
        LSQ_Node *node = head;
        value = head->data.value;
        head = NULL;
        tail = NULL;
        count = 0;
        free(node);
    }
    else
    {
        value = 0;
    }

    return value;
}

void MBlsq::PeekInit()
{
    peek_node = head;
}

bool MBlsq::PeekNextInt(int64_t &value)
{
    if(peek_node == NULL)
        return false;
    value = peek_node->data.value;
    peek_node = peek_node->next;
    return true;
}

}
