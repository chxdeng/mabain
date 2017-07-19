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

#ifndef __MBLSQ_H__
#define __MBLSQ_H__

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

namespace mabain {

typedef union _LSQ_Data
{
    void *data_ptr;
    int64_t value;
} LSQ_Data;
typedef struct _LSQ_Node
{
    LSQ_Data data;
    struct _LSQ_Node *next;
} LSQ_Node;

// mabain list/stack/queue class
class MBlsq
{
public:
    MBlsq(void (*free_fn)(void *));
    ~MBlsq();

    int      AddToHead(void *ptr); // Stack
    int      AddToTail(void *ptr); // Queue
    void    *RemoveFromHead();
    int      AddIntToHead(int64_t value);
    int      AddIntToTail(int64_t value);
    int64_t  RemoveIntFromHead();
    void     Clear();
    uint64_t Count() const;

    // Peek functions
    void     PeekInit();
    bool     PeekNextInt(int64_t &value);

private:
    LSQ_Node *head;
    LSQ_Node *tail;
    uint64_t count;
    void (*FreeFunc)(void *);

    // for peek
    const LSQ_Node *peek_node;
};

}

#endif
