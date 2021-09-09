/**
 * Copyright (C) 2021 Cisco Inc.
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

#ifndef __ITERATOR_H__
#define __ITERATOR_H__

namespace mabain {

typedef struct _iterator_node
{
    std::string *key;
    uint8_t     *data;
    int          data_len;
    uint16_t     bucket_index;
    bool         match;
    bool         leaf_node;
} iterator_node;

}

#endif
