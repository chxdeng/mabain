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

#ifndef __APPEND_H__
#define __APPEND_H__

#include "mb_data.h"
#include "dict.h"

namespace mabain {

class Append {
public:
    Append(Dict &dict_ref, EdgePtrs &eptrs);
    ~Append();
    int AddDataBuffer(const uint8_t *buff, int buff_len);
    bool IsExistingKey() const;
    size_t GetOldDataOffset() const;

private:
    int AppendDataBuffer(size_t &data_offset, const uint8_t *buff, int buff_len);
    int AddDataToLeafNode(const uint8_t *buff, int buff_len);

    Dict &dict;
    EdgePtrs &edge_ptrs;
    bool existing_key;
    size_t old_data_offset; // if existing_key is true
};

}

#endif
