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

#include "mb_iter.h"

namespace mabain {

MBIterator::MBIterator(const DB &db) : db_ref(db), iter(db_ref.begin())
{
}

const char* MBIterator::GetKey()
{
    return iter.key.c_str();
}

int MBIterator::GetKeyLen()
{
    return (int) iter.key.size();
}

const char* MBIterator::GetValue()
{
    return (const char*) iter.value.buff;
}

int MBIterator::GetValueLen()
{
    return iter.value.data_len;
}

bool MBIterator::HasKV()
{
    return iter != db_ref.end();
}

bool MBIterator::Next()
{
    ++iter;
    if (iter != db_ref.end()) {
        return true;
    }
    return false;
}

}
