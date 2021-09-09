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

// A simpler iterator interface for CGO
#ifndef __MB_ITER_H__
#define __MB_ITER_H__

#include "db.h"

namespace mabain {

class MBIterator
{
public:
    MBIterator(const DB &db);
    bool Next();
    bool HasKV();
    const char* GetKey();
    int GetKeyLen();
    const char* GetValue();
    int GetValueLen();
private:
    const DB &db_ref;
    DB::iterator iter;
};

}

#endif
