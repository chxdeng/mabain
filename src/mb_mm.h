/**
 * Copyright (C) 2025 Cisco Inc.
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

#ifndef __MB_MM_H__
#define __MB_MM_H__

#include <jemalloc/jemalloc.h>

namespace mabain {

class MemoryManagerMetadata {
public:
    MemoryManagerMetadata()
        : alloc_size(0)
        , extent_hooks(nullptr)
        , arena_index(0)
    {
        extent_hooks = new extent_hooks_t();
    }
    ~MemoryManagerMetadata()
    {
        if (extent_hooks != nullptr) {
            delete extent_hooks;
        }
    }

    size_t alloc_size;
    extent_hooks_t* extent_hooks;
    unsigned arena_index;
};

}

#endif
