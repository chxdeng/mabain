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
#include <stddef.h>
#include <unordered_map>

namespace mabain {

class MemoryManager {
public:
    MemoryManager();
    MemoryManager(void* addr, size_t size);
    ~MemoryManager();
    void Init(void* addr, size_t size);

    void* mb_malloc(size_t size);
    void mb_free(void* ptr);
    void mb_free(size_t offset);
    int mb_purge();
    size_t mb_allocated() const;
    static unsigned mb_get_num_arenas();
    static size_t mb_total_allocated();

    size_t get_shm_offset(void* ptr) { return static_cast<char*>(ptr) - static_cast<char*>(shm_addr); }

private:
    void configure_jemalloc();
    void* custom_extent_alloc(void* new_addr, size_t size, size_t alignment, bool* zero, bool* commit, unsigned arena_ind);
    static bool custom_extent_dalloc(extent_hooks_t* extent_hooks, void* addr, size_t size, bool committed, unsigned arena_ind);
    static bool custom_extent_commit(extent_hooks_t* extent_hooks, void* addr, size_t size, size_t offset, size_t length, unsigned arena_ind);
    static bool custom_extent_decommit(extent_hooks_t* extent_hooks, void* addr, size_t size, size_t offset, size_t length, unsigned arena_ind);
    static bool custom_extent_purge_lazy(extent_hooks_t* extent_hooks, void* addr, size_t size, size_t offset, size_t length, unsigned arena_ind);
    static bool custom_extent_purge_forced(extent_hooks_t* extent_hooks, void* addr, size_t size, size_t offset, size_t length, unsigned arena_ind);
    static bool custom_extent_split(extent_hooks_t* extent_hooks, void* addr, size_t size, size_t size_a, size_t size_b, bool committed, unsigned arena_ind);
    static bool custom_extent_merge(extent_hooks_t* extent_hooks, void* addr_a, size_t size_a, void* addr_b, size_t size_b, bool committed, unsigned arena_ind);

    void* shm_addr;
    size_t shm_size;
    size_t shm_offset;

    extent_hooks_t* extent_hooks;
    unsigned arena_index;

    static std::unordered_map<unsigned, MemoryManager*> arena_manager_map;
};
}

#endif