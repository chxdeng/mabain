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

#include <string.h>
#include <sys/mman.h>

#include "error.h"
#include "logger.h"
#include "mb_mm.h"

namespace mabain {

std::unordered_map<unsigned, MemoryManager*> MemoryManager::arena_manager_map;

MemoryManager::MemoryManager()
    : shm_addr(nullptr)
    , shm_size(0)
    , shm_offset(0)
{
    configure_jemalloc();
}

MemoryManager::MemoryManager(void* addr, size_t size)
    : shm_addr(addr)
    , shm_size(size)
    , shm_offset(0)
{
    configure_jemalloc();
}

MemoryManager::~MemoryManager()
{
    if (extent_hooks != nullptr) {
        delete extent_hooks;
    }
    std::string arena_destroy = "arena." + std::to_string(arena_index) + ".destroy";
    int rc = mallctl(arena_destroy.c_str(), nullptr, nullptr, nullptr, 0);
    if (rc != 0) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to destroy jemalloc arena %u, error code %d", arena_index, rc);
    }
}

void MemoryManager::Init(void* addr, size_t size)
{
    shm_addr = addr;
    shm_size = size;
}

void* MemoryManager::mb_malloc(size_t size)
{
    void* ptr = nullptr;
    if (size > 0) {
        ptr = mallocx(size, MALLOCX_ARENA(arena_index) | MALLOCX_TCACHE_NONE);
    }
    return ptr;
}

void MemoryManager::mb_free(void* ptr)
{
    if (ptr != nullptr) {
        dallocx(ptr, MALLOCX_ARENA(arena_index) | MALLOCX_TCACHE_NONE);
    }
}

void MemoryManager::mb_free(size_t offset)
{
    void* ptr = static_cast<char*>(shm_addr) + offset;
    dallocx(ptr, MALLOCX_ARENA(arena_index) | MALLOCX_TCACHE_NONE);
}

int MemoryManager::mb_purge()
{
    std::string arena_purge = "arena." + std::to_string(arena_index) + ".purge";
    if (mallctl(arena_purge.c_str(), nullptr, nullptr, nullptr, 0) != 0) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to perform jemalloc purge");
        return MBError::JEMALLOC_ERROR;
    }
    return MBError::SUCCESS;
}

size_t MemoryManager::mb_allocated() const
{
    size_t allocated;
    size_t sz = sizeof(allocated);
    std::string arena_allocated = "stats.arenas." + std::to_string(arena_index) + ".allocated";

    // Query the allocated memory for the specific arena
    int rc = mallctl(arena_allocated.c_str(), &allocated, &sz, nullptr, 0);
    if (rc != 0) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to query allocated memory for arena %u, error code %d",
            arena_index, rc);
        return 0;
    }
    return allocated;
}

unsigned MemoryManager::mb_get_num_arenas()
{
    unsigned narenas;
    size_t sz = sizeof(narenas);

    if (mallctl("arenas.narenas", &narenas, &sz, nullptr, 0) != 0) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to query number of arenas");
        return 0;
    }
    return narenas;
}

size_t MemoryManager::mb_total_allocated()
{
    size_t allocated;
    size_t sz = sizeof(allocated);
    std::string arena_allocated = "stats.allocated";
    int rc = mallctl(arena_allocated.c_str(), &allocated, &sz, nullptr, 0);
    if (rc != 0) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to query allocated memory error code %d", rc);
        return 0;
    }
    return allocated;
}

void MemoryManager::configure_jemalloc()
{
    extent_hooks = new extent_hooks_t();
    extent_hooks->alloc = [](extent_hooks_t* extent_hooks, void* addr, size_t size, size_t alignment, bool* zero, bool* commit, unsigned arena_ind) -> void* {
        return arena_manager_map[arena_ind]->custom_extent_alloc(addr, size, alignment, zero, commit, arena_ind);
    };
    extent_hooks->dalloc = custom_extent_dalloc;
    extent_hooks->commit = custom_extent_commit;
    extent_hooks->decommit = custom_extent_decommit;
    extent_hooks->purge_lazy = custom_extent_purge_lazy;
    extent_hooks->purge_forced = custom_extent_purge_forced;
    extent_hooks->split = custom_extent_split;
    // use default merge function
    extent_hooks->merge = nullptr;

    size_t hooks_sz = sizeof(extent_hooks);
    unsigned arena_ind;
    size_t arena_index_sz = sizeof(arena_ind);

    if (mallctl("arenas.create", &arena_ind, &arena_index_sz, nullptr, 0) != 0) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to create jemalloc arena");
        return;
    }
    Logger::Log(LOG_LEVEL_INFO, "jemalloc arena index: %u created", arena_ind);

    std::string arena_hooks = "arena." + std::to_string(arena_ind) + ".extent_hooks";
    if (mallctl(arena_hooks.c_str(), nullptr, nullptr, &extent_hooks, hooks_sz) != 0) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to set jemalloc extent hooks");
        return;
    }

    // Store the MemoryManager instance in the global map
    arena_manager_map[arena_ind] = this;

    this->arena_index = arena_ind;
}

void* MemoryManager::custom_extent_alloc(void* new_addr, size_t size, size_t alignment, bool* zero, bool* commit, unsigned arena_ind)
{
    MemoryManager* manager = arena_manager_map[arena_ind];

    void* ptr = nullptr;
    size_t aligned_offset;

    if (new_addr != nullptr) {
        ptr = new_addr;
        aligned_offset = manager->get_shm_offset(ptr);
        if (aligned_offset + size > manager->shm_size) {
            Logger::Log(LOG_LEVEL_WARN, "custom_extent_alloc: failed to extend new memory (offset: %zu, size: %zu, shm_size: %zu)",
                aligned_offset, size, manager->shm_size);
            return nullptr; // Return nullptr to indicate allocation failure
        }
    } else {
        // Align the offset
        aligned_offset = (manager->shm_offset + alignment - 1) & ~(alignment - 1);
        // Check if the allocation fits within the shared memory size
        if (aligned_offset + size > manager->shm_size) {
            Logger::Log(LOG_LEVEL_WARN, "custom_extent_alloc: failed to extend memory (offset: %zu, size: %zu, shm_size: %zu)",
                aligned_offset, size, manager->shm_size);
            return nullptr; // Return nullptr to indicate allocation failure
        }

        ptr = static_cast<char*>(manager->shm_addr) + aligned_offset;
        manager->shm_offset = aligned_offset + size;
        Logger::Log(LOG_LEVEL_DEBUG, "custom_extent_alloc: allocated %zu bytes at %p (offset: %zu)",
            size, ptr, aligned_offset);
    }
    // Set zero and commit flags
    if (*zero) {
        memset(ptr, 0, size);
    }

    // Ensure the memory is committed if *commit is true
    if (*commit) {
        if (madvise(ptr, size, MADV_WILLNEED) != 0) {
            Logger::Log(LOG_LEVEL_WARN, "custom_extent_alloc: failed to commit memory: %d %s",
                errno, strerror(errno));
            return nullptr; // Return nullptr to indicate allocation failure
        }
    }

    return ptr;
}

bool MemoryManager::custom_extent_dalloc(extent_hooks_t* extent_hooks, void* addr, size_t size,
    bool committed, unsigned arena_ind)
{
    Logger::Log(LOG_LEVEL_DEBUG, "custom_extent_dalloc: arena index %u size %zu at address %p",
        arena_ind, size, addr);
    return true;
}

bool MemoryManager::custom_extent_commit(extent_hooks_t* extent_hooks, void* addr, size_t size,
    size_t offset, size_t length, unsigned arena_ind)
{
    Logger::Log(LOG_LEVEL_DEBUG, "custom_extent_commit: arena index %u committed %zu bytes at %p",
        arena_ind, length, addr);
    return false;
}

bool MemoryManager::custom_extent_decommit(extent_hooks_t* extent_hooks, void* addr, size_t size,
    size_t offset, size_t length, unsigned arena_ind)
{
    Logger::Log(LOG_LEVEL_DEBUG, "custom_extent_decommit: arena index %u decommitted %zu bytes at %p",
        arena_ind, length, addr);
    return false;
}

bool MemoryManager::custom_extent_purge_lazy(extent_hooks_t* extent_hooks, void* addr, size_t size,
    size_t offset, size_t length, unsigned arena_ind)
{
    Logger::Log(LOG_LEVEL_DEBUG, "custom_extent_purge_lazy: arena index %u purged (lazy) %zu bytes at %p",
        arena_ind, length, addr);
    if (madvise(static_cast<char*>(addr) + offset, length, MADV_DONTNEED) != 0) {
        Logger::Log(LOG_LEVEL_WARN, "failed to purge(lazy) memory: %d %s", errno, strerror(errno));
        return true; // indicate failure
    }
    return false; // indicate success
}

bool MemoryManager::custom_extent_purge_forced(extent_hooks_t* extent_hooks, void* addr, size_t size,
    size_t offset, size_t length, unsigned arena_ind)
{
    Logger::Log(LOG_LEVEL_DEBUG, "custom_extent_purge_forced: arena index %u purged (forced) %zu bytes at %p",
        arena_ind, length, addr);
    if (madvise(static_cast<char*>(addr) + offset, length, MADV_DONTNEED) != 0) {
        Logger::Log(LOG_LEVEL_WARN, "failed to purge(forced) memory: %d %s", errno, strerror(errno));
        return true; // indicate failure
    }
    return false; // indicate success
}

bool MemoryManager::custom_extent_split(extent_hooks_t* extent_hooks, void* addr, size_t size, size_t size_a,
    size_t size_b, bool committed, unsigned arena_ind)
{
    Logger::Log(LOG_LEVEL_DEBUG, "custom_extent_split: arena index %u split %zu bytes at %p",
        arena_ind, size, addr);
    return false;
}

bool MemoryManager::custom_extent_merge(extent_hooks_t* extent_hooks, void* addr_a, size_t size_a,
    void* addr_b, size_t size_b, bool committed, unsigned arena_ind)
{
    Logger::Log(LOG_LEVEL_DEBUG, "custom_extent_merge: arena index %u merge %zu bytes at %p and %p",
        arena_ind, size_a, addr_a, addr_b);
    return true; // merge not performed
}

}