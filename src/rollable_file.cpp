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

#include <assert.h>
#include <climits>
#include <cstdlib>
#include <errno.h>
#include <fcntl.h>
#include <sstream>
#include <stdint.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "db.h"
#include "error.h"
#include "logger.h"
#include "resource_pool.h"
#include "rollable_file.h"

namespace mabain {

#define SLIDING_MEM_SIZE 16LLU * 1024 * 1024 // 16M
#define MAX_NUM_BLOCK 2 * 1024 // 2K
#define RC_OFFSET_PERCENTAGE 75 // default rc offset is placed at 75% of maximum size

const long RollableFile::page_size = sysconf(_SC_PAGESIZE);
std::unordered_map<unsigned, RollableFile*> RollableFile::arena_manager_map;

int RollableFile::ShmSync(uint8_t* addr, int size)
{
    off_t page_offset = ((off_t)addr) % RollableFile::page_size;
    return msync(addr - page_offset, size + page_offset, MS_SYNC);
}

RollableFile::RollableFile(const std::string& fpath, size_t blocksize, size_t memcap, int access_mode,
    long max_block, int in_rc_offset_percentage)
    : path(fpath)
    , block_size(blocksize)
    , mmap_mem(memcap)
    , sliding_mmap(access_mode & CONSTS::USE_SLIDING_WINDOW)
    , mode(access_mode)
    , max_num_block(max_block)
    , rc_offset_percentage(in_rc_offset_percentage)
    , mem_used(0)
{
    sliding_addr = NULL;
    sliding_mem_size = SLIDING_MEM_SIZE;
    sliding_size = 0;
    sliding_start = 0;
    sliding_map_off = 0;
    shm_sliding_start_ptr = NULL;

    if (mode & CONSTS::ACCESS_MODE_WRITER) {
        if (max_num_block == 0 || max_num_block > MAX_NUM_BLOCK)
            max_num_block = MAX_NUM_BLOCK;

        Logger::Log(LOG_LEVEL_DEBUG, "maximal block number for %s is %d",
            fpath.c_str(), max_num_block);

        if (rc_offset_percentage == 0 || rc_offset_percentage > 100 || rc_offset_percentage < 50)
            rc_offset_percentage = RC_OFFSET_PERCENTAGE;

        Logger::Log(LOG_LEVEL_DEBUG, "rc_offset_percentage is set to %d", rc_offset_percentage);
    }

    Logger::Log(LOG_LEVEL_DEBUG, "opening rollable file %s for %s, mmap size: %d",
        path.c_str(), (mode & CONSTS::ACCESS_MODE_WRITER) ? "writing" : "reading", mmap_mem);
    if (!sliding_mmap) {
        Logger::Log(LOG_LEVEL_DEBUG, "sliding mmap is turned off for " + fpath);
    } else {
        // page_size is used to check page alignment when mapping file to memory.
        if (RollableFile::page_size < 0) {
            Logger::Log(LOG_LEVEL_WARN, "failed to get page size, turning off sliding memory");
            sliding_mmap = false;
        } else {
            Logger::Log(LOG_LEVEL_DEBUG, "sliding mmap is turned on for " + fpath);
        }
    }

    files.assign(3, NULL);
    if (mode & CONSTS::SYNC_ON_WRITE)
        Logger::Log(LOG_LEVEL_DEBUG, "Sync is turned on for " + fpath);
}

void RollableFile::InitShmSlidingAddr(std::atomic<size_t>* shm_sliding_addr)
{
    shm_sliding_start_ptr = shm_sliding_addr;
#ifdef __DEBUG__
    assert(shm_sliding_start_ptr != NULL);
#endif
}

void RollableFile::Close()
{
    if (sliding_addr != NULL) {
        munmap(sliding_addr, sliding_size);
        sliding_addr = NULL;
    }
}

RollableFile::~RollableFile()
{
    Close();
}

int RollableFile::OpenAndMapBlockFile(size_t block_order, bool create_file)
{
    if (block_order >= max_num_block) {
        int level = LOG_LEVEL_DEBUG;
        if (mode & CONSTS::ACCESS_MODE_WRITER)
            level = LOG_LEVEL_WARN;
        Logger::Log(level, "block number %d overflow", block_order);
        return MBError::NO_RESOURCE;
    }

    int rval = MBError::SUCCESS;
    std::stringstream ss;
    ss << block_order;
#ifdef __DEBUG__
    assert(files[block_order] == NULL);
#endif

    bool map_file;
    if (mmap_mem > mem_used)
        map_file = true;
    else
        map_file = false;

    if (!map_file && (mode & CONSTS::MEMORY_ONLY_MODE))
        return MBError::NO_MEMORY;
    bool init_jem = false;
    if (block_order == 0 && (mode & CONSTS::OPTION_JEMALLOC)) {
        // Check if jemalloc is initialized already
        if (ResourcePool::getInstance().GetResourceByPath(path + "0") == nullptr) {
            // mm_meta is only initialized for the first block
            init_jem = true;
        }
    }
    files[block_order] = ResourcePool::getInstance().OpenFile(path + ss.str(),
        mode,
        block_size,
        map_file,
        create_file);
    if (map_file) {
        mem_used += block_size;
        if (init_jem) {
            rval = ConfigureJemalloc(files[0]->mm_meta);
        }
    } else if ((mode & CONSTS::MEMORY_ONLY_MODE) || (mode & CONSTS::OPTION_JEMALLOC)) {
        rval = MBError::MMAP_FAILED;
    }
    return rval;
}

// Need to make sure the required size at offset is aligned with
// block_size and mmap_size. We should not write the size in two
// different blocks or one in mmaped region and the other one on disk.
size_t RollableFile::CheckAlignment(size_t offset, int size)
{
    size_t block_offset = offset % block_size;

    if (block_offset + size > block_size) {
        // Start at the begining of the next block
        offset = offset + block_size - block_offset;
    }

    return offset;
}

int RollableFile::CheckAndOpenFile(size_t order, bool create_file)
{
    int rval = MBError::SUCCESS;

    if (order >= static_cast<size_t>(files.size()))
        files.resize(order + 3, NULL);

    if (files[order] == NULL)
        rval = OpenAndMapBlockFile(order, create_file);

    return rval;
}

// Get shared memory address for existing buffer
// No need to check alignment
uint8_t* RollableFile::GetShmPtr(size_t offset, int size)
{
    size_t order = offset / block_size;
    int rval = CheckAndOpenFile(order, false);

    if (rval != MBError::SUCCESS)
        return NULL;

    if (files[order]->IsMapped()) {
        size_t index = offset % block_size;
        return files[order]->GetMapAddr() + index;
    }

    if (sliding_mmap) {
        if (static_cast<off_t>(offset) >= sliding_start && offset + size <= sliding_start + sliding_size) {
            if (sliding_addr != NULL)
                return sliding_addr + (offset % block_size) - sliding_map_off;
        }
    }

    return NULL;
}

int RollableFile::Reserve(size_t& offset, int size, uint8_t*& ptr, bool map_new_sliding)
{
    int rval;
    ptr = NULL;
    offset = CheckAlignment(offset, size);

    size_t order = offset / block_size;
    rval = CheckAndOpenFile(order, true);
    if (rval != MBError::SUCCESS)
        return rval;

    if (files[order]->IsMapped()) {
        size_t index = offset % block_size;
        ptr = files[order]->GetMapAddr() + index;
        return rval;
    }

    if (sliding_mmap) {
        if (static_cast<off_t>(offset) >= sliding_start && offset + size <= sliding_start + sliding_size) {
            if (sliding_addr != NULL)
                ptr = sliding_addr + (offset % block_size) - sliding_map_off;
        } else if (map_new_sliding && offset >= sliding_start + sliding_size) {
            ptr = NewSlidingMapAddr(offset, size);
            if (ptr != NULL) {
                // Load the mmap starting offset to shared memory so that readers
                // can map the same region when reading it.
                shm_sliding_start_ptr->store(sliding_start, std::memory_order_relaxed);
            }
        }
    }

    return rval;
}

uint8_t* RollableFile::NewSlidingMapAddr(size_t offset, int size)
{
    if (sliding_addr != NULL) {
        // No need to call msync since munmap will write all memory
        // update to disk
        //msync(sliding_addr, sliding_size, MS_SYNC);
        munmap(sliding_addr, sliding_size);
    }

    sliding_start = offset;

    // Check page alignment
    int page_alignment = sliding_start % RollableFile::page_size;
    if (page_alignment != 0) {
        sliding_start -= page_alignment;
        if (sliding_start < 0)
            sliding_start = 0;
    }
    sliding_map_off = sliding_start % block_size;
    if (sliding_map_off + sliding_mem_size > block_size) {
        sliding_size = block_size - sliding_map_off;
    } else {
        sliding_size = sliding_mem_size;
    }

    size_t order = sliding_start / block_size;
    sliding_addr = files[order]->MapFile(sliding_size, sliding_map_off, true);
    if (sliding_addr != NULL) {
        if (static_cast<off_t>(offset) >= sliding_start && offset + size <= sliding_start + sliding_size)
            return sliding_addr + (offset % block_size) - sliding_map_off;
    } else {
        Logger::Log(LOG_LEVEL_WARN, "last mmap failed, disable sliding mmap");
        sliding_mmap = false;
    }

    return NULL;
}

size_t RollableFile::RandomWrite(const void* data, size_t size, off_t offset)
{
    size_t order = offset / block_size;
    int rval = CheckAndOpenFile(order, false);
    if (rval != MBError::SUCCESS)
        return 0;

    // Check sliding map
    if (sliding_mmap && sliding_addr != NULL) {
        if (offset >= sliding_start && offset + size <= sliding_start + sliding_size) {
            uint8_t* start_addr = sliding_addr + (offset % block_size) - sliding_map_off;
            memcpy(start_addr, data, size);
            if (mode & CONSTS::SYNC_ON_WRITE) {
                off_t page_off = ((off_t)start_addr) % RollableFile::page_size;
                if (msync(start_addr - page_off, size + page_off, MS_SYNC) == -1)
                    std::cout << "msync error\n";
            }
            return size;
        }
    }

    int index = offset % block_size;
    return files[order]->RandomWrite(data, size, index);
}

void* RollableFile::NewReaderSlidingMap(size_t order)
{
    off_t start_off = shm_sliding_start_ptr->load(std::memory_order_relaxed);
    if (start_off == 0 || start_off == sliding_start || start_off / block_size != (unsigned)order)
        return NULL;

    if (sliding_addr != NULL)
        munmap(sliding_addr, sliding_size);
    sliding_start = start_off;
    sliding_map_off = sliding_start % block_size;
    if (sliding_map_off + SLIDING_MEM_SIZE > block_size) {
        sliding_size = block_size - sliding_map_off;
    } else {
        sliding_size = SLIDING_MEM_SIZE;
    }

    sliding_addr = files[order]->MapFile(sliding_size, sliding_map_off, true);
    return sliding_addr;
}

size_t RollableFile::RandomRead(void* buff, size_t size, off_t offset)
{
    size_t order = offset / block_size;

    int rval = CheckAndOpenFile(order, false);
    if (rval != MBError::SUCCESS && rval != MBError::MMAP_FAILED)
        return 0;

    if (sliding_mmap) {
        if (!(mode & CONSTS::ACCESS_MODE_WRITER))
            NewReaderSlidingMap(order);

        // Check sliding map
        if (sliding_addr != NULL && offset >= sliding_start && offset + size <= sliding_start + sliding_size) {
            memcpy(buff, sliding_addr + (offset % block_size) - sliding_map_off, size);
            return size;
        }
    }

    int index = offset % block_size;
    return files[order]->RandomRead(buff, size, index);
}

void RollableFile::PrintStats(std::ostream& out_stream) const
{
    out_stream << "Rollable file: " << path << " stats:" << std::endl;
    out_stream << "\tshared memory size: " << mmap_mem << std::endl;
    if (sliding_mmap) {
        out_stream << "\tsliding mmap start: " << sliding_start << std::endl;
        out_stream << "\tsliding mmap size: " << sliding_mem_size << std::endl;
    }
}

void RollableFile::ResetSlidingWindow()
{
    if (sliding_addr != NULL) {
        munmap(sliding_addr, sliding_size);
        sliding_addr = NULL;
    }

    sliding_size = 0;
    sliding_start = 0;
    sliding_map_off = 0;
}

void RollableFile::Flush()
{
    for (std::vector<std::shared_ptr<MmapFileIO>>::iterator it = files.begin();
         it != files.end(); ++it) {
        if (*it != NULL) {
            (*it)->Flush();
        }
    }
}

size_t RollableFile::GetResourceCollectionOffset() const
{
    return int((rc_offset_percentage / 100.0f) * max_num_block) * block_size;
}

void RollableFile::RemoveUnused(size_t max_size, bool writer_mode)
{
    unsigned ibeg = max_size / (block_size + 1) + 1;
    for (auto i = ibeg; i < files.size(); i++) {
        if (files[i] != NULL) {
            if (files[i]->IsMapped() && mem_used > block_size)
                mem_used -= block_size;
            if (writer_mode) {
                ResourcePool::getInstance().RemoveResourceByPath(files[i]->GetFilePath());
                unlink(files[i]->GetFilePath().c_str());
            }
            files[i] = NULL;
        }
    }
}

////////////////////////////////////
// memory management using jemalloc
////////////////////////////////////

// Configure jemalloc hooks
int RollableFile::ConfigureJemalloc(MemoryManagerMetadata* mm_meta)
{
    if (!(mode & CONSTS::OPTION_JEMALLOC))
        return MBError::INVALID_ARG;

    extent_hooks_t* extent_hooks = mm_meta->extent_hooks;
    extent_hooks->alloc = [](extent_hooks_t* extent_hooks, void* addr, size_t size,
                              size_t alignment, bool* zero, bool* commit,
                              unsigned arena_ind) -> void* {
        return arena_manager_map[arena_ind]->custom_extent_alloc(addr, size, alignment,
            zero, commit, arena_ind);
    };
    extent_hooks->dalloc = custom_extent_dalloc;
    extent_hooks->commit = custom_extent_commit;
    extent_hooks->decommit = custom_extent_decommit;
    extent_hooks->purge_lazy = custom_extent_purge_lazy;
    extent_hooks->purge_forced = custom_extent_purge_forced;
    extent_hooks->split = custom_extent_split;
    extent_hooks->merge = custom_extent_merge;

    size_t hooks_sz = sizeof(extent_hooks);
    unsigned arena_ind;
    size_t arena_index_sz = sizeof(arena_ind);

    int rc = mallctl("arenas.create", &arena_ind, &arena_index_sz, nullptr, 0);
    if (rc != 0) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to create jemalloc arena %d", rc);
        return MBError::JEMALLOC_ERROR;
    }
    Logger::Log(LOG_LEVEL_INFO, "jemalloc arena index: %u created", arena_ind);

    std::string arena_hooks = "arena." + std::to_string(arena_ind) + ".extent_hooks";
    rc = mallctl(arena_hooks.c_str(), nullptr, nullptr, &extent_hooks, hooks_sz);
    if (rc != 0) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to set jemalloc extent hooks %d", rc);
        return MBError::JEMALLOC_ERROR;
    }

    // Store the MemoryManager instance in the global map
    arena_manager_map[arena_ind] = this;
    mm_meta->arena_index = arena_ind;
    return MBError::SUCCESS;
}

// Initialize memory manager with the initial offset
// This is used to pre-allocate memory for root node at offset 0
// The base pointer is returned for the caller to use
// This funnction must be called before any other memory allocation
void* RollableFile::PreAlloc(size_t init_off)
{
    int rval = CheckAndOpenFile(0, true);
    if (rval != MBError::SUCCESS) {
        throw(int) rval;
    }
    if (!files[0]->IsMapped())
        return nullptr;
    files[0]->mm_meta->alloc_size = init_off;
    return files[0]->GetMapAddr();
}

void* RollableFile::Malloc(size_t size, size_t& offset)
{
    void* ptr = nullptr;
    if (size > 0) {
        if (files[0] == nullptr) {
            // create the first block file for memory metadata if not exist
            int rval = CheckAndOpenFile(0, true);
            if (rval != MBError::SUCCESS) {
                throw(int) rval;
            }
        }
        unsigned arena_index = files[0]->mm_meta->arena_index;
        ptr = mallocx(size, MALLOCX_ARENA(arena_index) | MALLOCX_TCACHE_NONE);
        offset = get_shm_offset(ptr);
    }
    return ptr;
}

// offset is the total offset. It is used to find the block order and relative offset within the block.
size_t RollableFile::MemWrite(const void* src, size_t size, size_t offset)
{
    int block_order = offset / block_size;
    size_t relative_offset = offset % block_size;
    if (relative_offset + size > block_size) {
        throw(int) MBError::OUT_OF_BOUND;
    }
    memcpy(files[block_order]->GetMapAddr() + relative_offset, src, size);
    return size;
}

size_t RollableFile::MemRead(void* dst, size_t size, size_t offset)
{
    int block_order = offset / block_size;
    size_t relative_offset = offset % block_size;
    if (relative_offset + size > block_size) {
        throw(int) MBError::OUT_OF_BOUND;
    }
    memcpy(dst, files[block_order]->GetMapAddr() + relative_offset, size);
    return size;
}

void RollableFile::Free(void* ptr) const
{
    if (ptr != nullptr) {
        unsigned arena_index = files[0]->mm_meta->arena_index;
        dallocx(ptr, MALLOCX_ARENA(arena_index) | MALLOCX_TCACHE_NONE);
    }
}

void RollableFile::Free(size_t offset) const
{
    int block_order = offset / block_size;
    size_t relative_offset = offset % block_size;
    if (block_order >= (int)files.size()) {
        throw(int) MBError::OUT_OF_BOUND;
    }

    void* ptr = files[block_order]->GetMapAddr() + relative_offset;
    unsigned arena_index = files[0]->mm_meta->arena_index;
    dallocx(ptr, MALLOCX_ARENA(arena_index) | MALLOCX_TCACHE_NONE);
}

// Purge all unused dirty pages for the arena
void RollableFile::Purge() const
{
    if (!(mode & CONSTS::OPTION_JEMALLOC)) {
        return;
    }
    if (files[0] == nullptr) {
        Logger::Log(LOG_LEVEL_WARN, "jemalloc not initialized");
        return;
    }
    unsigned arena_index = files[0]->mm_meta->arena_index;
    std::string arena_purge = "arena." + std::to_string(arena_index) + ".purge";
    int rc = mallctl(arena_purge.c_str(), nullptr, nullptr, nullptr, 0);
    if (rc != 0) {
        Logger::Log(LOG_LEVEL_WARN, "failed to perform jemalloc purge error %d", rc);
    }
}

// Reset jemalloc
int RollableFile::ResetJemalloc()
{
    if (!(mode & CONSTS::OPTION_JEMALLOC)) {
        return MBError::INVALID_ARG;
    }
    if (files[0] == nullptr) {
        Logger::Log(LOG_LEVEL_DEBUG, "jemalloc not initialized, no need to reset");
        return MBError::SUCCESS;
    }
    unsigned arena_index = files[0]->mm_meta->arena_index;
    // Destroy the arena
    std::string arena_destroy = "arena." + std::to_string(arena_index) + ".destroy";
    int rc = mallctl(arena_destroy.c_str(), nullptr, nullptr, nullptr, 0);
    if (rc != 0) {
        Logger::Log(LOG_LEVEL_ERROR, "failed to destroy jemalloc arena %u, error code %d",
            arena_index, rc);
    }
    // Reset the memory manager metadata
    if (files[0]->mm_meta != nullptr) {
        delete files[0]->mm_meta;
        files[0]->mm_meta = nullptr;
    }
    files[0]->InitMemoryManager();
    return ConfigureJemalloc(files[0]->mm_meta);
}

void* RollableFile::custom_extent_alloc(void* new_addr, size_t size, size_t alignment,
    bool* zero, bool* commit, unsigned arena_ind)
{
    RollableFile* mgr = arena_manager_map[arena_ind];
    assert(mgr != nullptr);

    void* ptr = nullptr;
    size_t aligned_offset;
    MemoryManagerMetadata* mm_meta = mgr->files[0]->mm_meta;
    if (new_addr != nullptr) {
        ptr = new_addr;
        aligned_offset = mgr->get_aligned_offset(new_addr);
        if (aligned_offset + size > mgr->block_size) {
            Logger::Log(LOG_LEVEL_WARN, "custom_extent_alloc: arena %u failed to extend existing"
                                        " memory (aligned offset: %zu, size: %zu, block size: %zu)",
                arena_ind, aligned_offset, size, mgr->block_size);
            return nullptr;
        }
    } else {
        // Note alloc_size is the current total allocation size used by all blocks
        int block_order = mm_meta->alloc_size / mgr->block_size;
        size_t relative_offset = mm_meta->alloc_size % mgr->block_size;
        aligned_offset = (relative_offset + alignment - 1) & ~(alignment - 1);
        if (aligned_offset + size > mgr->block_size) {
            // Try next block
            block_order++;
            if ((size_t)block_order >= mgr->max_num_block) {
                Logger::Log(LOG_LEVEL_ERROR, "custom_extent_alloc: arena %u max block number exceeded",
                    " new memory (aligned offset: %zu, used: %zu, size: %zu)",
                    arena_ind, aligned_offset, mm_meta->alloc_size, size);
                throw(int) MBError::NO_MEMORY;
            }
            int rval = mgr->CheckAndOpenFile(block_order, true);
            if (rval != MBError::SUCCESS) {
                Logger::Log(LOG_LEVEL_ERROR, "custom_extent_alloc: arena %u failed to open"
                                             " new block file (order: %d, used: %zu, size: %zu)",
                    arena_ind, block_order, mm_meta->alloc_size, size);
                throw(int) MBError::MMAP_FAILED;
            }
            aligned_offset = 0; // reset aligned offset for new block
            if (aligned_offset + size > mgr->block_size) {
                Logger::Log(LOG_LEVEL_ERROR, "custom_extent_alloc: arena %u failed to extend"
                                             " new memory (offset: %zu, size: %zu, used: %zu, size: %zu)",
                    arena_ind, aligned_offset, size, mgr->block_size);
                throw(int) MBError::NO_MEMORY;
            }
        }
        mm_meta->alloc_size = block_order * mgr->block_size + aligned_offset + size;
        ptr = mgr->files[block_order]->GetMapAddr() + aligned_offset;
    }

    // Set zero and commit flags
    if (*zero) {
        memset(ptr, 0, size);
    }
    // Ensure the memory is committed if *commit is true
    if (*commit) {
        if (madvise(ptr, size, MADV_WILLNEED) != 0) {
            Logger::Log(LOG_LEVEL_WARN, "custom_extent_alloc: failed to commit memory: arena %u "
                                        "addr %p block index %d size %zu error %d %s",
                arena_ind, ptr, find_block_index(ptr), size, errno, strerror(errno));
        }
    }
    Logger::Log(LOG_LEVEL_DEBUG, "custom_extent_alloc: arena %u, allocated %zu bytes, used %zu",
        arena_ind, size, mm_meta->alloc_size);
    return ptr;
}

bool RollableFile::custom_extent_dalloc(extent_hooks_t* extent_hooks, void* addr,
    size_t size, bool committed, unsigned arena_ind)
{
    // Indicate that the deallocation is successful
    return true;
}

bool RollableFile::custom_extent_commit(extent_hooks_t* extent_hooks, void* addr,
    size_t size, size_t offset, size_t length, unsigned arena_ind)
{
    // Indicate that the commit is successful
    return false;
}

bool RollableFile::custom_extent_decommit(extent_hooks_t* extent_hooks, void* addr,
    size_t size, size_t offset, size_t length, unsigned arena_ind)
{
    // Ask OS to discard the memory
    if (madvise((char*)addr + offset, length, MADV_DONTNEED) != 0) {
        Logger::Log(LOG_LEVEL_DEBUG, "failed to decommit memory: %d %s",
            errno, strerror(errno));
        return true;
    }
    return false;
}

bool RollableFile::custom_extent_purge_lazy(extent_hooks_t* extent_hooks, void* addr,
    size_t size, size_t offset, size_t length, unsigned arena_ind)
{
    // Ask OS to discard the memory
    if (madvise(static_cast<char*>(addr) + offset, length, MADV_DONTNEED) != 0) {
        Logger::Log(LOG_LEVEL_DEBUG, "failed to purge(lazy) memory: %d %s",
            errno, strerror(errno));
        return true;
    }
    return false;
}

bool RollableFile::custom_extent_purge_forced(extent_hooks_t* extent_hooks, void* addr,
    size_t size, size_t offset, size_t length, unsigned arena_ind)
{
    // Ask OS to free up the memory
    if (madvise(static_cast<char*>(addr) + offset, length, MADV_FREE) != 0) {
        Logger::Log(LOG_LEVEL_DEBUG, "failed to purge(forced) memory: %d %s",
            errno, strerror(errno));
        return true;
    }
    return false;
}

bool RollableFile::custom_extent_split(extent_hooks_t* extent_hooks, void* addr,
    size_t size, size_t size_a, size_t size_b, bool committed, unsigned arena_ind)
{
    // Indicate that the split is successful
    return false;
}

bool RollableFile::custom_extent_merge(extent_hooks_t* extent_hooks, void* addr_a,
    size_t size_a, void* addr_b, size_t size_b, bool committed, unsigned arena_ind)
{
    // Check if the two extents are adjacent
    // Note if addr_a and addr_b are on different blocks, the following condition will
    // always be true. This is because the two addresses are from different mmaped regions.
    if ((char*)addr_a + size_a != addr_b) {
        return true;
    }
    return false;
}
}
