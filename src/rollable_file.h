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

#ifndef __ROLLABLE_FILE_H__
#define __ROLLABLE_FILE_H__

#include <assert.h>
#include <atomic>
#include <memory>
#include <stdint.h>
#include <string>
#include <sys/mman.h>
#include <unordered_map>
#include <vector>

#include "logger.h"
#include "mmap_file.h"

namespace mabain {

// Memory mapped file that can be rolled based on block size
class RollableFile {
public:
    RollableFile(const std::string& fpath, size_t blocksize,
        size_t memcap, int access_mode, long max_block = 0, int rc_offset_percentage = 75);
    ~RollableFile();

    // memory management using jemalloc
    int ConfigureJemalloc(MemoryManagerMetadata* mm_meta);
    void* PreAlloc(size_t init_offset);
    void* Malloc(size_t size, size_t& offset);
    size_t MemWrite(const void* src, size_t size, size_t offset);
    size_t MemRead(void* dst, size_t size, size_t offset);
    void Free(void* ptr) const;
    void Free(size_t offset) const;
    void Purge() const;
    int ResetJemalloc();

    size_t RandomWrite(const void* data, size_t size, off_t offset);
    size_t RandomRead(void* buff, size_t size, off_t offset);
    void InitShmSlidingAddr(std::atomic<size_t>* shm_sliding_addr);
    int Reserve(size_t& offset, int size, uint8_t*& ptr, bool map_new_sliding = true);
    uint8_t* GetShmPtr(size_t offset, int size);
    size_t CheckAlignment(size_t offset, int size);
    void PrintStats(std::ostream& out_stream = std::cout) const;
    void Close();
    void ResetSlidingWindow();

    void Flush();
    size_t GetResourceCollectionOffset() const;
    void RemoveUnused(size_t max_size, bool writer_mode);

    // Expose block size for clients that need chunked operations
    size_t GetBlockSize() const { return block_size; }

    static const long page_size;
    static int ShmSync(uint8_t* addr, int size);

private:
    int OpenAndMapBlockFile(size_t block_order, bool create_file);
    int CheckAndOpenFile(size_t block_order, bool create_file);
    uint8_t* NewSlidingMapAddr(size_t offset, int size);
    void* NewReaderSlidingMap(size_t order);

    // jemalloc hooks
    inline int find_block_index(void* ptr) const;
    inline size_t get_shm_offset(void* ptr) const;
    inline size_t get_aligned_offset(void* ptr) const;
    void* custom_extent_alloc(void* new_addr, size_t size, size_t alignment, bool* zero,
        bool* commit, unsigned arena_ind);
    static bool custom_extent_dalloc(extent_hooks_t* extent_hooks, void* addr, size_t size,
        bool committed, unsigned arena_ind);
    static bool custom_extent_commit(extent_hooks_t* extent_hooks, void* addr, size_t size,
        size_t offset, size_t length, unsigned arena_ind);
    static bool custom_extent_decommit(extent_hooks_t* extent_hooks, void* addr, size_t size,
        size_t offset, size_t length, unsigned arena_ind);
    static bool custom_extent_purge_lazy(extent_hooks_t* extent_hooks, void* addr, size_t size,
        size_t offset, size_t length, unsigned arena_ind);
    static bool custom_extent_purge_forced(extent_hooks_t* extent_hooks, void* addr, size_t size,
        size_t offset, size_t length, unsigned arena_ind);
    static bool custom_extent_split(extent_hooks_t* extent_hooks, void* addr, size_t size,
        size_t size_a, size_t size_b, bool committed, unsigned arena_ind);
    static bool custom_extent_merge(extent_hooks_t* extent_hooks, void* addr_a, size_t size_a,
        void* addr_b, size_t size_b, bool committed, unsigned arena_ind);

    std::string path;
    size_t block_size;
    size_t mmap_mem;
    bool sliding_mmap;
    int mode;
    size_t sliding_mem_size;
    // shared memory sliding start offset for reader
    // Note writer does not flush sliding mmap during writing.
    // Readers have to mmap the same region so that they won't
    // read unflushed data from disk.
    std::atomic<size_t>* shm_sliding_start_ptr;

    size_t max_num_block;

    std::vector<std::shared_ptr<MmapFileIO>> files;
    uint8_t* sliding_addr;
    size_t sliding_size;
    off_t sliding_start;
    off_t sliding_map_off;

    int rc_offset_percentage;
    size_t mem_used;

    // jemalloc only
    static std::unordered_map<unsigned, RollableFile*> arena_manager_map;
};

// Find the block index that contains the given pointer
inline int RollableFile::find_block_index(void* ptr) const
{
    for (auto i = 0; i < (int)files.size(); i++) {
        if (files[i] != nullptr && files[i]->IsMapped()) {
            uint8_t* base_ptr = files[i]->GetMapAddr();
            if (ptr >= base_ptr && ptr < (uint8_t*)base_ptr + block_size) {
                return i;
            }
        }
    }
    // should not reach here
    Logger::Log(LOG_LEVEL_ERROR, "failed to find block index for %p", ptr);
    assert(false);
    return -1;
}

// Get the shared memory offset of the given pointer
// Note that this is the total offset from the beginning of the first file
inline size_t RollableFile::get_shm_offset(void* ptr) const
{
    for (auto i = 0; i < (int)files.size(); i++) {
        if (files[i] != nullptr && files[i]->IsMapped()) {
            uint8_t* base_ptr = files[i]->GetMapAddr();
            if (ptr >= base_ptr && ptr < base_ptr + block_size) {
                return i * block_size + (uint8_t*)ptr - base_ptr;
            }
        }
    }
    // should not reach here
    Logger::Log(LOG_LEVEL_ERROR, "failed to get shm offset for %p", ptr);
    assert(false);
    return static_cast<size_t>(-1);
}

// Get the aligned offset of the given pointer relative to the beginning of the block
inline size_t RollableFile::get_aligned_offset(void* ptr) const
{
    for (auto i = 0; i < (int)files.size(); i++) {
        if (files[i] != nullptr && files[i]->IsMapped()) {
            uint8_t* base_ptr = files[i]->GetMapAddr();
            if (ptr >= base_ptr && ptr < base_ptr + block_size) {
                return (uint8_t*)ptr - base_ptr;
            }
        }
    }
    // should not reach here
    Logger::Log(LOG_LEVEL_ERROR, "failed to get aligned offset for %p", ptr);
    assert(false);
    return static_cast<size_t>(-1);
}

}

#endif
