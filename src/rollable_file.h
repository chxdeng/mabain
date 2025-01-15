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
    void* Malloc(size_t size, size_t& offset);
    int Memcpy(const void* src, size_t size, size_t offset);
    void Free(void* ptr) const;
    void Free(size_t offset) const;
    size_t Allocated() const;
    void Purge() const;

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

    static const long page_size;
    static int ShmSync(uint8_t* addr, int size);

private:
    int OpenAndMapBlockFile(size_t block_order, bool create_file);
    int CheckAndOpenFile(size_t block_order, bool create_file);
    uint8_t* NewSlidingMapAddr(size_t offset, int size);
    void* NewReaderSlidingMap(size_t order);

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
};

}

#endif
