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

#ifndef __MMAP_FILE__
#define __MMAP_FILE__

#include <cstring>
#include <iostream>
#include <stdint.h>
#include <string>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "file_io.h"
#include "logger.h"
#include "mb_mm.h"

namespace mabain {

// Memory mapped file class
class MmapFileIO : public FileIO {
public:
    MmapFileIO(const std::string& fpath, int mode, off_t filesize, bool sync = false);
    ~MmapFileIO();

    // Initialize memory manager (jemalloc)
    int InitMemoryManager();
    inline void* Malloc(size_t size, size_t& offset);
    inline int Memcpy(const void* src, size_t size, size_t offset) const;
    inline void Free(void* ptr) const;
    inline void Free(size_t offset) const;
    inline size_t Allocated() const;
    inline void Purge() const;

    uint8_t* MapFile(size_t size, off_t offset, bool sliding = false);
    bool IsMapped() const;
    size_t SeqWrite(const void* data, size_t size);
    size_t RandomWrite(const void* data, size_t size, off_t offset);
    size_t SeqRead(void* buff, size_t size);
    size_t RandomRead(void* buff, size_t size, off_t offset);
    void UnMapFile();
    uint8_t* GetMapAddr() const;
    void Flush();

private:
    bool mmap_file;
    size_t mmap_size;
    off_t mmap_start;
    off_t mmap_end;
    unsigned char* addr;
    // The maximal offset where data have been written
    size_t max_offset;
    // Current offset for sequential reading of writing only
    off_t curr_offset;

    MemoryManager* mem_mgr;
};

//////////////////////////////
// jemalloc interface
//////////////////////////////

inline void* MmapFileIO::Malloc(size_t size, size_t& offset)
{
    void* ptr = mem_mgr->mb_malloc(size);
    offset = mem_mgr->get_shm_offset(ptr);
    return ptr;
}

inline int MmapFileIO::Memcpy(const void* src, size_t size, size_t offset) const
{
    if (offset + size > mmap_size) {
        Logger::Log(LOG_LEVEL_ERROR, "memcpy out of bound: %lu %lu %lu", offset, size, mmap_size);
        throw(int) MBError::OUT_OF_BOUND;
    }
    memcpy(static_cast<uint8_t*>(addr) + offset, src, size);
    return MBError::SUCCESS;
}

inline void MmapFileIO::Free(void* ptr) const
{
    mem_mgr->mb_free(ptr);
}

inline void MmapFileIO::Free(size_t offset) const
{
    mem_mgr->mb_free(offset);
}

inline size_t MmapFileIO::Allocated() const
{
    if (mem_mgr == nullptr)
        return 0;
    return mem_mgr->mb_allocated();
}

inline void MmapFileIO::Purge() const
{
    if (mem_mgr == nullptr)
        return;
    mem_mgr->mb_purge();
}

}

#endif
