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

#include <sstream>
#include <cstdlib>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <errno.h>
#include <climits>

#include "db.h"
#include "rollable_file.h"
#include "logger.h"
#include "error.h"
#include "resource_pool.h"

namespace mabain {

#define SLIDING_MEM_SIZE     16LLU*1024*1024    // 16M
#define MAX_NUM_BLOCK        2*1024             // 2K
#define RC_OFFSET_PERCENTAGE 75                 // default rc offset is placed at 75% of maximum size

const long RollableFile::page_size = sysconf(_SC_PAGESIZE);
int RollableFile::ShmSync(uint8_t *addr, int size)
{
    off_t page_offset = ((off_t) addr) % RollableFile::page_size;
    return msync(addr-page_offset, size+page_offset, MS_SYNC);
}

RollableFile::RollableFile(const std::string &fpath, size_t blocksize, size_t memcap, int access_mode,
                            long max_block, int in_rc_offset_percentage)
          : path(fpath),
            block_size(blocksize),
            mmap_mem(memcap),
            sliding_mmap(access_mode & CONSTS::USE_SLIDING_WINDOW),
            mode(access_mode),
            max_num_block(max_block),
            rc_offset_percentage(in_rc_offset_percentage),
            mem_used(0)
{
    sliding_addr = NULL;
    sliding_mem_size = SLIDING_MEM_SIZE;
    sliding_size = 0;
    sliding_start = 0;
    sliding_map_off = 0;
    shm_sliding_start_ptr = NULL;

    if(mode & CONSTS::ACCESS_MODE_WRITER)
    {
        if(max_num_block == 0 || max_num_block > MAX_NUM_BLOCK)
            max_num_block = MAX_NUM_BLOCK;

        Logger::Log(LOG_LEVEL_INFO, "maximal block number for %s is %d",
                    fpath.c_str(), max_num_block);

        if(rc_offset_percentage == 0 || rc_offset_percentage > 100 || rc_offset_percentage < 50)
            rc_offset_percentage = RC_OFFSET_PERCENTAGE;

        Logger::Log(LOG_LEVEL_INFO, "rc_offset_percentage is set to %d", rc_offset_percentage);
    }

    Logger::Log(LOG_LEVEL_INFO, "opening rollable file %s for %s, mmap size: %d",
            path.c_str(), (mode & CONSTS::ACCESS_MODE_WRITER)?"writing":"reading", mmap_mem);
    if(!sliding_mmap)
    {
        Logger::Log(LOG_LEVEL_INFO, "sliding mmap is turned off for " + fpath);
    }
    else
    {
        // page_size is used to check page alignment when mapping file to memory.
        if(RollableFile::page_size < 0)
        {
            Logger::Log(LOG_LEVEL_WARN, "failed to get page size, turning off sliding memory");
            sliding_mmap = false;
        }
        else
        {
            Logger::Log(LOG_LEVEL_INFO, "sliding mmap is turned on for " + fpath);
        }
    }

    files.assign(3, NULL);
    if(mode & CONSTS::SYNC_ON_WRITE)
        Logger::Log(LOG_LEVEL_INFO, "Sync is turned on for " + fpath);
}

void RollableFile::InitShmSlidingAddr(std::atomic<size_t> *shm_sliding_addr)
{
    shm_sliding_start_ptr = shm_sliding_addr;
#ifdef __DEBUG__
    assert(shm_sliding_start_ptr != NULL);
#endif
}

void RollableFile::Close()
{
    if(sliding_addr != NULL)
    {
        munmap(sliding_addr, sliding_size);
        sliding_addr = NULL;
    }
}

RollableFile::~RollableFile()
{
    Close();
}

int RollableFile::OpenAndMapBlockFile(int block_order, bool create_file)
{
    if(block_order >= max_num_block)
    {
        int level = LOG_LEVEL_DEBUG;
        if(mode & CONSTS::ACCESS_MODE_WRITER)
            level = LOG_LEVEL_WARN;
        Logger::Log(level, "block number %d ovferflow", block_order);
        return MBError::NO_RESOURCE;
    }

    int rval = MBError::SUCCESS;
    std::stringstream ss;
    ss << block_order;
#ifdef __DEBUG__
    assert(files[block_order] == NULL);
#endif

    bool map_file;
    if(mmap_mem > mem_used)
        map_file = true; 
    else
        map_file = false;
    if(!map_file && (mode & CONSTS::MEMORY_ONLY_MODE))
        return MBError::NO_MEMORY;

    files[block_order] = ResourcePool::getInstance().OpenFile(path+ss.str(),
                                                              mode,
                                                              block_size,
                                                              map_file,
                                                              create_file);
    if(map_file)
        mem_used += block_size;
    else if(mode & CONSTS::MEMORY_ONLY_MODE)
        rval = MBError::MMAP_FAILED;
    return rval;
}

// Need to make sure the required size at offset is aligned with
// block_size and mmap_size. We should not write the size in two
// different blocks or one in mmaped region and the other one on disk.
size_t RollableFile::CheckAlignment(size_t offset, int size)
{
    size_t block_offset = offset % block_size;

    if(block_offset + size > block_size)
    {
        // Start at the begining of the next block
        offset = offset + block_size - block_offset;
    }

    return offset;
}

int RollableFile::CheckAndOpenFile(int order, bool create_file)
{
    int rval = MBError::SUCCESS;

    if(order >= static_cast<int>(files.size()))
        files.resize(order+3, NULL);

    if(files[order] == NULL)
        rval = OpenAndMapBlockFile(order, create_file);

    return rval;
}

// Get shared memory address for existing buffer
// No need to check alignment
uint8_t* RollableFile::GetShmPtr(size_t offset, int size)
{
    int order = offset / block_size;
    int rval = CheckAndOpenFile(order, false);

    if(rval != MBError::SUCCESS)
        return NULL;

    if(files[order]->IsMapped())
    {
        size_t index = offset % block_size;
        return files[order]->GetMapAddr() + index;
    }

    if(sliding_mmap)
    {
        if(static_cast<off_t>(offset) >= sliding_start &&
               offset + size <= sliding_start + sliding_size)
        {
            if(sliding_addr != NULL)
                return sliding_addr + (offset % block_size) - sliding_map_off;
        }
    }

    return NULL;
}

int RollableFile::Reserve(size_t &offset, int size, uint8_t* &ptr, bool map_new_sliding)
{
    int rval;
    ptr = NULL;
    offset = CheckAlignment(offset, size);

    int order = offset / block_size;
    rval = CheckAndOpenFile(order, true);
    if(rval != MBError::SUCCESS)
        return rval;

    if(files[order]->IsMapped())
    {
        size_t index = offset % block_size;
        ptr = files[order]->GetMapAddr() + index;
        return rval;
    }

    if(sliding_mmap)
    {
        if(static_cast<off_t>(offset) >= sliding_start &&
               offset + size <= sliding_start + sliding_size)
        {
            if(sliding_addr != NULL)
                ptr = sliding_addr + (offset % block_size) - sliding_map_off;
        }
        else if(map_new_sliding && offset >= sliding_start + sliding_size)
        {
            ptr = NewSlidingMapAddr(order, offset, size);
            if(ptr != NULL)
            {
                // Load the mmap starting offset to shared memory so that readers
                // can map the same region when reading it.
                shm_sliding_start_ptr->store(sliding_start, std::memory_order_relaxed);
            }
        }
    }

    return rval;
}

uint8_t* RollableFile::NewSlidingMapAddr(int order, size_t offset, int size)
{
    if(sliding_addr != NULL)
    {
        // No need to call msync since munmap will write all memory
        // update to disk
        //msync(sliding_addr, sliding_size, MS_SYNC);
        munmap(sliding_addr, sliding_size);
    }

    if(sliding_start == 0)
    {
        sliding_start = offset;
    }
    else
    {
        sliding_start += sliding_size;
    }

    // Check page alignment
    int page_alignment = sliding_start % RollableFile::page_size;
    if(page_alignment != 0)
    {
        sliding_start -= page_alignment;
        if(sliding_start < 0)
            sliding_start = 0;
    }
    sliding_map_off = sliding_start % block_size;
    if(sliding_map_off + sliding_mem_size > block_size)
    {
         sliding_size = block_size - sliding_map_off;
    }
    else
    {
         sliding_size = sliding_mem_size;
    }

    order = sliding_start / block_size;
    sliding_addr = files[order]->MapFile(sliding_size, sliding_map_off, true);
    if(sliding_addr != NULL)
    {
        if(static_cast<off_t>(offset) >= sliding_start &&
                          offset+size <= sliding_start+sliding_size)
            return sliding_addr + (offset % block_size) - sliding_map_off;
    }
    else
    {
        Logger::Log(LOG_LEVEL_WARN, "last mmap failed, disable sliding mmap");
        sliding_mmap = false;
    }

    return NULL;
}

size_t RollableFile::RandomWrite(const void *data, size_t size, off_t offset)
{
    int order = offset / block_size;
    int rval = CheckAndOpenFile(order, false);
    if(rval != MBError::SUCCESS)
        return 0;

    // Check sliding map
    if(sliding_mmap && sliding_addr != NULL)
    {
        if(offset >= sliding_start && offset+size <= sliding_start+sliding_size)
        {
            uint8_t *start_addr = sliding_addr + (offset % block_size) - sliding_map_off;
            memcpy(start_addr, data, size);
            if(mode & CONSTS::SYNC_ON_WRITE)
            {
                off_t page_off = ((off_t) start_addr) % RollableFile::page_size;
                if(msync(start_addr-page_off, size+page_off, MS_SYNC) == -1)
                    std::cout<<"msync error\n";
            }
            return size;
        }
    }

    int index = offset % block_size;
    return files[order]->RandomWrite(data, size, index);
}

void* RollableFile::NewReaderSlidingMap(int order)
{
    off_t start_off = shm_sliding_start_ptr->load(std::memory_order_relaxed);
    if(start_off == 0 || start_off == sliding_start || start_off/block_size != (unsigned)order)
        return NULL;

    if(sliding_addr != NULL)
        munmap(sliding_addr, sliding_size);
    sliding_start = start_off;
    sliding_map_off = sliding_start % block_size;
    if(sliding_map_off + SLIDING_MEM_SIZE > block_size)
    {
        sliding_size = block_size - sliding_map_off;
    }
    else
    {
        sliding_size = SLIDING_MEM_SIZE;
    }

    sliding_addr = files[order]->MapFile(sliding_size, sliding_map_off, true);
    return sliding_addr;
}

size_t RollableFile::RandomRead(void *buff, size_t size, off_t offset)
{
    int order = offset / block_size;
    int rval = CheckAndOpenFile(order, false);
    if(rval != MBError::SUCCESS && rval != MBError::MMAP_FAILED)
        return 0;

    if(sliding_mmap)
    {
        if(!(mode & CONSTS::ACCESS_MODE_WRITER))
            NewReaderSlidingMap(order);

        // Check sliding map
        if(sliding_addr != NULL && offset >= sliding_start &&
           offset+size <= sliding_start+sliding_size)
        {
            memcpy(buff, sliding_addr+(offset%block_size)-sliding_map_off, size);
            return size;
        }
    }

    int index = offset % block_size;
    return files[order]->RandomRead(buff, size, index);
}

void RollableFile::PrintStats(std::ostream &out_stream) const
{
    out_stream << "Rollable file: " << path << " stats:" << std::endl;
    out_stream << "\tshared memory size: " << mmap_mem << std::endl;
    if(sliding_mmap)
    {
        out_stream << "\tsliding mmap start: " << sliding_start << std::endl;
        out_stream << "\tsliding mmap size: " << sliding_mem_size << std::endl;
    }
}

void RollableFile::ResetSlidingWindow()
{
    if(sliding_addr != NULL)
    {
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
         it != files.end(); ++it)
    {
        if(*it != NULL)
        {
            (*it)->Flush();
        }
    }
}

size_t RollableFile::GetResourceCollectionOffset() const
{
    return int((rc_offset_percentage / 100.0f) * max_num_block) * block_size;
}

}
