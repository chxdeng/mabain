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

namespace mabain {

#define SLIDING_MEM_SIZE 16LLU*1024*1024

RollableFile::RollableFile(const std::string &fpath, size_t blocksize,
                           size_t memcap, bool smap, int access_mode,
                           bool sync, long max_block)
          : path(fpath),
            block_size(blocksize),
            mmap_mem(memcap),
            sliding_mmap(smap),
            mode(access_mode),
            sync_on_write(sync),
            max_num_block(max_block)
{
    sliding_addr = NULL;
    sliding_mem_size = SLIDING_MEM_SIZE;
    sliding_size = 0;
    sliding_start = 0;
    sliding_map_off = 0;

    if(max_num_block == 0)
    {
        // disk usage will be virtuall unlimited.
        max_num_block = LONG_MAX;
        if(mode & ACCESS_MODE_WRITER)
        {
            Logger::Log(LOG_LEVEL_INFO, "maximal block number for %s is %d",
                    fpath.c_str(), max_num_block);
        }
    }

    Logger::Log(LOG_LEVEL_INFO, "Opening rollable file %s for %s, mmap size: %d",
            path.c_str(), (mode&ACCESS_MODE_WRITER)?"writing":"reading", mmap_mem);
    if(!sliding_mmap)
    {
        Logger::Log(LOG_LEVEL_INFO, "Sliding mmap is turned off for " + fpath);
    }
    else
    {
        // page_size is used to check page alignment when mapping file to memory.
        page_size = sysconf(_SC_PAGESIZE);
        if(page_size < 0)
        {
            Logger::Log(LOG_LEVEL_WARN, "failed to get page size, turning off sliding memory");
            sliding_mmap = false;
        }
        else
        {
            Logger::Log(LOG_LEVEL_INFO, "Sliding mmap is turned on for " + fpath);
        }
    }

    files.assign(3, NULL);
    // Add the first file
    OpenAndMapBlockFile(0);
    if(sync)
        Logger::Log(LOG_LEVEL_INFO, "Sync is turned on for " + fpath);
}

void RollableFile::Close()
{
    for (std::vector<MmapFileIO*>::iterator it = files.begin();
         it != files.end(); ++it)
    {
        if(*it != NULL)
        {
            delete *it;
            *it = NULL;
        }
    }

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

int RollableFile::OpenAndMapBlockFile(int block_order)
{
    int rval = MBError::SUCCESS;
    std::stringstream ss;
    ss << block_order;

#ifdef __DEBUG__
    assert(files[block_order] == NULL);
#endif

    if(mode & ACCESS_MODE_WRITER)
    {
        files[block_order] = new MmapFileIO(path+ss.str(),
                                 O_RDWR | O_CREAT, block_size, sync_on_write);
    }
    else
    {
        int rw_mode = O_RDONLY;
#ifdef __SHM_LOCK__
        // Both reader and writer need to have write access to the mutex in header.
        // The header is in the first index file.
        if(block_order == 0 && path.find("_mabain_i") != std::string::npos)
            rw_mode = O_RDWR;
#endif
        files[block_order] = new MmapFileIO(path+ss.str(), rw_mode, 0, sync_on_write);
    }

    if(!files[block_order]->IsOpen())
        return MBError::OPEN_FAILURE;

    size_t mem_used = block_order*block_size;
    if(mmap_mem > mem_used)
    {
        if(mmap_mem > mem_used + block_size)
        {
            if(files[block_order]->MapFile(block_size, 0) == NULL)
            {
                char err_buf[32];
                rval = MBError::MMAP_FAILED;
                Logger::Log(LOG_LEVEL_WARN, "failed to mmap file %s:%s", (path+ss.str()).c_str(),
                        strerror_r(errno, err_buf, sizeof(err_buf)));
            }
        }
        else
        {
            if(files[block_order]->MapFile(mmap_mem - mem_used, 0) == NULL)
            {
                char err_buf[32];
                rval = MBError::MMAP_FAILED;
                Logger::Log(LOG_LEVEL_WARN, "failed to mmap partial file %s:%s",
                        (path+ss.str()).c_str(),
                        strerror_r(errno, err_buf, sizeof(err_buf)));
            }
        }
    }

    return rval;
}

// Need to make sure the required size at offset is aligned with
// block_size and mmap_size. We should not write the size in two
// different blocks or one in mmaped region and the other one on disk.
size_t RollableFile::CheckAlignment(size_t offset, int size)
{
    size_t index = offset % block_size;

    // Start at the begining of the next block
    if(index + size > block_size)
        offset = offset - index + block_size;

    // Check alignment with mmap_mem
    if(offset <  mmap_mem)
    {
        if(offset + size > mmap_mem)
        {
            offset = mmap_mem;
            // Need to check block_size alignement again since we changed
            // offset to mmap_mem.
            index = offset % block_size;
            if(index + size > block_size)
                offset = offset - index + block_size;
        }
    }

    return offset;
}

int RollableFile::CheckAndOpenFile(int order)
{
    int rval = MBError::SUCCESS;

    if(order >= static_cast<int>(files.size()))
        files.resize(order+3, NULL);

    if(files[order] == NULL)
        rval = OpenAndMapBlockFile(order);

    return rval;
}

int RollableFile::Reserve(size_t &offset, int size, uint8_t* &ptr, bool map_new_sliding)
{
    int rval;
    ptr = NULL;
    offset = CheckAlignment(offset, size);

    int order = offset / block_size;
    if(order >= max_num_block)
        return MBError::NO_DISK_STORAGE;

    rval = CheckAndOpenFile(order);
    if(rval != MBError::SUCCESS)
        return rval;

    // After CheckAlignment, the new range (offset -> offset+size)
    // should be either in mmaped or non-mamped region. It should
    // have no overlap with both of them.
    if(offset <  mmap_mem)
    {
        size_t index = offset % block_size;
        ptr = files[order]->GetMapAddr() + index;
        return rval;
    }

    // Can a file be mapped to two different address with different offset?
    // Tests showed that error will be thrown if a file is mapped in two regions
    // simultaneously. Therefore, the following check and return statements are
    // necessary.
    if(files[order]->IsMapped())
        return rval;

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
    int page_alignment = sliding_start % page_size;
    if(page_alignment != 0)
    {
        //TODOO sliding_start = sliding_start - page_size - page_alignment;
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
    int rval = CheckAndOpenFile(order);
    if(rval != MBError::SUCCESS)
        return 0;

    // Check sliding map
    if(sliding_mmap && sliding_addr != NULL)
    {
        if(offset >= sliding_start && offset+size <= sliding_start+sliding_size)
        {
            memcpy(sliding_addr+(offset%block_size)-sliding_map_off, data, size);
            return size;
        }
    }

    int index = offset % block_size;
    return files[order]->RandomWrite(data, size, index);
}

size_t RollableFile::RandomRead(void *buff, size_t size, off_t offset, bool use_sliding_mmap)
{
    int order = offset / block_size;
    int rval = CheckAndOpenFile(order);
    if(rval != MBError::SUCCESS && rval != MBError::MMAP_FAILED)
        return 0;

    // Check sliding map
    if(use_sliding_mmap && sliding_mmap && sliding_addr != NULL)
    {
        if(offset >= sliding_start && offset+size <= sliding_start+sliding_size)
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

}
