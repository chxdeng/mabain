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

#include <cstdlib>

#include <sys/mman.h>
#include <errno.h>
#include <string.h>
#include <assert.h>

#include "file_io.h"
#include "mmap_file.h"
#include "error.h"
#include "logger.h"
#include "rollable_file.h"

namespace mabain {

MmapFileIO::MmapFileIO(const std::string &fpath, int mode, off_t filesize, bool sync)
       : FileIO(fpath, mode, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH, sync),
         file_size(filesize)
{
    mmap_file = false;
    mmap_size = 0;
    mmap_start = 0xFFFFFFFFFFFFFFFF;
    mmap_end = 0;
    addr = NULL;

    max_offset = 0;
    curr_offset = 0;

    if(mode | O_CREAT)
        Logger::Log(LOG_LEVEL_INFO, "Opening file " + fpath);
    else
        Logger::Log(LOG_LEVEL_DEBUG, "Opening file " + fpath);

    int fd = Open();
    if(fd < 0)
    {
        Logger::Log(LOG_LEVEL_ERROR, "failed to open file %s with mode %d, errno %d",
                fpath.c_str(), mode, errno);
        return;
    }

    if(filesize > 0 && (mode & O_CREAT))
    {
        if(TruncateFile(filesize) != 0)
        {
            Logger::Log(LOG_LEVEL_ERROR, "failed to truncate file %s with size %d",
                    fpath.c_str(), static_cast<int>(filesize));
        }
        else
        {
            Logger::Log(LOG_LEVEL_INFO, "truncate file %s with size %d",
                    fpath.c_str(), static_cast<int>(filesize));
        }
    }
}

MmapFileIO::~MmapFileIO()
{
    UnMapFile();
}

uint8_t* MmapFileIO::MapFile(size_t size, off_t offset, bool sliding)
{
    int mode = PROT_READ;
    if(options & O_RDWR)
    {
        mode |= PROT_WRITE;
    }
    addr = static_cast<unsigned char *>(FileIO::MapFile(size, mode, MAP_SHARED, offset));
    if(addr == MAP_FAILED)
    {
        Logger::Log(LOG_LEVEL_WARN, "mmap (%s) failed errno=%d offset=%llu size=%llu",
                    path.c_str(), errno, offset, size);
        return NULL;
    }
    if(!sliding)
    {
        mmap_file = true;
        mmap_size = size;
        mmap_start = offset;
        mmap_end = offset + size;
    }

    if(mode | PROT_WRITE)
    {
        Logger::Log(LOG_LEVEL_INFO, "mmap file %s, sliding=%d, size=%d, offset=%d",
                path.c_str(), sliding, size, offset);
    }
    else
    {
        Logger::Log(LOG_LEVEL_DEBUG, "mmap file %s, sliding=%d, size=%d, offset=%d",
                path.c_str(), sliding, size, offset);
    }

    return addr;
}

void MmapFileIO::UnMapFile()
{
    if(mmap_file && addr != NULL)
    {
        munmap(addr, mmap_size);
        addr = NULL;
    }
}

// SeqWrite:
//     find the current offset first
//     call RandomWrite using the offset
//     reset the offset based on the number of bytes written
size_t MmapFileIO::SeqWrite(const void *data, size_t size)
{
    size_t bytes_written = MmapFileIO::RandomWrite(data, size, curr_offset);
    curr_offset += bytes_written;
    return bytes_written;
}

size_t MmapFileIO::RandomWrite(const void *data, size_t size, off_t offset)
{
    if(data == NULL)
        return 0;

    size_t bytes_written;

    if(!mmap_file)
    {
        // If the file is not mmaped, we just need to write to the file.
        bytes_written = FileIO::RandomWrite(data, size, offset);
        if(bytes_written + offset > max_offset)
        {
            max_offset = bytes_written + offset;
        }
        return bytes_written;
    }

    // If the file is mmaped, we need to write to the memory or file.
    off_t offset_end = static_cast<off_t>(size) + offset;
    const unsigned char *ptr = static_cast<const unsigned char *>(data);
    if(offset < mmap_start)
    {
        if(offset_end <= mmap_start)
        {
            // no overlap with mmap region
            bytes_written = FileIO::RandomWrite(ptr, size, offset);
        }
        else if(offset_end <= mmap_end)
        {
            // partial overlap with left region
            size_t written_left = mmap_start - offset;
            bytes_written = FileIO::RandomWrite(ptr, written_left, offset);
            ptr += written_left;
            // partial overlap with mmap region
            memcpy(addr+mmap_start, ptr, size-written_left);
            bytes_written += size - written_left;
            if(sync_on_write)
                RollableFile::ShmSync(addr+mmap_start, size-written_left);
        }
        else
        {
            // partial overlap with left region
            size_t written_left = mmap_start - offset;
            bytes_written = FileIO::RandomWrite(ptr, written_left, offset);
            ptr += written_left;
            // full overlap with mmap region
            memcpy(addr+mmap_start, ptr, mmap_size);
            bytes_written += mmap_size;
            written_left += mmap_size;
            ptr += mmap_size;
            if(sync_on_write)
                RollableFile::ShmSync(addr+mmap_start, mmap_size);
            // partial overlap with right region
            bytes_written += FileIO::RandomWrite(ptr, size-written_left, mmap_end);
        }
    }
    else if(offset < mmap_end)
    {
        if(offset_end <= mmap_end)
        {
            // full data is within the mmap region
            memcpy(addr+offset, ptr, size);
            bytes_written = size;
            if(sync_on_write)
                RollableFile::ShmSync(addr+offset, size);
        }
        else
        {
            // partial overlap with mmap region
            size_t written_left = mmap_end - offset;
            memcpy(addr+offset, ptr, written_left);
            ptr += written_left;
            bytes_written = written_left;
            if(sync_on_write)
                RollableFile::ShmSync(addr+offset, written_left);
            // partial overlap with the right region
            bytes_written += FileIO::RandomWrite(ptr, size-written_left, mmap_end);
        }
    }
    else
    {
        // no overlap with mmap region
        bytes_written = FileIO::RandomWrite(ptr, size, offset);
    }

    if(bytes_written + offset > max_offset)
        max_offset = bytes_written + offset;

    return bytes_written;
}

// SeqRead:
//     find the current offset first
//     call RandomRead using the offset
//     reset the offset based on the number of bytes read
size_t MmapFileIO::SeqRead(void *buff, size_t size)
{
    size_t bytes_read = MmapFileIO::RandomRead(buff, size, curr_offset);
    curr_offset += bytes_read;
    return bytes_read;
}

size_t MmapFileIO::RandomRead(void *buff, size_t size, off_t offset)
{
    if(buff == NULL)
        return 0;

    if(!mmap_file)
    {
        // If file is not mmaped, go directly to file IO.
        return FileIO::RandomRead(buff, size, offset);
    }

    size_t bytes_read;
    off_t offset_end = static_cast<off_t>(size) + offset;
    unsigned char *ptr = static_cast<unsigned char *>(buff);
    if(offset < mmap_start)
    {
        if(offset_end <= mmap_start)
        {
            // no overlap with mmap region
            bytes_read = FileIO::RandomRead(ptr, size, offset);
        }
        else if(offset_end <= mmap_end)
        {
            // partial overlap with left region
            size_t read_left = mmap_start - offset;
            bytes_read = FileIO::RandomRead(ptr, read_left, offset);
            ptr += read_left;
            // partial overlap with mmap region
            memcpy(ptr, addr+mmap_start, size-read_left);
            bytes_read += size - read_left;
        }
        else
        {
            // partial overlap with left region
            size_t read_left = mmap_start - offset;
            bytes_read = FileIO::RandomRead(ptr, read_left, offset);
            ptr += read_left;
            // full overlap with mmap region
            memcpy(ptr, addr+mmap_start, mmap_size);
            bytes_read += mmap_size;
            read_left += mmap_size;
            ptr += mmap_size;
            // partial overlap with right region
            bytes_read += FileIO::RandomRead(ptr, size-read_left, mmap_end);
        }
    }
    else if(offset < mmap_end)
    {
        if(offset_end <= mmap_end)
        {
            // full data is within the mmap region
            memcpy(ptr, addr+offset, size);
            bytes_read = size;
        }
        else
        {
            // full overlap with mmap region
            size_t read_left = mmap_end - offset;
            memcpy(ptr, addr+offset, read_left);
            ptr += read_left;
            bytes_read = read_left;
            // partial overlap with the right region
            bytes_read += FileIO::RandomRead(ptr, size-read_left, mmap_end);
        }
    }
    else
    {
        // no overlap with mmap region
        bytes_read = FileIO::RandomRead(ptr, size, offset);
    }

    return bytes_read;
}

bool MmapFileIO::IsMapped() const
{
    return mmap_file;
}

uint8_t* MmapFileIO::GetMapAddr() const
{
    return addr;
}

void MmapFileIO::Flush()
{
    if(addr != NULL)
	msync(addr, mmap_size, MS_SYNC);
    FileIO::Flush();
}

}
