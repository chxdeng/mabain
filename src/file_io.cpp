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

#include <sys/mman.h>
#include <errno.h>

#include "file_io.h"

namespace mabain {

FileIO::FileIO(const std::string &fpath, int oflags, int fmode, bool sync)
        : path(fpath),
          options(oflags),
          sync_on_write(sync),
          mode(fmode)
{
    fd = -1;
}

FileIO::~FileIO()
{
    if(fd > 0)
        close(fd);
}

int FileIO::Open()
{
    mode_t prev_mask = umask(0);
    fd = open(path.c_str(), options, mode);
    umask(prev_mask);

    return fd;
}

size_t FileIO::Write(const void *data, size_t size)
{
    size_t bytes_written;

    if(fd > 0)
    {
        bytes_written = write(fd, data, size);
        if(sync_on_write) fsync(fd);
    }
    else
    {
        bytes_written = 0;
    }

    return bytes_written;
}

size_t FileIO::Read(void *buff, size_t size)
{
    size_t bytes_read;

    if(fd > 0)
    {
        bytes_read = read(fd, buff, size);
    }
    else
    {
        bytes_read = 0;
    }

    return bytes_read;
}

size_t FileIO::RandomWrite(const void *data, size_t size, off_t offset)
{
    size_t bytes_written;

    if(fd > 0)
    {
        bytes_written = pwrite(fd, data, size, offset);
        if(sync_on_write) fsync(fd);
    }
    else
    {
        bytes_written = 0;
    }

    return bytes_written;
}

size_t FileIO::RandomRead(void *buff, size_t size, off_t offset)
{
    size_t bytes_read;

    if(fd > 0)
    {
        bytes_read = pread(fd, buff, size, offset);
    }
    else
    {
        bytes_read = 0;
    }

    return bytes_read;
}

void* FileIO::MapFile(size_t size, int prot, int flags, off_t offset)
{
    return mmap(NULL, size, prot, flags, fd, offset);
}

off_t FileIO::SetOffset(off_t offset)
{
    return lseek(fd, offset, SEEK_SET);
}

void FileIO::Close()
{
    if(fd > 0)
    {
        close(fd);
        fd = -1;
    }
}

bool FileIO::IsOpen() const
{
    return fd > 0;
}

int FileIO::TruncateFile(off_t filesize)
{
    if(fd > 0)
        return ftruncate(fd, filesize);

    return 1;
}

void FileIO::Flush()
{
    if(fd > 0)
        fsync(fd);
}

}
