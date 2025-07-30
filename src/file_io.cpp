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

#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "file_io.h"
#include "logger.h"

namespace mabain {

FileIO::FileIO(const std::string& fpath, int oflags, int fmode, bool sync)
    : path(fpath)
    , options(oflags)
    , sync_on_write(sync)
    , mode(fmode)
{
    fd = -1;
}

FileIO::~FileIO()
{
    if (fd > 0)
        close(fd);
}

int FileIO::Open()
{
    mode_t prev_mask = umask(0);
    fd = open(path.c_str(), options, mode);
    umask(prev_mask);

    return fd;
}

size_t FileIO::Write(const void* data, size_t size)
{
    if (options & MMAP_ANONYMOUS_MODE)
        return 0;

    size_t bytes_written;

    if (fd > 0) {
        bytes_written = write(fd, data, size);
        if (sync_on_write)
            fsync(fd);
    } else {
        bytes_written = 0;
    }

    return bytes_written;
}

size_t FileIO::Read(void* buff, size_t size)
{
    if (options & MMAP_ANONYMOUS_MODE)
        return 0;

    size_t bytes_read;

    if (fd > 0) {
        bytes_read = read(fd, buff, size);
    } else {
        bytes_read = 0;
    }

    return bytes_read;
}

size_t FileIO::RandomWrite(const void* data, size_t size, off_t offset)
{
    if (options & MMAP_ANONYMOUS_MODE)
        return 0;

    size_t bytes_written;

    if (fd > 0) {
        bytes_written = pwrite(fd, data, size, offset);
        if (sync_on_write)
            fsync(fd);
    } else {
        bytes_written = 0;
    }

    return bytes_written;
}

size_t FileIO::RandomRead(void* buff, size_t size, off_t offset)
{
    if (options & MMAP_ANONYMOUS_MODE)
        return 0;

    size_t bytes_read;

    if (fd > 0) {
        bytes_read = pread(fd, buff, size, offset);
    } else {
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
    if (fd > 0) {
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
    if (fd > 0)
        return ftruncate(fd, filesize);

    return 1;
}

int FileIO::AllocateFile(off_t filesize)
{
    if (fd <= 0)
        return 1;

    // First, get current file size
    struct stat st;
    if (fstat(fd, &st) != 0)
        return 1;

    // If we're shrinking the file, use ftruncate
    if (filesize < st.st_size) {
        return ftruncate(fd, filesize);
    }

    // If we're extending the file, use fallocate for better performance
    // and guaranteed space allocation
    if (filesize > st.st_size) {
        // Try fallocate first (Linux-specific, more efficient)
        if (fallocate(fd, 0, st.st_size, filesize - st.st_size) == 0) {
            return 0;
        }

        // Do NOT fall back to ftruncate - fallocate failure should be an error
        // because ftruncate creates sparse files that can cause SIGBUS under disk pressure
        Logger::Log(LOG_LEVEL_ERROR, "fallocate failed for file %s, errno=%d, not using ftruncate to avoid sparse files",
            path.c_str(), errno);
        return 1; // Return error instead of falling back to ftruncate
    }

    // File is already the right size
    return 0;
}

void FileIO::Flush()
{
    if (fd > 0)
        fsync(fd);
}

const std::string& FileIO::GetFilePath() const
{
    return path;
}

}
