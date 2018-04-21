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

#ifndef __FILE_IO_H__
#define __FILE_IO_H__

#include <string>
#include <iostream>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace mabain {

#define MMAP_ANONYMOUS_MODE 0x80000000 // This bit should not be used in fcntl.h.

// This is the basic file io class
class FileIO
{
public:
    FileIO(const std::string &fpath, int oflags, int fmode, bool sync);
    virtual ~FileIO();

    int  Open();
    int  TruncateFile(off_t filesize);
    bool IsOpen() const;
    void Close();

    size_t Write(const void *data, size_t bytes);
    size_t Read(void *buff, size_t bytes);
    off_t  SetOffset(off_t offset);

    virtual size_t RandomWrite(const void *data, size_t size, off_t offset);
    virtual size_t RandomRead(void *buff, size_t size, off_t offset);
    virtual void   Flush();

    const std::string& GetFilePath() const;

protected:
    std::string path;
    int options;
    bool sync_on_write;

    void* MapFile(size_t size, int prot, int flags, off_t offset);

private:
    int mode;

    int fd;
};

}

#endif
