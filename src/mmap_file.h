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

    uint8_t* MapFile(size_t size, off_t offset, bool sliding = false);
    bool IsMapped() const;
    size_t SeqWrite(const void* data, size_t size);
    size_t RandomWrite(const void* data, size_t size, off_t offset);
    size_t SeqRead(void* buff, size_t size);
    size_t RandomRead(void* buff, size_t size, off_t offset);
    void UnMapFile();
    uint8_t* GetMapAddr() const;
    void Flush();

    // for jemalloc
    int InitMemoryManager();
    MemoryManagerMetadata* mm_meta;

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
};

}
#endif
