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

#include <string>
#include <vector>
#include <stdint.h>
#include <sys/mman.h>
#include <assert.h>

#include "mmap_file.h"
#include "logger.h"

namespace mabain {

// Memory mapped file that can be rolled based on block size
class RollableFile {
public:
    RollableFile(const std::string &fpath, size_t blocksize,
                 size_t memcap, bool smap, int access_mode,
                 bool sync=false, long max_block=0);
    ~RollableFile();

    size_t   RandomWrite(const void *data, size_t size, off_t offset);
    size_t   RandomRead(void *buff, size_t size, off_t offset, bool use_sliding_mmap);
    int      Reserve(size_t &offset, int size, uint8_t* &ptr, bool map_new_sliding=true);
    void     PrintStats(std::ostream &out_stream = std::cout) const;
    void     Close();
    void     ResetSlidingWindow();

private:
    int      OpenAndMapBlockFile(int block_order);
    int      CheckAndOpenFile(int block_order);
    size_t   CheckAlignment(size_t offset, int size);
    uint8_t* NewSlidingMapAddr(int order, size_t offset, int size);

    std::string path;
    size_t block_size;
    size_t mmap_mem;
    bool sliding_mmap;
    int mode;
    size_t sliding_mem_size;
    bool sync_on_write;
    long max_num_block;

    std::vector<MmapFileIO*> files;
    uint8_t* sliding_addr;
    size_t sliding_size;
    off_t sliding_start;
    off_t sliding_map_off;
    long page_size;
};

}

#endif
