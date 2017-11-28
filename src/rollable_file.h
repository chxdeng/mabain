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
#include <atomic>

#include "mmap_file.h"
#include "logger.h"

namespace mabain {

// Memory mapped file that can be rolled based on block size
class RollableFile {
public:
    RollableFile(const std::string &fpath, size_t blocksize,
                 size_t memcap, int access_mode, long max_block=0);
    ~RollableFile();

    size_t   RandomWrite(const void *data, size_t size, off_t offset);
    size_t   RandomRead(void *buff, size_t size, off_t offset, bool reader_mode);
    void     InitShmSlidingAddr(std::atomic<size_t> *shm_sliding_addr);
    int      Reserve(size_t &offset, int size, uint8_t* &ptr, bool map_new_sliding=true);
    uint8_t* GetShmPtr(size_t offset, int size);
    size_t   CheckAlignment(size_t offset, int size);
    void     PrintStats(std::ostream &out_stream = std::cout) const;
    void     Close();
    void     ResetSlidingWindow();

    void     Flush();

    static const long page_size;
    static int ShmSync(uint8_t *addr, int size);

private:
    int      OpenAndMapBlockFile(int block_order);
    int      CheckAndOpenFile(int block_order);
    uint8_t* NewSlidingMapAddr(int order, size_t offset, int size);
    void*    NewReaderSlidingMap(int order);

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
    std::atomic<size_t> *shm_sliding_start_ptr;

    long max_num_block;

    std::vector<MmapFileIO*> files;
    uint8_t* sliding_addr;
    size_t sliding_size;
    off_t sliding_start;
    off_t sliding_map_off;
};

}

#endif
