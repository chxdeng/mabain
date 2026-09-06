/**
 * Copyright (C) 2018 Cisco Inc.
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

#ifndef __RESOURCE_POOL__
#define __RESOURCE_POOL__

#include <cstdint>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <string>
#include <unordered_map>

#include "mmap_file.h"

namespace mabain {

// One process-local owner for the cross-process rebuild flock. Local reference
// counting keeps one reader from releasing another reader's shared lock.
class RebuildBarrier {
public:
    explicit RebuildBarrier(std::shared_ptr<MmapFileIO> file);

    bool IsOpen() const;
    int LockShared();
    void UnlockShared();
    int LockExclusive();
    void UnlockExclusive();

private:
    std::shared_ptr<MmapFileIO> file;
    std::mutex lock_mutex;
    std::condition_variable lock_cv;
    uint32_t reader_count;
    uint32_t waiting_writer_count;
    bool writer_active;
};

// A singleton class for managing resource/file descriptors using
// shared_ptr. All db handles for the same db will share the same
// file descriptors so that we won't be running out of file descriptors
// when there are a large number of DB handles opened.
class ResourcePool {
public:
    ~ResourcePool();

    std::shared_ptr<MmapFileIO> OpenFile(const std::string& fpath, int mode,
        size_t file_size, bool& map_file,
        bool create_file);
    std::shared_ptr<MmapFileIO> OpenFileWithKey(const std::string& pool_key,
        const std::string& fpath, int mode,
        size_t file_size, bool& map_file,
        bool create_file);
    std::shared_ptr<RebuildBarrier> OpenRebuildBarrier(
        const std::string& pool_key, const std::string& fpath, int mode);
    void RemoveResourceByDB(const std::string& db_path);
    void RemoveResourceByPath(const std::string& path);
    void RemoveAll();
    bool CheckExistence(const std::string& header_path);
    int AddResourceByPath(const std::string& path, std::shared_ptr<MmapFileIO> resource);
    MmapFileIO* GetResourceByPath(const std::string& path);

    static ResourcePool& getInstance()
    {
        static ResourcePool instance; // only one instance per process
        return instance;
    }

private:
    ResourcePool();

    std::unordered_map<std::string, std::shared_ptr<MmapFileIO>> file_pool;
    std::unordered_map<std::string, std::shared_ptr<RebuildBarrier>> rebuild_barrier_pool;
    pthread_mutex_t pool_mutex;
};

}

#endif
