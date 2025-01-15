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

#include <string.h>

#include "error.h"
#include "mabain_consts.h"
#include "mmap_file.h"
#include "resource_pool.h"

namespace mabain {

ResourcePool::ResourcePool()
{
    pthread_mutex_init(&pool_mutex, NULL);
}

ResourcePool::~ResourcePool()
{
    pthread_mutex_destroy(&pool_mutex);
}

void ResourcePool::RemoveAll()
{
    pthread_mutex_lock(&pool_mutex);
    file_pool.clear();
    pthread_mutex_unlock(&pool_mutex);
}

// check if a in-memory db already exists
bool ResourcePool::CheckExistence(const std::string& header_path)
{
    pthread_mutex_lock(&pool_mutex);
    auto search = file_pool.find(header_path);
    pthread_mutex_unlock(&pool_mutex);

    return (search != file_pool.end());
}

void ResourcePool::RemoveResourceByPath(const std::string& path)
{
    pthread_mutex_lock(&pool_mutex);
    file_pool.erase(path);
    pthread_mutex_unlock(&pool_mutex);
}

void ResourcePool::RemoveResourceByDB(const std::string& db_path)
{
    pthread_mutex_lock(&pool_mutex);

    for (auto it = file_pool.begin(); it != file_pool.end();) {
        if (it->first.compare(0, db_path.size(), db_path) == 0)
            it = file_pool.erase(it);
        else
            it++;
    }

    pthread_mutex_unlock(&pool_mutex);
}

std::shared_ptr<MmapFileIO> ResourcePool::OpenFile(const std::string& fpath,
    int mode,
    size_t file_size,
    bool& map_file,
    bool create_file)
{
    std::shared_ptr<MmapFileIO> mmap_file;

    pthread_mutex_lock(&pool_mutex);

    auto search = file_pool.find(fpath);
    if (search == file_pool.end()) {
        int flags = O_RDWR;
        if (create_file)
            flags |= O_CREAT;
        if (mode & CONSTS::MEMORY_ONLY_MODE)
            flags |= MMAP_ANONYMOUS_MODE;

        mmap_file = std::shared_ptr<MmapFileIO>(
            new MmapFileIO(fpath,
                flags,
                file_size,
                mode & CONSTS::SYNC_ON_WRITE));
        if (map_file) {
            if (mmap_file->MapFile(file_size, 0) != NULL) {
                if (!(mode & CONSTS::MEMORY_ONLY_MODE))
                    mmap_file->Close();
                if (mode & CONSTS::OPTION_JEMALLOC) {
                    if (fpath.find("_mabain_h") == std::string::npos) { // no need to init MemoryManager for header file
                        mmap_file->InitMemoryManager();
                    }
                }
            } else {
                map_file = false;
            }
        }

        file_pool[fpath] = mmap_file;
    } else {
        mmap_file = search->second;
    }

    pthread_mutex_unlock(&pool_mutex);
    return mmap_file;
}

int ResourcePool::AddResourceByPath(const std::string& path, std::shared_ptr<MmapFileIO> resource)
{
    int rval = MBError::IN_DICT;

    pthread_mutex_lock(&pool_mutex);
    auto search = file_pool.find(path);
    if (search == file_pool.end()) {
        file_pool[path] = resource;
        rval = MBError::SUCCESS;
    }
    pthread_mutex_unlock(&pool_mutex);

    return rval;
}

}
