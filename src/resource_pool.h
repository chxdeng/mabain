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

#include <unordered_map>
#include <memory>
#include <string>
#include <pthread.h>

#include "mmap_file.h"

namespace mabain {

// A singleton class for managing resource/file descriptors using
// shared_ptr. All db handles for the same db will share the same
// file descriptors so that we won't be running out of file descriptors
// when there are a large number of DB handles opened.
class ResourcePool
{
public:
    ~ResourcePool();

    std::shared_ptr<MmapFileIO> OpenFile(const std::string &fpath, int mode,
                                         size_t file_size, bool &map_file,
                                         bool create_file);
    void RemoveResourceByDB(const std::string &db_path);
    void RemoveResourceByPath(const std::string &path);
    void RemoveAll();
    bool CheckExistence(const std::string &header_path);
    int  AddResourceByPath(const std::string &path, std::shared_ptr<MmapFileIO> resource);

    static ResourcePool& getInstance() {
        static ResourcePool instance; // only one instance per process
        return instance;
    }

private:
    ResourcePool();

    std::unordered_map<std::string, std::shared_ptr<MmapFileIO>> file_pool;
    pthread_mutex_t pool_mutex;
};

}

#endif
