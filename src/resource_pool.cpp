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

#include <string.h>

#include "resource_pool.h"
#include "mabain_consts.h"

namespace mabain {

ResourcePool::ResourcePool()
{
    pthread_mutex_init(&pool_mutex, NULL);
}

ResourcePool::~ResourcePool()
{
    pthread_mutex_destroy(&pool_mutex);
}

// decrement reference count for shared pointer
void ResourcePool::DecRefCountByDB(const std::string &db_path)
{
    pthread_mutex_lock(&pool_mutex);

    for(auto it = file_pool.begin(); it != file_pool.end();)
    {
        if(it->first.compare(0, db_path.size(), db_path) == 0)
            it = file_pool.erase(it);
        else
            it++;
    }

    pthread_mutex_unlock(&pool_mutex);
}

std::shared_ptr<MmapFileIO> ResourcePool::OpenFile(const std::string &fpath,
                                                   int mode,
                                                   size_t file_size,
                                                   bool &map_file)
{
    std::shared_ptr<MmapFileIO> mmap_file;

    pthread_mutex_lock(&pool_mutex);

    auto search = file_pool.find(fpath);
    if(search == file_pool.end())
    {
        mmap_file = std::shared_ptr<MmapFileIO>
                    (
                         new MmapFileIO(fpath,
                                        O_RDWR | O_CREAT,
                                        file_size,
                                        mode & CONSTS::SYNC_ON_WRITE)
                    );
        if(map_file)
        {
            if(mmap_file->MapFile(file_size, 0) != NULL)
            {
                mmap_file->Close();
            }
            else
            {
                char err_buf[32];
                map_file = false;
                Logger::Log(LOG_LEVEL_WARN, "failed to mmap file %s:%s", fpath.c_str(),
                    strerror_r(errno, err_buf, sizeof(err_buf)));
            }
        }

        file_pool[fpath] = mmap_file;
    }
    else
    {
        mmap_file = search->second;
    }

    pthread_mutex_unlock(&pool_mutex);
    return mmap_file;
}

}
