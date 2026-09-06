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
#include <string.h>
#include <sys/file.h>

#include "error.h"
#include "logger.h"
#include "mabain_consts.h"
#include "mmap_file.h"
#include "resource_pool.h"

namespace mabain {

namespace {

int WaitForFileLock(int fd, int op)
{
    if (fd < 0)
        return MBError::NOT_INITIALIZED;
    while (flock(fd, op) != 0) {
        if (errno == EINTR)
            continue;
        return MBError::MUTEX_ERROR;
    }
    return MBError::SUCCESS;
}

void UnlockFile(int fd)
{
    if (fd < 0)
        return;
    while (flock(fd, LOCK_UN) != 0) {
        if (errno != EINTR)
            break;
    }
}

} // namespace

RebuildBarrier::RebuildBarrier(std::shared_ptr<MmapFileIO> in_file)
    : file(in_file)
    , reader_count(0)
    , waiting_writer_count(0)
    , writer_active(false)
{
}

bool RebuildBarrier::IsOpen() const
{
    return file != nullptr && file->IsOpen();
}

int RebuildBarrier::LockShared()
{
    std::unique_lock<std::mutex> lock(lock_mutex);
    while (writer_active || waiting_writer_count != 0)
        lock_cv.wait(lock);

    if (reader_count == 0) {
        int rval = WaitForFileLock(file->GetFD(), LOCK_SH);
        if (rval != MBError::SUCCESS)
            return rval;
    }
    reader_count++;
    return MBError::SUCCESS;
}

void RebuildBarrier::UnlockShared()
{
    std::lock_guard<std::mutex> lock(lock_mutex);
    if (reader_count == 0)
        return;
    if (--reader_count == 0) {
        UnlockFile(file->GetFD());
        lock_cv.notify_all();
    }
}

int RebuildBarrier::LockExclusive()
{
    std::unique_lock<std::mutex> lock(lock_mutex);
    waiting_writer_count++;
    while (writer_active || reader_count != 0)
        lock_cv.wait(lock);
    waiting_writer_count--;
    writer_active = true;

    int rval = WaitForFileLock(file->GetFD(), LOCK_EX);
    if (rval != MBError::SUCCESS) {
        writer_active = false;
        lock_cv.notify_all();
    }
    return rval;
}

void RebuildBarrier::UnlockExclusive()
{
    std::lock_guard<std::mutex> lock(lock_mutex);
    if (!writer_active)
        return;
    UnlockFile(file->GetFD());
    writer_active = false;
    lock_cv.notify_all();
}

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
    rebuild_barrier_pool.clear();
    file_pool.clear();
    pthread_mutex_unlock(&pool_mutex);
}

// check if a in-memory db already exists
bool ResourcePool::CheckExistence(const std::string& header_path)
{
    pthread_mutex_lock(&pool_mutex);
    bool exists = file_pool.find(header_path) != file_pool.end();
    pthread_mutex_unlock(&pool_mutex);

    return exists;
}

void ResourcePool::RemoveResourceByPath(const std::string& path)
{
    Logger::Log(LOG_LEVEL_DEBUG, "remove resource %s", path.c_str());
    pthread_mutex_lock(&pool_mutex);
    rebuild_barrier_pool.erase(path);
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

    for (auto it = rebuild_barrier_pool.begin(); it != rebuild_barrier_pool.end();) {
        if (it->first.compare(0, db_path.size(), db_path) == 0)
            it = rebuild_barrier_pool.erase(it);
        else
            it++;
    }

    pthread_mutex_unlock(&pool_mutex);
}

std::shared_ptr<MmapFileIO> ResourcePool::OpenFileWithKey(
    const std::string& pool_key, const std::string& fpath, int mode,
    size_t file_size, bool& map_file, bool create_file)
{
    return OpenFileWithKey(pool_key, fpath, mode, file_size, map_file,
        create_file, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
}

std::shared_ptr<MmapFileIO> ResourcePool::OpenFileWithKey(const std::string& pool_key,
    const std::string& fpath,
    int mode,
    size_t file_size,
    bool& map_file,
    bool create_file,
    mode_t create_mode)
{
    std::shared_ptr<MmapFileIO> mmap_file;

    pthread_mutex_lock(&pool_mutex);

    auto search = file_pool.find(pool_key);
    if (search == file_pool.end()) {
        Logger::Log(LOG_LEVEL_DEBUG, "create resource %s", fpath.c_str());
        int flags = O_RDWR;
        if (create_file)
            flags |= O_CREAT;
        if (mode & CONSTS::MEMORY_ONLY_MODE)
            flags |= MMAP_ANONYMOUS_MODE;

        mmap_file = std::shared_ptr<MmapFileIO>(
            new MmapFileIO(fpath,
                flags,
                file_size,
                mode & CONSTS::SYNC_ON_WRITE,
                create_mode));
        if (!(mode & CONSTS::MEMORY_ONLY_MODE) && !mmap_file->IsOpen()) {
            pthread_mutex_unlock(&pool_mutex);
            return NULL;
        }

        if (map_file) {
            if (mmap_file->MapFile(file_size, 0) != NULL) {
                if (!(mode & CONSTS::MEMORY_ONLY_MODE))
                    mmap_file->Close();
                if (mode & CONSTS::OPTION_JEMALLOC) {
                    // only initialize memory manager for _mabain_i0 and _mabain_d0
                    if (fpath.find("_mabain_i0") != std::string::npos || fpath.find("_mabain_d0") != std::string::npos) {
                        mmap_file->InitMemoryManager();
                    }
                }
            } else {
                Logger::Log(LOG_LEVEL_DEBUG, "failed to map file %s", fpath.c_str());
                map_file = false;
            }
        }

        file_pool[pool_key] = mmap_file;
    } else {
        mmap_file = search->second;
    }

    pthread_mutex_unlock(&pool_mutex);
    return mmap_file;
}

std::shared_ptr<MmapFileIO> ResourcePool::OpenFile(const std::string& fpath,
    int mode,
    size_t file_size,
    bool& map_file,
    bool create_file)
{
    return OpenFile(fpath, mode, file_size, map_file, create_file,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
}

std::shared_ptr<MmapFileIO> ResourcePool::OpenFile(const std::string& fpath,
    int mode,
    size_t file_size,
    bool& map_file,
    bool create_file,
    mode_t create_mode)
{
    return OpenFileWithKey(fpath, fpath, mode, file_size, map_file, create_file,
        create_mode);
}

std::shared_ptr<RebuildBarrier> ResourcePool::OpenRebuildBarrier(
    const std::string& pool_key, const std::string& fpath, int mode)
{
    bool map_file = false;
    std::shared_ptr<MmapFileIO> file = OpenFileWithKey(
        pool_key, fpath, mode, 0, map_file, false);
    if (file == nullptr || !file->IsOpen())
        return nullptr;

    pthread_mutex_lock(&pool_mutex);
    auto search = rebuild_barrier_pool.find(pool_key);
    std::shared_ptr<RebuildBarrier> barrier;
    if (search == rebuild_barrier_pool.end()) {
        barrier = std::make_shared<RebuildBarrier>(file);
        rebuild_barrier_pool[pool_key] = barrier;
    } else {
        barrier = search->second;
    }
    pthread_mutex_unlock(&pool_mutex);
    return barrier;
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

MmapFileIO* ResourcePool::GetResourceByPath(const std::string& path)
{
    MmapFileIO* resource = nullptr;

    pthread_mutex_lock(&pool_mutex);
    auto search = file_pool.find(path);
    if (search != file_pool.end()) {
        resource = search->second.get();
    }
    pthread_mutex_unlock(&pool_mutex);

    return resource;
}

}
