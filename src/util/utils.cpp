/**
 * Copyright (C) 2021 Cisco Inc.
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

#include <cstring>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <stdint.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include "error.h"

namespace mabain {

int acquire_file_lock(const std::string& lock_file_path)
{
    int fd = open(lock_file_path.c_str(), O_WRONLY | O_CREAT,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd < 0) {
        std::cerr << "failed to open lock file " << lock_file_path
                  << " errno: " << errno << std::endl;
        return fd;
    }

    struct flock writer_lock;
    writer_lock.l_type = F_WRLCK;
    writer_lock.l_start = 0;
    writer_lock.l_whence = SEEK_SET;
    writer_lock.l_len = 0;
    if (fcntl(fd, F_SETLK, &writer_lock) != 0) {
        close(fd);
        return -1;
    }

    return fd;
}

int acquire_file_lock_wait_n(const std::string& lock_file_path, int ntry)
{
    int fd = -1;
    int cnt = 0;

    do {
        fd = acquire_file_lock(lock_file_path);
        if (fd >= 0)
            break;
        if (++cnt >= ntry)
            break;
        usleep(1000);
    } while (cnt < ntry);

    if (fd < 0) {
        std::cerr << "failed to lock file " << lock_file_path
                  << " errno: " << errno << std::endl;
    }
    return fd;
}

int acquire_init_file_lock(const std::string& lock_file_path, bool shared)
{
    int fd = open(lock_file_path.c_str(), O_RDWR | O_CREAT,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd < 0) {
        std::cerr << "failed to open lock file " << lock_file_path
                  << " errno: " << errno << std::endl;
        return fd;
    }

    const int operation = (shared ? LOCK_SH : LOCK_EX) | LOCK_NB;
    const int result = flock(fd, operation);

    if (result != 0) {
        const int lock_errno = errno;
        close(fd);
        errno = lock_errno;
        if (lock_errno != EWOULDBLOCK && lock_errno != EAGAIN) {
            std::cerr << "failed to lock file " << lock_file_path
                      << " errno: " << lock_errno << std::endl;
        }
        return -1;
    }

    return fd;
}

void release_file_lock(int& fd)
{
    if (fd < 0)
        return;
    close(fd);
    fd = -1;
}

uint64_t get_file_inode(const std::string& path)
{
    struct stat sb;
    if (stat(path.c_str(), &sb) < 0)
        return 0;
    return sb.st_ino;
}

bool directory_exists(const std::string& path)
{
    struct stat st;
    if (stat(path.c_str(), &st) == 0) {
        if ((st.st_mode & S_IFDIR) != 0)
            return true;
    }
    return false;
}

static int remove_matched_files(const std::string& dpath, const std::string& pattern)
{
    DIR* d;
    struct dirent* dir;
    d = opendir(dpath.c_str());
    if (d == NULL)
        return MBError::OPEN_FAILURE;

    while ((dir = readdir(d)) != NULL) {
        if (strncmp(pattern.c_str(), dir->d_name, pattern.size()) == 0) {
            std::string fpath = dpath + "/" + dir->d_name;
            if (std::remove(fpath.c_str()) != 0)
                std::cerr << "failed to remove " << fpath << std::endl;
        }
    }
    closedir(d);

    return MBError::SUCCESS;
}

void remove_db_queue_file(const std::string& db_dir, const char* queue_dir)
{
    // The queue ID is the header file's inode. Resolve it before deleting
    // the database files and remove only this database's queue.
    const std::string header_path = db_dir.empty() || db_dir.back() == '/'
        ? db_dir + "_mabain_h"
        : db_dir + "/_mabain_h";
    const uint64_t queue_id = get_file_inode(header_path);
    if (queue_id == 0)
        return;

    const std::string qdir = queue_dir != NULL ? queue_dir : "/dev/shm";
    const std::string queue_path = qdir + "/_mabain_q" + std::to_string(queue_id);
    if (std::remove(queue_path.c_str()) != 0 && errno != ENOENT)
        std::cerr << "failed to remove " << queue_path << std::endl;
}

int remove_db_files(const std::string& db_dir, const char* queue_dir)
{
    remove_db_queue_file(db_dir, queue_dir);
    remove_matched_files(db_dir, "_mabain_");
    return 0;
}

}
