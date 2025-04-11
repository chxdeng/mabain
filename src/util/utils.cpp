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
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>
#include <sys/stat.h>
#include <dirent.h>

#include "error.h"

namespace mabain {

int acquire_file_lock(const std::string &lock_file_path)
{
    int fd = open(lock_file_path.c_str(), O_WRONLY | O_CREAT,
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if(fd < 0)
    {
        std::cerr << "failed to open lock file " << lock_file_path
                  << " errno: " << errno << std::endl;
        return fd;
    }

    struct flock writer_lock;
    writer_lock.l_type = F_WRLCK;
    writer_lock.l_start = 0;
    writer_lock.l_whence = SEEK_SET;
    writer_lock.l_len = 0;
    if(fcntl(fd, F_SETLK, &writer_lock) != 0)
    {
        close(fd);
        return -1;
    }

    return fd;
}

int acquire_file_lock_wait_n(const std::string &lock_file_path, int ntry)
{
    int fd = -1;
    int cnt = 0;

    do
    {
        fd = acquire_file_lock(lock_file_path);
        if (fd >= 0) break;
        if (++cnt >= ntry) break;
        usleep(1000);
    } while (cnt < ntry);

    if (fd < 0)
    {
        std::cerr << "failed to lock file " << lock_file_path
                  << " errno: " << errno << std::endl;
    }
    return fd;
}

void release_file_lock(int &fd)
{
    if(fd < 0)
        return;
    close(fd);
    fd = -1;
}

uint64_t get_file_inode(const std::string &path)
{
    struct stat sb;
    if(stat(path.c_str(), &sb) < 0)
        return 0;
    return sb.st_ino;
}

bool directory_exists(const std::string &path)
{
    struct stat st;
    if (stat(path.c_str(), &st) == 0)
    {
        if ((st.st_mode & S_IFDIR) != 0) return true;
    }
    return false;
}

static int remove_matched_files(const std::string &dpath, const std::string &pattern)
{
    DIR *d;
    struct dirent *dir;
    d = opendir(dpath.c_str());
    if (d == NULL)
        return MBError::OPEN_FAILURE;

    while ( (dir = readdir(d)) != NULL )
    {
        if (strncmp(pattern.c_str(), dir->d_name, pattern.size()) == 0)
        {
            std::string fpath = dpath + "/" + dir->d_name;
            if (std::remove(fpath.c_str()) != 0)
                std::cerr << "failed to remove " << fpath << std::endl;
        }
    }
    closedir(d);

    return MBError::SUCCESS;
}

int remove_db_files(const std::string &db_dir)
{
    remove_matched_files("/dev/shm", "_mabain_q");
    remove_matched_files(db_dir, "_mabain_");
    return 0;
}

}
