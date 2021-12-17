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

#include <string>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>

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
        std::cerr << "failed to lock file " << lock_file_path
                  << " errno: " << errno << std::endl;
        close(fd);
        return -1;
    }

    return fd;
}

#define FILE_LOCK_RETRY_COUNT 5000
int acquire_file_lock_wait(const std::string &lock_file_path)
{
    int fd = -1;
    int cnt = 0;
    while (cnt++ < FILE_LOCK_RETRY_COUNT)
    {
        fd = acquire_file_lock(lock_file_path);
        if (fd >= 0) break;
        usleep(1000);
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

int remove_db_files(const std::string &db_dir)
{
    std::string cmd = "rm /dev/shm/_mabain_q*";
    if (system(cmd.c_str()) != 0) {
        std::cout << "failed to removed shared queue file\n";
    }
    cmd = "rm " + db_dir + "/_mabain_*";
    if (system(cmd.c_str()) != 0) {
        std::cout << "failed to removed shared db files\n";
    }
    return 0;
}

}
