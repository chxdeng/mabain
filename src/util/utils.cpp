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

#include "error.h"

namespace mabain {

int acquire_writer_lock(const std::string &lock_file_path)
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

void release_writer_lock(int &fd)
{
    if(fd < 0)
        return;
    close(fd);
    fd = -1;
}

}
