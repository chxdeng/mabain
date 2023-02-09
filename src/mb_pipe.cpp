/**
 * Copyright (C) 2020 Cisco Inc.
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

#include <fcntl.h>
#include <poll.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "error.h"
#include "logger.h"
#include "mabain_consts.h"
#include "mb_pipe.h"

namespace mabain {

MBPipe::MBPipe()
    : fifo_path("")
    , fd(-1)
{
}

MBPipe::MBPipe(const std::string& mbdir, int mode)
    : fifo_path(mbdir + "_mpipe")
    , fd(-1)
{
    if (mode & CONSTS::ACCESS_MODE_WRITER) {
        unlink(fifo_path.c_str());
        Logger::Log(LOG_LEVEL_INFO, "creating pipe %s", fifo_path.c_str());
        if (mkfifo(fifo_path.c_str(), S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH) < 0) {
            Logger::Log(LOG_LEVEL_ERROR, "failed to create fifo %s %s",
                fifo_path.c_str(), strerror(errno));
            return;
        }

        fd = open(fifo_path.c_str(), O_RDONLY | O_NONBLOCK);
        if (fd < 0) {
            Logger::Log(LOG_LEVEL_DEBUG, "failed to open fifo %s %s",
                fifo_path.c_str(), strerror(errno));
        }
    }
}

MBPipe::~MBPipe()
{
    Close();
}

void MBPipe::Close()
{
    if (fd > 0)
        close(fd);
    fd = -1;
}

void MBPipe::Wait(int timeout)
{
    if (fd < 0) {
        fd = open(fifo_path.c_str(), O_RDONLY | O_NONBLOCK);
        if (fd < 0) {
            Logger::Log(LOG_LEVEL_DEBUG, "failed to open fifo %s %d",
                fifo_path.c_str(), strerror(errno));
            usleep(timeout * 1000);
            return;
        }
    }

    int pollret;
    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLIN;
    pfd.revents = 0;
    pollret = poll(&pfd, 1, timeout);

    if (pollret == 0)
        return;
    if (pollret < 0) {
        Logger::Log(LOG_LEVEL_DEBUG, "poll on pipe read failed errno: %s",
            strerror(errno));
        usleep(timeout * 1000);
        return;
    }
#ifdef __APPLE__
    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL))
#else
    if (pfd.revents & (POLLRDHUP | POLLERR | POLLHUP | POLLNVAL))
#endif
    {
        Logger::Log(LOG_LEVEL_DEBUG, "pipe read poll error");
        Close();
        usleep(timeout * 1000);
        return;
    }

#define __MB_ASYNC_READ_BUFFER_SIZE 1024
    char buf[__MB_ASYNC_READ_BUFFER_SIZE];
    int nread;
    do {
        nread = read(fd, buf, __MB_ASYNC_READ_BUFFER_SIZE);
        if (nread < 0) {
            if (errno == EINTR)
                continue;
            if (!(errno == EAGAIN || errno == EWOULDBLOCK)) {
                Logger::Log(LOG_LEVEL_DEBUG, "pipe read failed %s",
                    strerror(errno));
                Close();
            }
            break;
        } else if (nread == 0) {
            Logger::Log(LOG_LEVEL_DEBUG, "pipe writer disconnected");
            Close();
        }
    } while (nread == __MB_ASYNC_READ_BUFFER_SIZE);
}

int MBPipe::Signal()
{
    if (fd < 0) {
        fd = open(fifo_path.c_str(), O_WRONLY | O_NONBLOCK);
        if (fd < 0) {
            Logger::Log(LOG_LEVEL_DEBUG, "failed to open fifo %s %s",
                fifo_path.c_str(), strerror(errno));
            return MBError::OPEN_FAILURE;
        }
    }

    int pollret;
    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLOUT;
    pfd.revents = 0;
    pollret = poll(&pfd, 1, 1);
    if (pollret == 0)
        return MBError::NO_RESOURCE;
    if (pollret < 0) {
        Logger::Log(LOG_LEVEL_DEBUG, "poll on pipe write failed errno: %s",
            strerror(errno));
        return MBError::INVALID_ARG;
    }
#ifdef __APPLE__
    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL))
#else
    if (pfd.revents & (POLLRDHUP | POLLERR | POLLHUP | POLLNVAL))
#endif
    {
        Logger::Log(LOG_LEVEL_DEBUG, "pipe write poll error");
        Close();
        return MBError::NO_RESOURCE;
    }

    if (write(fd, "", 1) < 0) {
        if (!(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            Logger::Log(LOG_LEVEL_DEBUG, "failed to signal async writer errno: %s",
                strerror(errno));
            Close();
        }
        return MBError::TRY_AGAIN;
    }

    return MBError::SUCCESS;
}

}
