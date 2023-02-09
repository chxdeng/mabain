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

#ifndef __MB_PIPE_H__
#define __MB_PIPE_H__

#include <string>

namespace mabain {

// Inter-process synchronization using named pipe
class MBPipe {
public:
    MBPipe();
    MBPipe(const std::string& mbdir, int mode);
    ~MBPipe();
    // timeout is in millisecond
    void Wait(int timeout);
    int Signal();

private:
    void Close();

    std::string fifo_path;
    int fd;
};

}

#endif
