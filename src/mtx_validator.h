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

#ifndef __MTX__VALIDATOR_H__
#define __MTX__VALIDATOR_H__

#include <thread>
#include <pthread.h>

#include "logger.h"
#include "drm_base.h"
#include "async_writer.h"

namespace mabain {

class MutexValidator
{
public:
    MutexValidator(const std::string &mbdir, const MBConfig &config);
    ~MutexValidator();
    bool IsRunning();
    void InvalidateDBVersion();

private:
    void ValidateMutex();

    std::shared_ptr<MmapFileIO> header_file;
    IndexHeader *header;
    std::thread tid;
    bool is_running;
};

}

#endif
