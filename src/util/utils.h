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

#ifndef __UTILS_H__
#define __UTILS_H__

#include <string>

namespace mabain {

int acquire_file_lock(const std::string& lock_file_path);
int acquire_file_lock_wait_n(const std::string& lock_file_path, int ntry);
void release_file_lock(int& fd);
uint64_t get_file_inode(const std::string& path);
bool directory_exists(const std::string& path);
int remove_db_files(const std::string& db_dir);

}

#endif
