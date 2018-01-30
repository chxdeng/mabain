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

// @author Shridhar Bhalerao <shbhaler@cisco.com>

#ifndef __DBBackup_H__
#define __DBBackup_H__

#include <string>

#include "db.h"
#include "dict.h"

namespace mabain {

class DBBackup
{
public:
	DBBackup(const DB &db);
	~DBBackup();
	int Backup(const char* bkup_dir);

private:
    static void copy_file (const std::string &src_path,
        const std::string &dest_path, char *buffer, int buffer_size);
    
    const DB &db_ref;
	const IndexHeader *header;
};

}
#endif
