/**
 * Copyright (C) 2017 Cisco Inc.
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

#include <mabain/db.h>

using namespace mabain;

const char *db_dir = "./tmp_dir/";

// Resource collection test
int main(int argc, char *argv[])
{
    if(argc == 2) {
        db_dir = argv[1];
    }

    DB db(db_dir, CONSTS::ACCESS_MODE_WRITER, 128LL*1024*1024, 128LL*1024*1024);
    if(!db.is_open()) {
        std::cerr << "failed to open mabain db: " << db.StatusStr() << "\n";
        exit(1);
    }

    db.CollectResource();
    db.Close();
    return 0;
}
