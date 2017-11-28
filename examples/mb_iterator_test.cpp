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

// mabain db iterator
int main(int argc, char *argv[])
{
    if(argc == 2) {
        db_dir = argv[1];
    }

    DB db(db_dir, CONSTS::ReaderOptions());
    if(!db.is_open()) {
        std::cerr << "failed to open mabain db: " << db.StatusStr() << "\n";
        exit(1);
    }

    std::string key, value;

    for(DB::iterator iter = db.begin(); iter != db.end(); ++iter) {
        std::cout << iter.key << ": " << std::string((char*)iter.value.buff, iter.value.data_len) << "\n";
    }

    db.Close();
    return 0;
}
