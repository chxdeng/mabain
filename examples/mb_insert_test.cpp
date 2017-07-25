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

// Insert key-value pair to mabain db
int main(int argc, char *argv[])
{
    if(argc == 2) {
        db_dir = argv[1];
    }

    DB db(db_dir, 1048576LL, 1048576LL, CONSTS::ACCESS_MODE_WRITER);
    if(!db.is_open()) {
        std::cerr << "failed to open mabain db: " << db.StatusStr() << "\n";
        exit(1);
    }

    std::string key, value;
    int rval;

    key = "Apple";
    value = "Red";
    rval = db.Add(key.c_str(), key.length(), value.c_str(), value.length());
    if(rval != MBError::SUCCESS) {
        std::cout << key << ": " << MBError::get_error_str(rval) << std::endl;
    }

    key = "Orange";
    value = "Yellow";
    rval = db.Add(key.c_str(), key.length(), value.c_str(), value.length());
    if(rval != MBError::SUCCESS) {
        std::cout << key << ": " << MBError::get_error_str(rval) << std::endl;
    }

    key = "Grape";
    value = "Purple";
    rval = db.Add(key.c_str(), key.length(), value.c_str(), value.length());
    if(rval != MBError::SUCCESS) {
        std::cout << key << ": " << MBError::get_error_str(rval) << std::endl;
    }

    db.PrintStats(std::cout);
    db.Close();
    return 0;
}
