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

// Perform exact key match
int main(int argc, char *argv[])
{
    if(argc == 2) {
        db_dir = argv[1];
    }

    DB db(db_dir, 1048576LL, 1048576LL, ACCESS_MODE_READER);
    if(!db.is_open()) {
        std::cerr << "failed to open mabain db: " << db.StatusStr() << "\n";
        exit(1);
    }

    std::string key;
    MBData mbdata;
    int rval;

    key = "Apple Pie";
    rval = db.FindLongestPrefix(key.c_str(), key.length(), mbdata);
    if(rval == MBError::SUCCESS) {
        std::cout << key << ": " << std::string(reinterpret_cast<char*>(mbdata.buff), mbdata.data_len) << "\n";
    } else {
        std::cout << key << ": not found\n";
    }

    key = "Grape juice";
    rval = db.FindLongestPrefix(key.c_str(), key.length(), mbdata);
    if(rval == MBError::SUCCESS) {
        std::cout << key << ": " << std::string(reinterpret_cast<char*>(mbdata.buff), mbdata.data_len) << "\n";
    } else {
        std::cout << key << ": not found\n";
    }

    key = "Orange paper";
    rval = db.FindLongestPrefix(key.c_str(), key.length(), mbdata);
    if(rval == MBError::SUCCESS) {
        std::cout << key << ": " << std::string(reinterpret_cast<char*>(mbdata.buff), mbdata.data_len) << "\n";
    } else {
        std::cout << key << ": not found\n";
    }

    key = "Kiwikiwi";
    rval = db.FindLongestPrefix(key.c_str(), key.length(), mbdata);
    if(rval == MBError::SUCCESS) {
        std::cout << key << ": " << std::string(reinterpret_cast<char*>(mbdata.buff), mbdata.data_len) << "\n";
    } else {
        std::cout << key << ": not found\n";
    }

    db.Close();
    return 0;
}
