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

// Remove key-value pair to mabain db
int main(int argc, char *argv[])
{
    if(argc == 2) {
        db_dir = argv[1];
    }

    DB db(db_dir, 1048576LL, 1048576LL, ACCESS_MODE_WRITER);
    if(!db.is_open()) {
        std::cerr << "failed to open mabain db: " << db.StatusStr() << "\n";
        exit(1);
    }

    std::string key, value;
    int rval;

    // Add

    key = "Apple";
    value = "Red";
    db.Add(key.c_str(), key.length(), value.c_str(), value.length());
    std::cout << "Added " << key << ": " << value << "\n";

    key = "Orange";
    value = "Yellow";
    db.Add(key.c_str(), key.length(), value.c_str(), value.length());
    std::cout << "Added " << key << ": " << value << "\n";

    key = "Grape";
    value = "Purple";
    db.Add(key.c_str(), key.length(), value.c_str(), value.length());
    std::cout << "Added " << key << ": " << value << "\n";

    // Remove

    key = "Apple";
    rval = db.Remove(key.c_str(), key.length());
    if(rval != MBError::SUCCESS)
        std::cerr << "failed to remove key " << key << "\n";
    else
        std::cout << "Removed " << key << ": " << value << "\n";

    key = "Orange";
    rval = db.Remove(key.c_str(), key.length());
    if(rval != MBError::SUCCESS)
        std::cerr << "failed to remove key " << key << "\n";
    else
        std::cout << "Removed " << key << ": " << value << "\n";

    key = "Grape";
    rval = db.Remove(key.c_str(), key.length());
    if(rval != MBError::SUCCESS)
        std::cerr << "failed to remove key " << key << "\n";
    else
        std::cout << "Removed " << key << ": " << value << "\n";

    // Query

    MBData mb_data;

    key = "Apple";
    rval = db.Find(key.c_str(), key.length(), mb_data);
    if(rval != MBError::SUCCESS)
        std::cout << "key " << key << " not found\n";

    key = "Orange";
    rval = db.Find(key.c_str(), key.length(), mb_data);
    if(rval != MBError::SUCCESS)
        std::cout << "key " << key << " not found\n";

    key = "Grape";
    rval = db.Find(key.c_str(), key.length(), mb_data);
    if(rval != MBError::SUCCESS)
        std::cout << "key " << key << " not found\n";

    db.PrintStats(std::cout);
    db.Close();
    return 0;
}
