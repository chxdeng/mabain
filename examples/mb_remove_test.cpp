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

    mabain::DB::SetLogFile("/var/tmp/mabain_test/mabain.log");
    DB db(db_dir, CONSTS::WriterOptions());
    if(!db.is_open()) {
        std::cerr << "failed to open mabain db: " << db.StatusStr() << "\n";
        exit(1);
    }

    std::string key[3], value[3];
    int rval;

    key[0] = "Apple";
    key[1] = "Orange";
    key[2] = "Grape";
    value[0] = "Red";
    value[1] = "Yellow";
    value[2] = "Purple";

    // Add
    for(int i = 0; i < 3; i++) {
        db.Add(key[i], value[i]);
    }

    // Remove
    for(int i = 0; i < 3; i++) {
        rval = db.Remove(key[i]);
        if(rval != MBError::SUCCESS)
            std::cerr << "failed to remove key " << key[i] << "\n";
        else
            std::cout << "Removed " << key[i] << ": " << value[i] << "\n";
    }

    // Query
    MBData mb_data;
    for(int i = 0; i < 3; i++) {
        rval = db.Find(key[i], mb_data);
        if(rval != MBError::SUCCESS)
            std::cout << "key " << key[i] << " not found\n";
    }

    db.PrintStats(std::cout);
    db.Close();
    mabain::DB::CloseLogFile();
    return 0;
}
