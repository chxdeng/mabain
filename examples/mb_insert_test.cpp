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

    mabain::DB::SetLogFile("/var/tmp/mabain_test/mabain.log");

    DB db(db_dir, CONSTS::WriterOptions());
    if(!db.is_open()) {
        std::cerr << "failed to open mabain db: " << db.StatusStr() << "\n";
        exit(1);
    }

    std::string key[3], value[3];
    int rval;

    key[0] = "Apple";
    value[0] = "Red";
    key[1] = "Orange";
    value[1] = "Yellow";
    key[2] = "Grape";
    value[2] = "Purple";

    for(int i = 0; i < 3; i++) {
        rval = db.Add(key[i], value[i]);
        if(rval != MBError::SUCCESS) {
            std::cout << key[i] << ": " << MBError::get_error_str(rval) << std::endl;
        }
    }

    db.PrintStats(std::cout);
    db.Close();

    mabain::DB::CloseLogFile();
    return 0;
}
