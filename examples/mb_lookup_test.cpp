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

    DB db(db_dir, CONSTS::ReaderOptions());
    if(!db.is_open()) {
        std::cerr << "failed to open mabain db: " << db.StatusStr() << "\n";
        exit(1);
    }

    MBData mbdata;
    int rval;
    std::string key[3];

    key[0] = "Apple";
    key[1] = "Grape";
    key[2] = "Orange";
    key[3] = "Kiwi";

    for(int i = 0; i < 4; i++) {
        rval = db.Find(key[i], mbdata);
        if(rval == MBError::SUCCESS) {
            std::cout << key[i] << ": " << std::string((char *) mbdata.buff, mbdata.data_len) << "\n";
        } else {
            std::cout << key[i] << ": not found\n";
        }
    }

    db.Close();
    return 0;
}
