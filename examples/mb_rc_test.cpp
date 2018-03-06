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

#include <string.h>
#include <mabain/db.h>

using namespace mabain;

const char *db_dir = "/var/tmp/db_test/mabain/";

// Resource collection test
int main(int argc, char *argv[])
{
    if(argc == 2) {
        db_dir = argv[1];
    }
    std::string cmd = std::string("mkdir -p ") + db_dir;
    if(system(cmd.c_str()) != 0) {
    }
    mabain::DB::SetLogFile("/var/tmp/mabain_test/mabain.log");
    mabain::MBConfig mbconf;
    memset(&mbconf, 0, sizeof(mbconf));
    mbconf.mbdir = db_dir;
    mbconf.options = CONSTS::WriterOptions();
    mbconf.block_size_index = 1024ULL*1024*1024;
    mbconf.block_size_data = 1024ULL*1024*1024;
    mbconf.num_entry_per_bucket = 10000;
    mbconf.max_num_index_block = 1000000000;
    mbconf.max_num_data_block = 1000000000;
    mbconf.memcap_index = (unsigned long long)128*1024*1024;
    mbconf.memcap_data = (unsigned long long)0;
    DB db(mbconf);
    if(!db.is_open()) {
        std::cerr << "failed to open mabain db: " << db.StatusStr() << "\n";
        exit(1);
    }

    db.CollectResource(1, 0xFFFFFFFFFFFF, 0xFFFFFFFFFFFF, 0xFFFFFFFFFFFF);
    db.Close();
    mabain::DB::CloseLogFile();
    return 0;
}
