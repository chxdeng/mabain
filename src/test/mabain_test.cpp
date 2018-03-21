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

#include <iostream>
#include <fstream>
#include <string>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>

#include "../db.h"
#include "../dict_mem.h"
#include "../dict.h"
#include "../error.h"
#include "../mabain_consts.h"
#include "../resource_pool.h"

using namespace mabain;

static const char* MB_DIR = "/var/tmp/mabain_test/";
static bool debug = false;

static void clean_db_dir()
{
    std::string cmd = std::string("rm -rf ") + MB_DIR + "/_mabain_* >" + MB_DIR + "/out 2>" + MB_DIR + "/err";
    if(system(cmd.c_str()) != 0) {
    }
    cmd = std::string("rm -rf ") + MB_DIR + "/backup/_mabain_* >" + MB_DIR + "/out 2>" + MB_DIR + "/err";
    if(system(cmd.c_str()) != 0) {
    }
}

#define VALUE_LENGTH 16
static void load_test(std::string &list_file, MBConfig &mbconf, int64_t expected_count)
{
    std::cout << "Loading " << list_file << "\n";
    DB db(mbconf);
    if(db.Status() != MBError::SUCCESS) {
        std::cout << "DB error: " << MBError::get_error_str(db.Status()) << "\n";
        exit(1);
    }
    std::ifstream in(list_file.c_str());
    if(!in.is_open()) {
        std::cerr << "cannot open file " << list_file << "\n";
        in.close();
        return;
    }

    int count = 0;
    int count_existing = 0;
    std::string line;
    char buff[256];
    int rval;
    struct timeval start, stop;

    memset(buff, 0, sizeof(buff));
    assert(sizeof(buff) >= VALUE_LENGTH);
    for(int i = 0; i < VALUE_LENGTH; i++) {
        buff[i] = 'a' + i;
    }

    gettimeofday(&start, NULL);
    while(std::getline(in, line)) {
        if(line.length() > (unsigned)CONSTS::MAX_KEY_LENGHTH) {
            if(debug)
                std::cout<<line<<"\n";
            continue;
        }

        rval = db.Add(line.c_str(), line.length(), buff, VALUE_LENGTH);
        if(rval == MBError::SUCCESS) {
            count++;
        }
        else if(rval == MBError::IN_DICT) {
            count_existing++;
        }
        else {
            std::cout << "Adding: " << line << ": " << MBError::get_error_str(rval) << "\n";
        }

        if(count % 1000000 == 0) std::cout << "Added " << count << "\n";
    }
    gettimeofday(&stop, NULL);
    in.close();
    db.PrintStats();
    int64_t db_cnt = db.Count();
    db.Close();
    int64_t tmdiff = (stop.tv_sec-start.tv_sec)*1000000 + (stop.tv_usec-start.tv_usec);
    std::cout << "count: " << count << " " << count_existing <<
                 "    time: " << 1.0*tmdiff/count << "\n";
    assert(expected_count == count);
    assert(expected_count == db_cnt);
}

static void update_test(std::string &list_file, MBConfig &mbconf, int64_t expected_count)
{
    std::cout << "Updating " << list_file << "\n";
    DB db(mbconf);
    if(db.Status() != MBError::SUCCESS) {
        std::cout << "DB error: " << MBError::get_error_str(db.Status()) << "\n";
        exit(1);
    }
    std::ifstream in(list_file.c_str());
    if(!in.is_open()) {
        std::cerr << "cannot open file " << list_file << "\n";
        in.close();
        return;
    }

    struct timeval start, stop;
    int64_t count = 0;
    std::string line;
    int rval;
    gettimeofday(&start, NULL);
    while(std::getline(in, line)) {
        if(line.length() > (unsigned)CONSTS::MAX_KEY_LENGHTH) {
            if(debug)
                std::cout<<line<<"\n";
            continue;
        }

        rval = db.Add(line, line, true);
        assert(rval == MBError::SUCCESS);
        count++;
        if(count % 1000000 == 0) std::cout << "Added " << count << "\n";
    }
    in.close();
    gettimeofday(&stop, NULL);
    db.PrintStats();
    int64_t db_cnt = db.Count();
    db.Close();
    int64_t tmdiff = (stop.tv_sec-start.tv_sec)*1000000 + (stop.tv_usec-start.tv_usec);
    std::cout << "count: " << count << "    time: " << 1.0*tmdiff/db_cnt << "\n";
    assert(expected_count == db_cnt);
}

static void lookup_test(std::string &list_file, MBConfig &mbconf, int64_t expected_count)
{
    std::cout << "Lookup test: " << list_file << "\n";
    DB db(mbconf);
    assert(db.Status() == MBError::SUCCESS);
    std::ifstream in(list_file.c_str());
    assert(in.is_open());
    int count = 0;
    std::string line;
    int rval;
    MBData data;
    struct timeval start, stop;
    int found = 0;
    gettimeofday(&start, NULL);
    while(std::getline(in, line)) {
        if(line.length() > (unsigned) CONSTS::MAX_KEY_LENGHTH) {
            continue;
        }
        rval = db.Find(line.c_str(), line.length(), data);
        if(rval == MBError::SUCCESS) {
            found++;
        } else {
            if(debug)
                std::cout << "failed: " << line << "\n";
        }
        count++;
        if(count % 1000000 == 0) std::cout << "Looked up " << count << "\n";
    }
    gettimeofday(&stop, NULL);
    in.close();
    db.Close();
    int64_t tmdiff = (stop.tv_sec-start.tv_sec)*1000000 + (stop.tv_usec-start.tv_usec);
    std::cout << "count: " << count << "    time: " << 1.0*tmdiff/count << "\n";
    std::cout << "found: " << found << "\n";
    assert(found == expected_count);
}

static void prefix_lookup_test(std::string &list_file, MBConfig &mbconf, int64_t expected_count)
{
    std::cout << "Lookup test: " << list_file << "\n";
    DB db(mbconf);
    assert(db.Status() == MBError::SUCCESS);
    std::ifstream in(list_file.c_str());
    assert(in.is_open());
    int count = 0;
    std::string line;
    int rval;
    MBData data(256, CONSTS::OPTION_ALL_PREFIX);
    struct timeval start, stop;
    int nfound = 0;
    gettimeofday(&start, NULL);
    while(std::getline(in, line)) {
        if(line.length() >(unsigned) CONSTS::MAX_KEY_LENGHTH) {
            continue;
        }

        while(data.next) {
            rval = db.FindPrefix(line.c_str(), line.length(), data);
            if(rval == MBError::SUCCESS) nfound++;
        }
        data.Clear();
        count++;
        if(count % 1000000 == 0) std::cout << "Looked up " << count << "\n";
    }
    gettimeofday(&stop, NULL);
    in.close();
    db.Close();
    int64_t tmdiff = (stop.tv_sec-start.tv_sec)*1000000 + (stop.tv_usec-start.tv_usec);
    std::cout << "count: " << count << " found " << nfound <<
                 "   time: " << 1.0*tmdiff/count << "\n";
    assert(expected_count == nfound);
}

static void longest_prefix_lookup_test(std::string &list_file, MBConfig &mbconf,
                                       int64_t expected_count)
{
    std::cout << "Lookup test: " << list_file << "\n";
    DB db(mbconf);
    assert(db.Status() == MBError::SUCCESS);
    std::ifstream in(list_file.c_str());
    assert(in.is_open());
    int count = 0;
    std::string line;
    int rval;
    MBData data;
    struct timeval start, stop;
    int nfound = 0;
    gettimeofday(&start, NULL);
    while(std::getline(in, line)) {
        if(line.length() > (unsigned) CONSTS::MAX_KEY_LENGHTH) {
            continue;
        }
        data.Clear();

        rval = db.FindLongestPrefix(line.c_str(), line.length(), data);
        if(rval == MBError::SUCCESS) {
            nfound++;
        }
        count++;
        if(count % 1000000 == 0) std::cout << "Looked up " << count << "\n";
    }
    gettimeofday(&stop, NULL);
    in.close();
    db.Close();
    int64_t tmdiff = (stop.tv_sec-start.tv_sec)*1000000 + (stop.tv_usec-start.tv_usec);
    std::cout << "count: " << count << " found " << nfound <<
                 "   time: " << 1.0*tmdiff/count << "\n";
    assert(expected_count == nfound);
}

static void delete_odd_test(std::string &list_file, MBConfig &mbconf, int64_t expected_count)
{
    std::cout << "deletion odd-entry test: " << list_file << "\n";
    DB db(mbconf);
    assert(db.Status() == MBError::SUCCESS);
    std::ifstream in(list_file.c_str());
    assert(in.is_open());
    int count = 0;
    int nfound = 0;
    std::string line;
    int rval;
    struct timeval start, stop;

    gettimeofday(&start, NULL);
    while(std::getline(in, line)) {
        if(line.length() > (unsigned) CONSTS::MAX_KEY_LENGHTH) {
            continue;
        }
        count++;
        if(count % 2 == 0)
        {
            rval = db.Remove(line);
            if(rval == MBError::SUCCESS) {
                nfound++;
            } else {
                std::cerr << "failed to remove " << line << ": " <<
                             MBError::get_error_str(rval) << "\n";
            }
            if(count % 1000000 == 0) std::cout << "deleted " << count << "\n";
        }
    }
    gettimeofday(&stop, NULL);
    in.close();
    db.PrintStats();
    db.Close();
    int64_t tmdiff = (stop.tv_sec-start.tv_sec)*1000000 + (stop.tv_usec-start.tv_usec);
    std::cout << "count: " << count << " deleted " << nfound <<
                 "   time: " << 1.0*tmdiff/count << "\n";
    assert(expected_count == nfound);
}

static void delete_test(std::string &list_file, MBConfig &mbconf, int64_t expected_count)
{
    std::cout << "deletion test: " << list_file << "\n";
    DB db(mbconf);
    assert(db.Status() == MBError::SUCCESS);
    std::ifstream in(list_file.c_str());
    assert(in.is_open());
    int count = 0;
    int nfound = 0;
    std::string line;
    int rval;
    struct timeval start, stop;
    int64_t db_count = db.Count();

    gettimeofday(&start, NULL);
    while(std::getline(in, line)) {
        if(line.length() > (unsigned) CONSTS::MAX_KEY_LENGHTH) {
            continue;
        }
        rval = db.Remove(line);
        if(rval == MBError::SUCCESS) {
            nfound++;
            db_count--;
            assert(db_count == db.Count());
        } else if(debug) {
            std::cerr << "failed to remove " << line << ": " <<
                         MBError::get_error_str(rval) << "\n";
        }
        count++;
        if(count % 1000000 == 0) std::cout << "deleted " << count << "\n";
    }
    gettimeofday(&stop, NULL);
    in.close();
    db.PrintStats();
    db.Close();
    int64_t tmdiff = (stop.tv_sec-start.tv_sec)*1000000 + (stop.tv_usec-start.tv_usec);
    std::cout << "count: " << count << " deleted " << nfound <<
                 "   time: " << 1.0*tmdiff/count << "\n";
    assert(expected_count == nfound);
}

static void shrink_test(std::string &list_file, MBConfig &mbconf,
                 int64_t expected_count)
{
    std::cout << "shrink test: " << list_file << "\n";
    DB db(mbconf);
    assert(db.Status() == MBError::SUCCESS);
    struct timeval start, stop;

    gettimeofday(&start, NULL);
    int rval = db.CollectResource(1024, 1024, 0xFFFFFFFFFFFFF, 0xFFFFFFFFFFFF);
    gettimeofday(&stop, NULL);
    assert(rval == MBError::SUCCESS);

    db.PrintStats();
    db.Close();
    int64_t tmdiff = (stop.tv_sec-start.tv_sec)*1000000 + (stop.tv_usec-start.tv_usec);
    std::cout << "shrink time: " << tmdiff/1000000. << "\n";
}

static void iterator_test(MBConfig &mbconf, int64_t expected_count)
{
    DB db(mbconf);
    assert(db.Status() == MBError::SUCCESS);
 
    int count = 0;
    struct timeval start, stop;
    gettimeofday(&start, NULL);
    for(DB::iterator iter = db.begin(); iter != db.end(); ++iter) {
        count++;
        if(count % 1000000 == 0) {
            std::cout << "iterated " << count << "\n";
        }
    }
    gettimeofday(&stop, NULL);
    db.PrintStats();
    int64_t db_cnt = db.Count();
    db.Close();
    int64_t tmdiff = (stop.tv_sec-start.tv_sec)*1000000 + (stop.tv_usec-start.tv_usec);
    std::cout << "count: " << count << "   time: " << 1.0*tmdiff/count << "\n";
    assert(count == db_cnt);
    assert(expected_count == count);
}

static void prune_test(MBConfig &mbconf, int64_t expected_count)
{
    DB db(mbconf);
    assert(db.is_open());

    int count = 0;
    struct timeval start, stop;
    gettimeofday(&start, NULL);
    // Delete one-third of the DB
    for(DB::iterator iter = db.begin(); iter != db.end(); ++iter) {
        count++;
        if(count % 3 == 0) {
            db.Remove(iter.key);    
        }
        if(count % 1000000 == 0) {
            std::cout << "iterated " << count << "\n";
        }
    }
    
    int64_t db_cnt = db.Count();
    db.PrintStats();
    assert(db.Close() == MBError::SUCCESS);

    gettimeofday(&stop, NULL);
    assert(expected_count == db_cnt);
    int64_t tmdiff = (stop.tv_sec-start.tv_sec)*1000000 + (stop.tv_usec-start.tv_usec);
    std::cout << "count: " << count << "   time: " << 1.0*tmdiff/count << "\n";
}

static void async_eviction_test(std::string &list_file, MBConfig &mbconf, int64_t expected_count)
{
    // Set up writer
    mbconf.options = CONSTS::ACCESS_MODE_WRITER | CONSTS::USE_SLIDING_WINDOW |
	             CONSTS::ASYNC_WRITER_MODE;
    // Use a small size so that we can test eviction
    DB db_async(mbconf);
    assert(db_async.is_open());

    mbconf.options = CONSTS::ACCESS_MODE_READER | CONSTS::USE_SLIDING_WINDOW;
    DB db(mbconf);
    assert(db_async.is_open());
    assert(db.SetAsyncWriterPtr(&db_async) == MBError::SUCCESS);
    assert(db.AsyncWriterEnabled());

    std::string line;
    std::ifstream in(list_file.c_str());
    assert(in.is_open());
    struct timeval start, stop;
    int64_t count = 0;
    gettimeofday(&start, NULL);
    int check_eviction_count = 3333;
    if(expected_count > 1000000) {
        check_eviction_count = 3333333;
    } else if(expected_count > 100000) {
        check_eviction_count = 333333;
    } else if(expected_count > 10000) {
        check_eviction_count = 33333;
    }
    while(std::getline(in, line)) {
        if(line.length() > (unsigned) CONSTS::MAX_KEY_LENGHTH) {
            continue;
        }

        assert(db.Add(line, line) == MBError::SUCCESS);
        count++;

        if(count % 1000000 == 0) {
            std::cout << "added " << count << "\n";
        }
        if(count % check_eviction_count == 0) {
            // Prune by total count
            assert(db.CollectResource(0xFFFFFFFFFFFF, 0xFFFFFFFFFFFF, 0xFFFFFFFFFFFF, 100) == MBError::SUCCESS);
        } 
    }
    gettimeofday(&stop, NULL);
    in.close();

    assert(db.UnsetAsyncWriterPtr(&db_async) == MBError::SUCCESS);
    db.Close();

    while(db_async.AsyncWriterBusy()) {
        usleep(50);
    }
    db_async.PrintStats();
    //TODOOOassert(db_async.Count() == expected_count);
    db_async.Close();
    int64_t tmdiff = (stop.tv_sec-start.tv_sec)*1000000 + (stop.tv_usec-start.tv_usec);
    std::cout << "count: " << count << "   time: " << 1.0*tmdiff/expected_count << "\n";
}

static void backup_test(std::string &backup_dir, std::string &list_file, MBConfig &mbconf, int64_t expected_count)
{
    DB db(mbconf);
    assert(db.Status() == MBError::SUCCESS);

    MBData data;
    int count = 0;
    struct timeval start, stop;
    gettimeofday(&start, NULL);

    DB::iterator iter = db.begin();
    std::string first_key = iter.key;

    // Create Backup DB directory
    std::string backup_mkdir_cmd = "mkdir " + backup_dir;
    if(system(backup_mkdir_cmd.c_str()) != 0) {
    }

    // backup DB
    assert(db.Backup(backup_dir.c_str()) == MBError::SUCCESS);

    // Remove key to verify existence later in backed up DB
    assert(db.Remove(first_key) == MBError::SUCCESS);
    assert(db.Find(first_key, data) != MBError::SUCCESS);

    // Open Backed up DB
    MBConfig mbconf_bak;
    memset(&mbconf_bak, 0, sizeof(mbconf_bak));
    mbconf_bak.mbdir = backup_dir.c_str();
    mbconf_bak.block_size_index = BLOCK_SIZE_ALIGN;
    mbconf_bak.block_size_data = 2*BLOCK_SIZE_ALIGN;
    mbconf_bak.num_entry_per_bucket = 512;
    DB db_bak(mbconf_bak);

    // Verify backed up DB has the first_key
    assert(db_bak.Find(first_key, data) == MBError::SUCCESS);

    // Now add removed key back
    assert(db.Add(first_key.c_str(), first_key.length(), data, true) == MBError::SUCCESS);

    gettimeofday(&stop, NULL);
    count = db.Count();
    int64_t tmdiff = (stop.tv_sec-start.tv_sec)*1000000 + (stop.tv_usec-start.tv_usec);
    std::cout << "count: " << count << "   time: " << 1.0*tmdiff/count << "\n";
    assert(db.Count() == expected_count);
    assert(db_bak.Count() == expected_count);
    db_bak.Close();
    db.Close();
}


static void SetTestStatus(bool success)
{
    std::string cmd;
    if(success) {
        cmd = std::string("touch ") + MB_DIR + "/_success";
    } else {
        cmd = std::string("rm ") + MB_DIR + "/_success >" + MB_DIR + "/out 2>" + MB_DIR + "/err";
    }
    if(system(cmd.c_str()) != 0) {
    }
}

int main(int argc, char *argv[])
{
    if(argc > 1) {
        MB_DIR = argv[1];
    }
    clean_db_dir();
    SetTestStatus(false);

    std::string test_list_file = "./test_list";
    if(argc > 2) {
        test_list_file = argv[2];
    }

    std::string backup_dir = std::string(MB_DIR) + "/backup/";
    if(argc > 3) {
        backup_dir = argv[3];
    }

    DB::SetLogFile(std::string(MB_DIR) + "/mabain.log");
    std::ifstream test_in(test_list_file);
    assert(test_in.is_open());
    std::string file;
    std::string mode;
    std::string remove;
    int64_t memcap;
    int64_t expected_count;
    bool remove_db;
    uint32_t db_id = 1;
    MBConfig mbconf;
    memset(&mbconf, 0, sizeof(mbconf));

    mbconf.mbdir = MB_DIR;
    mbconf.block_size_index = BLOCK_SIZE_ALIGN;
    mbconf.block_size_data = 2*BLOCK_SIZE_ALIGN;
    mbconf.num_entry_per_bucket = 512;
    while(test_in >> file >> mode >> memcap >> remove >> expected_count) {
        if(file[0] == '#') {
            std::cout << file << "\n";
            continue;
        }
        std::cout << "============================================\n";
        std::cout << file << " " << mode << " " << memcap << " " << remove << " " << expected_count << "\n";
        mbconf.connect_id = db_id;
        mbconf.memcap_index = memcap;
        mbconf.memcap_data = memcap;

        if(remove.compare("remove") == 0) {
            remove_db = true;
        } else {
            remove_db = false;
        }
        if(mode.compare("load") == 0) {
            mbconf.options = CONSTS::ACCESS_MODE_WRITER | CONSTS::USE_SLIDING_WINDOW;
            load_test(file, mbconf, expected_count);
        } else if(mode.compare("update") == 0) {
            mbconf.options = CONSTS::ACCESS_MODE_WRITER | CONSTS::USE_SLIDING_WINDOW;
            update_test(file, mbconf, expected_count);
        } else if(mode.compare("lookup") == 0) {
            mbconf.options = CONSTS::ACCESS_MODE_READER | CONSTS::USE_SLIDING_WINDOW;
            lookup_test(file, mbconf, expected_count);
        } else if(mode.compare("prefix") == 0) {
            mbconf.options = CONSTS::ACCESS_MODE_READER | CONSTS::USE_SLIDING_WINDOW;
            prefix_lookup_test(file, mbconf, expected_count);
        } else if(mode.compare("longest_prefix") == 0) {
            mbconf.options = CONSTS::ACCESS_MODE_READER;
            longest_prefix_lookup_test(file, mbconf, expected_count);
        } else if(mode.compare("iterator") == 0) {
            mbconf.options = CONSTS::ACCESS_MODE_READER;
            iterator_test(mbconf, expected_count);
        } else if(mode.compare("delete") == 0) {
            mbconf.options = CONSTS::ACCESS_MODE_WRITER;
            delete_test(file, mbconf, expected_count);
        } else if(mode.compare("delete_odd") == 0) {
            mbconf.options = CONSTS::ACCESS_MODE_WRITER | CONSTS::USE_SLIDING_WINDOW;
            delete_odd_test(file, mbconf, expected_count);
        } else if(mode.compare("shrink") == 0) {
            mbconf.options = CONSTS::ACCESS_MODE_WRITER | CONSTS::USE_SLIDING_WINDOW;
            shrink_test(file, mbconf, expected_count);
        } else if(mode.compare("prune") == 0) {
            mbconf.options = CONSTS::ACCESS_MODE_WRITER | CONSTS::USE_SLIDING_WINDOW;
            prune_test(mbconf, expected_count);
        } else if(mode.compare("eviction") == 0) {
            async_eviction_test(file, mbconf, expected_count);
        } else if(mode.compare("backup") == 0) {
            mbconf.options = CONSTS::ACCESS_MODE_WRITER;
            backup_test(backup_dir, file, mbconf, expected_count);
        } else {
            std::cerr << "Unknown test\n";
            abort();
        }

        if(remove_db) {
            clean_db_dir();
            ResourcePool::getInstance().RemoveResourceByDB(std::string(MB_DIR) + "_mabain_"); // need to remove resouce for next run
            mbconf.block_size_index += BLOCK_SIZE_ALIGN;
            mbconf.block_size_data  += BLOCK_SIZE_ALIGN;
        }
        db_id++;
    }

    test_in.close();
    DB::CloseLogFile();
    SetTestStatus(true);
    return 0;
}
