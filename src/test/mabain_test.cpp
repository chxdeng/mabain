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

using namespace mabain;

const std::string MB_DIR = std::string("/var/tmp/mabain_test/");

static void clean_db_dir()
{
    std::string cmd = std::string("rm -rf ") + MB_DIR + "*";
    if(system(cmd.c_str()) != 0) {
    }
}

static void clear_mabain_db()
{
    int option = CONSTS::WriterOptions();
    option &= ~CONSTS::ASYNC_WRITER_MODE;
    DB db(MB_DIR, option, 0, 0);
    if(db.Status() != MBError::SUCCESS) {
        std::cout << "DB error: " << MBError::get_error_str(db.Status()) << "\n";
        exit(1);
    }
    db.RemoveAll();
    db.Close();
    std::cout << "All entries in db were removed\n";
}

#define VALUE_LENGTH 16
static void load_test(std::string &list_file, int64_t memcap, uint32_t db_id,
               int64_t expected_count)
{
    std::cout << "Loading " << list_file << "\n";
    int option = CONSTS::WriterOptions();
    option &= ~CONSTS::ASYNC_WRITER_MODE;
    DB db(MB_DIR, option, memcap, 0, 10, db_id);
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
std::cout<<expected_count<<" "<<db_cnt<<"\n";
    assert(expected_count == db_cnt);
}

static void lookup_test(std::string &list_file, int64_t memcap, uint32_t db_id,
                 int64_t expected_count)
{
    std::cout << "Lookup test: " << list_file << "\n";
    DB db(MB_DIR, CONSTS::ReaderOptions(), memcap, 0, 0, db_id);
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
            //std::cout << "failed: " << line << "\n";
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

static void prefix_lookup_test(std::string &list_file, int64_t memcap,
                        uint32_t db_id, int64_t expected_count)
{
    std::cout << "Lookup test: " << list_file << "\n";
    DB db(MB_DIR, CONSTS::ReaderOptions(), memcap, 0, 0, db_id);
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

static void longest_prefix_lookup_test(std::string &list_file, int64_t memcap,
                                uint32_t db_id, int64_t expected_count)
{
    std::cout << "Lookup test: " << list_file << "\n";
    DB db(MB_DIR, CONSTS::ReaderOptions(), memcap, 0, 0, db_id);
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

static void delete_odd_test(std::string &list_file, int64_t memcap, uint32_t db_id,
                 int64_t expected_count)
{
    std::cout << "deletion odd-entry test: " << list_file << "\n";
    int option = CONSTS::WriterOptions();
    option &= ~CONSTS::ASYNC_WRITER_MODE;
    DB db(MB_DIR, option, memcap, 0, 0, db_id);
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

static void delete_test(std::string &list_file, int64_t memcap, uint32_t db_id,
                 int64_t expected_count)
{
    std::cout << "deletion test: " << list_file << "\n";
    int option = CONSTS::WriterOptions();
    option &= ~CONSTS::ASYNC_WRITER_MODE;
    DB db(MB_DIR, option, memcap, 0, 0, db_id);
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
        rval = db.Remove(line);
        if(rval == MBError::SUCCESS) {
            nfound++;
        } else {
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

static void shrink_test(std::string &list_file, int64_t memcap, uint32_t db_id,
                 int64_t expected_count)
{
    std::cout << "shrink test: " << list_file << "\n";
    int option = CONSTS::WriterOptions();
    option &= ~CONSTS::ASYNC_WRITER_MODE;
    DB db(MB_DIR, option, memcap, 0, 0, db_id);
    assert(db.Status() == MBError::SUCCESS);
    struct timeval start, stop;

    gettimeofday(&start, NULL);
    int rval = db.CollectResource(0, 0);
    gettimeofday(&stop, NULL);
    assert(rval == MBError::SUCCESS);

    db.PrintStats();
    db.Close();
    int64_t tmdiff = (stop.tv_sec-start.tv_sec)*1000000 + (stop.tv_usec-start.tv_usec);
    std::cout << "shrink time: " << tmdiff/1000000. << "\n";
}

static void iterator_test(int64_t memcap, uint32_t db_id, int64_t expected_count)
{
    DB db(MB_DIR, CONSTS::ReaderOptions(), memcap, 0, 0, db_id);
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

int main(int argc, char *argv[])
{
    clean_db_dir();

    std::string test_list_file = "./test_list";
    if(argc == 2) {
        test_list_file = argv[1];
    }
    std::ifstream test_in(test_list_file);
    assert(test_in.is_open());
    std::string file;
    std::string mode;
    std::string remove;
    int64_t memcap;
    int64_t expected_count;
    bool remove_db;
    uint32_t db_id = 1;
    while(test_in >> file >> mode >> memcap >> remove >> expected_count) {
        if(file[0] == '#') {
            std::cout << file << "\n";
            continue;
        }
        std::cout << "============================================\n";
        std::cout << file << " " << mode << " " << memcap << " " << remove << " " << expected_count << "\n";
        if(remove.compare("remove") == 0) {
            remove_db = true;
        } else {
            remove_db = false;
        }
        if(mode.compare("load") == 0) {
            load_test(file, memcap, db_id, expected_count);
        } else if(mode.compare("lookup") == 0) {
            lookup_test(file, memcap, db_id, expected_count);
        } else if(mode.compare("prefix") == 0) {
            prefix_lookup_test(file, memcap, db_id, expected_count);
        } else if(mode.compare("longest_prefix") == 0) {
            longest_prefix_lookup_test(file, memcap, db_id, expected_count);
        } else if(mode.compare("iterator") == 0) {
            iterator_test(memcap, db_id, expected_count);
        } else if(mode.compare("delete") == 0) {
            delete_test(file, memcap, db_id, expected_count);
        } else if(mode.compare("delete_odd") == 0) {
            delete_odd_test(file, memcap, db_id, expected_count);
        } else if(mode.compare("shrink") == 0) {
            shrink_test(file, memcap, db_id, expected_count);
        } else {
            std::cerr << "Unknown test\n";
            abort();
        }
       
        if(remove_db) {
            clear_mabain_db();
        }
        db_id++;
    }
    test_in.close();
    return 0;
}
