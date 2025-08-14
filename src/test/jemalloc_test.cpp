/**
 * Copyright (C) 2025 Cisco Inc.
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

#include <assert.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <sys/time.h>
#include <unistd.h>
#include <vector>

#include "../db.h"
#include "../dict.h"
#include "../dict_mem.h"
#include "../error.h"
#include "../mabain_consts.h"
#include "../resource_pool.h"

using namespace mabain;

#define BLOCK_SIZE (16 * 1024 * 1024LL) // 16MB
#define MAX_NUM_DATA_BLOCKS 30
#define MAX_NUM_INDEX_BLOCKS 30

static const char* MB_DIR = "/var/tmp/mabain_test/";
static bool debug = false;
static DB* global_db = nullptr;

// Helper function to create MBConfig with specified options
static MBConfig create_mbconfig(const MBConfig& base_conf, int options)
{
    MBConfig conf;
    memcpy(&conf, &base_conf, sizeof(conf));
    conf.options = options;
    conf.block_size_data = BLOCK_SIZE;
    conf.block_size_index = BLOCK_SIZE;
    conf.max_num_data_block = MAX_NUM_DATA_BLOCKS;
    conf.max_num_index_block = MAX_NUM_INDEX_BLOCKS;
    conf.memcap_index = conf.block_size_index * conf.max_num_index_block;
    conf.memcap_data = conf.block_size_data * conf.max_num_data_block;
    return conf;
}

// Helper function to create or get global DB instance
static DB* get_or_create_global_db(const MBConfig& mbconf)
{
    if (global_db == nullptr) {
        MBConfig conf = create_mbconfig(mbconf, CONSTS::ACCESS_MODE_WRITER | CONSTS::OPTION_JEMALLOC);
        global_db = new DB(conf);
        std::cout << global_db->StatusStr() << "\n";
        assert(global_db->is_open());
    }
    return global_db;
}

static void clean_db_dir()
{
    std::string pattern1 = std::string(MB_DIR) + "/_mabain_*";
    std::string pattern2 = std::string(MB_DIR) + "/backup/_mabain_*";

    try {
        // Remove files matching pattern1
        for (const auto& entry : std::filesystem::directory_iterator(MB_DIR)) {
            if (entry.path().filename().string().find("_mabain_") == 0) {
                std::filesystem::remove_all(entry.path());
            }
        }

        // Remove files matching pattern2 in backup directory
        std::string backup_dir = std::string(MB_DIR) + "/backup";
        if (std::filesystem::exists(backup_dir)) {
            for (const auto& entry : std::filesystem::directory_iterator(backup_dir)) {
                if (entry.path().filename().string().find("_mabain_") == 0) {
                    std::filesystem::remove_all(entry.path());
                }
            }
        }
    } catch (const std::filesystem::filesystem_error& ex) {
        // Ignore errors, similar to original system() calls
    }
}

static void SetTestStatus(bool success)
{
    std::string success_file = std::string(MB_DIR) + "/_success";

    try {
        if (success) {
            std::ofstream file(success_file);
            file.close();
        } else {
            std::filesystem::remove(success_file);
        }
    } catch (const std::filesystem::filesystem_error& ex) {
        // Ignore errors, similar to original system() calls
    }
}

static void lookup_test(std::string& list_file, MBConfig& mbconf, int64_t expected_count)
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
    while (std::getline(in, line)) {
        if (line.length() > (unsigned)CONSTS::MAX_KEY_LENGHTH) {
            continue;
        }
        rval = db.Find(line.c_str(), line.length(), data);
        if (rval == MBError::SUCCESS) {
            found++;
        } else {
            if (debug)
                std::cout << "failed: " << line << "\n";
        }
        count++;
        if (count % 1000000 == 0)
            std::cout << "Looked up " << count << "\n";
    }
    gettimeofday(&stop, NULL);
    in.close();
    db.Close();
    int64_t tmdiff = (stop.tv_sec - start.tv_sec) * 1000000 + (stop.tv_usec - start.tv_usec);
    std::cout << "count: " << count << "    time: " << 1.0 * tmdiff / count << "\n";
    std::cout << "found: " << found << "\n";
    if (expected_count > 0)
        assert(found == expected_count);
}

static void iterator_test(MBConfig& mbconf, int64_t expected_count)
{
    DB db(mbconf);
    assert(db.Status() == MBError::SUCCESS);

    int count = 0;
    struct timeval start, stop;
    gettimeofday(&start, NULL);
    for (DB::iterator iter = db.begin(); iter != db.end(); ++iter) {
        count++;
        if (count % 1000000 == 0) {
            std::cout << "iterated " << count << "\n";
        }
    }
    gettimeofday(&stop, NULL);
    db.PrintStats();
    int64_t db_cnt = db.Count();
    db.Close();
    int64_t tmdiff = (stop.tv_sec - start.tv_sec) * 1000000 + (stop.tv_usec - start.tv_usec);
    std::cout << "count: " << count << "   time: " << 1.0 * tmdiff / count << "\n";
    std::cout << "db_cnt: " << db_cnt << "  expected_count: " << expected_count << "\n";
    assert(count == db_cnt);
    assert(expected_count == count);
}

static void jemalloc_remove_all_test(std::string& list_file, const MBConfig& mbconf, int64_t expected_count)
{
    std::cout << "jemalloc remove all test: " << list_file << "\n";

    // Use global DB instance if it exists, otherwise create a new one
    global_db = get_or_create_global_db(mbconf);

    std::ifstream in(list_file.c_str());
    std::string line;
    std::vector<std::string> keys;
    std::cout << "Loading " << list_file << "\n";
    while (std::getline(in, line)) {
        if (line.length() > (unsigned)CONSTS::MAX_KEY_LENGHTH) {
            continue;
        }
        global_db->Add(line, line);
        keys.push_back(line);
    }
    in.close();

    std::cout << "test RemoveAll API\n";
    int rval = global_db->RemoveAll();
    assert(rval == MBError::SUCCESS);

    // For lookup and iterator tests, we need to create temporary reader instances
    MBConfig conf = create_mbconfig(mbconf, CONSTS::ACCESS_MODE_READER);

    lookup_test(list_file, conf, 0);
    iterator_test(conf, 0);

    for (size_t i = 0; i < keys.size(); i++) {
        global_db->Add(keys[i], keys[i]);
    }
    global_db->PrintStats();
    lookup_test(list_file, conf, keys.size());
    iterator_test(conf, keys.size());

    std::cout << "remove all test passed: " << list_file << "\n";
}

static void jemalloc_test(std::string& list_file, const MBConfig& mbconf, int64_t expected_count)
{
    std::cout << "Jemalloc test: " << list_file << "\n";

    // Use global DB instance if it exists, otherwise create a new one
    global_db = get_or_create_global_db(mbconf);

    std::ifstream in(list_file.c_str());
    std::string line;
    std::vector<std::string> keys;
    std::cout << "Loading " << list_file << "\n";
    while (std::getline(in, line)) {
        if (line.length() > (unsigned)CONSTS::MAX_KEY_LENGHTH) {
            continue;
        }
        global_db->Add(line, line);
        keys.push_back(line);
    }
    in.close();

    std::cout << "iterating test with jemalloc loading " << list_file << "\n";

    // For lookup and iterator tests, we need to create temporary reader instances
    MBConfig conf = create_mbconfig(mbconf, CONSTS::ACCESS_MODE_READER);

    lookup_test(list_file, conf, keys.size());
    iterator_test(conf, keys.size());

    std::cout << "Removing " << list_file << "\n";
    for (size_t i = 0; i < keys.size(); i++) {
        global_db->Remove(keys[i]);
    }

    global_db->PrintStats();
    std::cout << "Jemalloc test passed: " << list_file << "\n";
}

static void tracking_buffer_test(std::string& list_file, const MBConfig& mbconf, int64_t expected_count)
{
    std::cout << "Tracking buffer test: " << list_file << "\n";

    // Use global DB instance if it exists, otherwise create a new one
    global_db = get_or_create_global_db(mbconf);

    std::ifstream in(list_file.c_str());
    std::string line;
    std::vector<std::string> keys;
    std::cout << "Loading " << list_file << "\n";
    while (std::getline(in, line)) {
        if (line.length() > (unsigned)CONSTS::MAX_KEY_LENGHTH) {
            continue;
        }
        global_db->Add(line, line);
        keys.push_back(line);
    }
    in.close();
    std::cout << "Removing " << list_file << "\n";
    for (size_t i = 0; i < keys.size(); i++) {
        global_db->Remove(keys[i]);
    }

    std::ostringstream oss;
    global_db->PrintStats(oss);
    std::string stats = oss.str();
    std::cout << stats;
    if (stats.find("Size of tracking buffer: 0") == std::string::npos) {
        std::cerr << "Tracking buffer size is not 0\n";
        exit(1);
    }
    // Note root index node is never removed
    if (stats.find("Size of index tracking buffer: 1") == std::string::npos) {
        std::cerr << "Size of index tracking buffer is not 1\n";
        exit(1);
    }

    std::cout << "Tracking buffer test passed: " << list_file << "\n";
}

int main(int argc, char* argv[])
{
    if (argc > 1) {
        MB_DIR = argv[1];
    }
    clean_db_dir();
    SetTestStatus(false);

    std::string test_list_file = "./test_list";
    if (argc > 2) {
        test_list_file = argv[2];
    }

    std::string backup_dir = std::string(MB_DIR) + "/backup/";
    if (argc > 3) {
        backup_dir = argv[3];
    }

    DB::SetLogFile(std::string(MB_DIR) + "/mabain.log");
    DB::LogDebug();
    std::ifstream test_in(test_list_file);
    assert(test_in.is_open());
    std::string file;
    std::string mode;
    std::string remove;
    int64_t memcap;
    int64_t expected_count;
    bool remove_db = false;
    uint32_t db_id = 1;
    MBConfig mbconf;
    memset(&mbconf, 0, sizeof(mbconf));

    mbconf.mbdir = MB_DIR;
    mbconf.block_size_index = BLOCK_SIZE;
    mbconf.block_size_data = BLOCK_SIZE;
    mbconf.num_entry_per_bucket = 512;
    while (test_in >> file >> mode >> memcap >> remove >> expected_count) {
        if (file[0] == '#') {
            // comment line
            continue;
        }

        std::cout << "============================================\n";
        std::cout << file << " " << mode << " " << memcap << " " << remove << " " << expected_count << "\n";
        mbconf.connect_id = db_id;
        mbconf.memcap_index = memcap;
        mbconf.memcap_data = memcap;

        if (remove.compare("remove") == 0) {
            remove_db = true;
        } else {
            remove_db = false;
        }

        if (mode.compare("jemalloc") == 0) {
            jemalloc_test(file, mbconf, expected_count);
        } else if (mode.compare("remove_all") == 0) {
            jemalloc_remove_all_test(file, mbconf, expected_count);
        } else if (mode.compare("tracking_buffer") == 0) {
            tracking_buffer_test(file, mbconf, expected_count);
        } else {
            std::cerr << "Unknown test mode for jemalloc_test: " << mode << "\n";
            std::cerr << "Supported modes: jemalloc, remove_all, tracking_buffer\n";
            abort();
        }

        if (remove_db) {
            // Call RemoveAll API instead of manual cleanup
            DB db_cleanup(mbconf);
            if (db_cleanup.is_open()) {
                db_cleanup.RemoveAll();
                db_cleanup.Close();
            }
        }
        db_id++;
    }

    test_in.close();

    // Clean up global DB instance
    if (global_db != nullptr) {
        global_db->Close();
        delete global_db;
        global_db = nullptr;
    }

    DB::CloseLogFile();
    SetTestStatus(true);
    return 0;
}
